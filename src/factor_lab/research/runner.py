"""Resumable two-stage historical factor research runner."""

from __future__ import annotations

import hashlib
import json
import math
import re
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from factor_lab.portfolio import LongOnlyPortfolioConfig, evaluate_long_only_portfolio

from .contracts import FactorSpec, ValidationSpec
from .reporting import render_report
from .signals import directed_rank_blend, evaluate_factor_signal
from .validation import (
    FactorValidation,
    StageASelection,
    build_stage_a_selection,
    deterministic_block_bootstrap_mean,
    diagnose_train_similarity,
    evaluate_stage_a,
)


ENGINE_ID = "factor-lab/research/v4"
EVIDENCE_CLASS = "historical_diagnostic"
RESULTS_FIRST_SUITE = "results-first"
_DETAIL_FIELDS = {"periods", "trades", "optimization_audit"}
_REQUIRED_PROMOTION_GATE_KEYS = {
    "validation_net_excess_annual_return_min",
    "validation_net_sharpe_min",
    "validation_information_ratio_min",
    "validation_max_drawdown_min",
    "positive_half_year_ratio_min",
    "average_holding_count_min",
    "capacity_violation_count_max",
    "validation_excess_mean_bootstrap_lower_min",
    "benchmark_return_coverage_min",
    "execution_input_policy_match_ratio_min",
    "execution_input_future_violation_count_max",
    "execution_input_coverage_min",
    "validation_observations_min",
    "execution_period_coverage_min",
    "signal_evaluable_date_ratio_min",
    "signal_median_cross_section_coverage_min",
}
_ROBUSTNESS_ABSOLUTE_BLOCKERS = {
    "average_holding_count_below_threshold",
    "capacity_violation",
    "benchmark_return_coverage_below_threshold",
    "execution_input_policy_mismatch",
    "future_execution_input_detected",
    "execution_input_coverage_below_threshold",
    "validation_observations_below_threshold",
    "execution_period_coverage_below_threshold",
    "validation_signal_evaluable_ratio_below_threshold",
    "validation_signal_cross_section_coverage_below_threshold",
}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    temporary.replace(path)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _completed_run_valid(summary_path: Path, output_dir: Path, run_fingerprint: str) -> bool:
    """Verify the immutable outputs before accepting a completed checkpoint."""

    manifest_path = output_dir / "manifest.json"
    if not summary_path.is_file() or not manifest_path.is_file():
        return False
    try:
        summary = _read_json(summary_path)
        manifest = _read_json(manifest_path)
        if summary.get("status") != "completed":
            return False
        if summary.get("run_fingerprint") != run_fingerprint:
            return False
        if manifest.get("run_fingerprint") != run_fingerprint:
            return False
        rows = manifest.get("files") or []
        if not isinstance(rows, list) or not rows:
            return False
        root = output_dir.resolve()
        names: set[str] = set()
        for row in rows:
            relative = Path(str(row["path"]))
            if relative.is_absolute():
                return False
            path = (output_dir / relative).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                return False
            normalized = relative.as_posix()
            if normalized in names or _sha256_file(path) != row.get("sha256"):
                return False
            names.add(normalized)
        required = {"summary.json", "report.md"}
        required.update(
            f"factors/{_safe_name(str(name))}.json"
            for name in summary.get("stage_b_selected") or []
        )
        return required.issubset(names)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return False


def _git_state(project_root: Path) -> dict[str, Any]:
    def command(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", *args], cwd=project_root, check=True, capture_output=True, text=True
            )
            return result.stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            return None

    return {
        "commit": command("rev-parse", "HEAD"),
        "branch": command("branch", "--show-current"),
        "dirty": bool(command("status", "--porcelain")),
    }


def _implementation_sha256() -> str:
    package_root = Path(__file__).resolve().parents[1]
    paths = [
        *sorted((package_root / "research").glob("*.py")),
        *sorted((package_root / "portfolio").glob("*.py")),
        package_root / "cli.py",
    ]
    digest = hashlib.sha256()
    for path in paths:
        if not path.is_file():
            continue
        digest.update(path.relative_to(package_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _project_root(value: str | Path | None = None) -> Path:
    if value is not None:
        return Path(value).resolve()
    return Path(__file__).resolve().parents[3]


def _default_data_paths(root: Path) -> tuple[Path, Path]:
    canonical = root / "runtime" / "data" / "top500"
    features = canonical / "features.parquet"
    execution = canonical / "execution.parquet"
    if features.is_file() and execution.is_file():
        return features, execution
    legacy = root / "artifacts" / "expanded_long_only" / "feature_store"
    return legacy / "expanded_top500_features.parquet", legacy / "expanded_execution_prices.parquet"


def load_factor_suite(path: str | Path, suite: str) -> tuple[FactorSpec, list[FactorSpec]]:
    payload = _read_json(Path(path))
    control = FactorSpec.from_mapping(payload.get("control") or {})
    suite_rows = (payload.get("suites") or {}).get(suite)
    if not isinstance(suite_rows, list):
        raise ValueError(f"unknown factor suite: {suite}")
    rows = [FactorSpec.from_mapping(row) for row in suite_rows]
    names = [control.name, *(row.name for row in rows)]
    if len(names) != len(set(names)):
        rows = [row for row in rows if row.name != control.name]
    return control, rows


def _validation_spec(config: Mapping[str, Any]) -> ValidationSpec:
    values = dict(config.get("validation") or {})
    return ValidationSpec(**values)


def _portfolio_config(config: Mapping[str, Any]) -> LongOnlyPortfolioConfig:
    return LongOnlyPortfolioConfig.from_mapping(config)


def _feature_columns(path: Path, factors: Sequence[FactorSpec], validation: ValidationSpec) -> list[str]:
    available = set(pq.ParquetFile(path).schema_arrow.names)
    wanted = {
        validation.date_column,
        "ticker",
        "eligible",
        "universe_member",
        "st_filter_status",
        "label_exit_date",
        "financial_available_date",
        *validation.label_columns,
    }
    for factor in factors:
        wanted.update(factor.required_fields)
    missing = sorted({validation.date_column, "ticker"} - available)
    if missing:
        raise ValueError(f"feature store missing required columns: {missing}")
    return sorted(wanted & available)


def _load_features(path: Path, factors: Sequence[FactorSpec], validation: ValidationSpec) -> pd.DataFrame:
    frame = pd.read_parquet(path, columns=_feature_columns(path, factors, validation))
    frame[validation.date_column] = pd.to_datetime(frame[validation.date_column], errors="coerce")
    frame["ticker"] = frame["ticker"].astype(str)
    if frame[validation.date_column].isna().any():
        raise ValueError("feature store contains invalid dates")
    if frame.duplicated([validation.date_column, "ticker"]).any():
        raise ValueError("feature store contains duplicate date/ticker rows")
    input_rows = len(frame)
    applied: list[str] = []
    research_mask = pd.Series(True, index=frame.index)
    status_counts = (
        {
            str(key): int(value)
            for key, value in frame["st_filter_status"].fillna("missing").value_counts().items()
        }
        if "st_filter_status" in frame.columns
        else {}
    )
    for column in ("eligible", "universe_member"):
        if column not in frame.columns:
            continue
        applied.append(column)
        values = frame[column]
        if values.dtype == bool:
            accepted = values.fillna(False)
        else:
            accepted = values.astype(str).str.strip().str.casefold().isin(
                {"1", "true", "yes", "y"}
            )
        research_mask &= accepted
    excluded_status_counts = (
        {
            str(key): int(value)
            for key, value in frame.loc[~research_mask, "st_filter_status"]
            .fillna("missing")
            .value_counts()
            .items()
        }
        if "st_filter_status" in frame.columns
        else {}
    )
    frame = frame.loc[research_mask].copy()
    if frame.empty:
        raise ValueError("feature store has no eligible universe rows for research")
    result = frame.sort_values([validation.date_column, "ticker"]).reset_index(drop=True)
    result.attrs["research_universe_filter"] = {
        "columns_applied": applied,
        "input_row_count": int(input_rows),
        "included_row_count": int(len(result)),
        "excluded_row_count": int(input_rows - len(result)),
        "included_ratio": round(float(len(result) / input_rows), 8) if input_rows else 0.0,
        "st_filter_status_counts": status_counts,
        "excluded_st_filter_status_counts": excluded_status_counts,
    }
    return result


def _resolve_column(available: set[str], preferred: str, aliases: Sequence[str]) -> str | None:
    return next((name for name in (preferred, *aliases) if name in available), None)


def _load_execution(path: Path, config: LongOnlyPortfolioConfig) -> pd.DataFrame:
    available = set(pq.ParquetFile(path).schema_arrow.names)
    date_column = _resolve_column(available, config.date_column, ("date", "trade_date"))
    ticker_column = _resolve_column(available, config.ticker_column, ("ticker", "ts_code", "symbol"))
    open_column = _resolve_column(available, config.open_column, ("open_price", "open_adj", "open"))
    adv_column = _resolve_column(available, config.adv_column, ("amount_20d_avg", "adv", "average_daily_value"))
    volatility_column = _resolve_column(available, config.volatility_column, ("volatility", "vol_20"))
    required = {
        "date": date_column,
        "ticker": ticker_column,
        "open": open_column,
        "adv": adv_column,
        "volatility": volatility_column,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        raise ValueError(f"execution store missing required fields: {missing}")
    optional = {
        *config.eligible_columns,
        config.limit_up_column,
        config.limit_down_column,
        "is_suspended",
        "is_delisted",
        "split_ratio",
        "cash_dividend",
    }
    columns = {value for value in required.values() if value} | (optional & available)
    frame = pd.read_parquet(path, columns=sorted(columns))
    assert date_column and ticker_column
    frame[date_column] = pd.to_datetime(frame[date_column], errors="coerce")
    frame[ticker_column] = frame[ticker_column].astype(str)
    return frame.dropna(subset=[date_column, ticker_column]).sort_values([date_column, ticker_column])


def _compound(values: Iterable[float]) -> float:
    array = np.asarray(list(values), dtype=float)
    return float(np.prod(1.0 + array) - 1.0) if len(array) else 0.0


def _annualized(values: Sequence[float], periods_per_year: float) -> float:
    if not values:
        return 0.0
    total = _compound(values)
    if total <= -1.0:
        return -1.0
    return float((1.0 + total) ** (periods_per_year / len(values)) - 1.0)


def _ratio(values: Sequence[float], periods_per_year: float) -> float:
    if len(values) < 2:
        return 0.0
    standard_deviation = float(np.std(values, ddof=1))
    return float(np.mean(values) / standard_deviation * math.sqrt(periods_per_year)) if standard_deviation else 0.0


def _drawdown(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    curve = np.cumprod(1.0 + np.asarray(values, dtype=float))
    peak = np.maximum.accumulate(curve)
    return float(np.min(curve / peak - 1.0))


def _window_metrics(
    periods: Sequence[Mapping[str, Any]],
    *,
    start: str,
    end: str | None,
    periods_per_year: float,
    bootstrap_spec: ValidationSpec | None = None,
    bootstrap_key: str = "portfolio_active_return",
    expected_observations: int | None = None,
) -> dict[str, Any]:
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) if end else None
    rows = [
        row
        for row in periods
        if pd.Timestamp(row["signal_date"]) >= lower
        and (upper is None or pd.Timestamp(row["signal_date"]) <= upper)
        and (upper is None or pd.Timestamp(row["end_date"]) <= upper)
    ]
    net = [float(row.get("net_return") or 0.0) for row in rows]
    gross = [float(row.get("gross_return") or 0.0) for row in rows]
    benchmark = [float(row.get("benchmark_return") or 0.0) for row in rows]
    excess = [left - right for left, right in zip(net, benchmark)]
    half_year: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        date = pd.Timestamp(row["signal_date"])
        half_year.setdefault(f"{date.year}-H{1 if date.month <= 6 else 2}", []).append(row)
    half_year_excess = [
        _compound(float(item.get("net_return") or 0.0) for item in group)
        - _compound(float(item.get("benchmark_return") or 0.0) for item in group)
        for group in half_year.values()
    ]
    net_annual = _annualized(net, periods_per_year)
    benchmark_annual = _annualized(benchmark, periods_per_year)
    benchmark_expected = int(
        sum(int(row.get("benchmark_expected_endpoint_count") or 0) for row in rows)
    )
    benchmark_observed = int(
        sum(int(row.get("benchmark_observed_endpoint_count") or 0) for row in rows)
    )
    benchmark_complete = int(
        sum(int(row.get("benchmark_complete_return_count") or 0) for row in rows)
    )
    benchmark_constituents = benchmark_expected // 2
    bootstrap = (
        deterministic_block_bootstrap_mean(
            excess,
            samples=bootstrap_spec.bootstrap_samples,
            block_size=bootstrap_spec.bootstrap_block_size,
            confidence=bootstrap_spec.bootstrap_confidence,
            seed=bootstrap_spec.bootstrap_seed,
            key=bootstrap_key,
        ).to_dict()
        if bootstrap_spec is not None
        else None
    )
    input_dates = [
        pd.Timestamp(value)
        for row in rows
        for value in (row.get("execution_input_min_date"), row.get("execution_input_max_date"))
        if value
    ]
    future_input_violations = sum(
        int(row.get("execution_input_future_violation_count") or 0)
        for row in rows
    )
    execution_policy_matches = sum(
        str(row.get("execution_input_policy")) == "previous_visible_ticker_row"
        for row in rows
    )
    execution_input_required = int(
        sum(int(row.get("execution_input_required_count") or 0) for row in rows)
    )
    execution_input_observed = int(
        sum(int(row.get("execution_input_observed_count") or 0) for row in rows)
    )
    expected_periods = int(expected_observations) if expected_observations is not None else len(rows)
    return {
        "start": start,
        "end": end,
        "observations": len(rows),
        "expected_observations": expected_periods,
        "execution_period_coverage": round(
            len(rows) / expected_periods if expected_periods else 0.0, 8
        ),
        "net_return": round(_compound(net), 8),
        "gross_return": round(_compound(gross), 8),
        "benchmark_return": round(_compound(benchmark), 8),
        "net_annual_return": round(net_annual, 8),
        "benchmark_annual_return": round(benchmark_annual, 8),
        "net_excess_annual_return": round(net_annual - benchmark_annual, 8),
        "net_sharpe": round(_ratio(net, periods_per_year), 8),
        "information_ratio": round(_ratio(excess, periods_per_year), 8),
        "max_drawdown": round(_drawdown(net), 8),
        "positive_half_year_ratio": round(float(np.mean(np.asarray(half_year_excess) > 0.0)), 8)
        if half_year_excess
        else 0.0,
        "average_holding_count": round(float(np.mean([row.get("holding_count", 0) for row in rows])), 6)
        if rows
        else 0.0,
        "actual_turnover": round(float(np.mean([row.get("turnover", 0.0) for row in rows])), 8)
        if rows
        else 0.0,
        "annualized_turnover": round(
            float(np.mean([row.get("turnover", 0.0) for row in rows])) * periods_per_year,
            8,
        )
        if rows
        else 0.0,
        "benchmark_expected_endpoint_count": benchmark_expected,
        "benchmark_observed_endpoint_count": benchmark_observed,
        "benchmark_complete_return_count": benchmark_complete,
        "benchmark_missing_start_count": int(
            sum(int(row.get("benchmark_missing_start_count") or 0) for row in rows)
        ),
        "benchmark_missing_end_count": int(
            sum(int(row.get("benchmark_missing_end_count") or 0) for row in rows)
        ),
        "benchmark_endpoint_coverage": round(
            benchmark_observed / benchmark_expected if benchmark_expected else 0.0, 8
        ),
        "benchmark_return_coverage": round(
            benchmark_complete / benchmark_constituents if benchmark_constituents else 0.0,
            8,
        ),
        "excess_return_mean_bootstrap": bootstrap,
        "excess_return_mean_bootstrap_lower": (
            bootstrap.get("lower") if bootstrap is not None else None
        ),
        "execution_input_policy": "previous_visible_ticker_row",
        "execution_input_policy_match_ratio": round(
            execution_policy_matches / len(rows) if rows else 0.0, 8
        ),
        "execution_input_future_violation_count": int(future_input_violations),
        "execution_input_required_count": execution_input_required,
        "execution_input_observed_count": execution_input_observed,
        "execution_input_coverage": round(
            execution_input_observed / execution_input_required
            if execution_input_required
            else 1.0,
            8,
        ),
        "max_execution_input_age_days": max(
            (int(row.get("max_execution_input_age_days") or 0) for row in rows),
            default=0,
        ),
        "execution_input_min_date": str(min(input_dates).date()) if input_dates else None,
        "execution_input_max_date": str(max(input_dates).date()) if input_dates else None,
        "capacity_violation_count": int(sum(int(row.get("capacity_violation_count") or 0) for row in rows)),
        "blocked_trade_count": int(sum(int(row.get("blocked_trade_count") or 0) for row in rows)),
        "total_cost": round(float(sum(float((row.get("costs") or {}).get("total") or 0.0) for row in rows)), 4),
    }


def _gate(metrics: Mapping[str, Any], config: Mapping[str, Any]) -> tuple[bool, list[str]]:
    gate = config.get("promotion_gate") or {}
    missing_gate_keys = sorted(_REQUIRED_PROMOTION_GATE_KEYS - set(gate))
    checks = [
        (float(metrics.get("net_excess_annual_return") or 0.0) > float(gate.get("validation_net_excess_annual_return_min", 0.0)), "non_positive_validation_net_excess"),
        (float(metrics.get("net_sharpe") or 0.0) >= float(gate.get("validation_net_sharpe_min", 0.8)), "validation_sharpe_below_threshold"),
        (float(metrics.get("information_ratio") or 0.0) >= float(gate.get("validation_information_ratio_min", 0.5)), "validation_information_ratio_below_threshold"),
        (float(metrics.get("max_drawdown") or 0.0) >= float(gate.get("validation_max_drawdown_min", -0.25)), "validation_drawdown_exceeds_limit"),
        (float(metrics.get("positive_half_year_ratio") or 0.0) >= float(gate.get("positive_half_year_ratio_min", 0.6)), "positive_half_year_ratio_below_threshold"),
        (float(metrics.get("average_holding_count") or 0.0) >= float(gate.get("average_holding_count_min", 40)), "average_holding_count_below_threshold"),
        (int(metrics.get("capacity_violation_count") or 0) <= int(gate.get("capacity_violation_count_max", 0)), "capacity_violation"),
    ]
    if "validation_excess_mean_bootstrap_lower_min" in gate:
        lower = metrics.get("excess_return_mean_bootstrap_lower")
        checks.append(
            (
                lower is not None
                and float(lower)
                > float(gate["validation_excess_mean_bootstrap_lower_min"]),
                "validation_excess_bootstrap_lower_below_threshold",
            )
        )
    if "benchmark_return_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("benchmark_return_coverage") or 0.0)
                >= float(gate["benchmark_return_coverage_min"]),
                "benchmark_return_coverage_below_threshold",
            )
        )
    if "execution_input_policy_match_ratio_min" in gate:
        checks.append(
            (
                float(metrics.get("execution_input_policy_match_ratio") or 0.0)
                >= float(gate["execution_input_policy_match_ratio_min"]),
                "execution_input_policy_mismatch",
            )
        )
    if "execution_input_future_violation_count_max" in gate:
        checks.append(
            (
                int(metrics.get("execution_input_future_violation_count") or 0)
                <= int(gate["execution_input_future_violation_count_max"]),
                "future_execution_input_detected",
            )
        )
    if "execution_input_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("execution_input_coverage") or 0.0)
                >= float(gate["execution_input_coverage_min"]),
                "execution_input_coverage_below_threshold",
            )
        )
    if "validation_observations_min" in gate:
        minimum_observations = int(
            metrics.get("minimum_required_observations")
            or gate["validation_observations_min"]
        )
        checks.append(
            (
                int(metrics.get("observations") or 0)
                >= minimum_observations,
                "validation_observations_below_threshold",
            )
        )
    if "execution_period_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("execution_period_coverage") or 0.0)
                >= float(gate["execution_period_coverage_min"]),
                "execution_period_coverage_below_threshold",
            )
        )
    if "signal_evaluable_date_ratio_min" in gate:
        checks.append(
            (
                float(metrics.get("signal_evaluable_date_ratio") or 0.0)
                >= float(gate["signal_evaluable_date_ratio_min"]),
                "validation_signal_evaluable_ratio_below_threshold",
            )
        )
    if "signal_median_cross_section_coverage_min" in gate:
        checks.append(
            (
                float(metrics.get("signal_median_cross_section_coverage") or 0.0)
                >= float(gate["signal_median_cross_section_coverage_min"]),
                "validation_signal_cross_section_coverage_below_threshold",
            )
        )
    blockers = [
        *(f"missing_promotion_gate_config:{key}" for key in missing_gate_keys),
        *(reason for passed, reason in checks if not passed),
    ]
    return not blockers, blockers


def _audit_falsification(
    validation: FactorValidation,
    metrics: Mapping[str, Any],
    policy: ValidationSpec,
) -> tuple[bool, list[str]]:
    """Use audit evidence as a veto only, never as a ranking input."""

    signal_observations = validation.audit.evaluable_date_count
    portfolio_observations = int(metrics.get("observations") or 0)
    if min(signal_observations, portfolio_observations) < policy.audit_min_observations:
        return False, ["audit_insufficient_observations"]
    failures = list(validation.audit_signal_failures)
    active_interval = metrics.get("excess_return_mean_bootstrap") or {}
    if (
        active_interval.get("upper") is not None
        and float(active_interval["upper"]) < 0.0
    ):
        failures.append("audit_active_return_bootstrap_upper_negative")
    falsified = len(failures) >= policy.audit_min_failed_metrics
    return falsified, failures


def _compact_portfolio(payload: Mapping[str, Any]) -> dict[str, Any]:
    compact = {key: value for key, value in payload.items() if key not in _DETAIL_FIELDS}
    compact["details_omitted"] = sorted(_DETAIL_FIELDS)
    return compact


def _safe_name(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "factor"
    return value[:100]


def _expected_portfolio_observations(
    feature_frame: pd.DataFrame,
    execution_frame: pd.DataFrame,
    portfolio_config: LongOnlyPortfolioConfig,
    validation: ValidationSpec,
) -> dict[str, int]:
    """Count scheduled, fully-contained periods from the explicit signal anchor."""

    feature_dates = pd.to_datetime(
        feature_frame[portfolio_config.date_column], errors="coerce"
    ).dropna()
    execution_date_column = _resolve_column(
        set(execution_frame.columns),
        portfolio_config.date_column,
        ("date", "trade_date"),
    )
    if feature_dates.empty or execution_date_column is None:
        return {"train": 0, "validation": 0, "audit": 0}
    execution_dates = [
        pd.Timestamp(value)
        for value in sorted(
            pd.to_datetime(execution_frame[execution_date_column], errors="coerce")
            .dropna()
            .unique()
        )
    ]
    signal_start = feature_dates.min()
    first_index = next(
        (index for index, value in enumerate(execution_dates) if value >= signal_start),
        len(execution_dates),
    )
    scheduled = [
        (
            execution_dates[index],
            execution_dates[index + portfolio_config.holding_days + 1],
        )
        for index in range(
            first_index + portfolio_config.rebalance_offset_days,
            len(execution_dates) - portfolio_config.holding_days - 1,
            portfolio_config.rebalance_every_days,
        )
    ]
    boundaries = {
        "train": (pd.Timestamp(validation.train_start), pd.Timestamp(validation.train_end)),
        "validation": (
            pd.Timestamp(validation.validation_start),
            pd.Timestamp(validation.validation_end),
        ),
        "audit": (pd.Timestamp(validation.audit_start), None),
    }
    return {
        split: sum(
            signal_date >= lower
            and (upper is None or signal_date <= upper)
            and (upper is None or end_date <= upper)
            for signal_date, end_date in scheduled
        )
        for split, (lower, upper) in boundaries.items()
    }


def _portfolio_result(
    factor: FactorSpec,
    validation: FactorValidation,
    signal: pd.Series,
    feature_frame: pd.DataFrame,
    execution_frame: pd.DataFrame,
    portfolio_config: LongOnlyPortfolioConfig,
    research_config: Mapping[str, Any],
) -> dict[str, Any]:
    directed = pd.to_numeric(signal, errors="coerce") * validation.frozen_direction
    evaluation = evaluate_long_only_portfolio(
        feature_frame,
        directed,
        portfolio_config,
        pricing_frame=execution_frame,
        promotion_blockers=("historical_diagnostic_only",),
    ).to_dict()
    periods = list(evaluation.get("periods") or [])
    spec = _validation_spec(research_config)
    expected = _expected_portfolio_observations(
        feature_frame,
        execution_frame,
        portfolio_config,
        spec,
    )
    windows = {
        "train": _window_metrics(
            periods,
            start=spec.train_start,
            end=spec.train_end,
            periods_per_year=portfolio_config.periods_per_year,
            bootstrap_spec=spec,
            bootstrap_key="portfolio:train:active_return",
            expected_observations=expected["train"],
        ),
        "validation": _window_metrics(
            periods,
            start=spec.validation_start,
            end=spec.validation_end,
            periods_per_year=portfolio_config.periods_per_year,
            bootstrap_spec=spec,
            bootstrap_key="portfolio:validation:active_return",
            expected_observations=expected["validation"],
        ),
        "audit": _window_metrics(
            periods,
            start=spec.audit_start,
            end=None,
            periods_per_year=portfolio_config.periods_per_year,
            bootstrap_spec=spec,
            bootstrap_key="portfolio:audit:active_return",
            expected_observations=expected["audit"],
        ),
    }

    for split, diagnostics in (
        ("train", validation.train),
        ("validation", validation.validation),
        ("audit", validation.audit),
    ):
        windows[split]["signal_evaluable_date_ratio"] = diagnostics.evaluable_date_ratio
        windows[split]["signal_median_cross_section_coverage"] = (
            diagnostics.median_cross_section_coverage
        )
        base_minimum = int(
            (research_config.get("promotion_gate") or {}).get(
                "validation_observations_min", 0
            )
        )
        windows[split]["minimum_required_observations"] = int(
            math.ceil(
                base_minimum
                * spec.holding_days
                / portfolio_config.rebalance_every_days
            )
        )
    gate_passed, gate_blockers = _gate(windows["validation"], research_config)
    audit_falsified, audit_reasons = _audit_falsification(
        validation, windows["audit"], spec
    )
    if "audit_insufficient_observations" in audit_reasons:
        audit_status = "insufficient_evidence"
    elif audit_falsified:
        audit_status = "falsified"
    else:
        audit_status = "not_falsified"
    return {
        "factor_name": factor.name,
        "family": factor.family,
        "factor": factor.to_dict(),
        "frozen_direction": validation.frozen_direction,
        "stage_a": validation.to_dict(),
        "portfolio": _compact_portfolio(evaluation),
        "windows": windows,
        "gate_passed": gate_passed,
        "gate_blockers": gate_blockers,
        "audit_role": "falsification_only",
        "audit_status": audit_status,
        "audit_falsified": audit_falsified,
        "audit_falsification_reasons": audit_reasons,
        "period_active_returns": [
            {
                "signal_date": row.get("signal_date"),
                "end_date": row.get("end_date"),
                "net_return": float(row.get("net_return") or 0.0),
                "benchmark_return": float(row.get("benchmark_return") or 0.0),
                "active_return": float(row.get("net_return") or 0.0)
                - float(row.get("benchmark_return") or 0.0),
            }
            for row in periods
        ],
        "beats_control": False,
        "control_comparison": None,
        "validated": False,
    }


def _results_first_metrics(
    result: Mapping[str, Any],
    research_config: Mapping[str, Any],
    *,
    periods_per_year: float,
    reference_periods: Sequence[Mapping[str, Any]] | None = None,
    optimization_scope: str = "all_observed_history",
) -> dict[str, Any]:
    """Score one strategy over every observed historical period.

    This is deliberately an in-sample leaderboard. It optimizes the result the
    user asked for and never labels the winner as independently validated.
    """

    settings = dict(research_config.get("results_first") or {})
    incomplete_policy = str(
        settings.get("incomplete_period_policy", "exclude_from_ranking")
    )
    if incomplete_policy != "exclude_from_ranking":
        raise ValueError(
            "results_first incomplete_period_policy must be 'exclude_from_ranking'"
        )

    observed_rows = list(result.get("period_active_returns") or [])
    missing_periods = 0
    if reference_periods is None:
        rows = observed_rows
        observed_periods = len(rows)
        comparison_basis = "strategy_observed_periods"
    else:
        observed_by_date = {
            str(row.get("signal_date")): row
            for row in observed_rows
            if row.get("signal_date") is not None
        }
        rows = []
        observed_periods = 0
        for reference in reference_periods:
            signal_date = str(reference.get("signal_date"))
            candidate = observed_by_date.get(signal_date)
            if candidate is None:
                missing_periods += 1
                net_return = 0.0
            else:
                observed_periods += 1
                net_return = float(candidate.get("net_return") or 0.0)
            benchmark_return = float(reference.get("benchmark_return") or 0.0)
            rows.append(
                {
                    "signal_date": reference.get("signal_date"),
                    "net_return": net_return,
                    "benchmark_return": benchmark_return,
                    "active_return": net_return - benchmark_return,
                }
            )
        comparison_basis = "control_signal_dates"
    net = np.asarray([float(row.get("net_return") or 0.0) for row in rows], dtype=float)
    active = np.asarray(
        [float(row.get("active_return") or 0.0) for row in rows], dtype=float
    )
    finite = np.isfinite(net) & np.isfinite(active)
    net = net[finite]
    active = active[finite]
    if not len(net):
        return {
            "observations": 0,
            "historical_score": None,
            "net_annual_return": None,
            "net_sharpe": None,
            "information_ratio": None,
            "max_drawdown": None,
            "optimization_scope": optimization_scope,
            "comparison_period_basis": comparison_basis,
            "observed_strategy_periods": observed_periods,
            "missing_strategy_periods": missing_periods,
            "period_coverage": 0.0,
            "missing_period_score_policy": "cash_return_zero_diagnostic_only",
            "incomplete_period_ranking_policy": incomplete_policy,
        }

    growth = float(np.prod(1.0 + net))
    annual_return = (
        growth ** (float(periods_per_year) / len(net)) - 1.0
        if growth > 0.0
        else -1.0
    )
    net_std = float(np.std(net, ddof=1)) if len(net) > 1 else 0.0
    active_std = float(np.std(active, ddof=1)) if len(active) > 1 else 0.0
    scale = math.sqrt(float(periods_per_year))
    sharpe = float(np.mean(net) / net_std * scale) if net_std > 0.0 else 0.0
    information_ratio = (
        float(np.mean(active) / active_std * scale) if active_std > 0.0 else 0.0
    )
    nav = np.concatenate(([1.0], np.cumprod(1.0 + net)))
    peaks = np.maximum.accumulate(nav)
    drawdowns = nav / peaks - 1.0
    max_drawdown = float(np.min(drawdowns))

    default_score_weights = {
        "net_sharpe": 1.0,
        "information_ratio": 0.35,
        "net_annual_return": 0.50,
        "max_drawdown": 0.35,
    }
    configured_weights = dict(settings.get("score_weights") or {})
    unknown_weights = sorted(set(configured_weights) - set(default_score_weights))
    if unknown_weights:
        raise ValueError(
            "unsupported results_first score_weights: " + ", ".join(unknown_weights)
        )
    score_weights: dict[str, float] = {}
    for key, raw_value in {**default_score_weights, **configured_weights}.items():
        if isinstance(raw_value, (bool, np.bool_)):
            raise ValueError("results_first score_weights must be finite non-negative numbers")
        try:
            parsed_weight = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "results_first score_weights must be finite non-negative numbers"
            ) from exc
        if not np.isfinite(parsed_weight) or parsed_weight < 0.0:
            raise ValueError("results_first score_weights must be finite non-negative numbers")
        score_weights[key] = parsed_weight
    if not any(score_weights.values()):
        raise ValueError("at least one results_first score weight must be positive")
    return {
        "observations": int(len(net)),
        "optimization_scope": optimization_scope,
        "comparison_period_basis": comparison_basis,
        "observed_strategy_periods": observed_periods,
        "missing_strategy_periods": missing_periods,
        "period_coverage": round(observed_periods / len(rows), 8),
        "missing_period_score_policy": "cash_return_zero_diagnostic_only",
        "incomplete_period_ranking_policy": incomplete_policy,
        "historical_score": None,
        "score_method": "cross_strategy_percentile_weighted",
        "net_annual_return": round(annual_return, 8),
        "net_sharpe": round(sharpe, 8),
        "information_ratio": round(information_ratio, 8),
        "max_drawdown": round(max_drawdown, 8),
        "active_return_annual_mean": round(float(np.mean(active)) * periods_per_year, 8),
        "score_weights": score_weights,
    }


def _build_results_first_ensembles(
    frame: pd.DataFrame,
    control: FactorSpec,
    challengers: Sequence[FactorSpec],
    signals: Mapping[str, pd.Series],
    validations: Mapping[str, FactorValidation],
    validation_spec: ValidationSpec,
    research_config: Mapping[str, Any],
) -> tuple[list[FactorSpec], dict[str, pd.Series], dict[str, FactorValidation]]:
    settings = dict(research_config.get("results_first") or {})
    if str(settings.get("optimization_scope", "all_observed_history")) != (
        "all_observed_history"
    ):
        raise ValueError(
            "results_first optimization_scope must be 'all_observed_history'"
        )
    if str(settings.get("missing_challenger_policy", "fallback_control")) != (
        "fallback_control"
    ):
        raise ValueError(
            "results_first missing_challenger_policy must be 'fallback_control'"
        )
    raw_weights = tuple(settings.get("challenger_weights", (0.2, 0.4, 0.6)))
    if not raw_weights or any(isinstance(value, (bool, np.bool_)) for value in raw_weights):
        raise ValueError("results_first challenger_weights must be in (0, 1]")
    try:
        weights = tuple(float(value) for value in raw_weights)
    except (TypeError, ValueError) as exc:
        raise ValueError("results_first challenger_weights must be in (0, 1]") from exc
    if any(not np.isfinite(value) or value <= 0.0 or value > 1.0 for value in weights):
        raise ValueError("results_first challenger_weights must be in (0, 1]")
    if len(set(weights)) != len(weights):
        raise ValueError("results_first challenger_weights must be unique")
    weight_labels = tuple(
        format(value, ".12g").replace(".", "p") for value in weights
    )
    if len(set(weight_labels)) != len(weight_labels):
        raise ValueError("results_first challenger_weights must have unique labels")

    ensemble_factors: list[FactorSpec] = []
    ensemble_signals: dict[str, pd.Series] = {}
    ensemble_validations: dict[str, FactorValidation] = {}
    control_validation = validations[control.name]
    # Daily ranks are invariant across the weight grid.  Compute the control
    # once and each challenger's fallback-adjusted rank once instead of doing
    # two full groupby/rank passes for every candidate weight.
    control_rank = directed_rank_blend(
        frame,
        signals[control.name],
        signals[control.name],
        control_direction=control_validation.frozen_direction,
        challenger_direction=control_validation.frozen_direction,
        challenger_weight=0.0,
        date_column=validation_spec.date_column,
    )
    for challenger in challengers:
        challenger_validation = validations[challenger.name]
        effective_challenger_rank = directed_rank_blend(
            frame,
            signals[control.name],
            signals[challenger.name],
            control_direction=control_validation.frozen_direction,
            challenger_direction=challenger_validation.frozen_direction,
            challenger_weight=1.0,
            date_column=validation_spec.date_column,
        )
        for weight, weight_label in zip(weights, weight_labels, strict=True):
            name = f"blend__{control.name}__{challenger.name}__w{weight_label}"
            factor = FactorSpec(
                name=name,
                family="results_first_ensemble",
                kind="ensemble",
                direction_policy="pre_directed",
                params={
                    "control_factor": control.name,
                    "challenger_factor": challenger.name,
                    "control_direction": control_validation.frozen_direction,
                    "challenger_direction": challenger_validation.frozen_direction,
                    "challenger_weight": weight,
                    "missing_challenger_policy": "fallback_control",
                    "optimization_scope": "all_observed_history",
                },
                role="results_first_candidate",
            )
            signal = (
                (1.0 - weight) * control_rank + weight * effective_challenger_rank
            ).where(control_rank.notna()).rename(name)
            validation = evaluate_stage_a(
                frame,
                factor,
                validation_spec,
                signal=signal,
            )
            ensemble_factors.append(factor)
            ensemble_signals[name] = signal
            ensemble_validations[name] = validation
    return ensemble_factors, ensemble_signals, ensemble_validations


def _control_comparison(
    candidate: Mapping[str, Any],
    control: Mapping[str, Any],
    config: Mapping[str, Any],
    validation: ValidationSpec,
    *,
    correction_factor: int,
) -> dict[str, Any]:
    lower = pd.Timestamp(validation.validation_start)
    upper = pd.Timestamp(validation.validation_end)

    def window(payload: Mapping[str, Any]) -> dict[str, float]:
        return {
            str(row["signal_date"]): float(row.get("net_return") or 0.0)
            for row in payload.get("period_active_returns") or []
            if lower <= pd.Timestamp(str(row["signal_date"])) <= upper
            and pd.Timestamp(str(row["end_date"])) <= upper
        }

    candidate_returns = window(candidate)
    control_returns = window(control)
    common_dates = sorted(set(candidate_returns) & set(control_returns))
    differences = [
        candidate_returns[date_value] - control_returns[date_value]
        for date_value in common_dates
    ]
    familywise_alpha = 1.0 - validation.bootstrap_confidence
    adjusted_confidence = 1.0 - familywise_alpha / max(1, int(correction_factor))
    interval = deterministic_block_bootstrap_mean(
        differences,
        samples=validation.bootstrap_samples,
        block_size=validation.bootstrap_block_size,
        confidence=adjusted_confidence,
        seed=validation.bootstrap_seed,
        key="portfolio:validation:paired_control_improvement",
    ).to_dict()
    minimum_observations = int(
        (config.get("promotion_gate") or {}).get("validation_observations_min", 0)
    )
    blockers: list[str] = []
    if len(common_dates) < minimum_observations:
        blockers.append("control_comparison_observations_below_threshold")
    if interval.get("lower") is None or float(interval["lower"]) <= 0.0:
        blockers.append("control_improvement_bootstrap_lower_not_positive")
    return {
        "control_factor": control.get("factor_name"),
        "split": "validation",
        "common_observations": len(common_dates),
        "mean_net_return_difference": round(float(np.mean(differences)), 8)
        if differences
        else None,
        "bootstrap": interval,
        "simultaneous_confidence_method": "bonferroni_fwer",
        "correction_factor": max(1, int(correction_factor)),
        "passed": not blockers,
        "blockers": blockers,
    }


def _beats_control(
    candidate: Mapping[str, Any],
    control: Mapping[str, Any],
    config: Mapping[str, Any],
    comparison: Mapping[str, Any] | None,
) -> bool:
    left = (candidate.get("windows") or {}).get("validation") or {}
    right = (control.get("windows") or {}).get("validation") or {}
    tolerance = float((config.get("control_comparison") or {}).get("max_drawdown_worsening_tolerance", 0.02))
    return bool(
        candidate.get("gate_passed")
        and float(left.get("net_sharpe") or 0.0) > float(right.get("net_sharpe") or 0.0)
        and float(left.get("net_excess_annual_return") or 0.0) > float(right.get("net_excess_annual_return") or 0.0)
        and float(left.get("max_drawdown") or 0.0) >= float(right.get("max_drawdown") or 0.0) - tolerance
        and bool((comparison or {}).get("passed", False))
    )


def _canary_frames(
    features: pd.DataFrame,
    execution: pd.DataFrame,
    config: LongOnlyPortfolioConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.DatetimeIndex(sorted(features[config.date_column].dropna().unique()))[-20:]
    recent = features[features[config.date_column].isin(dates)]
    tickers = recent.groupby(config.ticker_column).size().sort_values(ascending=False).head(50).index.astype(str)
    feature_sample = recent[recent[config.ticker_column].astype(str).isin(tickers)].copy()
    execution_date_column = _resolve_column(set(execution.columns), config.date_column, ("date", "trade_date"))
    execution_ticker_column = _resolve_column(set(execution.columns), config.ticker_column, ("ticker", "ts_code", "symbol"))
    assert execution_date_column and execution_ticker_column
    all_execution_dates = pd.DatetimeIndex(sorted(execution[execution_date_column].dropna().unique()))
    future = all_execution_dates[all_execution_dates > dates[-1]][: config.holding_days + 1]
    allowed_dates = set(dates.tolist()) | set(future.tolist())
    execution_sample = execution[
        execution[execution_date_column].isin(allowed_dates)
        & execution[execution_ticker_column].astype(str).isin(tickers)
    ].copy()
    return feature_sample, execution_sample


def _anchor_window_aggregate(
    anchors: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return per-split min/median/max and a median synthetic window."""

    aggregate: dict[str, Any] = {}
    medians: dict[str, Any] = {}
    for split in ("train", "validation", "audit"):
        windows = [
            (row.get("windows") or {}).get(split) or {}
            for row in anchors
        ]
        numeric_keys = sorted(
            {
                key
                for window in windows
                for key, value in window.items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            }
        )
        split_aggregate: dict[str, Any] = {}
        split_median: dict[str, Any] = {}
        for key in numeric_keys:
            values = np.asarray(
                [float(window[key]) for window in windows if key in window],
                dtype=float,
            )
            values = values[np.isfinite(values)]
            if not len(values):
                continue
            median = float(np.median(values))
            split_aggregate[key] = {
                "min": round(float(np.min(values)), 8),
                "median": round(median, 8),
                "max": round(float(np.max(values)), 8),
            }
            split_median[key] = round(median, 8)
        aggregate[split] = split_aggregate
        medians[split] = split_median
    return aggregate, medians


def _robustness_integrity_blockers(
    anchors: Sequence[Mapping[str, Any]],
) -> list[str]:
    blockers = {
        str(blocker)
        for anchor in anchors
        for blocker in anchor.get("gate_blockers") or []
        if str(blocker) in _ROBUSTNESS_ABSOLUTE_BLOCKERS
        or str(blocker).startswith("missing_promotion_gate_config:")
    }
    return sorted(blockers)


def _run_robustness(
    factors: Sequence[FactorSpec],
    validations: Mapping[str, FactorValidation],
    signals: Mapping[str, pd.Series],
    features: pd.DataFrame,
    execution: pd.DataFrame,
    base_config: LongOnlyPortfolioConfig,
    research_config: Mapping[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    matrix = research_config.get("robustness") or {}
    offsets_by_days = matrix.get("anchor_offsets_by_rebalance_days") or {}
    minimum_pass_ratio = float(matrix.get("minimum_anchor_pass_ratio", 0.75))
    if not 0.0 <= minimum_pass_ratio <= 1.0:
        raise ValueError("robustness minimum_anchor_pass_ratio must be between 0 and 1")
    for factor in factors:
        for positions in matrix.get("position_counts") or (50, 75, 100):
            for rebalance in matrix.get("rebalance_every_days") or (5, 20):
                offsets = offsets_by_days.get(str(rebalance), [0])
                if not isinstance(offsets, list) or not offsets:
                    raise ValueError(f"robustness offsets missing for {rebalance} days")
                normalized_offsets = [int(value) for value in offsets]
                if len(normalized_offsets) != len(set(normalized_offsets)):
                    raise ValueError(f"duplicate robustness offsets for {rebalance} days")
                if any(value < 0 or value >= int(rebalance) for value in normalized_offsets):
                    raise ValueError(f"invalid robustness offset for {rebalance} days")
                anchors: list[dict[str, Any]] = []
                for offset in normalized_offsets:
                    config = replace(
                        base_config,
                        position_count=int(positions),
                        holding_days=int(rebalance),
                        rebalance_every_days=int(rebalance),
                        rebalance_offset_days=offset,
                        periods_per_year=252.0 / int(rebalance),
                    )
                    result = _portfolio_result(
                        factor,
                        validations[factor.name],
                        signals[factor.name],
                        features,
                        execution,
                        config,
                        research_config,
                    )
                    anchors.append(
                        {
                            "rebalance_offset_days": offset,
                            "gate_passed": result["gate_passed"],
                            "gate_blockers": result["gate_blockers"],
                            "windows": result["windows"],
                            "portfolio": result["portfolio"],
                        }
                    )
                statistics, median_windows = _anchor_window_aggregate(anchors)
                median_gate_passed, median_gate_blockers = _gate(
                    median_windows.get("validation") or {}, research_config
                )
                pass_ratio = float(
                    np.mean([bool(row["gate_passed"]) for row in anchors])
                )
                integrity_blockers = _robustness_integrity_blockers(anchors)
                robust = bool(
                    pass_ratio >= minimum_pass_ratio and median_gate_passed
                    and not integrity_blockers
                )
                blockers = [*median_gate_blockers, *integrity_blockers]
                if pass_ratio < minimum_pass_ratio:
                    blockers.append("anchor_pass_ratio_below_threshold")
                rows.append(
                    {
                        "factor_name": factor.name,
                        "position_count": int(positions),
                        "rebalance_every_days": int(rebalance),
                        "anchor_offsets": normalized_offsets,
                        "anchor_count": len(anchors),
                        "anchor_pass_ratio": round(pass_ratio, 8),
                        "minimum_anchor_pass_ratio": minimum_pass_ratio,
                        "median_gate_passed": median_gate_passed,
                        "robust": robust,
                        "robustness_blockers": list(dict.fromkeys(blockers)),
                        "window_statistics": statistics,
                        "median_windows": median_windows,
                        "anchors": anchors,
                        "exploratory_only": True,
                        "promotion_eligible": False,
                    }
                )
                _write_json(output_path, {"status": "running", "results": rows})
    payload = {
        "status": "completed",
        "search_stopped": True,
        "selection_basis": "train_shortlist_order",
        "audit_used_for_selection": False,
        "exploratory_only": True,
        "results": rows,
    }
    _write_json(output_path, payload)
    return payload


def run_research(
    *,
    project_root: str | Path | None = None,
    suite: str = RESULTS_FIRST_SUITE,
    mode: str = "canary",
    resume: bool = True,
    feature_path: str | Path | None = None,
    execution_path: str | Path | None = None,
    factors_path: str | Path | None = None,
    research_config_path: str | Path | None = None,
    run_robustness: bool = True,
) -> dict[str, Any]:
    """Run a deterministic historical research suite."""

    if mode not in {"canary", "full"}:
        raise ValueError("mode must be canary or full")
    root = _project_root(project_root)
    default_features, default_execution = _default_data_paths(root)
    feature_file = Path(feature_path or default_features).resolve()
    execution_file = Path(execution_path or default_execution).resolve()
    factors_file = Path(factors_path or root / "configs" / "factors.json").resolve()
    research_file = Path(research_config_path or root / "configs" / "research.json").resolve()
    for path in (feature_file, execution_file, factors_file, research_file):
        if not path.is_file():
            raise FileNotFoundError(path)

    research_config = _read_json(research_file)
    validation_spec = _validation_spec(research_config)
    control, challengers = load_factor_suite(factors_file, suite)
    if suite == RESULTS_FIRST_SUITE:
        control = replace(control, direction_policy="all_history_ic")
        challengers = [
            replace(factor, direction_policy="all_history_ic")
            for factor in challengers
        ]
    all_factors = [control, *challengers]
    feature_hash = _sha256_file(feature_file)
    execution_hash = _sha256_file(execution_file)
    implementation_hash = _implementation_sha256()
    fingerprint = _sha256_value(
        {
            "engine": ENGINE_ID,
            "mode": mode,
            "suite": suite,
            "implementation": implementation_hash,
            "features": feature_hash,
            "execution": execution_hash,
            "factors": [row.to_dict() for row in all_factors],
            "research": research_config,
            "run_robustness": bool(run_robustness),
        }
    )
    run_id = fingerprint[:16]
    output_dir = root / "runtime" / "runs" / run_id
    summary_path = output_dir / "summary.json"
    existing_summary = summary_path.is_file()
    if resume and _completed_run_valid(summary_path, output_dir, fingerprint):
        cached = _read_json(summary_path)
        _write_json(
            root / "runtime" / "runs" / "latest.json",
            {"run_id": run_id, "output_dir": str(output_dir), "summary_path": str(summary_path)},
        )
        return cached
    output_dir.mkdir(parents=True, exist_ok=True)
    factor_dir = output_dir / "factors"
    factor_dir.mkdir(parents=True, exist_ok=True)

    features = _load_features(feature_file, all_factors, validation_spec)
    signals = {
        factor.name: evaluate_factor_signal(features, factor, date_column=validation_spec.date_column)
        for factor in all_factors
    }
    stage_a_rows = [
        evaluate_stage_a(features, factor, validation_spec, signal=signals[factor.name])
        for factor in all_factors
    ]
    validations = {row.factor_name: row for row in stage_a_rows}
    similarities = diagnose_train_similarity(
        features, signals, stage_a_rows, validation_spec
    )
    stage_a_selection: StageASelection | None = None
    if suite in {"legacy-regression", RESULTS_FIRST_SUITE}:
        selected = [factor for factor in all_factors if factor.role != "diagnostic_only"]
    else:
        stage_a_selection = build_stage_a_selection(
            stage_a_rows,
            similarities,
            validation_spec,
            excluded_names={control.name},
        )
        factor_by_name = {factor.name: factor for factor in all_factors}
        selected = [
            control,
            *[
                factor_by_name[row.factor_name]
                for row in stage_a_selection.selected
            ],
        ]

    if suite == RESULTS_FIRST_SUITE:
        ensemble_factors, ensemble_signals, ensemble_validations = (
            _build_results_first_ensembles(
                features,
                control,
                [factor for factor in challengers if factor.role != "diagnostic_only"],
                signals,
                validations,
                validation_spec,
                research_config,
            )
        )
        signals.update(ensemble_signals)
        validations.update(ensemble_validations)
        stage_a_rows.extend(ensemble_validations[factor.name] for factor in ensemble_factors)
        # Standalone challengers provide component diagnostics and an optimized
        # direction, but only fallback-control ensembles are exposure-comparable
        # enough to enter the expensive portfolio leaderboard.
        selected = [control, *ensemble_factors]

    portfolio_config = _portfolio_config(research_config)
    execution = _load_execution(execution_file, portfolio_config)
    execution_date_column = _resolve_column(
        set(execution.columns), portfolio_config.date_column, ("date", "trade_date")
    )
    assert execution_date_column is not None
    last_decision_date = features[validation_spec.date_column].max()
    execution_tail = execution[execution[execution_date_column] > last_decision_date]
    evaluation_features = features
    evaluation_execution = execution
    if mode == "canary":
        evaluation_features, evaluation_execution = _canary_frames(features, execution, portfolio_config)

    stage_b: list[dict[str, Any]] = []
    for factor in selected:
        result_path = factor_dir / f"{_safe_name(factor.name)}.json"
        cached_result: dict[str, Any] | None = None
        if resume and not existing_summary and result_path.is_file():
            payload = _read_json(result_path)
            if (
                payload.get("run_fingerprint") == fingerprint
                and (payload.get("result") or {}).get("factor_name") == factor.name
            ):
                cached_result = payload.get("result")
        if cached_result is None:
            signal = signals[factor.name]
            if mode == "canary":
                signal = signal.loc[evaluation_features.index]
            cached_result = _portfolio_result(
                factor,
                validations[factor.name],
                signal,
                evaluation_features,
                evaluation_execution,
                portfolio_config,
                research_config,
            )
            _write_json(result_path, {"run_fingerprint": fingerprint, "result": cached_result})
        stage_b.append(cached_result)

    control_result = next(row for row in stage_b if row["factor_name"] == control.name)
    validated: list[str] = []
    pre_audit_confirmed: list[str] = []
    results_first_rankings: list[dict[str, Any]] = []
    results_first_excluded: list[dict[str, Any]] = []
    best_historical_strategy: str | None = None
    if suite == RESULTS_FIRST_SUITE:
        comparison_periods = list(control_result.get("period_active_returns") or [])
        for row in stage_b:
            metrics = _results_first_metrics(
                row,
                research_config,
                periods_per_year=portfolio_config.periods_per_year,
                reference_periods=comparison_periods,
                optimization_scope=(
                    "all_observed_history"
                    if mode == "full"
                    else "canary_recent_window_smoke_only"
                ),
            )
            row["results_first_metrics"] = metrics
            row["strategy_kind"] = (
                "control"
                if row["factor_name"] == control.name
                else "ensemble"
                if ((row.get("factor") or {}).get("kind") == "ensemble")
                else "standalone_diagnostic"
            )
            row["results_first_ranking_eligible"] = bool(
                row["strategy_kind"] in {"control", "ensemble"}
                and metrics.get("observations")
                and float(metrics.get("period_coverage") or 0.0) >= 1.0
            )
            row["results_first_ranking_exclusion_reason"] = (
                None
                if row["results_first_ranking_eligible"]
                else "standalone_diagnostic_only"
                if row["strategy_kind"] == "standalone_diagnostic"
                else "incomplete_control_period_coverage"
            )
            row["pre_audit_confirmed"] = False
            row["validated"] = False

        if mode == "full":
            def ranking_value(row: Mapping[str, Any], key: str) -> float:
                value = (row.get("results_first_metrics") or {}).get(key)
                if value is None:
                    return -np.inf
                parsed = float(value)
                return parsed if np.isfinite(parsed) else -np.inf

            score_rows = [
                row for row in stage_b if row["results_first_ranking_eligible"]
            ]
            if score_rows:
                score_weights = dict(
                    (score_rows[0].get("results_first_metrics") or {}).get(
                        "score_weights"
                    )
                    or {}
                )
                weight_total = float(sum(score_weights.values()))
                for row in score_rows:
                    (row.get("results_first_metrics") or {})[
                        "score_percentiles"
                    ] = {}
                for metric_name, weight in score_weights.items():
                    metric_ranks = pd.Series(
                        [ranking_value(row, metric_name) for row in score_rows],
                        dtype=float,
                    ).rank(method="average", pct=True)
                    for row, percentile in zip(
                        score_rows, metric_ranks.tolist(), strict=True
                    ):
                        (row.get("results_first_metrics") or {})[
                            "score_percentiles"
                        ][metric_name] = round(float(percentile), 8)
                for row in score_rows:
                    metrics = row.get("results_first_metrics") or {}
                    percentiles = metrics["score_percentiles"]
                    metrics["historical_score"] = round(
                        sum(
                            float(score_weights[key]) * float(percentiles[key])
                            for key in score_weights
                        )
                        / weight_total,
                        8,
                    )

            ranked_rows = sorted(
                score_rows,
                key=lambda row: (
                    -ranking_value(row, "historical_score"),
                    -ranking_value(row, "net_annual_return"),
                    str(row.get("factor_name") or ""),
                ),
            )
            control_score = ranking_value(control_result, "historical_score")
            if not np.isfinite(control_score):
                control_score = 0.0
            for rank, row in enumerate(ranked_rows, start=1):
                metrics = dict(row.get("results_first_metrics") or {})
                score = metrics.get("historical_score")
                row["historical_rank"] = rank
                row["historical_score_delta_vs_control"] = (
                    round(float(score) - control_score, 8) if score is not None else None
                )
                results_first_rankings.append(
                    {
                        "rank": rank,
                        "factor_name": row.get("factor_name"),
                        "strategy_kind": row.get("strategy_kind"),
                        "historical_score_delta_vs_control": row.get(
                            "historical_score_delta_vs_control"
                        ),
                        **metrics,
                    }
                )
            if results_first_rankings:
                best_historical_strategy = str(results_first_rankings[0]["factor_name"])
            results_first_excluded = [
                {
                    "factor_name": row.get("factor_name"),
                    "strategy_kind": row.get("strategy_kind"),
                    "reason": row.get("results_first_ranking_exclusion_reason"),
                    "period_coverage": (row.get("results_first_metrics") or {}).get(
                        "period_coverage"
                    ),
                    "observed_strategy_periods": (
                        row.get("results_first_metrics") or {}
                    ).get("observed_strategy_periods"),
                    "observations": (row.get("results_first_metrics") or {}).get(
                        "observations"
                    ),
                }
                for row in stage_b
                if not row["results_first_ranking_eligible"]
            ]
    else:
        challenger_count = max(
            1, sum(row["factor_name"] != control.name for row in stage_b)
        )
        for row in stage_b:
            if row["factor_name"] == control.name:
                continue
            comparison = _control_comparison(
                row,
                control_result,
                research_config,
                validation_spec,
                correction_factor=challenger_count,
            )
            row["control_comparison"] = comparison
            row["beats_control"] = _beats_control(
                row, control_result, research_config, comparison
            )
            row["pre_audit_confirmed"] = bool(
                suite != "legacy-regression"
                and row["gate_passed"]
                and row["beats_control"]
            )
            if row["pre_audit_confirmed"]:
                pre_audit_confirmed.append(str(row["factor_name"]))
            row["validated"] = bool(
                mode == "full"
                and row["pre_audit_confirmed"]
                and not row.get("audit_falsified", False)
            )
            if row["validated"]:
                validated.append(str(row["factor_name"]))
    # Per-factor artifacts are authoritative evidence too.  Persist comparison
    # flags only after the control result is known so they cannot contradict
    # the final summary.
    for row in stage_b:
        result_path = factor_dir / f"{_safe_name(str(row['factor_name']))}.json"
        _write_json(result_path, {"run_fingerprint": fingerprint, "result": row})

    robustness: dict[str, Any] | None = None
    if (
        suite != RESULTS_FIRST_SUITE
        and mode == "full"
        and suite in {"next", "recovery"}
        and not pre_audit_confirmed
        and run_robustness
    ):
        # The finite matrix covers every train-admitted challenger.  It must not
        # pick a "best" subject from validation or audit results, because doing
        # so would turn the diagnostic matrix into an unregistered search.
        challenger_by_name = {factor.name: factor for factor in challengers}
        robustness_factors = [
            control,
            *[
                challenger_by_name[str(row["factor_name"])]
                for row in stage_b
                if row["factor_name"] != control.name
            ],
        ]
        robustness = _run_robustness(
            robustness_factors,
            validations,
            signals,
            features,
            execution,
            portfolio_config,
            research_config,
            output_dir / "robustness.json",
        )

    if suite == RESULTS_FIRST_SUITE and mode == "full":
        search_status = "results_first_historical_ranking_completed"
    elif suite == RESULTS_FIRST_SUITE:
        search_status = "results_first_canary_smoke"
    elif mode == "canary":
        search_status = "canary_smoke"
    elif suite == "legacy-regression":
        search_status = "legacy_regression_completed"
    elif validated:
        search_status = "confirmed_candidate_found"
    elif pre_audit_confirmed:
        search_status = "audit_falsified_stop"
    elif robustness is not None:
        search_status = "robustness_completed_exhausted"
    elif not run_robustness:
        search_status = "robustness_skipped"
    else:
        search_status = "completed_no_candidate"

    data_warning = research_config.get("data_warning")
    if not data_warning and ("st_filter_status" not in features.columns or any(
        "unverified" in str(value).casefold() or "degraded" in str(value).casefold()
        for value in features.get("st_filter_status", pd.Series(dtype=str)).dropna().unique()
    )):
        data_warning = "st_history_unverified"
    summary: dict[str, Any] = {
        "schema_version": 2,
        "engine": ENGINE_ID,
        "status": "completed",
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "suite": suite,
        "mode": mode,
        "evidence_class": EVIDENCE_CLASS,
        "investment_claim_allowed": False,
        "promotion_triggered": False,
        "canary_smoke_only": mode == "canary",
        "gate_results_interpretable": mode == "full" and suite != RESULTS_FIRST_SUITE,
        "ranking_results_interpretable": (
            mode == "full" and suite == RESULTS_FIRST_SUITE
        ),
        "git": _git_state(root),
        "implementation_sha256": implementation_hash,
        "data": {
            "feature_path": str(feature_file),
            "feature_sha256": feature_hash,
            "execution_path": str(execution_file),
            "execution_sha256": execution_hash,
            "start_date": features[validation_spec.date_column].min().date().isoformat(),
            "end_date": features[validation_spec.date_column].max().date().isoformat(),
            "row_count": int(len(features)),
            "ticker_count": int(features["ticker"].nunique()),
            "warning": data_warning,
            "execution_start_date": execution[execution_date_column].min().date().isoformat(),
            "execution_end_date": execution[execution_date_column].max().date().isoformat(),
            "execution_tail_policy": "pricing_only_after_last_decision_date",
            "execution_tail_row_count": int(len(execution_tail)),
            "execution_tail_date_count": int(execution_tail[execution_date_column].nunique()),
            "research_universe_filter": features.attrs.get(
                "research_universe_filter", {}
            ),
        },
        "control_factor": control.name,
        "stage_a": [row.to_dict() for row in stage_a_rows],
        "stage_a_selection": stage_a_selection.to_dict()
        if stage_a_selection is not None
        else {
            "basis": (
                "all_registered_plus_runtime_ensembles"
                if suite == RESULTS_FIRST_SUITE
                else "legacy_regression_all_registered"
            ),
            "selected": [row.name for row in selected if row.name != control.name],
            "decisions": [],
            "similarities": [row.to_dict() for row in similarities],
        },
        "stage_b_selected": [row.name for row in selected],
        "stage_b": stage_b,
        "validated_factors": validated,
        "validated_count": len(validated),
        "pre_audit_confirmed_factors": pre_audit_confirmed,
        "results_first": {
            "enabled": suite == RESULTS_FIRST_SUITE,
            "ranking_available": suite == RESULTS_FIRST_SUITE and mode == "full",
            "optimization_scope": (
                "all_observed_history"
                if suite == RESULTS_FIRST_SUITE and mode == "full"
                else "canary_recent_window_smoke_only"
                if suite == RESULTS_FIRST_SUITE
                else None
            ),
            "comparison_period_basis": "control_signal_dates"
            if suite == RESULTS_FIRST_SUITE
            else None,
            "missing_period_score_policy": "cash_return_zero_diagnostic_only"
            if suite == RESULTS_FIRST_SUITE
            else None,
            "incomplete_period_ranking_policy": "exclude_from_ranking"
            if suite == RESULTS_FIRST_SUITE
            else None,
            "best_historical_strategy": best_historical_strategy,
            "rankings": results_first_rankings,
            "excluded_from_ranking": results_first_excluded,
        },
        "robustness": robustness,
        "search_status": search_status,
        "search_stopped": bool(
            mode == "full"
            and suite not in {"legacy-regression", RESULTS_FIRST_SUITE}
        ),
    }
    report_path = output_dir / "report.md"
    report_path.write_text(render_report(summary), encoding="utf-8")
    # ``summary.json`` is the completed marker.  Build and hash it under a
    # pending name, publish the manifest, then atomically expose the summary.
    pending_summary_path = output_dir / "summary.pending.json"
    _write_json(pending_summary_path, summary)
    artifact_paths: list[tuple[str, Path]] = [
        ("summary.json", pending_summary_path),
        ("report.md", report_path),
        *[
            (path.relative_to(output_dir).as_posix(), path)
            for path in sorted(factor_dir.glob("*.json"))
        ],
    ]
    if (output_dir / "robustness.json").is_file():
        artifact_paths.append(("robustness.json", output_dir / "robustness.json"))
    manifest = {
        "schema_version": 1,
        "algorithm": "sha256",
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "files": [
            {
                "path": logical_path,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for logical_path, path in artifact_paths
        ],
    }
    _write_json(output_dir / "manifest.json", manifest)
    pending_summary_path.replace(summary_path)
    _write_json(
        root / "runtime" / "runs" / "latest.json",
        {"run_id": run_id, "output_dir": str(output_dir), "summary_path": str(summary_path)},
    )
    return summary


def latest_run(project_root: str | Path | None = None) -> dict[str, Any] | None:
    root = _project_root(project_root)
    path = root / "runtime" / "runs" / "latest.json"
    return _read_json(path) if path.is_file() else None


__all__ = ["ENGINE_ID", "latest_run", "load_factor_suite", "run_research"]
