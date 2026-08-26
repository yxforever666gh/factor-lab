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
from .signals import evaluate_factor_signal
from .validation import FactorValidation, evaluate_stage_a, select_stage_b


ENGINE_ID = "factor-lab/lightweight-research/v2"
EVIDENCE_CLASS = "historical_diagnostic"
_DETAIL_FIELDS = {"periods", "trades", "optimization_audit"}


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
        "st_filter_status",
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
    return frame.sort_values([validation.date_column, "ticker"]).reset_index(drop=True)


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
    periods: Sequence[Mapping[str, Any]], *, start: str, end: str | None, periods_per_year: float
) -> dict[str, Any]:
    lower = pd.Timestamp(start)
    upper = pd.Timestamp(end) if end else None
    rows = [
        row
        for row in periods
        if pd.Timestamp(row["start_date"]) >= lower
        and (upper is None or pd.Timestamp(row["start_date"]) <= upper)
    ]
    net = [float(row.get("net_return") or 0.0) for row in rows]
    gross = [float(row.get("gross_return") or 0.0) for row in rows]
    benchmark = [float(row.get("benchmark_return") or 0.0) for row in rows]
    excess = [left - right for left, right in zip(net, benchmark)]
    half_year: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        date = pd.Timestamp(row["start_date"])
        half_year.setdefault(f"{date.year}-H{1 if date.month <= 6 else 2}", []).append(row)
    half_year_excess = [
        _compound(float(item.get("net_return") or 0.0) for item in group)
        - _compound(float(item.get("benchmark_return") or 0.0) for item in group)
        for group in half_year.values()
    ]
    net_annual = _annualized(net, periods_per_year)
    benchmark_annual = _annualized(benchmark, periods_per_year)
    return {
        "start": start,
        "end": end,
        "observations": len(rows),
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
        "capacity_violation_count": int(sum(int(row.get("capacity_violation_count") or 0) for row in rows)),
        "blocked_trade_count": int(sum(int(row.get("blocked_trade_count") or 0) for row in rows)),
        "total_cost": round(float(sum(float((row.get("costs") or {}).get("total") or 0.0) for row in rows)), 4),
    }


def _gate(metrics: Mapping[str, Any], config: Mapping[str, Any]) -> tuple[bool, list[str]]:
    gate = config.get("promotion_gate") or {}
    checks = [
        (float(metrics.get("net_excess_annual_return") or 0.0) > float(gate.get("validation_net_excess_annual_return_min", 0.0)), "non_positive_validation_net_excess"),
        (float(metrics.get("net_sharpe") or 0.0) >= float(gate.get("validation_net_sharpe_min", 0.8)), "validation_sharpe_below_threshold"),
        (float(metrics.get("information_ratio") or 0.0) >= float(gate.get("validation_information_ratio_min", 0.5)), "validation_information_ratio_below_threshold"),
        (float(metrics.get("max_drawdown") or 0.0) >= float(gate.get("validation_max_drawdown_min", -0.25)), "validation_drawdown_exceeds_limit"),
        (float(metrics.get("positive_half_year_ratio") or 0.0) >= float(gate.get("positive_half_year_ratio_min", 0.6)), "positive_half_year_ratio_below_threshold"),
        (float(metrics.get("average_holding_count") or 0.0) >= float(gate.get("average_holding_count_min", 40)), "average_holding_count_below_threshold"),
        (int(metrics.get("capacity_violation_count") or 0) <= int(gate.get("capacity_violation_count_max", 0)), "capacity_violation"),
    ]
    blockers = [reason for passed, reason in checks if not passed]
    return not blockers, blockers


def _compact_portfolio(payload: Mapping[str, Any]) -> dict[str, Any]:
    compact = {key: value for key, value in payload.items() if key not in _DETAIL_FIELDS}
    compact["details_omitted"] = sorted(_DETAIL_FIELDS)
    return compact


def _safe_name(name: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "factor"
    return value[:100]


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
    windows = {
        "train": _window_metrics(periods, start=spec.train_start, end=spec.train_end, periods_per_year=portfolio_config.periods_per_year),
        "validation": _window_metrics(periods, start=spec.validation_start, end=spec.validation_end, periods_per_year=portfolio_config.periods_per_year),
        "audit": _window_metrics(periods, start=spec.audit_start, end=None, periods_per_year=portfolio_config.periods_per_year),
    }
    gate_passed, gate_blockers = _gate(windows["validation"], research_config)
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
        "beats_control": False,
        "validated": False,
    }


def _beats_control(candidate: Mapping[str, Any], control: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    left = (candidate.get("windows") or {}).get("validation") or {}
    right = (control.get("windows") or {}).get("validation") or {}
    tolerance = float((config.get("control_comparison") or {}).get("max_drawdown_worsening_tolerance", 0.02))
    return bool(
        candidate.get("gate_passed")
        and float(left.get("net_sharpe") or 0.0) > float(right.get("net_sharpe") or 0.0)
        and float(left.get("net_excess_annual_return") or 0.0) > float(right.get("net_excess_annual_return") or 0.0)
        and float(left.get("max_drawdown") or 0.0) >= float(right.get("max_drawdown") or 0.0) - tolerance
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
    for factor in factors:
        for positions in matrix.get("position_counts") or (50, 75, 100):
            for rebalance in matrix.get("rebalance_every_days") or (5, 20):
                config = replace(
                    base_config,
                    position_count=int(positions),
                    holding_days=int(rebalance),
                    rebalance_every_days=int(rebalance),
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
                rows.append(
                    {
                        "factor_name": factor.name,
                        "position_count": int(positions),
                        "rebalance_every_days": int(rebalance),
                        "gate_passed": result["gate_passed"],
                        "gate_blockers": result["gate_blockers"],
                        "windows": result["windows"],
                        "portfolio": result["portfolio"],
                    }
                )
                _write_json(output_path, {"status": "running", "results": rows})
    payload = {"status": "completed", "search_stopped": True, "results": rows}
    _write_json(output_path, payload)
    return payload


def run_research(
    *,
    project_root: str | Path | None = None,
    suite: str = "next",
    mode: str = "canary",
    resume: bool = True,
    feature_path: str | Path | None = None,
    execution_path: str | Path | None = None,
    factors_path: str | Path | None = None,
    research_config_path: str | Path | None = None,
    run_robustness: bool = True,
) -> dict[str, Any]:
    """Run the deterministic two-stage historical research loop."""

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
        }
    )
    run_id = fingerprint[:16]
    output_dir = root / "runtime" / "runs" / run_id
    summary_path = output_dir / "summary.json"
    if resume and summary_path.is_file():
        cached = _read_json(summary_path)
        if cached.get("status") == "completed" and cached.get("run_fingerprint") == fingerprint:
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
    if suite == "legacy-regression":
        selected = [factor for factor in all_factors if factor.role != "diagnostic_only"]
    else:
        selected_names = {
            control.name,
            *(row.factor_name for row in select_stage_b(stage_a_rows, validation_spec, excluded_names={control.name})),
        }
        selected = [factor for factor in all_factors if factor.name in selected_names]

    portfolio_config = _portfolio_config(research_config)
    execution = _load_execution(execution_file, portfolio_config)
    evaluation_features = features
    evaluation_execution = execution
    if mode == "canary":
        evaluation_features, evaluation_execution = _canary_frames(features, execution, portfolio_config)

    stage_b: list[dict[str, Any]] = []
    for factor in selected:
        result_path = factor_dir / f"{_safe_name(factor.name)}.json"
        cached_result: dict[str, Any] | None = None
        if resume and result_path.is_file():
            payload = _read_json(result_path)
            if payload.get("run_fingerprint") == fingerprint:
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
    for row in stage_b:
        if row["factor_name"] == control.name:
            continue
        row["beats_control"] = _beats_control(row, control_result, research_config)
        row["validated"] = bool(row["gate_passed"] and row["beats_control"])
        if row["validated"]:
            validated.append(str(row["factor_name"]))

    robustness: dict[str, Any] | None = None
    if mode == "full" and suite == "next" and not validated and run_robustness:
        challenger_results = [row for row in stage_b if row["factor_name"] != control.name]
        best_name = None
        if challenger_results:
            best_name = max(
                challenger_results,
                key=lambda row: (
                    float(((row.get("windows") or {}).get("validation") or {}).get("net_sharpe") or -999.0),
                    float(((row.get("windows") or {}).get("validation") or {}).get("net_excess_annual_return") or -999.0),
                    str(row.get("factor_name")),
                ),
            )["factor_name"]
        robustness_factors = [control]
        if best_name:
            robustness_factors.append(next(row for row in challengers if row.name == best_name))
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

    data_warning = research_config.get("data_warning")
    if not data_warning and ("st_filter_status" not in features.columns or any(
        "unverified" in str(value).casefold() or "degraded" in str(value).casefold()
        for value in features.get("st_filter_status", pd.Series(dtype=str)).dropna().unique()
    )):
        data_warning = "st_history_unverified"
    summary: dict[str, Any] = {
        "schema_version": 1,
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
        },
        "control_factor": control.name,
        "stage_a": [row.to_dict() for row in stage_a_rows],
        "stage_b_selected": [row.name for row in selected],
        "stage_b": stage_b,
        "validated_factors": validated,
        "validated_count": len(validated),
        "robustness": robustness,
        "search_stopped": bool(mode == "full" and suite == "next" and not validated),
    }
    _write_json(summary_path, summary)
    report_path = output_dir / "report.md"
    report_path.write_text(render_report(summary), encoding="utf-8")
    artifact_paths = [summary_path, report_path, *sorted(factor_dir.glob("*.json"))]
    if (output_dir / "robustness.json").is_file():
        artifact_paths.append(output_dir / "robustness.json")
    manifest = {
        "schema_version": 1,
        "algorithm": "sha256",
        "run_id": run_id,
        "run_fingerprint": fingerprint,
        "files": [
            {
                "path": path.relative_to(output_dir).as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
            for path in artifact_paths
        ],
    }
    _write_json(output_dir / "manifest.json", manifest)
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
