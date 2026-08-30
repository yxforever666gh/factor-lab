#!/usr/bin/env python
"""Reproduce the 6.0 fixed-core exit15 versus exit25 exact comparison."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from factor_lab.data import audit_suspensions_snapshot  # noqa: E402
from factor_lab.portfolio.long_only import (  # noqa: E402
    LongOnlyCostConfig,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)
from factor_lab.research.runner import _load_execution  # noqa: E402
from factor_lab.strategy import (  # noqa: E402
    LowChurnStrategyConfig,
    REQUIRED_SIGNAL_COLUMNS,
    fixed_core_score,
    generate_sleeve_target_schedule,
)


PERIODS_PER_YEAR = 25.2
DEFAULT_START = pd.Timestamp("2017-01-03")
PHASE_NAMES = ("train", "validation", "audit", "full")


def _path(value: str | Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()


def _portable_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
            default=str,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _git_text(*args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout.strip()


def _annualized_return(
    values: Iterable[float], periods_per_year: float = PERIODS_PER_YEAR
) -> float:
    array = np.asarray(list(values), dtype=float)
    if array.size == 0:
        return 0.0
    terminal = float(np.prod(1.0 + array))
    return -1.0 if terminal <= 0.0 else terminal ** (periods_per_year / array.size) - 1.0


def _annualized_ratio(
    values: Iterable[float], periods_per_year: float = PERIODS_PER_YEAR
) -> float:
    array = np.asarray(list(values), dtype=float)
    if array.size < 2:
        return 0.0
    deviation = float(np.std(array, ddof=1))
    if deviation <= 0.0 or not math.isfinite(deviation):
        return 0.0
    return float(np.mean(array) / deviation * math.sqrt(periods_per_year))


def _quantiles(
    values: Iterable[float], *, lower_is_worse: bool = True
) -> dict[str, float]:
    array = np.asarray(list(values), dtype=float)
    return {
        "q20": float(np.quantile(array, 0.2)),
        "median": float(np.median(array)),
        "worst": float(np.min(array) if lower_is_worse else np.max(array)),
    }


def _load_signal_frame(path: Path, *, start: pd.Timestamp) -> pd.DataFrame:
    frame = pd.read_parquet(path, columns=list(REQUIRED_SIGNAL_COLUMNS))
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.normalize()
    if frame["date"].isna().any():
        raise ValueError("feature store contains invalid dates")
    frame["ticker"] = frame["ticker"].astype(str)
    frame = frame.loc[frame["date"].ge(start)].sort_values(
        ["date", "ticker"], kind="mergesort"
    )
    if frame.duplicated(["date", "ticker"]).any():
        raise ValueError("feature store contains duplicate date/ticker rows")
    frame = frame.reset_index(drop=True)
    frame["score"] = fixed_core_score(frame)
    if not np.isfinite(frame["score"]).any():
        raise ValueError("feature store has no finite fixed-core scores")
    return frame


def _load_exact_pricing(
    execution_path: Path,
    feature_path: Path,
    suspension_path: Path,
    suspension_metadata_path: Path,
    loader_config: LongOnlyPortfolioConfig,
    *,
    start: pd.Timestamp,
    signal_tickers: set[str],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    execution_dates = pd.to_datetime(
        pd.read_parquet(execution_path, columns=["date"])["date"],
        errors="raise",
    ).dt.normalize()
    suspension_audit = audit_suspensions_snapshot(
        suspension_path,
        metadata_path=suspension_metadata_path,
        requested_start=execution_dates.min().date().isoformat(),
        requested_end=execution_dates.max().date().isoformat(),
    )
    pricing = _load_execution(
        execution_path,
        loader_config,
        feature_path=feature_path,
        suspension_path=suspension_path,
        suspension_snapshot_audit=suspension_audit,
    )
    event_audit = dict(pricing.attrs.get("security_event_injection") or {})
    pricing["date"] = pd.to_datetime(pricing["date"], errors="raise").dt.normalize()
    pricing["ticker"] = pricing["ticker"].astype(str)
    pricing = pricing.loc[
        pricing["date"].ge(start) & pricing["ticker"].isin(signal_tickers)
    ].sort_values(["date", "ticker"], kind="mergesort")
    if pricing.duplicated(["date", "ticker"]).any():
        raise ValueError("event-enriched pricing contains duplicate date/ticker rows")
    pricing = pricing.reset_index(drop=True)
    return pricing, {
        "suspension_snapshot_audit": suspension_audit,
        "security_event_injection": event_audit,
        "rows": int(len(pricing)),
        "tickers": int(pricing["ticker"].nunique()),
        "min_date": pricing["date"].min().date().isoformat(),
        "max_date": pricing["date"].max().date().isoformat(),
    }


def _apply_physical_cutoff(
    signal: pd.DataFrame,
    pricing: pd.DataFrame,
    *,
    max_outcome_date: pd.Timestamp | None,
    holding_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    calendar = [pd.Timestamp(value) for value in sorted(pricing["date"].unique())]
    if max_outcome_date is None:
        return signal, pricing, {
            "requested_max_outcome_date": None,
            "applied": False,
            "pricing_max_date": calendar[-1].date().isoformat(),
            "signal_max_date": signal["date"].max().date().isoformat(),
        }
    eligible_end_indices = [
        index for index, value in enumerate(calendar) if value <= max_outcome_date
    ]
    if not eligible_end_indices:
        raise ValueError("max_outcome_date precedes the pricing calendar")
    end_index = eligible_end_indices[-1]
    horizon = int(holding_days) + 1
    if end_index < horizon:
        raise ValueError("max_outcome_date does not contain one complete holding horizon")
    last_signal_date = calendar[end_index - horizon]
    applied_end = calendar[end_index]
    signal = signal.loc[signal["date"].le(last_signal_date)].copy()
    pricing = pricing.loc[pricing["date"].le(applied_end)].copy()
    return signal, pricing, {
        "requested_max_outcome_date": max_outcome_date.date().isoformat(),
        "applied": True,
        "applied_pricing_max_date": applied_end.date().isoformat(),
        "last_signal_date_with_complete_t_plus_11": last_signal_date.date().isoformat(),
        "holding_end_rule": f"calendar_index_plus_{horizon}",
    }


def _strategy_definition(config: LowChurnStrategyConfig, name: str) -> dict[str, Any]:
    return {
        "id": name,
        "score_formula": (
            "(1-0.70)*rank(earnings_yield/pb) + "
            "0.70*rank(rank(book_yield)+rank(earnings_yield)+rank(-volatility_20))"
        ),
        "defensive_weight": config.defensive_weight,
        "position_count": config.position_count,
        "retention_exit_rank": config.retention_exit_rank,
        "retention_buffer": config.retention_buffer,
        "position_weight": config.position_weight,
        "sleeve_count": config.sleeve_count,
        "anchor_date": config.anchor_date,
        "rank_tie_break": "score_desc_then_ticker_asc",
        "signal_timing": "close_t",
        "entry": "next_official_session_open_adj",
        "holding_end": "open_adj_at_t_plus_11",
    }


def _phase_bounds(args: argparse.Namespace) -> dict[str, tuple[pd.Timestamp, pd.Timestamp]]:
    timestamp_max = pd.Timestamp.max.normalize()
    return {
        "train": (pd.Timestamp(args.start_date), pd.Timestamp(args.train_end)),
        "validation": (
            pd.Timestamp(args.validation_start),
            pd.Timestamp(args.validation_end),
        ),
        "audit": (pd.Timestamp(args.audit_start), timestamp_max),
        "full": (pd.Timestamp(args.start_date), timestamp_max),
    }


def _daily_drawdown(result: Any, selected_periods: list[dict[str, Any]]) -> float:
    if not selected_periods:
        return 0.0
    start_sequence = int(selected_periods[0]["account_nav_path_start_sequence"])
    end_sequence = int(selected_periods[-1]["account_nav_path_end_sequence"])
    nav = np.asarray(
        [
            float(row["nav"])
            for row in result.account_nav_path
            if start_sequence <= int(row["sequence"]) <= end_sequence
        ],
        dtype=float,
    )
    if not len(nav):
        return 0.0
    return float(np.min(nav / np.maximum.accumulate(nav) - 1.0))


def _phase_result(
    result: Any,
    *,
    phase_start: pd.Timestamp,
    phase_end: pd.Timestamp,
) -> dict[str, Any]:
    selected = [
        row
        for row in result.periods
        if pd.Timestamp(row["signal_date"]) >= phase_start
        and pd.Timestamp(row["end_date"]) <= phase_end
    ]
    net = [float(row["net_return"]) for row in selected]
    gross = [float(row["gross_return"]) for row in selected]
    benchmark = [float(row["benchmark_return"]) for row in selected]
    active = [candidate - control for candidate, control in zip(net, benchmark)]
    return {
        "observations": len(selected),
        "start_signal_date": selected[0]["signal_date"] if selected else None,
        "end_signal_date": selected[-1]["signal_date"] if selected else None,
        "max_outcome_end_date": selected[-1]["end_date"] if selected else None,
        "net_cagr": _annualized_return(net),
        "gross_cagr": _annualized_return(gross),
        "benchmark_cagr": _annualized_return(benchmark),
        "net_sharpe": _annualized_ratio(net),
        "information_ratio": _annualized_ratio(active),
        "daily_max_drawdown": _daily_drawdown(result, selected),
        "mean_turnover": (
            float(np.mean([float(row["turnover"]) for row in selected]))
            if selected
            else 0.0
        ),
        "total_cost_fraction": float(
            np.sum(
                [
                    float(row["costs"]["total"])
                    / float(row["accounting_start_nav"])
                    for row in selected
                ]
            )
        ),
        "blocked_trade_count": int(
            sum(int(row["blocked_trade_count"]) for row in selected)
        ),
        "capacity_limited_count": int(
            sum(int(row["capacity_limited_count"]) for row in selected)
        ),
        "capacity_violation_count": int(
            sum(int(row["capacity_violation_count"]) for row in selected)
        ),
        "forced_delist_write_down_count": int(
            sum(int(row["forced_delist_write_down_count"]) for row in selected)
        ),
        "signal_dates": [row["signal_date"] for row in selected],
        "outcome_end_dates": [row["end_date"] for row in selected],
        "period_returns": net,
    }


def _compact_result(
    name: str,
    definition: Mapping[str, Any],
    offset: int,
    result: Any,
    phase_bounds: Mapping[str, tuple[pd.Timestamp, pd.Timestamp]],
) -> dict[str, Any]:
    canonical_periods = [
        {
            "signal_date": row["signal_date"],
            "start_date": row["start_date"],
            "end_date": row["end_date"],
            "net_return": row["net_return"],
            "gross_return": row["gross_return"],
            "benchmark_return": row["benchmark_return"],
            "turnover": row["turnover"],
            "selected_tickers": row["selected_tickers"],
            "target_weights": row["target_weights"],
            "costs": row["costs"],
            "blocked_trade_count": row["blocked_trade_count"],
            "capacity_limited_count": row["capacity_limited_count"],
            "capacity_violation_count": row["capacity_violation_count"],
        }
        for row in result.periods
    ]
    return {
        "candidate_id": name,
        "offset": offset,
        "definition": dict(definition),
        "status": result.status,
        "reason": result.reason,
        "observations": int(result.observations),
        "period_trace_sha256": _canonical_sha256(canonical_periods),
        "daily_nav_sha256": _canonical_sha256(result.account_nav_path),
        "capacity_usage": float(result.capacity_usage),
        "total_cost": float(result.total_cost),
        "trade_count": int(result.trade_count),
        "phases": {
            phase: _phase_result(
                result, phase_start=bounds[0], phase_end=bounds[1]
            )
            for phase, bounds in phase_bounds.items()
        },
    }


def _evaluate_definition(
    signal_frame: pd.DataFrame,
    pricing: pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
    strategy_config: LowChurnStrategyConfig,
    name: str,
    portfolio_base: Mapping[str, Any],
    phase_bounds: Mapping[str, tuple[pd.Timestamp, pd.Timestamp]],
) -> list[dict[str, Any]]:
    definition = _strategy_definition(strategy_config, name)
    schedule = generate_sleeve_target_schedule(
        signal_frame,
        calendar,
        strategy_config,
    )
    signal = signal_frame[["date", "ticker", "score"]].copy()
    output = []
    for offset in range(strategy_config.sleeve_count):
        started = time.time()
        decisions = [
            row
            for row in schedule
            if int(row["sleeve"]) == offset and row["status"] == "ok"
        ]
        targets = {
            row["signal_date"]: row["target_weights"] for row in decisions
        }
        audits = {
            row["signal_date"]: {
                "promotion_eligible": True,
                "generator": "factor_lab.strategy.generate_sleeve_target_schedule",
                "calendar_index": row["calendar_index"],
                "sleeve": row["sleeve"],
            }
            for row in decisions
        }
        portfolio_config = LongOnlyPortfolioConfig(
            **portfolio_base,
            position_count=strategy_config.position_count,
            retention_buffer=strategy_config.retention_buffer,
            target_weight=strategy_config.position_weight,
            rebalance_offset_days=offset,
        )
        result = evaluate_long_only_portfolio(
            signal,
            "score",
            portfolio_config,
            pricing_frame=pricing,
            target_weights_by_date=targets,
            optimization_audit_by_date=audits,
            require_optimized_targets=True,
        )
        if result.status != "ok":
            raise RuntimeError(f"{name}/offset{offset}: {result.reason}")
        compact = _compact_result(
            name, definition, offset, result, phase_bounds
        )
        output.append(compact)
        print(
            f"exact {name} offset={offset} "
            f"full_cagr={compact['phases']['full']['net_cagr']:.6f} "
            f"seconds={time.time() - started:.1f}",
            flush=True,
        )
    return output


def _pair_candidate(
    candidate_rows: list[dict[str, Any]], control_rows: list[dict[str, Any]]
) -> None:
    control = {int(row["offset"]): row for row in control_rows}
    for candidate in candidate_rows:
        comparator = control[int(candidate["offset"])]
        for phase in PHASE_NAMES:
            candidate_phase = candidate["phases"][phase]
            control_phase = comparator["phases"][phase]
            if candidate_phase["signal_dates"] != control_phase["signal_dates"]:
                raise RuntimeError(
                    f"candidate/control signal schedule mismatch offset={candidate['offset']} phase={phase}"
                )
            if candidate_phase["outcome_end_dates"] != control_phase["outcome_end_dates"]:
                raise RuntimeError(
                    f"candidate/control outcome schedule mismatch offset={candidate['offset']} phase={phase}"
                )
            candidate_returns = np.asarray(
                candidate_phase["period_returns"], dtype=float
            )
            control_returns = np.asarray(control_phase["period_returns"], dtype=float)
            relative = (1.0 + candidate_returns) / (1.0 + control_returns) - 1.0
            spread = candidate_returns - control_returns
            candidate_phase["relative_to_control"] = {
                "paired_relative_cagr": _annualized_return(relative),
                "paired_spread_ir": _annualized_ratio(spread),
                "net_cagr_delta": candidate_phase["net_cagr"]
                - control_phase["net_cagr"],
                "net_sharpe_delta": candidate_phase["net_sharpe"]
                - control_phase["net_sharpe"],
                "daily_max_drawdown_delta": candidate_phase["daily_max_drawdown"]
                - control_phase["daily_max_drawdown"],
                "turnover_delta": candidate_phase["mean_turnover"]
                - control_phase["mean_turnover"],
                "positive_period_fraction": (
                    float(np.mean(spread > 0.0)) if len(spread) else 0.0
                ),
            }


def _summary(
    rows: list[dict[str, Any]], *, is_control: bool
) -> dict[str, Any]:
    output = {
        "candidate_id": rows[0]["candidate_id"],
        "definition": rows[0]["definition"],
        "offset_count": len(rows),
        "phases": {},
    }
    for phase in PHASE_NAMES:
        phase_rows = [row["phases"][phase] for row in rows]
        active = [
            0.0
            if is_control
            else float(row["relative_to_control"]["paired_relative_cagr"])
            for row in phase_rows
        ]
        output["phases"][phase] = {
            "net_cagr": _quantiles(row["net_cagr"] for row in phase_rows),
            "net_sharpe": _quantiles(row["net_sharpe"] for row in phase_rows),
            "daily_max_drawdown": _quantiles(
                row["daily_max_drawdown"] for row in phase_rows
            ),
            "mean_turnover": _quantiles(
                (row["mean_turnover"] for row in phase_rows),
                lower_is_worse=False,
            ),
            "paired_relative_cagr": _quantiles(active),
            "positive_relative_offset_count": int(sum(value > 0.0 for value in active)),
            "capacity_violation_count": int(
                sum(row["capacity_violation_count"] for row in phase_rows)
            ),
            "blocked_trade_count": int(
                sum(row["blocked_trade_count"] for row in phase_rows)
            ),
        }
    return output


def _strip_series(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = json.loads(json.dumps(rows, allow_nan=False, default=str))
    for row in payload:
        for phase in row["phases"].values():
            phase.pop("signal_dates", None)
            phase.pop("outcome_end_dates", None)
            phase.pop("period_returns", None)
    return payload


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", default="runtime/data/top500/features.parquet")
    parser.add_argument("--execution", default="runtime/data/top500/execution.parquet")
    parser.add_argument("--suspensions", default="runtime/data/top500/suspensions.parquet")
    parser.add_argument(
        "--suspension-metadata",
        default="runtime/data/top500/suspensions.meta.json",
    )
    parser.add_argument("--research-config", default="configs/research.json")
    parser.add_argument("--output", required=True)
    parser.add_argument("--start-date", default="2017-01-03")
    parser.add_argument("--train-end", default="2022-12-31")
    parser.add_argument("--validation-start", default="2023-01-01")
    parser.add_argument("--validation-end", default="2024-12-31")
    parser.add_argument("--audit-start", default="2025-01-01")
    parser.add_argument(
        "--max-outcome-date",
        help="Physically truncate execution inputs and signals so every t+11 outcome ends on or before this date.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    feature_path = _path(args.features)
    execution_path = _path(args.execution)
    suspension_path = _path(args.suspensions)
    suspension_metadata_path = _path(args.suspension_metadata)
    research_config_path = _path(args.research_config)
    output_path = _path(args.output)
    required_paths = (
        feature_path,
        execution_path,
        suspension_path,
        suspension_metadata_path,
        research_config_path,
    )
    missing = [str(path) for path in required_paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"required inputs are missing: {missing}")

    start = pd.Timestamp(args.start_date).normalize()
    phase_bounds = _phase_bounds(args)
    max_outcome_date = (
        pd.Timestamp(args.max_outcome_date).normalize()
        if args.max_outcome_date
        else None
    )
    research = json.loads(research_config_path.read_text(encoding="utf-8"))
    costs = LongOnlyCostConfig(**research["costs"])
    portfolio_base = {
        "capital": 50_000_000.0,
        "holding_days": 10,
        "rebalance_every_days": 10,
        "periods_per_year": PERIODS_PER_YEAR,
        "date_column": "date",
        "ticker_column": "ticker",
        "open_column": "open_adj",
        "price_basis": "adjusted_total_return",
        "price_source": "canonical_top500_execution_open_adj",
        "lot_size": 0,
        "adv_column": "adv_20",
        "volatility_column": "volatility_20",
        "eligible_columns": ("eligible", "universe_member"),
        "limit_up_column": "is_one_price_limit_up",
        "limit_down_column": "is_one_price_limit_down",
        "max_stale_position_age_days": 21,
        "max_adv_participation": 0.05,
        "costs": costs,
    }
    loader_config = LongOnlyPortfolioConfig(
        **portfolio_base,
        position_count=10,
        retention_buffer=5,
        target_weight=0.1,
        rebalance_offset_days=0,
    )

    print("loading canonical signal frame", flush=True)
    signal_frame = _load_signal_frame(feature_path, start=start)
    print("loading event-enriched exact pricing", flush=True)
    pricing, pricing_audit = _load_exact_pricing(
        execution_path,
        feature_path,
        suspension_path,
        suspension_metadata_path,
        loader_config,
        start=start,
        signal_tickers=set(signal_frame["ticker"]),
    )
    signal_frame, pricing, cutoff_audit = _apply_physical_cutoff(
        signal_frame,
        pricing,
        max_outcome_date=max_outcome_date,
        holding_days=10,
    )
    calendar = [pd.Timestamp(value) for value in sorted(pricing["date"].unique())]
    control_config = LowChurnStrategyConfig(retention_exit_rank=15)
    candidate_config = LowChurnStrategyConfig(retention_exit_rank=25)
    control_name = "fixed_core_top10_exit15"
    candidate_name = "fixed_core_top10_exit25"

    control_rows = _evaluate_definition(
        signal_frame,
        pricing,
        calendar,
        control_config,
        control_name,
        portfolio_base,
        phase_bounds,
    )
    candidate_rows = _evaluate_definition(
        signal_frame,
        pricing,
        calendar,
        candidate_config,
        candidate_name,
        portfolio_base,
        phase_bounds,
    )
    _pair_candidate(candidate_rows, control_rows)
    control_summary = _summary(control_rows, is_control=True)
    candidate_summary = _summary(candidate_rows, is_control=False)

    maximum_selection_end = max(
        (
            row["phases"]["validation"]["max_outcome_end_date"]
            for row in candidate_rows
            if row["phases"]["validation"]["max_outcome_end_date"] is not None
        ),
        default=None,
    )
    source_hashes = {
        "runner": _sha256_file(SCRIPT_PATH),
        "strategy": _sha256_file(PROJECT_ROOT / "src/factor_lab/strategy.py"),
        "long_only": _sha256_file(PROJECT_ROOT / "src/factor_lab/portfolio/long_only.py"),
        "execution_kernel": _sha256_file(
            PROJECT_ROOT / "src/factor_lab/portfolio/execution.py"
        ),
        "research_runner": _sha256_file(
            PROJECT_ROOT / "src/factor_lab/research/runner.py"
        ),
        "research_config": _sha256_file(research_config_path),
        "features": _sha256_file(feature_path),
        "execution": _sha256_file(execution_path),
        "suspensions": _sha256_file(suspension_path),
        "suspension_metadata": _sha256_file(suspension_metadata_path),
    }
    result = {
        "schema_version": 1,
        "kind": "factor_lab_low_churn_exact_evidence",
        "evidence_class": "post_selected_historical_diagnostic_only",
        "promotion_or_profit_claim_allowed": False,
        "repository": {
            "root": ".",
            "head": _git_text("rev-parse", "HEAD"),
            "branch": _git_text("branch", "--show-current"),
        },
        "inputs": {
            "paths": {
                "features": _portable_path(feature_path),
                "execution": _portable_path(execution_path),
                "suspensions": _portable_path(suspension_path),
                "suspension_metadata": _portable_path(suspension_metadata_path),
                "research_config": _portable_path(research_config_path),
            },
            "sha256": source_hashes,
        },
        "execution_contract": {
            "capital": 50_000_000.0,
            "holding_days": 10,
            "rebalance_every_days": 10,
            "offsets": list(range(10)),
            "entry": "next official session open_adj",
            "holding_end": "open_adj at signal calendar index + 11",
            "price_basis": "adjusted_total_return",
            "costs": research["costs"],
            "max_adv_participation": 0.05,
            "max_stale_position_age_days": 21,
        },
        "phase_contract": {
            "train": {
                "signal_start": args.start_date,
                "outcome_end_max": args.train_end,
            },
            "validation": {
                "signal_start": args.validation_start,
                "outcome_end_max": args.validation_end,
            },
            "audit": {"signal_start": args.audit_start},
            "inclusion_rule": "signal_date >= phase_start and end_date <= phase_end",
            "physical_run_cutoff": cutoff_audit,
        },
        "pricing_audit": pricing_audit,
        "definitions": {
            "control": _strategy_definition(control_config, control_name),
            "candidate": _strategy_definition(candidate_config, candidate_name),
        },
        "summary": {
            "control": control_summary,
            "candidate": candidate_summary,
        },
        "per_offset": {
            "control": _strip_series(control_rows),
            "candidate": _strip_series(candidate_rows),
        },
        "assertions": {
            "candidate_and_control_score_formula_identical": True,
            "only_retention_exit_rank_differs": True,
            "candidate_and_control_schedule_paired": True,
            "selection_max_outcome_end_date": maximum_selection_end,
            "selection_does_not_cross_2024_12_31": (
                maximum_selection_end is None or maximum_selection_end <= "2024-12-31"
            ),
            "labels_or_forward_returns_loaded": False,
            "target_state_separate_per_absolute_offset": True,
            "offset_count": len(candidate_rows),
            "all_evaluations_ok": all(
                row["status"] == "ok" for row in control_rows + candidate_rows
            ),
            "capacity_violation_count": int(
                sum(
                    row["phases"]["full"]["capacity_violation_count"]
                    for row in control_rows + candidate_rows
                )
            ),
        },
        "disclosure": {
            "candidate_was_selected_after_historical_search": True,
            "audit_period_already_seen_elsewhere": True,
            "superseded_signal_date_only_split": True,
            "future_qualification_requires_new_data": True,
        },
    }
    result["payload_sha256"] = _canonical_sha256(result)
    _write_json(output_path, result)
    print(f"complete path={output_path}", flush=True)
    print(f"file_sha256={_sha256_file(output_path)}", flush=True)
    print(f"payload_sha256={result['payload_sha256']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
