"""Canonical Research OS adapter around the executable long-only engine."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, time
from typing import Any, Mapping, Protocol
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from factor_lab.long_only_portfolio import (
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)
from factor_lab.research_os.risk_optimizer import (
    StockOptimizationPolicy,
    optimize_stock_weights,
)


CANONICAL_EVALUATOR_VERSION = "research_os.long_only.v2"


class RunRecorder(Protocol):
    def record_canonical_run(self, payload: Mapping[str, Any]) -> Any: ...


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        return dict(value.model_dump(mode="json"))
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"unsupported specification type: {type(value)!r}")


def canonical_fingerprint(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _column(frame: pd.DataFrame, preferred: str, aliases: tuple[str, ...]) -> str | None:
    return next((name for name in (preferred, *aliases) if name in frame.columns), None)


def _signal_series(frame: pd.DataFrame, signal: str | pd.Series) -> pd.Series:
    if isinstance(signal, str):
        if signal not in frame.columns:
            raise KeyError(signal)
        return pd.to_numeric(frame[signal], errors="coerce")
    if len(signal) != len(frame):
        raise ValueError("signal length must equal frame length")
    if signal.index.equals(frame.index):
        return pd.to_numeric(signal, errors="coerce")
    return pd.Series(pd.to_numeric(signal, errors="coerce").to_numpy(), index=frame.index)


def _returns_wide(
    values: pd.DataFrame,
    *,
    date_column: str,
    ticker_column: str,
) -> pd.DataFrame:
    frame = values.copy()
    if isinstance(frame.index, pd.DatetimeIndex):
        result = frame
    else:
        date_col = _column(frame, date_column, ("trade_date",))
        ticker_col = _column(frame, ticker_column, ("ts_code", "symbol"))
        return_col = _column(frame, "return", ("ret", "daily_return"))
        if date_col and ticker_col and return_col:
            frame[date_col] = pd.to_datetime(frame[date_col], errors="raise")
            result = frame.pivot(index=date_col, columns=ticker_col, values=return_col)
        elif date_col:
            result = frame.set_index(pd.to_datetime(frame.pop(date_col), errors="raise"))
        else:
            raise ValueError(
                "returns_history requires a DatetimeIndex, or date/ticker/return columns"
            )
    result.index = pd.DatetimeIndex(pd.to_datetime(result.index, errors="raise")).tz_localize(None).normalize()
    result.columns = result.columns.astype(str)
    return result.sort_index().apply(pd.to_numeric, errors="coerce")


def _optimizer_policy(
    config: LongOnlyPortfolioConfig,
    value: StockOptimizationPolicy | Mapping[str, Any] | None,
    portfolio_policy: Mapping[str, Any] | None = None,
) -> StockOptimizationPolicy:
    public = dict(portfolio_policy or {})
    if isinstance(public.get("portfolio"), Mapping):
        public = dict(public["portfolio"])
    public_aliases = {
        "minimum_position_count": "min_positions",
        "maximum_position_count": "max_positions",
        "industry_active_weight_limit": "industry_deviation",
        "size_active_weight_limit": "size_deviation",
        "minimum_beta": "beta_min",
        "maximum_beta": "beta_max",
    }
    inherited = {
        optimizer_name: public[contract_name]
        for contract_name, optimizer_name in public_aliases.items()
        if contract_name in public
    }
    if isinstance(value, StockOptimizationPolicy):
        explicit = asdict(value)
    else:
        explicit = dict(value or {})
    source = {**inherited, **explicit}
    known = StockOptimizationPolicy.__dataclass_fields__
    unknown = sorted(set(source) - set(known))
    if unknown:
        raise ValueError(f"unknown stock optimization settings: {unknown}")
    base = StockOptimizationPolicy(**source)
    # An optimizer override may tighten the registered portfolio contract, but
    # it must never silently loosen the policy that participates in the
    # experiment fingerprint.
    if "industry_active_weight_limit" in public:
        source["industry_deviation"] = min(
            base.industry_deviation, float(public["industry_active_weight_limit"])
        )
    if "size_active_weight_limit" in public:
        source["size_deviation"] = min(
            base.size_deviation, float(public["size_active_weight_limit"])
        )
    if "minimum_beta" in public:
        source["beta_min"] = max(base.beta_min, float(public["minimum_beta"]))
    if "maximum_beta" in public:
        source["beta_max"] = min(base.beta_max, float(public["maximum_beta"]))
    if "minimum_position_count" in public:
        source["min_positions"] = max(
            base.min_positions, int(public["minimum_position_count"])
        )
    if "maximum_position_count" in public:
        source["max_positions"] = min(
            base.max_positions, int(public["maximum_position_count"])
        )
    base = StockOptimizationPolicy(**source)
    if base.beta_min > base.beta_max:
        raise ValueError("effective stock-optimization beta bounds are empty")
    if base.min_positions > config.position_count:
        raise ValueError(
            "effective minimum positions exceeds the registered target count"
        )
    return StockOptimizationPolicy(
        **{
            **asdict(base),
            "min_positions": min(base.min_positions, config.position_count),
            "max_positions": min(base.max_positions, config.position_count),
            "max_position_weight": min(base.max_position_weight, config.target_weight),
            "max_adv_participation": min(
                base.max_adv_participation, config.max_adv_participation
            ),
            "capital": config.capital,
        }
    )


def _benchmark_at(
    benchmark_weights: pd.Series | pd.DataFrame | Mapping[str, float] | None,
    *,
    signal_date: pd.Timestamp,
    cutoff: pd.Timestamp,
    metadata: pd.DataFrame,
    date_column: str,
    ticker_column: str,
) -> tuple[pd.Series, str]:
    if benchmark_weights is None:
        if metadata.empty:
            return pd.Series(dtype=float), "missing_pit_eligible_universe"
        return (
            pd.Series(1.0 / len(metadata), index=metadata.index, dtype=float),
            "pit_eligible_universe_equal_weight",
        )
    if isinstance(benchmark_weights, Mapping) and not isinstance(
        benchmark_weights, (pd.Series, pd.DataFrame)
    ):
        benchmark_weights = pd.Series(dict(benchmark_weights), dtype=float)
    if isinstance(benchmark_weights, pd.Series):
        result = pd.to_numeric(benchmark_weights, errors="coerce")
        result.index = result.index.astype(str)
        return result, "static_explicit"

    frame = benchmark_weights.copy()
    if isinstance(frame.index, pd.DatetimeIndex):
        eligible = frame.loc[frame.index.tz_localize(None).normalize() <= signal_date]
        if eligible.empty:
            return pd.Series(dtype=float), "missing"
        row = pd.to_numeric(eligible.iloc[-1], errors="coerce")
        row.index = row.index.astype(str)
        return row, "pit_wide"
    date_col = _column(frame, date_column, ("trade_date",))
    ticker_col = _column(frame, ticker_column, ("ts_code", "symbol"))
    weight_col = _column(frame, "benchmark_weight", ("weight",))
    if not date_col or not ticker_col or not weight_col or "available_at" not in frame.columns:
        raise ValueError(
            "long benchmark weights require date, ticker, benchmark_weight and available_at"
        )
    frame[date_col] = pd.to_datetime(frame[date_col], errors="raise").dt.tz_localize(None).dt.normalize()
    frame["available_at"] = pd.to_datetime(frame["available_at"], errors="raise", utc=True)
    eligible = frame.loc[
        (frame[date_col] <= signal_date) & (frame["available_at"] <= cutoff)
    ].sort_values([ticker_col, date_col, "available_at"])
    eligible = eligible.drop_duplicates(ticker_col, keep="last")
    result = pd.Series(
        pd.to_numeric(eligible[weight_col], errors="coerce").to_numpy(),
        index=eligible[ticker_col].astype(str),
        dtype=float,
    )
    return result, "pit_long"


def _optimized_targets(
    *,
    frame: pd.DataFrame,
    signal: str | pd.Series,
    pricing_frame: pd.DataFrame | None,
    exposure_frame: pd.DataFrame | None,
    returns_history: pd.DataFrame | None,
    benchmark_weights: pd.Series | pd.DataFrame | Mapping[str, float] | None,
    config: LongOnlyPortfolioConfig,
    optimization_policy: StockOptimizationPolicy | Mapping[str, Any] | None,
    portfolio_policy: Mapping[str, Any] | None,
    required: bool,
) -> tuple[dict[pd.Timestamp, dict[str, float]], dict[pd.Timestamp, dict[str, Any]], tuple[str, ...], StockOptimizationPolicy]:
    policy = _optimizer_policy(config, optimization_policy, portfolio_policy)
    targets: dict[pd.Timestamp, dict[str, float]] = {}
    audits: dict[pd.Timestamp, dict[str, Any]] = {}
    blockers: list[str] = []
    if exposure_frame is None or returns_history is None:
        if required:
            if exposure_frame is None:
                blockers.append("missing_pit_exposure_data")
            if returns_history is None:
                blockers.append("missing_historical_returns_for_risk_model")
        return targets, audits, tuple(blockers), policy

    date_col = _column(frame, config.date_column, ("trade_date",))
    ticker_col = _column(frame, config.ticker_column, ("ts_code", "symbol"))
    if not date_col or not ticker_col:
        blockers.append("missing_signal_date_or_ticker_for_risk_optimization")
        return targets, audits, tuple(blockers), policy
    exposure = exposure_frame.copy()
    exposure_date_col = _column(exposure, config.date_column, ("trade_date",))
    exposure_ticker_col = _column(
        exposure, config.ticker_column, ("ts_code", "symbol")
    )
    required_columns = {
        "industry",
        "size_bucket",
        "beta",
        "adv_20",
        "industry_is_pit",
        "available_at",
    }
    missing = sorted(required_columns - set(exposure.columns))
    if not exposure_date_col or not exposure_ticker_col:
        missing.extend(["date", "ticker"])
    if missing:
        blockers.append("missing_pit_exposure_columns:" + ",".join(sorted(set(missing))))
        return targets, audits, tuple(blockers), policy

    exposure[exposure_date_col] = pd.to_datetime(
        exposure[exposure_date_col], errors="raise"
    ).dt.tz_localize(None).dt.normalize()
    exposure["available_at"] = pd.to_datetime(
        exposure["available_at"], errors="raise", utc=True
    )
    exposure[exposure_ticker_col] = exposure[exposure_ticker_col].astype(str)
    history = _returns_wide(
        returns_history,
        date_column=config.date_column,
        ticker_column=config.ticker_column,
    )

    signals = frame[[date_col, ticker_col]].copy()
    signals["_score"] = _signal_series(frame, signal).to_numpy()
    signals[date_col] = pd.to_datetime(signals[date_col], errors="raise").dt.tz_localize(None).dt.normalize()
    signals[ticker_col] = signals[ticker_col].astype(str)
    signals = signals.drop_duplicates([date_col, ticker_col], keep="last")
    price_source = pricing_frame if pricing_frame is not None else frame
    price_date_col = _column(price_source, config.date_column, ("trade_date",))
    if not price_date_col:
        blockers.append("missing_pricing_dates_for_risk_optimization")
        return targets, audits, tuple(blockers), policy
    price_dates = sorted(
        pd.to_datetime(price_source[price_date_col], errors="raise")
        .dt.tz_localize(None)
        .dt.normalize()
        .unique()
    )
    decision_dates = {
        pd.Timestamp(price_dates[index]).normalize()
        for index in range(
            0,
            len(price_dates) - config.holding_days - 1,
            config.rebalance_every_days,
        )
    }
    signal_dates = sorted(set(signals[date_col]).intersection(decision_dates))
    shanghai = ZoneInfo("Asia/Shanghai")
    previous = pd.Series(dtype=float)
    for signal_date in signal_dates:
        cutoff = pd.Timestamp(
            datetime.combine(signal_date.date(), time(15, 0), tzinfo=shanghai)
        ).tz_convert("UTC")
        known = exposure.loc[
            (exposure[exposure_date_col] <= signal_date)
            & (exposure["available_at"] <= cutoff)
        ].sort_values([exposure_ticker_col, exposure_date_col, "available_at"])
        known = known.drop_duplicates(exposure_ticker_col, keep="last")
        metadata = known.set_index(exposure_ticker_col)[
            ["industry", "size_bucket", "beta", "adv_20", "industry_is_pit"]
        ]
        score_rows = signals.loc[signals[date_col] == signal_date]
        scores = pd.Series(
            pd.to_numeric(score_rows["_score"], errors="coerce").to_numpy(),
            index=score_rows[ticker_col].astype(str),
            dtype=float,
        )
        historical = history.loc[history.index <= signal_date]
        benchmark, benchmark_mode = _benchmark_at(
            benchmark_weights,
            signal_date=signal_date,
            cutoff=cutoff,
            metadata=metadata,
            date_column=config.date_column,
            ticker_column=config.ticker_column,
        )
        if required and benchmark_mode in {"static_explicit", "pit_wide"}:
            blockers.append(
                f"benchmark_weights_not_bitemporal:{signal_date.date()}:{benchmark_mode}"
            )
        result = optimize_stock_weights(
            scores,
            historical,
            metadata,
            benchmark,
            previous_weights=previous,
            policy=policy,
        )
        audit = {
            "optimizer_status": result.status,
            "promotion_eligible": result.promotion_eligible,
            "decision_cutoff": cutoff.isoformat(),
            "exposure_rows": len(metadata),
            "returns_end": (
                None if historical.empty else historical.index.max().isoformat()
            ),
            "return_observations": len(historical),
            "benchmark_mode": benchmark_mode,
            "optimizer": result.audit,
        }
        audits[signal_date] = audit
        if result.promotion_eligible:
            targets[signal_date] = result.weights
            previous = pd.Series(result.weights, dtype=float)
        else:
            blockers.append(f"stock_optimizer:{signal_date.date()}:{result.status}")
    if required and not signal_dates:
        blockers.append("no_rebalance_signal_dates_for_risk_optimization")
    return targets, audits, tuple(dict.fromkeys(blockers)), policy


@dataclass(frozen=True)
class CanonicalEvaluationResult:
    run_id: str
    experiment_id: str
    snapshot_id: str
    factor_or_sleeve_id: str
    evaluator_version: str
    status: str
    result: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CanonicalLongOnlyEvaluator:
    """The only promotion-eligible portfolio evaluator in Research OS."""

    def __init__(self, recorder: RunRecorder | None = None) -> None:
        self.recorder = recorder

    @staticmethod
    def validate_policy(policy: Mapping[str, Any], *, production_contract: bool) -> LongOnlyPortfolioConfig:
        mode = str(policy.get("mode") or (policy.get("portfolio") or {}).get("mode") or "long_only")
        if mode != "long_only":
            raise ValueError("Research OS supports promotion only for long_only portfolios")
        # Translate public contract names explicitly.  Otherwise a valid
        # fingerprint could describe one policy while dataclass defaults make
        # the engine execute another one.
        source = policy.get("portfolio") if isinstance(policy.get("portfolio"), Mapping) else policy
        normalized_portfolio = dict(source)
        aliases = {
            "rebalance_sessions": "rebalance_every_days",
            "target_position_count": "position_count",
            "maximum_stock_weight": "target_weight",
            "maximum_adv_participation": "max_adv_participation",
        }
        for public_name, engine_name in aliases.items():
            if public_name in source and engine_name not in normalized_portfolio:
                normalized_portfolio[engine_name] = source[public_name]
        if "rebalance_sessions" in source and "holding_days" not in normalized_portfolio:
            normalized_portfolio["holding_days"] = source["rebalance_sessions"]
        normalized = dict(policy)
        if source is policy:
            normalized.update(normalized_portfolio)
        else:
            normalized["portfolio"] = normalized_portfolio
        config = LongOnlyPortfolioConfig.from_mapping(normalized)
        if production_contract:
            errors: list[str] = []
            if abs(config.capital - 50_000_000.0) > 1e-6:
                errors.append("capital_must_equal_50000000")
            if config.holding_days != 5 or config.rebalance_every_days != 5:
                errors.append("weekly_non_overlapping_5d_required")
            if not 50 <= config.position_count <= 100:
                errors.append("position_count_must_be_50_to_100")
            if config.target_weight > 0.02 + 1e-12:
                errors.append("single_position_weight_above_2pct")
            if config.max_adv_participation > 0.05 + 1e-12:
                errors.append("adv_participation_above_5pct")
            if float(source.get("industry_active_weight_limit", 0.05)) > 0.05 + 1e-12:
                errors.append("industry_active_weight_limit_above_5pct")
            if float(source.get("size_active_weight_limit", 0.05)) > 0.05 + 1e-12:
                errors.append("size_active_weight_limit_above_5pct")
            if float(source.get("minimum_beta", 0.9)) < 0.9 - 1e-12:
                errors.append("minimum_beta_below_0_9")
            if float(source.get("maximum_beta", 1.1)) > 1.1 + 1e-12:
                errors.append("maximum_beta_above_1_1")
            minimum_positions = int(source.get("minimum_position_count", 50))
            maximum_positions = int(source.get("maximum_position_count", 100))
            if not minimum_positions <= config.position_count <= maximum_positions:
                errors.append("position_count_outside_registered_bounds")
            if str(source.get("benchmark", "eligible_universe_equal_weight")) != "eligible_universe_equal_weight":
                errors.append("eligible_universe_equal_weight_benchmark_required")
            if str(source.get("covariance_estimator", "ledoit_wolf")) != "ledoit_wolf":
                errors.append("ledoit_wolf_covariance_required")
            if errors:
                raise ValueError(";".join(errors))
        return config

    def evaluate(
        self,
        *,
        experiment_id: str,
        snapshot_id: str,
        factor_or_sleeve_id: str,
        frame: pd.DataFrame,
        signal: str | pd.Series,
        portfolio_policy: Mapping[str, Any] | Any,
        pricing_frame: pd.DataFrame | None = None,
        exposure_frame: pd.DataFrame | None = None,
        returns_history: pd.DataFrame | None = None,
        benchmark_weights: pd.Series | pd.DataFrame | Mapping[str, float] | None = None,
        optimization_policy: StockOptimizationPolicy | Mapping[str, Any] | None = None,
        production_contract: bool = True,
        environment_hash: str = "unknown",
    ) -> CanonicalEvaluationResult:
        policy = _mapping(portfolio_policy)
        config = self.validate_policy(policy, production_contract=production_contract)
        targets, optimization_audit, risk_blockers, resolved_optimization_policy = (
            _optimized_targets(
                frame=frame,
                signal=signal,
                pricing_frame=pricing_frame,
                exposure_frame=exposure_frame,
                returns_history=returns_history,
                benchmark_weights=benchmark_weights,
                config=config,
                optimization_policy=optimization_policy,
                portfolio_policy=policy,
                required=production_contract,
            )
        )
        identity = {
            "experiment_id": experiment_id,
            "snapshot_id": snapshot_id,
            "factor_or_sleeve_id": factor_or_sleeve_id,
            "portfolio_policy": policy,
            "evaluator_version": CANONICAL_EVALUATOR_VERSION,
            "environment_hash": environment_hash,
            "risk_optimization_policy": asdict(resolved_optimization_policy),
            "risk_input_mode": (
                "pit_exposure_and_historical_returns"
                if exposure_frame is not None and returns_history is not None
                else "diagnostic_equal_weight_fallback"
            ),
        }
        run_id = canonical_fingerprint(identity)
        evaluation = evaluate_long_only_portfolio(
            frame,
            signal,
            config,
            pricing_frame=pricing_frame,
            target_weights_by_date=targets,
            optimization_audit_by_date=optimization_audit,
            promotion_blockers=risk_blockers,
            require_optimized_targets=production_contract,
        ).to_dict()
        payload = CanonicalEvaluationResult(
            run_id=run_id,
            experiment_id=experiment_id,
            snapshot_id=snapshot_id,
            factor_or_sleeve_id=factor_or_sleeve_id,
            evaluator_version=CANONICAL_EVALUATOR_VERSION,
            status=str(evaluation.get("status") or "unknown"),
            result=evaluation,
        )
        if self.recorder is not None:
            self.recorder.record_canonical_run(payload.to_dict())
        return payload
