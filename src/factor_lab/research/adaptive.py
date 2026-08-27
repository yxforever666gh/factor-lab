"""Pure, causal decision primitives for the frozen Factor Lab 5.0 protocol.

The functions in this module have no filesystem, network, or portfolio-engine
side effects.  They turn already-costed shadow-account observations and
point-in-time market features into deterministic decisions.  In particular,
an allocation made for signal date ``t`` may only consume shadow periods whose
``end_date`` is strictly earlier than ``t``.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


_HISTORY_POLICY = "only_periods_with_end_date_strictly_before_current_signal_date"
_SCORE_METHOD = "cumulative_sum_log1p_of_independent_costed_shadow_net_returns"


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be an object")
    return dict(value)


def _finite(value: Any, *, name: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be finite")
    return parsed


def _positive_int(value: Any, *, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed <= 0 or isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def _iso_date(value: Any, *, name: str) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a valid date") from exc
    if pd.isna(parsed):
        raise ValueError(f"{name} must be a valid date")
    if parsed.tzinfo is not None:
        parsed = parsed.tz_convert("UTC").tz_localize(None)
    return parsed.normalize().date().isoformat()


def _sha256(value: Any) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class AdaptiveExpertSpec:
    """Frozen online-allocation and target-combination parameters."""

    ordered_experts: tuple[str, ...]
    anchor_expert: str
    anchor_weight: float
    flexible_sleeve_weight: float
    flexible_sleeve_experts: tuple[str, ...]
    initial_sleeve_weights: tuple[float, ...]
    learning_rate: float
    history_policy: str
    position_count_per_expert: int
    maximum_combined_position_count: int

    @classmethod
    def from_protocol(cls, protocol: Mapping[str, Any]) -> "AdaptiveExpertSpec":
        experts = _mapping(protocol.get("experts"), name="experts")
        allocator = _mapping(protocol.get("online_allocator"), name="online_allocator")
        portfolio = _mapping(protocol.get("portfolio"), name="portfolio")
        ordered = tuple(str(value) for value in experts.get("ordered_registry") or ())
        flexible = tuple(
            str(value) for value in allocator.get("flexible_sleeve_experts") or ()
        )
        if len(ordered) != 4 or len(set(ordered)) != 4 or any(not x for x in ordered):
            raise ValueError("adaptive protocol requires four unique ordered experts")
        if flexible != ordered:
            raise ValueError("flexible sleeve experts must match the ordered registry")
        anchor = str(allocator.get("anchor_expert") or "")
        if anchor not in ordered or anchor != str(experts.get("fixed_core") or ""):
            raise ValueError("anchor expert must be the frozen fixed core")
        anchor_weight = _finite(
            allocator.get("anchor_weight"), name="online_allocator.anchor_weight"
        )
        sleeve_weight = _finite(
            allocator.get("flexible_sleeve_weight"),
            name="online_allocator.flexible_sleeve_weight",
        )
        if anchor_weight < 0.0 or sleeve_weight < 0.0 or not math.isclose(
            anchor_weight + sleeve_weight, 1.0, rel_tol=0.0, abs_tol=1e-12
        ):
            raise ValueError("anchor and flexible sleeve weights must sum to one")
        initial = tuple(
            _finite(value, name="online_allocator.initial_sleeve_weights")
            for value in allocator.get("initial_sleeve_weights") or ()
        )
        if len(initial) != len(ordered) or any(value <= 0.0 for value in initial):
            raise ValueError("initial sleeve weights must positively cover all experts")
        if not math.isclose(sum(initial), 1.0, rel_tol=0.0, abs_tol=1e-12):
            raise ValueError("initial sleeve weights must sum to one")
        learning_rate = _finite(
            allocator.get("learning_rate"), name="online_allocator.learning_rate"
        )
        if learning_rate < 0.0:
            raise ValueError("online allocator learning_rate must be non-negative")
        if allocator.get("score") != _SCORE_METHOD:
            raise ValueError("online allocator score differs from frozen 5.0")
        if allocator.get("allocation") != "softmax_score":
            raise ValueError("online allocator allocation differs from frozen 5.0")
        if (
            allocator.get("decay") is not None
            or allocator.get("score_clipping") is not None
            or allocator.get("hyperparameter_search") is not False
        ):
            raise ValueError("5.0 forbids decay, clipping, and hyperparameter search")
        history_policy = str(experts.get("shadow_history_policy") or "")
        if history_policy != _HISTORY_POLICY:
            raise ValueError("shadow history policy must use strictly matured periods")
        return cls(
            ordered_experts=ordered,
            anchor_expert=anchor,
            anchor_weight=anchor_weight,
            flexible_sleeve_weight=sleeve_weight,
            flexible_sleeve_experts=flexible,
            initial_sleeve_weights=initial,
            learning_rate=learning_rate,
            history_policy=history_policy,
            position_count_per_expert=_positive_int(
                portfolio.get("position_count_per_expert"),
                name="portfolio.position_count_per_expert",
            ),
            maximum_combined_position_count=_positive_int(
                portfolio.get("maximum_combined_position_count"),
                name="portfolio.maximum_combined_position_count",
            ),
        )


@dataclass(frozen=True)
class MarketOverlaySpec:
    """Frozen point-in-time market-risk overlay parameters."""

    return_field: str
    breadth_field: str
    trend_window: int
    volatility_window: int
    volatility_ddof: int
    annualization_days: int
    target_annual_volatility: float
    volatility_scalar_min: float
    volatility_scalar_max: float
    cap_both: float
    cap_exactly_one: float
    cap_neither: float
    minimum_history: int
    minimum_return_coverage: float
    minimum_breadth_coverage: float

    @classmethod
    def from_protocol(cls, protocol: Mapping[str, Any]) -> "MarketOverlaySpec":
        raw = _mapping(protocol.get("market_overlay"), name="market_overlay")
        if raw.get("market_proxy") != (
            "equal_weight_return_1d_over_current_PIT_eligible_universe"
        ):
            raise ValueError("market proxy differs from frozen 5.0")
        if raw.get("market_level") != "cumulative_product_one_plus_market_proxy_return":
            raise ValueError("market level differs from frozen 5.0")
        if raw.get("trend_rule") != "market_level_greater_than_simple_moving_average":
            raise ValueError("trend rule differs from frozen 5.0")
        if raw.get("breadth_rule") != (
            "fraction_strictly_greater_than_zero_at_least_0.5"
        ):
            raise ValueError("breadth rule differs from frozen 5.0")
        if raw.get("exposure") != "minimum_of_regime_cap_and_volatility_scalar":
            raise ValueError("overlay exposure rule differs from frozen 5.0")
        if raw.get("missing_value_policy") != "fail_closed_no_backfill_no_forward_fill":
            raise ValueError("overlay missing-value policy differs from frozen 5.0")
        scalar_clip = tuple(raw.get("volatility_scalar_clip") or ())
        if len(scalar_clip) != 2:
            raise ValueError("volatility scalar clip must have two bounds")
        scalar_min = _finite(scalar_clip[0], name="volatility scalar minimum")
        scalar_max = _finite(scalar_clip[1], name="volatility scalar maximum")
        if not 0.0 <= scalar_min <= scalar_max <= 1.0:
            raise ValueError("volatility scalar clip must lie within [0, 1]")
        caps = _mapping(raw.get("regime_exposure_caps"), name="regime_exposure_caps")
        cap_both = _finite(caps.get("trend_and_breadth"), name="trend_and_breadth cap")
        cap_one = _finite(caps.get("exactly_one"), name="exactly_one cap")
        cap_neither = _finite(caps.get("neither"), name="neither cap")
        if any(not 0.0 <= value <= 1.0 for value in (cap_both, cap_one, cap_neither)):
            raise ValueError("regime exposure caps must lie within [0, 1]")
        return_coverage = _finite(
            raw.get("minimum_daily_return_coverage"),
            name="minimum_daily_return_coverage",
        )
        breadth_coverage = _finite(
            raw.get("minimum_daily_breadth_coverage"),
            name="minimum_daily_breadth_coverage",
        )
        if not 0.0 <= return_coverage <= 1.0 or not 0.0 <= breadth_coverage <= 1.0:
            raise ValueError("overlay coverage thresholds must lie within [0, 1]")
        ddof = int(raw.get("volatility_ddof"))
        if ddof != 1:
            raise ValueError("5.0 volatility ddof must be one")
        return cls(
            return_field="return_1d",
            breadth_field=str(raw.get("breadth_field") or ""),
            trend_window=_positive_int(
                raw.get("trend_window_trading_days"), name="trend_window_trading_days"
            ),
            volatility_window=_positive_int(
                raw.get("volatility_window_trading_days"),
                name="volatility_window_trading_days",
            ),
            volatility_ddof=ddof,
            annualization_days=_positive_int(
                raw.get("annualization_days"), name="annualization_days"
            ),
            target_annual_volatility=_finite(
                raw.get("target_annual_volatility"), name="target_annual_volatility"
            ),
            volatility_scalar_min=scalar_min,
            volatility_scalar_max=scalar_max,
            cap_both=cap_both,
            cap_exactly_one=cap_one,
            cap_neither=cap_neither,
            minimum_history=_positive_int(
                raw.get("minimum_history_trading_days"),
                name="minimum_history_trading_days",
            ),
            minimum_return_coverage=return_coverage,
            minimum_breadth_coverage=breadth_coverage,
        )


@dataclass(frozen=True)
class ExpertPeriod:
    signal_date: str
    start_date: str
    end_date: str
    net_return: float

    def identity(self) -> tuple[str, str, str]:
        return (self.signal_date, self.start_date, self.end_date)


@dataclass(frozen=True)
class ExpertTrace:
    expert_name: str
    rebalance_offset_days: int
    periods: tuple[ExpertPeriod, ...]


@dataclass(frozen=True)
class ExpertCohort:
    signal_date: str
    start_date: str
    end_date: str
    net_returns: tuple[tuple[str, float], ...]

    def returns_by_expert(self) -> dict[str, float]:
        return dict(self.net_returns)


@dataclass(frozen=True)
class OnlineAllocationDecision:
    signal_date: str
    history_policy: str
    matured_cohort_count: int
    excluded_unmatured_cohort_count: int
    latest_matured_end_date: str | None
    cumulative_log_scores: tuple[tuple[str, float], ...]
    flexible_sleeve_weights: tuple[tuple[str, float], ...]
    total_expert_weights: tuple[tuple[str, float], ...]
    history_sha256: str
    future_feedback_violation_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date,
            "history_policy": self.history_policy,
            "matured_cohort_count": self.matured_cohort_count,
            "excluded_unmatured_cohort_count": self.excluded_unmatured_cohort_count,
            "latest_matured_end_date": self.latest_matured_end_date,
            "cumulative_log_scores": dict(self.cumulative_log_scores),
            "flexible_sleeve_weights": dict(self.flexible_sleeve_weights),
            "total_expert_weights": dict(self.total_expert_weights),
            "history_sha256": self.history_sha256,
            "future_feedback_violation_count": self.future_feedback_violation_count,
        }


@dataclass(frozen=True)
class MarketOverlayDecision:
    signal_date: str
    status: str
    ready: bool
    exposure: float
    observation_count: int
    return_coverage: float | None
    breadth_coverage: float | None
    market_level: float | None
    trend_sma: float | None
    trend_positive: bool | None
    breadth_fraction_positive: float | None
    breadth_positive: bool | None
    annualized_volatility: float | None
    volatility_scalar: float | None
    regime_cap: float | None
    latest_input_date: str | None
    future_overlay_violation_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date,
            "status": self.status,
            "ready": self.ready,
            "exposure": self.exposure,
            "observation_count": self.observation_count,
            "return_coverage": self.return_coverage,
            "breadth_coverage": self.breadth_coverage,
            "market_level": self.market_level,
            "trend_sma": self.trend_sma,
            "trend_positive": self.trend_positive,
            "breadth_fraction_positive": self.breadth_fraction_positive,
            "breadth_positive": self.breadth_positive,
            "annualized_volatility": self.annualized_volatility,
            "volatility_scalar": self.volatility_scalar,
            "regime_cap": self.regime_cap,
            "latest_input_date": self.latest_input_date,
            "future_overlay_violation_count": self.future_overlay_violation_count,
        }


@dataclass(frozen=True)
class CombinedTargetDecision:
    signal_date: str
    exposure: float
    expert_weights: tuple[tuple[str, float], ...]
    combined_target_weights: tuple[tuple[str, float], ...]
    invested_weight: float
    cash_weight: float
    overlap_position_count: int
    component_position_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date,
            "exposure": self.exposure,
            "expert_weights": dict(self.expert_weights),
            "combined_target_weights": dict(self.combined_target_weights),
            "invested_weight": self.invested_weight,
            "cash_weight": self.cash_weight,
            "overlap_position_count": self.overlap_position_count,
            "component_position_count": self.component_position_count,
        }


def expert_trace_from_evaluation(
    name: str,
    result: Mapping[str, Any],
    rebalance_offset_days: int,
) -> ExpertTrace:
    """Extract the independently costed period return trace for one expert."""

    expert_name = str(name).strip()
    if not expert_name:
        raise ValueError("expert name must be non-empty")
    observed_name = result.get("factor_name")
    if observed_name is not None and str(observed_name) != expert_name:
        raise ValueError("expert result factor_name does not match the registry")
    raw_periods = result.get("period_active_returns")
    if not isinstance(raw_periods, list) or not raw_periods:
        raise ValueError("expert result must contain costed period_active_returns")
    periods: list[ExpertPeriod] = []
    seen: set[tuple[str, str, str]] = set()
    previous_signal: str | None = None
    for raw in raw_periods:
        if not isinstance(raw, Mapping):
            raise ValueError("expert period must be an object")
        signal_date = _iso_date(raw.get("signal_date"), name="period signal_date")
        start_date = _iso_date(raw.get("start_date"), name="period start_date")
        end_date = _iso_date(raw.get("end_date"), name="period end_date")
        if not signal_date < start_date <= end_date:
            raise ValueError("expert period dates must satisfy signal < start <= end")
        identity = (signal_date, start_date, end_date)
        if identity in seen:
            raise ValueError("expert trace contains a duplicate period")
        if previous_signal is not None and signal_date <= previous_signal:
            raise ValueError("expert periods must be strictly ordered by signal date")
        seen.add(identity)
        previous_signal = signal_date
        net_return = _finite(raw.get("net_return"), name="period net_return")
        if net_return <= -1.0:
            raise ValueError("costed shadow net return must be greater than -100%")
        periods.append(
            ExpertPeriod(
                signal_date=signal_date,
                start_date=start_date,
                end_date=end_date,
                net_return=net_return,
            )
        )
    return ExpertTrace(
        expert_name=expert_name,
        rebalance_offset_days=int(rebalance_offset_days),
        periods=tuple(periods),
    )


def align_expert_cohorts(
    traces: Sequence[ExpertTrace],
    required_experts: Sequence[str],
) -> tuple[ExpertCohort, ...]:
    """Require identical observation boundaries before comparing experts."""

    required = tuple(str(value) for value in required_experts)
    if not required or len(required) != len(set(required)):
        raise ValueError("required_experts must be unique and non-empty")
    lookup: dict[str, ExpertTrace] = {}
    for trace in traces:
        if not isinstance(trace, ExpertTrace):
            raise TypeError("traces must contain ExpertTrace objects")
        if trace.expert_name in lookup:
            raise ValueError("duplicate expert trace")
        lookup[trace.expert_name] = trace
    if set(lookup) != set(required):
        raise ValueError("expert traces must exactly match required_experts")
    offsets = {lookup[name].rebalance_offset_days for name in required}
    if len(offsets) != 1:
        raise ValueError("expert traces must share one rebalance offset")
    reference = lookup[required[0]].periods
    if not reference:
        raise ValueError("expert traces must contain at least one cohort")
    identities = tuple(period.identity() for period in reference)
    for name in required[1:]:
        observed = tuple(period.identity() for period in lookup[name].periods)
        if observed != identities:
            raise ValueError("expert traces do not share identical period boundaries")
    output: list[ExpertCohort] = []
    for index, (signal_date, start_date, end_date) in enumerate(identities):
        output.append(
            ExpertCohort(
                signal_date=signal_date,
                start_date=start_date,
                end_date=end_date,
                net_returns=tuple(
                    (name, lookup[name].periods[index].net_return) for name in required
                ),
            )
        )
    return tuple(output)


def online_wealth_allocations(
    signal_dates: Sequence[Any],
    cohorts: Sequence[ExpertCohort],
    spec: AdaptiveExpertSpec,
) -> dict[str, OnlineAllocationDecision]:
    """Allocate the flexible sleeve using only strictly matured net returns."""

    dates = tuple(_iso_date(value, name="signal date") for value in signal_dates)
    if not dates or len(dates) != len(set(dates)) or dates != tuple(sorted(dates)):
        raise ValueError("signal_dates must be unique and strictly ordered")
    required = spec.ordered_experts
    for cohort in cohorts:
        if tuple(name for name, _ in cohort.net_returns) != required:
            raise ValueError("cohort expert order differs from the frozen registry")
    output: dict[str, OnlineAllocationDecision] = {}
    log_prior = np.log(np.asarray(spec.initial_sleeve_weights, dtype=float))
    for signal_date in dates:
        # A historical decision must be invariant when genuinely future
        # cohorts are appended later.  Only cohorts whose own signal was
        # already observable may contribute either to the matured history or
        # to the diagnostic count of currently-unmatured observations.
        observable = [
            cohort for cohort in cohorts if cohort.signal_date <= signal_date
        ]
        matured = [
            cohort for cohort in observable if cohort.end_date < signal_date
        ]
        scores = np.zeros(len(required), dtype=float)
        history_rows: list[dict[str, Any]] = []
        for cohort in matured:
            returns = cohort.returns_by_expert()
            values = np.asarray([returns[name] for name in required], dtype=float)
            if not np.isfinite(values).all() or bool((values <= -1.0).any()):
                raise ValueError("matured cohort contains an invalid net return")
            scores += np.log1p(values)
            history_rows.append(
                {
                    "signal_date": cohort.signal_date,
                    "start_date": cohort.start_date,
                    "end_date": cohort.end_date,
                    "net_returns": {name: returns[name] for name in required},
                }
            )
        logits = log_prior + spec.learning_rate * scores
        logits -= float(np.max(logits))
        sleeve = np.exp(logits)
        sleeve /= float(sleeve.sum())
        total = spec.flexible_sleeve_weight * sleeve
        total[required.index(spec.anchor_expert)] += spec.anchor_weight
        if not math.isclose(float(total.sum()), 1.0, rel_tol=0.0, abs_tol=1e-12):
            raise RuntimeError("online allocation lost funding conservation")
        output[signal_date] = OnlineAllocationDecision(
            signal_date=signal_date,
            history_policy=spec.history_policy,
            matured_cohort_count=len(matured),
            excluded_unmatured_cohort_count=len(observable) - len(matured),
            latest_matured_end_date=max((row.end_date for row in matured), default=None),
            cumulative_log_scores=tuple(
                (name, float(scores[index])) for index, name in enumerate(required)
            ),
            flexible_sleeve_weights=tuple(
                (name, float(sleeve[index])) for index, name in enumerate(required)
            ),
            total_expert_weights=tuple(
                (name, float(total[index])) for index, name in enumerate(required)
            ),
            history_sha256=_sha256(history_rows),
        )
    return output


def _optional_float(value: Any) -> float | None:
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def build_market_overlay(
    features: pd.DataFrame,
    signal_dates: Sequence[Any],
    spec: MarketOverlaySpec,
) -> dict[str, MarketOverlayDecision]:
    """Build the frozen trend/breadth/volatility overlay without filling data."""

    required_columns = {"date", "ticker", spec.return_field, spec.breadth_field}
    missing = required_columns - set(features.columns)
    if missing:
        raise ValueError(f"market overlay features missing columns: {sorted(missing)}")
    frame = features[list(required_columns)].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    frame["ticker"] = frame["ticker"].astype(str)
    if frame[["date", "ticker"]].duplicated().any():
        raise ValueError("market overlay features contain duplicate date/ticker rows")
    frame[spec.return_field] = pd.to_numeric(
        frame[spec.return_field], errors="coerce"
    ).replace([np.inf, -np.inf], np.nan)
    frame[spec.breadth_field] = pd.to_numeric(
        frame[spec.breadth_field], errors="coerce"
    ).replace([np.inf, -np.inf], np.nan)

    daily_rows: list[dict[str, Any]] = []
    for date_value, group in frame.groupby("date", sort=True):
        count = len(group)
        returns = group[spec.return_field]
        breadth = group[spec.breadth_field]
        return_count = int(returns.notna().sum())
        breadth_count = int(breadth.notna().sum())
        return_coverage = return_count / count if count else 0.0
        breadth_coverage = breadth_count / count if count else 0.0
        return_valid = count > 0 and (
            return_coverage >= spec.minimum_return_coverage
        )
        breadth_valid = count > 0 and (
            breadth_coverage >= spec.minimum_breadth_coverage
        )
        daily_rows.append(
            {
                "date": pd.Timestamp(date_value),
                "return_coverage": float(return_coverage),
                "breadth_coverage": float(breadth_coverage),
                # The market-level series depends only on return coverage.
                # Momentum has its own legitimate PIT warm-up and must not
                # poison an otherwise observable return history.
                "market_return": float(returns.mean()) if return_valid else np.nan,
                "breadth_fraction": float((breadth.dropna() > 0.0).mean())
                if breadth_valid
                else np.nan,
                "return_coverage_valid": bool(return_valid),
                "breadth_coverage_valid": bool(breadth_valid),
            }
        )
    daily = pd.DataFrame(daily_rows).sort_values("date").reset_index(drop=True)
    if daily.empty:
        raise ValueError("market overlay features contain no observations")
    # A missing market return invalidates the cumulative level from that point;
    # this is intentionally stricter than silently filling the gap.
    daily["market_level"] = (1.0 + daily["market_return"]).cumprod(skipna=False)
    daily["trend_sma"] = daily["market_level"].rolling(
        spec.trend_window, min_periods=spec.trend_window
    ).mean()
    daily["daily_volatility"] = daily["market_return"].rolling(
        spec.volatility_window,
        min_periods=spec.volatility_window,
    ).std(ddof=spec.volatility_ddof)
    by_date = {row.date.date().isoformat(): row for row in daily.itertuples(index=False)}

    dates = tuple(_iso_date(value, name="signal date") for value in signal_dates)
    if not dates or len(dates) != len(set(dates)) or dates != tuple(sorted(dates)):
        raise ValueError("signal_dates must be unique and strictly ordered")
    all_dates = tuple(row.date.date().isoformat() for row in daily.itertuples(index=False))
    output: dict[str, MarketOverlayDecision] = {}
    for signal_date in dates:
        prior_dates = tuple(value for value in all_dates if value <= signal_date)
        row = by_date.get(signal_date)
        ready = False
        status = "missing_signal_date"
        exposure = 0.0
        trend_positive: bool | None = None
        breadth_positive: bool | None = None
        annualized_volatility: float | None = None
        volatility_scalar: float | None = None
        regime_cap: float | None = None
        if row is not None:
            coverage_valid = bool(
                row.return_coverage_valid and row.breadth_coverage_valid
            )
            calculations_valid = all(
                math.isfinite(float(value))
                for value in (
                    row.market_level,
                    row.trend_sma,
                    row.breadth_fraction,
                    row.daily_volatility,
                )
            )
            if len(prior_dates) < spec.minimum_history:
                status = "insufficient_history"
            elif not coverage_valid:
                status = "coverage_failure"
            elif not calculations_valid:
                status = "invalid_unfilled_history"
            else:
                trend_positive = bool(float(row.market_level) > float(row.trend_sma))
                breadth_positive = bool(float(row.breadth_fraction) >= 0.5)
                daily_volatility = float(row.daily_volatility)
                annualized_volatility = daily_volatility * math.sqrt(
                    spec.annualization_days
                )
                raw_scalar = (
                    spec.volatility_scalar_max
                    if annualized_volatility <= 0.0
                    else spec.target_annual_volatility / annualized_volatility
                )
                volatility_scalar = float(
                    np.clip(
                        raw_scalar,
                        spec.volatility_scalar_min,
                        spec.volatility_scalar_max,
                    )
                )
                if trend_positive and breadth_positive:
                    regime_cap = spec.cap_both
                elif trend_positive or breadth_positive:
                    regime_cap = spec.cap_exactly_one
                else:
                    regime_cap = spec.cap_neither
                exposure = min(regime_cap, volatility_scalar)
                ready = True
                status = "ready"
        output[signal_date] = MarketOverlayDecision(
            signal_date=signal_date,
            status=status,
            ready=ready,
            exposure=float(exposure),
            observation_count=len(prior_dates),
            return_coverage=None if row is None else float(row.return_coverage),
            breadth_coverage=None if row is None else float(row.breadth_coverage),
            market_level=None if row is None else _optional_float(row.market_level),
            trend_sma=None if row is None else _optional_float(row.trend_sma),
            trend_positive=trend_positive,
            breadth_fraction_positive=None
            if row is None
            else _optional_float(row.breadth_fraction),
            breadth_positive=breadth_positive,
            annualized_volatility=annualized_volatility,
            volatility_scalar=volatility_scalar,
            regime_cap=regime_cap,
            latest_input_date=signal_date if row is not None else None,
        )
    return output


def combine_expert_targets(
    *,
    signal_date: Any,
    targets_by_expert: Mapping[str, Mapping[str, Any]],
    expert_weights: Mapping[str, Any],
    exposure: Any,
    spec: AdaptiveExpertSpec,
) -> CombinedTargetDecision:
    """Combine explicit expert weights; overlaps add and cash is retained."""

    date_value = _iso_date(signal_date, name="signal_date")
    if set(targets_by_expert) != set(spec.ordered_experts):
        raise ValueError("targets must cover exactly the frozen expert registry")
    if set(expert_weights) != set(spec.ordered_experts):
        raise ValueError("expert weights must cover exactly the frozen registry")
    weights = {
        name: _finite(expert_weights[name], name=f"expert weight {name}")
        for name in spec.ordered_experts
    }
    if any(value < 0.0 for value in weights.values()) or not math.isclose(
        sum(weights.values()), 1.0, rel_tol=0.0, abs_tol=1e-8
    ):
        raise ValueError("expert weights must be long-only and sum to one")
    exposure_value = _finite(exposure, name="exposure")
    if not 0.0 <= exposure_value <= 1.0:
        raise ValueError("exposure must lie within [0, 1]")

    combined: dict[str, float] = {}
    component_count = 0
    ticker_occurrences: dict[str, int] = {}
    for name in spec.ordered_experts:
        raw_targets = targets_by_expert[name]
        if not isinstance(raw_targets, Mapping) or not raw_targets:
            raise ValueError(f"expert {name} target weights must be non-empty")
        if len(raw_targets) > spec.position_count_per_expert:
            raise ValueError(f"expert {name} exceeds its frozen position count")
        normalized: dict[str, float] = {}
        for raw_ticker, raw_weight in raw_targets.items():
            ticker = str(raw_ticker).strip()
            target_weight = _finite(raw_weight, name=f"target weight {name}/{ticker}")
            if not ticker or target_weight < 0.0:
                raise ValueError("expert targets must be long-only with non-empty tickers")
            if target_weight > 1e-15:
                normalized[ticker] = target_weight
        if not normalized or sum(normalized.values()) > 1.0 + 1e-8:
            raise ValueError(f"expert {name} target weights violate funding")
        component_count += len(normalized)
        for ticker, target_weight in normalized.items():
            contribution = exposure_value * weights[name] * target_weight
            combined[ticker] = combined.get(ticker, 0.0) + contribution
            ticker_occurrences[ticker] = ticker_occurrences.get(ticker, 0) + 1
    combined = {
        ticker: float(weight)
        for ticker, weight in sorted(combined.items())
        if weight > 1e-15
    }
    if len(combined) > spec.maximum_combined_position_count:
        raise ValueError("combined target exceeds the frozen maximum position count")
    invested = float(sum(combined.values()))
    if invested > exposure_value + 1e-8 or invested > 1.0 + 1e-8:
        raise ValueError("combined target exceeds available funding")
    return CombinedTargetDecision(
        signal_date=date_value,
        exposure=exposure_value,
        expert_weights=tuple((name, weights[name]) for name in spec.ordered_experts),
        combined_target_weights=tuple(combined.items()),
        invested_weight=invested,
        cash_weight=max(0.0, 1.0 - invested),
        overlap_position_count=sum(value > 1 for value in ticker_occurrences.values()),
        component_position_count=component_count,
    )


__all__ = [
    "AdaptiveExpertSpec",
    "MarketOverlaySpec",
    "ExpertTrace",
    "ExpertCohort",
    "OnlineAllocationDecision",
    "MarketOverlayDecision",
    "CombinedTargetDecision",
    "expert_trace_from_evaluation",
    "align_expert_cohorts",
    "online_wealth_allocations",
    "build_market_overlay",
    "combine_expert_targets",
]
