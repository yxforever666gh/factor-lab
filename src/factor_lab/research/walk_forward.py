"""Causal walk-forward selection over pre-registered portfolio signals.

The selector is intentionally small.  Static candidate accounts provide only
fully matured, cost-after returns.  At each decision date the deployed signal
is chosen without reading a period whose end date is on or after that date.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


HISTORY_POLICY = "end_date_strictly_before_signal_date"


@dataclass(frozen=True, slots=True)
class WalkForwardSelectorSpec:
    lookback_trading_days: int = 504
    minimum_completed_periods: int = 48
    update_every_trading_days: int = 20
    score_method: str = "net_sharpe"
    control_score_guard: float = 0.10
    selection_count: int = 1
    selection_weighting: str = "equal"
    history_policy: str = HISTORY_POLICY
    missing_signal_policy: str = "fallback_control"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "WalkForwardSelectorSpec":
        integer_fields = (
            "lookback_trading_days",
            "minimum_completed_periods",
            "update_every_trading_days",
            "selection_count",
        )
        if any(isinstance(value.get(field), (bool, np.bool_)) for field in integer_fields):
            raise ValueError("walk_forward integer settings must be integers")
        spec = cls(
            lookback_trading_days=int(value.get("lookback_trading_days", 504)),
            minimum_completed_periods=int(value.get("minimum_completed_periods", 48)),
            update_every_trading_days=int(value.get("update_every_trading_days", 20)),
            score_method=str(value.get("score_method", "net_sharpe")),
            control_score_guard=float(value.get("control_score_guard", 0.10)),
            selection_count=int(value.get("selection_count", 1)),
            selection_weighting=str(value.get("selection_weighting", "equal")),
            history_policy=str(value.get("history_policy", HISTORY_POLICY)),
            missing_signal_policy=str(
                value.get("missing_signal_policy", "fallback_control")
            ),
        )
        spec.validate()
        return spec

    def validate(self) -> None:
        if self.lookback_trading_days <= 0:
            raise ValueError("walk_forward lookback_trading_days must be positive")
        if self.minimum_completed_periods < 2:
            raise ValueError(
                "walk_forward minimum_completed_periods must be at least 2"
            )
        if self.update_every_trading_days <= 0:
            raise ValueError(
                "walk_forward update_every_trading_days must be positive"
            )
        if self.score_method != "net_sharpe":
            raise ValueError("walk_forward score_method must be 'net_sharpe'")
        if not np.isfinite(self.control_score_guard) or self.control_score_guard < 0:
            raise ValueError(
                "walk_forward control_score_guard must be finite and non-negative"
            )
        if self.selection_count <= 0:
            raise ValueError("walk_forward selection_count must be positive")
        if self.selection_weighting != "equal":
            raise ValueError("walk_forward selection_weighting must be 'equal'")
        if self.history_policy != HISTORY_POLICY:
            raise ValueError(
                f"walk_forward history_policy must be '{HISTORY_POLICY}'"
            )
        if self.missing_signal_policy != "fallback_control":
            raise ValueError(
                "walk_forward missing_signal_policy must be 'fallback_control'"
            )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _period_frame(rows: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "signal_date": row.get("signal_date"),
                "start_date": row.get("start_date"),
                "end_date": row.get("end_date"),
                "net_return": row.get("net_return"),
            }
            for row in rows
        ]
    )
    if frame.empty:
        return pd.DataFrame(
            columns=["signal_date", "start_date", "end_date", "net_return"]
        )
    frame["signal_date"] = pd.to_datetime(frame["signal_date"], errors="coerce")
    frame["start_date"] = pd.to_datetime(frame["start_date"], errors="coerce")
    frame["end_date"] = pd.to_datetime(frame["end_date"], errors="coerce")
    frame["net_return"] = pd.to_numeric(frame["net_return"], errors="coerce")
    if frame[["signal_date", "start_date", "end_date", "net_return"]].isna().any().any():
        raise ValueError("walk_forward period history contains invalid required values")
    if frame["signal_date"].duplicated(keep=False).any():
        raise ValueError("walk_forward period history contains duplicate signal_date")
    valid_chronology = (frame["signal_date"] < frame["start_date"]) & (
        frame["start_date"] <= frame["end_date"]
    )
    if not bool(valid_chronology.all()):
        raise ValueError(
            "walk_forward period chronology must satisfy "
            "signal_date < start_date <= end_date"
        )
    return frame.sort_values("signal_date").reset_index(drop=True)


def _net_sharpe(values: Sequence[float], periods_per_year: float) -> float:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if len(array) < 2:
        return float("-inf")
    standard_deviation = float(np.std(array, ddof=1))
    if standard_deviation <= 0.0:
        return float("inf") if float(np.mean(array)) > 0.0 else 0.0
    return float(np.mean(array) / standard_deviation * np.sqrt(periods_per_year))


def causal_candidate_decisions(
    *,
    trading_dates: Sequence[pd.Timestamp],
    signal_dates: Sequence[pd.Timestamp],
    candidate_periods: Mapping[str, Sequence[Mapping[str, Any]]],
    control_factor: str,
    selector: WalkForwardSelectorSpec,
    periods_per_year: float,
) -> dict[str, Any]:
    """Select a small equal-weight candidate set from strictly matured rows."""

    candidate_names = list(candidate_periods)
    if not candidate_names or control_factor not in candidate_periods:
        raise ValueError("walk_forward candidate registry must include the control")
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_dates))).drop_duplicates().sort_values()
    if calendar.empty:
        raise ValueError("walk_forward trading calendar is empty")
    calendar_position = {pd.Timestamp(value): index for index, value in enumerate(calendar)}
    schedule = pd.DatetimeIndex(pd.to_datetime(list(signal_dates))).drop_duplicates().sort_values()
    if any(pd.Timestamp(value) not in calendar_position for value in schedule):
        raise ValueError("walk_forward signal date is absent from the trading calendar")
    histories = {name: _period_frame(candidate_periods[name]) for name in candidate_names}
    control_history = histories[control_factor]
    if not np.isfinite(periods_per_year) or periods_per_year <= 0.0:
        raise ValueError("walk_forward periods_per_year must be positive")

    selected_factors = (control_factor,)
    last_update_position: int | None = None
    updates: list[dict[str, Any]] = []
    selections: list[dict[str, Any]] = []
    future_violations = 0
    switch_count = 0

    for signal_date in schedule:
        signal_date = pd.Timestamp(signal_date)
        position = calendar_position[signal_date]
        should_update = (
            last_update_position is None
            or position - last_update_position >= selector.update_every_trading_days
        )
        update: dict[str, Any] | None = None
        if should_update:
            lookback_position = max(0, position - selector.lookback_trading_days)
            lookback_start = pd.Timestamp(calendar[lookback_position])
            reference = control_history[
                (control_history["end_date"] < signal_date)
                & (control_history["signal_date"] < signal_date)
                & (control_history["signal_date"] >= lookback_start)
            ]
            reference_dates = tuple(reference["signal_date"].tolist())
            candidate_scores: list[dict[str, Any]] = []
            latest_used_end_date: pd.Timestamp | None = None
            if len(reference_dates) >= selector.minimum_completed_periods:
                for registry_index, name in enumerate(candidate_names):
                    history = histories[name]
                    available = history[
                        (history["end_date"] < signal_date)
                        & (history["signal_date"] < signal_date)
                        & history["signal_date"].isin(reference_dates)
                    ]
                    by_date = available.set_index("signal_date")
                    complete = len(by_date) == len(reference_dates) and all(
                        date in by_date.index for date in reference_dates
                    )
                    if not complete:
                        candidate_scores.append(
                            {
                                "factor_name": name,
                                "eligible": False,
                                "reason": "incomplete_matured_period_coverage",
                                "observations": int(len(by_date)),
                                "score": None,
                                "registry_index": registry_index,
                            }
                        )
                        continue
                    ordered = by_date.loc[list(reference_dates)]
                    latest = pd.Timestamp(ordered["end_date"].max())
                    if latest >= signal_date:
                        future_violations += 1
                    latest_used_end_date = (
                        latest
                        if latest_used_end_date is None
                        else max(latest_used_end_date, latest)
                    )
                    score = _net_sharpe(
                        ordered["net_return"].to_numpy(dtype=float), periods_per_year
                    )
                    candidate_scores.append(
                        {
                            "factor_name": name,
                            "eligible": bool(np.isfinite(score)),
                            "reason": None if np.isfinite(score) else "non_finite_score",
                            "observations": int(len(ordered)),
                            "score": round(float(score), 8)
                            if np.isfinite(score)
                            else None,
                            "registry_index": registry_index,
                        }
                    )
            eligible = [row for row in candidate_scores if row["eligible"]]
            fallback_reason: str | None = None
            previous_factors = selected_factors
            if not eligible:
                selected_factors = (control_factor,)
                fallback_reason = "insufficient_matured_history"
            else:
                eligible.sort(
                    key=lambda row: (-float(row["score"]), int(row["registry_index"]))
                )
                control_row = next(
                    (row for row in eligible if row["factor_name"] == control_factor),
                    None,
                )
                if control_row is None:
                    selected_factors = (control_factor,)
                    fallback_reason = "control_history_incomplete"
                else:
                    control_score = float(control_row["score"])
                    qualified = [
                        row
                        for row in eligible
                        if row["factor_name"] == control_factor
                        or float(row["score"])
                        >= control_score + selector.control_score_guard
                    ]
                    qualified.sort(
                        key=lambda row: (
                            -float(row["score"]),
                            int(row["registry_index"]),
                        )
                    )
                    selected_factors = tuple(
                        str(row["factor_name"])
                        for row in qualified[: selector.selection_count]
                    )
                    if selected_factors == (control_factor,) and str(
                        eligible[0]["factor_name"]
                    ) != control_factor:
                        fallback_reason = "leader_below_control_guard"
            selected = selected_factors[0]
            selected_weights = {
                name: 1.0 / len(selected_factors) for name in selected_factors
            }
            if frozenset(selected_factors) != frozenset(previous_factors):
                switch_count += 1
            last_update_position = position
            update = {
                "decision_date": signal_date.date().isoformat(),
                "lookback_start": lookback_start.date().isoformat(),
                "latest_used_end_date": latest_used_end_date.date().isoformat()
                if latest_used_end_date is not None
                else None,
                "reference_observations": int(len(reference_dates)),
                "selected_factor": selected,
                "selected_factors": list(selected_factors),
                "selected_weights": selected_weights,
                "previous_factor": previous_factors[0],
                "previous_factors": list(previous_factors),
                "fallback_reason": fallback_reason,
                "candidate_scores": [
                    {key: value for key, value in row.items() if key != "registry_index"}
                    for row in candidate_scores
                ],
            }
            updates.append(update)
        selections.append(
            {
                "signal_date": signal_date.date().isoformat(),
                "selected_factor": selected_factors[0],
                "selected_factors": list(selected_factors),
                "selected_weights": {
                    name: 1.0 / len(selected_factors)
                    for name in selected_factors
                },
                "updated": should_update,
                "latest_used_end_date": (
                    update.get("latest_used_end_date") if update is not None else None
                ),
            }
        )

    return {
        "history_policy": selector.history_policy,
        "candidate_registry": candidate_names,
        "control_factor": control_factor,
        "signal_date_count": len(selections),
        "update_count": len(updates),
        "switch_count": switch_count,
        "future_selection_violation_count": int(future_violations),
        "updates": updates,
        "selections": selections,
    }


def build_dynamic_signal(
    frame: pd.DataFrame,
    *,
    candidate_signals: Mapping[str, pd.Series],
    decisions: Mapping[str, Any],
    control_factor: str,
    date_column: str = "date",
) -> pd.Series:
    """Apply causal decisions and fall back to the control rank per stock."""

    if control_factor not in candidate_signals:
        raise ValueError("walk_forward candidate signals must include the control")
    if date_column not in frame.columns:
        raise ValueError(f"missing walk_forward date column: {date_column}")
    control = pd.to_numeric(candidate_signals[control_factor], errors="coerce")
    if len(control) != len(frame):
        raise ValueError("walk_forward control signal length mismatch")
    selection_by_date = {
        pd.Timestamp(row["signal_date"]): row
        for row in decisions.get("selections") or []
    }
    dates = pd.to_datetime(frame[date_column], errors="coerce")
    result = pd.Series(np.nan, index=frame.index, dtype=float, name="walk_forward")
    for date_value, index in frame.groupby(dates, sort=False).groups.items():
        decision = selection_by_date.get(pd.Timestamp(date_value)) or {
            "selected_factor": control_factor
        }
        selected = tuple(
            str(name)
            for name in (
                decision.get("selected_factors")
                or [decision.get("selected_factor") or control_factor]
            )
        )
        if not selected or len(selected) != len(set(selected)):
            raise ValueError("walk_forward selected factors must be unique and non-empty")
        configured_weights = dict(decision.get("selected_weights") or {})
        if configured_weights and set(configured_weights) != set(selected):
            raise ValueError(
                "walk_forward selected weights must cover the selected factors"
            )
        weights = np.asarray(
            [
                float(configured_weights.get(name, 1.0 / len(selected)))
                for name in selected
            ],
            dtype=float,
        )
        if (
            not np.isfinite(weights).all()
            or bool((weights < 0.0).any())
            or float(weights.sum()) <= 0.0
        ):
            raise ValueError("walk_forward selected weights must be finite and non-negative")
        weights = weights / float(weights.sum())
        blended = pd.Series(0.0, index=index, dtype=float)
        for selected_name, weight in zip(selected, weights, strict=True):
            candidate = candidate_signals.get(selected_name)
            if candidate is None or len(candidate) != len(frame):
                raise ValueError(
                    f"walk_forward selected unknown candidate: {selected_name}"
                )
            selected_values = pd.to_numeric(candidate.loc[index], errors="coerce")
            blended = blended + float(weight) * selected_values.where(
                selected_values.notna(), control.loc[index]
            )
        result.loc[index] = blended
    return result


def walk_forward_offsets(
    settings: Mapping[str, Any], rebalance_every_days: int
) -> tuple[int, ...]:
    """Require every rebalance phase in deterministic ascending order."""

    raw = settings.get("rebalance_offsets")
    if rebalance_every_days <= 0:
        raise ValueError("walk_forward rebalance interval must be positive")
    if raw is None:
        offsets = tuple(range(rebalance_every_days))
    else:
        if not isinstance(raw, (list, tuple)) or any(
            isinstance(value, (bool, np.bool_)) for value in raw
        ):
            raise ValueError(
                "walk_forward rebalance_offsets must list every offset in ascending order"
            )
        try:
            offsets = tuple(int(value) for value in raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "walk_forward rebalance_offsets must list every offset in ascending order"
            ) from exc
    expected = tuple(range(rebalance_every_days))
    if offsets != expected:
        raise ValueError(
            "walk_forward rebalance_offsets must list every offset in ascending order"
        )
    return offsets


def phase_distribution(
    values: Sequence[float], quantile: float
) -> dict[str, float]:
    """Summarize correlated phase paths without selecting the best phase."""

    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if not len(array):
        return {}
    return {
        "worst": round(float(np.min(array)), 8),
        "q20": round(float(np.quantile(array, quantile)), 8),
        "median": round(float(np.median(array)), 8),
        "best": round(float(np.max(array)), 8),
        "iqr": round(
            float(np.quantile(array, 0.75) - np.quantile(array, 0.25)), 8
        ),
    }


def walk_forward_phase_rankings(
    metrics_by_strategy: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    control_factor: str,
    dynamic_factor: str,
    phase_quantile: float,
    benchmark_return_coverage_minimum: float = 0.95,
) -> list[dict[str, Any]]:
    """Rank strategies by fixed Q20 phase metrics, never by best offset."""

    if not 0.0 <= float(benchmark_return_coverage_minimum) <= 1.0:
        raise ValueError("benchmark_return_coverage_minimum must be in [0, 1]")

    metric_names = (
        "net_annual_return",
        "net_sharpe",
        "information_ratio",
        "max_drawdown",
    )
    weights = {
        "net_annual_return": 0.50,
        "net_sharpe": 0.25,
        "information_ratio": 0.15,
        "max_drawdown": 0.10,
    }
    control_metrics = list(metrics_by_strategy[control_factor])
    expected_offset_count = len(control_metrics)

    def finite_number(value: Any) -> float | None:
        if isinstance(value, (bool, np.bool_)):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if np.isfinite(parsed) else None

    def audit_offsets(
        offset_metrics: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], tuple[int, ...]]:
        failures: list[dict[str, Any]] = []
        observed_offset_count = len(offset_metrics)
        if expected_offset_count == 0:
            failures.append(
                {
                    "reason": "control_has_no_configured_offsets",
                    "rebalance_offset_days": None,
                    "expected_offset_count": 0,
                    "observed_offset_count": observed_offset_count,
                }
            )
        for offset in range(observed_offset_count, expected_offset_count):
            failures.append(
                {
                    "reason": "missing_configured_offset",
                    "rebalance_offset_days": offset,
                    "expected_offset_count": expected_offset_count,
                    "observed_offset_count": observed_offset_count,
                }
            )
        for offset in range(expected_offset_count, observed_offset_count):
            failures.append(
                {
                    "reason": "unexpected_configured_offset",
                    "rebalance_offset_days": offset,
                    "expected_offset_count": expected_offset_count,
                    "observed_offset_count": observed_offset_count,
                }
            )

        complete_offsets: list[int] = []
        for offset, metrics in enumerate(offset_metrics[:expected_offset_count]):
            offset_failures: list[dict[str, Any]] = []
            if metrics.get("equal_aum_account_audit_valid") is not True:
                offset_failures.append(
                    {
                        "reason": "equal_aum_scoring_account_audit_failed",
                        "rebalance_offset_days": offset,
                        "observed_valid": metrics.get(
                            "equal_aum_account_audit_valid"
                        ),
                        "audit_reasons": list(
                            metrics.get("equal_aum_account_audit_reasons") or []
                        ),
                        "common_window_execution_integrity": list(
                            metrics.get(
                                "equal_aum_account_execution_integrity"
                            )
                            or []
                        ),
                    }
                )
            if metrics.get("daily_nav_path_complete") is not True:
                offset_failures.append(
                    {
                        "reason": "common_interval_daily_nav_path_incomplete",
                        "rebalance_offset_days": offset,
                        "observed_daily_nav_path_complete": metrics.get(
                            "daily_nav_path_complete"
                        ),
                        "observed_daily_nav_observations": finite_number(
                            metrics.get("daily_nav_observations")
                        ),
                        "required_daily_nav_path_complete": True,
                    }
                )
            coverage = finite_number(metrics.get("period_coverage"))
            if coverage != 1.0:
                offset_failures.append(
                    {
                        "reason": "common_interval_period_coverage_not_one",
                        "rebalance_offset_days": offset,
                        "observed_period_coverage": round(coverage, 8)
                        if coverage is not None
                        else None,
                        "required_period_coverage": 1.0,
                    }
                )
            observations = finite_number(metrics.get("observations"))
            if observations is None or observations <= 0.0:
                offset_failures.append(
                    {
                        "reason": "common_interval_observations_not_positive",
                        "rebalance_offset_days": offset,
                        "observed_observations": observations,
                        "required_observations_min_exclusive": 0.0,
                    }
                )
            benchmark_coverage = finite_number(
                metrics.get("benchmark_return_coverage_min")
            )
            if (
                benchmark_coverage is None
                or benchmark_coverage < benchmark_return_coverage_minimum
            ):
                offset_failures.append(
                    {
                        "reason": "common_interval_benchmark_coverage_below_minimum",
                        "rebalance_offset_days": offset,
                        "observed_benchmark_return_coverage_min": (
                            round(benchmark_coverage, 8)
                            if benchmark_coverage is not None
                            else None
                        ),
                        "required_benchmark_return_coverage_min": round(
                            float(benchmark_return_coverage_minimum), 8
                        ),
                    }
                )
            for metric_name in metric_names:
                if (
                    metric_name == "max_drawdown"
                    and metrics.get("daily_nav_path_complete") is not True
                ):
                    continue
                if finite_number(metrics.get(metric_name)) is None:
                    offset_failures.append(
                        {
                            "reason": "common_interval_ranking_metric_not_finite",
                            "rebalance_offset_days": offset,
                            "metric_name": metric_name,
                            "observed_value": None,
                        }
                    )
            failures.extend(offset_failures)
            if not offset_failures:
                complete_offsets.append(offset)
        return failures, tuple(complete_offsets)

    offset_audits = {
        strategy_name: audit_offsets(list(offset_metrics))
        for strategy_name, offset_metrics in metrics_by_strategy.items()
    }
    control_complete_offsets = set(offset_audits[control_factor][1])
    rows: list[dict[str, Any]] = []
    for strategy_name, offset_metrics in metrics_by_strategy.items():
        offset_metrics = list(offset_metrics)
        offset_failures, complete_offsets = offset_audits[strategy_name]
        common_complete_offsets = tuple(
            offset
            for offset in complete_offsets
            if offset in control_complete_offsets
        )
        distributions = {
            name: phase_distribution(
                [
                    float(offset_metrics[offset][name])
                    for offset in complete_offsets
                ],
                phase_quantile,
            )
            for name in metric_names
        }
        deltas = {
            name: [
                float(offset_metrics[offset][name])
                - float(control_metrics[offset][name])
                for offset in common_complete_offsets
            ]
            for name in metric_names
        }
        rows.append(
            {
                "strategy_name": strategy_name,
                "strategy_kind": (
                    "walk_forward_dynamic"
                    if strategy_name == dynamic_factor
                    else "control"
                    if strategy_name == control_factor
                    else "static_candidate"
                ),
                "offset_count": len(offset_metrics),
                "expected_offset_count": expected_offset_count,
                "complete_rebalance_offsets": list(complete_offsets),
                "common_complete_rebalance_offsets": list(common_complete_offsets),
                "phase_metrics": distributions,
                "phase_deltas_vs_control": {
                    name: phase_distribution(values, phase_quantile)
                    for name, values in deltas.items()
                },
                "positive_annual_return_delta_ratio": round(
                    float(np.mean(np.asarray(deltas["net_annual_return"]) > 0.0)), 8
                )
                if deltas["net_annual_return"]
                else 0.0,
                "phase_ranking_eligible": not offset_failures,
                "excluded_from_phase_ranking": bool(offset_failures),
                "phase_ranking_exclusion_reasons": offset_failures,
                "phase_score": None,
                "phase_score_percentiles": {},
            }
        )
    eligible_rows = [row for row in rows if row["phase_ranking_eligible"]]
    for metric_name in weights:
        values = [
            float((row["phase_metrics"].get(metric_name) or {}).get("q20", -np.inf))
            for row in eligible_rows
        ]
        percentiles = pd.Series(values, dtype=float).rank(method="average", pct=True)
        for row, percentile in zip(eligible_rows, percentiles.tolist(), strict=True):
            row["phase_score_percentiles"][metric_name] = round(
                float(percentile), 8
            )
    for row in eligible_rows:
        row["phase_score"] = round(
            sum(
                weights[name] * row["phase_score_percentiles"][name]
                for name in weights
            ),
            8,
        )
    eligible_rows.sort(
        key=lambda row: (
            -float(row["phase_score"]),
            -float(
                (row["phase_metrics"].get("net_annual_return") or {}).get(
                    "q20", -np.inf
                )
            ),
            str(row["strategy_name"]),
        )
    )
    for rank, row in enumerate(eligible_rows, start=1):
        row["rank"] = rank
    excluded_rows = [row for row in rows if row["excluded_from_phase_ranking"]]
    for row in excluded_rows:
        row["rank"] = None
    return [*eligible_rows, *excluded_rows]


__all__ = [
    "HISTORY_POLICY",
    "WalkForwardSelectorSpec",
    "build_dynamic_signal",
    "causal_candidate_decisions",
    "phase_distribution",
    "walk_forward_offsets",
    "walk_forward_phase_rankings",
]
