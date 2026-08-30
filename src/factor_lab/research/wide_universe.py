"""Pre-registered 6.2 widened-opportunity-set target and evidence helpers.

The module deliberately contains no raw-data I/O.  It consumes an already
audited Top-25 ranking trace, produces one independent low-churn state per
absolute-calendar sleeve, and summarizes exact execution-kernel results.  The
selection runner can therefore keep train, validation, winner freeze, and
audit as physically separate stages.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.strategy import LowChurnStrategyConfig, select_low_churn_targets


CONTROL_ID = "daily_adv20_top500_control"
CHALLENGER_IDS = ("daily_adv20_ge_100m", "daily_adv20_top1500")
UNIVERSE_IDS = (CONTROL_ID, *CHALLENGER_IDS)
FORBIDDEN_COLUMN_TOKENS = ("forward", "label", "future", "outcome", "return_after")
PERIODS_PER_YEAR = 25.2
CAPACITY_RECONCILIATION_ABS_TOL_RMB = 1e-6


@dataclass(frozen=True)
class PhaseBounds:
    """Inclusive signal start and inclusive exact outcome-end boundary."""

    start: pd.Timestamp
    end: pd.Timestamp

    @classmethod
    def from_values(cls, start: Any, end: Any) -> "PhaseBounds":
        left = pd.Timestamp(start).normalize()
        right = pd.Timestamp(end).normalize()
        if right < left:
            raise ValueError("phase end must not precede phase start")
        return cls(start=left, end=right)


def canonical_sha256(value: Any) -> str:
    """Return the deterministic digest used by 6.2 manifests and evidence."""

    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _normalized_calendar(values: Sequence[Any]) -> tuple[pd.Timestamp, ...]:
    calendar = tuple(pd.Timestamp(value).normalize() for value in values)
    if not calendar:
        raise ValueError("official calendar must not be empty")
    if list(calendar) != sorted(calendar) or len(set(calendar)) != len(calendar):
        raise ValueError("official calendar must be unique and strictly increasing")
    return calendar


def audit_rankings(
    rankings: pd.DataFrame,
    calendar: Sequence[Any],
    *,
    expected_universes: Sequence[str] = UNIVERSE_IDS,
    top_n: int = 25,
    anchor_date: Any = "2017-01-03",
) -> pd.DataFrame:
    """Validate and normalize the physically truncated Top-25 trace."""

    if not isinstance(rankings, pd.DataFrame) or rankings.empty:
        raise ValueError("rankings must be a non-empty DataFrame")
    forbidden = sorted(
        column
        for column in rankings.columns
        if any(token in str(column).casefold() for token in FORBIDDEN_COLUMN_TOKENS)
    )
    if forbidden:
        raise ValueError(f"ranking loader rejects forbidden columns: {forbidden}")
    required = {"candidate_id", "date", "ticker", "rank", "score"}
    missing = sorted(required - set(rankings.columns))
    if missing:
        raise ValueError(f"rankings missing columns: {missing}")

    official = _normalized_calendar(calendar)
    official_set = set(official)
    anchor = pd.Timestamp(anchor_date).normalize()
    if anchor not in official_set:
        raise ValueError("official calendar does not contain the frozen anchor")
    work = rankings.copy()
    work["candidate_id"] = work["candidate_id"].astype(str)
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    work["ticker"] = work["ticker"].astype(str).str.strip()
    work["rank"] = pd.to_numeric(work["rank"], errors="coerce")
    work["score"] = pd.to_numeric(work["score"], errors="coerce")
    if (
        work["date"].isna().any()
        or work["ticker"].eq("").any()
        or work["rank"].isna().any()
        or not np.isfinite(work["score"]).all()
    ):
        raise ValueError("rankings contain invalid identities, ranks, or scores")
    if not work["date"].isin(official_set).all():
        raise ValueError("rankings contain a non-official session")
    if bool(work["date"].lt(anchor).any()):
        raise ValueError("rankings precede the frozen anchor")
    if work.duplicated(["candidate_id", "date", "ticker"]).any():
        raise ValueError("rankings contain duplicate candidate/date/ticker rows")
    if work.duplicated(["candidate_id", "date", "rank"]).any():
        raise ValueError("rankings contain duplicate candidate/date/rank rows")

    expected = tuple(map(str, expected_universes))
    actual = tuple(sorted(work["candidate_id"].unique()))
    if actual != tuple(sorted(expected)):
        raise ValueError(f"unexpected candidate ids: {actual}")
    work["rank"] = work["rank"].astype(int)
    if bool(work["rank"].lt(1).any()) or bool(work["rank"].gt(int(top_n)).any()):
        raise ValueError("ranking values fall outside the frozen Top-N trace")

    dates_by_candidate: dict[str, tuple[pd.Timestamp, ...]] = {}
    for candidate_id, candidate in work.groupby("candidate_id", sort=True):
        candidate_dates = tuple(sorted(candidate["date"].unique()))
        dates_by_candidate[str(candidate_id)] = candidate_dates
        for signal_date, day in candidate.groupby("date", sort=True):
            ordered = day.sort_values("rank", kind="mergesort")
            expected_ranks = list(range(1, int(top_n) + 1))
            if ordered["rank"].tolist() != expected_ranks:
                raise ValueError(
                    f"{candidate_id}/{pd.Timestamp(signal_date).date()} lacks exact Top-{top_n}"
                )
            expected_order = day.sort_values(
                ["score", "ticker"],
                ascending=[False, True],
                kind="mergesort",
            )["ticker"].tolist()
            if ordered["ticker"].tolist() != expected_order:
                raise ValueError("rank trace violates score-desc/ticker-asc ordering")
    if len(set(dates_by_candidate.values())) != 1:
        raise ValueError("all candidate universes must share exact signal dates")
    signal_dates = next(iter(dates_by_candidate.values()))
    if not signal_dates or signal_dates[0] != anchor:
        raise ValueError("ranking trace must begin on the frozen anchor")
    first = official.index(anchor)
    last = official.index(signal_dates[-1])
    if signal_dates != official[first : last + 1]:
        raise ValueError("ranking trace has an official-calendar gap")
    return work.sort_values(
        ["candidate_id", "date", "rank"], kind="mergesort"
    ).reset_index(drop=True)


def build_target_decisions(
    rankings: pd.DataFrame,
    calendar: Sequence[Any],
    config: LowChurnStrategyConfig | None = None,
    *,
    expected_universes: Sequence[str] = UNIVERSE_IDS,
) -> list[dict[str, Any]]:
    """Create deterministic Top10/exit25 targets for ten independent sleeves."""

    cfg = config or LowChurnStrategyConfig(retention_exit_rank=25)
    if (
        int(cfg.position_count) != 10
        or int(cfg.retention_exit_rank) != 25
        or int(cfg.sleeve_count) != 10
        or not math.isclose(float(cfg.position_weight), 0.1)
    ):
        raise ValueError("6.2 target construction requires Top10/exit25/10 sleeves/0.1")
    official = _normalized_calendar(calendar)
    work = audit_rankings(
        rankings,
        official,
        expected_universes=expected_universes,
        top_n=int(cfg.retention_exit_rank),
        anchor_date=cfg.anchor_date,
    )
    ranking_map = {
        (str(candidate_id), pd.Timestamp(date).normalize()): tuple(
            day.sort_values("rank", kind="mergesort")["ticker"].astype(str)
        )
        for (candidate_id, date), day in work.groupby(
            ["candidate_id", "date"], sort=False
        )
    }
    last_signal = work["date"].max()
    anchor = pd.Timestamp(cfg.anchor_date).normalize()
    anchor_index = official.index(anchor)
    states: dict[tuple[str, int], dict[str, float]] = {
        (str(candidate_id), sleeve): {}
        for candidate_id in expected_universes
        for sleeve in range(int(cfg.sleeve_count))
    }
    decisions: list[dict[str, Any]] = []
    for calendar_index in range(anchor_index, official.index(last_signal) + 1):
        signal_date = official[calendar_index]
        sleeve = (calendar_index - anchor_index) % int(cfg.sleeve_count)
        for candidate_id in expected_universes:
            key = (str(candidate_id), signal_date)
            if key not in ranking_map:
                raise ValueError(f"missing ranking at {candidate_id}/{signal_date.date()}")
            state_key = (str(candidate_id), sleeve)
            targets = select_low_churn_targets(
                ranking_map[key], states[state_key], cfg
            )
            if len(targets) != int(cfg.position_count):
                raise ValueError("a scheduled decision does not contain exact Top10 targets")
            states[state_key] = dict(targets)
            decisions.append(
                {
                    "candidate_id": str(candidate_id),
                    "sleeve": sleeve,
                    "calendar_index": calendar_index,
                    "signal_date": signal_date.date().isoformat(),
                    "ranking_sha256": canonical_sha256(ranking_map[key]),
                    "target_weights": dict(sorted(targets.items())),
                }
            )
    return decisions


def decisions_frame(decisions: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    """Return a long, Parquet-friendly target artifact."""

    rows: list[dict[str, Any]] = []
    for raw in decisions:
        decision = dict(raw)
        weights = decision.pop("target_weights", None)
        if not isinstance(weights, Mapping) or not weights:
            raise ValueError("decision target_weights must be a non-empty mapping")
        for ticker, weight in sorted(weights.items()):
            rows.append(
                {
                    **decision,
                    "ticker": str(ticker),
                    "target_weight": float(weight),
                }
            )
    output = pd.DataFrame(rows)
    if output.empty or output.duplicated(
        ["candidate_id", "sleeve", "signal_date", "ticker"]
    ).any():
        raise ValueError("target decisions are empty or duplicated")
    return output.sort_values(
        ["candidate_id", "sleeve", "signal_date", "ticker"], kind="mergesort"
    ).reset_index(drop=True)


def target_maps(
    decisions: Sequence[Mapping[str, Any]], candidate_id: str, sleeve: int
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, Any]]]:
    """Return exact-kernel target and audit maps for one candidate/offset."""

    selected = [
        dict(row)
        for row in decisions
        if str(row.get("candidate_id")) == str(candidate_id)
        and int(row.get("sleeve", -1)) == int(sleeve)
    ]
    if not selected:
        raise ValueError(f"no decisions for {candidate_id}/offset{sleeve}")
    targets: dict[str, dict[str, float]] = {}
    audits: dict[str, dict[str, Any]] = {}
    for row in selected:
        signal_date = str(row["signal_date"])
        if signal_date in targets:
            raise ValueError("duplicate target decision date")
        weights = {str(k): float(v) for k, v in dict(row["target_weights"]).items()}
        targets[signal_date] = weights
        audits[signal_date] = {
            "promotion_eligible": True,
            "generator": "factor_lab.research.wide_universe.build_target_decisions",
            "candidate_id": str(candidate_id),
            "sleeve": int(sleeve),
            "calendar_index": int(row["calendar_index"]),
            "ranking_sha256": str(row["ranking_sha256"]),
        }
    return targets, audits


def annualized_return(values: Iterable[float]) -> float:
    array = np.asarray(list(values), dtype=float)
    if array.size == 0:
        return 0.0
    terminal = float(np.prod(1.0 + array))
    return -1.0 if terminal <= 0.0 else terminal ** (PERIODS_PER_YEAR / array.size) - 1.0


def annualized_ratio(values: Iterable[float]) -> float:
    array = np.asarray(list(values), dtype=float)
    if array.size < 2:
        return 0.0
    deviation = float(np.std(array, ddof=1))
    if deviation <= 0.0 or not math.isfinite(deviation):
        return 0.0
    return float(np.mean(array) / deviation * math.sqrt(PERIODS_PER_YEAR))


def _daily_drawdown(result: Any, periods: Sequence[Mapping[str, Any]]) -> float:
    if not periods:
        return 0.0
    start = int(periods[0]["account_nav_path_start_sequence"])
    end = int(periods[-1]["account_nav_path_end_sequence"])
    nav = np.asarray(
        [
            float(row["nav"])
            for row in result.account_nav_path
            if start <= int(row["sequence"]) <= end
        ],
        dtype=float,
    )
    if nav.size == 0 or not np.isfinite(nav).all() or bool(np.any(nav <= 0.0)):
        raise ValueError("phase NAV path is missing, non-finite, or non-positive")
    return float(np.min(nav / np.maximum.accumulate(nav) - 1.0))


def capacity_metrics(
    trades: Sequence[Mapping[str, Any]],
    periods: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Aggregate requested/executed notionals as ratio-of-sums.

    Both buys and sells, and both executed and blocked orders, enter the
    denominator.  ``capacity_limited`` is the kernel's full-precision boolean;
    it is never reverse engineered from the serialized ADV/notional values.
    """

    start_dates = {str(row["start_date"]) for row in periods}
    selected = [row for row in trades if str(row.get("date")) in start_dates]

    def aggregate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        requested_values: list[float] = []
        limited_values: list[float] = []
        executed_values: list[float] = []
        for row in rows:
            side = str(row.get("side") or "")
            if side not in {"buy", "sell"}:
                raise ValueError(f"invalid execution side: {side!r}")
            try:
                request = float(row.get("requested_notional") or 0.0)
                fill = float(row.get("executed_notional") or 0.0)
            except (TypeError, ValueError) as exc:
                raise ValueError("execution notionals must be numeric") from exc
            if (
                not math.isfinite(request)
                or not math.isfinite(fill)
                or request < 0.0
                or fill < 0.0
                or fill > request + 1e-4
            ):
                raise ValueError("execution notionals violate finite long-only bounds")
            if str(row.get("status")) == "blocked" and fill > 1e-4:
                raise ValueError("blocked order has executed notional")
            requested_values.append(request)
            executed_values.append(fill)
            if bool(row.get("capacity_limited")):
                limited_values.append(request)
        requested = math.fsum(requested_values)
        limited = math.fsum(limited_values)
        executed = math.fsum(executed_values)
        return {
            "order_count": len(rows),
            "requested_notional_total": requested,
            "capacity_limited_requested_notional": limited,
            "executed_notional_total": executed,
            "capacity_limited_requested_notional_ratio": (
                limited / requested if requested > 0.0 else 0.0
            ),
            "requested_notional_fill_ratio": (
                executed / requested if requested > 0.0 else 1.0
            ),
        }

    overall = aggregate(selected)
    overall["trade_record_notional_precision_rmb_decimals"] = 4
    overall["activity_gate_passed"] = bool(
        overall["order_count"] > 0 and overall["requested_notional_total"] > 0.0
    )
    overall["by_side"] = {
        side: aggregate([row for row in selected if str(row.get("side")) == side])
        for side in ("buy", "sell")
    }
    side_requested = math.fsum(
        float(value["requested_notional_total"])
        for value in overall["by_side"].values()
    )
    side_executed = math.fsum(
        float(value["executed_notional_total"])
        for value in overall["by_side"].values()
    )
    if not math.isclose(
        side_requested,
        float(overall["requested_notional_total"]),
        rel_tol=0.0,
        abs_tol=CAPACITY_RECONCILIATION_ABS_TOL_RMB,
    ) or not math.isclose(
        side_executed,
        float(overall["executed_notional_total"]),
        rel_tol=0.0,
        abs_tol=CAPACITY_RECONCILIATION_ABS_TOL_RMB,
    ):
        raise RuntimeError("capacity by-side totals do not reconcile")
    return overall


def summarize_phase(result: Any, bounds: PhaseBounds) -> dict[str, Any]:
    """Summarize exact periods whose signal and end both fit the phase."""

    periods = [
        row
        for row in result.periods
        if pd.Timestamp(row["signal_date"]).normalize() >= bounds.start
        and pd.Timestamp(row["end_date"]).normalize() <= bounds.end
    ]
    net = [float(row["net_return"]) for row in periods]
    gross = [float(row["gross_return"]) for row in periods]
    input_required = sum(int(row["execution_input_required_count"]) for row in periods)
    input_observed = sum(int(row["execution_input_observed_count"]) for row in periods)
    capacity = capacity_metrics(result.trades, periods)
    return {
        "observations": len(periods),
        "start_signal_date": periods[0]["signal_date"] if periods else None,
        "end_signal_date": periods[-1]["signal_date"] if periods else None,
        "max_outcome_end_date": periods[-1]["end_date"] if periods else None,
        "net_cagr": annualized_return(net),
        "gross_cagr": annualized_return(gross),
        "net_sharpe": annualized_ratio(net),
        "daily_max_drawdown": _daily_drawdown(result, periods),
        "mean_turnover": (
            float(np.mean([float(row["turnover"]) for row in periods]))
            if periods
            else 0.0
        ),
        "total_cost_fraction": float(
            sum(
                float(row["costs"]["total"]) / float(row["accounting_start_nav"])
                for row in periods
            )
        ),
        "blocked_trade_count": sum(int(row["blocked_trade_count"]) for row in periods),
        "capacity_limited_count": sum(
            int(row["capacity_limited_count"]) for row in periods
        ),
        "capacity_violation_count": sum(
            int(row["capacity_violation_count"]) for row in periods
        ),
        "forced_delist_write_down_count": sum(
            int(row["forced_delist_write_down_count"]) for row in periods
        ),
        "execution_input_required_count": input_required,
        "execution_input_observed_count": input_observed,
        "execution_input_coverage": (
            input_observed / input_required if input_required else 1.0
        ),
        "execution_input_future_violation_count": sum(
            int(row["execution_input_future_violation_count"]) for row in periods
        ),
        "capacity": capacity,
        "signal_dates": [row["signal_date"] for row in periods],
        "start_dates": [row["start_date"] for row in periods],
        "outcome_end_dates": [row["end_date"] for row in periods],
        "period_returns": net,
    }


def pair_phase(candidate: Mapping[str, Any], control: Mapping[str, Any]) -> dict[str, Any]:
    """Attach an exact-date, paired relative return comparison."""

    for key in ("signal_dates", "start_dates", "outcome_end_dates"):
        if list(candidate.get(key) or []) != list(control.get(key) or []):
            raise ValueError(f"candidate/control paired schedule mismatch: {key}")
    candidate_returns = np.asarray(candidate.get("period_returns") or [], dtype=float)
    control_returns = np.asarray(control.get("period_returns") or [], dtype=float)
    if candidate_returns.shape != control_returns.shape:
        raise ValueError("candidate/control paired returns differ in length")
    if bool(np.any(1.0 + control_returns <= 0.0)):
        raise ValueError("control period return prevents relative compounding")
    relative = (1.0 + candidate_returns) / (1.0 + control_returns) - 1.0
    spread = candidate_returns - control_returns
    output = dict(candidate)
    output["relative_to_control"] = {
        "paired_relative_cagr": annualized_return(relative),
        "paired_spread_ir": annualized_ratio(spread),
        "net_cagr_delta": float(candidate["net_cagr"]) - float(control["net_cagr"]),
        "daily_max_drawdown_delta": float(candidate["daily_max_drawdown"])
        - float(control["daily_max_drawdown"]),
        "turnover_delta": float(candidate["mean_turnover"])
        - float(control["mean_turnover"]),
        "positive_period_fraction": float(np.mean(spread > 0.0)) if spread.size else 0.0,
    }
    return output


def strip_return_series(value: Mapping[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe evidence copy without selection return vectors."""

    output = json.loads(json.dumps(value, allow_nan=False, default=str))
    for key in ("signal_dates", "start_dates", "outcome_end_dates", "period_returns"):
        output.pop(key, None)
    return output


def quantiles(values: Iterable[float]) -> dict[str, float]:
    array = np.asarray(list(values), dtype=float)
    if array.size == 0 or not np.isfinite(array).all():
        raise ValueError("quantile input must be finite and non-empty")
    return {
        "q20": float(np.quantile(array, 0.2, method="linear")),
        "median": float(np.median(array)),
        "worst": float(np.min(array)),
    }


def candidate_gate(
    candidate_phases: Sequence[Mapping[str, Any]],
    control_phases: Sequence[Mapping[str, Any]],
    *,
    minimum_observations: int = 1,
) -> dict[str, Any]:
    """Apply every phase gate per offset, then the cross-offset q20/count gate."""

    if len(candidate_phases) != 10 or len(control_phases) != 10:
        raise ValueError("6.2 gates require exactly ten independent offsets")
    paired = [
        pair_phase(candidate, control)
        for candidate, control in zip(candidate_phases, control_phases)
    ]
    relative = [
        float(row["relative_to_control"]["paired_relative_cagr"])
        for row in paired
    ]
    offset_checks: list[dict[str, Any]] = []
    for offset, (candidate, control) in enumerate(zip(paired, control_phases)):
        capacity = dict(candidate["capacity"])
        checks = {
            "minimum_observations": int(candidate["observations"])
            >= int(minimum_observations),
            "daily_max_drawdown": float(candidate["daily_max_drawdown"]) >= -0.35,
            "drawdown_worsening": float(candidate["daily_max_drawdown"])
            - float(control["daily_max_drawdown"])
            >= -0.02,
            "capacity_violation_count": int(candidate["capacity_violation_count"]) == 0,
            "execution_input_future_violation_count": int(
                candidate["execution_input_future_violation_count"]
            )
            == 0,
            "execution_input_coverage": math.isclose(
                float(candidate["execution_input_coverage"]),
                1.0,
                rel_tol=0.0,
                abs_tol=1e-12,
            ),
            "capacity_limited_requested_notional_ratio": float(
                capacity["capacity_limited_requested_notional_ratio"]
            )
            <= 0.05,
            "requested_notional_fill_ratio": float(
                capacity["requested_notional_fill_ratio"]
            )
            >= 0.95,
            "activity": bool(capacity["activity_gate_passed"]),
        }
        offset_checks.append(
            {
                "offset": offset,
                "checks": checks,
                "passed": all(checks.values()),
            }
        )
    relative_quantiles = quantiles(relative)
    cross_checks = {
        "q20_paired_relative_cagr_strictly_positive": relative_quantiles["q20"]
        > 0.0,
        "positive_relative_offset_count_at_least_7": sum(value > 0.0 for value in relative)
        >= 7,
        "all_offset_execution_and_risk_gates": all(
            row["passed"] for row in offset_checks
        ),
    }
    return {
        "passed": all(cross_checks.values()),
        "checks": cross_checks,
        "offset_checks": offset_checks,
        "paired_relative_cagr": relative_quantiles,
        "positive_relative_offset_count": sum(value > 0.0 for value in relative),
        "validation_median_relative_cagr": relative_quantiles["median"],
        "worst_capacity_limited_requested_notional_ratio": max(
            float(row["capacity"]["capacity_limited_requested_notional_ratio"])
            for row in paired
        ),
        "worst_requested_notional_fill_ratio": min(
            float(row["capacity"]["requested_notional_fill_ratio"])
            for row in paired
        ),
        "paired_phases": paired,
    }


def select_winner(
    train_gates: Mapping[str, Mapping[str, Any]],
    validation_gates: Mapping[str, Mapping[str, Any]],
    *,
    turnover_by_candidate: Mapping[str, float],
) -> str | None:
    """Select at most one challenger after both frozen gates; never force one."""

    eligible = [
        candidate_id
        for candidate_id in CHALLENGER_IDS
        if bool((train_gates.get(candidate_id) or {}).get("passed"))
        and bool((validation_gates.get(candidate_id) or {}).get("passed"))
    ]
    if not eligible:
        return None

    def key(candidate_id: str) -> tuple[float, float, float, float, str]:
        train = train_gates[candidate_id]
        validation = validation_gates[candidate_id]
        min_q20 = min(
            float(train["paired_relative_cagr"]["q20"]),
            float(validation["paired_relative_cagr"]["q20"]),
        )
        return (
            -min_q20,
            -float(validation["paired_relative_cagr"]["median"]),
            float(validation["worst_capacity_limited_requested_notional_ratio"]),
            float(turnover_by_candidate[candidate_id]),
            candidate_id,
        )

    return sorted(eligible, key=key)[0]


__all__ = [
    "CAPACITY_RECONCILIATION_ABS_TOL_RMB",
    "CHALLENGER_IDS",
    "CONTROL_ID",
    "PhaseBounds",
    "UNIVERSE_IDS",
    "annualized_ratio",
    "annualized_return",
    "audit_rankings",
    "build_target_decisions",
    "candidate_gate",
    "canonical_sha256",
    "capacity_metrics",
    "decisions_frame",
    "pair_phase",
    "quantiles",
    "select_winner",
    "strip_return_series",
    "summarize_phase",
    "target_maps",
]
