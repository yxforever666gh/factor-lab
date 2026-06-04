from __future__ import annotations

import math
from typing import Any


def _float_or_none(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(parsed):
        return None
    return parsed


def _risk_adjusted_return(candidate: dict[str, Any]) -> float | None:
    for key in ("risk_adjusted_return", "sharpe", "information_ratio"):
        value = _float_or_none(candidate.get(key))
        if value is not None:
            return value
    return None


def is_drawdown_safe_candidate(
    candidate: dict[str, Any],
    *,
    max_drawdown_limit: float = -0.35,
    turnover_limit: float = 0.35,
    round_trip_cost_limit: float = 0.005,
) -> bool:
    """Return True only for candidates that clear drawdown, turnover, and cost gates.

    The drawdown gate is intentionally strict: a max drawdown equal to the limit is
    still not safe for the small-institutional repair path, leaving a small buffer
    before automation can proceed.
    """

    drawdown = _float_or_none(candidate.get("max_drawdown"))
    if drawdown is None or drawdown <= float(max_drawdown_limit):
        return False

    turnover = _float_or_none(candidate.get("turnover_one_way_estimate", candidate.get("turnover_mean")))
    if turnover is not None and turnover > float(turnover_limit):
        return False

    cost = _float_or_none(candidate.get("estimated_round_trip_cost"))
    if cost is None:
        cost_bps = _float_or_none(candidate.get("cost_bps"))
        cost = cost_bps / 10000.0 * 2.0 if cost_bps is not None else None
    if cost is not None and cost > float(round_trip_cost_limit):
        return False

    return True


def _ranking_key(
    candidate: dict[str, Any],
    *,
    max_drawdown_limit: float,
    turnover_limit: float,
    round_trip_cost_limit: float,
) -> tuple[bool, float, float, float, float]:
    safe = is_drawdown_safe_candidate(
        candidate,
        max_drawdown_limit=max_drawdown_limit,
        turnover_limit=turnover_limit,
        round_trip_cost_limit=round_trip_cost_limit,
    )
    risk_return = _risk_adjusted_return(candidate)
    turnover = _float_or_none(candidate.get("turnover_one_way_estimate", candidate.get("turnover_mean")))
    cost = _float_or_none(candidate.get("estimated_round_trip_cost"))
    drawdown = _float_or_none(candidate.get("max_drawdown"))
    return (
        safe,
        risk_return if risk_return is not None else float("-inf"),
        -(turnover if turnover is not None else float("inf")),
        -(cost if cost is not None else float("inf")),
        drawdown if drawdown is not None else float("-inf"),
    )


def rank_drawdown_repair_candidates(
    candidates: list[dict[str, Any]],
    *,
    max_drawdown_limit: float = -0.35,
    turnover_limit: float = 0.35,
    round_trip_cost_limit: float = 0.005,
) -> list[dict[str, Any]]:
    return sorted(
        [candidate for candidate in candidates if isinstance(candidate, dict)],
        key=lambda candidate: _ranking_key(
            candidate,
            max_drawdown_limit=max_drawdown_limit,
            turnover_limit=turnover_limit,
            round_trip_cost_limit=round_trip_cost_limit,
        ),
        reverse=True,
    )


def build_drawdown_repair_result(
    candidates: list[dict[str, Any]],
    *,
    max_drawdown_limit: float = -0.35,
    turnover_limit: float = 0.35,
    round_trip_cost_limit: float = 0.005,
) -> dict[str, Any]:
    ranked = rank_drawdown_repair_candidates(
        candidates,
        max_drawdown_limit=max_drawdown_limit,
        turnover_limit=turnover_limit,
        round_trip_cost_limit=round_trip_cost_limit,
    )
    safe = [
        candidate
        for candidate in ranked
        if is_drawdown_safe_candidate(
            candidate,
            max_drawdown_limit=max_drawdown_limit,
            turnover_limit=turnover_limit,
            round_trip_cost_limit=round_trip_cost_limit,
        )
    ]
    drawdowns = [_float_or_none(candidate.get("max_drawdown")) for candidate in candidates if isinstance(candidate, dict)]
    valid_drawdowns = [value for value in drawdowns if value is not None]
    best_available = round(max(valid_drawdowns), 6) if valid_drawdowns else None
    gap = round(float(max_drawdown_limit) - best_available, 6) if best_available is not None else None
    return {
        "repair_status": "candidate_found" if safe else "blocked_no_drawdown_safe_candidate",
        "candidate_count": len(safe),
        "recommended_candidate": safe[0] if safe else None,
        "ranked_candidates": ranked,
        "best_available_max_drawdown": best_available,
        "drawdown_gap_to_limit": gap,
        "automation_allowed": False,
        "thresholds": {
            "max_drawdown_limit": float(max_drawdown_limit),
            "turnover_limit": float(turnover_limit),
            "round_trip_cost_limit": float(round_trip_cost_limit),
        },
    }
