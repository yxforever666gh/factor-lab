from __future__ import annotations

from typing import Any

DEFAULT_SAFETY = {
    "no_timer": True,
    "no_daemon": True,
    "no_live_trading": True,
    "no_automatic_promotion": True,
}

DEFAULT_CONSTRAINTS = {
    "max_next_backtests": 120,
    "allowed_cost_bps": [30, 60],
    "allowed_holding_counts": [75, 100],
    "require_non_duplicate_semantic_hash": True,
    "require_expected_information_gain_min": "medium",
}


def _base_decision(config: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = config or {}
    constraints = dict(DEFAULT_CONSTRAINTS)
    if "max_next_backtests" in cfg:
        constraints["max_next_backtests"] = int(cfg["max_next_backtests"])
    return {
        "schema_version": 1,
        "strategy_decision": "continue_with_constraints",
        "plan_status": "planned",
        "reason_codes": [],
        "allowed_branches": ["repair_same_route", "risk_reduction_branch", "portfolio_construction_branch", "cost_robustness_branch"],
        "blocked_branches": [],
        "route_action": {"type": "continue_with_constraints"},
        "experiment_constraints": constraints,
        "manual_approval_required": False,
        "safety": dict(DEFAULT_SAFETY),
    }


def _reason_codes(evidence: dict[str, Any]) -> list[str]:
    loop = evidence.get("loop_analysis") or {}
    codes = list(loop.get("reason_codes") or [])
    seen: set[str] = set()
    return [str(c) for c in codes if c and not (str(c) in seen or seen.add(str(c)))]


def _has_repeated_failures(evidence: dict[str, Any], reason_codes: list[str]) -> bool:
    if "repeated_oos_failures" in reason_codes:
        return True
    cycles = evidence.get("cycles") or []
    return sum(1 for c in cycles if c.get("oos_class") == "fail") >= 2


def decide_strategy(evidence: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    decision = _base_decision(config)
    reason_codes = _reason_codes(evidence)

    current_route_status = str(evidence.get("current_route_status") or "").lower()
    if current_route_status in {"stop", "stopped", "exhausted", "route_exhausted"}:
        decision.update({
            "strategy_decision": "stop_route",
            "plan_status": "stopped",
            "reason_codes": ["route_stopped"],
            "allowed_branches": [],
            "blocked_branches": ["repair_same_route", "portfolio_construction_branch", "cost_robustness_branch", "risk_reduction_branch"],
            "route_action": {"type": "stop_route"},
            "manual_approval_required": True,
        })
        return decision

    missing_fields = list(evidence.get("missing_required_fields") or [])
    if missing_fields:
        decision.update({
            "strategy_decision": "request_data",
            "plan_status": "blocked",
            "reason_codes": ["missing_required_fields"],
            "allowed_branches": [],
            "blocked_branches": [],
            "route_action": {"type": "request_data"},
            "manual_approval_required": True,
            "data_request": {
                "missing_fields": missing_fields,
                "reason": "current primitives cannot test requested mechanism without these fields",
            },
        })
        return decision

    ready_alternatives = list(evidence.get("ready_alternative_routes") or [])
    if ready_alternatives and _has_repeated_failures(evidence, reason_codes):
        mechanism_id = str(ready_alternatives[0])
        decision.update({
            "strategy_decision": "switch_mechanism_route",
            "reason_codes": reason_codes or ["repeated_oos_failures"],
            "allowed_branches": ["repair_same_route", "risk_reduction_branch"],
            "blocked_branches": list((evidence.get("loop_analysis") or {}).get("blocked_branches") or []),
            "route_action": {"type": "switch_mechanism_route", "mechanism_id": mechanism_id},
        })
        return decision

    if reason_codes:
        blocked = list((evidence.get("loop_analysis") or {}).get("blocked_branches") or [])
        allowed = ["risk_reduction_branch"] if "drawdown_not_improving" in reason_codes else ["repair_same_route", "risk_reduction_branch"]
        if "branch_loop_detected" in reason_codes or "semantic_repeat_limit_reached" in reason_codes or "drawdown_not_improving" in reason_codes:
            strategy = "shrink_search_space"
        else:
            strategy = "continue_with_constraints"
        decision.update({
            "strategy_decision": strategy,
            "reason_codes": reason_codes,
            "allowed_branches": allowed,
            "blocked_branches": sorted(set(str(b) for b in blocked)),
            "route_action": {"type": "continue_with_constraints"},
        })
        return decision

    decision["reason_codes"] = ["no_strategy_blocker_detected"]
    return decision
