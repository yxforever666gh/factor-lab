from __future__ import annotations

from typing import Any

from factor_lab.harvest_data_request import build_harvest_data_request
from factor_lab.harvest_mechanism_routes import load_mechanism_routes


def _has_positive_return(oos: dict[str, Any]) -> bool:
    for key in ("best_total_return", "total_return", "best_return"):
        try:
            if float(oos.get(key, 0) or 0) > 0:
                return True
        except Exception:
            pass
    return False


def _alt_route(current: str) -> str | None:
    for rid in load_mechanism_routes().keys():
        if rid != current:
            return rid
    return None


def decide_next_research_branch(
    *,
    diagnosis: dict[str, Any],
    oos_validation: dict[str, Any],
    failure_attribution: dict[str, Any],
    route_state: dict[str, Any],
    mechanism_route: dict[str, Any],
    available_fields: set[str] | None = None,
) -> dict[str, Any]:
    blockers = set(failure_attribution.get("primary_blockers") or [])
    failures = set(diagnosis.get("failure_classes") or []) | set(oos_validation.get("reasons") or [])
    status = str(route_state.get("current_route_status") or "active")
    mid = str(mechanism_route.get("mechanism_id") or route_state.get("current_route") or "unknown")
    data_request = build_harvest_data_request(mechanism_route, available_fields)
    rationale: list[str] = []
    blocked = False
    manual = False
    expected = "produce a higher-information next experiment"
    decision = "repair_same_route"

    if status == "stop":
        decision = "stop_route"; blocked = True; manual = True; rationale.append("route_state_stop")
        expected = "avoid repeating semantically exhausted route"
    elif data_request.get("blocked"):
        decision = "data_request"; blocked = True; manual = True; rationale.append("missing_required_fields")
        expected = "identify data needed before more backtests"
    elif status == "demote" and _alt_route(mid):
        decision = "switch_route"; rationale.append("route_state_demote")
        expected = "test an alternative bounded mechanism route"
    elif "zero_cost_only_best" in blockers:
        decision = "cost_robustness_branch"; rationale.append("zero_cost_only_best")
        expected = "test whether evidence survives realistic transaction costs"
    elif ("possible_portfolio_construction_issue" in blockers or "middle_hump" in blockers):
        decision = "portfolio_construction_branch"; rationale.append("portfolio_monetization_issue")
        expected = "test whether bucket/risk construction monetizes existing signal"
    elif (_has_positive_return(oos_validation) and ("drawdown_too_high" in failures or "drawdown_below_threshold" in failures or "drawdown_concentrated_by_window" in blockers)):
        decision = "risk_reduction_branch"; rationale.append("positive_return_with_drawdown_blocker")
        expected = "test whether risk controls preserve return while reducing drawdown"
    else:
        rationale.append("default_repair_same_route")

    return {
        "schema_version": 1,
        "mechanism_id": mid,
        "decision": decision,
        "rationale": rationale,
        "blocked": blocked,
        "expected_information_gain": expected,
        "manual_approval_required": manual,
        "data_request": data_request if decision == "data_request" else None,
    }
