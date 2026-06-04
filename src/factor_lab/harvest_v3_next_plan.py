from __future__ import annotations

from typing import Any


def _next_id(cycle_id: str) -> str:
    try:
        return f"cycle_{int(str(cycle_id).split('_')[-1]) + 1:04d}"
    except Exception:
        return "cycle_next"


def build_v3_next_cycle_plan(
    *,
    current_cycle_id: str,
    diagnosis: dict[str, Any],
    oos_validation: dict[str, Any],
    failure_attribution: dict[str, Any],
    route_state: dict[str, Any],
    research_decision: dict[str, Any],
    portfolio_branch_plan: dict[str, Any] | None = None,
    data_request: dict[str, Any] | None = None,
) -> dict[str, Any]:
    branch = str(research_decision.get("decision") or "repair_same_route")
    if branch == "data_request":
        status = "blocked"
    elif branch == "stop_route":
        status = "stopped"
    else:
        status = "planned"
    portfolio_branch_plan = portfolio_branch_plan or {}
    data_request = data_request or {}
    experiments: list[dict[str, Any]] = []
    for action in portfolio_branch_plan.get("actions") or []:
        experiments.append({"type": "action", "action": action})
    for construction in portfolio_branch_plan.get("portfolio_construction") or []:
        experiments.append({"type": "portfolio_construction", "config": construction})
    if not experiments and status == "planned":
        experiments.append({"type": branch, "description": "bounded branch-specific Harvest experiment"})
    return {
        "schema_version": 1,
        "cycle_id": _next_id(current_cycle_id),
        "based_on_cycle": current_cycle_id,
        "plan_status": status,
        "branch": branch,
        "rationale": list(research_decision.get("rationale") or []),
        "experiments": experiments,
        "expected_information_gain": research_decision.get("expected_information_gain"),
        "stop_conditions": ["oos_class remains fail", "semantic duplicate detected", "manual review required"],
        "success_criteria": {"sharpe_min": 0.7, "max_drawdown_min": -0.35, "manual_review_required": True},
        "manual_approval_required": bool(research_decision.get("manual_approval_required")),
        "data_request": data_request,
        "route_state_status": route_state.get("current_route_status"),
        "oos_class": oos_validation.get("oos_class"),
        "failure_blockers": failure_attribution.get("primary_blockers") or diagnosis.get("failure_classes") or [],
    }
