from __future__ import annotations

from typing import Any


def materialize_v3_next_plan(
    v3_plan: dict[str, Any],
    *,
    next_cycle_id: str,
    dataset_path: str,
    mechanism_route: dict[str, Any] | None = None,
    strategy_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if v3_plan.get("plan_status") != "planned":
        raise ValueError("v3 next cycle plan is not planned")

    actions: list[dict[str, Any]] = []
    portfolio_construction: list[dict[str, Any]] = []
    for item in v3_plan.get("experiments") or []:
        if item.get("type") == "action":
            action = dict(item.get("action") or {})
            if action:
                actions.append(action)
        elif item.get("type") == "portfolio_construction":
            config = dict(item.get("config") or {})
            if config:
                portfolio_construction.append(config)

    if not actions and not portfolio_construction:
        branch = str(v3_plan.get("branch") or "repair_same_route")
        if branch == "cost_robustness_branch":
            actions.append({"type": "restrict_costs", "cost_bps_values": [30, 60]})
        else:
            actions.append({"type": "restrict_costs", "cost_bps_values": [30, 60]})

    effective_branch = str(v3_plan.get("branch") or "repair_same_route")
    controller_constraints: dict[str, Any] = {}
    if strategy_plan:
        constraints = strategy_plan.get("experiment_constraints") or {}
        allowed_costs = set(constraints.get("allowed_cost_bps") or [])
        allowed_holding_counts = set(constraints.get("allowed_holding_counts") or [])
        for action in actions:
            if action.get("type") == "restrict_costs" and allowed_costs:
                action["cost_bps_values"] = [v for v in action.get("cost_bps_values", []) if v in allowed_costs]
            if action.get("type") == "set_holding_counts" and allowed_holding_counts:
                action["holding_counts"] = [v for v in action.get("holding_counts", []) if v in allowed_holding_counts]
        for key in ["max_next_backtests", "require_non_duplicate_semantic_hash", "require_expected_information_gain_min"]:
            if key in constraints:
                controller_constraints[key] = constraints[key]
        controller_constraints["allowed_branches"] = list(strategy_plan.get("allowed_branches") or [])
        controller_constraints["blocked_branches"] = list(strategy_plan.get("blocked_branches") or [])
        allowed_branches = controller_constraints["allowed_branches"]
        blocked_branches = set(controller_constraints["blocked_branches"])
        if effective_branch in blocked_branches or (allowed_branches and effective_branch not in allowed_branches):
            effective_branch = str(allowed_branches[0]) if allowed_branches else "manual_review"

    plan = {
        "schema_version": 2,
        "cycle_id": next_cycle_id,
        "based_on_cycle": v3_plan.get("based_on_cycle"),
        "plan_status": "planned",
        "objective": f"v4 materialized {effective_branch} branch",
        "dataset_path": dataset_path,
        "mechanism_route": mechanism_route or {},
        "actions": actions,
        "portfolio_construction": portfolio_construction,
        "research_decision": {
            "decision": effective_branch,
            "source_v3_decision": v3_plan.get("branch") or "repair_same_route",
            "rationale": list(v3_plan.get("rationale") or []),
            "expected_information_gain": v3_plan.get("expected_information_gain"),
        },
        "executable": True,
        "success_criteria": v3_plan.get("success_criteria") or {
            "sharpe_min": 0.7,
            "max_drawdown_min": -0.35,
            "positive_at_cost_bps": 30,
            "min_ok_windows": 2,
        },
    }
    if strategy_plan:
        plan["strategy_plan_id"] = strategy_plan.get("strategy_run_id")
        plan["controller_constraints"] = controller_constraints
    return plan
