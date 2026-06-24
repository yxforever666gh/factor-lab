from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

GATE_KEYS = ["live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]
SUPPORTED_BASE_CHECKS = [
    "industry_split_robustness",
    "size_split_robustness",
    "regime_split_robustness",
    "turnover_sensitivity",
    "drawdown_sensitivity",
    "cost_sensitivity_probe",
]


def _closed_gates() -> dict[str, bool]:
    return {key: False for key in GATE_KEYS}


def _risk_cost_constraints(action: str, mutation_request: dict[str, Any]) -> dict[str, Any]:
    if action == "request_more_data":
        return {}
    support = mutation_request.get("support_summary") or {}
    cost = support.get("cost") or {}
    drawdown = support.get("drawdown") or {}
    constraints: dict[str, Any] = {}
    if action in {"mutate_risk_or_cost_model", "continue_research", "mutate_conditions"}:
        constraints["liquidity_turnover_filter"] = {
            "purpose": "Reduce implementation cost and high-turnover fragility before rerunning controlled diagnostics.",
            "rule": "exclude highest turnover/cost bucket until cost-adjusted spread is positive",
            "source_cost_adjusted_mean_return": cost.get("cost_adjusted_mean_return"),
        }
        constraints["drawdown_guard"] = {
            "purpose": "Avoid buckets where tail loss dominates the observed spread.",
            "rule": "reject or separately diagnose cohorts with extreme worst_forward_return before continuation",
            "source_worst_forward_return": drawdown.get("worst_forward_return"),
        }
    if action == "add_regime_filter":
        constraints["regime_filter"] = {
            "purpose": "Keep only regimes where spread is positive before further research.",
            "rule": "derive supported market_regime subset from controlled verdict support summary",
        }
    return constraints


def _requested_checks(action: str, mutation_request: dict[str, Any]) -> list[str]:
    if action == "request_more_data":
        return []
    checks = dict(((mutation_request.get("support_summary") or {}).get("checks") or {}))
    kept = [name for name in SUPPORTED_BASE_CHECKS if name in checks or name in {"drawdown_sensitivity", "cost_sensitivity_probe"}]
    if action == "mutate_risk_or_cost_model":
        for name in ["turnover_sensitivity", "drawdown_sensitivity", "cost_sensitivity_probe"]:
            if name not in kept:
                kept.append(name)
    if action == "add_regime_filter" and "regime_split_robustness" not in kept:
        kept.append("regime_split_robustness")
    return [name for name in SUPPORTED_BASE_CHECKS if name in kept]


def build_next_iteration_from_mutation(*, run_id: str, previous_iteration_plan: dict[str, Any], mutation_request: dict[str, Any]) -> dict[str, Any]:
    action = mutation_request.get("action") or "mutate_conditions"
    plan = copy.deepcopy(previous_iteration_plan)
    plan.update(
        {
            "run_id": run_id + "_agent_iteration_plan",
            "mode": "agent_generated_iteration_plan_v2",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "mutation_parent_run_id": previous_iteration_plan.get("run_id"),
            "source_mutation_request_run_id": mutation_request.get("run_id"),
            "mutation_action": action,
            "mutation_reason_codes": mutation_request.get("reason_codes") or [],
            "risk_cost_constraints": _risk_cost_constraints(action, mutation_request),
            "production_boundaries": _closed_gates(),
        }
    )
    if action == "request_more_data":
        assumptions = dict(plan.get("data_feasibility_assumptions") or {})
        blocked_if = list(assumptions.get("blocked_if") or [])
        missing = mutation_request.get("missing_columns") or []
        blocked_if.append(f"missing_columns_required_before_execution={missing}")
        assumptions["blocked_if"] = blocked_if
        assumptions["missing_columns"] = missing
        plan["data_feasibility_assumptions"] = assumptions
    else:
        mutation_logic = dict(plan.get("mutation_logic") or {})
        allowed = list(mutation_logic.get("allowed_mutations") or [])
        for item in mutation_request.get("requested_actions") or []:
            if item not in allowed:
                allowed.append(item)
        mutation_logic["allowed_mutations"] = allowed
        mutation_logic["active_mutation"] = action
        plan["mutation_logic"] = mutation_logic
        design = dict(plan.get("controlled_research_backtest_design") or {})
        design["objective"] = "Rerun controlled diagnostics after mutation; do not advance to strategy generation."
        design["minimum_checks"] = _requested_checks(action, mutation_request)
        plan["controlled_research_backtest_design"] = design
    request = {
        "schema_version": 1,
        "run_id": run_id + "_execution_request",
        "mode": "controlled_research_execution_request_v2",
        "phenomenon_id": plan.get("phenomenon_id"),
        "source_agent_iteration_plan_run_id": plan.get("run_id"),
        "source_mutation_request_run_id": mutation_request.get("run_id"),
        "mutation_action": action,
        "controlled_research_backtest_allowed": action != "request_more_data",
        "production_execution_allowed": False,
        "queue_write_allowed": False,
        "live_trading_allowed": False,
        "requested_checks": _requested_checks(action, mutation_request),
        "stop_conditions": plan.get("stop_conditions") or [],
        "risk_cost_constraints": plan.get("risk_cost_constraints") or {},
    }
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "next_iteration_bundle",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_previous_iteration_plan_run_id": previous_iteration_plan.get("run_id"),
        "source_mutation_request_run_id": mutation_request.get("run_id"),
        "mutation_action": action,
        "agent_iteration_plan_v2": plan,
        "research_execution_request_v2": request,
        **_closed_gates(),
    }


def validate_next_iteration_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    plan = bundle.get("agent_iteration_plan_v2") or {}
    request = bundle.get("research_execution_request_v2") or {}
    for key in GATE_KEYS:
        if bundle.get(key) is not False:
            reason_codes.append(f"bundle_gate_not_closed_{key}")
        if (plan.get("production_boundaries") or {}).get(key) is not False:
            reason_codes.append(f"plan_gate_not_closed_{key}")
        if key in request and request.get(key) is not False:
            reason_codes.append(f"execution_request_gate_not_closed_{key}")
    action = bundle.get("mutation_action")
    if action != "request_more_data" and not plan.get("risk_cost_constraints"):
        reason_codes.append("missing_risk_cost_constraints")
    if action != "request_more_data" and not request.get("requested_checks"):
        reason_codes.append("missing_requested_checks")
    if request.get("production_execution_allowed") is not False:
        reason_codes.append("execution_request_production_not_closed")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def next_iteration_bundle_to_markdown(bundle: dict[str, Any]) -> str:
    plan = bundle.get("agent_iteration_plan_v2") or {}
    request = bundle.get("research_execution_request_v2") or {}
    lines = [
        "# Next Iteration Bundle",
        "",
        f"run_id: {bundle.get('run_id')}",
        f"phenomenon_id: {plan.get('phenomenon_id')}",
        f"mutation_action: {bundle.get('mutation_action')}",
        f"parent_plan: {bundle.get('source_previous_iteration_plan_run_id')}",
        f"controlled_research_backtest_allowed: {request.get('controlled_research_backtest_allowed')}",
        f"queue_write_allowed: {request.get('queue_write_allowed')}",
        "",
        "## Risk/cost constraints",
    ]
    for name, item in (plan.get("risk_cost_constraints") or {}).items():
        lines.append(f"- {name}: {item.get('purpose')}")
    lines.extend(["", "## Requested checks"])
    lines.extend(f"- {check}" for check in request.get("requested_checks") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_next_iteration_bundle(bundle: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "bundle_json": out / "next_iteration_bundle.json",
        "bundle_markdown": out / "next_iteration_bundle.md",
        "plan_json": out / "agent_iteration_plan_v2.json",
        "request_json": out / "research_execution_request_v2.json",
    }
    paths["bundle_json"].write_text(json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["bundle_markdown"].write_text(next_iteration_bundle_to_markdown(bundle), encoding="utf-8")
    paths["plan_json"].write_text(json.dumps(bundle.get("agent_iteration_plan_v2") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["request_json"].write_text(json.dumps(bundle.get("research_execution_request_v2") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return paths
