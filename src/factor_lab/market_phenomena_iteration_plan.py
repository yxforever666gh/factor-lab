from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FORBIDDEN_LOW_LEVEL_TA_TERMS = ["Bollinger", "MA cross", "RSI", "MACD", "KDJ", "grid", "martingale"]
PRODUCTION_GATE_KEYS = [
    "live_trading_allowed",
    "queue_write_allowed",
    "timer_enable_allowed",
    "daemon_restore_allowed",
    "auto_promotion_allowed",
]


def _first_target(worker_contract: dict[str, Any]) -> dict[str, Any]:
    handoffs = worker_contract.get("target_handoffs") or []
    if handoffs:
        return handoffs[0]
    ids = worker_contract.get("target_phenomena") or []
    return {"phenomenon_id": ids[0] if ids else None, "title": ids[0] if ids else None}


def _boundaries() -> dict[str, bool]:
    return {key: False for key in PRODUCTION_GATE_KEYS}


def build_agent_iteration_plan(*, run_id: str, worker_contract: dict[str, Any]) -> dict[str, Any]:
    target = _first_target(worker_contract)
    phenomenon_id = str(target.get("phenomenon_id") or "")
    title = str(target.get("title") or phenomenon_id)
    target_group = str(target.get("target_group") or "")
    research_tasks = list(target.get("research_tasks") or [])
    iteration_policy = target.get("iteration_policy") or {}
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "agent_generated_iteration_plan",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_worker_contract_run_id": worker_contract.get("run_id"),
        "phenomenon_id": phenomenon_id,
        "title": title,
        "target_group": target_group,
        "controlled_execution_allowed": True,
        "production_boundaries": _boundaries(),
        "mechanism_hypothesis": {
            "claim": "Firms moving from apparent value traps into balance-sheet repair can be repriced with delay because some constrained holders wait for accounting confirmation while faster capital is reluctant to underwrite weak historical fundamentals.",
            "mispricing_mechanism": "capital_constraint_and_slow_confirmation",
            "why_not_arbitraged_immediately": "The group has poor prior fundamentals, limited institutional sponsorship, and uncertain repair persistence, so arbitrage capital demands confirmation and the repricing path is state-dependent.",
        },
        "participant_logic": {
            "participants": ["forced_seller", "capital_constraint_value_manager", "fundamental_repair_buyer", "risk_budgeted_institution"],
            "constraint": "capital_constraint_and_reputation_risk_after_prior_fundamental_damage",
            "expected_behavior": "Forced or benchmark-aware sellers reduce exposure before repair is fully recognized; repair buyers accumulate only after balance-sheet evidence and liquidity improve.",
        },
        "observable_variables": {
            "primary": ["valuation_percentile", "balance_sheet_repair_signal", "debt_ratio_delta", "current_ratio_delta", "cash_flow_repair_proxy"],
            "controls": ["industry", "size_bucket", "turnover_bucket", "market_regime", "prior_drawdown"],
            "target": "forward_excess_return_vs_industry_size_control",
        },
        "data_feasibility_assumptions": {
            "usable_row_count": target.get("usable_row_count"),
            "usable_ticker_count": target.get("usable_ticker_count"),
            "pit_required": True,
            "blocked_if": ["insufficient PIT accounting coverage", "missing industry or size controls", "cannot form target/control groups without leakage"],
        },
        "controlled_research_backtest_design": {
            "objective": "Test whether the phenomenon survives robustness splits before any strategy design.",
            "unit": "phenomenon cohort versus matched controls",
            "allowed_tasks": research_tasks,
            "minimum_checks": ["industry_split_robustness", "size_split_robustness", "regime_split_robustness", "turnover_sensitivity", "drawdown_sensitivity", "cost_sensitivity_probe"],
            "success_condition": "Positive spread versus matched controls with acceptable drawdown sensitivity across core splits.",
        },
        "split_regime_tests": [
            {"name": "industry_split_robustness", "purpose": "Separate repair effect from industry rebound."},
            {"name": "size_split_robustness", "purpose": "Check small-cap/liquidity concentration."},
            {"name": "regime_split_robustness", "purpose": "Check if effect only exists in broad risk-on rebounds."},
            {"name": "turnover_sensitivity", "purpose": "Check implementation fragility and crowding proxy."},
            {"name": "drawdown_sensitivity", "purpose": "Identify whether return spread is paid for crash exposure."},
        ],
        "drawdown_failure_diagnostics": {
            "if_drawdown_fails": iteration_policy.get("if_drawdown_fails") or ["add_regime_filter", "add_liquidity_filter"],
            "diagnose": ["industry crash overlap", "liquidity withdrawal", "balance-sheet repair reversal", "valuation trap relapse"],
        },
        "mutation_logic": {
            "if_return_fails": iteration_policy.get("if_return_fails") or ["mutate_factor_definition", "change_thresholds"],
            "if_split_unstable": iteration_policy.get("if_split_unstable") or ["keep_only_supported_regime", "write_rejection_for_unstable_splits"],
            "allowed_mutations": ["holding_horizon_variation", "condition_threshold_variation", "factor_definition_mutation"],
        },
        "stop_conditions": [
            "negative spread versus matched controls after robustness splits",
            "performance entirely explained by one industry, one size bucket, or one risk-on regime",
            "drawdown exposure dominates return spread",
            "PIT/data coverage is insufficient for leakage-safe cohort construction",
        ],
        "artifact_write_plan": {
            "plan_json": "artifacts/market_phenomena/agent_iteration_plan.json",
            "plan_markdown": "artifacts/market_phenomena/agent_iteration_plan.md",
            "execution_request_json": "artifacts/market_phenomena/research_execution_request.json",
            "verification_checklist_markdown": "artifacts/market_phenomena/research_verification_checklist.md",
        },
    }


def validate_agent_iteration_plan(plan: dict[str, Any], worker_contract: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    for section in worker_contract.get("required_plan_sections") or []:
        if not plan.get(section):
            reason_codes.append(f"missing_required_section_{section}")
    expected_ids = set(worker_contract.get("target_phenomena") or [])
    if expected_ids and plan.get("phenomenon_id") not in expected_ids:
        reason_codes.append("phenomenon_not_in_worker_contract")
    gates = plan.get("production_boundaries") or {}
    for key in PRODUCTION_GATE_KEYS:
        if gates.get(key) is not False:
            reason_codes.append(f"production_gate_not_closed_{key}")
    if plan.get("controlled_execution_allowed") is not True:
        reason_codes.append("controlled_execution_not_allowed")
    serialized = json.dumps(plan, ensure_ascii=False)
    for term in FORBIDDEN_LOW_LEVEL_TA_TERMS:
        if term in serialized:
            reason_codes.append(f"forbidden_low_level_ta_term_{term}")
    if "strategy_count" in serialized:
        reason_codes.append("strategy_count_objective_detected")
    if "participant_logic" in plan and "mechanism_hypothesis" in plan:
        participant_text = json.dumps(plan.get("participant_logic"), ensure_ascii=False)
        mechanism_text = json.dumps(plan.get("mechanism_hypothesis"), ensure_ascii=False)
        if not any(token in participant_text + mechanism_text for token in ["forced_seller", "capital_constraint", "risk_budgeted"]):
            reason_codes.append("missing_participant_constraint_logic")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def _execution_request(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": plan.get("run_id"),
        "mode": "controlled_research_execution_request",
        "phenomenon_id": plan.get("phenomenon_id"),
        "controlled_research_backtest_allowed": True,
        "production_execution_allowed": False,
        "live_trading_allowed": False,
        "queue_write_allowed": False,
        "requested_checks": (plan.get("controlled_research_backtest_design") or {}).get("minimum_checks") or [],
        "stop_conditions": plan.get("stop_conditions") or [],
    }


def _verification_checklist(plan: dict[str, Any]) -> str:
    lines = [
        "# Research Verification Checklist",
        "",
        f"run_id: {plan.get('run_id')}",
        f"phenomenon_id: {plan.get('phenomenon_id')}",
        "",
        "- [ ] No production queue writes",
        "- [ ] No timer/daemon restore",
        "- [ ] No live trading",
        "- [ ] PIT-safe data only",
        "- [ ] Industry, size, regime, turnover, and drawdown splits checked",
        "- [ ] Reject if spread is explained by one brittle bucket",
        "- [ ] Write result artifacts before any further mutation",
    ]
    return "\n".join(lines).rstrip() + "\n"


def iteration_plan_to_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# Agent-generated Iteration Plan",
        "",
        f"run_id: {plan.get('run_id')}",
        f"phenomenon_id: {plan.get('phenomenon_id')}",
        f"title: {plan.get('title')}",
        f"source_worker_contract_run_id: {plan.get('source_worker_contract_run_id')}",
        "",
        "## Mechanism hypothesis",
        json.dumps(plan.get("mechanism_hypothesis"), ensure_ascii=False, indent=2),
        "",
        "## Participant logic",
        json.dumps(plan.get("participant_logic"), ensure_ascii=False, indent=2),
        "",
        "## Controlled research backtest design",
        json.dumps(plan.get("controlled_research_backtest_design"), ensure_ascii=False, indent=2),
        "",
        "## Stop conditions",
    ]
    lines.extend(f"- {item}" for item in plan.get("stop_conditions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_agent_iteration_plan(plan: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "plan_json": out / "agent_iteration_plan.json",
        "plan_markdown": out / "agent_iteration_plan.md",
        "execution_request_json": out / "research_execution_request.json",
        "verification_checklist_markdown": out / "research_verification_checklist.md",
    }
    paths["plan_json"].write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["plan_markdown"].write_text(iteration_plan_to_markdown(plan), encoding="utf-8")
    paths["execution_request_json"].write_text(json.dumps(_execution_request(plan), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["verification_checklist_markdown"].write_text(_verification_checklist(plan), encoding="utf-8")
    return paths
