from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_FORBIDDEN_TA_TERMS = ["Bollinger", "MA cross", "RSI", "MACD", "KDJ", "grid", "martingale"]


def build_agent_policy(*, run_id: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "agent_boundary_research_policy",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "agent_role": "autonomous_market_phenomena_researcher",
        "assistant_role": "boundary_tool_artifact_verifier",
        "core_principle": "The agent chooses research mechanisms, variables, tests, controlled backtests, diagnoses, and mutations; the assistant/code defines boundaries, tools, artifact contracts, and verification.",
        "agent_may_choose": [
            "mechanism_hypotheses",
            "observable_variables",
            "validation_methods",
            "controlled_research_backtest_designs",
            "split_regime_tests",
            "factor_mutations",
            "failure_drawdown_diagnoses",
            "next_generation_hypotheses",
        ],
        "agent_must_not": [
            "live_trading",
            "production_queue_writes",
            "timer_daemon_restore",
            "auto_promotion",
            "low_level_ta_core_logic",
            "manual_assistant_research_task_lists_masquerading_as_autonomy",
            "strategy_count_as_objective",
        ],
        "forbidden_core_logic_terms": REQUIRED_FORBIDDEN_TA_TERMS,
        "required_agent_outputs": [
            "mechanism_hypothesis",
            "participant_logic",
            "observable_variables",
            "data_feasibility_assumptions",
            "controlled_research_backtest_design",
            "drawdown_failure_diagnostics",
            "mutation_logic",
            "stop_conditions",
            "artifact_write_plan",
        ],
        "research_gates": {
            "controlled_research_backtest_allowed_after_evidence_gate": True,
            "agent_generated_iteration_plan_required": True,
            "validator_must_pass_before_execution": True,
        },
        "production_gates": {
            "queue_write_allowed": False,
            "timer_enable_allowed": False,
            "daemon_restore_allowed": False,
            "auto_promotion_allowed": False,
            "live_trading_allowed": False,
        },
    }


def validate_agent_policy(policy: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    allowed = set(policy.get("agent_may_choose") or [])
    required_allowed = {"mechanism_hypotheses", "observable_variables", "controlled_research_backtest_designs", "failure_drawdown_diagnoses", "next_generation_hypotheses"}
    if not required_allowed.issubset(allowed):
        reason_codes.append("missing_required_agent_choice_scope")
    forbidden = set(policy.get("agent_must_not") or [])
    required_forbidden = {"live_trading", "production_queue_writes", "timer_daemon_restore", "auto_promotion", "low_level_ta_core_logic", "manual_assistant_research_task_lists_masquerading_as_autonomy"}
    if not required_forbidden.issubset(forbidden):
        reason_codes.append("missing_required_forbidden_actions")
    if not set(REQUIRED_FORBIDDEN_TA_TERMS).issubset(set(policy.get("forbidden_core_logic_terms") or [])):
        reason_codes.append("missing_required_forbidden_core_logic_terms")
    production = policy.get("production_gates") or {}
    for key in ["queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed", "live_trading_allowed"]:
        if production.get(key) is not False:
            reason_codes.append(f"production_gate_not_closed_{key}")
    if policy.get("assistant_role") != "boundary_tool_artifact_verifier":
        reason_codes.append("assistant_role_not_boundary_verifier")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def agent_policy_to_markdown(policy: dict[str, Any]) -> str:
    lines = [
        "# Market Phenomena Agent Policy",
        "",
        f"run_id: {policy.get('run_id')}",
        f"agent_role: {policy.get('agent_role')}",
        f"assistant_role: {policy.get('assistant_role')}",
        "scope: autonomous research/backtest iteration; not live trading, not production queue, not auto-promotion",
        "",
        "## Core principle",
        str(policy.get("core_principle")),
        "",
        "## Agent may choose",
    ]
    lines.extend(f"- {item}" for item in policy.get("agent_may_choose") or [])
    lines.append("")
    lines.append("## Agent must not")
    lines.extend(f"- {item}" for item in policy.get("agent_must_not") or [])
    lines.append("")
    lines.append("## Forbidden core logic terms")
    lines.extend(f"- {item}" for item in policy.get("forbidden_core_logic_terms") or [])
    lines.append("")
    lines.append("## Required agent outputs")
    lines.extend(f"- {item}" for item in policy.get("required_agent_outputs") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_agent_policy(policy: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "agent_policy.json"
    markdown_path = out / "agent_policy.md"
    json_path.write_text(json.dumps(policy, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(agent_policy_to_markdown(policy), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
