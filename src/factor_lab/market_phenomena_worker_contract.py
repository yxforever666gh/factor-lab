from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_PLAN_SECTIONS = [
    "mechanism_hypothesis",
    "participant_logic",
    "observable_variables",
    "data_feasibility_assumptions",
    "controlled_research_backtest_design",
    "split_regime_tests",
    "drawdown_failure_diagnostics",
    "mutation_logic",
    "stop_conditions",
    "artifact_write_plan",
]

REQUIRED_OUTPUT_ARTIFACTS = [
    "agent_iteration_plan.json",
    "agent_iteration_plan.md",
    "research_execution_request.json",
    "research_verification_checklist.md",
]


def _target_handoffs(research_handoff: dict[str, Any]) -> list[dict[str, Any]]:
    handoffs = research_handoff.get("handoffs") or []
    return [
        h
        for h in handoffs
        if h.get("handoff_status") == "ready_for_controlled_research_backtest"
        and h.get("controlled_research_backtest_allowed") is True
        and h.get("live_trading_allowed") is False
        and h.get("queue_write_allowed") is False
    ]


def _worker_prompt(*, agent_policy: dict[str, Any], target_handoffs: list[dict[str, Any]]) -> str:
    forbidden_terms = agent_policy.get("forbidden_core_logic_terms") or []
    target_lines = []
    for h in target_handoffs:
        target_lines.append(
            f"- {h.get('phenomenon_id')}: {h.get('title')} | question: {h.get('next_research_question')}"
        )
    forbidden_lines = "\n".join(f"- Do not use {term} as core logic." for term in forbidden_terms)
    return (
        "You are a Hermes-native autonomous market phenomena research worker.\n"
        "You choose the research plan: mechanism, variables, tests, controlled backtest design, failure diagnosis, and mutations.\n"
        "The assistant/code only defines boundaries, artifacts, and verification.\n\n"
        "Targets:\n"
        + ("\n".join(target_lines) if target_lines else "- NONE")
        + "\n\nForbidden core logic:\n"
        + forbidden_lines
        + "\n\nProduction boundaries:\n"
        "- Do not write production queue entries.\n"
        "- Do not enable timers or daemons.\n"
        "- Do not auto-promote candidates.\n"
        "- Do not perform live trading.\n"
        "- Do not pin model or provider settings; use the active Hermes runtime.\n\n"
        "Required outputs: agent_iteration_plan.json, agent_iteration_plan.md, "
        "research_execution_request.json, research_verification_checklist.md.\n"
        "Every plan must include: mechanism_hypothesis, participant_logic, observable_variables, "
        "data_feasibility_assumptions, controlled_research_backtest_design, split_regime_tests, "
        "drawdown_failure_diagnostics, mutation_logic, stop_conditions, artifact_write_plan.\n"
    )


def build_worker_contract(
    *,
    run_id: str,
    agent_policy: dict[str, Any],
    research_handoff: dict[str, Any],
    phenomenon_verdict: dict[str, Any],
    minimal_result: dict[str, Any],
    lessons_markdown: str,
    data_catalog_summary: dict[str, Any],
) -> dict[str, Any]:
    targets = _target_handoffs(research_handoff)
    target_ids = [str(h.get("phenomenon_id")) for h in targets if h.get("phenomenon_id")]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "hermes_research_worker_contract",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "worker_role": "hermes_market_phenomena_research_worker",
        "worker_command_template": "hermes chat -q '<worker_prompt>' --toolsets terminal,file",
        "model_provider_pinning_allowed": False,
        "target_phenomena": target_ids,
        "target_handoffs": targets,
        "required_plan_sections": REQUIRED_PLAN_SECTIONS,
        "required_output_artifacts": REQUIRED_OUTPUT_ARTIFACTS,
        "worker_prompt": _worker_prompt(agent_policy=agent_policy, target_handoffs=targets),
        "source_artifacts": {
            "agent_policy_run_id": agent_policy.get("run_id"),
            "research_handoff_run_id": research_handoff.get("run_id"),
            "phenomenon_verdict_run_id": phenomenon_verdict.get("run_id"),
            "minimal_result_run_id": minimal_result.get("run_id"),
        },
        "context_summary": {
            "phenomenon_verdict": phenomenon_verdict,
            "minimal_result": minimal_result,
            "lessons_markdown_excerpt": lessons_markdown[:4000],
            "data_catalog_summary": data_catalog_summary,
        },
        "closed_gates": {
            "live_trading_allowed": False,
            "queue_write_allowed": False,
            "timer_enable_allowed": False,
            "daemon_restore_allowed": False,
            "auto_promotion_allowed": False,
        },
    }


def validate_worker_contract(contract: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    if not contract.get("target_phenomena"):
        reason_codes.append("missing_target_phenomena")
    if contract.get("model_provider_pinning_allowed") is not False:
        reason_codes.append("model_provider_pinning_not_closed")
    prompt = contract.get("worker_prompt") or ""
    for required in ["You choose the research plan", "Do not write production queue", "Do not perform live trading"]:
        if required not in prompt:
            reason_codes.append(f"missing_prompt_boundary_{required.replace(' ', '_')}")
    sections = set(contract.get("required_plan_sections") or [])
    if not set(REQUIRED_PLAN_SECTIONS).issubset(sections):
        reason_codes.append("missing_required_plan_sections")
    outputs = set(contract.get("required_output_artifacts") or [])
    if not set(REQUIRED_OUTPUT_ARTIFACTS).issubset(outputs):
        reason_codes.append("missing_required_output_artifacts")
    gates = contract.get("closed_gates") or {}
    for key in ["live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]:
        if gates.get(key) is not False:
            reason_codes.append(f"gate_not_closed_{key}")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def worker_contract_to_markdown(contract: dict[str, Any]) -> str:
    lines = [
        "# Hermes Research Worker Contract",
        "",
        f"run_id: {contract.get('run_id')}",
        f"worker_role: {contract.get('worker_role')}",
        f"command_template: `{contract.get('worker_command_template')}`",
        "model/provider pinning: forbidden",
        "",
        "## Target phenomena",
    ]
    for phenomenon_id in contract.get("target_phenomena") or []:
        lines.append(f"- {phenomenon_id}")
    lines.extend(["", "## Required plan sections"])
    lines.extend(f"- {item}" for item in contract.get("required_plan_sections") or [])
    lines.extend(["", "## Required output artifacts"])
    lines.extend(f"- {item}" for item in contract.get("required_output_artifacts") or [])
    lines.extend(["", "## Worker prompt", "", "```text", str(contract.get("worker_prompt") or "").rstrip(), "```"])
    return "\n".join(lines).rstrip() + "\n"


def write_worker_contract(contract: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "worker_contract.json"
    markdown_path = out / "worker_contract.md"
    json_path.write_text(json.dumps(contract, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(worker_contract_to_markdown(contract), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
