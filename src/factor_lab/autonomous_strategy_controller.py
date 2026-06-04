from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]

ARTIFACT_PRIORITY = [
    "proxy_workstream_report.json",
    "proxy_route_verdict.json",
    "proxy_cheap_screen_result.json",
    "proxy_cheap_screen_plan.json",
    "proxy_pit_alignment.json",
    "pit_cache_extension_run.json",
    "pit_cache_extension_plan.json",
    "pit_overlay_diagnostic.json",
    "quality_profit_proxy_field_resolution.json",
    "quality_profit_proxy_value_repair_revision.json",
    "quality_cashflow_field_resolution.json",
    "industry_cycle_route_closure.json",
]


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_controller_artifacts(artifact_dir: str | Path) -> dict[str, dict[str, Any]]:
    root = Path(artifact_dir)
    return {name: data for name in ARTIFACT_PRIORITY if (data := _load_json(root / name)) is not None}


def build_autonomous_strategy_controller_state(*, run_id: str, artifacts: dict[str, dict[str, Any]]) -> dict[str, Any]:
    current_state = "insufficient_artifacts"
    latest_artifact = None
    latest_decision = None
    recommended_next_step = "inspect_autonomous_strategy_lab_state"
    human_required = True
    human_decision = {
        "question": "Autonomous Strategy Lab lacks enough artifacts to infer a safe next action.",
        "options": ["inspect_artifacts", "rerun_controller_after_artifacts_exist"],
    }
    next_allowed_actions = ["inspect_autonomous_strategy_lab_state"]
    reason_codes = ["no_recognized_artifact_state"]

    proxy_report = artifacts.get("proxy_workstream_report.json")
    proxy_verdict = artifacts.get("proxy_route_verdict.json")
    proxy_result = artifacts.get("proxy_cheap_screen_result.json")
    proxy_plan = artifacts.get("proxy_cheap_screen_plan.json")
    proxy_alignment = artifacts.get("proxy_pit_alignment.json")
    pit_run = artifacts.get("pit_cache_extension_run.json")
    pit_plan = artifacts.get("pit_cache_extension_plan.json")
    pit_overlay = artifacts.get("pit_overlay_diagnostic.json")
    proxy_field = artifacts.get("quality_profit_proxy_field_resolution.json")
    proxy_revision = artifacts.get("quality_profit_proxy_value_repair_revision.json")
    cashflow_field = artifacts.get("quality_cashflow_field_resolution.json")
    closure = artifacts.get("industry_cycle_route_closure.json")

    if proxy_report and proxy_report.get("alpha_status") == "failed":
        current_state = "proxy_workstream_completed_failed_alpha"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_workstream_report.json"
        latest_decision = proxy_report.get("route_verdict")
        recommended_next_step = proxy_report.get("next_recommended_workstream") or "request_new_mechanism_or_revisit_risk_model"
        human_required = False
        human_decision = None
        next_allowed_actions = [recommended_next_step]
        reason_codes = ["proxy_workstream_report_complete", "alpha_failed", "full_project_execution_still_blocked"]
    elif proxy_report and proxy_report.get("alpha_status") == "manual_review":
        current_state = "proxy_workstream_completed_manual_review"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_workstream_report.json"
        latest_decision = proxy_report.get("route_verdict")
        recommended_next_step = proxy_report.get("next_recommended_workstream") or "manual_review_before_any_execution"
        human_required = True
        human_decision = {"question": "Review proxy workstream before any execution.", "options": ["approve_manual_review_plan", "stop_route", "request_new_mechanism"]}
        next_allowed_actions = ["manual_review_before_any_execution"]
        reason_codes = ["proxy_workstream_report_complete", "manual_review_required"]
    elif proxy_verdict and proxy_verdict.get("verdict") == "stop_route":
        current_state = "proxy_route_stopped"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_route_verdict.json"
        latest_decision = proxy_verdict.get("verdict")
        recommended_next_step = "write_proxy_workstream_report"
        human_required = False
        human_decision = None
        next_allowed_actions = ["write_proxy_workstream_report"]
        reason_codes = ["proxy_route_verdict_stop_route", "full_project_execution_still_blocked"]
    elif proxy_verdict and proxy_verdict.get("verdict") == "manual_review_before_controlled_backtest":
        current_state = "manual_review_required_before_controlled_backtest"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_route_verdict.json"
        latest_decision = proxy_verdict.get("verdict")
        recommended_next_step = "manual_review_proxy_candidate"
        human_required = True
        human_decision = {"question": "Review proxy candidate before any controlled backtest.", "options": ["approve_controlled_backtest_plan", "stop_route", "request_revision"]}
        next_allowed_actions = ["manual_review_proxy_candidate"]
        reason_codes = ["manual_review_required", "controlled_backtest_still_blocked"]
    elif proxy_result and proxy_result.get("overall_status") == "fail":
        current_state = "proxy_cheap_screen_failed"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_cheap_screen_result.json"
        latest_decision = proxy_result.get("recommended_next_step")
        recommended_next_step = "write_proxy_route_verdict"
        human_required = False
        human_decision = None
        next_allowed_actions = ["write_proxy_route_verdict"]
        reason_codes = ["proxy_cheap_screen_failed", "full_project_execution_still_blocked"]
    elif proxy_result and proxy_result.get("overall_status") == "manual_review":
        current_state = "proxy_cheap_screen_manual_review_candidate"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_cheap_screen_result.json"
        latest_decision = proxy_result.get("recommended_next_step")
        recommended_next_step = "write_proxy_route_verdict"
        human_required = False
        human_decision = None
        next_allowed_actions = ["write_proxy_route_verdict"]
        reason_codes = ["proxy_cheap_screen_candidate_passed", "controlled_backtest_still_requires_later_approval"]
    elif proxy_plan and proxy_plan.get("decision") == "prepare_proxy_cheap_screen_execution":
        current_state = "proxy_cheap_screen_plan_ready"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_cheap_screen_plan.json"
        latest_decision = proxy_plan.get("decision")
        recommended_next_step = "run_proxy_cheap_screen_execution"
        human_required = False
        human_decision = None
        next_allowed_actions = ["run_proxy_cheap_screen_execution"]
        reason_codes = ["proxy_cheap_screen_plan_ready", "full_project_execution_still_blocked"]
    elif proxy_alignment and proxy_alignment.get("decision") == "prepare_proxy_cheap_screen_plan":
        current_state = "proxy_pit_alignment_passed"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_pit_alignment.json"
        latest_decision = proxy_alignment.get("decision")
        recommended_next_step = "write_proxy_cheap_screen_plan"
        human_required = False
        human_decision = None
        next_allowed_actions = ["write_proxy_cheap_screen_plan"]
        reason_codes = ["proxy_pit_alignment_passed", "full_project_execution_still_blocked"]
    elif proxy_alignment and proxy_alignment.get("decision") == "block_proxy_pit_alignment":
        current_state = "proxy_pit_alignment_blocked"
        latest_artifact = "artifacts/autonomous_strategy_lab/proxy_pit_alignment.json"
        latest_decision = proxy_alignment.get("decision")
        recommended_next_step = "inspect_pit_alignment_blockers"
        human_required = False
        human_decision = None
        next_allowed_actions = ["inspect_pit_alignment_blockers"]
        reason_codes = ["proxy_pit_alignment_failed", "full_project_execution_still_blocked"]
    elif pit_overlay and pit_overlay.get("decision") == "prepare_proxy_pit_alignment_review":
        current_state = "pit_cache_extension_completed"
        latest_artifact = "artifacts/autonomous_strategy_lab/pit_overlay_diagnostic.json"
        latest_decision = pit_overlay.get("decision")
        recommended_next_step = "prove_proxy_report_date_alignment"
        human_required = False
        human_decision = None
        next_allowed_actions = ["prove_proxy_report_date_alignment", "rerun_proxy_field_resolution_with_pit_overlay"]
        reason_codes = ["pit_overlay_coverage_passed", "phase_6c_cache_extension_complete", "full_project_execution_still_blocked"]
    elif pit_run and pit_run.get("execution_status") == "completed" and pit_run.get("coverage_pass") is True:
        current_state = "pit_cache_extension_chunk_completed" if pit_run.get("chunk_mode") else "pit_cache_extension_completed"
        latest_artifact = "artifacts/autonomous_strategy_lab/pit_cache_extension_run.json"
        latest_decision = pit_run.get("recommended_next_step")
        recommended_next_step = "run_next_pit_cache_extension_chunk" if pit_run.get("chunk_mode") else "rerun_proxy_field_resolution_with_pit_overlay"
        human_required = False
        human_decision = None
        next_allowed_actions = [recommended_next_step]
        reason_codes = ["pit_cache_extension_run_completed", "coverage_passed_for_run", "full_project_execution_still_blocked"]
    elif pit_run and pit_run.get("execution_status") in {"failed", "blocked"}:
        current_state = "pit_cache_extension_run_blocked_or_failed"
        latest_artifact = "artifacts/autonomous_strategy_lab/pit_cache_extension_run.json"
        latest_decision = pit_run.get("failure_reason")
        recommended_next_step = "inspect_pit_cache_extension_failure"
        human_required = False
        human_decision = None
        next_allowed_actions = ["inspect_pit_cache_extension_failure", "retry_bounded_pit_cache_extension"]
        reason_codes = ["pit_cache_extension_run_failed_or_blocked", "full_project_execution_still_blocked"]
    elif pit_plan and pit_plan.get("decision") == "await_human_approval_for_pit_cache_extension":
        current_state = "ready_autonomous_pit_cache_extension"
        latest_artifact = "artifacts/autonomous_strategy_lab/pit_cache_extension_plan.json"
        latest_decision = pit_plan.get("decision")
        recommended_next_step = "run_autonomous_pit_cache_extension"
        human_required = False
        human_decision = None
        next_allowed_actions = ["run_autonomous_pit_cache_extension", "rerun_pit_overlay_diagnostic"]
        reason_codes = [
            "pit_cache_extension_plan_ready",
            "user_granted_temporary_autonomy_for_data_cache_artifact_diagnostics",
            "full_project_execution_still_blocked",
        ]
    elif pit_overlay and pit_overlay.get("decision") == "extend_pit_cache_coverage":
        current_state = "blocked_pit_overlay_coverage"
        latest_artifact = "artifacts/autonomous_strategy_lab/pit_overlay_diagnostic.json"
        latest_decision = pit_overlay.get("decision")
        recommended_next_step = "prepare_pit_cache_extension_plan"
        human_required = True
        human_decision = {
            "question": "Extend PIT financial cache coverage for the current base universe/window?",
            "options": [
                "extend_pit_cache",
                "stop_proxy_route",
                "lower_coverage_threshold_with_manual_approval",
            ],
        }
        next_allowed_actions = ["write_pit_cache_extension_plan", "manual_review"]
        reason_codes = ["pit_overlay_coverage_below_threshold", "external_data_or_cache_extension_requires_human_decision"]
    elif proxy_field and proxy_field.get("decision") == "block_low_coverage":
        current_state = "blocked_proxy_low_coverage"
        latest_artifact = "artifacts/autonomous_strategy_lab/quality_profit_proxy_field_resolution.json"
        latest_decision = proxy_field.get("decision")
        recommended_next_step = "run_pit_overlay_diagnostic"
        human_required = False
        human_decision = None
        next_allowed_actions = ["run_pit_overlay_diagnostic"]
        reason_codes = ["proxy_fields_low_coverage", "safe_diagnostic_next_step"]
    elif proxy_revision and proxy_revision.get("revision_status") == "ready_for_proxy_field_resolution":
        current_state = "ready_proxy_field_resolution"
        latest_artifact = "artifacts/autonomous_strategy_lab/quality_profit_proxy_value_repair_revision.json"
        latest_decision = proxy_revision.get("decision")
        recommended_next_step = "run_quality_profit_proxy_field_resolution"
        human_required = False
        human_decision = None
        next_allowed_actions = ["run_quality_profit_proxy_field_resolution"]
        reason_codes = ["proxy_revision_ready", "safe_field_resolution_next_step"]
    elif cashflow_field and cashflow_field.get("decision") == "request_data":
        current_state = "blocked_quality_cashflow_request_data"
        latest_artifact = "artifacts/autonomous_strategy_lab/quality_cashflow_field_resolution.json"
        latest_decision = cashflow_field.get("decision")
        recommended_next_step = "create_proxy_revision_or_request_data"
        human_required = True
        human_decision = {
            "question": "Original quality-cashflow route is data-blocked; choose proxy revision or external data request.",
            "options": ["create_proxy_revision", "request_external_data", "stop_route"],
        }
        next_allowed_actions = ["manual_review", "write_proxy_revision"]
        reason_codes = ["quality_cashflow_route_blocked_request_data"]
    elif closure and closure.get("route_status") == "stopped":
        current_state = "industry_route_stopped"
        latest_artifact = "artifacts/autonomous_strategy_lab/industry_cycle_route_closure.json"
        latest_decision = closure.get("stop_reason")
        recommended_next_step = "request_new_mechanism"
        human_required = False
        human_decision = None
        next_allowed_actions = ["write_new_mechanism_request"]
        reason_codes = ["industry_cycle_route_stopped", "safe_mechanism_request_next_step"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "autonomous_strategy_controller_once",
        "current_state": current_state,
        "latest_artifact": latest_artifact,
        "latest_decision": latest_decision,
        "recommended_next_step": recommended_next_step,
        "human_required": human_required,
        "human_decision": human_decision,
        "reason_codes": reason_codes,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
        "artifact_inventory": sorted(artifacts.keys()),
    }


def controller_state_to_markdown(state: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Controller State",
        "",
        f"current_state: {state.get('current_state')}",
        f"latest_artifact: {state.get('latest_artifact')}",
        f"latest_decision: {state.get('latest_decision')}",
        f"recommended_next_step: {state.get('recommended_next_step')}",
        f"human_required: {state.get('human_required')}",
        f"controlled_execution_allowed: {state.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {state.get('queue_write_allowed')}",
        f"timer_enable_allowed: {state.get('timer_enable_allowed')}",
        "",
        "## Reason codes",
    ]
    lines.extend(f"- {code}" for code in state.get("reason_codes") or [])
    lines += ["", "## Next allowed actions"]
    lines.extend(f"- {action}" for action in state.get("next_allowed_actions") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in state.get("blocked_actions") or [])
    if state.get("human_decision"):
        lines += ["", "## Human decision", f"question: {state['human_decision'].get('question')}", "", "options:"]
        lines.extend(f"- {option}" for option in state["human_decision"].get("options") or [])
    lines += ["", "## Artifact inventory"]
    lines.extend(f"- {artifact}" for artifact in state.get("artifact_inventory") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_controller_state(state: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "controller_state.json"
    markdown_path = out / "controller_state.md"
    json_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(controller_state_to_markdown(state), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
