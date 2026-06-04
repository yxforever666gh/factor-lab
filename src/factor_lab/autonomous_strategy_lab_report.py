from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

AUTONOMOUS_STRATEGY_LAB_ROOT = Path("artifacts/autonomous_strategy_lab")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_error": str(exc), "_path": str(path)}


def build_autonomous_strategy_lab_report(root: str | Path = ".") -> dict[str, Any]:
    base = Path(root) / AUTONOMOUS_STRATEGY_LAB_ROOT
    route_registry = _read_json(Path(root) / "configs" / "autonomous_strategy_routes.json")
    field_derivations = _read_json(base / "field_derivation_specs.json")
    cheap_screen_plan = _read_json(base / "cheap_screen_plan.json")
    cheap_screen_result = _read_json(base / "cheap_screen_result.json")
    risk_diagnostic = _read_json(base / "cheap_screen_risk_diagnostic.json")
    route_verdict = _read_json(base / "route_verdict.json")
    risk_filter_probe = _read_json(base / "value_trap_risk_filter_probe.json")
    distress_field_resolution = _read_json(base / "quality_cashflow_distress_field_resolution.json")
    new_mechanism_request = _read_json(base / "new_mechanism_request.json")
    mechanism_request_pack = _read_json(base / "mechanism_researcher_request.json")
    coverage_preflight = _read_json(base / "historical_valuation_coverage_preflight.json")
    cache_extension_plan = _read_json(base / "cache_extension_plan.json")
    controlled_execution = _read_json(base / "controlled_execution_decision.json")

    route_statuses = [
        {
            "route_id": route.get("route_id"),
            "route_status": route.get("route_status"),
            "recommended_next_step": route.get("recommended_next_step"),
            "missing_fields": route.get("missing_fields") or [],
            "controlled_execution_allowed": bool(route.get("controlled_execution_allowed")),
            "queue_write_allowed": bool(route.get("queue_write_allowed")),
        }
        for route in route_registry.get("routes") or []
    ]
    blocked_actions = sorted(set(
        list(cheap_screen_plan.get("blocked_actions") or [])
        + list(coverage_preflight.get("blocked_actions") or [])
        + list(controlled_execution.get("blocked_actions") or [])
    ))
    allowed_actions = sorted(set(
        list(coverage_preflight.get("next_allowed_actions") or [])
        + list(controlled_execution.get("next_allowed_actions") or [])
    ))
    status = controlled_execution.get("execution_status") or coverage_preflight.get("overall_status") or "unknown"
    if coverage_preflight.get("overall_status") == "blocked":
        decision = "request_data"
    elif mechanism_request_pack.get("decision"):
        decision = mechanism_request_pack.get("decision")
    elif new_mechanism_request.get("decision"):
        decision = new_mechanism_request.get("decision")
    elif risk_filter_probe.get("overall_status") in {"fail", "manual_review", "blocked"}:
        decision = risk_filter_probe.get("recommended_next_step") or "stop_route"
    elif route_verdict.get("verdict"):
        decision = route_verdict.get("verdict")
    elif risk_diagnostic.get("overall_status") in {"fail", "manual_review", "blocked"}:
        decision = risk_diagnostic.get("recommended_next_step") or "manual_review_risk"
    elif cheap_screen_result.get("overall_status") in {"fail", "manual_review", "blocked"}:
        decision = cheap_screen_result.get("recommended_next_step") or "manual_review"
    elif controlled_execution.get("execution_status") == "blocked":
        decision = "manual_review"
    else:
        decision = status
    report = {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "decision": decision,
        "route_statuses": route_statuses,
        "derived_field_count": len(field_derivations.get("derived_fields") or []),
        "cheap_screen_task_count": cheap_screen_plan.get("task_count", 0),
        "cheap_screen_overall_status": cheap_screen_result.get("overall_status"),
        "cheap_screen_recommended_next_step": cheap_screen_result.get("recommended_next_step"),
        "cheap_screen_information_status": cheap_screen_result.get("information_screen_status"),
        "cheap_screen_risk_status": cheap_screen_result.get("risk_screen_status"),
        "cheap_screen_spread": cheap_screen_result.get("cheap_expensive_spread"),
        "cheap_screen_rank_ic": cheap_screen_result.get("rank_ic"),
        "cheap_screen_drawdown_proxy": cheap_screen_result.get("drawdown_proxy"),
        "risk_diagnostic_overall_status": risk_diagnostic.get("overall_status"),
        "risk_diagnostic_recommended_next_step": risk_diagnostic.get("recommended_next_step"),
        "risk_diagnostic_original_drawdown": risk_diagnostic.get("original_drawdown"),
        "risk_diagnostic_best_repair_candidate": risk_diagnostic.get("best_repair_candidate"),
        "route_verdict": route_verdict.get("verdict"),
        "route_verdict_reason_codes": route_verdict.get("reason_codes") or [],
        "route_verdict_max_next_risk_filter_probes": route_verdict.get("max_next_risk_filter_probes"),
        "risk_filter_probe_overall_status": risk_filter_probe.get("overall_status"),
        "risk_filter_probe_recommended_next_step": risk_filter_probe.get("recommended_next_step"),
        "risk_filter_probe_best_candidate": risk_filter_probe.get("best_candidate"),
        "next_mechanism_field_resolution_decision": distress_field_resolution.get("decision"),
        "next_mechanism_ready_for_distress_screen": distress_field_resolution.get("ready_for_distress_screen"),
        "next_mechanism_unresolved_field_count": distress_field_resolution.get("unresolved_field_count"),
        "new_mechanism_request_decision": new_mechanism_request.get("decision"),
        "new_mechanism_candidate_families": new_mechanism_request.get("candidate_next_mechanism_families") or [],
        "mechanism_request_pack_decision": mechanism_request_pack.get("decision"),
        "mechanism_request_worker_task_count": len(mechanism_request_pack.get("worker_tasks") or []),
        "coverage_overall_status": coverage_preflight.get("overall_status"),
        "coverage_field_summary": coverage_preflight.get("field_coverage") or [],
        "execution_status": controlled_execution.get("execution_status"),
        "cache_extension_status": cache_extension_plan.get("execution_status"),
        "cache_extension_action": cache_extension_plan.get("action"),
        "cache_extension_external_request_required": bool(cache_extension_plan.get("external_request_required")),
        "cache_extension_target_cache_path": cache_extension_plan.get("target_cache_path"),
        "execution_reason_codes": controlled_execution.get("reason_codes") or [],
        "controlled_execution_started": bool(controlled_execution.get("controlled_execution_started")),
        "controlled_execution_allowed": bool(controlled_execution.get("controlled_execution_allowed")),
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": blocked_actions,
        "allowed_actions": allowed_actions,
        "artifact_paths": {
            "route_registry": "configs/autonomous_strategy_routes.json",
            "field_derivations": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "field_derivation_specs.json"),
            "cheap_screen_plan": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "cheap_screen_plan.json"),
            "cheap_screen_result": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "cheap_screen_result.json"),
            "risk_diagnostic": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "cheap_screen_risk_diagnostic.json"),
            "route_verdict": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "route_verdict.json"),
            "risk_filter_probe": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "value_trap_risk_filter_probe.json"),
            "next_mechanism_field_resolution": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "quality_cashflow_distress_field_resolution.json"),
            "new_mechanism_request": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "new_mechanism_request.json"),
            "mechanism_request_pack": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "mechanism_researcher_request.json"),
            "coverage_preflight": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "historical_valuation_coverage_preflight.json"),
            "cache_extension_plan": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "cache_extension_plan.json"),
            "controlled_execution": str(AUTONOMOUS_STRATEGY_LAB_ROOT / "controlled_execution_decision.json"),
        },
    }
    return report


def autonomous_strategy_lab_report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Lab Status",
        "",
        f"status: {report.get('status')}",
        f"decision: {report.get('decision')}",
        f"coverage_overall_status: {report.get('coverage_overall_status')}",
        f"cheap_screen_overall_status: {report.get('cheap_screen_overall_status')}",
        f"cheap_screen_recommended_next_step: {report.get('cheap_screen_recommended_next_step')}",
        f"cheap_screen_information_status: {report.get('cheap_screen_information_status')}",
        f"cheap_screen_risk_status: {report.get('cheap_screen_risk_status')}",
        f"cheap_screen_spread: {report.get('cheap_screen_spread')}",
        f"cheap_screen_rank_ic: {report.get('cheap_screen_rank_ic')}",
        f"cheap_screen_drawdown_proxy: {report.get('cheap_screen_drawdown_proxy')}",
        f"risk_diagnostic_overall_status: {report.get('risk_diagnostic_overall_status')}",
        f"risk_diagnostic_recommended_next_step: {report.get('risk_diagnostic_recommended_next_step')}",
        f"risk_diagnostic_original_drawdown: {report.get('risk_diagnostic_original_drawdown')}",
        f"risk_diagnostic_best_repair_candidate: {report.get('risk_diagnostic_best_repair_candidate')}",
        f"route_verdict: {report.get('route_verdict')}",
        f"route_verdict_max_next_risk_filter_probes: {report.get('route_verdict_max_next_risk_filter_probes')}",
        f"risk_filter_probe_overall_status: {report.get('risk_filter_probe_overall_status')}",
        f"risk_filter_probe_recommended_next_step: {report.get('risk_filter_probe_recommended_next_step')}",
        f"risk_filter_probe_best_candidate: {report.get('risk_filter_probe_best_candidate')}",
        f"next_mechanism_field_resolution_decision: {report.get('next_mechanism_field_resolution_decision')}",
        f"next_mechanism_ready_for_distress_screen: {report.get('next_mechanism_ready_for_distress_screen')}",
        f"next_mechanism_unresolved_field_count: {report.get('next_mechanism_unresolved_field_count')}",
        f"new_mechanism_request_decision: {report.get('new_mechanism_request_decision')}",
        f"new_mechanism_candidate_families: {report.get('new_mechanism_candidate_families')}",
        f"mechanism_request_pack_decision: {report.get('mechanism_request_pack_decision')}",
        f"mechanism_request_worker_task_count: {report.get('mechanism_request_worker_task_count')}",
        f"execution_status: {report.get('execution_status')}",
        f"cache_extension_status: {report.get('cache_extension_status')}",
        f"cache_extension_action: {report.get('cache_extension_action')}",
        f"cache_extension_external_request_required: {report.get('cache_extension_external_request_required')}",
        f"cache_extension_target_cache_path: {report.get('cache_extension_target_cache_path')}",
        f"controlled_execution_started: {report.get('controlled_execution_started')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        f"timer_enable_allowed: {report.get('timer_enable_allowed')}",
        "",
        "## Execution reason codes",
    ]
    lines.extend(f"- {reason}" for reason in report.get("execution_reason_codes") or [])
    lines.append("")
    lines.append("## Coverage fields")
    for item in report.get("coverage_field_summary") or []:
        lines.append(
            f"- {item.get('derived_field')}: {item.get('status')} "
            f"({item.get('eligible_ticker_count')}/{item.get('ticker_count')}, ratio={item.get('eligible_ticker_ratio')})"
        )
    if not report.get("coverage_field_summary"):
        lines.append("- none")
    lines.append("")
    lines.append("## Route statuses")
    for route in report.get("route_statuses") or []:
        lines.append(f"- {route.get('route_id')}: {route.get('route_status')} -> {route.get('recommended_next_step')}")
    if not report.get("route_statuses"):
        lines.append("- none")
    lines.append("")
    lines.append("## Allowed actions")
    lines.extend(f"- {action}" for action in report.get("allowed_actions") or [])
    lines.append("")
    lines.append("## Blocked actions")
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_autonomous_strategy_lab_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "status_report.json"
    md_path = out / "status_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(autonomous_strategy_lab_report_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
