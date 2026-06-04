from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_data_request_report(
    *,
    run_id: str,
    worker_verdict: dict[str, Any],
    route_registry: dict[str, Any],
) -> dict[str, Any]:
    missing_routes: dict[str, set[str]] = defaultdict(set)
    blocked_routes: dict[str, set[str]] = defaultdict(set)
    blocked_route_ids: list[str] = []
    cheap_screen_candidates: list[str] = []

    for route in route_registry.get("routes") or []:
        route_id = str(route.get("route_id") or "")
        if route.get("route_status") == "blocked_missing_fields":
            blocked_route_ids.append(route_id)
        if route.get("route_status") == "cheap_screen_candidate":
            cheap_screen_candidates.append(route_id)
        for field in route.get("missing_fields") or []:
            missing_routes[str(field)].add(route_id)
        for field in route.get("blocked_fields") or []:
            blocked_routes[str(field)].add(route_id)

    field_requests = []
    for field in sorted(blocked_routes):
        field_requests.append({
            "field": field,
            "request_type": "blocked_field_provider_support",
            "routes": sorted(blocked_routes[field]),
        })
    for field in sorted(missing_routes):
        field_requests.append({
            "field": field,
            "request_type": "missing_field",
            "routes": sorted(missing_routes[field]),
        })

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": "request_data",
        "consensus_decision": worker_verdict.get("consensus_decision"),
        "worker_reason_codes": sorted(set(worker_verdict.get("reason_codes") or [])),
        "route_count": len(route_registry.get("routes") or []),
        "blocked_route_ids": sorted(blocked_route_ids),
        "cheap_screen_candidate_route_ids": sorted(cheap_screen_candidates),
        "field_requests": field_requests,
        "next_allowed_actions": [
            "write_blocker_report",
            "draft_new_mechanism_or_data_request",
            "resolve_field_availability",
            "rerun_route_registry_after_data_update",
        ],
        "blocked_actions": [
            "same_route_full_backtest_batch",
            "queue_write",
            "timer_enable",
            "broad_daemon_restore",
            "auto_promotion",
            "drawdown_limit_relaxation",
            "controlled_execution_without_data_resolution",
        ],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "automation_allowed": False,
        "live_trading_enabled": False,
    }


def data_request_report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Data Request Report",
        "",
        f"run_id: {report.get('run_id')}",
        f"decision: {report.get('decision')}",
        f"consensus_decision: {report.get('consensus_decision')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Blocked routes",
    ]
    lines.extend(f"- {route}" for route in report.get("blocked_route_ids") or [])
    if not report.get("blocked_route_ids"):
        lines.append("- none")
    lines.extend(["", "## Field requests", "", "| Field | Request type | Routes |", "|---|---|---|"])
    for row in report.get("field_requests") or []:
        lines.append(f"| {row['field']} | {row['request_type']} | {', '.join(row['routes'])} |")
    lines.extend(["", "## Next allowed actions"])
    lines.extend(f"- {action}" for action in report.get("next_allowed_actions") or [])
    lines.extend(["", "## Blocked actions"])
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_data_request_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "data_request_report.json"
    md_path = out / "data_request_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(data_request_report_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
