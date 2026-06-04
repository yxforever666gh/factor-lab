from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_HISTORY_RE = re.compile(r"^(?P<base>.+)_history_(?P<window>\d+d)$")


def build_field_resolution_report(
    *,
    run_id: str,
    data_request_report: dict[str, Any],
    schema_fields: set[str],
    available_fields: set[str],
    blocked_fields: set[str],
    aliases: dict[str, str] | None = None,
) -> dict[str, Any]:
    aliases = aliases or {}
    resolutions = []
    for request in data_request_report.get("field_requests") or []:
        field = str(request.get("field") or "").strip()
        resolution = _resolve_field(field, schema_fields=schema_fields, available_fields=available_fields, blocked_fields=blocked_fields, aliases=aliases)
        resolution.update({
            "field": field,
            "request_type": request.get("request_type"),
            "routes": list(request.get("routes") or []),
        })
        resolutions.append(resolution)
    resolutions.sort(key=lambda row: (row["field"], row["resolution_status"]))
    blocking_statuses = {
        "blocked_provider_support_required",
        "external_data_required",
        "missing_source_field_for_derivation",
    }
    ready_for_rerun = bool(resolutions) and all(row["resolution_status"] not in blocking_statuses for row in resolutions)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "decision": "resolve_field_availability",
        "field_resolutions": resolutions,
        "ready_for_route_registry_rerun": ready_for_rerun,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "automation_allowed": False,
        "next_allowed_actions": [
            "implement_derivable_field_specs",
            "request_external_data_for_external_fields",
            "keep_blocked_fields_out_of_available_set",
            "rerun_route_registry_after_field_resolution",
        ],
        "blocked_actions": [
            "same_route_full_backtest_batch",
            "queue_write",
            "timer_enable",
            "broad_daemon_restore",
            "controlled_execution_without_field_resolution",
        ],
    }


def field_resolution_report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Field Resolution Report",
        "",
        f"run_id: {report.get('run_id')}",
        f"decision: {report.get('decision')}",
        f"ready_for_route_registry_rerun: {report.get('ready_for_route_registry_rerun')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "| Field | Resolution | Source field | Routes |",
        "|---|---|---|---|",
    ]
    for row in report.get("field_resolutions") or []:
        lines.append(
            f"| {row['field']} | {row['resolution_status']} | {row.get('source_field') or ''} | {', '.join(row.get('routes') or [])} |"
        )
    lines.extend(["", "## Next allowed actions"])
    lines.extend(f"- {action}" for action in report.get("next_allowed_actions") or [])
    lines.extend(["", "## Blocked actions"])
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_field_resolution_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "field_resolution_report.json"
    md_path = out / "field_resolution_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(field_resolution_report_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def _resolve_field(
    field: str,
    *,
    schema_fields: set[str],
    available_fields: set[str],
    blocked_fields: set[str],
    aliases: dict[str, str],
) -> dict[str, Any]:
    if field in blocked_fields:
        return {"resolution_status": "blocked_provider_support_required", "source_field": field}
    if field in available_fields:
        return {"resolution_status": "already_available", "source_field": field}
    alias_target = aliases.get(field)
    if alias_target and alias_target in available_fields:
        return {"resolution_status": "alias_available", "source_field": alias_target}
    history_match = _HISTORY_RE.match(field)
    if history_match:
        base = history_match.group("base")
        if base in available_fields:
            return {
                "resolution_status": "derivable_from_available_history",
                "source_field": base,
                "derivation": f"rolling_history_window:{history_match.group('window')}",
            }
        return {"resolution_status": "missing_source_field_for_derivation", "source_field": base}
    if field in schema_fields:
        return {"resolution_status": "schema_field_not_provider_available", "source_field": field}
    return {"resolution_status": "external_data_required", "source_field": None}
