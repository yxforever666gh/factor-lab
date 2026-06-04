from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DERIVABLE_FIELDS = {
    "industry_return_60d": {
        "source_fields": ["industry", "date", "return_1d"],
        "derivation": "industry_mean_return_1d_then_rolling_sum:60d",
    }
}


def build_industry_cycle_field_resolution(*, run_id: str, route_registry_v2: dict[str, Any], available_fields: set[str]) -> dict[str, Any]:
    route_id = route_registry_v2.get("top_route_id") or "industry_cycle_inflection_value_anchor_v1"
    route = next((r for r in route_registry_v2.get("routes") or [] if r.get("route_id") == route_id), {})
    rows = []
    for field in route.get("required_fields") or []:
        if field in available_fields:
            status = "available"
            source_fields = [field]
            derivation = None
        elif field in DERIVABLE_FIELDS and set(DERIVABLE_FIELDS[field]["source_fields"]).issubset(available_fields):
            status = "derivable"
            source_fields = DERIVABLE_FIELDS[field]["source_fields"]
            derivation = DERIVABLE_FIELDS[field]["derivation"]
        elif field in DERIVABLE_FIELDS:
            status = "missing_source_for_derivation"
            source_fields = DERIVABLE_FIELDS[field]["source_fields"]
            derivation = DERIVABLE_FIELDS[field]["derivation"]
        else:
            status = "missing_external_or_not_supported"
            source_fields = []
            derivation = None
        rows.append({"field": field, "resolution_status": status, "source_fields": source_fields, "derivation": derivation})
    blocking = {"missing_source_for_derivation", "missing_external_or_not_supported"}
    ready = all(r["resolution_status"] not in blocking for r in rows)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "industry_cycle_field_resolution",
        "route_id": route_id,
        "decision": "prepare_industry_cycle_derivation_specs" if ready else "request_data_or_change_route",
        "field_resolutions": rows,
        "ready_for_derivation_specs": ready,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "next_allowed_actions": ["write_industry_cycle_derivation_specs"] if ready else ["request_data_or_change_route"],
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def industry_cycle_field_resolution_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Industry Cycle Field Resolution",
        "",
        f"route_id: {report.get('route_id')}",
        f"decision: {report.get('decision')}",
        f"ready_for_derivation_specs: {report.get('ready_for_derivation_specs')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "| Field | Status | Source fields | Derivation |",
        "|---|---|---|---|",
    ]
    for row in report.get("field_resolutions") or []:
        lines.append(f"| {row.get('field')} | {row.get('resolution_status')} | {', '.join(row.get('source_fields') or [])} | {row.get('derivation') or ''} |")
    return "\n".join(lines).rstrip() + "\n"


def write_industry_cycle_field_resolution(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    jp = out / "industry_cycle_field_resolution.json"
    mp = out / "industry_cycle_field_resolution.md"
    jp.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    mp.write_text(industry_cycle_field_resolution_to_markdown(report), encoding="utf-8")
    return {"json": jp, "markdown": mp}
