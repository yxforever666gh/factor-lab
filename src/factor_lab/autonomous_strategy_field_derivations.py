from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_field_derivation_specs(field_resolution_report: dict[str, Any]) -> dict[str, Any]:
    derived_fields = []
    for row in field_resolution_report.get("field_resolutions") or []:
        if row.get("resolution_status") != "derivable_from_available_history":
            continue
        derived_fields.append({
            "field": row.get("field"),
            "source_field": row.get("source_field"),
            "derivation": row.get("derivation"),
            "routes": list(row.get("routes") or []),
            "implementation_status": "spec_only_not_materialized",
        })
    return {
        "schema_version": 1,
        "run_id": field_resolution_report.get("run_id"),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "derived_fields": derived_fields,
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "automation_allowed": False,
        "materialized": False,
        "next_allowed_actions": [
            "review_derivation_specs",
            "rerun_route_registry_with_spec_available_fields",
            "prepare_cheap_screen_preview_if_route_unblocked",
        ],
    }


def field_derivation_specs_to_markdown(specs: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Field Derivation Specs",
        "",
        f"run_id: {specs.get('run_id')}",
        f"materialized: {specs.get('materialized')}",
        f"controlled_execution_allowed: {specs.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {specs.get('queue_write_allowed')}",
        "",
        "| Field | Source field | Derivation | Status | Routes |",
        "|---|---|---|---|---|",
    ]
    for row in specs.get("derived_fields") or []:
        lines.append(
            f"| {row['field']} | {row.get('source_field') or ''} | {row.get('derivation') or ''} | {row.get('implementation_status')} | {', '.join(row.get('routes') or [])} |"
        )
    return "\n".join(lines).rstrip() + "\n"


def write_field_derivation_specs(specs: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "field_derivation_specs.json"
    md_path = out / "field_derivation_specs.md"
    json_path.write_text(json.dumps(specs, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(field_derivation_specs_to_markdown(specs), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
