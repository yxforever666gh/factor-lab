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

PIT_SENSITIVE_PROXY_FIELDS = {"roe", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"}


def _resolve_proxy_field(field: str, available_fields: set[str], coverage_by_field: dict[str, float]) -> dict[str, Any]:
    if field not in available_fields:
        return {
            "field": field,
            "resolution_status": "missing_external_or_not_supported",
            "coverage": 0.0,
            "pit_safety_status": "blocked_missing_field",
            "source_fields": [],
        }
    coverage = float(coverage_by_field.get(field, 1.0))
    if coverage < 0.6:
        status = "available_but_low_coverage"
    elif field in PIT_SENSITIVE_PROXY_FIELDS:
        status = "available_requires_pit_validation"
    else:
        status = "available"
    return {
        "field": field,
        "resolution_status": status,
        "coverage": coverage,
        "pit_safety_status": "requires_report_date_alignment" if field in PIT_SENSITIVE_PROXY_FIELDS else "not_pit_sensitive",
        "source_fields": [field],
    }


def build_quality_profit_proxy_field_resolution(
    *,
    run_id: str,
    proxy_revision: dict[str, Any],
    available_fields: set[str],
    coverage_by_field: dict[str, float] | None = None,
) -> dict[str, Any]:
    coverage_by_field = coverage_by_field or {}
    mechanism_id = proxy_revision.get("mechanism_id") or "quality_profit_proxy_value_repair_v1"
    required_fields = proxy_revision.get("proxy_required_fields") or []
    rows = [_resolve_proxy_field(field, available_fields, coverage_by_field) for field in required_fields]
    missing_fields = [row["field"] for row in rows if row["resolution_status"] == "missing_external_or_not_supported"]
    low_coverage_fields = [row["field"] for row in rows if row["resolution_status"] == "available_but_low_coverage"]
    pit_validation_fields = [row["field"] for row in rows if row["resolution_status"] == "available_requires_pit_validation"]
    if missing_fields:
        decision = "request_data"
        recommended_next_step = "request_missing_proxy_fields"
        next_allowed_actions = ["request_data", "revise_proxy_mechanism"]
        ready_for_proxy_cheap_screen_plan = False
    elif low_coverage_fields:
        decision = "block_low_coverage"
        recommended_next_step = "extend_proxy_field_coverage"
        next_allowed_actions = ["coverage_preflight", "extend_cache"]
        ready_for_proxy_cheap_screen_plan = False
    elif pit_validation_fields:
        decision = "block_until_pit_alignment"
        recommended_next_step = "prove_proxy_report_date_alignment"
        next_allowed_actions = ["pit_safety_preflight"]
        ready_for_proxy_cheap_screen_plan = False
    else:
        decision = "prepare_proxy_cheap_screen_plan"
        recommended_next_step = "write_quality_profit_proxy_cheap_screen_plan"
        next_allowed_actions = ["proxy_cheap_screen_plan"]
        ready_for_proxy_cheap_screen_plan = True
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "quality_profit_proxy_field_resolution",
        "mechanism_id": mechanism_id,
        "decision": decision,
        "recommended_next_step": recommended_next_step,
        "ready_for_proxy_cheap_screen_plan": ready_for_proxy_cheap_screen_plan,
        "field_resolutions": rows,
        "missing_fields": missing_fields,
        "low_coverage_fields": low_coverage_fields,
        "pit_validation_fields": pit_validation_fields,
        "proxy_caveats": proxy_revision.get("proxy_caveats") or [],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
    }


def quality_profit_proxy_field_resolution_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quality Profit Proxy Field Resolution",
        "",
        f"mechanism_id: {report.get('mechanism_id')}",
        f"decision: {report.get('decision')}",
        f"recommended_next_step: {report.get('recommended_next_step')}",
        f"ready_for_proxy_cheap_screen_plan: {report.get('ready_for_proxy_cheap_screen_plan')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "| Field | Status | Coverage | PIT status | Source fields |",
        "|---|---|---:|---|---|",
    ]
    for row in report.get("field_resolutions") or []:
        lines.append(
            f"| {row.get('field')} | {row.get('resolution_status')} | {row.get('coverage')} | "
            f"{row.get('pit_safety_status')} | {', '.join(row.get('source_fields') or [])} |"
        )
    lines += ["", "## Proxy caveats"]
    lines.extend(f"- {caveat}" for caveat in report.get("proxy_caveats") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_quality_profit_proxy_field_resolution(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_profit_proxy_field_resolution.json"
    markdown_path = out / "quality_profit_proxy_field_resolution.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(quality_profit_proxy_field_resolution_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
