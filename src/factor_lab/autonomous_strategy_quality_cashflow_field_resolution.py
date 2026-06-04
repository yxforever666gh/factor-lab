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

ALIASES = {
    "debt_to_assets": "debt_to_asset",
    "net_profit_yoy": "profit_yoy",
    "market_cap": "total_mv",
    "turnover_rate": "turnover",
}

PROXY_CANDIDATES = {
    "operating_cashflow_yoy": ["operating_cashflow_to_profit"],
    "ocfps": ["operating_cashflow_to_profit"],
}

PIT_SENSITIVE_FIELDS = {
    "ocfps",
    "roe",
    "gross_margin",
    "net_profit_yoy",
    "operating_cashflow_yoy",
    "debt_to_assets",
    "current_ratio",
    "quick_ratio",
    "interest_coverage",
}


def _resolve_field(field: str, available_fields: set[str]) -> dict[str, Any]:
    if field in available_fields:
        status = "available_requires_pit_validation" if field in PIT_SENSITIVE_FIELDS else "available"
        return {
            "field": field,
            "resolution_status": status,
            "source_fields": [field],
            "derivation": None,
            "pit_safety_status": "requires_report_date_alignment" if field in PIT_SENSITIVE_FIELDS else "not_pit_sensitive",
        }
    alias = ALIASES.get(field)
    if alias and alias in available_fields:
        return {
            "field": field,
            "resolution_status": "available_alias_requires_pit_validation" if field in PIT_SENSITIVE_FIELDS else "available_alias",
            "source_fields": [alias],
            "derivation": f"alias:{alias}",
            "pit_safety_status": "requires_report_date_alignment" if field in PIT_SENSITIVE_FIELDS else "not_pit_sensitive",
        }
    proxies = [candidate for candidate in PROXY_CANDIDATES.get(field, []) if candidate in available_fields]
    if proxies:
        return {
            "field": field,
            "resolution_status": "proxy_available_but_not_equivalent",
            "source_fields": proxies,
            "derivation": "proxy_only:not_true_field",
            "pit_safety_status": "blocked_without_manual_proxy_approval_and_report_date_alignment",
        }
    return {
        "field": field,
        "resolution_status": "missing_external_or_not_supported",
        "source_fields": [],
        "derivation": None,
        "pit_safety_status": "blocked_missing_field",
    }


def build_quality_cashflow_field_resolution(*, run_id: str, mechanism_request: dict[str, Any], available_fields: set[str]) -> dict[str, Any]:
    mechanism_id = mechanism_request.get("mechanism_id") or "quality_cashflow_value_repair_v1"
    required_fields = mechanism_request.get("required_fields") or []
    rows = [_resolve_field(field, available_fields) for field in required_fields]
    blocking_statuses = {
        "missing_external_or_not_supported",
        "proxy_available_but_not_equivalent",
    }
    pit_blocking_statuses = {
        "available_requires_pit_validation",
        "available_alias_requires_pit_validation",
    }
    missing_fields = [row["field"] for row in rows if row["resolution_status"] == "missing_external_or_not_supported"]
    proxy_blocked_fields = [row["field"] for row in rows if row["resolution_status"] == "proxy_available_but_not_equivalent"]
    pit_validation_fields = [row["field"] for row in rows if row["resolution_status"] in pit_blocking_statuses]
    hard_blocked = any(row["resolution_status"] in blocking_statuses for row in rows)
    pit_blocked = bool(pit_validation_fields)
    ready_for_cheap_screen = not hard_blocked and not pit_blocked
    if hard_blocked:
        decision = "request_data"
        recommended_next_step = "request_missing_quality_cashflow_fields"
        next_allowed_actions = ["request_data", "revise_mechanism_fields"]
    elif pit_blocked:
        decision = "block_until_pit_alignment"
        recommended_next_step = "prove_report_date_alignment"
        next_allowed_actions = ["pit_safety_preflight"]
    else:
        decision = "prepare_quality_cashflow_cheap_screen"
        recommended_next_step = "write_quality_cashflow_cheap_screen_plan"
        next_allowed_actions = ["cheap_screen_plan"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "quality_cashflow_field_resolution",
        "mechanism_id": mechanism_id,
        "decision": decision,
        "recommended_next_step": recommended_next_step,
        "ready_for_cheap_screen": ready_for_cheap_screen,
        "field_resolutions": rows,
        "missing_fields": missing_fields,
        "proxy_blocked_fields": proxy_blocked_fields,
        "pit_validation_fields": pit_validation_fields,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
        "data_request": {
            "missing_fields": missing_fields,
            "proxy_blocked_fields": proxy_blocked_fields,
            "pit_validation_fields": pit_validation_fields,
            "reason": "quality_cashflow_value_repair_v1 cannot proceed to cheap screen until true fields are available and PIT alignment is proven.",
        },
    }


def quality_cashflow_field_resolution_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quality Cashflow Field Resolution",
        "",
        f"mechanism_id: {report.get('mechanism_id')}",
        f"decision: {report.get('decision')}",
        f"recommended_next_step: {report.get('recommended_next_step')}",
        f"ready_for_cheap_screen: {report.get('ready_for_cheap_screen')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "| Field | Status | Source fields | PIT status | Derivation |",
        "|---|---|---|---|---|",
    ]
    for row in report.get("field_resolutions") or []:
        lines.append(
            f"| {row.get('field')} | {row.get('resolution_status')} | "
            f"{', '.join(row.get('source_fields') or [])} | {row.get('pit_safety_status')} | {row.get('derivation') or ''} |"
        )
    lines += ["", "## Data request"]
    dr = report.get("data_request") or {}
    lines.append(f"- missing_fields: {', '.join(dr.get('missing_fields') or [])}")
    lines.append(f"- proxy_blocked_fields: {', '.join(dr.get('proxy_blocked_fields') or [])}")
    lines.append(f"- pit_validation_fields: {', '.join(dr.get('pit_validation_fields') or [])}")
    lines.append(f"- reason: {dr.get('reason')}")
    return "\n".join(lines).rstrip() + "\n"


def write_quality_cashflow_field_resolution(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_cashflow_field_resolution.json"
    markdown_path = out / "quality_cashflow_field_resolution.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(quality_cashflow_field_resolution_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
