from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DISTRESS_FIELD_RULES: dict[str, dict[str, Any]] = {
    "debt_to_asset": {
        "aliases": ["debt_to_assets"],
        "proxy_fields": [],
        "pit_required": True,
        "description": "Balance-sheet leverage; usable when PIT financial cache has announcement-aligned values.",
    },
    "roe": {
        "aliases": [],
        "proxy_fields": [],
        "pit_required": False,
        "description": "Profitability quality proxy already present in feature cache; still review provenance before controlled execution.",
    },
    "operating_cashflow_ttm": {
        "aliases": [],
        "proxy_fields": ["operating_cashflow_to_profit"],
        "pit_required": True,
        "description": "TTM operating cashflow is not directly present; operating_cashflow_to_profit can act as a distress proxy only after PIT safety review.",
    },
    "net_profit_ttm": {
        "aliases": [],
        "proxy_fields": ["profit_yoy", "netprofit_yoy"],
        "pit_required": True,
        "description": "TTM net profit is not directly present; yoy fields are not equivalent and should not be treated as available.",
    },
    "interest_coverage": {
        "aliases": [],
        "proxy_fields": [],
        "pit_required": True,
        "description": "Interest coverage requires finance cost / EBIT-style data and is currently missing.",
    },
}


def _resolve_one(field: str, *, feature_fields: set[str], pit_fields: set[str]) -> dict[str, Any]:
    rule = DISTRESS_FIELD_RULES[field]
    aliases = set(rule.get("aliases") or [])
    proxy_fields = [p for p in rule.get("proxy_fields") or [] if p in feature_fields or p in pit_fields]
    direct_feature = field in feature_fields or bool(aliases & feature_fields)
    direct_pit = field in pit_fields or bool(aliases & pit_fields)
    if rule.get("pit_required") and direct_pit:
        status = "pit_available"
        source_field = field if field in pit_fields else sorted(aliases & pit_fields)[0]
    elif rule.get("pit_required") and direct_feature and not direct_pit:
        status = "available_but_pit_alignment_required"
        source_field = field if field in feature_fields else sorted(aliases & feature_fields)[0]
    elif direct_feature:
        status = "available"
        source_field = field if field in feature_fields else sorted(aliases & feature_fields)[0]
    elif proxy_fields:
        status = "proxy_available_requires_review"
        source_field = proxy_fields[0]
    else:
        status = "missing_external_or_derivation_required"
        source_field = None
    return {
        "field": field,
        "resolution_status": status,
        "source_field": source_field,
        "proxy_fields": proxy_fields,
        "pit_required": bool(rule.get("pit_required")),
        "description": rule.get("description"),
    }


def build_quality_cashflow_distress_field_resolution(
    *,
    run_id: str,
    route_registry: dict[str, Any],
    feature_fields: set[str],
    pit_fields: set[str],
    route_id: str = "quality_cashflow_distress_filter",
) -> dict[str, Any]:
    route = next((r for r in route_registry.get("routes") or [] if r.get("route_id") == route_id), {})
    required = [field for field in route.get("required_fields") or [] if field in DISTRESS_FIELD_RULES]
    if not required:
        required = list(DISTRESS_FIELD_RULES)
    resolutions = [_resolve_one(field, feature_fields=feature_fields, pit_fields=pit_fields) for field in required]
    blocking_statuses = {"missing_external_or_derivation_required", "available_but_pit_alignment_required", "proxy_available_requires_review"}
    unresolved = [row for row in resolutions if row["resolution_status"] in blocking_statuses]
    ready_for_distress_screen = not unresolved
    if ready_for_distress_screen:
        decision = "prepare_distress_cheap_screen"
    else:
        decision = "resolve_missing_pit_cashflow_leverage_fields"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "next_mechanism_field_resolution",
        "route_id": route_id,
        "route_status": route.get("route_status"),
        "decision": decision,
        "field_resolutions": resolutions,
        "ready_for_distress_screen": ready_for_distress_screen,
        "unresolved_field_count": len(unresolved),
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "next_allowed_actions": ["build_pit_safety_preflight_or_request_data"] if unresolved else ["prepare_distress_cheap_screen"],
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def distress_field_resolution_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quality Cashflow Distress Field Resolution",
        "",
        f"run_id: {report.get('run_id')}",
        f"route_id: {report.get('route_id')}",
        f"decision: {report.get('decision')}",
        f"ready_for_distress_screen: {report.get('ready_for_distress_screen')}",
        f"unresolved_field_count: {report.get('unresolved_field_count')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "| Field | Status | Source | Proxy fields |",
        "|---|---|---|---|",
    ]
    for row in report.get("field_resolutions") or []:
        lines.append(f"| {row.get('field')} | {row.get('resolution_status')} | {row.get('source_field') or ''} | {', '.join(row.get('proxy_fields') or [])} |")
    lines.append("")
    lines.append("## Next allowed actions")
    lines.extend(f"- {action}" for action in report.get("next_allowed_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_distress_field_resolution(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_cashflow_distress_field_resolution.json"
    md_path = out / "quality_cashflow_distress_field_resolution.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(distress_field_resolution_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
