from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRIORITY = {
    "industry_cycle_inflection_with_value_anchor": 1,
    "industry_cycle_inflection_value_anchor": 1,
    "balance_sheet_improvement_recovery": 2,
    "earnings_revision_valuation_repair": 3,
}


def _priority(route: dict[str, Any]) -> int:
    fam = str(route.get("mechanism_family") or "")
    for key, value in PRIORITY.items():
        if fam.startswith(key):
            return value
    return 99


def build_route_registry_v2(*, run_id: str, preview_response: dict[str, Any]) -> dict[str, Any]:
    routes = []
    for candidate in preview_response.get("candidate_routes") or []:
        data_status = candidate.get("data_status")
        if data_status == "derivable_from_available_market_history":
            route_status = "field_resolution_candidate"
            recommended = "run_field_resolution"
        elif data_status == "proxy_available_requires_review":
            route_status = "proxy_review_candidate"
            recommended = "run_proxy_field_resolution"
        else:
            route_status = "request_data_candidate"
            recommended = "write_data_request"
        routes.append({
            "schema_version": 1,
            "route_id": candidate.get("route_id"),
            "mechanism_family": candidate.get("mechanism_family"),
            "economic_mechanism": candidate.get("economic_mechanism"),
            "required_fields": candidate.get("required_fields") or [],
            "cheap_screens": candidate.get("cheap_screens") or [],
            "falsification_criteria": candidate.get("falsification_criteria") or [],
            "data_status": data_status,
            "route_status": route_status,
            "recommended_next_step": recommended,
            "priority": _priority(candidate),
            "controlled_execution_allowed": False,
            "queue_write_allowed": False,
            "max_backtests_before_review": 0,
        })
    routes.sort(key=lambda row: (row["priority"], row["route_id"] or ""))
    return {
        "schema_version": 2,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_worker_key": preview_response.get("worker_key"),
        "source_decision_recommendation": preview_response.get("decision_recommendation"),
        "decision_recommendation": "run_field_resolution_for_top_candidate" if routes else "request_new_mechanism",
        "routes": routes,
        "top_route_id": routes[0]["route_id"] if routes else None,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def route_registry_v2_to_markdown(registry: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Route Registry v2",
        "",
        f"decision_recommendation: {registry.get('decision_recommendation')}",
        f"top_route_id: {registry.get('top_route_id')}",
        f"controlled_execution_allowed: {registry.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {registry.get('queue_write_allowed')}",
        "",
        "| Priority | Route | Status | Data status | Next |",
        "|---:|---|---|---|---|",
    ]
    for route in registry.get("routes") or []:
        lines.append(f"| {route.get('priority')} | {route.get('route_id')} | {route.get('route_status')} | {route.get('data_status')} | {route.get('recommended_next_step')} |")
    return "\n".join(lines).rstrip() + "\n"


def write_route_registry_v2(registry: dict[str, Any], *, config_dir: str | Path) -> dict[str, Path]:
    out = Path(config_dir); out.mkdir(parents=True, exist_ok=True)
    jp = out / "autonomous_strategy_routes_v2.json"
    mp = out / "autonomous_strategy_routes_v2.md"
    jp.write_text(json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    mp.write_text(route_registry_v2_to_markdown(registry), encoding="utf-8")
    return {"json": jp, "markdown": mp}
