from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SYSTEM_FIELDS = {"date", "ticker"}


def build_route_registry_from_worker_response(
    worker_response: dict[str, Any],
    *,
    available_fields: set[str],
    blocked_fields: set[str],
    source_path: str | None = None,
) -> dict[str, Any]:
    routes: list[dict[str, Any]] = []
    for proposal in worker_response.get("route_proposals") or []:
        required = _string_list(proposal.get("required_fields"))
        declared_missing = set(_string_list(proposal.get("known_missing_or_blocked_fields")))
        schema_missing = {field for field in required if field not in available_fields and field not in SYSTEM_FIELDS}
        schema_blocked = {field for field in required if field in blocked_fields}
        blocked = sorted(schema_blocked)
        missing_fields = sorted(((declared_missing | schema_missing) - schema_blocked) - available_fields)
        route_status = "blocked_missing_fields" if missing_fields or blocked else "cheap_screen_candidate"
        routes.append({
            "route_id": str(proposal.get("route_id") or proposal.get("mechanism_id") or "").strip(),
            "mechanism_id": str(proposal.get("mechanism_id") or proposal.get("route_id") or "").strip(),
            "economic_mechanism": str(proposal.get("economic_mechanism") or "").strip(),
            "required_fields": required,
            "missing_fields": missing_fields,
            "blocked_fields": blocked,
            "cheap_screens": _string_list(proposal.get("cheap_screens")),
            "falsification_criteria": _string_list(proposal.get("falsification_criteria")),
            "recommended_next_step": str(proposal.get("recommended_next_step") or "manual_review").strip(),
            "route_status": route_status,
            "requires_manual_review": True,
            "queue_write_allowed": False,
            "controlled_execution_allowed": False,
            "max_backtests_before_review": 0,
        })
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_worker_key": worker_response.get("worker_key"),
        "source_path": source_path,
        "decision_recommendation": worker_response.get("decision_recommendation"),
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "routes": routes,
    }


def route_registry_to_markdown(registry: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Route Registry",
        "",
        f"generated_at_utc: {registry.get('generated_at_utc')}",
        f"source_worker_key: {registry.get('source_worker_key')}",
        f"decision_recommendation: {registry.get('decision_recommendation')}",
        f"queue_write_allowed: {registry.get('queue_write_allowed')}",
        f"controlled_execution_allowed: {registry.get('controlled_execution_allowed')}",
        "",
        "| Route | Status | Missing fields | Blocked fields | Next step |",
        "|---|---|---|---|---|",
    ]
    for route in registry.get("routes") or []:
        lines.append(
            "| {route_id} | {status} | {missing} | {blocked} | {next_step} |".format(
                route_id=route.get("route_id"),
                status=route.get("route_status"),
                missing=", ".join(route.get("missing_fields") or []),
                blocked=", ".join(route.get("blocked_fields") or []),
                next_step=route.get("recommended_next_step"),
            )
        )
    return "\n".join(lines).rstrip() + "\n"


def write_route_registry(registry: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "autonomous_strategy_routes.json"
    md_path = out / "autonomous_strategy_routes.md"
    json_path.write_text(json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(route_registry_to_markdown(registry), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def _string_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []
