from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return {}


def _load_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8") if path.exists() else ""
    except Exception:
        return ""


def _promoted_routes(route_policy: dict[str, Any]) -> list[str]:
    routes: list[str] = []
    for item in route_policy.get("promoted_routes") or route_policy.get("routes") or []:
        if isinstance(item, str):
            routes.append(item)
        elif item.get("oos_status") in {"bucket_aware_pass", "pass"} or item.get("status") == "promoted":
            routes.append(str(item.get("route_id") or item.get("mechanism_id") or item.get("name")))
    return sorted({r for r in routes if r and r != "None"})


def build_harvest_state_snapshot(*, root: str | Path = ROOT) -> dict[str, Any]:
    root = Path(root)
    route_policy = _load_json(root / "artifacts/controlled_route_policy.json")
    quality = _load_json(root / "artifacts/research_quality_summary.json")
    ledger = _load_json(root / "artifacts/controlled_run_ledger_summary.json")
    data_blockers = _load_json(root / "knowledge/data_blockers.json")
    latest_auto = _load_json(root / "artifacts/autonomous_research_loop/latest_cycle.json")
    latest_harvest = _load_json(root / "artifacts/harvest_agent/latest_cycle.json")
    audit = _load_json(root / "artifacts/runtime_takeover_audit.json")
    blockers: list[str] = []
    if audit.get("polluted_paths") or "generated/" in str(audit).lower() or "broad" in str(audit).lower():
        blockers.append("old_path_pollution_detected")
    for source in (quality, ledger):
        for key in ("current_blockers", "blockers"):
            if isinstance(source.get(key), list):
                blockers.extend(str(x) for x in source[key])
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "research_quality_summary": quality,
        "controlled_route_policy": route_policy,
        "controlled_run_ledger_summary": ledger,
        "mechanism_lessons": _load_text(root / "knowledge/mechanism_lessons.md"),
        "data_blockers": data_blockers,
        "research_waste": _load_text(root / "knowledge/research_waste.md"),
        "autonomous_research_loop_knowledge": _load_text(root / "knowledge/autonomous_research_loop.md"),
        "latest_autonomous_cycle": latest_auto,
        "latest_harvest_cycle": latest_harvest,
        "latest_harvest_verdict": latest_harvest.get("verdict"),
        "promoted_bucket_aware_routes": _promoted_routes(route_policy),
        "current_blockers": sorted(set(blockers)),
    }


def write_harvest_state_snapshot(cycle_id: str, *, root: str | Path = ROOT) -> Path:
    out = Path(root) / "artifacts/harvest_agent" / cycle_id / "state_snapshot.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(build_harvest_state_snapshot(root=root), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out
