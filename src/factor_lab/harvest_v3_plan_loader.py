from __future__ import annotations

import json
from pathlib import Path
from typing import Any


HARVEST_ROOT = Path("artifacts/harvest_agent")


def load_latest_v3_next_plan(root: str | Path = ".") -> dict[str, Any] | None:
    root = Path(root)
    base = root / HARVEST_ROOT
    latest_path = base / "latest_cycle.json"
    if not latest_path.exists():
        return None
    try:
        latest = json.loads(latest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    cycle_id = latest.get("cycle_id")
    if not cycle_id:
        return None
    plan_path = base / str(cycle_id) / "v3_next_cycle_plan.json"
    if not plan_path.exists():
        return None
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["_source_path"] = str(plan_path)
    plan["_source_cycle_id"] = str(cycle_id)
    return plan


def classify_v3_next_plan(plan: dict[str, Any]) -> dict[str, str]:
    status = str(plan.get("plan_status") or "")
    if status == "blocked":
        return {"decision": "block", "reason": "plan_status_blocked"}
    if status == "stopped":
        return {"decision": "stop", "reason": "plan_status_stopped"}
    if bool(plan.get("manual_approval_required")):
        return {"decision": "manual_review", "reason": "manual_approval_required"}
    if status == "planned":
        return {"decision": "executable", "reason": "planned"}
    return {"decision": "block", "reason": "plan_status_unknown"}
