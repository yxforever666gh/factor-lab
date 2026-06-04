#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from factor_lab.harvest_strategy_governor import HARVEST_ROOT, load_latest_strategy_plan


def inspect_harvest_strategy_status(root: str | Path = ".") -> dict[str, Any]:
    root = Path(root)
    pointer_path = root / HARVEST_ROOT / "latest_strategy_run.json"
    if not pointer_path.exists():
        return {"schema_version": 1, "strategy_status": "missing", "latest_strategy_run": None}
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"schema_version": 1, "strategy_status": "invalid_pointer", "error": str(exc)}
    plan = load_latest_strategy_plan(root)
    if plan is None:
        return {"schema_version": 1, "strategy_status": "missing_plan", "latest_strategy_run": pointer}
    return {
        "schema_version": 1,
        "strategy_status": "available",
        "strategy_run_id": plan.get("strategy_run_id") or pointer.get("strategy_run_id"),
        "strategy_decision": plan.get("strategy_decision"),
        "plan_status": plan.get("plan_status"),
        "based_on_cycle_id": plan.get("based_on_cycle_id"),
        "based_on_controller_run_id": plan.get("based_on_controller_run_id"),
        "reason_codes": plan.get("reason_codes") or [],
        "manual_approval_required": bool(plan.get("manual_approval_required")),
        "safety": plan.get("safety") or {},
        "latest_strategy_run": pointer,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect latest Harvest v5 strategy plan status.")
    parser.add_argument("--root", default=str(ROOT))
    args = parser.parse_args()
    print(json.dumps(inspect_harvest_strategy_status(args.root), ensure_ascii=False, indent=2, sort_keys=True))
