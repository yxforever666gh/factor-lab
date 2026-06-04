#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.harvest_executor import run_harvest_cycle


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Harvest cycle Phase 6 execution safely (dry-run default).")
    parser.add_argument("--root", default=".")
    parser.add_argument("--cycle-id", default="cycle_0001")
    parser.add_argument("--plan", help="Path to cycle charter/plan JSON")
    parser.add_argument("--gate", help="Path to gate decision JSON")
    parser.add_argument("--dry-run", action="store_true", default=False)
    parser.add_argument("--allow-controlled-execution", action="store_true", help="Required for controlled local execution")
    parser.add_argument("--max-experiments", type=int)
    args = parser.parse_args()
    root = Path(args.root)
    cycle_dir = root / "artifacts/harvest_agent" / args.cycle_id
    plan_path = Path(args.plan) if args.plan else cycle_dir / "cycle_plan.json"
    if not plan_path.exists():
        alt = cycle_dir / "cycle_charter.json"
        plan_path = alt if alt.exists() else plan_path
    gate_path = Path(args.gate) if args.gate else cycle_dir / "gate_decision.json"
    plan = _load(plan_path) if plan_path.exists() else {"cycle_id": args.cycle_id, "research_budget": {"max_experiments": 1}, "proposals": []}
    gate = _load(gate_path) if gate_path.exists() else {"decision": "allow_dry_run", "allowed_experiments": [p.get("proposal_id") or p.get("experiment_id") for p in plan.get("proposals", [])]}
    result = run_harvest_cycle(plan, gate, root=root, dry_run=args.dry_run, allow_controlled_execution=args.allow_controlled_execution, max_experiments=args.max_experiments)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
