#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.harvest_gate import check_harvest_gate


def main() -> int:
    parser = argparse.ArgumentParser(description="Check deterministic Harvest Agent gate.")
    parser.add_argument("plan_path")
    parser.add_argument("--allow-controlled-execution", action="store_true")
    args = parser.parse_args()
    plan = json.loads(Path(args.plan_path).read_text(encoding="utf-8"))
    decision = check_harvest_gate(plan, reviewer_decision={"decision": "allow"}, allow_controlled_execution=args.allow_controlled_execution)
    out = Path(args.plan_path).parent / "gate_decision.json"
    out.write_text(json.dumps(decision, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(decision, indent=2))
    return 0 if decision["decision"] != "block" else 2


if __name__ == "__main__":
    raise SystemExit(main())
