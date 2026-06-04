#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.harvest_agent_policy import load_harvest_agent_policy
from factor_lab.harvest_planner import build_harvest_cycle_plan, write_harvest_cycle_plan
from factor_lab.harvest_state import build_harvest_state_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Write Harvest Agent cycle charter and proposals.")
    parser.add_argument("--cycle-id", default="cycle_0001")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    policy = load_harvest_agent_policy()
    state = build_harvest_state_snapshot()
    plan = build_harvest_cycle_plan(state, cycle_id=args.cycle_id, policy=policy)
    if args.dry_run:
        print(json.dumps(plan, indent=2))
    else:
        paths = write_harvest_cycle_plan(plan)
        print(paths["charter_path"])
        print(paths["proposals_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
