#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from factor_lab.harvest_next_plan import write_next_plan


def main() -> int:
    parser = argparse.ArgumentParser(description="Write next Harvest plan from verdict.")
    parser.add_argument("--root", default=".")
    parser.add_argument("--previous-cycle-id", default="cycle_0001")
    args = parser.parse_args()
    plan = write_next_plan(root=args.root, previous_cycle_id=args.previous_cycle_id)
    print(json.dumps(plan, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
