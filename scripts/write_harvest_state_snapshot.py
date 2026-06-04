#!/usr/bin/env python3
from __future__ import annotations

import argparse

from factor_lab.harvest_state import write_harvest_state_snapshot


def main() -> int:
    parser = argparse.ArgumentParser(description="Write Harvest Agent state snapshot.")
    parser.add_argument("--cycle-id", default="cycle_0001")
    args = parser.parse_args()
    out = write_harvest_state_snapshot(args.cycle_id)
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
