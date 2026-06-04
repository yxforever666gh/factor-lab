#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from factor_lab.harvest_verdict import write_verdict


def main() -> int:
    parser = argparse.ArgumentParser(description="Write Harvest verdict from evidence ledger.")
    parser.add_argument("--root", default=".")
    parser.add_argument("--cycle-id", default="cycle_0001")
    args = parser.parse_args()
    verdict = write_verdict(root=args.root, cycle_id=args.cycle_id)
    print(json.dumps(verdict, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
