#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from factor_lab.harvest_evidence import write_evidence_ledger


def main() -> int:
    parser = argparse.ArgumentParser(description="Write Harvest evidence ledger from run artifacts.")
    parser.add_argument("--root", default=".")
    parser.add_argument("--cycle-id", default="cycle_0001")
    args = parser.parse_args()
    ledger = write_evidence_ledger(root=args.root, cycle_id=args.cycle_id)
    print(json.dumps(ledger, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
