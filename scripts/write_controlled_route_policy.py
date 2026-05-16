#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.controlled_route_policy import write_controlled_route_policy


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-path", default="artifacts/controlled_run_ledger_summary.json")
    parser.add_argument("--output-path", default="artifacts/controlled_route_policy.json")
    args = parser.parse_args()
    policy = write_controlled_route_policy(summary_path=args.summary_path, output_path=args.output_path)
    print(json.dumps(policy, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
