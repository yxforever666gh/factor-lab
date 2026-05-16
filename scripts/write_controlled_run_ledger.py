#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.controlled_run_ledger import write_controlled_run_ledger


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs-root", default="artifacts/value_route_bucket_aware/runs")
    parser.add_argument("--output-dir", default="artifacts")
    args = parser.parse_args()
    result = write_controlled_run_ledger(runs_root=args.runs_root, output_dir=args.output_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
