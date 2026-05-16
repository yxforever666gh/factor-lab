#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.value_route_followup_batch import write_value_route_followup_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--output-dir", default="artifacts/value_route_followups")
    parser.add_argument("--source-dir", default="artifacts/value_route_bucket_aware")
    parser.add_argument("--route-policy-path", default="artifacts/controlled_route_policy.json")
    args = parser.parse_args()

    result = write_value_route_followup_batch(
        output_dir=Path(args.output_dir),
        dry_run=not args.write,
        route_policy_path=Path(args.route_policy_path),
        source_dir=Path(args.source_dir),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
