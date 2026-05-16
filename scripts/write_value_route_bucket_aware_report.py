#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.value_route_bucket_aware_report import write_bucket_aware_comparison_report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-run-dir", default="artifacts/value_route_controlled_smoke/2_value_quality_no_distress_v2")
    parser.add_argument("--bucket-run-dir", default="artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware")
    parser.add_argument("--output-dir", default="artifacts/value_route_bucket_aware")
    args = parser.parse_args()
    result = write_bucket_aware_comparison_report(original_run_dir=args.original_run_dir, bucket_run_dir=args.bucket_run_dir, output_dir=args.output_dir)
    print(json.dumps({"decision": result["decision"], "json_path": result["json_path"], "markdown_path": result["markdown_path"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
