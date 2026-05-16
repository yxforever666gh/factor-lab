#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.value_route_bucket_aware_batch import write_value_route_bucket_aware_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--include-all-routes", action="store_true")
    parser.add_argument("--output-dir", default="artifacts/value_route_bucket_aware")
    args = parser.parse_args()
    batch = write_value_route_bucket_aware_batch(output_dir=args.output_dir, dry_run=not args.write, include_all_routes=args.include_all_routes)
    print(json.dumps({"configs": len(batch["configs"]), "written": args.write, "output_dir": args.output_dir}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
