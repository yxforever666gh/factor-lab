#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.value_route_direction_batch import write_value_route_direction_batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--output-dir", default="artifacts/value_route_direction_batch")
    args = parser.parse_args()
    batch = write_value_route_direction_batch(output_dir=args.output_dir, dry_run=not args.write)
    print(json.dumps({"configs": len(batch["configs"]), "output_dir": args.output_dir, "written": args.write}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
