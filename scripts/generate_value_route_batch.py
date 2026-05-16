#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.value_route_batch import write_value_route_batch

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate value-route OOS validation batch configs.")
    parser.add_argument("--output-dir", default=str(ROOT / "artifacts" / "value_route_batches"))
    parser.add_argument("--write", action="store_true", help="Write manifest/configs. Default is dry-run.")
    args = parser.parse_args()
    batch = write_value_route_batch(output_dir=args.output_dir, dry_run=not args.write)
    print(json.dumps({"dry_run": not args.write, "configs": len(batch["configs"]), "blocked": len(batch["blocked"]), "output_dir": args.output_dir}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
