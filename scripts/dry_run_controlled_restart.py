#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.controlled_restart_audit import write_controlled_restart_dry_run


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="artifacts/factor_lab.db")
    parser.add_argument("--output-dir", default="artifacts")
    args = parser.parse_args()
    result = write_controlled_restart_dry_run(db_path=args.db_path, output_dir=args.output_dir)
    print(json.dumps({"pending_count": result["pending_count"], "would_run_count": result["would_run_count"], "blocked_count": result["blocked_count"], "json_path": result["json_path"]}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
