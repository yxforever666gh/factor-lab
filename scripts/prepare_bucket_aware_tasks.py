#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks


def _expand_config_globs(patterns: list[str]) -> list[Path]:
    paths: list[Path] = []
    for pattern in patterns:
        paths.extend(Path().glob(pattern))
    return sorted({path for path in paths})


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db-path", default="artifacts/factor_lab.db")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--priority", type=int, default=0)
    parser.add_argument("--force-new", action="store_true")
    parser.add_argument("--config-glob", action="append", default=[])
    parser.add_argument("--route-id", default=None)
    parser.add_argument("--followup-type", default=None)
    parser.add_argument("--value-sleeve-policy", default=None)
    parser.add_argument("config_paths", nargs="*")
    args = parser.parse_args()

    glob_paths = _expand_config_globs(args.config_glob)
    explicit_paths = [Path(p) for p in args.config_paths]
    config_paths = explicit_paths + glob_paths
    result = prepare_bucket_aware_tasks(
        config_paths=config_paths if config_paths else None,
        db_path=args.db_path,
        dry_run=not args.write,
        limit=args.limit,
        priority=args.priority,
        force_new=args.force_new,
        route_id=args.route_id,
        followup_type=args.followup_type,
        value_sleeve_policy_path=args.value_sleeve_policy,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
