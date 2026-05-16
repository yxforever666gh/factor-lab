#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from factor_lab.controlled_admission_feeder import load_feeder_config, resolve_feeder_policy, run_controlled_admission_feeder


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--config", default="configs/controlled_admission_feeder.json")
    parser.add_argument("--profile", choices=["conservative", "balanced", "probe"])
    parser.add_argument("--allow-force-new-probe", action="store_true")
    parser.add_argument("--db-path", default="artifacts/factor_lab.db")
    parser.add_argument("--output-dir", default="artifacts/controlled_admission_feeder")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--priority", type=int)
    parser.add_argument("--cooldown-minutes", type=int)
    parser.add_argument("--daily-budget", type=int)
    parser.add_argument("--force-new", action="store_true")
    args = parser.parse_args()
    config = load_feeder_config(args.config)
    overrides = {
        "profile": args.profile,
        "limit": args.limit,
        "priority": args.priority,
        "cooldown_minutes": args.cooldown_minutes,
        "daily_budget": args.daily_budget,
        "force_new": True if args.force_new else None,
    }
    policy = resolve_feeder_policy(config, cli_overrides=overrides, allow_force_new_probe=args.allow_force_new_probe)
    result = run_controlled_admission_feeder(
        db_path=args.db_path,
        output_dir=args.output_dir,
        write=args.write,
        limit=policy["limit"],
        priority=policy["priority"],
        cooldown_minutes=policy["cooldown_minutes"],
        daily_budget=policy["daily_budget"],
        force_new=policy["force_new"],
        profile=policy["profile"],
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
