from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.data_cache import ensure_feature_coverage, read_feature_meta
from factor_lab.timing import WorkflowTiming
from factor_lab.tushare_provider import TushareDataProvider
from factor_lab.universe import default_universe_name


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare and prewarm tushare feature caches.")
    parser.add_argument("--start-date", help="Coverage start date YYYY-MM-DD")
    parser.add_argument("--end-date", help="Coverage end date YYYY-MM-DD")
    parser.add_argument("--window-days", type=int, action="append", default=[], help="Prewarm rolling recent windows ending at --end-date (repeatable)")
    parser.add_argument("--universe-limit", type=int, default=20)
    parser.add_argument("--universe-name")
    parser.add_argument("--cache-dir", default="artifacts/tushare_cache")
    parser.add_argument("--output", default="artifacts/data_prepare_status.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.end_date:
        args.end_date = datetime.utcnow().strftime("%Y-%m-%d")
    if not args.start_date and not args.window_days:
        raise SystemExit("Provide --start-date/--end-date or at least one --window-days")

    provider = TushareDataProvider()
    timing = WorkflowTiming()
    universe_name = args.universe_name or default_universe_name(args.universe_limit)
    prepared = []

    if args.start_date:
        with timing.stage("prepare_explicit_range"):
            ensure_feature_coverage(
                provider=provider,
                universe_limit=args.universe_limit,
                start_date=args.start_date,
                end_date=args.end_date,
                cache_dir=args.cache_dir,
                universe_name=universe_name,
                timing=timing,
            )
        prepared.append({"start_date": args.start_date, "end_date": args.end_date, "kind": "explicit"})

    for days in sorted(set(args.window_days)):
        start_date = (datetime.fromisoformat(args.end_date) - timedelta(days=days)).strftime("%Y-%m-%d")
        with timing.stage(f"prepare_window_{days}d"):
            ensure_feature_coverage(
                provider=provider,
                universe_limit=args.universe_limit,
                start_date=start_date,
                end_date=args.end_date,
                cache_dir=args.cache_dir,
                universe_name=universe_name,
                timing=timing,
            )
        prepared.append({"start_date": start_date, "end_date": args.end_date, "kind": f"window_{days}d"})

    payload = {
        "prepared": prepared,
        "universe_name": universe_name,
        "universe_limit": args.universe_limit,
        "cache_dir": args.cache_dir,
        "feature_meta": read_feature_meta(universe_name, args.cache_dir),
        "timing": timing.snapshot(),
        "updated_at_utc": datetime.utcnow().isoformat() + "Z",
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))


if __name__ == "__main__":
    main()
