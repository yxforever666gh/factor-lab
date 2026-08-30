#!/usr/bin/env python
"""Synchronize immutable Tushare report_rc date partitions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from factor_lab.data.analyst import sync_analyst_reports  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--config", default="configs/data.json")
    parser.add_argument("--raw-root")
    parser.add_argument(
        "--request-rate-per-minute",
        type=float,
        default=1.0,
        help="Defaults to the currently observed report_rc trial rate.",
    )
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args(argv)
    if args.request_rate_per_minute <= 0:
        parser.error("--request-rate-per-minute must be positive")
    result = sync_analyst_reports(
        args.start_date,
        args.end_date,
        config_path=args.config,
        raw_root=args.raw_root,
        request_rate_per_minute=args.request_rate_per_minute,
        resume=not args.no_resume,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result.get("status") == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
