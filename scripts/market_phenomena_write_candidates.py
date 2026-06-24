#!/usr/bin/env python
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from factor_lab.market_phenomena_generator import build_seed_candidates_report
from factor_lab.market_phenomena_schema import write_candidates_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Write market phenomenon candidate artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--market", default="cn_equity_daily")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "phenomena_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_seed_candidates_report(run_id=run_id, market=args.market)
    paths = write_candidates_report(report, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()
