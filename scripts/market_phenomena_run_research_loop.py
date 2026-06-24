#!/usr/bin/env python
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from factor_lab.market_phenomena_research_loop import run_research_loop, write_research_loop_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the artifact-only market phenomena research loop.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--feature-frame", default="artifacts/autonomous_strategy_lab/proxy_cheap_screen_feature_frame.csv")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    feature_frame = pd.read_csv(args.feature_frame)
    run_id = args.run_id or "market_phenomena_loop_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = run_research_loop(run_id=run_id, feature_frame=feature_frame, output_dir=args.output_dir)
    report["feature_frame_path"] = args.feature_frame
    paths = write_research_loop_report(report, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()
