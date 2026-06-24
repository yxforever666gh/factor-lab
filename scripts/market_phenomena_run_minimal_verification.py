#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from factor_lab.market_phenomena_experiment_runner import build_minimal_verification_result, write_minimal_verification_result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run minimal non-trading verification experiments for market phenomena.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--plan", default="artifacts/market_phenomena/minimal_verification_plan.json")
    parser.add_argument("--feature-frame", default="artifacts/autonomous_strategy_lab/quality_profit_proxy_feature_frame_with_pit.csv")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    plan = json.loads(Path(args.plan).read_text(encoding="utf-8"))
    feature_frame = pd.read_csv(args.feature_frame)
    run_id = args.run_id or "minimal_verification_result_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_minimal_verification_result(run_id=run_id, plan_report=plan, feature_frame=feature_frame)
    report["feature_frame_path"] = args.feature_frame
    paths = write_minimal_verification_result(report, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()
