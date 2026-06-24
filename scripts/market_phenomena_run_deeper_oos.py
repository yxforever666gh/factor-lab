#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.market_phenomena_deeper_oos import (
    build_deeper_oos_horizon_report,
    validate_deeper_oos_horizon_report,
    write_deeper_oos_horizon_report,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def parse_horizons(text: str) -> list[int]:
    return [int(item.strip()) for item in text.split(",") if item.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deeper OOS and holding-horizon diagnostics for a market phenomenon.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--iteration-plan", default="artifacts/market_phenomena/agent_iteration_plan_v2.json")
    parser.add_argument("--feature-frame", default="artifacts/autonomous_strategy_lab/proxy_cheap_screen_feature_frame.csv")
    parser.add_argument("--horizons", default="5,20,60,120")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "deeper_oos_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_deeper_oos_horizon_report(
        run_id=run_id,
        iteration_plan=read_json(args.iteration_plan),
        feature_frame=pd.read_csv(args.feature_frame),
        horizons=parse_horizons(args.horizons),
    )
    validation = validate_deeper_oos_horizon_report(report)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid deeper OOS report: {validation}")
    paths = write_deeper_oos_horizon_report(report, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")
    print(f"decision {report['decision']}")
    print(f"reason_codes {','.join(report['reason_codes'])}")
    print(f"result_count {report['summary']['result_count']}")
    print(f"pass {report['summary'].get('pass', 0)}")
    print(f"fail {report['summary'].get('fail', 0)}")
    print(f"insufficient_sample {report['summary'].get('insufficient_sample', 0)}")
    print(f"queue_write_allowed {report['queue_write_allowed']}")


if __name__ == "__main__":
    main()
