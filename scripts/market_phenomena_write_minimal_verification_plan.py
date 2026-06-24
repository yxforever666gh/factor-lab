#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.market_phenomena_experiment_plan import build_minimal_verification_plan, write_minimal_verification_plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Write minimal verification plan artifacts for market phenomena.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--data-feasibility", default="artifacts/market_phenomena/phenomenon_data_feasibility.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    data_feasibility = json.loads(Path(args.data_feasibility).read_text(encoding="utf-8"))
    run_id = args.run_id or "minimal_verification_plan_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report = build_minimal_verification_plan(run_id=run_id, data_feasibility_review=data_feasibility)
    paths = write_minimal_verification_plan(report, args.output_dir)
    print(f"wrote {paths['json']}")
    print(f"wrote {paths['markdown']}")


if __name__ == "__main__":
    main()
