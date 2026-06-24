#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.market_phenomena_controlled_execution import (
    build_controlled_execution_plan,
    run_controlled_research_execution,
    validate_controlled_execution_plan,
    write_controlled_execution_artifacts,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run controlled market phenomena research execution adapter.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--execution-request", default="artifacts/market_phenomena/research_execution_request.json")
    parser.add_argument("--iteration-plan", default="artifacts/market_phenomena/agent_iteration_plan.json")
    parser.add_argument("--feature-frame", default="artifacts/autonomous_strategy_lab/proxy_cheap_screen_feature_frame.csv")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "controlled_execution_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    execution_request = read_json(args.execution_request)
    iteration_plan = read_json(args.iteration_plan)
    execution_plan = build_controlled_execution_plan(
        run_id=run_id + "_plan",
        execution_request=execution_request,
        iteration_plan=iteration_plan,
    )
    validation = validate_controlled_execution_plan(execution_plan)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid controlled execution plan: {validation}")
    feature_frame = pd.read_csv(args.feature_frame)
    result = run_controlled_research_execution(
        run_id=run_id,
        execution_plan=execution_plan,
        feature_frame=feature_frame,
    )
    paths = write_controlled_execution_artifacts(execution_plan, result, args.output_dir)
    print(f"wrote {paths['plan_json']}")
    print(f"wrote {paths['result_json']}")
    print(f"wrote {paths['result_markdown']}")
    print(f"result_status {result['result_status']}")
    print(f"executed {result['summary']['executed']}")
    print(f"blocked {result['summary']['blocked']}")
    print(f"production_execution_allowed {result['production_execution_allowed']}")
    print(f"queue_write_allowed {result['queue_write_allowed']}")


if __name__ == "__main__":
    main()
