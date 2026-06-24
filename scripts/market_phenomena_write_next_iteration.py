#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_mutation_loop import (
    build_next_iteration_from_mutation,
    validate_next_iteration_bundle,
    write_next_iteration_bundle,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate next iteration plan/request from mutation request.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--previous-plan", default="artifacts/market_phenomena/agent_iteration_plan.json")
    parser.add_argument("--mutation-request", default="artifacts/market_phenomena/next_mutation_request.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "next_iteration_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bundle = build_next_iteration_from_mutation(
        run_id=run_id,
        previous_iteration_plan=read_json(args.previous_plan),
        mutation_request=read_json(args.mutation_request),
    )
    validation = validate_next_iteration_bundle(bundle)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid next iteration bundle: {validation}")
    paths = write_next_iteration_bundle(bundle, args.output_dir)
    print(f"wrote {paths['bundle_json']}")
    print(f"wrote {paths['bundle_markdown']}")
    print(f"wrote {paths['plan_json']}")
    print(f"wrote {paths['request_json']}")
    print(f"mutation_action {bundle['mutation_action']}")
    print(f"controlled_research_backtest_allowed {bundle['research_execution_request_v2']['controlled_research_backtest_allowed']}")
    print(f"requested_checks {len(bundle['research_execution_request_v2']['requested_checks'])}")
    print(f"queue_write_allowed {bundle['research_execution_request_v2']['queue_write_allowed']}")


if __name__ == "__main__":
    main()
