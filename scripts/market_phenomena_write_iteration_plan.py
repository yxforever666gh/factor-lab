#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_iteration_plan import (
    build_agent_iteration_plan,
    validate_agent_iteration_plan,
    write_agent_iteration_plan,
)


def read_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Write agent-generated market phenomena iteration plan artifacts.")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--worker-contract", default="artifacts/market_phenomena/worker_contract.json")
    parser.add_argument("--output-dir", default="artifacts/market_phenomena")
    args = parser.parse_args()

    run_id = args.run_id or "agent_iteration_plan_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    contract = read_json(args.worker_contract)
    plan = build_agent_iteration_plan(run_id=run_id, worker_contract=contract)
    validation = validate_agent_iteration_plan(plan, contract)
    if validation["decision"] != "keep":
        raise SystemExit(f"invalid agent iteration plan: {validation}")
    paths = write_agent_iteration_plan(plan, args.output_dir)
    print(f"wrote {paths['plan_json']}")
    print(f"wrote {paths['plan_markdown']}")
    print(f"wrote {paths['execution_request_json']}")
    print(f"wrote {paths['verification_checklist_markdown']}")
    print(f"phenomenon_id {plan['phenomenon_id']}")
    print(f"controlled_execution_allowed {plan['controlled_execution_allowed']}")
    print(f"production_queue_allowed {plan['production_boundaries']['queue_write_allowed']}")


if __name__ == "__main__":
    main()
