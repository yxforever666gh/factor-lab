#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_quality_cashflow_request import (
    build_quality_cashflow_value_repair_request,
    write_quality_cashflow_value_repair_request,
)

ROOT = Path(__file__).resolve().parents[1]
ASL = ROOT / "artifacts" / "autonomous_strategy_lab"
DEFAULT_CLOSURE = ASL / "industry_cycle_route_closure.json"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write quality-cashflow value-repair mechanism request.")
    parser.add_argument("--run-id", default="quality_cashflow_value_repair_v1")
    parser.add_argument("--route-closure", default=str(DEFAULT_CLOSURE))
    parser.add_argument("--output-dir", default=str(ASL))
    args = parser.parse_args(argv)

    route_closure_path = Path(args.route_closure)
    route_closure = json.loads(route_closure_path.read_text(encoding="utf-8"))
    request = build_quality_cashflow_value_repair_request(run_id=args.run_id, route_closure=route_closure)
    paths = write_quality_cashflow_value_repair_request(request, args.output_dir)
    print(
        json.dumps(
            {
                "mechanism_id": request["mechanism_id"],
                "decision": request["decision"],
                "prerequisite_status": request["prerequisite_status"],
                "recommended_next_step": request["recommended_next_step"],
                "controlled_execution_allowed": request["controlled_execution_allowed"],
                "queue_write_allowed": request["queue_write_allowed"],
                "json_path": str(paths["json"].relative_to(ROOT)),
                "markdown_path": str(paths["markdown"].relative_to(ROOT)),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
