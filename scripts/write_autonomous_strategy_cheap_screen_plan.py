#!/usr/bin/env python3
"""Write preview-only cheap screen plan from route registry."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_cheap_screen_planner import build_cheap_screen_plan, write_cheap_screen_plan

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROUTE_REGISTRY = ROOT / "configs" / "autonomous_strategy_routes.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--route-registry", default=str(DEFAULT_ROUTE_REGISTRY))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args(argv)

    route_registry = json.loads(Path(args.route_registry).read_text(encoding="utf-8"))
    plan = build_cheap_screen_plan(run_id=args.run_id, route_registry=route_registry)
    paths = write_cheap_screen_plan(plan, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "mode": plan["mode"],
        "task_count": plan["task_count"],
        "max_backtests_before_review": plan["max_backtests_before_review"],
        "controlled_execution_allowed": plan["controlled_execution_allowed"],
        "queue_write_allowed": plan["queue_write_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
