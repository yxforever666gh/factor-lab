#!/usr/bin/env python3
"""Write controlled execution adapter decision for Autonomous Strategy Lab."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.autonomous_strategy_execution_adapter import (
    build_controlled_execution_decision,
    write_execution_decision,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHEAP_SCREEN_PLAN = ROOT / "artifacts" / "autonomous_strategy_lab" / "cheap_screen_plan.json"
DEFAULT_COVERAGE_PREFLIGHT = ROOT / "artifacts" / "autonomous_strategy_lab" / "historical_valuation_coverage_preflight.json"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "autonomous_strategy_lab"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--cheap-screen-plan", default=str(DEFAULT_CHEAP_SCREEN_PLAN))
    parser.add_argument("--coverage-preflight", default=str(DEFAULT_COVERAGE_PREFLIGHT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--allow-controlled-execution", action="store_true")
    parser.add_argument("--max-backtests", type=int, default=1)
    parser.add_argument("--policy-cap", type=int, default=1)
    args = parser.parse_args(argv)

    cheap_screen_plan = json.loads(Path(args.cheap_screen_plan).read_text(encoding="utf-8"))
    coverage_preflight = json.loads(Path(args.coverage_preflight).read_text(encoding="utf-8"))
    decision = build_controlled_execution_decision(
        run_id=args.run_id,
        cheap_screen_plan=cheap_screen_plan,
        coverage_preflight=coverage_preflight,
        allow_controlled_execution=args.allow_controlled_execution,
        max_backtests_requested=args.max_backtests,
        policy_cap=args.policy_cap,
    )
    paths = write_execution_decision(decision, args.output_dir)
    print(json.dumps({
        "run_id": args.run_id,
        "execution_status": decision["execution_status"],
        "reason_codes": decision["reason_codes"],
        "controlled_execution_started": decision["controlled_execution_started"],
        "controlled_execution_allowed": decision["controlled_execution_allowed"],
        "max_backtests_allowed": decision["max_backtests_allowed"],
        "queue_write_allowed": decision["queue_write_allowed"],
        "timer_enable_allowed": decision["timer_enable_allowed"],
        "json_path": str(paths["json"].relative_to(ROOT)) if paths["json"].is_relative_to(ROOT) else str(paths["json"]),
        "markdown_path": str(paths["markdown"].relative_to(ROOT)) if paths["markdown"].is_relative_to(ROOT) else str(paths["markdown"]),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
