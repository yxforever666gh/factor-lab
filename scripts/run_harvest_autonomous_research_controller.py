#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from factor_lab.harvest_autonomous_research_controller import run_harvest_autonomous_research_controller
from factor_lab.harvest_controller_policy import HarvestControllerPolicy


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run bounded Harvest v4 autonomous research controller.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--max-cycles", type=int, default=3)
    parser.add_argument("--max-backtests", type=int, default=300)
    parser.add_argument("--max-attempts-per-cycle", type=int, default=5)
    parser.add_argument("--allow-controlled-execution", action="store_true")
    parser.add_argument("--stop-on-data-request", action="store_true", default=True)
    parser.add_argument("--no-stop-on-data-request", dest="stop_on_data_request", action="store_false")
    parser.add_argument("--stop-on-route-stop", action="store_true", default=True)
    parser.add_argument("--no-stop-on-route-stop", dest="stop_on_route_stop", action="store_false")
    parser.add_argument("--stop-on-manual-review", action="store_true", default=True)
    parser.add_argument("--no-stop-on-manual-review", dest="stop_on_manual_review", action="store_false")
    parser.add_argument("--use-latest-strategy-plan", action="store_true")
    parser.add_argument("--use-autonomous-strategy-lab-decision", action="store_true")
    args = parser.parse_args()

    policy = HarvestControllerPolicy(
        max_cycles=args.max_cycles,
        max_backtests=args.max_backtests,
        max_attempts_per_cycle=args.max_attempts_per_cycle,
        allow_controlled_execution=args.allow_controlled_execution,
        stop_on_data_request=args.stop_on_data_request,
        stop_on_route_stop=args.stop_on_route_stop,
        stop_on_manual_review=args.stop_on_manual_review,
    )
    out = run_harvest_autonomous_research_controller(
        root=args.root,
        policy=policy,
        use_latest_strategy_plan=args.use_latest_strategy_plan,
        use_autonomous_strategy_lab_decision=args.use_autonomous_strategy_lab_decision,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2))
