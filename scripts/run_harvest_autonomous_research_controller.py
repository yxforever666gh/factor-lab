#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint


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

    raise SystemExit(
        retired_legacy_entrypoint(
            "scripts/run_harvest_autonomous_research_controller.py"
        )
    )
