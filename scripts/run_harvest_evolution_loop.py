#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from factor_lab.harvest_evolution_loop import run_harvest_evolution_loop


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run bounded Harvest self-correction evolution loop.")
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--allow-controlled-execution", action="store_true")
    args = parser.parse_args()
    out = run_harvest_evolution_loop(root=ROOT, cycles=args.cycles, allow_controlled_execution=args.allow_controlled_execution)
    print(json.dumps(out, ensure_ascii=False, indent=2))
