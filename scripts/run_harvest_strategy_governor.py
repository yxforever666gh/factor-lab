#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from factor_lab.harvest_strategy_governor import run_harvest_strategy_governor


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run deterministic Harvest v5 research strategy governor.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--lookback-cycles", type=int, default=8)
    parser.add_argument("--max-next-backtests", type=int, default=120)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    out = run_harvest_strategy_governor(
        root=args.root,
        lookback_cycles=args.lookback_cycles,
        max_next_backtests=args.max_next_backtests,
        write=args.write,
    )
    print(json.dumps(out, ensure_ascii=False, indent=2, sort_keys=True))
