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
    parser = argparse.ArgumentParser(description="Run bounded Harvest self-correction evolution loop.")
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--allow-controlled-execution", action="store_true")
    args = parser.parse_args()
    raise SystemExit(retired_legacy_entrypoint("scripts/run_harvest_evolution_loop.py"))
