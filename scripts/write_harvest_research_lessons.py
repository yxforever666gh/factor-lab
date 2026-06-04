#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from factor_lab.harvest_research_lessons import write_harvest_research_lessons


if __name__ == "__main__":
    print(json.dumps(write_harvest_research_lessons(ROOT), ensure_ascii=False, indent=2))
