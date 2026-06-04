#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(ROOT / 'src'))

from factor_lab.harvest_knowledge import update_harvest_knowledge


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--cycle-id')
    ap.add_argument('--root', default=str(ROOT))
    args = ap.parse_args()
    print(json.dumps(update_harvest_knowledge(root=args.root, cycle_id=args.cycle_id), ensure_ascii=False, indent=2))
