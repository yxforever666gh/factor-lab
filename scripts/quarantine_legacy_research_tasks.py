#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.research_queue_quarantine import quarantine_legacy_pending_tasks

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Quarantine legacy pending research tasks under runtime takeover policy.")
    parser.add_argument("--db", default=str(ROOT / "artifacts" / "factor_lab.db"))
    parser.add_argument("--write", action="store_true", help="Actually mark blocked pending tasks as failed/quarantined. Default is dry-run.")
    args = parser.parse_args()
    result = quarantine_legacy_pending_tasks(db_path=args.db, dry_run=not args.write)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
