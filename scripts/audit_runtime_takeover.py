#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from factor_lab.runtime_takeover_audit import write_runtime_takeover_audit

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit whether Factor Lab runtime is still dominated by old low-value paths.")
    parser.add_argument("--db", default=str(ROOT / "artifacts" / "factor_lab.db"))
    parser.add_argument("--json", default=str(ROOT / "artifacts" / "runtime_takeover_audit.json"))
    parser.add_argument("--markdown", default=str(ROOT / "artifacts" / "runtime_takeover_audit.md"))
    args = parser.parse_args()
    payload = write_runtime_takeover_audit(db_path=args.db, json_path=args.json, markdown_path=args.markdown)
    print(f"wrote {args.json} and {args.markdown}; recommendations={payload.get('recommendations')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
