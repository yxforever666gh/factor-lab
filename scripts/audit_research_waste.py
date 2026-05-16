#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.research_waste_audit import (  # noqa: E402
    DEFAULT_DB_PATH,
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    write_research_waste_audit,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Factor Lab research waste and duplicate evidence.")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path")
    parser.add_argument("--json", default=str(DEFAULT_JSON_PATH), help="JSON output path")
    parser.add_argument("--markdown", default=str(DEFAULT_MARKDOWN_PATH), help="Markdown output path")
    args = parser.parse_args()

    audit = write_research_waste_audit(db_path=args.db, json_path=args.json, markdown_path=args.markdown)
    print(json.dumps({
        "schema_version": audit.get("schema_version"),
        "json_path": args.json,
        "markdown_path": args.markdown,
        "last_24h_total": ((audit.get("workflow_runs") or {}).get("last_24h") or {}).get("total"),
        "duplicate_ratio_24h": (audit.get("duplicates") or {}).get("duplicate_config_fingerprint_ratio_24h"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
