#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_repair_blocker_manual_review import (
    write_repair_blocker_manual_review,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVIDENCE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.json"
DEFAULT_REPAIR_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
DEFAULT_GROUP_DIAGNOSTIC_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write small institutional repair blocker manual review artifact.")
    parser.add_argument("--evidence-path", default=str(DEFAULT_EVIDENCE_PATH))
    parser.add_argument("--repair-path", default=str(DEFAULT_REPAIR_PATH))
    parser.add_argument("--group-diagnostic-path", default=str(DEFAULT_GROUP_DIAGNOSTIC_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_repair_blocker_manual_review(
        evidence_path=args.evidence_path,
        repair_path=args.repair_path,
        group_diagnostic_path=args.group_diagnostic_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "review_status": payload.get("review_status"),
                "primary_issue": payload.get("primary_issue"),
                "repair_status": payload.get("repair_status"),
                "candidate_count": payload.get("candidate_count"),
                "queue_write_allowed": (payload.get("safety") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("safety") or {}).get("automation_allowed"),
            },
            ensure_ascii=False,
        )
    )
