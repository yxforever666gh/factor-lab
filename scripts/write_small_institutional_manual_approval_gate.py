#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_manual_approval_gate import write_manual_approval_gate

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANUAL_REVIEW_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "manual_approval_gate.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "manual_approval_gate.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write small institutional manual approval gate artifact.")
    parser.add_argument("--manual-review-path", default=str(DEFAULT_MANUAL_REVIEW_PATH))
    parser.add_argument("--approval-path", default=None)
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_manual_approval_gate(
        manual_review_path=args.manual_review_path,
        approval_path=args.approval_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "gate_status": payload.get("gate_status"),
                "human_approval_present": payload.get("human_approval_present"),
                "risk_relaxation_allowed": payload.get("risk_relaxation_allowed"),
                "automated_rerun_allowed": payload.get("automated_rerun_allowed"),
                "queue_write_allowed": payload.get("queue_write_allowed"),
                "broad_daemon_allowed": payload.get("broad_daemon_allowed"),
                "automation_allowed": payload.get("automation_allowed"),
            },
            ensure_ascii=False,
        )
    )
