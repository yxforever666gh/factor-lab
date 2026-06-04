#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_approval_summary import write_operator_approval_summary

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GATE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "manual_approval_gate.json"
DEFAULT_MANUAL_REVIEW_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_approval_summary.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_approval_summary.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write small institutional operator approval summary.")
    parser.add_argument("--gate-path", default=str(DEFAULT_GATE_PATH))
    parser.add_argument("--manual-review-path", default=str(DEFAULT_MANUAL_REVIEW_PATH))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_operator_approval_summary(
        gate_path=args.gate_path,
        manual_review_path=args.manual_review_path,
        status_path=args.status_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "summary_status": payload.get("summary_status"),
                "approval_required": payload.get("approval_required"),
                "required_decision_axis": payload.get("required_decision_axis"),
                "queue_write_allowed": (payload.get("safety") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("safety") or {}).get("automation_allowed"),
                "live_trading_enabled": (payload.get("safety") or {}).get("live_trading_enabled"),
            },
            ensure_ascii=False,
        )
    )
