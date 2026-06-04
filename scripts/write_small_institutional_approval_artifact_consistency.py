#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_approval_artifact_consistency import write_approval_artifact_consistency

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANUAL_REVIEW_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
DEFAULT_GATE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "manual_approval_gate.json"
DEFAULT_OPERATOR_SUMMARY_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_approval_summary.json"
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "approval_artifact_consistency.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "approval_artifact_consistency.md"


if __name__ == "__main__":
    payload = write_approval_artifact_consistency(
        manual_review_path=DEFAULT_MANUAL_REVIEW_PATH,
        gate_path=DEFAULT_GATE_PATH,
        operator_summary_path=DEFAULT_OPERATOR_SUMMARY_PATH,
        status_path=DEFAULT_STATUS_PATH,
        json_path=DEFAULT_JSON_PATH,
        markdown_path=DEFAULT_MARKDOWN_PATH,
    )
    print(
        json.dumps(
            {
                "consistency_status": payload.get("consistency_status"),
                "inconsistency_count": len(payload.get("inconsistencies") or []),
                "staleness_warning_count": len(payload.get("staleness_warnings") or []),
                "queue_write_allowed": (payload.get("safety_flags") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety_flags") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("safety_flags") or {}).get("automation_allowed"),
                "automated_rerun_allowed": (payload.get("safety_flags") or {}).get("automated_rerun_allowed"),
                "live_trading_enabled": (payload.get("safety_flags") or {}).get("live_trading_enabled"),
            },
            ensure_ascii=False,
        )
    )
