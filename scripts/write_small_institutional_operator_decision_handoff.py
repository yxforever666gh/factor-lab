#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_decision_handoff import write_operator_decision_handoff

ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_DIR = ROOT / "artifacts" / "small_institutional_simulation"
DEFAULT_INTAKE_VALIDATION_PATH = ARTIFACT_DIR / "operator_decision_intake_validation.json"
DEFAULT_MANUAL_APPROVAL_GATE_PATH = ARTIFACT_DIR / "manual_approval_gate.json"
DEFAULT_REPAIR_BLOCKER_REVIEW_PATH = ARTIFACT_DIR / "repair_blocker_manual_review.json"
DEFAULT_APPROVAL_CONSISTENCY_PATH = ARTIFACT_DIR / "approval_artifact_consistency.json"
DEFAULT_JSON_PATH = ARTIFACT_DIR / "operator_decision_handoff.json"
DEFAULT_MARKDOWN_PATH = ARTIFACT_DIR / "operator_decision_handoff.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write non-mutating operator decision handoff artifact.")
    parser.add_argument("--intake-validation-path", default=str(DEFAULT_INTAKE_VALIDATION_PATH))
    parser.add_argument("--manual-approval-gate-path", default=str(DEFAULT_MANUAL_APPROVAL_GATE_PATH))
    parser.add_argument("--repair-blocker-review-path", default=str(DEFAULT_REPAIR_BLOCKER_REVIEW_PATH))
    parser.add_argument("--approval-consistency-path", default=str(DEFAULT_APPROVAL_CONSISTENCY_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_operator_decision_handoff(
        intake_validation_path=args.intake_validation_path,
        manual_approval_gate_path=args.manual_approval_gate_path,
        repair_blocker_review_path=args.repair_blocker_review_path,
        approval_consistency_path=args.approval_consistency_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "handoff_status": payload.get("handoff_status"),
                "intake_status": payload.get("intake_status"),
                "decision_type": payload.get("decision_type"),
                "execution_allowed": payload.get("execution_allowed"),
                "separate_execution_plan_required": payload.get("separate_execution_plan_required"),
                "queue_write_allowed": (payload.get("safety") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("safety") or {}).get("automation_allowed"),
                "live_trading_enabled": (payload.get("safety") or {}).get("live_trading_enabled"),
            },
            ensure_ascii=False,
        )
    )
