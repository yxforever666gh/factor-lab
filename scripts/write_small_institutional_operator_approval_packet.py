#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_approval_packet import write_operator_approval_packet

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATUS_PATH = ROOT / "artifacts" / "small_institutionalization" / "status.json"
DEFAULT_DIAGNOSTICS_PATH = ROOT / "artifacts" / "paper_portfolio" / "portfolio_diagnostics.json"
DEFAULT_DRAWDOWN_BLOCKER_EVIDENCE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_blocker_evidence.json"
DEFAULT_MANUAL_REVIEW_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_manual_review.json"
DEFAULT_INTAKE_VALIDATION_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_intake_validation.json"
DEFAULT_HANDOFF_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_handoff.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_approval_packet.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_approval_packet.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write small institutional operator approval packet.")
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--diagnostics-path", default=str(DEFAULT_DIAGNOSTICS_PATH))
    parser.add_argument("--drawdown-blocker-evidence-path", default=str(DEFAULT_DRAWDOWN_BLOCKER_EVIDENCE_PATH))
    parser.add_argument("--manual-review-path", default=str(DEFAULT_MANUAL_REVIEW_PATH))
    parser.add_argument("--intake-validation-path", default=str(DEFAULT_INTAKE_VALIDATION_PATH))
    parser.add_argument("--handoff-path", default=str(DEFAULT_HANDOFF_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_operator_approval_packet(
        status_path=args.status_path,
        diagnostics_path=args.diagnostics_path,
        drawdown_blocker_evidence_path=args.drawdown_blocker_evidence_path,
        manual_review_path=args.manual_review_path,
        intake_validation_path=args.intake_validation_path,
        handoff_path=args.handoff_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "packet_status": payload.get("packet_status"),
                "missing_artifacts": payload.get("missing_artifacts"),
                "decision_axis": (payload.get("drawdown_blocker") or {}).get("decision_axis"),
                "queue_write_allowed": (payload.get("safety") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("safety") or {}).get("automation_allowed"),
                "execution_allowed": (payload.get("safety") or {}).get("execution_allowed"),
            },
            ensure_ascii=False,
        )
    )
