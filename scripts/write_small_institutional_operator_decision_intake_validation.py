#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.small_institutional_operator_decision_intake import write_operator_decision_intake_validation

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INTAKE_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_intake.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_intake_validation.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "operator_decision_intake_validation.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate non-mutating operator decision intake artifact.")
    parser.add_argument("--intake-path", default=str(DEFAULT_INTAKE_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_operator_decision_intake_validation(
        intake_path=args.intake_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "intake_status": payload.get("intake_status"),
                "decision_type": payload.get("decision_type"),
                "validation_error_count": len(payload.get("validation_errors") or []),
                "queue_write_allowed": (payload.get("safety") or {}).get("queue_write_allowed"),
                "broad_daemon_allowed": (payload.get("safety") or {}).get("broad_daemon_allowed"),
                "automation_allowed": (payload.get("safety") or {}).get("automation_allowed"),
                "non_mutating": payload.get("non_mutating"),
            },
            ensure_ascii=False,
        )
    )
