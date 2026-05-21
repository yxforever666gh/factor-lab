#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.simulated_portfolio_repair_review import write_repair_blocker_review

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPAIR_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_review.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "repair_blocker_review.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write simulated portfolio repair blocker manual-review artifacts.")
    parser.add_argument("--repair-path", default=str(DEFAULT_REPAIR_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_repair_blocker_review(
        repair_path=args.repair_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "review_status": payload.get("review_status"),
                "primary_blocker": payload.get("primary_blocker"),
                "automation_allowed": payload.get("automation_allowed"),
                "manual_decision_required": payload.get("manual_decision_required"),
                "recommended_action": payload.get("recommended_action"),
            },
            ensure_ascii=False,
        )
    )
