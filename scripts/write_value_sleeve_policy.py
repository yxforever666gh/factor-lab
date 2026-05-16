#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_lab.value_sleeve_policy import write_value_sleeve_policy

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Write value sleeve policy artifacts")
    parser.add_argument("--decision", default=str(ROOT / "artifacts" / "value_sleeve_validation" / "value_sleeve_decision.json"))
    parser.add_argument("--scorecard", default=str(ROOT / "artifacts" / "value_sleeve_validation" / "route_scorecard.json"))
    parser.add_argument("--json", default=str(ROOT / "artifacts" / "value_sleeve_validation" / "value_sleeve_policy.json"))
    parser.add_argument("--markdown", default=str(ROOT / "artifacts" / "value_sleeve_validation" / "value_sleeve_policy.md"))
    args = parser.parse_args()
    policy = write_value_sleeve_policy(json_path=args.json, markdown_path=args.markdown, decision_path=args.decision, scorecard_path=args.scorecard)
    print(json.dumps({
        "wrote": [args.json, args.markdown],
        "decision": policy.get("decision"),
        "primary_route": policy.get("primary_route"),
        "confirmation_route": policy.get("confirmation_route"),
        "low_weight_route": policy.get("low_weight_route"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
