#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.simulated_portfolio_construction_repair import (
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_MATRIX_PATH,
    DEFAULT_POLICY_PATH,
    write_simulated_portfolio_construction_repair,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write simulated portfolio construction repair diagnostics.")
    parser.add_argument("--matrix-path", default=str(DEFAULT_MATRIX_PATH))
    parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_simulated_portfolio_construction_repair(
        matrix_path=args.matrix_path,
        policy_path=args.policy_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "repair_status": payload.get("repair_status"),
                "candidate_count": payload.get("candidate_count"),
                "recommended_candidate": payload.get("recommended_candidate"),
                "automation_allowed": payload.get("automation_allowed"),
            },
            ensure_ascii=False,
        )
    )
