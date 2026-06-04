#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.simulated_portfolio_drawdown_group_diagnostic import write_simulated_portfolio_drawdown_group_diagnostic

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.json"
DEFAULT_POLICY_PATH = ROOT / "configs" / "small_institutional_simulation_policy.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "drawdown_group_diagnostic.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write bounded drawdown group diagnostics for simulated portfolio matrix results.")
    parser.add_argument("--matrix-path", default=str(DEFAULT_MATRIX_PATH))
    parser.add_argument("--policy-path", default=str(DEFAULT_POLICY_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_simulated_portfolio_drawdown_group_diagnostic(
        matrix_path=args.matrix_path,
        policy_path=args.policy_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    recommendation = payload.get("recommended_manual_axis") or {}
    print(
        json.dumps(
            {
                "diagnostic_status": payload.get("diagnostic_status"),
                "drawdown_limit": payload.get("drawdown_limit"),
                "recommended_dimension": recommendation.get("dimension"),
                "recommended_value": recommendation.get("value"),
                "automation_allowed": payload.get("automation_allowed"),
            },
            ensure_ascii=False,
        )
    )
