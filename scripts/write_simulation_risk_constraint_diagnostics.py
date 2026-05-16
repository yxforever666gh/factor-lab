#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.simulation_risk_constraint_diagnostics import write_simulation_risk_constraint_diagnostics

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MATRIX_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "matrix.json"
DEFAULT_SELF_DIAGNOSIS_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "self_diagnosis.json"
DEFAULT_REPAIR_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "portfolio_construction_repair.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_constraint_diagnostics.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_constraint_diagnostics.md"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write simulation drawdown risk-constraint diagnostics.")
    parser.add_argument("--matrix-path", default=str(DEFAULT_MATRIX_PATH))
    parser.add_argument("--self-diagnosis-path", default=str(DEFAULT_SELF_DIAGNOSIS_PATH))
    parser.add_argument("--repair-path", default=str(DEFAULT_REPAIR_PATH))
    parser.add_argument("--json-path", default=str(DEFAULT_JSON_PATH))
    parser.add_argument("--markdown-path", default=str(DEFAULT_MARKDOWN_PATH))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    payload = write_simulation_risk_constraint_diagnostics(
        matrix_path=args.matrix_path,
        self_diagnosis_path=args.self_diagnosis_path,
        repair_path=args.repair_path,
        json_path=args.json_path,
        markdown_path=args.markdown_path,
    )
    print(
        json.dumps(
            {
                "diagnostic_status": payload.get("diagnostic_status"),
                "best_available_drawdown": payload.get("best_available_drawdown"),
                "drawdown_threshold": payload.get("drawdown_threshold"),
                "drawdown_gap": payload.get("drawdown_gap"),
                "recommended_safe_next_step": payload.get("recommended_safe_next_step"),
                "automation_allowed": payload.get("automation_allowed"),
            },
            ensure_ascii=False,
        )
    )
