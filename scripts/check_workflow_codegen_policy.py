from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
CODING_POLICY_PATH = ROOT / "configs" / "research_coding_policy.json"
WORKFLOW_PATH = ROOT / "src" / "factor_lab" / "workflow.py"
ANALYTICS_PATH = ROOT / "src" / "factor_lab" / "analytics.py"
PORTFOLIO_PATH = ROOT / "src" / "factor_lab" / "portfolio.py"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def run_checks() -> dict:
    policy = json.loads(CODING_POLICY_PATH.read_text(encoding="utf-8")) if CODING_POLICY_PATH.exists() else {}
    workflow = _read(WORKFLOW_PATH)
    analytics = _read(ANALYTICS_PATH)
    portfolio = _read(PORTFOLIO_PATH)

    findings: list[dict] = []

    workflow_apply_factor_count = workflow.count("apply_factor(")
    analytics_apply_factor_count = analytics.count("apply_factor(")
    portfolio_apply_factor_count = portfolio.count("apply_factor(")
    workflow_copy_count = workflow.count("dataset.frame.copy()")

    if workflow_apply_factor_count > 2:
        findings.append({
            "severity": "warning",
            "rule": "repeated_apply_factor_for_same_definition",
            "message": f"workflow.py contains {workflow_apply_factor_count} apply_factor calls; review cache reuse.",
        })
    if analytics_apply_factor_count > 1:
        findings.append({
            "severity": "warning",
            "rule": "split_or_rolling_recompute_factor_values",
            "message": f"analytics.py contains {analytics_apply_factor_count} apply_factor calls; ensure split/rolling reuse cached values when possible.",
        })
    if portfolio_apply_factor_count > 1:
        findings.append({
            "severity": "warning",
            "rule": "portfolio_recomputes_existing_signals",
            "message": f"portfolio.py contains {portfolio_apply_factor_count} apply_factor calls; prefer shared signal inputs.",
        })
    if workflow_copy_count > 1:
        findings.append({
            "severity": "warning",
            "rule": "full_dataframe_copy_inside_factor_loop",
            "message": f"workflow.py contains {workflow_copy_count} dataset.frame.copy() calls; review local-frame alternatives.",
        })

    return {
        "policy_name": policy.get("name"),
        "checked_files": [str(WORKFLOW_PATH), str(ANALYTICS_PATH), str(PORTFOLIO_PATH)],
        "findings": findings,
        "summary": {
            "workflow_apply_factor_count": workflow_apply_factor_count,
            "analytics_apply_factor_count": analytics_apply_factor_count,
            "portfolio_apply_factor_count": portfolio_apply_factor_count,
            "workflow_copy_count": workflow_copy_count,
        },
    }


if __name__ == "__main__":
    result = run_checks()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result["findings"]:
        raise SystemExit(1)
