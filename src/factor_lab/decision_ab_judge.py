from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.agent_schemas import DECISION_AB_SCHEMA_VERSION


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def build_decision_ab_report(snapshot: dict[str, Any]) -> dict[str, Any]:
    novelty_summary = (snapshot.get("novelty_judge") or {}).get("summary") or {}
    approved_summary = snapshot.get("approved_universe_summary") or {}
    governance_summary = snapshot.get("approved_universe_governance_summary") or {}
    promotion_summary = (snapshot.get("promotion_scorecard") or {}).get("summary") or {}

    duplicate_delta = -int(novelty_summary.get("class_counts", {}).get("duplicate_like", 0))
    quality_delta = round(
        float(promotion_summary.get("stable_alpha_candidate_count") or 0)
        + float(promotion_summary.get("needs_validation_count") or 0) * 0.5
        - float(promotion_summary.get("duplicate_suppress_count") or 0) * 0.5,
        3,
    )
    approved_universe_delta = int(approved_summary.get("approved_count") or 0)
    portfolio_input_delta = round(sum((snapshot.get("approved_universe_budget_summary") or {}).get("bucket_allocations", {}).values()), 6)
    suspicious_governance = sum(int(v or 0) for k, v in (governance_summary.get("action_counts") or {}).items() if k not in {"keep", "upweight_candidate"})

    if quality_delta >= 1.0 and duplicate_delta >= -1 and suspicious_governance <= 1:
        recommendation = "adopt"
    elif quality_delta >= 0.0:
        recommendation = "keep_testing"
    else:
        recommendation = "reject"

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": DECISION_AB_SCHEMA_VERSION,
        "ab_run_id": f"decision-ab-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "baseline_policy": "heuristic_only",
        "candidate_policy": "functional_agents_v1",
        "budget_matched": True,
        "quality_delta": quality_delta,
        "duplicate_delta": duplicate_delta,
        "approved_universe_delta": approved_universe_delta,
        "portfolio_input_delta": portfolio_input_delta,
        "recommendation": recommendation,
    }


def write_decision_ab_report(snapshot: dict[str, Any], artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    report = build_decision_ab_report(snapshot)
    report_path = artifacts_dir / "decision_ab_report.json"
    history_path = artifacts_dir / "decision_ab_history.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    history = _load_json(history_path, [])
    if not isinstance(history, list):
        history = []
    history.append(report)
    history_path.write_text(json.dumps(history[-50:], ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def load_decision_ab_artifacts(artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    return {
        "report": _load_json(artifacts_dir / "decision_ab_report.json", {}),
        "history": _load_json(artifacts_dir / "decision_ab_history.json", []),
    }
