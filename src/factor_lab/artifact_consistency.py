from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _latest_finished_run(db_path: Path) -> dict[str, Any] | None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT run_id, created_at_utc, config_path, output_dir FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _stale(artifact_time: str | None, latest_run_time: str | None) -> bool:
    if not artifact_time or not latest_run_time:
        return False
    try:
        return datetime.fromisoformat(artifact_time) < datetime.fromisoformat(latest_run_time)
    except Exception:
        return False


def build_artifact_consistency_report(db_path: str | Path, artifacts_dir: str | Path) -> dict[str, Any]:
    db_path = Path(db_path)
    artifacts_dir = Path(artifacts_dir)
    latest_run = _latest_finished_run(db_path)
    latest_run_time = (latest_run or {}).get("created_at_utc")

    approved = _read_json(artifacts_dir / "approved_candidate_universe.json", {})
    portfolio = _read_json(artifacts_dir / "paper_portfolio" / "current_portfolio.json", {})
    summary_text = (artifacts_dir / "latest_summary.txt").read_text(encoding="utf-8") if (artifacts_dir / "latest_summary.txt").exists() else ""
    scorecard = _read_json(artifacts_dir / "promotion_scorecard.json", {})
    snapshot = _read_json(artifacts_dir / "research_planner_snapshot.json", {})
    decision_ab = _read_json(artifacts_dir / "decision_ab_report.json", {})

    artifacts = {
        "approved_universe": {
            "generated_at_utc": approved.get("generated_at_utc"),
            "stale_vs_latest_run": _stale(approved.get("generated_at_utc"), latest_run_time),
            "approved_count": (approved.get("summary") or {}).get("approved_count"),
        },
        "paper_portfolio": {
            "generated_at_utc": portfolio.get("generated_at_utc"),
            "stale_vs_latest_run": _stale(portfolio.get("generated_at_utc"), latest_run_time),
            "input_source": (portfolio.get("input_source") or {}).get("source"),
            "approved_count": (portfolio.get("input_source") or {}).get("approved_count"),
        },
        "promotion_scorecard": {
            "generated_at_utc": scorecard.get("generated_at_utc"),
            "stale_vs_latest_run": _stale(scorecard.get("generated_at_utc"), latest_run_time),
            "stable_alpha_candidate_count": (scorecard.get("summary") or {}).get("stable_alpha_candidate_count"),
        },
        "research_planner_snapshot": {
            "generated_at_utc": snapshot.get("generated_at_utc") or snapshot.get("approved_universe", {}).get("generated_at_utc"),
            "stale_vs_latest_run": _stale(snapshot.get("generated_at_utc") or snapshot.get("approved_universe", {}).get("generated_at_utc"), latest_run_time),
            "approved_universe_count": (snapshot.get("approved_universe_summary") or {}).get("approved_count"),
        },
        "decision_ab": {
            "generated_at_utc": decision_ab.get("generated_at_utc"),
            "stale_vs_latest_run": _stale(decision_ab.get("generated_at_utc"), latest_run_time),
            "recommendation": decision_ab.get("recommendation"),
        },
    }

    warnings: list[str] = []
    au_count = artifacts["approved_universe"].get("approved_count")
    portfolio_au_count = artifacts["paper_portfolio"].get("approved_count")
    snapshot_au_count = artifacts["research_planner_snapshot"].get("approved_universe_count")
    if len({x for x in [au_count, portfolio_au_count, snapshot_au_count] if x is not None}) > 1:
        warnings.append("approved_universe_count_mismatch")
    for key, meta in artifacts.items():
        if meta.get("stale_vs_latest_run"):
            warnings.append(f"{key}_stale_vs_latest_run")
    if "Approved Universe：0 个候选" in summary_text and any((x or 0) > 0 for x in [au_count, portfolio_au_count, snapshot_au_count]):
        warnings.append("summary_au_count_mismatch")

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": "factor_lab.artifact_consistency_report.v1",
        "latest_run": latest_run,
        "artifacts": artifacts,
        "warnings": warnings,
        "warning_count": len(warnings),
    }


def write_artifact_consistency_report(db_path: str | Path, artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    payload = build_artifact_consistency_report(db_path, artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "artifact_consistency_report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
