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


def build_factor_quality_effect_report(db_path: str | Path, artifacts_dir: str | Path) -> dict[str, Any]:
    db_path = Path(db_path)
    artifacts_dir = Path(artifacts_dir)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        latest_runs = [dict(row) for row in conn.execute(
            "SELECT created_at_utc, config_path, output_dir, status FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 20"
        ).fetchall()]
        table_names = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "research_tasks" in table_names:
            recent_tasks = [dict(row) for row in conn.execute(
                "SELECT created_at_utc, task_type, status, priority, worker_note, last_error FROM research_tasks ORDER BY created_at_utc DESC LIMIT 50"
            ).fetchall()]
        else:
            recent_tasks = []
    finally:
        conn.close()

    approved = _read_json(artifacts_dir / "approved_candidate_universe.json", {})
    novelty = _read_json(artifacts_dir / "novelty_judge_summary.json", {})
    decision_ab = _read_json(artifacts_dir / "decision_ab_report.json", {})
    promotion = _read_json(artifacts_dir / "promotion_scorecard.json", {})
    consistency = _read_json(artifacts_dir / "artifact_consistency_report.json", {})
    diagnosis = _read_json(artifacts_dir / "au_zero_diagnosis.json", {})

    runtime = {
        "recent_finished_run_count": len([row for row in latest_runs if row.get("status") == "finished"]),
        "recent_failed_run_count": len([row for row in latest_runs if row.get("status") == "failed"]),
        "recent_finished_task_count": len([row for row in recent_tasks if row.get("status") == "finished"]),
        "recent_failed_task_count": len([row for row in recent_tasks if row.get("status") == "failed"]),
        "judgment": "improved" if len([row for row in recent_tasks if row.get("status") == "finished"]) >= len([row for row in recent_tasks if row.get("status") == "failed"]) else "not_improved",
    }
    discovery = {
        "approved_count": (approved.get("summary") or {}).get("approved_count") or 0,
        "novelty_class_counts": novelty.get("class_counts") or {},
        "stable_alpha_candidate_count": (promotion.get("summary") or {}).get("stable_alpha_candidate_count") or 0,
        "needs_validation_count": (promotion.get("summary") or {}).get("needs_validation_count") or 0,
        "judgment": "improved" if ((approved.get("summary") or {}).get("approved_count") or 0) > 0 else "not_improved",
    }
    quality = {
        "decision_ab_recommendation": decision_ab.get("recommendation"),
        "quality_delta": decision_ab.get("quality_delta"),
        "au_zero_direct_cause": (diagnosis.get("summary") or {}).get("direct_cause"),
        "artifact_warning_count": consistency.get("warning_count") or 0,
        "judgment": "improved" if decision_ab.get("recommendation") == "adopt" and ((approved.get("summary") or {}).get("approved_count") or 0) > 0 else "not_proven",
    }
    final_judgment = {
        "runtime": runtime["judgment"],
        "candidate_discovery": discovery["judgment"],
        "factor_quality": quality["judgment"],
    }
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": "factor_lab.factor_quality_effect_report.v1",
        "runtime": runtime,
        "candidate_discovery": discovery,
        "factor_quality": quality,
        "final_judgment": final_judgment,
    }


def write_factor_quality_effect_report(db_path: str | Path, artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    payload = build_factor_quality_effect_report(db_path, artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "factor_quality_effect_report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Factor Quality Effect Report",
        "",
        f"- runtime: {payload['final_judgment']['runtime']}",
        f"- candidate_discovery: {payload['final_judgment']['candidate_discovery']}",
        f"- factor_quality: {payload['final_judgment']['factor_quality']}",
        f"- AU count: {payload['candidate_discovery']['approved_count']}",
        f"- A/B recommendation: {payload['factor_quality']['decision_ab_recommendation']}",
        f"- A/B quality delta: {payload['factor_quality']['quality_delta']}",
        f"- AU zero cause: {payload['factor_quality']['au_zero_direct_cause']}",
        f"- artifact warnings: {payload['factor_quality']['artifact_warning_count']}",
    ]
    (artifacts_dir / "factor_quality_effect_report.md").write_text("\n".join(lines), encoding="utf-8")
    return payload
