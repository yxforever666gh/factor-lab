from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
DB_PATH = ARTIFACTS / "factor_lab.db"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def build_research_metrics(
    *,
    db_path: str | Path = DB_PATH,
    memory_path: str | Path = ARTIFACTS / "research_memory.json",
    learning_path: str | Path = ARTIFACTS / "research_learning.json",
    candidate_pool_path: str | Path = ARTIFACTS / "research_candidate_pool.json",
    output_path: str | Path = ARTIFACTS / "research_metrics.json",
) -> dict[str, Any]:
    memory = _read_json(Path(memory_path), {})
    learning = _read_json(Path(learning_path), {})
    candidate_pool = _read_json(Path(candidate_pool_path), {})

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        recent_tasks = [dict(row) for row in conn.execute(
            "select task_id, task_type, status, worker_note, created_at_utc, finished_at_utc, payload_json from research_tasks order by created_at_utc desc limit 120"
        )]
    finally:
        conn.close()

    generated_candidate_tasks = []
    for row in recent_tasks:
        try:
            payload = json.loads(row.get("payload_json") or "{}")
        except Exception:
            payload = {}
        if payload.get("source") == "candidate_generation":
            generated_candidate_tasks.append({**row, "payload": payload})

    execution_feedback = list(memory.get("execution_feedback") or [])[-120:]
    generated_candidate_outcomes = list(memory.get("generated_candidate_outcomes") or [])[-120:]
    representative_reviews = list(memory.get("representative_candidate_reviews") or [])[-120:]
    operator_stats = learning.get("operator_stats") or {}

    duplicate_ratio = None
    candidate_pool_summary = candidate_pool.get("summary") or {}
    quality_priority = candidate_pool_summary.get("quality_priority") or {}
    candidate_count = int((candidate_pool_summary.get("candidate_count") or 0))
    suppressed_count = int((candidate_pool_summary.get("suppressed_candidate_count") or 0))
    if candidate_count + suppressed_count > 0:
        duplicate_ratio = round(suppressed_count / (candidate_count + suppressed_count), 6)

    outcome_counts: dict[str, int] = {}
    for row in execution_feedback:
        key = row.get("outcome_class") or "unknown"
        outcome_counts[key] = int(outcome_counts.get(key) or 0) + 1

    generated_execution_rate = None
    generated_finished = len([row for row in generated_candidate_tasks if row.get("status") == "finished"])
    if generated_candidate_tasks:
        generated_execution_rate = round(generated_finished / len(generated_candidate_tasks), 6)

    now = datetime.now(timezone.utc)
    windows = {}
    for hours in (6, 24):
        cutoff = now - timedelta(hours=hours)
        outcome_rows = [row for row in generated_candidate_outcomes if (_parse_iso(row.get("updated_at_utc")) or now) >= cutoff]
        feedback_rows = [row for row in execution_feedback if (_parse_iso(row.get("updated_at_utc")) or now) >= cutoff]
        task_rows = [row for row in generated_candidate_tasks if (_parse_iso(row.get("created_at_utc")) or now) >= cutoff]
        windows[f"{hours}h"] = {
            "generated_candidate_tasks": len(task_rows),
            "generated_candidate_outcomes": len(outcome_rows),
            "high_value_failure_count": len([row for row in outcome_rows if row.get("outcome_class") == "high_value_failure"]),
            "low_value_repeat_count": len([row for row in outcome_rows if row.get("outcome_class") == "low_value_repeat"]),
            "execution_feedback_count": len(feedback_rows),
        }

    metrics = {
        "generated_candidate_task_count_recent": len(generated_candidate_tasks),
        "generated_candidate_execution_rate_recent": generated_execution_rate,
        "generated_candidate_outcome_count": len(generated_candidate_outcomes),
        "high_value_failure_count_recent": outcome_counts.get("high_value_failure", 0),
        "low_value_repeat_count_recent": outcome_counts.get("low_value_repeat", 0),
        "duplicate_suppression_ratio": duplicate_ratio,
        "representative_review_count": len(representative_reviews),
        "operator_stats": operator_stats,
        "research_mode": learning.get("research_mode") or {},
        "autonomy_profile": learning.get("autonomy_profile") or {},
        "quality_priority_mode": bool(quality_priority.get("quality_priority_mode")),
        "quality_priority_reasons": quality_priority.get("reasons") or [],
        "frontier_evidence_missing_count": len(quality_priority.get("frontier_evidence_missing") or []),
        "frontier_needs_validation_count": len(quality_priority.get("frontier_needs_validation") or []),
        "quality_duplicate_pressure": int(quality_priority.get("duplicate_pressure") or 0),
        "quality_generated_candidate_budget": int(quality_priority.get("generated_candidate_budget") or 0),
        "observation_windows": windows,
    }

    payload = {
        "generated_at_utc": memory.get("updated_at_utc"),
        "metrics": metrics,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
