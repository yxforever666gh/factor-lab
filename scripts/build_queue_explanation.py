#!/usr/bin/env python3
"""Build a lightweight explanation for the current Factor Lab research queue."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = PROJECT_ROOT / "artifacts"
DB_PATH = ARTIFACTS / "factor_lab.db"
OUTPUT_PATH = ARTIFACTS / "research_queue_explanation.json"


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def db_snapshot(db_path: Path = DB_PATH) -> dict[str, Any]:
    snapshot: dict[str, Any] = {
        "counts": {"pending": 0, "running": 0, "finished": 0, "failed": 0},
        "recent_failures": [],
        "recent_running": [],
        "recent_pending": [],
        "top_worker_notes": {},
        "error": None,
    }
    if not db_path.exists():
        snapshot["error"] = f"missing db: {db_path}"
        return snapshot
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
        conn.row_factory = sqlite3.Row
        try:
            for row in conn.execute("SELECT status, COUNT(*) AS n FROM research_tasks GROUP BY status"):
                snapshot["counts"][row["status"]] = int(row["n"] or 0)
            for key, status in (("recent_failures", "failed"), ("recent_running", "running"), ("recent_pending", "pending")):
                rows = conn.execute(
                    "SELECT task_id, task_type, status, priority, created_at_utc, started_at_utc, finished_at_utc, worker_note, last_error "
                    "FROM research_tasks WHERE status=? ORDER BY COALESCE(finished_at_utc, started_at_utc, created_at_utc) DESC LIMIT 10",
                    (status,),
                ).fetchall()
                snapshot[key] = [dict(row) for row in rows]
            notes = Counter()
            for row in conn.execute("SELECT worker_note FROM research_tasks WHERE worker_note IS NOT NULL ORDER BY created_at_utc DESC LIMIT 500"):
                note = (row["worker_note"] or "").strip()
                if note:
                    notes[note[:120]] += 1
            snapshot["top_worker_notes"] = dict(notes.most_common(10))
        finally:
            conn.close()
    except Exception as exc:
        snapshot["error"] = str(exc)
    return snapshot


def infer_recommendation(counts: dict[str, int], artifacts: dict[str, Any], db_error: str | None) -> str:
    if db_error:
        return "inspect_db"
    if counts.get("running", 0) > 0:
        return "wait"
    if counts.get("pending", 0) > 0:
        return "wait"
    refill = artifacts.get("research_queue_refill_state") or {}
    stagnation = artifacts.get("research_stagnation") or {}
    if refill.get("recovery_used") or stagnation.get("consecutive_no_injection", 0) >= 2:
        return "repair"
    if refill.get("queue_empty") is True:
        return "reseed"
    return "inspect_llm"


def build_explanation(db_path: Path = DB_PATH, artifacts_dir: Path = ARTIFACTS) -> dict[str, Any]:
    db = db_snapshot(db_path)
    artifact_docs = {
        "research_queue_refill_state": read_json(artifacts_dir / "research_queue_refill_state.json"),
        "research_stagnation": read_json(artifacts_dir / "research_stagnation.json"),
        "research_planner_validated": read_json(artifacts_dir / "research_planner_validated.json"),
        "research_planner_injected": read_json(artifacts_dir / "research_planner_injected.json"),
        "opportunity_execution_plan": read_json(artifacts_dir / "opportunity_execution_plan.json"),
    }
    counts = db.get("counts") or {}
    refill = artifact_docs["research_queue_refill_state"] or {}
    validated = artifact_docs["research_planner_validated"] or {}
    rejected = validated.get("rejected") or validated.get("rejected_tasks") or []
    skip_reasons = Counter()
    for row in rejected:
        reason = row.get("reason") or row.get("rejection_reason") or row.get("status")
        if reason:
            skip_reasons[str(reason)] += 1
    explanation = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "queue_counts": counts,
        "deficits": {
            "validation_deficit": refill.get("validation_deficit"),
            "exploration_deficit": refill.get("exploration_deficit"),
        },
        "last_planner_run": {
            "accepted_count": validated.get("accepted_count"),
            "rejected_count": validated.get("rejected_count"),
            "planner_injected": refill.get("planner_injected"),
        },
        "last_opportunity_injection": {
            "opportunity_injected": refill.get("opportunity_injected"),
            "source": (artifact_docs["opportunity_execution_plan"] or {}).get("source"),
        },
        "top_skip_reasons": dict(skip_reasons.most_common(10)),
        "cooldown_blocked_examples": [row for row in rejected if "cooldown" in json.dumps(row, ensure_ascii=False).lower() or "fingerprint" in json.dumps(row, ensure_ascii=False).lower()][:10],
        "stale_running_tasks": [],
        "recent_running": db.get("recent_running") or [],
        "recent_pending": db.get("recent_pending") or [],
        "recent_failures": db.get("recent_failures") or [],
        "top_worker_notes": db.get("top_worker_notes") or {},
        "artifacts": artifact_docs,
        "recommendation": infer_recommendation(counts, artifact_docs, db.get("error")),
        "db_error": db.get("error"),
    }
    return explanation


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--artifacts-dir", type=Path, default=ARTIFACTS)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()
    explanation = build_explanation(args.db, args.artifacts_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(explanation, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(args.output), "recommendation": explanation.get("recommendation"), "queue_counts": explanation.get("queue_counts")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
