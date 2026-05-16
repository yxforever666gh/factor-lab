from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from factor_lab.runtime_takeover_policy import RuntimeTakeoverPolicy, load_runtime_takeover_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def _rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    return [dict(row) for row in conn.execute("SELECT rowid, * FROM research_tasks WHERE status='pending'").fetchall()]


def quarantine_legacy_pending_tasks(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    policy: RuntimeTakeoverPolicy | None = None,
    dry_run: bool = True,
) -> dict[str, Any]:
    policy = policy or load_runtime_takeover_policy()
    db_path = Path(db_path)
    conn = sqlite3.connect(db_path)
    try:
        pending = _rows(conn)
        blocked: list[dict[str, Any]] = []
        for task in pending:
            decision = policy.evaluate_task(task)
            if decision["decision"] == "block":
                blocked.append({"rowid": task["rowid"], "task_id": task.get("task_id"), "worker_note": task.get("worker_note"), "decision": decision})
        if not dry_run:
            for item in blocked:
                reasons = ",".join(item["decision"].get("reasons") or [])
                suffix = f" | runtime_takeover_quarantined={reasons}"
                conn.execute(
                    "UPDATE research_tasks SET status='failed', last_error=?, worker_note=COALESCE(worker_note,'') || ? WHERE rowid=? AND status='pending'",
                    ("runtime_takeover_quarantined", suffix, item["rowid"]),
                )
            conn.commit()
    finally:
        conn.close()
    return {
        "dry_run": dry_run,
        "pending_count": len(pending),
        "would_quarantine_count": len(blocked),
        "quarantined_count": 0 if dry_run else len(blocked),
        "tasks": blocked,
    }
