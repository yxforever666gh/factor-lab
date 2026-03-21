from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from factor_lab.storage import ExperimentStore


MAX_CONSECUTIVE_FAILURES = 3
CIRCUIT_OPEN_COOLDOWN_MINUTES = 5
EXPLORATION_NO_GAIN_THRESHOLD = 3
TASK_REPEAT_COOLDOWN_MINUTES = 180


def parse_iso_utc(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def queue_budget_snapshot(store: ExperimentStore) -> dict[str, int]:
    tasks = store.list_research_tasks(limit=200)
    counts = {"baseline": 0, "validation": 0, "exploration": 0}
    for task in tasks:
        if task["status"] not in {"pending", "running"}:
            continue
        note = task.get("worker_note") or ""
        if note.startswith("baseline"):
            counts["baseline"] += 1
        elif note.startswith("validation"):
            counts["validation"] += 1
        elif note.startswith("exploration"):
            counts["exploration"] += 1
    return counts


def recent_failure_stats(store: ExperimentStore, limit: int = 20) -> dict[str, Any]:
    tasks = store.list_research_tasks(limit=limit)
    consecutive_failures = 0
    last_failure_at = None
    for task in tasks:
        if task["status"] == "failed":
            consecutive_failures += 1
            if last_failure_at is None:
                last_failure_at = parse_iso_utc(task.get("finished_at_utc")) or parse_iso_utc(task.get("created_at_utc"))
        elif task["status"] == "finished":
            break
    failed_recently = len([t for t in tasks[:10] if t["status"] == "failed"])
    cooldown_active = False
    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES and last_failure_at is not None:
        cooldown_active = datetime.now(timezone.utc) - last_failure_at < timedelta(minutes=CIRCUIT_OPEN_COOLDOWN_MINUTES)
    return {
        "consecutive_failures": consecutive_failures,
        "failed_recently": failed_recently,
        "last_failure_at": last_failure_at.isoformat() if last_failure_at else None,
        "cooldown_active": cooldown_active,
    }


def recently_finished_same_fingerprint(store: ExperimentStore, fingerprint: str, cooldown_minutes: int = TASK_REPEAT_COOLDOWN_MINUTES) -> bool:
    tasks = store.list_research_tasks(limit=300)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
    for task in tasks:
        if task.get("fingerprint") != fingerprint:
            continue
        if task.get("status") != "finished":
            continue
        finished_at = parse_iso_utc(task.get("finished_at_utc"))
        if finished_at and finished_at >= cutoff:
            return True
    return False


def exploration_health(store: ExperimentStore, limit: int = 50) -> dict[str, Any]:
    tasks = store.list_research_tasks(limit=limit)
    exploration_tasks = [t for t in tasks if t["task_type"] == "generated_batch" and t["status"] == "finished"]
    recent_no_gain = 0
    recent_gain = 0
    for task in exploration_tasks[:EXPLORATION_NO_GAIN_THRESHOLD]:
        note = task.get("worker_note") or ""
        if "no_significant_information_gain" in note:
            recent_no_gain += 1
        elif "knowledge_gain=" in note:
            recent_gain += 1
    return {
        "recent_no_gain": recent_no_gain,
        "recent_gain": recent_gain,
        "should_throttle": recent_no_gain >= EXPLORATION_NO_GAIN_THRESHOLD,
    }
