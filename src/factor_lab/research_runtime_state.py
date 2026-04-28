from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from factor_lab.storage import ExperimentStore


MAX_CONSECUTIVE_FAILURES = 3
CIRCUIT_OPEN_COOLDOWN_MINUTES = 5
EXPLORATION_NO_GAIN_THRESHOLD = 3
TASK_REPEAT_COOLDOWN_MINUTES = 180
HIGH_EPISTEMIC_GAIN_MARKERS = {
    "boundary_confirmed",
    "new_branch_opened",
    "repeated_graveyard_confirmed",
    "uncertainty_reduced",
    "stable_candidate_confirmed",
    "candidate_survival_check",
    "exploration_candidate_survived",
}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(1, value)


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


def task_repeat_cooldown_minutes(
    *,
    task_type: str | None = None,
    payload: dict[str, Any] | None = None,
    worker_note: str | None = None,
) -> int:
    payload = payload or {}
    note = (worker_note or "").lower()
    task_name = (task_type or "").strip().lower()
    base = _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_MINUTES", TASK_REPEAT_COOLDOWN_MINUTES)

    cooldown = base
    if task_name == "diagnostic":
        cooldown = _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_DIAGNOSTIC_MINUTES", 45)
    elif task_name == "batch":
        cooldown = _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_BATCH_MINUTES", 120)
    elif task_name == "workflow":
        cooldown = _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_WORKFLOW_MINUTES", base)
    elif task_name == "generated_batch":
        cooldown = _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_GENERATED_BATCH_MINUTES", base)

    expected_gain = {
        str(item).strip()
        for item in (payload.get("expected_information_gain") or payload.get("knowledge_gain") or [])
        if item
    }
    if expected_gain & HIGH_EPISTEMIC_GAIN_MARKERS:
        cooldown = min(cooldown, _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_HIGH_EPISTEMIC_MINUTES", 30))

    opportunity_type = str(payload.get("opportunity_type") or "").strip().lower()
    if opportunity_type in {"diagnose", "confirm", "probe"}:
        cooldown = min(cooldown, _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_LIGHT_REVIEW_MINUTES", 45))

    if "recovery_step" in note:
        cooldown = min(cooldown, _env_int("RESEARCH_TASK_REPEAT_COOLDOWN_RECOVERY_MINUTES", 30))

    return max(1, cooldown)


def recently_finished_same_fingerprint(
    store: ExperimentStore,
    fingerprint: str,
    cooldown_minutes: int | None = None,
    *,
    task_type: str | None = None,
    payload: dict[str, Any] | None = None,
    worker_note: str | None = None,
) -> bool:
    tasks = store.list_research_tasks(limit=300)
    cooldown = cooldown_minutes if cooldown_minutes is not None else task_repeat_cooldown_minutes(task_type=task_type, payload=payload, worker_note=worker_note)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown)
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
