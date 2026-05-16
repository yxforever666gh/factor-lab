from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

from factor_lab.research_runtime_state import queue_budget_snapshot, recent_failure_stats, parse_iso_utc
from factor_lab.storage import ExperimentStore


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
REPAIR_INCIDENTS_PATH = ARTIFACTS / "repair_incidents.jsonl"
REPAIR_OBSERVATIONS_PATH = ARTIFACTS / "runtime_observations.jsonl"
REPAIR_INCIDENT_STATE_PATH = ARTIFACTS / "runtime_incident_state.json"
REPAIR_FEEDBACK_PATH = ARTIFACTS / "repair_feedback.json"
REPAIR_METRICS_PATH = ARTIFACTS / "repair_metrics.json"
SYSTEM_HEARTBEAT_PATH = ARTIFACTS / "system_heartbeat.jsonl"
RESEARCH_DAEMON_STATUS_PATH = ARTIFACTS / "research_daemon_status.json"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_jsonl(path: Path, limit: int = 50) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines()[-limit:]:
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except Exception:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def load_runtime_incident_state() -> dict[str, Any]:
    return _read_json(REPAIR_INCIDENT_STATE_PATH, {"updated_at_utc": None, "incidents": {}})


def write_runtime_incident_state(payload: dict[str, Any]) -> None:
    payload = dict(payload or {})
    payload["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    _write_json(REPAIR_INCIDENT_STATE_PATH, payload)


def append_runtime_observation(payload: dict[str, Any]) -> None:
    REPAIR_OBSERVATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPAIR_OBSERVATIONS_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def append_repair_incident(payload: dict[str, Any]) -> None:
    REPAIR_INCIDENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPAIR_INCIDENTS_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _task_output_dir(task: dict[str, Any]) -> Path | None:
    payload = task.get("payload") or {}
    output_dir = payload.get("output_dir")
    if not output_dir:
        return None
    return Path(output_dir)


def _looks_like_completed_workflow_output(output_dir: Path) -> bool:
    required = [
        output_dir / "results.json",
        output_dir / "factor_scores.json",
        output_dir / "factor_graveyard.json",
        output_dir / "summary.md",
        output_dir / "portfolio_results.json",
        output_dir / "timing.json",
    ]
    return all(path.exists() for path in required)


def _looks_like_completed_batch_output(output_dir: Path) -> bool:
    return (output_dir / "batch_summary.json").exists() and (output_dir / "batch_comparison.json").exists()


def task_outputs_look_complete(task: dict[str, Any]) -> bool:
    output_dir = _task_output_dir(task)
    if not output_dir or not output_dir.exists():
        return False
    task_type = task.get("task_type")
    if task_type == "workflow":
        return _looks_like_completed_workflow_output(output_dir)
    if task_type in {"batch", "generated_batch"}:
        return _looks_like_completed_batch_output(output_dir)
    return False


def repair_running_task_state_file(task: dict[str, Any], *, status: str, error: str | None) -> None:
    output_dir = _task_output_dir(task)
    if not output_dir:
        return
    task_state_path = output_dir / "task_state.json"
    if not task_state_path.exists():
        return
    try:
        state = json.loads(task_state_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if state.get("status") != "running":
        return
    state["status"] = status
    state["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    state["error"] = error
    task_state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def stale_running_candidates(store: ExperimentStore, *, stale_minutes: int = 10, limit: int = 1000) -> list[dict[str, Any]]:
    if not hasattr(store, "list_research_tasks_by_status"):
        return []
    tasks = store.list_research_tasks_by_status(("running",), limit=limit, oldest_first=True)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
    candidates: list[dict[str, Any]] = []
    for task in tasks:
        started_at = parse_iso_utc(task.get("started_at_utc")) or parse_iso_utc(task.get("created_at_utc"))
        if not started_at or started_at >= cutoff:
            continue
        output_dir = _task_output_dir(task)
        outputs_complete = task_outputs_look_complete(task)
        candidates.append(
            {
                "task_id": task.get("task_id"),
                "task_type": task.get("task_type"),
                "started_at_utc": task.get("started_at_utc") or task.get("created_at_utc"),
                "age_minutes": round((datetime.now(timezone.utc) - started_at).total_seconds() / 60.0, 1),
                "worker_note": task.get("worker_note"),
                "output_dir": str(output_dir) if output_dir else None,
                "outputs_complete": outputs_complete,
                "outputs_present": bool(output_dir and output_dir.exists()),
            }
        )
    return candidates


def _heartbeat_gap() -> dict[str, Any]:
    rows = _read_jsonl(SYSTEM_HEARTBEAT_PATH, limit=20)
    if not rows:
        return {"available": False, "seconds_since_last": None, "last_recorded_at_utc": None}
    last = rows[-1]
    ts = parse_iso_utc(last.get("recorded_at_utc"))
    if not ts:
        return {"available": False, "seconds_since_last": None, "last_recorded_at_utc": last.get("recorded_at_utc")}
    seconds = round((datetime.now(timezone.utc) - ts).total_seconds(), 1)
    return {
        "available": True,
        "seconds_since_last": seconds,
        "last_recorded_at_utc": last.get("recorded_at_utc"),
        "last_scope": last.get("scope"),
        "last_status": last.get("status"),
    }


def _open_incidents(limit: int = 50) -> list[dict[str, Any]]:
    state = load_runtime_incident_state()
    incidents = list((state.get("incidents") or {}).values())
    incidents = [row for row in incidents if row.get("status") not in {"resolved", "verified", "suppressed"}]
    incidents.sort(key=lambda row: row.get("last_seen_at_utc") or row.get("first_seen_at_utc") or "", reverse=True)
    return incidents[:limit]


def _recent_failed_or_risky_tasks(store: ExperimentStore, limit: int = 20) -> list[dict[str, Any]]:
    tasks = store.list_research_tasks(limit=200)
    rows = []
    for task in tasks:
        note = f"{task.get('worker_note') or ''} {task.get('last_error') or ''}"
        if task.get("status") in {"failed", "quarantined"} or "budget_guard" in note or "rss exceeded" in note.lower() or "timeout" in note.lower():
            rows.append(task)
        if len(rows) >= limit:
            break
    return rows


def _queue_counts(store: ExperimentStore) -> dict[str, int]:
    tasks = store.list_research_tasks(limit=200)
    return {
        "pending": len([t for t in tasks if t.get("status") == "pending"]),
        "running": len([t for t in tasks if t.get("status") == "running"]),
        "finished": len([t for t in tasks if t.get("status") == "finished"]),
        "failed": len([t for t in tasks if t.get("status") == "failed"]),
    }


def _age_seconds(value: str | None) -> float | None:
    dt = parse_iso_utc(value)
    if dt is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())


def classify_queue_liveness(
    *,
    queue_counts: dict[str, Any],
    recent_research_tasks: list[dict[str, Any]],
    refill_state: dict[str, Any] | None = None,
    heartbeat_gap: dict[str, Any] | None = None,
    failure_state: dict[str, Any] | None = None,
    recent_activity_seconds: int = 120,
) -> dict[str, Any]:
    refill_state = refill_state or {}
    heartbeat_gap = heartbeat_gap or {}
    failure_state = failure_state or {}

    pending = int(queue_counts.get("pending") or 0)
    running = int(queue_counts.get("running") or 0)
    active = pending + running

    latest_finished_age: float | None = None
    for task in recent_research_tasks:
        if task.get("status") != "finished":
            continue
        age = _age_seconds(task.get("finished_at_utc") or task.get("created_at_utc"))
        if age is None:
            continue
        latest_finished_age = age if latest_finished_age is None else min(latest_finished_age, age)

    refill_age = _age_seconds(refill_state.get("updated_at_utc"))
    recent_injected_count = int(refill_state.get("planner_injected") or 0) + int(refill_state.get("opportunity_injected") or 0)
    try:
        heartbeat_seconds = float(heartbeat_gap.get("seconds_since_last"))
    except Exception:
        heartbeat_seconds = None

    base = {
        "active_count": active,
        "last_task_finished_age_seconds": latest_finished_age,
        "last_injection_age_seconds": refill_age,
        "recent_injected_count": recent_injected_count,
    }
    if active > 0:
        return {**base, "state": "active", "is_queue_stall": False, "reason": "pending_or_running_tasks"}

    recent_task = latest_finished_age is not None and latest_finished_age <= recent_activity_seconds
    recent_injection = recent_injected_count > 0 and refill_age is not None and refill_age <= recent_activity_seconds
    repeat_blocked = int(refill_state.get("repeat_blocked_count") or 0) > 0
    cooldown_active = bool(failure_state.get("cooldown_active"))
    fresh_heartbeat = heartbeat_seconds is not None and heartbeat_seconds <= recent_activity_seconds

    if recent_task or recent_injection:
        return {**base, "state": "healthy_idle", "is_queue_stall": False, "reason": "recent_activity"}
    if repeat_blocked or cooldown_active:
        return {**base, "state": "cooldown_idle", "is_queue_stall": False, "reason": "cooldown_or_repeat_blocked"}
    if fresh_heartbeat:
        return {**base, "state": "healthy_idle", "is_queue_stall": False, "reason": "recent_activity_heartbeat"}
    return {**base, "state": "queue_stall", "is_queue_stall": True, "reason": "empty_queue_without_recent_activity"}


def build_repair_runtime_snapshot(
    store: ExperimentStore,
    output_path: str | Path | None = None,
    *,
    stale_minutes: int = 10,
) -> dict[str, Any]:
    daemon_status = _read_json(RESEARCH_DAEMON_STATUS_PATH, {})
    refill_state = _read_json(ARTIFACTS / "research_queue_refill_state.json", {})
    reseed_diagnostics = _read_json(ARTIFACTS / "research_queue_reseed_diagnostics.json", {})
    if reseed_diagnostics and "repeat_blocked_count" not in refill_state:
        refill_state = {**refill_state, "repeat_blocked_count": reseed_diagnostics.get("repeat_blocked_count") or 0}
    queue_counts = _queue_counts(store)
    failure_state = recent_failure_stats(store)
    heartbeat_gap = _heartbeat_gap()
    recent_research_tasks = store.list_research_tasks(limit=30)
    snapshot = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "daemon_status": daemon_status,
        "queue_budget": queue_budget_snapshot(store),
        "queue_counts": queue_counts,
        "queue_liveness": classify_queue_liveness(
            queue_counts=queue_counts,
            recent_research_tasks=recent_research_tasks,
            refill_state=refill_state,
            heartbeat_gap=heartbeat_gap,
            failure_state=failure_state,
        ),
        "failure_state": failure_state,
        "blocked_lane_status": daemon_status.get("blocked_lane_status") or {},
        "route_status": daemon_status.get("route_status") or {},
        "resource_pressure": {
            "rss_mb": daemon_status.get("rss_mb"),
            "rss_ratio": daemon_status.get("rss_ratio"),
            "cpu_usage_ratio": daemon_status.get("cpu_usage_ratio"),
            "mem_pressure": daemon_status.get("mem_pressure"),
            "load1": daemon_status.get("load1"),
        },
        "heartbeat_gap": heartbeat_gap,
        "recent_research_tasks": recent_research_tasks,
        "recent_failed_or_risky_tasks": _recent_failed_or_risky_tasks(store),
        "stale_running_candidates": stale_running_candidates(store, stale_minutes=stale_minutes),
        "status_file_consistency": {
            "daemon_status_available": bool(daemon_status),
            "daemon_state": daemon_status.get("state"),
            "last_processed_summary": ((daemon_status.get("last_processed") or {}).get("summary")),
        },
        "open_incidents": _open_incidents(),
        "repair_feedback": _read_json(REPAIR_FEEDBACK_PATH, {}),
        "repair_metrics": _read_json(REPAIR_METRICS_PATH, {}),
    }
    if output_path is not None:
        Path(output_path).write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    return snapshot


def write_repair_summary_artifacts() -> dict[str, Any]:
    state = load_runtime_incident_state()
    incidents = list((state.get("incidents") or {}).values())
    now = datetime.now(timezone.utc)
    cutoff_24h = now - timedelta(hours=24)

    def _recent(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
        recent_rows = []
        for row in rows:
            dt = parse_iso_utc(row.get(field))
            if dt and dt >= cutoff_24h:
                recent_rows.append(row)
        return recent_rows

    def _is_legacy_observation(row: dict[str, Any]) -> bool:
        if str(row.get("incident_type") or "") != "unknown":
            return False
        actions = list(row.get("actions") or [])
        if not actions:
            return False
        return all(
            str(action.get("action_type") or "") == "mark_incident_only"
            and str(action.get("status") or "") in {"noop", "observed"}
            for action in actions
        )

    recent_incidents = [row for row in _recent(_read_jsonl(REPAIR_INCIDENTS_PATH, limit=2000), "recorded_at_utc") if not _is_legacy_observation(row)]
    recent_observations = _recent(_read_jsonl(REPAIR_OBSERVATIONS_PATH, limit=2000), "recorded_at_utc")
    active_incidents = [row for row in incidents if row.get("status") not in {"resolved", "verified", "suppressed"}]

    type_counts: dict[str, int] = {}
    for row in recent_incidents:
        incident_type = str(row.get("incident_type") or "unknown")
        type_counts[incident_type] = type_counts.get(incident_type, 0) + 1

    verification_counts = {"verified": 0, "repair_failed": 0, "needs_manual": 0, "active": 0}
    for row in incidents:
        status = str(row.get("status") or "active")
        if status in verification_counts:
            verification_counts[status] += 1
        elif status not in {"resolved", "suppressed"}:
            verification_counts["active"] += 1

    restart_recently = False
    for row in recent_incidents:
        for action in (row.get("actions") or []):
            if action.get("action_type") == "restart_daemon" and action.get("status") == "ok":
                restart_recently = True
                break
        if restart_recently:
            break

    blocked_families = sorted(
        {
            str(row.get("target") or "")
            for row in active_incidents
            if row.get("incident_type") == "blocked_lane_deadlock" and row.get("target")
        }
    )

    feedback = {
        "generated_at_utc": now.isoformat(),
        "active_incident_count": len(active_incidents),
        "active_incident_types": sorted({str(row.get("incident_type") or "unknown") for row in active_incidents}),
        "blocked_families": blocked_families,
        "route_unhealthy": any(row.get("incident_type") == "provider_route_failure" for row in active_incidents),
        "restart_recently": restart_recently,
        "top_incident_types_24h": sorted(type_counts.items(), key=lambda item: (-item[1], item[0]))[:5],
    }
    metrics = {
        "generated_at_utc": now.isoformat(),
        "incident_count_24h": len(recent_incidents),
        "observation_count_24h": len(recent_observations),
        "incident_type_counts_24h": type_counts,
        "verification_counts": verification_counts,
        "active_incident_count": len(active_incidents),
    }
    _write_json(REPAIR_FEEDBACK_PATH, feedback)
    _write_json(REPAIR_METRICS_PATH, metrics)
    return {"repair_feedback": feedback, "repair_metrics": metrics}
