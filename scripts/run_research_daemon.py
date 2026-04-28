from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fcntl

SCRIPT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_ROOT / "src"))

from factor_lab.adaptive_scheduler import apply_scheduler_env, compute_scheduler_policy, route_status_snapshot, user_idle_snapshot, write_scheduler_state
from factor_lab.heartbeat import append_heartbeat
from factor_lab.paths import artifacts_dir, project_root
from factor_lab.research_queue import process_report_refresh_requests, report_refresh_requested, run_orchestrator


def _root_path() -> Path:
    return project_root()


def _artifacts_path() -> Path:
    return artifacts_dir()


def _status_path() -> Path:
    return _artifacts_path() / "research_daemon_status.json"


def _heartbeat_path() -> Path:
    return _artifacts_path() / "research_daemon_heartbeat.json"


def _db_path() -> Path:
    return _artifacts_path() / "factor_lab.db"


def _status_history_path() -> Path:
    return _artifacts_path() / "research_daemon_status_history.jsonl"


def _lock_path() -> Path:
    return _artifacts_path() / "research_daemon.lock"


RUNNING = True
LAST_PREWARM_AT = 0.0
LOCK_HANDLE = None


def cpu_count() -> int:
    return max(1, os.cpu_count() or 1)


def read_system_load() -> dict[str, float]:
    try:
        load1, load5, load15 = os.getloadavg()
    except Exception:
        return {"load1": 0.0, "load5": 0.0, "load15": 0.0, "cpu_usage_ratio": 0.0}
    ratio = load1 / cpu_count()
    return {"load1": round(load1, 3), "load5": round(load5, 3), "load15": round(load15, 3), "cpu_usage_ratio": round(ratio, 4)}


def read_meminfo_mb() -> dict[str, int]:
    totals = {"mem_total_mb": 0, "mem_available_mb": 0, "swap_total_mb": 0, "swap_free_mb": 0}
    try:
        text = Path('/proc/meminfo').read_text(encoding='utf-8', errors='ignore')
    except Exception:
        return totals
    wanted = {
        'MemTotal:': 'mem_total_mb',
        'MemAvailable:': 'mem_available_mb',
        'SwapTotal:': 'swap_total_mb',
        'SwapFree:': 'swap_free_mb',
    }
    for line in text.splitlines():
        parts = line.split()
        if len(parts) < 2 or parts[0] not in wanted:
            continue
        try:
            totals[wanted[parts[0]]] = int(parts[1]) // 1024
        except Exception:
            continue
    return totals


def compute_dynamic_throttle(*, base_max_tasks: int, rss_limit_mb: int) -> dict[str, Any]:
    load = read_system_load()
    mem = read_meminfo_mb()
    current_rss_mb = read_rss_mb()
    usage_ratio = float(load.get('cpu_usage_ratio') or 0.0)
    mem_total = int(mem.get('mem_total_mb') or 0)
    mem_available = int(mem.get('mem_available_mb') or 0)
    mem_pressure = 1.0 - (mem_available / mem_total) if mem_total > 0 else 0.0
    rss_ratio = (current_rss_mb / rss_limit_mb) if rss_limit_mb > 0 else 0.0

    idle = user_idle_snapshot()
    route = route_status_snapshot()
    policy = compute_scheduler_policy(
        base_max_tasks=base_max_tasks,
        cpu_usage_ratio=usage_ratio,
        mem_pressure=mem_pressure,
        mem_available_mb=mem_available,
        rss_ratio=rss_ratio,
        idle_snapshot=idle,
        route_status=route,
    )
    apply_scheduler_env(policy)
    write_scheduler_state(policy, cpu_usage_ratio=usage_ratio, mem_pressure=mem_pressure, rss_ratio=rss_ratio)

    return {
        'mode': policy.get('mode') or 'unknown',
        'mode_reason': policy.get('reason'),
        'dynamic_max_tasks': int(policy.get('dynamic_max_tasks') or max(1, base_max_tasks)),
        'dynamic_batch_workers': int(policy.get('dynamic_batch_workers') or 1),
        'cpu_budget': int(policy.get('cpu_budget') or max(1, base_max_tasks)),
        'network_budget': int(policy.get('network_budget') or 0),
        'light_task_budget': int(policy.get('light_task_budget') or 1),
        'queue_caps': policy.get('queue_caps') or {},
        'opportunity_enqueue_limit': int(policy.get('opportunity_enqueue_limit') or 1),
        'idle_state': idle,
        'route_status': route,
        'target_cpu_ratio': policy.get('cpu_target_max') or 0.8,
        'target_cpu_min_ratio': policy.get('cpu_target_min') or 0.45,
        'rss_mb': current_rss_mb,
        'rss_ratio': round(rss_ratio, 4),
        'mem_pressure': round(mem_pressure, 4),
        **load,
        **mem,
    }


def read_rss_mb(status_path: Path = Path("/proc/self/status")) -> int:
    try:
        text = status_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return 0
    for line in text.splitlines():
        if not line.startswith("VmRSS:"):
            continue
        parts = line.split()
        if len(parts) < 2:
            return 0
        try:
            kb = int(parts[1])
        except Exception:
            return 0
        return kb // 1024
    return 0


def should_recycle_daemon(
    *,
    processed_tasks_total: int,
    max_tasks_before_restart: int,
    rss_limit_mb: int,
    rss_mb: int,
    idle: bool = False,
) -> str | None:
    recycle_mode = (os.getenv("RESEARCH_DAEMON_RECYCLE_MODE") or "idle_preferred").strip().lower()
    if rss_limit_mb > 0 and rss_mb >= rss_limit_mb:
        return "rss_limit_exceeded"
    if max_tasks_before_restart > 0 and processed_tasks_total >= max_tasks_before_restart:
        if recycle_mode in {"immediate", "always"}:
            return "task_budget_reached"
        if recycle_mode in {"disabled", "off", "none"}:
            return None
        if idle:
            return "task_budget_reached"
    return None


def handle_stop(signum, frame):
    global RUNNING
    RUNNING = False


def acquire_single_instance_lock() -> bool:
    global LOCK_HANDLE
    lock_path = _lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    LOCK_HANDLE = open(lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(LOCK_HANDLE.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return False
    LOCK_HANDLE.seek(0)
    LOCK_HANDLE.truncate()
    LOCK_HANDLE.write(
        json.dumps(
            {
                "pid": os.getpid(),
                "started_at_utc": datetime.now(timezone.utc).isoformat(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    LOCK_HANDLE.flush()
    return True


def write_daemon_heartbeat(state: str, status_payload: dict[str, Any]) -> None:
    """Write a small, stable heartbeat document for WebUI/control pages."""
    heartbeat_path = _heartbeat_path()
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    queue = {"pending": 0, "running": 0, "finished_24h": 0, "failed_24h": 0}
    current_task = None
    db_path = _db_path()
    if db_path.exists():
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=0.2)
            conn.row_factory = sqlite3.Row
            try:
                for row in conn.execute("SELECT status, COUNT(*) AS n FROM research_tasks GROUP BY status").fetchall():
                    if row["status"] in queue:
                        queue[row["status"]] = int(row["n"] or 0)
                queue["finished_24h"] = int(conn.execute("SELECT COUNT(*) FROM research_tasks WHERE status='finished' AND finished_at_utc >= datetime('now', '-1 day')").fetchone()[0])
                queue["failed_24h"] = int(conn.execute("SELECT COUNT(*) FROM research_tasks WHERE status='failed' AND finished_at_utc >= datetime('now', '-1 day')").fetchone()[0])
                row = conn.execute("SELECT task_id, task_type, status, started_at_utc, created_at_utc FROM research_tasks WHERE status='running' ORDER BY COALESCE(started_at_utc, created_at_utc) DESC LIMIT 1").fetchone()
                current_task = dict(row) if row else None
            finally:
                conn.close()
        except Exception as exc:
            queue["error"] = str(exc)[:200]
    last_processed = status_payload.get("last_processed") or {}
    if current_task is None and last_processed:
        current_task = {
            "id": last_processed.get("id") or last_processed.get("task_id"),
            "type": last_processed.get("task_type"),
            "status": last_processed.get("status"),
            "started_at": last_processed.get("started_at_utc") or last_processed.get("created_at_utc"),
        }
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "project_root": str(_root_path()),
        "provider": os.getenv("FACTOR_LAB_DECISION_PROVIDER") or os.getenv("FACTOR_LAB_LIVE_DECISION_PROVIDER") or "unknown",
        "state": state,
        "queue": queue,
        "current_task": current_task,
        "last_injection": status_payload.get("last_injection") or status_payload.get("planner_injection") or {},
        "skip_reasons_24h": status_payload.get("skip_reasons_24h") or status_payload.get("skip_reasons") or {},
        "processed_tasks_total": status_payload.get("processed_tasks_total", 0),
        "rss_mb": status_payload.get("rss_mb"),
    }
    tmp_path = heartbeat_path.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(heartbeat_path)


def write_status(state: str, **extra: Any):
    status_path = _status_path()
    status_history_path = _status_history_path()
    status_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "state": state,
        **extra,
    }
    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        write_daemon_heartbeat(state, payload)
    except Exception:
        pass
    try:
        compact = {k: v for k, v in payload.items() if k not in {'last_processed', 'prewarm'}}
        with status_history_path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(compact, ensure_ascii=False) + "\n")
    except Exception:
        pass


def merge_status_fields(*parts: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for part in parts:
        merged.update(part or {})
    return merged


def orchestrator_status_context(result: dict[str, Any] | None) -> dict[str, Any]:
    result = result or {}
    blocked_task_types = list(result.get("blocked_task_types") or [])
    blocked_lane_status = result.get("blocked_lane_status") or {}
    context: dict[str, Any] = {}
    if blocked_task_types:
        context["blocked_task_types"] = blocked_task_types
    if blocked_lane_status:
        context["blocked_lane_status"] = blocked_lane_status
        if blocked_lane_status.get("summary"):
            context["blocked_lane_summary"] = blocked_lane_status.get("summary")
        if blocked_lane_status.get("blocked_pending_count") is not None:
            context["blocked_pending_count"] = blocked_lane_status.get("blocked_pending_count")
        if blocked_lane_status.get("unblocked_pending_count") is not None:
            context["unblocked_pending_count"] = blocked_lane_status.get("unblocked_pending_count")
    return context


def _emit_wake_event_via_openclaw(text: str) -> str:
    """Emit wake event via OpenClaw CLI if available.
    
    Returns status:
    - 'disabled': wake events are disabled via env var
    - 'unavailable': openclaw CLI not found
    - 'delivered': event successfully sent
    - 'failed': openclaw CLI failed
    """
    # Check if wake events are enabled
    if os.getenv("RESEARCH_DAEMON_WAKE_EVENTS", "0").strip().lower() not in {"1", "true", "yes", "on"}:
        return "disabled"
    
    # Check if openclaw CLI is available
    if not shutil.which("openclaw"):
        return "unavailable"
    
    # Attempt to send the event
    try:
        result = subprocess.run(
            ["openclaw", "system", "event", "--mode", "now", "--text", text],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return "delivered"
        else:
            return "failed"
    except Exception:
        return "failed"


def emit_wake_event(text: str) -> None:
    """Emit wake event via OpenClaw CLI if available.
    
    This is a non-fatal operation - daemon continues even if notification fails.
    Factor Lab notifications are noisy in chat; keep them opt-in.
    Set RESEARCH_DAEMON_WAKE_EVENTS=1 to re-enable proactive chat wake events.
    """
    status = _emit_wake_event_via_openclaw(text)
    # Log unavailable/failed status for debugging, but don't crash
    if status == "unavailable":
        # OpenClaw CLI not installed - this is expected in some environments
        pass
    elif status == "failed":
        # OpenClaw CLI failed - log but continue
        pass


def maybe_emit_stall_alert(status: dict, *, cooldown_seconds: int = 300) -> None:
    """Send a proactive system event if the daemon is stuck/idling for too long.

    This reduces the "silent stall" failure mode: the loop is alive but doing nothing.
    """
    try:
        last_alert_at = float(status.get("stall_alert_last_sent_at") or 0.0)
    except Exception:
        last_alert_at = 0.0

    now = time.time()
    if last_alert_at and now - last_alert_at < cooldown_seconds:
        return

    state = status.get("state")
    if state not in {"idle", "guardrail"}:
        return

    guardrail = status.get("guardrail")
    reason = f"guardrail={guardrail}" if guardrail else "idle"
    msg = (
        "Reminder: Factor Lab research daemon appears stalled (" + reason + "). "
        "If this persists, check artifacts/system_heartbeat.jsonl and artifacts/research_stagnation.json."
    )
    emit_wake_event(msg)
    status["stall_alert_last_sent_at"] = now
    write_status(state or "unknown", **{k: v for k, v in status.items() if k not in {"state"}})


def maybe_run_prewarm() -> dict | None:
    global LAST_PREWARM_AT
    windows_env = os.getenv("RESEARCH_DAEMON_PREWARM_WINDOWS", "").strip()
    if not windows_env:
        return None
    interval_seconds = int(os.getenv("RESEARCH_DAEMON_PREWARM_INTERVAL_SECONDS", "21600"))
    now = time.time()
    if LAST_PREWARM_AT and now - LAST_PREWARM_AT < interval_seconds:
        return None

    windows = [item.strip() for item in windows_env.split(",") if item.strip()]
    if not windows:
        return None

    root = _root_path()
    artifacts = _artifacts_path()
    universe_limit = os.getenv("RESEARCH_DAEMON_PREWARM_UNIVERSE_LIMIT", "20")
    output = artifacts / "data_prepare_status.json"
    command = [
        sys.executable,
        str(root / "scripts" / "prepare_tushare_data.py"),
        "--end-date",
        datetime.now().strftime("%Y-%m-%d"),
        "--universe-limit",
        str(universe_limit),
        "--output",
        str(output),
    ]
    for window in windows:
        command.extend(["--window-days", window])

    result = subprocess.run(command, cwd=root, capture_output=True, text=True)
    LAST_PREWARM_AT = now
    return {
        "ok": result.returncode == 0,
        "returncode": result.returncode,
        "stdout": result.stdout[-1000:],
        "stderr": result.stderr[-1000:],
        "windows": windows,
    }


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    if not acquire_single_instance_lock():
        append_heartbeat("research_daemon", "skipped", summary="research daemon skipped: lock already held")
        write_status("skipped", reason="lock_already_held")
        raise SystemExit(0)

    idle_sleep_seconds = int(os.getenv("RESEARCH_DAEMON_IDLE_SECONDS", "60"))
    base_max_tasks_per_loop = int(os.getenv("RESEARCH_DAEMON_MAX_TASKS", "1"))
    max_tasks_before_restart = int(os.getenv("RESEARCH_DAEMON_MAX_TASKS_BEFORE_RESTART", "8"))
    rss_limit_mb = int(os.getenv("RESEARCH_DAEMON_RSS_LIMIT_MB", "2048"))
    processed_tasks_total = 0

    append_heartbeat("research_daemon", "started", summary="research daemon started")
    write_status(
        "running",
        idle_sleep_seconds=idle_sleep_seconds,
        max_tasks_per_loop=base_max_tasks_per_loop,
        max_tasks_before_restart=max_tasks_before_restart,
        rss_limit_mb=rss_limit_mb,
        cpu_cores=cpu_count(),
    )

    while RUNNING:
        try:
            throttle = compute_dynamic_throttle(base_max_tasks=base_max_tasks_per_loop, rss_limit_mb=rss_limit_mb)
            dynamic_max_tasks = int(throttle['dynamic_max_tasks'])
            dynamic_batch_workers = int(throttle['dynamic_batch_workers'])
            os.environ['FACTOR_LAB_BATCH_MAX_WORKERS'] = str(dynamic_batch_workers)
            result = run_orchestrator(max_tasks=dynamic_max_tasks)
            processed = result.get("processed", [])
            status_context = orchestrator_status_context(result)
            guardrail = result.get("guardrail")
            if guardrail:
                write_status(
                    "guardrail",
                    **merge_status_fields(
                        throttle,
                        status_context,
                        {
                            "guardrail": guardrail,
                            "processed_count": len(processed),
                            "processed_tasks_total": processed_tasks_total,
                            "max_tasks_per_loop": dynamic_max_tasks,
                            "batch_max_workers": dynamic_batch_workers,
                        },
                    ),
                )
                emit_wake_event(f"Factor Lab guardrail triggered: {guardrail}.")
                time.sleep(idle_sleep_seconds)
                continue

            if processed:
                processed_tasks_total += len(processed)
                latest = processed[-1]
                current_rss_mb = int(throttle.get('rss_mb') or read_rss_mb())
                write_status(
                    "running",
                    **merge_status_fields(
                        throttle,
                        status_context,
                        {
                            "processed_count": len(processed),
                            "processed_tasks_total": processed_tasks_total,
                            "rss_mb": current_rss_mb,
                            "max_tasks_per_loop": dynamic_max_tasks,
                            "batch_max_workers": dynamic_batch_workers,
                            "last_processed": latest,
                        },
                    ),
                )
                if latest.get("status") == "finished":
                    emit_wake_event(f"Factor Lab task finished: {latest.get('summary', 'task completed')}")
                elif latest.get("status") == "failed":
                    emit_wake_event(f"Factor Lab task failed: {latest.get('error', 'unknown error')}")

                recycle_reason = should_recycle_daemon(
                    processed_tasks_total=processed_tasks_total,
                    max_tasks_before_restart=max_tasks_before_restart,
                    rss_limit_mb=rss_limit_mb,
                    rss_mb=current_rss_mb,
                )
                if recycle_reason:
                    append_heartbeat(
                        "research_daemon",
                        "recycling",
                        summary=(
                            f"daemon exiting for recycle: reason={recycle_reason}, "
                            f"processed_tasks_total={processed_tasks_total}, rss_mb={current_rss_mb}"
                        ),
                    )
                    write_status(
                        "recycling",
                        reason=recycle_reason,
                        processed_tasks_total=processed_tasks_total,
                        rss_mb=current_rss_mb,
                    )
                    raise SystemExit(0)
                process_report_refresh_requests()
                time.sleep(2)
            else:
                remaining_preview = result.get("remaining_preview") or []
                pending_after = [row for row in remaining_preview if row.get("status") == "pending"]
                if pending_after:
                    write_status(
                        "running",
                        **merge_status_fields(
                            throttle,
                            status_context,
                            {
                                "processed_count": 0,
                                "planner_pending": len(pending_after),
                                "max_tasks_per_loop": dynamic_max_tasks,
                                "batch_max_workers": dynamic_batch_workers,
                            },
                        ),
                    )
                    time.sleep(2)
                else:
                    prewarm = maybe_run_prewarm()
                    current_rss_mb = read_rss_mb()
                    if prewarm:
                        write_status(
                            "idle",
                            **merge_status_fields(
                                throttle,
                                status_context,
                                {
                                    "processed_count": 0,
                                    "processed_tasks_total": processed_tasks_total,
                                    "rss_mb": current_rss_mb,
                                    "max_tasks_per_loop": dynamic_max_tasks,
                                    "batch_max_workers": dynamic_batch_workers,
                                    "prewarm": prewarm,
                                },
                            ),
                        )
                        if not prewarm.get("ok"):
                            emit_wake_event(f"Factor Lab prewarm failed: {prewarm.get('stderr') or prewarm.get('stdout') or 'unknown error'}")
                    else:
                        write_status(
                            "idle",
                            **merge_status_fields(
                                throttle,
                                status_context,
                                {
                                    "processed_count": 0,
                                    "processed_tasks_total": processed_tasks_total,
                                    "rss_mb": current_rss_mb,
                                    "max_tasks_per_loop": dynamic_max_tasks,
                                    "batch_max_workers": dynamic_batch_workers,
                                    "report_refresh_requested": report_refresh_requested(),
                                },
                            ),
                        )
                    process_report_refresh_requests()
                    recycle_reason = should_recycle_daemon(
                        processed_tasks_total=processed_tasks_total,
                        max_tasks_before_restart=max_tasks_before_restart,
                        rss_limit_mb=rss_limit_mb,
                        rss_mb=current_rss_mb,
                        idle=True,
                    )
                    if recycle_reason:
                        append_heartbeat(
                            "research_daemon",
                            "recycling",
                            summary=(
                                f"daemon exiting for recycle: reason={recycle_reason}, "
                                f"processed_tasks_total={processed_tasks_total}, rss_mb={current_rss_mb}"
                            ),
                        )
                        write_status(
                            "recycling",
                            reason=recycle_reason,
                            processed_tasks_total=processed_tasks_total,
                            rss_mb=current_rss_mb,
                        )
                        raise SystemExit(0)
                    try:
                        status_path = _status_path()
                        status_doc = json.loads(status_path.read_text(encoding="utf-8")) if status_path.exists() else {}
                    except Exception:
                        status_doc = {}
                    maybe_emit_stall_alert(status_doc, cooldown_seconds=300)
                    time.sleep(idle_sleep_seconds)
        except Exception as exc:
            append_heartbeat("research_daemon", "failed", message=str(exc))
            write_status("failed", error=str(exc))
            emit_wake_event(f"Factor Lab daemon failed: {str(exc)}")
            time.sleep(idle_sleep_seconds)

    append_heartbeat("research_daemon", "stopped", summary="research daemon stopped")
    write_status("stopped")
