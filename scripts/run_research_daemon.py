from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from factor_lab.heartbeat import append_heartbeat
from factor_lab.research_queue import run_orchestrator

ROOT = Path(__file__).resolve().parents[1]
STATUS_PATH = ROOT / "artifacts" / "research_daemon_status.json"
RUNNING = True
LAST_PREWARM_AT = 0.0


def handle_stop(signum, frame):
    global RUNNING
    RUNNING = False


def write_status(state: str, **extra):
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "state": state,
        **extra,
    }
    STATUS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def emit_wake_event(text: str) -> None:
    os.system(f'openclaw system event --mode now --text {json.dumps(text, ensure_ascii=False)} >/dev/null 2>&1')


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

    universe_limit = os.getenv("RESEARCH_DAEMON_PREWARM_UNIVERSE_LIMIT", "20")
    output = ROOT / "artifacts" / "data_prepare_status.json"
    command = [
        sys.executable,
        str(ROOT / "scripts" / "prepare_tushare_data.py"),
        "--end-date",
        datetime.now().strftime("%Y-%m-%d"),
        "--universe-limit",
        str(universe_limit),
        "--output",
        str(output),
    ]
    for window in windows:
        command.extend(["--window-days", window])

    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
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

    idle_sleep_seconds = int(os.getenv("RESEARCH_DAEMON_IDLE_SECONDS", "60"))
    max_tasks_per_loop = int(os.getenv("RESEARCH_DAEMON_MAX_TASKS", "1"))

    append_heartbeat("research_daemon", "started", summary="research daemon started")
    write_status("running", idle_sleep_seconds=idle_sleep_seconds, max_tasks_per_loop=max_tasks_per_loop)

    while RUNNING:
        try:
            result = run_orchestrator(max_tasks=max_tasks_per_loop)
            processed = result.get("processed", [])
            guardrail = result.get("guardrail")
            if guardrail:
                write_status("guardrail", guardrail=guardrail, processed_count=len(processed))
                emit_wake_event(f"Factor Lab guardrail triggered: {guardrail}.")
                time.sleep(idle_sleep_seconds)
                continue

            if processed:
                # processed is appended in execution order; the last element is the most recently processed task.
                latest = processed[-1]
                write_status("running", processed_count=len(processed), last_processed=latest)
                if latest.get("status") == "finished":
                    emit_wake_event(f"Factor Lab task finished: {latest.get('summary', 'task completed')}")
                elif latest.get("status") == "failed":
                    emit_wake_event(f"Factor Lab task failed: {latest.get('error', 'unknown error')}")
                time.sleep(2)
            else:
                remaining_preview = result.get("remaining_preview") or []
                pending_after = [row for row in remaining_preview if row.get("status") == "pending"]
                if pending_after:
                    write_status("running", processed_count=0, planner_pending=len(pending_after))
                    time.sleep(2)
                else:
                    prewarm = maybe_run_prewarm()
                    if prewarm:
                        write_status("idle", processed_count=0, prewarm=prewarm)
                        if not prewarm.get("ok"):
                            emit_wake_event(f"Factor Lab prewarm failed: {prewarm.get('stderr') or prewarm.get('stdout') or 'unknown error'}")
                    else:
                        write_status("idle", processed_count=0)
                    # Proactive alert if we are idling/guardrailed for too long.
                    try:
                        status_doc = json.loads(STATUS_PATH.read_text(encoding="utf-8")) if STATUS_PATH.exists() else {}
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
