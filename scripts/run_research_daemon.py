from __future__ import annotations

import json
import os
import signal
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
                write_status("running", processed_count=len(processed), last_processed=processed[0])
                first = processed[0]
                if first.get("status") == "finished":
                    emit_wake_event(f"Factor Lab task finished: {first.get('summary', 'task completed')}")
                elif first.get("status") == "failed":
                    emit_wake_event(f"Factor Lab task failed: {first.get('error', 'unknown error')}")
                time.sleep(2)
            else:
                remaining_preview = result.get("remaining_preview") or []
                pending_after = [row for row in remaining_preview if row.get("status") == "pending"]
                if pending_after:
                    write_status("running", processed_count=0, planner_pending=len(pending_after))
                    time.sleep(2)
                else:
                    write_status("idle", processed_count=0)
                    time.sleep(idle_sleep_seconds)
        except Exception as exc:
            append_heartbeat("research_daemon", "failed", message=str(exc))
            write_status("failed", error=str(exc))
            emit_wake_event(f"Factor Lab daemon failed: {str(exc)}")
            time.sleep(idle_sleep_seconds)

    append_heartbeat("research_daemon", "stopped", summary="research daemon stopped")
    write_status("stopped")
