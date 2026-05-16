#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.controlled_restart_audit import dry_run_controlled_restart

SERVICE_NAME = "factor-lab-research-daemon.service"
SENSITIVE_MARKERS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "CREDENTIAL", "AUTH")


def _project_root() -> Path:
    return ROOT


def _db_path() -> Path:
    return _project_root() / "artifacts" / "factor_lab.db"


def _is_sensitive_key(key: str) -> bool:
    upper = key.upper()
    return any(marker in upper for marker in SENSITIVE_MARKERS)


def redact_sensitive(value: Any, *, key: str = "") -> Any:
    if _is_sensitive_key(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(k): redact_sensitive(v, key=str(k)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_sensitive(v, key=key) for v in value]
    return value


def _run(command: list[str], *, cwd: Path | None = None, timeout: int = 10) -> dict[str, Any]:
    try:
        result = subprocess.run(command, cwd=cwd, capture_output=True, text=True, check=False, timeout=timeout)
        return {
            "command": command,
            "returncode": result.returncode,
            "stdout": (result.stdout or "")[-8000:],
            "stderr": (result.stderr or "")[-8000:],
        }
    except Exception as exc:
        return {"command": command, "returncode": None, "error": str(exc)}


def _find_running_workflow_task(db_path: str | Path) -> dict[str, Any] | None:
    path = Path(db_path)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.5)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT task_id, task_type, status, started_at_utc, created_at_utc, worker_note, payload_json
            FROM research_tasks
            WHERE status='running' AND task_type='workflow'
            ORDER BY COALESCE(started_at_utc, created_at_utc) DESC
            LIMIT 1
            """
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _task_snapshot(task_id: str | None, db_path: str | Path) -> dict[str, Any] | None:
    if not task_id:
        return None
    path = Path(db_path)
    if not path.exists():
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=0.5)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT task_id, task_type, status, started_at_utc, finished_at_utc, last_error, worker_note, payload_json
            FROM research_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _residual_processes() -> list[dict[str, str]]:
    result = _run(["ps", "-eo", "pid,ppid,pgid,etime,cmd"], cwd=_project_root(), timeout=10)
    rows: list[dict[str, str]] = []
    for line in str(result.get("stdout") or "").splitlines()[1:]:
        if not line.strip():
            continue
        if "probe_research_daemon_active_worker_stop.py" in line:
            continue
        if "run_research_daemon.py" not in line and "run_research_task_worker.py" not in line:
            continue
        parts = line.split(None, 4)
        if len(parts) < 5:
            continue
        rows.append({"pid": parts[0], "ppid": parts[1], "pgid": parts[2], "etime": parts[3], "cmd": parts[4]})
    return rows


def _journal_has_stop_timeout(journal_text: str) -> bool:
    lowered = journal_text.lower()
    return "stop-sigterm" in lowered or "timed out. killing" in lowered or "failed with result 'timeout'" in lowered


def _journal_since_arg(iso_timestamp: str) -> str:
    text = str(iso_timestamp or "")
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text.replace("T", " ").split(".")[0]


def _write_outputs(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "daemon_active_worker_stop_probe.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Research Daemon Active Worker Stop Probe",
        "",
        f"OK: {payload.get('ok')}",
        f"Exit reason: {payload.get('exit_reason') or ''}",
        f"Started at: {payload.get('started_at_utc')}",
        f"Finished at: {payload.get('finished_at_utc')}",
        f"Running task: {(payload.get('running_task') or {}).get('task_id') or ''}",
        f"Residual processes: {len(payload.get('residual_processes') or [])}",
        f"Stop timeout detected: {payload.get('stop_timeout_detected')}",
        "",
        "## Commands",
    ]
    for item in payload.get("commands") or []:
        lines.append(f"- `{ ' '.join(item.get('command') or []) }` rc={item.get('returncode')} error={item.get('error') or ''}")
    (output_dir / "daemon_active_worker_stop_probe.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def probe_active_worker_stop(
    *,
    wait_running_seconds: int = 30,
    poll_seconds: float = 1.0,
    stop_timeout_seconds: int = 30,
    output_dir: str | Path = "artifacts",
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    root = _project_root()
    db_path = Path(db_path) if db_path else _db_path()
    out = Path(output_dir)
    commands: list[dict[str, Any]] = []
    payload: dict[str, Any] = {
        "ok": False,
        "service": SERVICE_NAME,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "wait_running_seconds": wait_running_seconds,
        "poll_seconds": poll_seconds,
        "commands": commands,
        "environment_sample": redact_sensitive({k: os.environ.get(k) for k in sorted(os.environ) if k.startswith(("FACTOR_LAB", "RESEARCH_", "PYTHON", "XDG"))}),
    }
    try:
        audit = dry_run_controlled_restart(db_path=db_path)
        payload["initial_dry_run"] = audit
        if int(audit.get("would_run_count") or 0) <= 0:
            payload["exit_reason"] = "no_admitted_workflow"
            return payload

        commands.append(_run(["systemctl", "--user", "start", SERVICE_NAME], cwd=root, timeout=15))
        if commands[-1].get("returncode") != 0:
            payload["exit_reason"] = "systemctl_start_failed"
            return payload

        running_task = None
        deadline = time.time() + max(0, wait_running_seconds)
        while time.time() <= deadline:
            running_task = _find_running_workflow_task(db_path)
            if running_task:
                break
            time.sleep(max(0.1, poll_seconds))
        payload["running_task"] = redact_sensitive(running_task) if running_task else None
        if not running_task:
            payload["exit_reason"] = "running_task_not_observed"
            return payload

        commands.append(_run(["systemctl", "--user", "stop", SERVICE_NAME], cwd=root, timeout=stop_timeout_seconds))
        time.sleep(max(0.1, poll_seconds))
        commands.append(_run(["systemctl", "--user", "is-active", SERVICE_NAME], cwd=root, timeout=5))
        commands.append(
            _run(
                [
                    "journalctl",
                    "--user",
                    "-u",
                    SERVICE_NAME,
                    "--since",
                    _journal_since_arg(str(payload["started_at_utc"])),
                    "-n",
                    "120",
                    "--no-pager",
                ],
                cwd=root,
                timeout=10,
            )
        )

        task_id = str(running_task.get("task_id") or "")
        payload["post_stop_task"] = redact_sensitive(_task_snapshot(task_id, db_path))
        payload["residual_processes"] = _residual_processes()
        journal_text = "\n".join(str(item.get("stdout") or "") for item in commands if item.get("command", [None])[0] == "journalctl")
        payload["stop_timeout_detected"] = _journal_has_stop_timeout(journal_text)
        active_stdout = "\n".join(str(item.get("stdout") or "") for item in commands if item.get("command", [])[:3] == ["systemctl", "--user", "is-active"])
        payload["service_inactive"] = "inactive" in active_stdout or "failed" in active_stdout
        payload["ok"] = bool(payload["service_inactive"] and not payload["residual_processes"] and not payload["stop_timeout_detected"])
        payload["exit_reason"] = "clean_stop" if payload["ok"] else "unsafe_stop"
        return payload
    except Exception as exc:
        payload["error"] = str(exc)
        payload["exit_reason"] = "exception"
        return payload
    finally:
        commands.append(_run(["systemctl", "--user", "stop", SERVICE_NAME], cwd=root, timeout=stop_timeout_seconds))
        commands.append(_run(["systemctl", "--user", "reset-failed", SERVICE_NAME], cwd=root, timeout=10))
        payload["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        _write_outputs(out, payload)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wait-running-seconds", type=int, default=30)
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--stop-timeout-seconds", type=int, default=30)
    parser.add_argument("--output-dir", default="artifacts")
    parser.add_argument("--db-path", default=None)
    args = parser.parse_args()
    result = probe_active_worker_stop(
        wait_running_seconds=args.wait_running_seconds,
        poll_seconds=args.poll_seconds,
        stop_timeout_seconds=args.stop_timeout_seconds,
        output_dir=args.output_dir,
        db_path=args.db_path,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result.get("ok") else 2)


if __name__ == "__main__":
    main()
