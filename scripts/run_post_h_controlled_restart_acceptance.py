#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks
from factor_lab.controlled_restart_audit import dry_run_controlled_restart
from factor_lab.research_os.legacy_entrypoint import retired_legacy_entrypoint

SERVICE_NAME = "factor-lab-research-daemon.service"
PROCESS_PATTERNS = (
    "run_research_daemon.py",
    "run_research_task_worker.py",
    "run_controlled_orchestrator_once.py",
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run(command: list[str], *, timeout: int = 600, env: dict[str, str] | None = None) -> dict[str, Any]:
    completed = subprocess.run(command, text=True, capture_output=True, timeout=timeout, env=env)
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _service_state() -> str:
    result = _run(["systemctl", "--user", "is-active", SERVICE_NAME], timeout=30)
    stdout = (result.get("stdout") or "").strip()
    return stdout or "unknown"


def _residual_processes() -> list[dict[str, str]]:
    result = _run(["ps", "-eo", "pid,ppid,stat,etime,pcpu,pmem,cmd"], timeout=30)
    rows: list[dict[str, str]] = []
    for line in (result.get("stdout") or "").splitlines()[1:]:
        if any(pattern in line for pattern in PROCESS_PATTERNS) and "grep" not in line:
            parts = line.split(None, 6)
            if len(parts) >= 7:
                rows.append(
                    {
                        "pid": parts[0],
                        "ppid": parts[1],
                        "stat": parts[2],
                        "etime": parts[3],
                        "pcpu": parts[4],
                        "pmem": parts[5],
                        "cmd": parts[6],
                    }
                )
            else:
                rows.append({"cmd": line.strip()})
    return rows


def _stale_running_tasks(db_path: str | Path) -> list[dict[str, Any]]:
    path = Path(db_path)
    if not path.exists():
        return []
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT task_id, task_type, status, started_at_utc, worker_note, last_error
            FROM research_tasks
            WHERE status='running'
            ORDER BY started_at_utc DESC
            LIMIT 20
            """
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _run_bounded_daemon(*, timeout: int = 600) -> dict[str, Any]:
    import os

    env = os.environ.copy()
    env.update(
        {
            "RESEARCH_DAEMON_MAX_LOOPS": "1",
            "RESEARCH_DAEMON_EXIT_WHEN_IDLE": "1",
            "RESEARCH_DAEMON_EXIT_WHEN_NO_CLAIMABLE": "1",
            "RESEARCH_DAEMON_CONTROLLED_ONLY": "1",
            "RESEARCH_DAEMON_IDLE_SECONDS": "2",
            "RESEARCH_TASK_WORKER_TIMEOUT_SECONDS_WORKFLOW": env.get("RESEARCH_TASK_WORKER_TIMEOUT_SECONDS_WORKFLOW", "300"),
            "RESEARCH_TASK_WORKER_KILL_GRACE_SECONDS": env.get("RESEARCH_TASK_WORKER_KILL_GRACE_SECONDS", "5"),
        }
    )
    return _run(["python3", "scripts/run_research_daemon.py"], timeout=timeout, env=env)


def _run_one_shot(*, timeout: int = 600) -> dict[str, Any]:
    return _run(
        ["python3", "scripts/run_controlled_orchestrator_once.py", "--max-tasks", "1", "--require-would-run"],
        timeout=timeout,
    )


def _run_runtime_audit() -> dict[str, Any]:
    result = _run(["python3", "scripts/audit_runtime_takeover.py"], timeout=180)
    audit_path = Path("artifacts/runtime_takeover_audit.json")
    payload: dict[str, Any] = {"command_result": result}
    if audit_path.exists():
        try:
            payload.update(json.loads(audit_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError as exc:
            payload["json_error"] = str(exc)
    return payload


def _write_outputs(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "acceptance.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Post-H Controlled Restart Acceptance",
        "",
        f"OK: {payload.get('ok')}",
        f"Exit reason: {payload.get('exit_reason')}",
        f"Passed rounds: {payload.get('passed_round_count')} / {payload.get('requested_rounds')}",
        "",
        "## Rounds",
        "",
    ]
    for round_payload in payload.get("rounds", []):
        lines.extend(
            [
                f"### Round {round_payload.get('round_index')}",
                f"- OK: {round_payload.get('ok')}",
                f"- Exit reason: {round_payload.get('exit_reason')}",
                f"- Task IDs: {round_payload.get('task_ids')}",
                f"- Post-prepare would_run_count: {(round_payload.get('post_prepare_dry_run') or {}).get('would_run_count')}",
                f"- Post-run would_run_count: {(round_payload.get('post_run_dry_run') or {}).get('would_run_count')}",
                f"- Residual processes: {len(round_payload.get('residual_processes') or [])}",
                f"- Stale running tasks: {len(round_payload.get('stale_running_tasks') or [])}",
                "",
            ]
        )
    (output_dir / "acceptance.md").write_text("\n".join(lines), encoding="utf-8")


def _fail(payload: dict[str, Any], output_dir: Path, reason: str) -> dict[str, Any]:
    payload["ok"] = False
    payload["exit_reason"] = reason
    payload["finished_at_utc"] = _now()
    payload["passed_round_count"] = sum(1 for round_payload in payload.get("rounds", []) if round_payload.get("ok"))
    _write_outputs(output_dir, payload)
    return payload


def run_acceptance(
    *,
    rounds: int = 3,
    output_dir: str | Path = "artifacts/post_h_acceptance",
    db_path: str | Path = "artifacts/factor_lab.db",
    one_shot: bool = False,
    command_timeout: int = 600,
) -> dict[str, Any]:
    output = Path(output_dir)
    payload: dict[str, Any] = {
        "ok": False,
        "started_at_utc": _now(),
        "requested_rounds": int(rounds),
        "execution_mode": "one_shot" if one_shot else "bounded_daemon",
        "rounds": [],
    }

    service_state = _service_state()
    payload["initial_service_state"] = service_state
    if service_state != "inactive":
        return _fail(payload, output, "service_not_inactive")

    residual = _residual_processes()
    payload["initial_residual_processes"] = residual
    if residual:
        return _fail(payload, output, "residual_processes_before_start")

    baseline_dry_run = dry_run_controlled_restart(db_path=db_path)
    payload["baseline_dry_run"] = baseline_dry_run

    for index in range(1, int(rounds) + 1):
        round_payload: dict[str, Any] = {"round_index": index, "started_at_utc": _now(), "ok": False}
        payload["rounds"].append(round_payload)

        prepare_result = prepare_bucket_aware_tasks(
            db_path=db_path,
            dry_run=False,
            limit=1,
            priority=0,
            force_new=True,
        )
        round_payload["prepare_result"] = prepare_result
        round_payload["task_ids"] = [
            task.get("task_id") or task.get("id")
            for task in prepare_result.get("tasks", [])
            if isinstance(task, dict)
        ]

        post_prepare = dry_run_controlled_restart(db_path=db_path)
        round_payload["post_prepare_dry_run"] = post_prepare
        if int(post_prepare.get("would_run_count") or 0) != 1:
            round_payload["exit_reason"] = "unexpected_would_run_count"
            return _fail(payload, output, "unexpected_would_run_count")

        command_result = _run_one_shot(timeout=command_timeout) if one_shot else _run_bounded_daemon(timeout=command_timeout)
        round_payload["run_result"] = command_result
        if int(command_result.get("returncode") or 0) != 0:
            round_payload["exit_reason"] = "runner_failed"
            return _fail(payload, output, "runner_failed")

        post_run = dry_run_controlled_restart(db_path=db_path)
        round_payload["post_run_dry_run"] = post_run
        if int(post_run.get("would_run_count") or 0) != 0:
            round_payload["exit_reason"] = "post_run_would_run_not_zero"
            return _fail(payload, output, "post_run_would_run_not_zero")

        stale = _stale_running_tasks(db_path)
        round_payload["stale_running_tasks"] = stale
        if stale:
            round_payload["exit_reason"] = "stale_running_tasks"
            return _fail(payload, output, "stale_running_tasks")

        residual_after = _residual_processes()
        round_payload["residual_processes"] = residual_after
        if residual_after:
            round_payload["exit_reason"] = "residual_processes_after_round"
            return _fail(payload, output, "residual_processes_after_round")

        round_payload["ok"] = True
        round_payload["exit_reason"] = "round_passed"
        round_payload["finished_at_utc"] = _now()
        _write_outputs(output, payload)

    payload["runtime_audit"] = _run_runtime_audit()
    payload["ok"] = True
    payload["exit_reason"] = "passed"
    payload["finished_at_utc"] = _now()
    payload["passed_round_count"] = int(rounds)
    _write_outputs(output, payload)
    return payload


def main() -> int:
    # This acceptance harness intentionally enqueued and consumed legacy
    # SQLite tasks.  Keep its helpers importable, but retire the executable.
    return retired_legacy_entrypoint("scripts/run_post_h_controlled_restart_acceptance.py")


if __name__ == "__main__":
    raise SystemExit(main())
