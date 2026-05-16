from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "probe_research_daemon_active_worker_stop.py"
    spec = importlib.util.spec_from_file_location("probe_research_daemon_active_worker_stop_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_active_worker_probe_refuses_when_no_admitted_workflow(tmp_path, monkeypatch):
    probe = _load_module()
    calls = []

    monkeypatch.setattr(probe, "dry_run_controlled_restart", lambda db_path=None: {"would_run_count": 0, "pending_count": 4})
    monkeypatch.setattr(probe, "_run", lambda command, **kwargs: calls.append(command) or {"command": command, "returncode": 0})

    result = probe.probe_active_worker_stop(output_dir=tmp_path)

    assert result["ok"] is False
    assert result["exit_reason"] == "no_admitted_workflow"
    assert not any(command[:3] == ["systemctl", "--user", "start"] for command in calls)
    assert (tmp_path / "daemon_active_worker_stop_probe.json").exists()


def test_active_worker_probe_starts_waits_for_running_and_stops(tmp_path, monkeypatch):
    probe = _load_module()
    commands = []
    task_states = iter([
        None,
        {"task_id": "task-1", "task_type": "workflow", "status": "running", "started_at_utc": "now"},
    ])

    monkeypatch.setattr(probe, "dry_run_controlled_restart", lambda db_path=None: {"would_run_count": 1, "pending_count": 5})
    monkeypatch.setattr(probe, "_find_running_workflow_task", lambda db_path: next(task_states))
    monkeypatch.setattr(probe, "_residual_processes", lambda: [])
    monkeypatch.setattr(probe.time, "sleep", lambda seconds: None)

    def fake_run(command, **kwargs):
        commands.append(command)
        stdout = "inactive\n" if command[:3] == ["systemctl", "--user", "is-active"] else ""
        if command and command[0] == "journalctl":
            stdout = "clean stop\n"
        return {"command": command, "returncode": 0, "stdout": stdout, "stderr": ""}

    monkeypatch.setattr(probe, "_run", fake_run)

    result = probe.probe_active_worker_stop(output_dir=tmp_path, wait_running_seconds=5, poll_seconds=1)

    assert result["ok"] is True
    assert result["running_task"]["task_id"] == "task-1"
    assert ["systemctl", "--user", "start", probe.SERVICE_NAME] in commands
    assert ["systemctl", "--user", "stop", probe.SERVICE_NAME] in commands
    journal_commands = [command for command in commands if command and command[0] == "journalctl"]
    assert journal_commands and "--since" in journal_commands[-1]
    assert result["stop_timeout_detected"] is False
    payload = json.loads((tmp_path / "daemon_active_worker_stop_probe.json").read_text(encoding="utf-8"))
    assert payload["ok"] is True
    assert (tmp_path / "daemon_active_worker_stop_probe.md").exists()


def test_active_worker_probe_flags_residual_processes_and_journal_timeout(tmp_path, monkeypatch):
    probe = _load_module()

    monkeypatch.setattr(probe, "dry_run_controlled_restart", lambda db_path=None: {"would_run_count": 1, "pending_count": 5})
    monkeypatch.setattr(probe, "_find_running_workflow_task", lambda db_path: {"task_id": "task-1", "task_type": "workflow", "status": "running"})
    monkeypatch.setattr(probe, "_residual_processes", lambda: [{"pid": "123", "cmd": "python run_research_task_worker.py"}])
    monkeypatch.setattr(probe.time, "sleep", lambda seconds: None)

    def fake_run(command, **kwargs):
        stdout = "State 'stop-sigterm' timed out. Killing.\n" if command and command[0] == "journalctl" else ""
        return {"command": command, "returncode": 0, "stdout": stdout, "stderr": ""}

    monkeypatch.setattr(probe, "_run", fake_run)

    result = probe.probe_active_worker_stop(output_dir=tmp_path, wait_running_seconds=1, poll_seconds=1)

    assert result["ok"] is False
    assert result["stop_timeout_detected"] is True
    assert result["residual_processes"]
