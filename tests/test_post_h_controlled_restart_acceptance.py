from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_post_h_controlled_restart_acceptance.py"
    spec = importlib.util.spec_from_file_location("post_h_acceptance_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_acceptance_refuses_when_service_is_active(tmp_path, monkeypatch):
    acceptance = _load_module()
    monkeypatch.setattr(acceptance, "_service_state", lambda: "active")
    monkeypatch.setattr(acceptance, "_residual_processes", lambda: [])

    result = acceptance.run_acceptance(rounds=3, output_dir=tmp_path)

    assert result["ok"] is False
    assert result["exit_reason"] == "service_not_inactive"
    assert result["rounds"] == []
    assert (tmp_path / "acceptance.json").exists()


def test_acceptance_requires_would_run_one_after_prepare(tmp_path, monkeypatch):
    acceptance = _load_module()
    dry_runs = iter([
        {"would_run_count": 0, "pending_count": 4},
        {"would_run_count": 2, "pending_count": 6},
    ])
    monkeypatch.setattr(acceptance, "_service_state", lambda: "inactive")
    monkeypatch.setattr(acceptance, "_residual_processes", lambda: [])
    monkeypatch.setattr(acceptance, "dry_run_controlled_restart", lambda db_path=None: next(dry_runs))
    monkeypatch.setattr(acceptance, "prepare_bucket_aware_tasks", lambda **kwargs: {"created_count": 1, "tasks": [{"task_id": "t1"}]})

    result = acceptance.run_acceptance(rounds=1, output_dir=tmp_path)

    assert result["ok"] is False
    assert result["exit_reason"] == "unexpected_would_run_count"
    assert result["rounds"][0]["post_prepare_dry_run"]["would_run_count"] == 2


def test_acceptance_records_each_successful_round(tmp_path, monkeypatch):
    acceptance = _load_module()
    dry_values = iter([
        {"would_run_count": 0, "pending_count": 4},
        {"would_run_count": 1, "pending_count": 5},
        {"would_run_count": 0, "pending_count": 4},
        {"would_run_count": 1, "pending_count": 5},
        {"would_run_count": 0, "pending_count": 4},
    ])
    calls = []
    monkeypatch.setattr(acceptance, "_service_state", lambda: "inactive")
    monkeypatch.setattr(acceptance, "_residual_processes", lambda: [])
    monkeypatch.setattr(acceptance, "_stale_running_tasks", lambda db_path: [])
    monkeypatch.setattr(acceptance, "dry_run_controlled_restart", lambda db_path=None: next(dry_values))
    monkeypatch.setattr(acceptance, "prepare_bucket_aware_tasks", lambda **kwargs: {"created_count": 1, "tasks": [{"task_id": f"t{len(calls)+1}"}]})

    def fake_run_bounded_daemon(**kwargs):
        calls.append(kwargs)
        return {"returncode": 0, "stdout": "ok", "stderr": "", "command": ["python3", "scripts/run_research_daemon.py"]}

    monkeypatch.setattr(acceptance, "_run_bounded_daemon", fake_run_bounded_daemon)
    monkeypatch.setattr(acceptance, "_run_runtime_audit", lambda: {"recommendations": ["pause_daemon"]})

    result = acceptance.run_acceptance(rounds=2, output_dir=tmp_path)

    assert result["ok"] is True
    assert result["passed_round_count"] == 2
    assert len(result["rounds"]) == 2
    assert all(round_payload["ok"] for round_payload in result["rounds"])
    artifact = json.loads((tmp_path / "acceptance.json").read_text(encoding="utf-8"))
    assert artifact["ok"] is True
    assert (tmp_path / "acceptance.md").exists()


def test_acceptance_fails_on_stale_running_task(tmp_path, monkeypatch):
    acceptance = _load_module()
    dry_values = iter([
        {"would_run_count": 0, "pending_count": 4},
        {"would_run_count": 1, "pending_count": 5},
        {"would_run_count": 0, "pending_count": 4},
    ])
    monkeypatch.setattr(acceptance, "_service_state", lambda: "inactive")
    monkeypatch.setattr(acceptance, "_residual_processes", lambda: [])
    monkeypatch.setattr(acceptance, "dry_run_controlled_restart", lambda db_path=None: next(dry_values))
    monkeypatch.setattr(acceptance, "prepare_bucket_aware_tasks", lambda **kwargs: {"created_count": 1, "tasks": [{"task_id": "t1"}]})
    monkeypatch.setattr(acceptance, "_run_bounded_daemon", lambda **kwargs: {"returncode": 0, "stdout": "ok", "stderr": ""})
    monkeypatch.setattr(acceptance, "_stale_running_tasks", lambda db_path: [{"task_id": "stale", "status": "running"}])

    result = acceptance.run_acceptance(rounds=1, output_dir=tmp_path)

    assert result["ok"] is False
    assert result["exit_reason"] == "stale_running_tasks"
    assert result["rounds"][0]["stale_running_tasks"]
