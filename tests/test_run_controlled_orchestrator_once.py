from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_controlled_orchestrator_once.py"
    spec = importlib.util.spec_from_file_location("run_controlled_orchestrator_once_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_controlled_runner_refuses_when_dry_run_has_zero_would_run(tmp_path, monkeypatch):
    runner = _load_module()
    monkeypatch.setattr(runner, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 0, "pending_count": 1, "blocked_count": 0})
    called = {"orchestrator": False}
    monkeypatch.setattr(runner, "run_orchestrator", lambda max_tasks: called.__setitem__("orchestrator", True))

    result = runner.run_controlled_orchestrator_once(max_tasks=2, require_would_run=True, output_dir=tmp_path)

    assert result["ok"] is False
    assert result["exit_reason"] == "no_admitted_workflow"
    assert called["orchestrator"] is False


def test_controlled_runner_caps_max_tasks_to_would_run(tmp_path, monkeypatch):
    runner = _load_module()
    monkeypatch.setattr(runner, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 1, "pending_count": 1, "blocked_count": 0})
    seen = {}
    def fake_run_orchestrator(max_tasks):
        seen["max_tasks"] = max_tasks
        return {"processed": [{"status": "finished"}]}
    monkeypatch.setattr(runner, "run_orchestrator", fake_run_orchestrator)

    result = runner.run_controlled_orchestrator_once(max_tasks=3, require_would_run=True, output_dir=tmp_path)

    assert result["ok"] is True
    assert seen["max_tasks"] == 1
    assert result["processed_count"] == 1


def test_controlled_runner_writes_summary_json(tmp_path, monkeypatch):
    runner = _load_module()
    monkeypatch.setattr(runner, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 1, "pending_count": 1, "blocked_count": 0})
    monkeypatch.setattr(runner, "run_orchestrator", lambda max_tasks: {"processed": []})

    result = runner.run_controlled_orchestrator_once(max_tasks=1, require_would_run=True, output_dir=tmp_path)

    payload = json.loads((tmp_path / "controlled_orchestrator_once.json").read_text(encoding="utf-8"))
    assert payload["exit_reason"] == result["exit_reason"]
    assert (tmp_path / "controlled_orchestrator_once.md").exists()


def test_controlled_runner_writes_failure_payload_on_timeout(tmp_path, monkeypatch):
    runner = _load_module()
    monkeypatch.setattr(runner, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 1, "pending_count": 1, "blocked_count": 0})
    monkeypatch.setattr(
        runner,
        "run_orchestrator",
        lambda max_tasks: (_ for _ in ()).throw(RuntimeError("research task worker timeout after 1s")),
    )

    result = runner.run_controlled_orchestrator_once(max_tasks=1, require_would_run=True, output_dir=tmp_path)

    payload = json.loads((tmp_path / "controlled_orchestrator_once.json").read_text(encoding="utf-8"))
    assert result["ok"] is False
    assert result["exit_reason"] == "orchestrator_failed"
    assert "timeout" in result["error"]
    assert payload["exit_reason"] == "orchestrator_failed"
    assert (tmp_path / "controlled_orchestrator_once.md").exists()
