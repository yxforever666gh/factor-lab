import importlib.util
import json
import sys
from pathlib import Path


def _load_daemon_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_research_daemon.py"
    module_name = "run_research_daemon_heartbeat_test"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_write_daemon_heartbeat_writes_required_schema(tmp_path, monkeypatch):
    daemon = _load_daemon_module()
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    monkeypatch.setattr(daemon, "_artifacts_path", lambda: artifacts)
    monkeypatch.setattr(daemon, "_root_path", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_DECISION_PROVIDER", "real_llm")

    daemon.write_daemon_heartbeat(
        "running",
        {
            "processed_tasks_total": 7,
            "rss_mb": 123,
            "last_processed": {"id": "task-1", "task_type": "workflow", "status": "finished"},
            "skip_reasons_24h": {"recently_finished_same_fingerprint": 2},
        },
    )

    payload = json.loads((artifacts / "research_daemon_heartbeat.json").read_text(encoding="utf-8"))
    assert payload["pid"]
    assert payload["project_root"] == str(tmp_path)
    assert payload["provider"] == "real_llm"
    assert payload["state"] == "running"
    assert payload["queue"]["pending"] == 0
    assert payload["queue"]["running"] == 0
    assert payload["queue"]["finished_24h"] == 0
    assert payload["queue"]["failed_24h"] == 0
    assert payload["current_task"]["id"] == "task-1"
    assert payload["processed_tasks_total"] == 7
    assert payload["rss_mb"] == 123
    assert "api_key" not in json.dumps(payload).lower()
