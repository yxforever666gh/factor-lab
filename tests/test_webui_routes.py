import time
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
from fastapi.testclient import TestClient

from factor_lab import webui_app


def test_dashboard_root_is_lightweight(monkeypatch, tmp_path: Path):
    env_path = tmp_path / ".env"
    env_path.write_text("FACTOR_LAB_DECISION_PROVIDER=real_llm\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_path)
    monkeypatch.setattr(webui_app, "_quick_daemon_status", lambda: {"active": True, "label": "active", "detail": "test"})
    monkeypatch.setattr(webui_app, "_quick_latest_runs", lambda limit=5: [])
    monkeypatch.setattr(
        webui_app,
        "get_cached_health_metrics",
        lambda: (_ for _ in ()).throw(AssertionError("root page must not call heavy health metrics")),
    )
    client = TestClient(webui_app.app)

    start = time.monotonic()
    response = client.get("/")
    elapsed = time.monotonic() - start

    assert response.status_code == 200
    assert elapsed < 1.0
    assert "轻量首页" in response.text
    assert "完整驾驶舱" in response.text
    assert "real_llm" in response.text


def test_control_page_is_read_only(monkeypatch, tmp_path: Path):
    env_path = tmp_path / ".env"
    env_path.write_text("FACTOR_LAB_DECISION_PROVIDER=real_llm\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_path)
    monkeypatch.setattr(
        webui_app,
        "_systemd_service_snapshot",
        lambda service: {
            "name": service,
            "active_state": "active",
            "main_pid": "123",
            "working_directory": "/home/admin/factor-lab",
            "exec_start": "python scripts/run_research_daemon.py",
            "fragment_path": "/tmp/service",
        },
    )
    monkeypatch.setattr(
        webui_app,
        "_quick_research_queue_snapshot",
        lambda: (
            {"pending": 1, "running": 2, "finished_24h": 3, "failed_24h": 4},
            {"id": "t1", "task_type": "workflow", "status": "running", "created_at_utc": "now", "worker_note": "note"},
        ),
    )
    monkeypatch.setattr(webui_app, "_quick_heartbeat", lambda: {"timestamp": "now"})
    client = TestClient(webui_app.app)

    response = client.get("/control")

    assert response.status_code == 200
    assert "Read-only Control" in response.text
    assert "Provider" in response.text
    assert "Queue" in response.text
    assert "real_llm" in response.text
