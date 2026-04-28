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


def test_llm_usage_page_renders_24h_ledger_summary_and_chart(monkeypatch, tmp_path: Path):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    db_path = artifacts / "factor_lab.db"
    db_path.write_text("", encoding="utf-8")
    ledger = artifacts / "llm_usage_ledger.jsonl"
    ledger.write_text(
        '\n'.join([
            '{"created_at_utc":"2026-04-28T00:00:00+00:00","success":true,"decision_type":"planner","model":"gpt-5.5","profile_name":"ai-continue","context_mode":"compact","estimated_user_prompt_tokens_4c":100,"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15,"cached_tokens":7,"cache_creation_tokens":2,"uncached_prompt_tokens":3,"usage_source":"provider"}}',
            '{"created_at_utc":"2026-04-28T00:01:00+00:00","success":false,"decision_type":"failure_analyst","model":"gpt-5.5","profile_name":"nowcoding","context_mode":"compact","estimated_user_prompt_tokens_4c":200,"usage":{"prompt_tokens":null,"completion_tokens":null,"total_tokens":null,"usage_source":"missing"},"error_type":"http_error:403"}',
            '{"created_at_utc":"2026-04-26T00:00:00+00:00","success":true,"decision_type":"old_agent","model":"old-model","profile_name":"old","context_mode":"compact","estimated_user_prompt_tokens_4c":999,"usage":{"prompt_tokens":900,"completion_tokens":99,"total_tokens":999,"usage_source":"provider"}}',
        ]) + '\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "DB_PATH", db_path)
    monkeypatch.setattr(webui_app, "_utcnow", lambda: webui_app.datetime(2026, 4, 28, 1, 0, tzinfo=webui_app.timezone.utc))
    client = TestClient(webui_app.app)

    response = client.get("/llm-usage")

    assert response.status_code == 200
    assert "LLM Token 用量" in response.text
    assert "最近 24h" in response.text
    assert "2026-04-28 09:00" in response.text
    assert "Asia/Shanghai" in response.text
    assert "Token 趋势图" in response.text
    assert "llm-usage-chart" in response.text
    assert "08:00" in response.text
    assert "total_tokens" in response.text
    assert "cached_tokens" in response.text
    assert "cache_creation_tokens" in response.text
    assert "cached_tokens_missing_rows" in response.text
    assert "cache_creation_tokens_missing_rows" in response.text
    assert "uncached_prompt_tokens_missing_rows" in response.text
    assert "estimated_cost_usd" in response.text
    assert "cost_usd" in response.text
    assert "$0.000066" in response.text
    assert "7" in response.text
    assert "15" in response.text
    assert "planner" in response.text
    assert "failure_analyst" in response.text
    assert "http_error:403" in response.text
    assert "old_agent" not in response.text
    assert "old-model" not in response.text


def test_llm_usage_page_is_linked_from_navigation():
    client = TestClient(webui_app.app)

    response = client.get("/llm-usage")

    assert response.status_code == 200
    assert 'href="/llm-usage"' in response.text


def test_base_template_has_responsive_zoom_and_mobile_layout_rules():
    template = (Path(__file__).resolve().parents[1] / "src" / "factor_lab" / "webui_templates" / "base.html").read_text(encoding="utf-8")

    assert "@media (max-width: 900px)" in template
    assert "grid-template-columns: 1fr" in template
    assert "overflow-wrap: anywhere" in template
    assert "min-width: 0" in template
    assert "max-width: 100%" in template


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
