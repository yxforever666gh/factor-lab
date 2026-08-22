import time
import re
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
from fastapi.testclient import TestClient

from factor_lab import webui_app


def test_dashboard_root_is_lightweight(monkeypatch, tmp_path: Path):
    env_path = tmp_path / ".env"
    env_path.write_text("FACTOR_LAB_DECISION_PROVIDER=direct_model\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_path)
    monkeypatch.setattr(webui_app, "_quick_daemon_status", lambda: {"active": True, "label": "active", "detail": "test"})
    monkeypatch.setattr(webui_app, "_quick_latest_runs", lambda limit=5: [])
    webui_app._OVERVIEW_CACHE.update({"key": None, "created_at": 0.0, "payload": None})
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
    assert "Champion 与当前暴露" in response.text
    assert "数据健康" in response.text
    assert "Legacy 研究参考" not in response.text


def test_llm_usage_page_redirects_without_reading_legacy_ledger(monkeypatch):
    monkeypatch.setattr(
        webui_app,
        "_load_llm_usage_rows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("legacy JSONL ledger must not be read")
        ),
    )

    response = TestClient(webui_app.app).get("/llm-usage", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/data-sources"


def test_redirect_only_llm_usage_page_is_not_linked_from_navigation():
    client = TestClient(webui_app.app)

    response = client.get("/llm-usage")

    assert response.status_code == 200
    assert 'href="/llm-usage"' not in response.text


def test_research_quality_page_redirects_before_legacy_summary(monkeypatch):
    monkeypatch.setattr(
        webui_app,
        "build_research_quality_summary",
        lambda: (_ for _ in ()).throw(AssertionError("legacy research summary must not run")),
    )
    client = TestClient(webui_app.app)

    response = client.get("/research-quality", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/research"


def test_base_template_has_responsive_zoom_and_mobile_layout_rules():
    template = (Path(__file__).resolve().parents[1] / "src" / "factor_lab" / "webui_static" / "webui.css").read_text(encoding="utf-8")

    assert "@media (max-width: 900px)" in template
    assert "grid-template-columns: 1fr" in template
    assert "overflow-x: auto" in template
    assert "min-width: 0" in template
    assert ".table-wrap" in template


def test_control_page_redirects_before_legacy_runtime_state(monkeypatch, tmp_path: Path):
    env_path = tmp_path / ".env"
    env_path.write_text("FACTOR_LAB_DECISION_PROVIDER=direct_model\n", encoding="utf-8")
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

    response = client.get("/control", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/runs"


def test_primary_navigation_has_exactly_five_grouped_entries():
    response = TestClient(webui_app.app).get("/")
    nav = re.search(r'<nav class="primary-nav"[\s\S]*?</nav>', response.text)

    assert response.status_code == 200
    assert nav is not None
    assert nav.group(0).count("<a ") == 5
    for label, href in [("总览", "/"), ("研究", "/research"), ("组合", "/portfolios"), ("运行", "/runs"), ("设置", "/data-sources")]:
        assert label in nav.group(0)
        assert f'href="{href}"' in nav.group(0)


def test_unprojected_secondary_page_redirects_to_active_primary_group():
    client = TestClient(webui_app.app)
    redirect = client.get("/robustness", follow_redirects=False)
    response = client.get("/robustness")

    assert redirect.status_code == 307
    assert redirect.headers["location"] == "/research"
    assert response.status_code == 200
    assert re.search(r'href="/research" class="nav-item active"', response.text)
    assert re.search(
        r'<nav class="mobile-nav"[\s\S]*?href="/research" class="active" aria-current="page"',
        response.text,
    )
    assert 'class="secondary-nav"' in response.text
    assert "研究总览" in response.text
    assert 'href="/research#hypothesis-lineage"' in response.text
    assert 'href="/research#trial-budget"' in response.text
    assert 'href="/research#experiments"' in response.text
    assert 'href="/research#recovery"' in response.text
    assert "实验与否证" in response.text
    for href in ["/factors", "/candidates", "/families", "/candidate-clusters", "/research-quality"]:
        assert f'href="{href}"' not in response.text


def test_static_css_and_legacy_dashboard_redirects():
    client = TestClient(webui_app.app)

    css = client.get("/static/webui.css")
    assert css.status_code == 200
    assert "text/css" in css.headers["content-type"]
    for path in ["/cockpit", "/dashboard-full"]:
        response = client.get(path, follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/"


def test_ops_actions_are_post_only(monkeypatch):
    calls = []
    monkeypatch.setattr(webui_app, "trigger_script", lambda script: calls.append(script) or {"script": script, "returncode": 0, "stdout": "ok", "stderr": ""})
    client = TestClient(webui_app.app)

    assert client.get("/ops/run/workflow").status_code == 405
    response = client.post("/ops/run/workflow")
    assert response.status_code == 410
    assert calls == []
    assert client.post("/ops/run/not-a-target").status_code == 404


def test_model_settings_excludes_removed_hermes_provider():
    response = TestClient(webui_app.app).get("/settings")

    assert response.status_code == 200
    assert "direct_model" in response.text
    assert "heuristic" in response.text
    assert "mock" in response.text
    assert "hermes_native_gateway" not in response.text
