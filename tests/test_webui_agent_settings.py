from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
from fastapi.testclient import TestClient

from factor_lab import webui_app


def test_load_agent_settings_renders_default_roles(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("FACTOR_LAB_LLM_FALLBACK_ORDER=nowcoding,ai-continue,ccvibe\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)

    settings = webui_app.load_agent_settings()

    assert [role["name"] for role in settings["roles"]] == ["planner", "failure_analyst", "reviewer", "data_quality"]
    assert settings["roles"][0]["display_name"] == "规划 Agent"
    assert settings["roles"][1]["display_name"] == "失败诊断 Agent"
    assert settings["roles"][2]["display_name"] == "质量复核 Agent"
    assert settings["roles"][3]["display_name"] == "数据质量 Agent"
    assert settings["roles"][0]["llm_fallback_order"] == "nowcoding,ai-continue,ccvibe"


def test_save_agent_settings_writes_roles_json_and_runtime_env(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)

    settings = webui_app.save_agent_settings(
        {
            "role_name_0": "planner",
            "role_display_name_0": "规划 Agent",
            "role_enabled_0": "on",
            "role_decision_types_0": "planner",
            "role_purpose_0": "plan",
            "role_system_prompt_0": "planner prompt",
            "role_fallback_order_0": "ccvibe,nowcoding",
            "role_timeout_seconds_0": "90",
            "role_max_retries_0": "1",
            "role_strict_schema_0": "on",
            "role_legacy_agent_id_0": "factor-lab-planner",
        }
    )

    text = env_file.read_text(encoding="utf-8")
    assert "WEB_UI_PORT=8765" in text
    assert "FACTOR_LAB_AGENT_ROLES_JSON=" in text
    assert webui_app.os.environ["FACTOR_LAB_AGENT_ROLES_JSON"]
    assert settings["roles"][0]["name"] == "planner"
    assert settings["roles"][0]["llm_fallback_order"] == "ccvibe,nowcoding"


def test_agents_page_renders_default_roles(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("FACTOR_LAB_LLM_FALLBACK_ORDER=nowcoding,ai-continue\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)
    client = TestClient(webui_app.app)

    response = client.get("/agents")

    assert response.status_code == 200
    assert "规划 Agent" in response.text
    assert "失败诊断 Agent" in response.text
    assert "质量复核 Agent" in response.text
    assert "数据质量 Agent" in response.text
    assert "Agent 设置" in response.text


def test_agents_save_restarts_daemon(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    calls = []

    def fake_restart():
        calls.append("restart")
        return {"ok": True}

    monkeypatch.setattr(webui_app, "restart_research_daemon_after_settings_save", fake_restart)
    client = TestClient(webui_app.app)

    response = client.post(
        "/agents",
        data={
            "role_name_0": "planner",
            "role_display_name_0": "规划 Agent",
            "role_enabled_0": "on",
            "role_decision_types_0": "planner",
            "role_purpose_0": "plan",
            "role_system_prompt_0": "planner prompt",
            "role_fallback_order_0": "ccvibe,nowcoding",
            "role_timeout_seconds_0": "90",
            "role_max_retries_0": "1",
            "role_strict_schema_0": "on",
            "role_legacy_agent_id_0": "factor-lab-planner",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/agents?saved=1&restart=1"
    assert calls == ["restart"]


def test_llm_page_shows_agent_roles(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("FACTOR_LAB_LLM_FALLBACK_ORDER=ccvibe,nowcoding\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)
    client = TestClient(webui_app.app)

    response = client.get("/llm")

    assert response.status_code == 200
    assert "Agent Roles" in response.text
    assert "planner" in response.text
    assert "failure_analyst" in response.text
    assert "reviewer" in response.text
    assert "data_quality" in response.text
