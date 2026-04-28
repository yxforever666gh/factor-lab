from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
from fastapi.testclient import TestClient

from factor_lab import webui_app


def test_load_llm_settings_masks_api_key_and_reads_env_file(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FACTOR_LAB_DECISION_PROVIDER=real_llm\n"
        "FACTOR_LAB_LLM_BASE_URL=https://example.test/v1\n"
        "FACTOR_LAB_LLM_MODEL=gpt-test\n"
        "FACTOR_LAB_LLM_API_KEY=sk-secret-value\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)

    settings = webui_app.load_llm_settings()

    assert settings["decision_provider"] == "real_llm"
    assert settings["base_url"] == "https://example.test/v1"
    assert settings["model"] == "gpt-test"
    assert settings["api_key_configured"] is True
    assert settings["api_key_masked"] == "sk-s...alue"
    assert settings["api_key"] == ""


def test_save_llm_settings_updates_env_file_preserves_unrelated_values_and_runtime_env(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# existing comment\n"
        "WEB_UI_PORT=8765\n"
        "FACTOR_LAB_LLM_API_KEY=old-secret\n"
        "FACTOR_LAB_LLM_MODEL=old-model\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    for key in webui_app.LLM_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)

    saved = webui_app.save_llm_settings(
        {
            "decision_provider": "real_llm",
            "live_decision_provider": "real_llm",
            "observation_decision_provider": "real_llm",
            "base_url": "https://new.example/v1",
            "model": "new-model",
            "api_key": "new-secret",
        }
    )

    text = env_file.read_text(encoding="utf-8")
    assert "# existing comment" in text
    assert "WEB_UI_PORT=8765" in text
    assert "FACTOR_LAB_DECISION_PROVIDER=real_llm" in text
    assert "FACTOR_LAB_LIVE_DECISION_PROVIDER=real_llm" in text
    assert "FACTOR_LAB_OBSERVATION_DECISION_PROVIDER=real_llm" in text
    assert "FACTOR_LAB_LLM_BASE_URL=https://new.example/v1" in text
    assert "FACTOR_LAB_LLM_MODEL=new-model" in text
    assert "FACTOR_LAB_LLM_API_KEY=new-secret" in text
    assert saved["api_key_masked"] == "new-...cret"
    assert webui_app.os.environ["FACTOR_LAB_LLM_MODEL"] == "new-model"


def test_save_llm_settings_keeps_existing_api_key_when_form_leaves_it_blank(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FACTOR_LAB_LLM_API_KEY=keep-secret\n"
        "FACTOR_LAB_LLM_MODEL=old-model\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.setenv("FACTOR_LAB_LLM_API_KEY", "keep-secret")

    webui_app.save_llm_settings(
        {
            "decision_provider": "real_llm",
            "live_decision_provider": "real_llm",
            "observation_decision_provider": "real_llm",
            "base_url": "https://new.example/v1",
            "model": "new-model",
            "api_key": "",
        }
    )

    text = env_file.read_text(encoding="utf-8")
    assert "FACTOR_LAB_LLM_API_KEY=keep-secret" in text
    assert webui_app.os.environ["FACTOR_LAB_LLM_API_KEY"] == "keep-secret"


def test_load_llm_settings_reads_multiple_profiles_in_fallback_order(tmp_path: Path, monkeypatch):
    profiles = [
        {"name": "backup", "base_url": "https://backup.test/v1", "model": "backup-model", "api_key": "backup-secret", "api_format": "anthropic", "enabled": True},
        {"name": "primary", "base_url": "https://primary.test/v1", "model": "primary-model", "api_key": "primary-secret", "api_format": "openai_responses", "enabled": True},
    ]
    env_file = tmp_path / ".env"
    env_file.write_text(
        "FACTOR_LAB_LLM_PROFILES_JSON=" + webui_app.json.dumps(profiles, ensure_ascii=False) + "\n"
        "FACTOR_LAB_LLM_FALLBACK_ORDER=primary,backup\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.delenv("FACTOR_LAB_LLM_PROFILES_JSON", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_FALLBACK_ORDER", raising=False)

    settings = webui_app.load_llm_settings()

    assert [profile["name"] for profile in settings["profiles"]] == ["primary", "backup"]
    assert settings["profiles"][0]["api_key"] == ""
    assert settings["profiles"][0]["api_key_masked"] == "prim...cret"
    assert settings["profiles"][0]["api_format"] == "openai_responses"
    assert settings["profiles"][1]["api_format"] == "anthropic"
    assert settings["fallback_order"] == "primary,backup"


def test_save_llm_settings_writes_profile_api_format_selection(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)

    settings = webui_app.save_llm_settings(
        {
            "decision_provider": "real_llm",
            "live_decision_provider": "real_llm",
            "observation_decision_provider": "real_llm",
            "profile_order_0": "1",
            "profile_name_0": "primary",
            "profile_base_url_0": "https://primary.test/v1",
            "profile_model_0": "gpt-5.5",
            "profile_api_key_0": "primary-secret",
            "profile_api_format_0": "openai_responses",
            "profile_enabled_0": "on",
            "profile_order_1": "2",
            "profile_name_1": "opus",
            "profile_base_url_1": "https://anthropic.test/v1",
            "profile_model_1": "opus4.7",
            "profile_api_key_1": "opus-secret",
            "profile_api_format_1": "anthropic",
            "profile_enabled_1": "on",
        }
    )

    profiles = webui_app.json.loads(webui_app.os.environ["FACTOR_LAB_LLM_PROFILES_JSON"])
    assert profiles[0]["api_format"] == "openai_responses"
    assert profiles[1]["api_format"] == "anthropic"
    assert settings["profiles"][0]["api_format"] == "openai_responses"


def test_settings_page_renders_api_format_choices_and_model_test_button(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("FACTOR_LAB_LLM_MODEL=gpt-5.5\nFACTOR_LAB_LLM_API_KEY=secret\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    client = TestClient(webui_app.app)

    response = client.get("/settings")

    assert response.status_code == 200
    assert "测试模型" in response.text
    assert "OpenAI Responses" in response.text
    assert "OpenAI Chat Completions" in response.text
    assert "Anthropic Messages" in response.text


def test_settings_model_test_button_runs_selected_profile_without_saving(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    tested = []

    def fake_test(profile):
        tested.append(profile)
        return {"ok": True, "message": "模型测试成功", "api_format": profile["api_format"], "model": profile["model"]}

    monkeypatch.setattr(webui_app, "test_llm_profile_connection", fake_test)
    client = TestClient(webui_app.app)

    response = client.post(
        "/settings/test-model",
        data={
            "profile_test_index": "0",
            "profile_name_0": "primary",
            "profile_base_url_0": "https://primary.test/v1",
            "profile_model_0": "gpt-5.5",
            "profile_api_key_0": "secret",
            "profile_api_format_0": "openai_responses",
            "profile_enabled_0": "on",
        },
    )

    assert response.status_code == 200
    assert "模型测试成功" in response.text
    assert tested[0]["api_format"] == "openai_responses"
    assert "FACTOR_LAB_LLM_PROFILES_JSON" not in env_file.read_text(encoding="utf-8")


def test_save_llm_settings_writes_multiple_profiles_and_legacy_first_profile(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    for key in [*webui_app.LLM_ENV_KEYS, "FACTOR_LAB_LLM_PROFILES_JSON", "FACTOR_LAB_LLM_FALLBACK_ORDER"]:
        monkeypatch.delenv(key, raising=False)

    settings = webui_app.save_llm_settings(
        {
            "decision_provider": "real_llm",
            "live_decision_provider": "real_llm",
            "observation_decision_provider": "real_llm",
            "profile_name_0": "primary",
            "profile_base_url_0": "https://primary.test/v1",
            "profile_model_0": "primary-model",
            "profile_api_key_0": "primary-secret",
            "profile_enabled_0": "on",
            "profile_name_1": "backup",
            "profile_base_url_1": "https://backup.test/v1",
            "profile_model_1": "backup-model",
            "profile_api_key_1": "backup-secret",
            "profile_enabled_1": "on",
            "fallback_order": "backup,primary",
        }
    )

    text = env_file.read_text(encoding="utf-8")
    assert "FACTOR_LAB_LLM_FALLBACK_ORDER=backup,primary" in text
    assert "FACTOR_LAB_LLM_PROFILES_JSON=" in text
    assert "FACTOR_LAB_LLM_BASE_URL=https://backup.test/v1" in text
    assert "FACTOR_LAB_LLM_MODEL=backup-model" in text
    assert "FACTOR_LAB_LLM_API_KEY=backup-secret" in text
    profiles = webui_app.json.loads(webui_app.os.environ["FACTOR_LAB_LLM_PROFILES_JSON"])
    assert [profile["name"] for profile in profiles] == ["backup", "primary"]
    assert settings["profiles"][0]["name"] == "backup"


def test_save_llm_settings_uses_numeric_order_fields_when_present(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)

    settings = webui_app.save_llm_settings(
        {
            "decision_provider": "real_llm",
            "live_decision_provider": "real_llm",
            "observation_decision_provider": "real_llm",
            "profile_order_0": "2",
            "profile_name_0": "primary",
            "profile_base_url_0": "https://primary.test/v1",
            "profile_model_0": "primary-model",
            "profile_api_key_0": "primary-secret",
            "profile_enabled_0": "on",
            "profile_order_1": "1",
            "profile_name_1": "backup",
            "profile_base_url_1": "https://backup.test/v1",
            "profile_model_1": "backup-model",
            "profile_api_key_1": "backup-secret",
            "profile_enabled_1": "on",
        }
    )

    assert settings["fallback_order"] == "backup,primary"
    assert [profile["name"] for profile in settings["profiles"]] == ["backup", "primary"]


def test_restart_research_daemon_after_settings_save_reports_success(monkeypatch):
    calls = []

    def fake_run(command, capture_output, text, timeout):
        calls.append(command)
        return type("Completed", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    monkeypatch.setattr(webui_app.subprocess, "run", fake_run)

    result = webui_app.restart_research_daemon_after_settings_save()

    assert result["ok"] is True
    assert calls == [["systemctl", "--user", "restart", "factor-lab-research-daemon.service"]]
