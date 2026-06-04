from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
from fastapi.testclient import TestClient

from factor_lab import webui_app


def _clear_data_source_env(monkeypatch):
    for key in getattr(webui_app, "DATA_SOURCE_ENV_KEYS", [
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON",
        "FACTOR_LAB_DATA_SOURCE_ORDER",
        "FACTOR_LAB_PRIMARY_DATA_SOURCE",
        "TUSHARE_TOKEN",
        "DIEMENG_API_KEY",
    ]):
        monkeypatch.delenv(key, raising=False)


def test_load_data_source_settings_masks_keys_and_reads_env_file(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    profiles = [
        {"name": "backup-diemeng", "source_type": "diemeng", "api_key": "diemeng-secret", "enabled": True, "notes": "backup"},
        {"name": "primary-tushare", "source_type": "tushare", "api_key": "tushare-secret", "enabled": True, "notes": "main"},
    ]
    env_file.write_text(
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=" + webui_app.json.dumps(profiles, ensure_ascii=False) + "\n"
        "FACTOR_LAB_DATA_SOURCE_ORDER=primary-tushare,backup-diemeng\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)

    settings = webui_app.load_data_source_settings()

    assert [profile["name"] for profile in settings["profiles"]] == ["primary-tushare", "backup-diemeng"]
    assert settings["profiles"][0]["api_key"] == ""
    assert settings["profiles"][0]["api_key_configured"] is True
    assert settings["profiles"][0]["api_key_masked"] == "tush...cret"
    assert settings["profiles"][0]["source_type"] == "tushare"
    assert settings["order"] == "primary-tushare,backup-diemeng"


def test_save_data_source_settings_preserves_unrelated_env_and_updates_runtime_env(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "# keep me\n"
        "WEB_UI_PORT=8765\n"
        "TUSHARE_TOKEN=old-token\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)

    saved = webui_app.save_data_source_settings(
        {
            "source_order_0": "1",
            "source_enabled_0": "on",
            "source_type_0": "tushare",
            "source_name_0": "primary-tushare",
            "source_api_key_0": "new-tushare-token",
            "source_notes_0": "main source",
        }
    )

    text = env_file.read_text(encoding="utf-8")
    assert "# keep me" in text
    assert "WEB_UI_PORT=8765" in text
    assert "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=" in text
    assert "FACTOR_LAB_DATA_SOURCE_ORDER=primary-tushare" in text
    assert "FACTOR_LAB_PRIMARY_DATA_SOURCE=tushare" in text
    assert "TUSHARE_TOKEN=new-tushare-token" in text
    assert webui_app.os.environ["TUSHARE_TOKEN"] == "new-tushare-token"
    assert saved["profiles"][0]["api_key_masked"] == "new-...oken"


def test_save_data_source_settings_keeps_existing_key_when_form_leaves_it_blank(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    profiles = [
        {"name": "primary-tushare", "source_type": "tushare", "api_key": "keep-secret", "enabled": True, "notes": ""},
    ]
    env_file.write_text(
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=" + webui_app.json.dumps(profiles, ensure_ascii=False) + "\n"
        "FACTOR_LAB_DATA_SOURCE_ORDER=primary-tushare\n"
        "TUSHARE_TOKEN=keep-secret\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)

    webui_app.save_data_source_settings(
        {
            "source_order_0": "1",
            "source_enabled_0": "on",
            "source_type_0": "tushare",
            "source_name_0": "primary-tushare",
            "source_api_key_0": "",
            "source_notes_0": "renamed note",
        }
    )

    text = env_file.read_text(encoding="utf-8")
    assert "TUSHARE_TOKEN=keep-secret" in text
    profiles = webui_app.json.loads(webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"])
    assert profiles[0]["api_key"] == "keep-secret"
    assert profiles[0]["notes"] == "renamed note"


def test_save_data_source_settings_uses_numeric_order_fields_and_enabled_only_order(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)

    settings = webui_app.save_data_source_settings(
        {
            "source_order_0": "2",
            "source_enabled_0": "on",
            "source_type_0": "tushare",
            "source_name_0": "primary-tushare",
            "source_api_key_0": "tushare-token",
            "source_order_1": "1",
            "source_enabled_1": "on",
            "source_type_1": "diemeng",
            "source_name_1": "backup-diemeng",
            "source_api_key_1": "diemeng-token",
            "source_order_2": "3",
            "source_type_2": "custom",
            "source_name_2": "disabled-custom",
            "source_api_key_2": "custom-token",
        }
    )

    assert [profile["name"] for profile in settings["profiles"]] == ["backup-diemeng", "primary-tushare", "disabled-custom"]
    assert settings["order"] == "backup-diemeng,primary-tushare"
    assert webui_app.os.environ["TUSHARE_TOKEN"] == "tushare-token"
    assert webui_app.os.environ["DIEMENG_API_KEY"] == "diemeng-token"


def test_save_data_source_settings_ignores_fully_blank_rows(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)

    settings = webui_app.save_data_source_settings(
        {
            "source_order_0": "1",
            "source_type_0": "tushare",
            "source_name_0": "",
            "source_api_key_0": "",
            "source_notes_0": "",
        }
    )

    assert settings["profiles"] == []
    assert webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"] == "[]"


def test_data_sources_page_renders_draggable_rows_without_raw_secret(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    profiles = [
        {"name": "primary-tushare", "source_type": "tushare", "api_key": "super-secret-token", "enabled": True, "notes": ""},
    ]
    env_file.write_text(
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=" + webui_app.json.dumps(profiles, ensure_ascii=False) + "\n"
        "FACTOR_LAB_DATA_SOURCE_ORDER=primary-tushare\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)
    client = TestClient(webui_app.app)

    response = client.get("/data-sources")

    assert response.status_code == 200
    assert "数据源设置" in response.text
    assert "draggable=\"true\"" in response.text
    assert "data-source-profile-rows" in response.text
    assert "Tushare" in response.text
    assert "super-secret-token" not in response.text
    assert "supe...oken" in response.text


def test_data_sources_post_saves_and_redirects(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.setattr(webui_app, "restart_research_daemon_after_settings_save", lambda: {"ok": True})
    _clear_data_source_env(monkeypatch)
    client = TestClient(webui_app.app)

    response = client.post(
        "/data-sources",
        data={
            "source_order_0": "1",
            "source_enabled_0": "on",
            "source_type_0": "tushare",
            "source_name_0": "primary-tushare",
            "source_api_key_0": "new-token",
            "source_notes_0": "main",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/data-sources?saved=1&restart=1"
    assert "TUSHARE_TOKEN=new-token" in env_file.read_text(encoding="utf-8")


def test_data_source_test_button_runs_selected_profile_without_saving(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    tested = []

    def fake_test(profile):
        tested.append(profile)
        return {"ok": True, "message": "数据源测试成功", "source_type": profile["source_type"], "name": profile["name"]}

    monkeypatch.setattr(webui_app, "test_data_source_connection", fake_test)
    _clear_data_source_env(monkeypatch)
    client = TestClient(webui_app.app)

    response = client.post(
        "/data-sources/test",
        data={
            "source_test_index": "0",
            "source_order_0": "1",
            "source_enabled_0": "on",
            "source_type_0": "tushare",
            "source_name_0": "primary-tushare",
            "source_api_key_0": "secret-token",
            "source_notes_0": "main",
        },
    )

    assert response.status_code == 200
    assert "数据源测试成功" in response.text
    assert tested[0]["source_type"] == "tushare"
    assert "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON" not in env_file.read_text(encoding="utf-8")
