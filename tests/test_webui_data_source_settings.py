from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd
import pytest

pytest.importorskip("fastapi")
pytest.importorskip("jinja2")
from fastapi.testclient import TestClient

from factor_lab import webui_app
from factor_lab.webui.services import env_settings as env_settings_service


def _clear_data_source_env(monkeypatch):
    for key in getattr(webui_app, "DATA_SOURCE_ENV_KEYS", [
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON",
        "FACTOR_LAB_DATA_SOURCE_ORDER",
        "FACTOR_LAB_PRIMARY_DATA_SOURCE",
        "TUSHARE_TOKEN",
        "DIEMENG_API_KEY",
    ]):
        monkeypatch.delenv(key, raising=False)


def _use_test_secrets(tmp_path: Path, monkeypatch) -> Path:
    secrets = tmp_path / "secrets"
    monkeypatch.setenv("FACTOR_LAB_SECRETS_DIR", str(secrets))
    return secrets


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
    secrets = _use_test_secrets(tmp_path, monkeypatch)

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
    assert "new-tushare-token" not in text
    assert "TUSHARE_TOKEN=\n" in text
    assert webui_app.os.environ["TUSHARE_TOKEN"] == ""
    raw_profiles = webui_app.json.loads(
        webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"]
    )
    assert raw_profiles[0]["api_key"] == ""
    assert raw_profiles[0]["credential_ref"].startswith("secret://")
    assert saved["profiles"][0]["api_key_masked"] == "已安全配置"
    secret_file = secrets / raw_profiles[0]["credential_ref"].removeprefix("secret://")
    assert secret_file.read_text(encoding="utf-8").strip() == "new-tushare-token"


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
    secrets = _use_test_secrets(tmp_path, monkeypatch)

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
    assert "keep-secret" not in text
    profiles = webui_app.json.loads(webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"])
    assert profiles[0]["api_key"] == ""
    assert profiles[0]["credential_ref"].startswith("secret://")
    assert profiles[0]["notes"] == "renamed note"
    secret_file = secrets / profiles[0]["credential_ref"].removeprefix("secret://")
    assert secret_file.read_text(encoding="utf-8").strip() == "keep-secret"


def test_save_data_source_settings_uses_numeric_order_fields_and_enabled_only_order(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)
    secrets = _use_test_secrets(tmp_path, monkeypatch)

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
    assert webui_app.os.environ["TUSHARE_TOKEN"] == ""
    assert webui_app.os.environ["DIEMENG_API_KEY"] == ""
    raw_profiles = webui_app.json.loads(
        webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"]
    )
    assert all(profile["api_key"] == "" for profile in raw_profiles)
    diemeng_secret = secrets / raw_profiles[0]["credential_ref"].removeprefix("secret://")
    tushare_secret = secrets / raw_profiles[1]["credential_ref"].removeprefix("secret://")
    assert diemeng_secret.read_text(encoding="utf-8").strip() == "diemeng-token"
    assert tushare_secret.read_text(encoding="utf-8").strip() == "tushare-token"


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
    assert "AkShare" in response.text
    assert "本地 PIT 文件" in response.text
    assert "super-secret-token" not in response.text
    assert "supe...oken" in response.text


def test_data_sources_post_saves_and_redirects(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    monkeypatch.setattr(webui_app, "restart_research_daemon_after_settings_save", lambda: {"ok": True})
    _clear_data_source_env(monkeypatch)
    secrets = _use_test_secrets(tmp_path, monkeypatch)
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
        headers={"Origin": "http://testserver"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/data-sources?saved=1&restart=1"
    env_text = env_file.read_text(encoding="utf-8")
    assert "new-token" not in env_text
    raw_profiles = webui_app.json.loads(
        webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"]
    )
    secret_file = secrets / raw_profiles[0]["credential_ref"].removeprefix("secret://")
    assert secret_file.read_text(encoding="utf-8").strip() == "new-token"


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
        headers={"Origin": "http://testserver"},
    )

    assert response.status_code == 200
    assert "数据源测试成功" in response.text
    assert tested[0]["source_type"] == "tushare"
    assert "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON" not in env_file.read_text(encoding="utf-8")


def test_data_source_settings_post_requires_same_origin_or_csrf_token(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    client = TestClient(webui_app.app)

    missing = client.post("/data-sources", data={"source_name_0": "blocked"})
    cross_site = client.post(
        "/data-sources",
        headers={"Origin": "https://attacker.example"},
        data={"source_name_0": "blocked"},
    )

    assert missing.status_code == 403
    assert cross_site.status_code == 403
    assert env_file.read_text(encoding="utf-8") == "WEB_UI_PORT=8765\n"


def test_local_profile_is_saved_with_deterministic_absolute_root(tmp_path: Path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("WEB_UI_PORT=8765\n", encoding="utf-8")
    (tmp_path / "pit").mkdir()
    monkeypatch.setattr(webui_app, "env_file", lambda: env_file)
    _clear_data_source_env(monkeypatch)
    _use_test_secrets(tmp_path, monkeypatch)

    saved = webui_app.save_data_source_settings(
        {
            "source_order_0": "1",
            "source_enabled_0": "on",
            "source_type_0": "local_file",
            "source_name_0": "pit-reference-local",
            "source_root_0": "pit",
            "source_probe_file_0": "historical_st.parquet",
        }
    )

    raw = webui_app.json.loads(
        webui_app.os.environ["FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"]
    )
    assert raw[0]["extra"] == {
        "root": str((tmp_path / "pit").resolve()),
        "probe_file": "historical_st.parquet",
        "base_url": "",
    }
    assert saved["profiles"][0]["root"] == str((tmp_path / "pit").resolve())
    assert saved["profiles"][0]["api_key"] == ""


def test_local_profile_probe_is_limited_to_named_file_under_root(tmp_path: Path) -> None:
    root = tmp_path / "pit"
    root.mkdir()
    probe = root / "historical_st.parquet"
    probe.write_bytes(b"PAR1")
    base = {
        "name": "pit-reference-local",
        "source_type": "local_file",
        "api_key": "",
        "extra": {"root": str(root), "probe_file": probe.name},
    }

    assert webui_app.test_data_source_connection(base)["ok"] is True
    escaped = {
        **base,
        "extra": {"root": str(root), "probe_file": "../outside.parquet"},
    }
    result = webui_app.test_data_source_connection(escaped)
    assert result["ok"] is False
    assert "数据源测试失败" in result["message"]


def test_akshare_profile_probe_uses_bounded_single_security_request(monkeypatch) -> None:
    calls = []

    def bounded(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame({"日期": ["2024-01-02"]})

    monkeypatch.setitem(
        sys.modules, "akshare", SimpleNamespace(stock_zh_a_hist=bounded)
    )
    result = webui_app.test_data_source_connection(
        {"name": "secondary-akshare", "source_type": "akshare", "api_key": ""}
    )

    assert result["ok"] is True
    assert calls == [
        {
            "symbol": "000001",
            "period": "daily",
            "start_date": "20240102",
            "end_date": "20240103",
            "adjust": "",
        }
    ]


def test_tushare_probe_error_never_echoes_secret(monkeypatch) -> None:
    secret = "secret-token-that-must-stay-masked"

    class Client:
        def query(self, *_args, **_kwargs):
            raise RuntimeError(f"provider rejected {secret}")

    monkeypatch.setitem(
        sys.modules, "tushare", SimpleNamespace(pro_api=lambda _token: Client())
    )
    result = webui_app.test_data_source_connection(
        {"name": "primary-tushare", "source_type": "tushare", "api_key": secret}
    )

    assert result["ok"] is False
    assert secret not in result["message"]
    assert "***" in result["message"]


def test_diemeng_probe_resolves_secret_reference_without_exposing_key(
    tmp_path: Path, monkeypatch
) -> None:
    secrets = _use_test_secrets(tmp_path, monkeypatch)
    (secrets / "source-diemeng-primary").parent.mkdir(parents=True)
    (secrets / "source-diemeng-primary").write_text(
        "diemeng-private-key\n", encoding="utf-8"
    )
    captured = {}

    class Adapter:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def probe(self):
            return SimpleNamespace(health=env_settings_service.SourceHealth.HEALTHY)

    monkeypatch.setattr(env_settings_service, "DiemengSourceAdapter", Adapter)
    result = webui_app.test_data_source_connection(
        {
            "name": "primary",
            "source_type": "diemeng",
            "credential_ref": "secret://source-diemeng-primary",
            "api_key": "",
            "extra": {"base_url": "https://mg.diemeng.chat"},
        }
    )

    assert result["ok"] is True
    assert captured["api_key"] == "diemeng-private-key"
    assert captured["base_url"] == "https://mg.diemeng.chat"
    assert "diemeng-private-key" not in str(result)
