from __future__ import annotations

import json
from pathlib import Path

import pytest

from factor_lab.webui import runtime_guard
from factor_lab.webui.services import env_settings


def test_data_source_profile_cannot_reference_infrastructure_or_other_source_secret(
    tmp_path: Path,
) -> None:
    for reference in (
        "secret://postgres_password",
        "secret://minio_root_password",
        "secret://source-diemeng-primary",
        "env://TUSHARE_TOKEN",
        "secret://../postgres_password",
    ):
        values = {
            "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON": json.dumps(
                [
                    {
                        "name": "primary-tushare",
                        "source_type": "tushare",
                        "credential_ref": reference,
                        "api_key": "",
                        "enabled": True,
                    }
                ]
            )
        }
        with pytest.raises(ValueError):
            env_settings.load_data_source_profiles(values, {})


def test_llm_profile_cannot_reference_database_minio_or_source_secret() -> None:
    for reference in (
        "secret://postgres_password",
        "secret://webui_postgres_password",
        "secret://minio_root_user",
        "secret://tushare_token",
        "secret://source-tushare-primary",
    ):
        values = {
            "FACTOR_LAB_LLM_PROFILES_JSON": json.dumps(
                [
                    {
                        "name": "default",
                        "model": "test-model",
                        "credential_ref": reference,
                    }
                ]
            )
        }
        with pytest.raises(ValueError):
            env_settings.load_llm_profiles(values, {})


def test_allowed_source_and_llm_refs_still_resolve_for_connection_tests(
    tmp_path: Path,
) -> None:
    editor = tmp_path / "settings-secrets"
    editor.mkdir()
    (editor / "source-tushare-primary").write_text(
        "test-source-credential\n", encoding="utf-8"
    )
    (editor / "llm-default").write_text(
        "test-llm-credential\n", encoding="utf-8"
    )
    environment = {"FACTOR_LAB_SECRETS_DIR": str(editor)}

    assert env_settings._profile_credential(
        {
            "source_type": "tushare",
            "credential_ref": "secret://source-tushare-primary",
        },
        environment,
    ) == "test-source-credential"
    assert env_settings.resolve_llm_profile_credential(
        {"credential_ref": "secret://llm-default"}, environment
    ) == "test-llm-credential"


def test_secret_atomic_write_refuses_symlink_target(
    tmp_path: Path,
) -> None:
    editor = tmp_path / "settings-secrets"
    editor.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("must-not-change\n", encoding="utf-8")
    target = editor / "tushare_token"
    try:
        target.symlink_to(outside)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")

    with pytest.raises(ValueError, match="符号链接|重解析点"):
        env_settings._write_profile_secret(
            source_type="tushare",
            name="primary",
            value="replacement-secret",
            environ={"FACTOR_LAB_SECRETS_DIR": str(editor)},
            canonical=True,
        )
    assert outside.read_text(encoding="utf-8") == "must-not-change\n"


def test_env_atomic_write_refuses_symlink_settings_file(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside.env"
    outside.write_text("SAFE=1\n", encoding="utf-8")
    settings = tmp_path / "webui.env"
    try:
        settings.symlink_to(outside)
    except OSError:
        pytest.skip("symlink creation is unavailable on this Windows host")

    with pytest.raises(ValueError, match="符号链接|重解析点"):
        env_settings.save_llm_settings(
            {"model": "safe-model"},
            env_file_func=lambda: settings,
            environ={"FACTOR_LAB_SECRETS_DIR": str(tmp_path / "editor")},
        )
    assert outside.read_text(encoding="utf-8") == "SAFE=1\n"


def test_runtime_guard_rejects_full_secret_directory_even_with_valid_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    editor = tmp_path / "editor"
    settings = tmp_path / "settings"
    db_secret = tmp_path / "db-password"
    editor.mkdir()
    settings.mkdir()
    db_secret.write_text("webui-only-password\n", encoding="utf-8")
    (editor / "postgres_password").write_text(
        "forbidden-owner-password\n", encoding="utf-8"
    )
    monkeypatch.setattr(runtime_guard, "WEBUI_SECRET_EDITOR", editor)
    monkeypatch.setattr(runtime_guard, "WEBUI_SETTINGS_DIRECTORY", settings)
    monkeypatch.setattr(runtime_guard, "WEBUI_DATABASE_SECRET", db_secret)

    with pytest.raises(
        runtime_guard.WebUIRuntimeIsolationError, match="forbidden entry"
    ):
        runtime_guard.validate_webui_runtime(
            {
                "FACTOR_LAB_PRODUCTION_ROLE": "webui",
                "FACTOR_LAB_SECRETS_DIR": str(editor),
                "FACTOR_LAB_ENV_FILE": str(settings / "webui.env"),
                "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(db_secret),
                "FACTOR_LAB_DATABASE_URL": (
                    "postgresql+psycopg://factor_lab_webui@postgres:5432/factor_lab"
                ),
                "RESEARCH_OS_POSTGRES_USER": "factor_lab_webui",
                "RESEARCH_OS_WEBUI_POSTGRES_USER": "factor_lab_webui",
            },
            require_mounts=False,
        )


def test_runtime_guard_accepts_only_minimal_webui_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    editor = tmp_path / "editor"
    settings = tmp_path / "settings"
    db_secret = tmp_path / "db-password"
    editor.mkdir()
    settings.mkdir()
    db_secret.write_text("webui-only-password\n", encoding="utf-8")
    (editor / "tushare_token").write_text(
        "source-only-secret\n", encoding="utf-8"
    )
    monkeypatch.setattr(runtime_guard, "WEBUI_SECRET_EDITOR", editor)
    monkeypatch.setattr(runtime_guard, "WEBUI_SETTINGS_DIRECTORY", settings)
    monkeypatch.setattr(runtime_guard, "WEBUI_DATABASE_SECRET", db_secret)
    environment = {
        "FACTOR_LAB_PRODUCTION_ROLE": "webui",
        "FACTOR_LAB_SECRETS_DIR": str(editor),
        "FACTOR_LAB_ENV_FILE": str(settings / "webui.env"),
        "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(db_secret),
        "FACTOR_LAB_DATABASE_URL": (
            "postgresql+psycopg://factor_lab_webui@postgres:5432/factor_lab"
        ),
        "RESEARCH_OS_POSTGRES_USER": "factor_lab_webui",
        "RESEARCH_OS_WEBUI_POSTGRES_USER": "factor_lab_webui",
    }

    runtime_guard.validate_webui_runtime(environment, require_mounts=False)
    with pytest.raises(
        runtime_guard.WebUIRuntimeIsolationError, match="forbidden worker"
    ):
        runtime_guard.validate_webui_runtime(
            {**environment, "TUSHARE_TOKEN_FILE": "/run/secrets/tushare_token"},
            require_mounts=False,
        )
