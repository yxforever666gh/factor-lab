from __future__ import annotations

from pathlib import Path

import pytest

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.credentials import CredentialResolutionError
from factor_lab.research_os.runtime import ResearchOSSettings, RunCoordinator, doctor


def test_settings_never_expose_secrets() -> None:
    settings = ResearchOSSettings.from_env(
        {
            "FACTOR_LAB_DATABASE_URL": "postgresql+psycopg://user:secret@db:5432/lab",
            "FACTOR_LAB_ICEBERG_CATALOG_URI": "postgresql+psycopg://ice:berg@db:5432/ice",
            "FACTOR_LAB_OBJECT_STORE_SECRET_KEY": "minio-secret",
        }
    )
    public = settings.public_dict()
    assert "secret" not in public["database_url"]
    assert "berg" not in public["iceberg_catalog_uri"]
    assert public["object_store_secret_key"] == "***"


def test_doctor_can_run_without_touching_network(tmp_path: Path) -> None:
    report = doctor(
        ResearchOSSettings(
            database_url=f"sqlite:///{tmp_path / 'catalog.db'}",
            lake_root=tmp_path / "lake",
            snapshot_root=tmp_path / "snapshots",
        ),
        check_network=False,
    )
    assert report.status in {"ready", "degraded", "blocked"}
    assert report.settings["object_store_secret_key"] == "***"


def test_production_settings_require_secret_files_and_passwordless_urls(
    tmp_path: Path,
) -> None:
    access = tmp_path / "access"
    secret = tmp_path / "secret"
    postgres = tmp_path / "postgres"
    access.write_text("minio-access\n", encoding="utf-8")
    secret.write_text("minio-secret\n", encoding="utf-8")
    postgres.write_text("database-secret\n", encoding="utf-8")
    settings = ResearchOSSettings.from_env(
        {
            "FACTOR_LAB_ENVIRONMENT": "production",
            "FACTOR_LAB_DATABASE_URL": "postgresql+psycopg://research@postgres:5432/lab",
            "FACTOR_LAB_ICEBERG_CATALOG_URI": "postgresql+psycopg2://research@postgres:5432/lab",
            "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(postgres),
            "FACTOR_LAB_OBJECT_STORE_ACCESS_KEY_FILE": str(access),
            "FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE": str(secret),
        }
    )
    assert settings.object_store_access_key == "minio-access"
    assert settings.object_store_secret_key == "minio-secret"
    assert settings.database_password_file == postgres
    assert "database-secret" not in repr(settings)
    assert settings.database_connect_args() == {"password": "database-secret"}
    assert settings.public_dict()["database_credential_configured"] is True
    assert settings.public_dict()["object_store_access_key"] == "***"

    with pytest.raises(CredentialResolutionError, match="forbidden in production"):
        ResearchOSSettings.from_env(
            {
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_OBJECT_STORE_SECRET_KEY": "raw-secret",
            }
        )
    with pytest.raises(CredentialResolutionError, match="forbidden in production"):
        ResearchOSSettings.from_env(
            {
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_POSTGRES_PASSWORD": "raw-secret",
            }
        )
    with pytest.raises(CredentialResolutionError, match="must not embed a password"):
        ResearchOSSettings.from_env(
            {
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_DATABASE_URL": "postgresql+psycopg://user:secret@db/lab",
            }
        )


def test_postgres_password_file_aliases_must_agree(tmp_path: Path) -> None:
    canonical = tmp_path / "canonical"
    compose = tmp_path / "compose"
    canonical.write_text("first-secret\n", encoding="utf-8")
    compose.write_text("second-secret\n", encoding="utf-8")

    with pytest.raises(CredentialResolutionError, match="aliases disagree"):
        ResearchOSSettings.from_env(
            {
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(canonical),
                "RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(compose),
            }
        )

    settings = ResearchOSSettings.from_env(
        {
            "FACTOR_LAB_ENVIRONMENT": "production",
            "RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(compose),
        }
    )
    assert settings.database_connect_args() == {"password": "second-secret"}


def test_run_coordinator_records_success_and_failure(tmp_path: Path) -> None:
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        coordinator = RunCoordinator(catalog)
        assert coordinator.execute("doctor", {"as_of": "2026-08-22"}, lambda: {"ok": True}) == {"ok": True}

        with pytest.raises(RuntimeError, match="boom"):
            coordinator.execute("sync", {"partition": "2026-08-22"}, lambda: (_ for _ in ()).throw(RuntimeError("boom")))
