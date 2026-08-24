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


def test_settings_public_dict_redacts_connection_query_and_fragment_values() -> None:
    query_secret = "query-private-value"
    fragment_secret = "fragment-private-value"
    settings = ResearchOSSettings(
        database_url=(
            "postgresql+psycopg://user@db:5432/lab"
            f"?password={query_secret}#{fragment_secret}"
        ),
        iceberg_catalog_uri=(
            "postgresql+psycopg2://user@db:5432/lab"
            f"?sslkey={query_secret}#{fragment_secret}"
        ),
    )

    public = settings.public_dict()

    assert query_secret not in str(public)
    assert fragment_secret not in str(public)
    assert public["database_url"].endswith("?***#***")
    assert public["iceberg_catalog_uri"].endswith("?***#***")


@pytest.mark.parametrize(
    ("database_url", "expected_username", "expected_hostname"),
    (
        (
            "postgresql+psycopg://same:same@[2001:db8::1]:5432/same",
            "same",
            "2001:db8::1",
        ),
        (
            "postgresql+psycopg://user:db@db:5432/db?label=db#db",
            "user",
            "db",
        ),
        (
            "postgresql+psycopg://user:p%40ss@[::1]:5432/lab",
            "user",
            "::1",
        ),
    ),
)
def test_settings_public_dict_masks_only_userinfo_password_with_repeated_or_encoded_values(
    database_url: str,
    expected_username: str,
    expected_hostname: str,
) -> None:
    from urllib.parse import urlparse

    public_url = ResearchOSSettings(database_url=database_url).public_dict()[
        "database_url"
    ]
    parsed = urlparse(public_url)

    assert parsed.username == expected_username
    assert parsed.password == "***"
    assert parsed.hostname == expected_hostname
    assert ":same@" not in public_url
    assert ":db@" not in public_url
    assert "p%40ss" not in public_url


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


@pytest.mark.parametrize(
    "query_key",
    (
        "password",
        "passfile",
        "sslpassword",
        "api_token",
        "client_secret",
        "credential_ref",
        "sslkey",
        "servicefile",
        "pass%77ord",
    ),
)
def test_production_postgres_urls_reject_credential_shaped_query_without_echoing_value(
    query_key: str,
) -> None:
    query_secret = "query-private-value"

    with pytest.raises(
        CredentialResolutionError,
        match="must not carry credentials",
    ) as rejected:
        ResearchOSSettings.from_env(
            {
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_DATABASE_URL": (
                    "postgresql+psycopg://research@postgres:5432/lab"
                    f"?{query_key}={query_secret}"
                ),
            }
        )

    assert query_secret not in str(rejected.value)


def test_production_postgres_url_allows_noncredential_transport_query() -> None:
    settings = ResearchOSSettings.from_env(
        {
            "FACTOR_LAB_ENVIRONMENT": "production",
            "FACTOR_LAB_DATABASE_URL": (
                "postgresql+psycopg://research@postgres:5432/lab"
                "?sslmode=require&connect_timeout=5"
            ),
        }
    )

    assert "sslmode=require" in settings.database_url
    assert "sslmode=require" not in settings.public_dict()["database_url"]


def test_malformed_production_postgres_query_does_not_retain_parser_secret() -> None:
    with pytest.raises(
        CredentialResolutionError,
        match="query is malformed",
    ) as rejected:
        ResearchOSSettings.from_env(
            {
                "FACTOR_LAB_ENVIRONMENT": "production",
                "FACTOR_LAB_DATABASE_URL": (
                    "postgresql+psycopg://research@postgres:5432/lab"
                    "?password=%FFquery-private-value"
                ),
            }
        )

    assert "query-private-value" not in str(rejected.value)
    assert rejected.value.__context__ is None


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
