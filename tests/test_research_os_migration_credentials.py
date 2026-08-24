from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import runpy
from types import SimpleNamespace

import pytest

from factor_lab.research_os.credentials import CredentialResolutionError
from factor_lab.research_os.migration_credentials import (
    online_migration_connect_args,
)


ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_ENV = ROOT / "src" / "factor_lab" / "research_os" / "migrations" / "env.py"


def test_postgres_online_migration_resolves_either_matching_password_file_alias(
    tmp_path: Path,
) -> None:
    password_file = tmp_path / "postgres-password"
    password_file.write_text("migration-private-password\n", encoding="utf-8")
    url = "postgresql+psycopg://factor_lab@127.0.0.1:15432/factor_lab"

    assert online_migration_connect_args(
        url,
        env={"FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(password_file)},
    ) == {"password": "migration-private-password"}
    assert online_migration_connect_args(
        url,
        env={"RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(password_file)},
    ) == {"password": "migration-private-password"}
    assert online_migration_connect_args(
        url,
        env={
            "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(password_file),
            "RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(password_file),
        },
    ) == {"password": "migration-private-password"}


def test_postgres_online_migration_rejects_alias_conflict_and_plaintext_url(
    tmp_path: Path,
) -> None:
    canonical = tmp_path / "canonical-password"
    compose = tmp_path / "compose-password"
    canonical.write_text("first-private-password\n", encoding="utf-8")
    compose.write_text("second-private-password\n", encoding="utf-8")
    url = "postgresql+psycopg://factor_lab@127.0.0.1:15432/factor_lab"

    with pytest.raises(CredentialResolutionError, match="aliases disagree") as conflict:
        online_migration_connect_args(
            url,
            env={
                "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(canonical),
                "RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(compose),
            },
        )
    assert "first-private-password" not in str(conflict.value)
    assert "second-private-password" not in str(conflict.value)

    with pytest.raises(
        CredentialResolutionError,
        match="must not embed a password",
    ):
        online_migration_connect_args(
            "postgresql+psycopg://factor_lab:inline-secret@127.0.0.1:15432/factor_lab",
            env={"FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(canonical)},
        )


@pytest.mark.parametrize(
    "query_key",
    ("password", "passfile", "sslpassword", "api_token", "client_secret"),
)
def test_postgres_online_migration_rejects_query_credentials_without_echoing_value(
    tmp_path: Path,
    query_key: str,
) -> None:
    password_file = tmp_path / "postgres-password"
    password_file.write_text("migration-private-password\n", encoding="utf-8")
    query_secret = "query-private-value"

    with pytest.raises(
        CredentialResolutionError,
        match="must not carry credentials",
    ) as rejected:
        online_migration_connect_args(
            (
                "postgresql+psycopg://factor_lab@127.0.0.1:15432/factor_lab"
                f"?{query_key}={query_secret}"
            ),
            env={"FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(password_file)},
        )

    assert query_secret not in str(rejected.value)


def test_non_postgres_migration_does_not_touch_postgres_secret_aliases() -> None:
    assert online_migration_connect_args(
        "sqlite:///migration.sqlite",
        env={
            "FACTOR_LAB_POSTGRES_PASSWORD_FILE": "missing-primary",
            "RESEARCH_OS_POSTGRES_PASSWORD_FILE": "missing-alias",
        },
    ) == {}


@pytest.mark.parametrize(
    "ambient",
    [
        {},
        {"PGPASSWORD": "ambient-private-password"},
        {"PGPASSFILE": "ambient-pgpass"},
        {
            "PGPASSWORD": "ambient-private-password",
            "PGPASSFILE": "ambient-pgpass",
        },
    ],
)
def test_postgres_online_migration_never_falls_back_to_ambient_libpq_credentials(
    ambient: dict[str, str],
) -> None:
    with pytest.raises(
        CredentialResolutionError,
        match="requires a password-file credential",
    ) as missing:
        online_migration_connect_args(
            "postgresql+psycopg://factor_lab@127.0.0.1:15432/factor_lab",
            env=ambient,
        )

    assert "ambient-private-password" not in str(missing.value)
    assert "ambient-pgpass" not in str(missing.value)


def test_alembic_online_engine_receives_password_only_as_connect_arg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    alembic = pytest.importorskip("alembic")
    sqlalchemy = pytest.importorskip("sqlalchemy")
    password_file = tmp_path / "postgres-password"
    password_file.write_text("engine-private-password\n", encoding="utf-8")
    database_url = (
        "postgresql+psycopg://factor_lab@127.0.0.1:15432/factor_lab"
    )
    captured: dict[str, object] = {}

    class FakeConfig:
        config_file_name = None
        config_ini_section = "alembic"

        def __init__(self) -> None:
            self.values: dict[str, str] = {}

        def set_main_option(self, key: str, value: str) -> None:
            self.values[key] = value.replace("%%", "%")

        def get_main_option(self, key: str) -> str:
            return self.values[key]

        def get_section(self, _name: str, _default: object) -> dict[str, str]:
            return dict(self.values)

    class FakeEngine:
        def connect(self):
            return nullcontext(object())

    fake_config = FakeConfig()
    fake_context = SimpleNamespace(
        config=fake_config,
        is_offline_mode=lambda: False,
        configure=lambda **kwargs: captured.setdefault("configure", kwargs),
        begin_transaction=lambda: nullcontext(),
        run_migrations=lambda: captured.setdefault("migrations_ran", True),
    )

    def fake_engine_from_config(configuration, **kwargs):
        captured["configuration"] = dict(configuration)
        captured["engine_kwargs"] = dict(kwargs)
        return FakeEngine()

    monkeypatch.setattr(alembic, "context", fake_context)
    monkeypatch.setattr(sqlalchemy, "engine_from_config", fake_engine_from_config)
    monkeypatch.setenv("RESEARCH_OS_DATABASE_URL", database_url)
    monkeypatch.setenv(
        "FACTOR_LAB_POSTGRES_PASSWORD_FILE",
        str(password_file),
    )
    monkeypatch.delenv("RESEARCH_OS_POSTGRES_PASSWORD_FILE", raising=False)

    runpy.run_path(str(ALEMBIC_ENV), run_name="factor_lab_test_alembic_env")

    assert captured["configuration"] == {"sqlalchemy.url": database_url}
    engine_kwargs = captured["engine_kwargs"]
    assert isinstance(engine_kwargs, dict)
    assert engine_kwargs["connect_args"] == {
        "password": "engine-private-password"
    }
    assert "engine-private-password" not in str(captured["configuration"])
    assert engine_kwargs["poolclass"] is sqlalchemy.pool.NullPool
    assert captured["migrations_ran"] is True
