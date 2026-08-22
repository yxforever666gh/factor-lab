import os
from datetime import datetime
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from factor_lab.research_os.catalog import new_evidence_epoch
from factor_lab.research_os.fingerprint import content_fingerprint


ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_CONFIG = ROOT / "infra" / "research_os" / "alembic.ini"


def test_core_catalog_imports_and_operates_without_sqlalchemy() -> None:
    script = r"""
import builtins
import pydantic

original_import = builtins.__import__
def blocked_import(name, *args, **kwargs):
    if name == "sqlalchemy" or name.startswith("sqlalchemy."):
        raise ImportError("blocked optional dependency")
    return original_import(name, *args, **kwargs)
builtins.__import__ = blocked_import

from factor_lab.research_os import ResearchCatalog
from factor_lab.research_os.orm import SQLALCHEMY_AVAILABLE
assert SQLALCHEMY_AVAILABLE is False
with ResearchCatalog(":memory:") as catalog:
    catalog.initialize_schema()
    assert catalog.catalog_summary().totals["experiments"] == 0
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                filter(
                    None,
                    [
                        str(ROOT / "src"),
                        os.environ.get("PYTHONPATH", ""),
                    ],
                )
            ),
        },
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_alembic_migration_upgrades_and_matches_orm_metadata(tmp_path) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "migration.sqlite"
    environment = os.environ.copy()
    environment["RESEARCH_OS_DATABASE_URL"] = (
        f"sqlite:///{database.resolve().as_posix()}"
    )
    upgrade = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(ALEMBIC_CONFIG),
            "upgrade",
            "head",
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
    )
    assert upgrade.returncode == 0, upgrade.stderr
    check = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(ALEMBIC_CONFIG),
            "check",
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
    )
    assert check.returncode == 0, check.stderr
    assert "No new upgrade operations detected" in (check.stdout + check.stderr)
    with sqlite3.connect(database) as connection:
        authority = connection.execute(
            "SELECT marker_key, environment, authority_schema, marker_hash "
            "FROM ros_runtime_authority"
        ).fetchall()
    assert len(authority) == 1
    assert authority[0][:3] == (
        "research_os",
        "test",
        "research-os/runtime-authority/v1",
    )
    assert len(authority[0][3]) == 64


def test_evidence_epoch_migration_preserves_active_singleton_as_pointer(
    tmp_path,
) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "epoch-upgrade.sqlite"
    environment = {
        **os.environ,
        "RESEARCH_OS_DATABASE_URL": f"sqlite:///{database.resolve().as_posix()}",
    }

    def upgrade(target: str) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "alembic",
                "-c",
                str(ALEMBIC_CONFIG),
                "upgrade",
                target,
            ],
            cwd=ROOT,
            env=environment,
            text=True,
            capture_output=True,
        )
        assert completed.returncode == 0, completed.stderr

    upgrade("0009_shadow_fleet_closure")
    frozen_at = "2026-08-22T08:00:00+00:00"
    activated_at = "2026-08-22T08:01:00+00:00"
    epoch = new_evidence_epoch(
        architecture_version="migration-v1",
        frozen_at=datetime.fromisoformat(frozen_at),
        code_hash="1" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
    )
    window = {
        "epoch_id": epoch.epoch_id,
        "epoch_hash": epoch.epoch_hash,
        "first_forward_session": "2026-08-24",
        "calendar_snapshot_id": "calendar-gold",
        "calendar_snapshot_hash": "5" * 64,
        "calendar_content_hash": "6" * 64,
        "activated_at": activated_at,
    }
    window_hash = content_fingerprint(
        window, domain="factor-lab/research-os/v1/evidence-window"
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO ros_data_snapshots(
                snapshot_id, schema_version, tier, uri, content_hash, as_of,
                quality_status, ref_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "calendar-gold",
                "research-os/data-snapshot/v1",
                "gold",
                "iceberg://calendar",
                "5" * 64,
                frozen_at,
                "accepted",
                "{}",
                frozen_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO ros_evidence_epochs(
                epoch_slot, epoch_id, schema_version, architecture_version,
                frozen_at, code_hash, configuration_hash,
                dependency_lock_hash, dirty_patch_hash, epoch_hash,
                first_forward_session, calendar_snapshot_id,
                calendar_snapshot_hash, calendar_content_hash,
                evidence_window_hash, activated_at
            ) VALUES ('research_os', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                epoch.epoch_id,
                epoch.schema_version,
                epoch.architecture_version,
                frozen_at,
                epoch.code_hash,
                epoch.configuration_hash,
                epoch.dependency_lock_hash,
                epoch.dirty_patch_hash,
                epoch.epoch_hash,
                "2026-08-24",
                "calendar-gold",
                "5" * 64,
                "6" * 64,
                window_hash,
                activated_at,
            ),
        )
    upgrade("head")
    with sqlite3.connect(database) as connection:
        pointer = connection.execute(
            "SELECT pointer_key, epoch_id FROM ros_evidence_epoch_active_pointer"
        ).fetchone()
        columns = {
            row[1]
            for row in connection.execute("PRAGMA table_info(ros_evidence_epochs)")
        }
        head = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert pointer == ("research_os", epoch.epoch_id)
    assert {"closed_at", "superseded_by_epoch_id"}.issubset(columns)
    assert head == ("0010_evidence_epoch_versions",)


def test_postgresql_offline_migration_contains_authority_and_shadow_guards() -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    environment = os.environ.copy()
    environment.pop("RESEARCH_OS_DATABASE_URL", None)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(ALEMBIC_CONFIG),
            "upgrade",
            "head",
            "--sql",
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
    )
    assert completed.returncode == 0, completed.stderr
    sql = completed.stdout
    assert "uq_ros_results_one_authoritative" in sql
    assert "WHERE authoritative" in sql
    assert "uq_ros_shadow_account_sequence" in sql
    assert "ck_ros_shadow_position_long_only" in sql
    assert "uq_ros_trials_admitted_fingerprint" in sql
    assert "ros_runtime_authority" in sql
    assert "'production'" in sql
    assert "ros_shadow_fleet_closures" in sql
    assert "ck_ros_shadow_fleet_epoch_binding" in sql


def test_postgresql_catalog_startup_validates_migrations_without_create_all(
    monkeypatch,
) -> None:
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from factor_lab.research_os import catalog as catalog_module
    from factor_lab.research_os import orm

    class FakeDialect:
        name = "postgresql"

    class FakeEngine:
        dialect = FakeDialect()

    class EmptyInspector:
        @staticmethod
        def get_table_names():
            return []

    backend = catalog_module._SQLAlchemyCatalog.__new__(
        catalog_module._SQLAlchemyCatalog
    )
    backend._engine = FakeEngine()
    create_all_called = False

    def forbidden_create_all(*_args, **_kwargs):
        nonlocal create_all_called
        create_all_called = True

    monkeypatch.setattr(sqlalchemy, "inspect", lambda _engine: EmptyInspector())
    monkeypatch.setattr(orm.Base.metadata, "create_all", forbidden_create_all)
    with pytest.raises(catalog_module.CatalogError, match="not migrated"):
        backend.initialize_schema()
    assert create_all_called is False
