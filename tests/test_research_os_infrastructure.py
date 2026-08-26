import os
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from factor_lab.research_os.catalog import new_evidence_epoch
from factor_lab.research_os.data_incidents import DataIncident, DataPipelineStage
from factor_lab.research_os.fingerprint import content_fingerprint


ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_CONFIG = ROOT / "infra" / "research_os" / "alembic.ini"


def _run_alembic(database: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = {
        **os.environ,
        "RESEARCH_OS_DATABASE_URL": f"sqlite:///{database.resolve().as_posix()}",
    }
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            str(ALEMBIC_CONFIG),
            *arguments,
        ],
        cwd=ROOT,
        env=environment,
        text=True,
        capture_output=True,
    )


def _sqlite_database_snapshot(database: Path) -> tuple[tuple, tuple]:
    with sqlite3.connect(database) as connection:
        schema = tuple(
            connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
            ).fetchall()
        )
        table_rows = []
        for object_type, name, _table_name, _sql in schema:
            if object_type != "table":
                continue
            quoted_name = name.replace('"', '""')
            rows = tuple(
                connection.execute(
                    f'SELECT * FROM "{quoted_name}" ORDER BY rowid'
                ).fetchall()
            )
            table_rows.append((name, rows))
    return schema, tuple(table_rows)


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


def test_incident_control_migration_backfills_typed_sqlite_rows_idempotently(
    tmp_path,
) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "incident-outbox-upgrade.sqlite"
    upgrade_0012 = _run_alembic(
        database, "upgrade", "0012_partition_repair_gen"
    )
    assert upgrade_0012.returncode == 0, upgrade_0012.stderr

    occurred_at = "2026-08-25T12:34:56.123456+00:00"
    domain_incident = DataIncident(
        stage=DataPipelineStage.DATA_QUALITY,
        partition_key="2026-08-25",
        error_code="DQ_FAILED",
        message="legacy open domain incident",
        occurred_at=datetime.fromisoformat(occurred_at),
        source_ids=("tushare",),
        evidence_hashes=("b" * 64,),
    )
    payload = {
        "domain_incident_id": domain_incident.incident_id,
        "dagster_run_id": "legacy-open-dagster-run",
        "failed_step_key": "data_quality_gate",
    }
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO ros_data_incidents(
                incident_id, incident_hash, partition_run_id, partition_key,
                stage, status, error_code, message, source_ids_json,
                evidence_hashes_json, payload_json, occurred_at,
                resolved_at, resolution_hash
            ) VALUES (?, ?, NULL, ?, ?, 'open', ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                "incident-legacy-open",
                "a" * 64,
                "2026-08-25",
                "data_quality",
                "DQ_FAILED",
                "legacy open domain incident",
                json.dumps(["tushare"]),
                json.dumps(["b" * 64]),
                json.dumps(payload),
                occurred_at,
            ),
        )

    first_upgrade = _run_alembic(database, "upgrade", "head")
    assert first_upgrade.returncode == 0, first_upgrade.stderr
    second_upgrade = _run_alembic(database, "upgrade", "head")
    assert second_upgrade.returncode == 0, second_upgrade.stderr

    with sqlite3.connect(database) as connection:
        actions = connection.execute(
            "SELECT incident_id, action_kind, status, attempts, fencing_token, "
            "result_json, created_at, updated_at "
            "FROM ros_incident_control_actions"
        ).fetchall()
        head = connection.execute(
            "SELECT version_num FROM alembic_version"
        ).fetchone()
    assert len(actions) == 1
    assert actions[0][:5] == (
        "incident-legacy-open",
        "freeze_fleet",
        "pending",
        0,
        0,
    )
    assert json.loads(actions[0][5]) == {}
    assert datetime.fromisoformat(actions[0][6]).replace(tzinfo=None) == datetime(
        2026, 8, 25, 12, 34, 56, 123456
    )
    assert actions[0][6] == actions[0][7]
    assert head == ("0013_incident_control_outbox",)


@pytest.mark.parametrize(
    "invalid_payload",
    (
        {},
        {
            "domain_incident_id": "dinc_" + "0" * 32,
            "dagster_run_id": "forged-domain-run",
            "failed_step_key": "data_quality_gate",
        },
    ),
)
def test_incident_control_upgrade_rejects_untyped_open_before_database_change(
    tmp_path, invalid_payload: dict[str, str]
) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    suffix = "missing" if not invalid_payload else "forged"
    database = tmp_path / f"incident-outbox-invalid-{suffix}.sqlite"
    upgrade_0012 = _run_alembic(
        database, "upgrade", "0012_partition_repair_gen"
    )
    assert upgrade_0012.returncode == 0, upgrade_0012.stderr

    occurred_at = "2026-08-25T12:34:56+00:00"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO ros_data_incidents(
                incident_id, incident_hash, partition_run_id, partition_key,
                stage, status, error_code, message, source_ids_json,
                evidence_hashes_json, payload_json, occurred_at,
                resolved_at, resolution_hash
            ) VALUES (?, ?, NULL, ?, ?, 'open', ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                f"incident-invalid-{suffix}",
                "a" * 64,
                "2026-08-25",
                "data_quality",
                "DQ_FAILED",
                "untyped legacy open incident",
                json.dumps(["tushare"]),
                json.dumps(["b" * 64]),
                json.dumps(invalid_payload),
                occurred_at,
            ),
        )

    before = _sqlite_database_snapshot(database)
    upgrade = _run_alembic(database, "upgrade", "head")
    assert upgrade.returncode != 0
    assert "Cannot migrate OPEN incident" in (
        upgrade.stdout + upgrade.stderr
    )
    assert _sqlite_database_snapshot(database) == before


def test_incident_control_downgrade_refuses_before_any_database_change(
    tmp_path,
) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "incident-outbox-downgrade.sqlite"
    upgrade_0012 = _run_alembic(
        database, "upgrade", "0012_partition_repair_gen"
    )
    assert upgrade_0012.returncode == 0, upgrade_0012.stderr

    occurred_at = "2026-08-25T12:34:56+00:00"
    domain_incident = DataIncident(
        stage=DataPipelineStage.DATA_QUALITY,
        partition_key="2026-08-25",
        error_code="DQ_FAILED",
        message="typed incident control evidence",
        occurred_at=datetime.fromisoformat(occurred_at),
        source_ids=("tushare",),
        evidence_hashes=("b" * 64,),
    )
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO ros_data_incidents(
                incident_id, incident_hash, partition_run_id, partition_key,
                stage, status, error_code, message, source_ids_json,
                evidence_hashes_json, payload_json, occurred_at,
                resolved_at, resolution_hash
            ) VALUES (?, ?, NULL, ?, ?, 'open', ?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (
                "incident-downgrade-open",
                "a" * 64,
                "2026-08-25",
                "data_quality",
                "DQ_FAILED",
                "typed incident control evidence",
                json.dumps(["tushare"]),
                json.dumps(["b" * 64]),
                json.dumps(
                    {
                        "domain_incident_id": domain_incident.incident_id,
                        "dagster_run_id": "downgrade-dagster-run",
                        "failed_step_key": "data_quality_gate",
                    }
                ),
                occurred_at,
            ),
        )
    upgrade = _run_alembic(database, "upgrade", "head")
    assert upgrade.returncode == 0, upgrade.stderr

    before = _sqlite_database_snapshot(database)
    downgrade = _run_alembic(
        database, "downgrade", "0012_partition_repair_gen"
    )
    assert downgrade.returncode != 0
    assert "Refusing downgrade 0013_incident_control_outbox" in (
        downgrade.stdout + downgrade.stderr
    )
    assert _sqlite_database_snapshot(database) == before


def test_incident_control_offline_downgrade_is_refused(tmp_path) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "incident-outbox-offline-downgrade.sqlite"
    downgrade = _run_alembic(
        database,
        "downgrade",
        "0013_incident_control_outbox:0012_partition_repair_gen",
        "--sql",
    )
    assert downgrade.returncode != 0
    assert "Refusing offline downgrade 0013_incident_control_outbox" in (
        downgrade.stdout + downgrade.stderr
    )


def test_incident_control_offline_upgrade_is_refused(tmp_path) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "incident-outbox-offline-upgrade.sqlite"
    upgrade = _run_alembic(
        database,
        "upgrade",
        "0012_partition_repair_gen:0013_incident_control_outbox",
        "--sql",
    )
    assert upgrade.returncode != 0
    assert "Refusing offline upgrade 0013_incident_control_outbox" in (
        upgrade.stdout + upgrade.stderr
    )


@pytest.mark.parametrize("evidence_kind", ["non_base_generation", "authority"])
def test_partition_repair_downgrade_refuses_before_any_database_change(
    tmp_path, evidence_kind: str
) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / f"repair-downgrade-{evidence_kind}.sqlite"
    upgrade = _run_alembic(database, "upgrade", "0012_partition_repair_gen")
    assert upgrade.returncode == 0, upgrade.stderr

    timestamp = "2026-08-25T00:00:00+00:00"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            INSERT INTO ros_partition_runs(
                partition_run_id, source_id, dataset, partition_key, status,
                attempts, details_json, created_at, updated_at, generation
            ) VALUES (?, ?, ?, ?, 'pending', 0, '{}', ?, ?, 'base')
            """,
            (
                "partition-base",
                "tushare",
                "daily",
                "2026-08-25",
                timestamp,
                timestamp,
            ),
        )
        if evidence_kind == "non_base_generation":
            connection.execute(
                """
                INSERT INTO ros_partition_runs(
                    partition_run_id, source_id, dataset, partition_key,
                    status, attempts, details_json, created_at, updated_at,
                    generation, repair_parent_partition_run_id,
                    repair_parent_hash, repair_fingerprint
                ) VALUES (?, ?, ?, ?, 'pending', 0, '{}', ?, ?, ?, ?, ?, ?)
                """,
                (
                    "partition-repair-1",
                    "tushare",
                    "daily",
                    "2026-08-25",
                    timestamp,
                    timestamp,
                    "repair-1",
                    "partition-base",
                    "c" * 64,
                    "d" * 64,
                ),
            )
        else:
            connection.execute(
                """
                INSERT INTO ros_partition_repair_authorities(
                    authority_id, scope_key, incident_id, source_id, dataset,
                    partition_key, generation, parent_partition_run_id,
                    parent_terminal_hash, successor_partition_run_id,
                    repair_fingerprint, created_at
                ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "authority-1",
                    "scope-1",
                    "tushare",
                    "daily",
                    "2026-08-25",
                    "base",
                    "partition-base",
                    "e" * 64,
                    "partition-base",
                    "f" * 64,
                    timestamp,
                ),
            )

    before = _sqlite_database_snapshot(database)
    downgrade = _run_alembic(
        database, "downgrade", "0011_shadow_execution_incidents"
    )
    assert downgrade.returncode != 0
    assert "Refusing downgrade 0012_partition_repair_gen" in (
        downgrade.stdout + downgrade.stderr
    )
    assert _sqlite_database_snapshot(database) == before


def test_empty_partition_repair_migration_roundtrips(tmp_path) -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    database = tmp_path / "repair-empty-roundtrip.sqlite"
    upgrade = _run_alembic(database, "upgrade", "0012_partition_repair_gen")
    assert upgrade.returncode == 0, upgrade.stderr
    downgrade = _run_alembic(
        database, "downgrade", "0011_shadow_execution_incidents"
    )
    assert downgrade.returncode == 0, downgrade.stderr

    with sqlite3.connect(database) as connection:
        columns_after_downgrade = {
            row[1]
            for row in connection.execute("PRAGMA table_info(ros_partition_runs)")
        }
        authority_table_after_downgrade = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'ros_partition_repair_authorities'"
        ).fetchone()
        version_after_downgrade = connection.execute(
            "SELECT version_num FROM alembic_version"
        ).fetchone()
    assert "generation" not in columns_after_downgrade
    assert authority_table_after_downgrade is None
    assert version_after_downgrade == ("0011_shadow_execution_incidents",)

    repeat_upgrade = _run_alembic(
        database, "upgrade", "0012_partition_repair_gen"
    )
    assert repeat_upgrade.returncode == 0, repeat_upgrade.stderr
    with sqlite3.connect(database) as connection:
        columns_after_upgrade = {
            row[1]
            for row in connection.execute("PRAGMA table_info(ros_partition_runs)")
        }
        authority_table_after_upgrade = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'ros_partition_repair_authorities'"
        ).fetchone()
    assert "generation" in columns_after_upgrade
    assert authority_table_after_upgrade == ("ros_partition_repair_authorities",)


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
    assert head == ("0013_incident_control_outbox",)


def test_postgresql_offline_migration_contains_authority_and_shadow_guards() -> None:
    pytest.importorskip("alembic")
    pytest.importorskip("sqlalchemy")
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    revisions = tuple(
        ScriptDirectory.from_config(Config(str(ALEMBIC_CONFIG))).walk_revisions()
    )
    assert revisions
    assert all(len(item.revision) <= 32 for item in revisions)
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
            "0012_partition_repair_gen",
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
