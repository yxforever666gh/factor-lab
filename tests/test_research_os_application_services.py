from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from factor_lab.research_os import application_services as application_services_module
from factor_lab.research_os import orm
from factor_lab.research_os.application_services import (
    APPLICATION_SERVICES_SCHEMA_VERSION,
    ORCHESTRATION_CONFIG_ENV,
    ApplicationServices,
    create_services,
)
from factor_lab.research_os.catalog import LifecycleEvent, ResearchCatalog
from factor_lab.research_os.champion_control import AuthoritativeChampionControl
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EnvironmentRef,
    EvaluationInputBindings,
    ExperimentSpec,
    FactorSpec,
    LabelSpec,
    LifecycleState,
    PortfolioPolicy,
    Preregistration,
    RecoveryCase,
    RecoveryCaseStatus,
    SnapshotTier,
    UniverseSpec,
    ValidationProtocol,
)
from factor_lab.research_os.governance import HISTORICAL_HOLDOUT_ID
from factor_lab.research_os.legacy_bronze_seed import (
    LEGACY_SEED_TRUST_LABELS,
    SnapshotPromotionBlocked,
    assert_snapshot_promotion_allowed,
)
from factor_lab.research_os.iceberg_service import (
    IcebergCommit,
    IcebergPublicationError,
    PyIcebergGoldPublisher,
)
from factor_lab.research_os.orchestration import (
    CycleName,
    OperationName,
    OperationRequest,
    OperationResult,
    OrchestrationFailure,
    ServiceNotConfigured,
    execute_operation,
)
from factor_lab.research_os.object_store import ArchivedObject, S3ImmutableArchive
from factor_lab.research_os.production_ledger import (
    CapabilityStatus,
    IncidentStatus,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
    ProductionLedgerError,
)
from factor_lab.research_os.runtime import ResearchOSSettings
from factor_lab.research_os.snapshots import build_immutable_snapshot_manifest


HASHES = {
    "code_hash": "1" * 64,
    "dependency_lock_hash": "2" * 64,
    "config_hash": "3" * 64,
    "dirty_patch_hash": "4" * 64,
}


@dataclass
class FakeGoldPublisher:
    calls: list[dict]

    def publish(
        self,
        frame: pd.DataFrame,
        *,
        table_identifier: str,
        tag: str,
        snapshot_key: str,
        partition_key: str,
    ) -> IcebergCommit:
        self.calls.append(
            {
                "frame": frame.copy(),
                "table_identifier": table_identifier,
                "tag": tag,
                "snapshot_key": snapshot_key,
                "partition_key": partition_key,
            }
        )
        return IcebergCommit(
            table_identifier=table_identifier,
            snapshot_id=901,
            tag=tag,
            row_count=len(frame),
        )


@dataclass
class FakeObjectStoreArchive:
    calls: list[tuple[str, str]]

    def archive_file(self, path, *, logical_path: str) -> ArchivedObject:
        source = Path(path)
        self.calls.append((str(source), logical_path))
        return ArchivedObject(
            uri=f"s3://factor-lab/research-os/{logical_path}/{source.name}",
            key=f"research-os/{logical_path}/{source.name}",
            sha256="a" * 64,
            size_bytes=source.stat().st_size,
            reused=False,
        )


class _MemoryObjectStoreFileSystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def exists(self, path: str) -> bool:
        return path in self.objects

    def open(self, path: str, mode: str = "rb"):
        if mode == "rb":
            return io.BytesIO(self.objects[path])
        if mode != "wb":
            raise ValueError(mode)
        filesystem = self

        class _Writer(io.BytesIO):
            def close(self) -> None:
                filesystem.objects[path] = self.getvalue()
                super().close()

        return _Writer()


def _settings(root: Path) -> ResearchOSSettings:
    return ResearchOSSettings(
        database_url=f"sqlite:///{root / 'catalog.db'}",
        lake_root=root / "lake",
        snapshot_root=root / "snapshots",
        legacy_sqlite_path=root / "legacy.db",
        environment="test",
    )


def _write_inputs(root: Path, *, empty_st: bool = False) -> tuple[Path, Path]:
    source_root = root / "source"
    source_root.mkdir()
    market = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000002.SZ"],
            "date": ["2024-01-02", "2024-01-02"],
            "available_at": [
                "2024-01-02T07:30:00+00:00",
                "2024-01-02T07:30:00+00:00",
            ],
            "open_adj": [10.0, 20.0],
            "close_adj": [10.5, 19.5],
            "adv_20": [100_000_000.0, 90_000_000.0],
            "volatility_20": [0.02, 0.03],
        }
    )
    market.to_parquet(source_root / "daily.parquet", index=False)
    st_path = root / "historical_st.csv"
    pd.DataFrame(
        columns=["ts_code", "start_date", "end_date"]
        if empty_st
        else None,
        data=(
            []
            if empty_st
            else [{"ts_code": "000002.SZ", "start_date": "2019-01-01", "end_date": "2019-02-01"}]
        ),
    ).to_csv(st_path, index=False)
    return source_root, st_path


def _config(root: Path, *, empty_st: bool = False, champion: Path | None = None) -> dict:
    source_root, st_path = _write_inputs(root, empty_st=empty_st)
    return {
        "schema_version": APPLICATION_SERVICES_SCHEMA_VERSION,
        "repository": str(root),
        "path_base": str(root),
        "iceberg": {"catalog_name": "factorlab"},
        "daily": {
            "sources": [
                {
                    "source": "local_file",
                    "root": str(source_root),
                    "priority": 10,
                    "path_templates": {"daily": "daily.parquet"},
                    "request": {"dataset": "daily"},
                    "contract": {
                        "dataset": "daily",
                        "key_fields": ["ticker", "date"],
                        "event_time_field": "date",
                        "release_timing": "session close plus 30 minutes",
                        "fields": [
                            {"name": "ticker", "dtype": "string", "nullable": False},
                            {"name": "date", "dtype": "date", "nullable": False},
                            {"name": "available_at", "dtype": "datetime", "nullable": False},
                            {"name": "open_adj", "dtype": "float64", "nullable": False},
                            {"name": "close_adj", "dtype": "float64", "nullable": False},
                            {"name": "adv_20", "dtype": "float64", "nullable": False},
                            {"name": "volatility_20", "dtype": "float64", "nullable": False},
                        ],
                    },
                    "canonicalization": {
                        "entity_columns": ["ticker"],
                        "event_time_column": "date",
                        "available_at_column": "available_at",
                        "value_columns": [
                            "open_adj",
                            "close_adj",
                            "adv_20",
                            "volatility_20",
                        ],
                    },
                }
            ],
            "data_quality": {
                "historical_st": {
                    "path": str(st_path),
                    "available": True,
                    "degraded": False,
                },
                "minimum_rows": 2,
                "required_gold_columns": [
                    "ticker",
                    "open_adj",
                    "close_adj",
                    "adv_20",
                    "volatility_20",
                ],
                "core_coverage_columns": ["open_adj", "close_adj"],
                "minimum_core_coverage": 0.95,
            },
            "gold": {
                "table_identifier": "factor_lab.gold_daily",
                "tag_prefix": "ros_",
            },
            "shadow": {
                "input_mode": "test",
                "champion_input_path": str(champion or root / "absent_champion.json")
            },
        },
    }


def _services(
    root: Path,
    config: dict,
    publisher: FakeGoldPublisher,
    *,
    catalog: ResearchCatalog | None = None,
    object_store_archive: FakeObjectStoreArchive | None = None,
) -> ApplicationServices:
    settings = _settings(root)
    return ApplicationServices(
        config,
        settings=settings,
        catalog=catalog or ResearchCatalog(settings.database_url),
        iceberg_publisher=publisher,
        object_store_archive=object_store_archive,
        env={},
        config_base=root,
        environment_hashes_override=HASHES,
        now=lambda: datetime(2026, 8, 22, tzinfo=timezone.utc),
    )


def _sqlite_fk_authority(root: Path):
    """Use one SQLite authority with PostgreSQL-like foreign-key enforcement."""

    database = root / "catalog.db"
    catalog = ResearchCatalog(database)
    catalog.initialize_schema()
    engine = create_engine(f"sqlite+pysqlite:///{database.as_posix()}")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _connection_record) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    orm.Base.metadata.create_all(engine)
    with engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
    return catalog, engine, ProductionLedger(engine)


def test_production_source_loader_injects_top_level_canonical_origin() -> None:
    service = object.__new__(ApplicationServices)
    service._production_authority = True
    service.config = {
        "security": {
            "source_transport": {
                "tushare": {
                    "api_origin": "HTTPS://API.TUSHARE.PRO:443/dataapi/"
                }
            }
        }
    }
    raw = {"source": "tushare", "request": {"dataset": "daily"}}

    bound = service._bind_source_transport_authority(raw)

    assert "api_origin" not in raw
    assert bound["api_origin"] == "https://api.tushare.pro/dataapi"


def test_daily_bronze_and_silver_are_archived_outside_local_cache(tmp_path: Path) -> None:
    config = _config(tmp_path)
    archive = FakeObjectStoreArchive([])
    service = _services(
        tmp_path, config, FakeGoldPublisher([]), object_store_archive=archive
    )

    source = execute_operation(service, _request(OperationName.SOURCE_SYNC))
    silver = execute_operation(service, _request(OperationName.SOURCE_RECONCILIATION))

    source_row = source.outputs["sources"][0]
    assert source_row["data_object_uri"].startswith("s3://factor-lab/")
    assert source_row["metadata_object_uri"].startswith("s3://factor-lab/")
    assert source_row["bronze_manifest_object"]["uri"].startswith("s3://factor-lab/")
    assert silver.outputs["silver_object"]["uri"].startswith("s3://factor-lab/")
    assert silver.outputs["silver_manifest_object"]["uri"].startswith("s3://factor-lab/")
    assert any(logical.startswith("bronze/") for _, logical in archive.calls)
    assert any(logical.startswith("silver/") for _, logical in archive.calls)


def _configure_non_blocking_sample(
    config: dict,
    root: Path,
    *,
    sample_available: bool,
) -> None:
    config["daily"]["sources"][0]["partition_cadence"] = {
        "kind": "trading_session",
        "ledger_identity": "required_daily",
    }
    config["daily"]["sources"].append(
        {
            "source": "local_file",
            "profile_name": "optional_akshare",
            "root": str(root / "source"),
            "priority": 20,
            "path_templates": {
                "daily_sample_crosscheck": "missing-optional.parquet"
            },
            "partition_cadence": {
                "kind": "trading_session",
                "ledger_identity": "optional_akshare",
            },
            "request": {"dataset": "daily_sample_crosscheck"},
            "contract": {
                "dataset": "daily_sample_crosscheck",
                "key_fields": ["ticker", "date"],
                "event_time_field": "date",
                "release_timing": "session close plus one day",
                "fields": [
                    {"name": "ticker", "dtype": "string", "nullable": False},
                    {"name": "date", "dtype": "date", "nullable": False},
                ],
            },
            "canonicalization": {
                "entity_columns": ["ticker"],
                "event_time_column": "date",
                "value_columns": ["ticker"],
                "availability": {
                    "mode": "session_release_time",
                    "time": "15:30:00",
                    "timezone": "Asia/Shanghai",
                    "lag_days": 1,
                },
            },
            "evidence_role": "non_blocking_sample",
            "non_blocking": True,
        }
    )
    if sample_available:
        pd.DataFrame(
            [{"ticker": "000001.SZ", "date": "2024-01-03"}]
        ).to_parquet(root / "source" / "missing-optional.parquet", index=False)


@pytest.mark.parametrize("sample_available", [False, True])
def test_explicit_non_blocking_source_is_audited_but_never_promoted(
    tmp_path: Path,
    sample_available: bool,
) -> None:
    config = _config(tmp_path)
    _configure_non_blocking_sample(
        config,
        tmp_path,
        sample_available=sample_available,
    )
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    try:
        service = _services(
            tmp_path,
            config,
            FakeGoldPublisher([]),
            catalog=catalog,
        )
        service.production_ledger = ledger

        source = service.execute(_request(OperationName.SOURCE_SYNC))
        silver = service.execute(_request(OperationName.SOURCE_RECONCILIATION))

        assert source.status == "completed"
        assert len(source.outputs["sources"]) == 1
        assert len(source.outputs["bronze_snapshot_ids"]) == 1
        assert len(source.outputs["non_blocking_samples"]) == int(sample_available)
        assert len(source.outputs["degraded_sources"]) == int(not sample_available)
        if sample_available:
            sample = source.outputs["non_blocking_samples"][0]
            assert sample["reconciliation_eligible"] is False
            assert sample["capability_status"] == "accepted"
        else:
            degradation = source.outputs["degraded_sources"][0]
            assert degradation["capability_status"] == "degraded"
            assert degradation["incident_status"] == "resolved"
            assert degradation["accepted_bronze_published"] is False
            assert degradation["reconciliation_eligible"] is False
            assert "missing-optional.parquet" not in json.dumps(degradation)

        required = ledger.get_partition(
            PartitionIdentity("required_daily", "daily", "2024-01-03")
        )
        optional = ledger.get_partition(
            PartitionIdentity(
                "optional_akshare", "daily_sample_crosscheck", "2024-01-03"
            )
        )
        stage = ledger.get_partition(
            PartitionIdentity("research_os", "stage_source", "2024-01-03")
        )
        assert required is not None and required.status is PartitionStatus.SUCCEEDED
        assert optional is not None
        assert optional.status is (
            PartitionStatus.SUCCEEDED if sample_available else PartitionStatus.FAILED
        )
        assert (optional.output_snapshot_id is not None) is sample_available
        assert stage is not None and stage.status is PartitionStatus.SUCCEEDED

        with Session(engine) as session:
            capability = session.get(
                orm.SourceCapabilityModel,
                ("optional_akshare", "daily_sample_crosscheck"),
            )
        assert capability is not None
        assert capability.status == (
            CapabilityStatus.ACCEPTED.value
            if sample_available
            else CapabilityStatus.DEGRADED.value
        )
        assert "missing-optional.parquet" not in capability.detail
        incidents = ledger.list_incidents(limit=10)
        assert len(incidents) == int(not sample_available)
        if not sample_available:
            assert incidents[0].status is IncidentStatus.RESOLVED
            assert incidents[0].error_code == "non_blocking_source_degraded"

        assert silver.status == "completed"
        silver_frame = pd.read_parquet(silver.outputs["silver_path"])
        assert set(silver_frame["dataset"].astype(str)) == {"daily"}
        bronze = catalog.list_snapshots(tier="bronze", limit=10)
        assert len(bronze) == (2 if sample_available else 1)
        silver_record = catalog.get_snapshot(silver.outputs["silver_snapshot_id"])
        assert silver_record is not None
        assert silver_record.reference.parent_snapshot_ids == (
            source.outputs["bronze_snapshot_ids"][0],
        )
        if sample_available:
            sample_snapshot_id = source.outputs["non_blocking_samples"][0][
                "bronze_snapshot_id"
            ]
            sample_record = catalog.get_snapshot(sample_snapshot_id)
            assert sample_record is not None
            assert {
                "gold_promotion_forbidden",
                "non_blocking_sample",
            }.issubset(sample_record.reference.trust_labels)
            with pytest.raises(SnapshotPromotionBlocked):
                assert_snapshot_promotion_allowed(catalog, (sample_snapshot_id,))
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_non_blocking_archive_failure_still_fails_the_source_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _configure_non_blocking_sample(config, tmp_path, sample_available=True)
    archive = FakeObjectStoreArchive([])
    archive_file = archive.archive_file

    def fail_optional_archive(path, *, logical_path: str):
        if "daily_sample_crosscheck" in logical_path:
            raise RuntimeError("injected object-store archive failure")
        return archive_file(path, logical_path=logical_path)

    monkeypatch.setattr(archive, "archive_file", fail_optional_archive)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    try:
        service = _services(
            tmp_path,
            config,
            FakeGoldPublisher([]),
            catalog=catalog,
            object_store_archive=archive,
        )
        service.production_ledger = ledger

        result = service.execute(_request(OperationName.SOURCE_SYNC))

        assert result.status == "failed"
        assert result.outputs["error_type"] == "RuntimeError"
        required = ledger.get_partition(
            PartitionIdentity("required_daily", "daily", "2024-01-03")
        )
        optional = ledger.get_partition(
            PartitionIdentity(
                "optional_akshare", "daily_sample_crosscheck", "2024-01-03"
            )
        )
        stage = ledger.get_partition(
            PartitionIdentity("research_os", "stage_source", "2024-01-03")
        )
        assert required is not None and required.status is PartitionStatus.SUCCEEDED
        assert optional is not None and optional.status is PartitionStatus.FAILED
        assert stage is not None and stage.status is PartitionStatus.FAILED
        assert ledger.list_incidents(limit=10) == ()
        with Session(engine) as session:
            capabilities = session.query(orm.SourceCapabilityModel).all()
        assert capabilities == []
        assert len(catalog.list_snapshots(tier="bronze", limit=10)) == 1
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


@pytest.mark.parametrize("failure_type", [ValueError, RuntimeError])
def test_non_blocking_programming_failure_is_not_provider_degradation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
) -> None:
    config = _config(tmp_path)
    _configure_non_blocking_sample(config, tmp_path, sample_available=True)
    original_sync = application_services_module.sync_bronze

    def fail_optional(source, *args, **kwargs):
        if source.get("non_blocking") is True:
            raise failure_type("injected optional-source programming failure")
        return original_sync(source, *args, **kwargs)

    monkeypatch.setattr(application_services_module, "sync_bronze", fail_optional)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    try:
        service = _services(
            tmp_path,
            config,
            FakeGoldPublisher([]),
            catalog=catalog,
        )
        service.production_ledger = ledger

        result = service.execute(_request(OperationName.SOURCE_SYNC))

        assert result.status == "failed"
        assert result.outputs["error_type"] == failure_type.__name__
        optional = ledger.get_partition(
            PartitionIdentity(
                "optional_akshare", "daily_sample_crosscheck", "2024-01-03"
            )
        )
        stage = ledger.get_partition(
            PartitionIdentity("research_os", "stage_source", "2024-01-03")
        )
        assert optional is not None and optional.status is PartitionStatus.FAILED
        assert stage is not None and stage.status is PartitionStatus.FAILED
        assert ledger.list_incidents(limit=10) == ()
        with Session(engine) as session:
            capabilities = session.query(orm.SourceCapabilityModel).all()
        assert capabilities == []
        assert len(catalog.list_snapshots(tier="bronze", limit=10)) == 1
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_non_blocking_capability_persistence_failure_is_not_provider_degradation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    _configure_non_blocking_sample(config, tmp_path, sample_available=True)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)

    def fail_capability(_record):
        raise ProductionLedgerError("injected capability persistence failure")

    monkeypatch.setattr(ledger, "upsert_capability", fail_capability)
    try:
        service = _services(
            tmp_path,
            config,
            FakeGoldPublisher([]),
            catalog=catalog,
        )
        service.production_ledger = ledger

        result = service.execute(_request(OperationName.SOURCE_SYNC))

        assert result.status == "failed"
        assert result.outputs["error_type"] == "ProductionLedgerError"
        optional = ledger.get_partition(
            PartitionIdentity(
                "optional_akshare", "daily_sample_crosscheck", "2024-01-03"
            )
        )
        stage = ledger.get_partition(
            PartitionIdentity("research_os", "stage_source", "2024-01-03")
        )
        assert optional is not None and optional.status is PartitionStatus.FAILED
        assert stage is not None and stage.status is PartitionStatus.FAILED
        assert ledger.list_incidents(limit=10) == ()
        with Session(engine) as session:
            capabilities = session.query(orm.SourceCapabilityModel).all()
        assert capabilities == []
        bronze = catalog.list_snapshots(tier="bronze", limit=10)
        assert len(bronze) == 2
        optional_bronze = next(
            record
            for record in bronze
            if "non_blocking_sample" in record.reference.trust_labels
        )
        assert "gold_promotion_forbidden" in optional_bronze.reference.trust_labels
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_required_source_failure_remains_fail_closed(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    config["daily"]["sources"][0]["partition_cadence"] = {
        "kind": "trading_session",
        "ledger_identity": "required_daily",
    }
    missing_root = tmp_path / "missing-required-source"
    missing_root.mkdir()
    config["daily"]["sources"][0]["root"] = str(missing_root)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    try:
        service = _services(
            tmp_path,
            config,
            FakeGoldPublisher([]),
            catalog=catalog,
        )
        service.production_ledger = ledger

        result = service.execute(_request(OperationName.SOURCE_SYNC))
        assert result.status == "failed"
        assert result.outputs["error_type"] == "RuntimeError"

        required = ledger.get_partition(
            PartitionIdentity("required_daily", "daily", "2024-01-03")
        )
        stage = ledger.get_partition(
            PartitionIdentity("research_os", "stage_source", "2024-01-03")
        )
        assert required is not None and required.status is PartitionStatus.FAILED
        assert stage is not None and stage.status is PartitionStatus.FAILED
        assert ledger.list_incidents(limit=10) == ()
        with Session(engine) as session:
            capabilities = session.query(orm.SourceCapabilityModel).all()
        assert capabilities == []
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_legacy_seed_trust_labels_block_silver_before_file_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    source = execute_operation(service, _request(OperationName.SOURCE_SYNC))
    source_row = source.outputs["sources"][0]
    blocked = build_immutable_snapshot_manifest(
        (source_row["data_path"], source_row["metadata_path"]),
        base_dir=service.settings.lake_root,
        tier="bronze",
        as_of=source_row["ingested_at"],
        parent_snapshot_ids=(),
        environment_hashes=HASHES,
        quality_report={"status": "blocked"},
        trust_labels=LEGACY_SEED_TRUST_LABELS,
    )
    service.catalog.register_snapshot(
        blocked.to_snapshot_ref(uri="s3://factor-lab/legacy-seed")
    )
    poisoned = OperationResult(
        operation=OperationName.SOURCE_SYNC,
        status="completed",
        summary="injected legacy seed",
        outputs={
            "sources": [
                {
                    **source.outputs["sources"][0],
                    "bronze_snapshot_id": blocked.snapshot_id,
                }
            ],
            "bronze_snapshot_ids": [blocked.snapshot_id],
        },
    )
    monkeypatch.setattr(
        service,
        "_dependency",
        lambda _request, operation, **_kwargs: (
            poisoned
            if operation is OperationName.SOURCE_SYNC
            else pytest.fail("unexpected dependency")
        ),
    )

    with pytest.raises(OrchestrationFailure, match="block Silver promotion"):
        service._source_reconciliation(_request(OperationName.SOURCE_RECONCILIATION))


def test_stale_cached_non_blocking_source_cannot_enter_formal_silver(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    source = execute_operation(service, _request(OperationName.SOURCE_SYNC))
    service.config["daily"]["sources"].append(
        {
            "source": "local_file",
            "profile_name": "optional_akshare",
            "non_blocking": True,
            "evidence_role": "non_blocking_sample",
        }
    )
    stale_row = {
        key: value
        for key, value in source.outputs["sources"][0].items()
        if key != "reconciliation_eligible"
    }
    stale_row["source_config_index"] = 1
    poisoned = OperationResult(
        operation=OperationName.SOURCE_SYNC,
        status="completed",
        summary="cached by a pre-split source implementation",
        outputs={
            "sources": [source.outputs["sources"][0], stale_row],
            "bronze_snapshot_ids": [
                source.outputs["bronze_snapshot_ids"][0],
                stale_row["bronze_snapshot_id"],
            ],
        },
    )
    monkeypatch.setattr(
        service,
        "_dependency",
        lambda _request, operation, **_kwargs: (
            poisoned
            if operation is OperationName.SOURCE_SYNC
            else pytest.fail("unexpected dependency")
        ),
    )

    with pytest.raises(
        OrchestrationFailure,
        match="non-blocking source evidence appeared in formal source outputs",
    ):
        service._source_reconciliation(_request(OperationName.SOURCE_RECONCILIATION))


def test_formal_source_cannot_disclaim_reconciliation_in_dependency_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _config(tmp_path)
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    source = execute_operation(service, _request(OperationName.SOURCE_SYNC))
    row = {
        **source.outputs["sources"][0],
        "reconciliation_eligible": False,
    }
    poisoned = OperationResult(
        operation=OperationName.SOURCE_SYNC,
        status="completed",
        summary="injected contradictory role marker",
        outputs={
            "sources": [row],
            "bronze_snapshot_ids": [row["bronze_snapshot_id"]],
        },
    )
    monkeypatch.setattr(
        service,
        "_dependency",
        lambda _request, operation, **_kwargs: (
            poisoned
            if operation is OperationName.SOURCE_SYNC
            else pytest.fail("unexpected dependency")
        ),
    )

    with pytest.raises(
        OrchestrationFailure,
        match="formal source explicitly forbids reconciliation",
    ):
        service._source_reconciliation(_request(OperationName.SOURCE_RECONCILIATION))


def test_missing_bronze_local_cache_is_hydrated_from_manifest_bound_minio(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    archive = S3ImmutableArchive(
        bucket="factor-lab", filesystem=_MemoryObjectStoreFileSystem()
    )
    service = _services(
        tmp_path, config, FakeGoldPublisher([]), object_store_archive=archive
    )
    source = execute_operation(service, _request(OperationName.SOURCE_SYNC))
    source_row = source.outputs["sources"][0]
    data_path = Path(source_row["data_path"])
    metadata_path = Path(source_row["metadata_path"])
    expected_data = data_path.read_bytes()
    expected_metadata = metadata_path.read_bytes()
    data_path.unlink()
    metadata_path.chmod(0o666)
    metadata_path.unlink()

    service._hydrate_cached_bronze_files(source)

    assert data_path.read_bytes() == expected_data
    assert metadata_path.read_bytes() == expected_metadata
    assert hashlib.sha256(data_path.read_bytes()).hexdigest() == source_row["sha256"]


def test_missing_silver_local_cache_is_hydrated_from_manifest_bound_minio(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    archive = S3ImmutableArchive(
        bucket="factor-lab", filesystem=_MemoryObjectStoreFileSystem()
    )
    service = _services(
        tmp_path, config, FakeGoldPublisher([]), object_store_archive=archive
    )
    execute_operation(service, _request(OperationName.SOURCE_SYNC))
    silver = execute_operation(
        service, _request(OperationName.SOURCE_RECONCILIATION)
    )
    silver_path = Path(silver.outputs["silver_path"])
    audit_path = Path(silver.outputs["audit_path"])
    expected_silver = silver_path.read_bytes()
    expected_audit = audit_path.read_bytes()
    silver_path.unlink()
    audit_path.chmod(0o666)
    audit_path.unlink()

    service._hydrate_cached_silver_files(silver)

    assert silver_path.read_bytes() == expected_silver
    assert audit_path.read_bytes() == expected_audit


def test_full_history_gold_hydrates_all_succeeded_silver_partitions(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    archive = S3ImmutableArchive(
        bucket="factor-lab", filesystem=_MemoryObjectStoreFileSystem()
    )
    service = _services(
        tmp_path, config, FakeGoldPublisher([]), object_store_archive=archive
    )
    execute_operation(service, _request(OperationName.SOURCE_SYNC))
    silver = execute_operation(
        service, _request(OperationName.SOURCE_RECONCILIATION)
    )
    silver_path = Path(silver.outputs["silver_path"])
    audit_path = Path(silver.outputs["audit_path"])
    expected_silver = silver_path.read_bytes()
    expected_audit = audit_path.read_bytes()
    silver_path.unlink()
    audit_path.chmod(0o666)
    audit_path.unlink()
    query_calls: list[dict] = []

    def list_partitions(**kwargs):
        query_calls.append(dict(kwargs))
        return (
            SimpleNamespace(
                identity=SimpleNamespace(partition_key="2024-01-03"),
                details={"operation_result": silver.to_dict()},
            ),
        )

    service.production_ledger = SimpleNamespace(list_partitions=list_partitions)

    service._hydrate_historical_silver_cache()

    assert silver_path.read_bytes() == expected_silver
    assert audit_path.read_bytes() == expected_audit
    assert query_calls[0]["source_id"] == "research_os"
    assert query_calls[0]["dataset"] == "stage_silver"


def test_postgresql_application_services_require_object_store_archive(tmp_path: Path) -> None:
    config = _config(tmp_path)
    settings = ResearchOSSettings(
        database_url="postgresql+psycopg://factor_lab:pw@127.0.0.1:5433/factor_lab",
        lake_root=tmp_path / "lake",
        snapshot_root=tmp_path / "snapshots",
        environment="production",
    )
    with pytest.raises(ServiceNotConfigured, match="object-store archive"):
        ApplicationServices(
            config,
            settings=settings,
            catalog=ResearchCatalog(settings.database_url),
            iceberg_publisher=FakeGoldPublisher([]),
            config_base=tmp_path,
            environment_hashes_override=HASHES,
        )


def _request(operation: OperationName, *, run_id: str = "dagster-run-1") -> OperationRequest:
    return OperationRequest(
        operation=operation,
        cycle=CycleName.DAILY,
        partition_key="2024-01-03",
        run_id=run_id,
    )


@pytest.mark.parametrize("first_status", ["failed", "blocked"])
def test_authoritative_backfill_terminal_retry_runs_new_generation(
    tmp_path: Path,
    first_status: str,
) -> None:
    config = _config(tmp_path)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    service = _services(
        tmp_path,
        config,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service.production_ledger = ledger
    calls: list[str] = []

    def flaky_handler(request: OperationRequest) -> OperationResult:
        calls.append(request.run_id)
        if len(calls) == 1:
            return OperationResult(
                operation=request.operation,
                status=first_status,
                summary="injected first terminal failure",
            )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary="repaired generation completed",
            outputs={"generation_attempt": len(calls)},
        )

    service._handlers[OperationName.SOURCE_SYNC] = flaky_handler
    request = _request(OperationName.SOURCE_SYNC, run_id="retry-generation-run")
    try:
        first = service.execute_authoritative_backfill(request)
        second = service.execute_authoritative_backfill(request)
        repeated = service.execute_authoritative_backfill(request)

        assert first.status == first_status
        assert second.status == repeated.status == "completed"
        assert calls == ["retry-generation-run", "retry-generation-run"]
        base_identity = PartitionIdentity(
            "research_os", "stage_source", request.partition_key
        )
        base = ledger.get_partition(base_identity)
        assert base is not None
        assert base.status is (
            PartitionStatus.FAILED
            if first_status == "failed"
            else PartitionStatus.QUARANTINED
        )
        rows = ledger.list_partitions(
            source_id="research_os", dataset="stage_source"
        )
        assert len(rows) == 2
        successor = next(row for row in rows if row.identity != base_identity)
        assert successor.status is PartitionStatus.SUCCEEDED
        assert successor.repair_parent_partition_run_id == base_identity.partition_run_id
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_source_child_quarantine_is_repaired_by_real_handler_successor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    config["daily"]["sources"][0]["partition_cadence"] = {
        "kind": "trading_session",
        "ledger_identity": "required_daily",
    }
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    service = _services(
        tmp_path,
        config,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service.production_ledger = ledger
    request = _request(OperationName.SOURCE_SYNC, run_id="source-child-repair")
    child_base = PartitionIdentity(
        "required_daily", "daily", request.partition_key
    )
    stage_base = PartitionIdentity(
        "research_os", "stage_source", request.partition_key
    )
    created_at = service._now() - timedelta(minutes=2)
    for identity in (child_base, stage_base):
        ledger.ensure_partition(identity, created_at=created_at)
        lease = ledger.claim(
            owner=f"quarantine-{identity.source_id}",
            identity=identity,
            now=created_at,
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.QUARANTINED,
            completed_at=created_at + timedelta(minutes=1),
            error_code="source_disputed",
            error="injected old source dispute",
        )

    original_sync_bronze = application_services_module.sync_bronze
    handler_calls: list[str] = []

    def observed_sync_bronze(*args, **kwargs):
        handler_calls.append("sync_bronze")
        return original_sync_bronze(*args, **kwargs)

    monkeypatch.setattr(
        application_services_module, "sync_bronze", observed_sync_bronze
    )
    try:
        repaired = service.execute_authoritative_backfill(request)
        repeated = service.execute_authoritative_backfill(request)

        assert repaired.status == repeated.status == "completed"
        assert handler_calls == ["sync_bronze"]
        assert ledger.get_partition(child_base).status is PartitionStatus.QUARANTINED
        assert ledger.get_partition(stage_base).status is PartitionStatus.QUARANTINED
        child_rows = ledger.list_partitions(
            source_id="required_daily", dataset="daily"
        )
        assert len(child_rows) == 2
        child_successor = next(
            row for row in child_rows if row.identity != child_base
        )
        assert child_successor.status is PartitionStatus.SUCCEEDED
        assert (
            child_successor.repair_parent_partition_run_id
            == child_base.partition_run_id
        )
        stage_rows = ledger.list_partitions(
            source_id="research_os", dataset="stage_source"
        )
        assert len(stage_rows) == 2
        assert next(
            row for row in stage_rows if row.identity != stage_base
        ).status is PartitionStatus.SUCCEEDED
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_downstream_dependency_selects_succeeded_generic_retry_leaf(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    service = _services(
        tmp_path,
        config,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service.production_ledger = ledger
    source_calls = 0

    def source_handler(request: OperationRequest) -> OperationResult:
        nonlocal source_calls
        source_calls += 1
        return OperationResult(
            operation=request.operation,
            status=("failed" if source_calls == 1 else "completed"),
            summary="first source failed" if source_calls == 1 else "source repaired",
            outputs={"sources": [], "attempt": source_calls},
        )

    def silver_handler(request: OperationRequest) -> OperationResult:
        source = service._dependency(request, OperationName.SOURCE_SYNC)
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary="silver consumed repaired source",
            outputs={"source_attempt": source.outputs["attempt"]},
        )

    service._handlers[OperationName.SOURCE_SYNC] = source_handler
    service._handlers[OperationName.SOURCE_RECONCILIATION] = silver_handler
    try:
        source_request = _request(
            OperationName.SOURCE_SYNC, run_id="generic-source-retry"
        )
        assert service.execute_authoritative_backfill(source_request).status == "failed"
        assert service.execute_authoritative_backfill(source_request).status == "completed"

        silver = service.execute_authoritative_backfill(
            _request(
                OperationName.SOURCE_RECONCILIATION,
                run_id="silver-after-generic-source-retry",
            )
        )
        assert silver.status == "completed"
        assert silver.outputs["source_attempt"] == 2
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_failed_generic_successor_is_exact_incident_repair_parent(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    service = _services(
        tmp_path,
        config,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service.production_ledger = ledger

    def failed_source(request: OperationRequest) -> OperationResult:
        return OperationResult(
            operation=request.operation,
            status="failed",
            summary="injected persistent source failure",
        )

    service._handlers[OperationName.SOURCE_SYNC] = failed_source
    request = _request(
        OperationName.SOURCE_SYNC, run_id="failed-source-successor"
    )
    try:
        assert service.execute_authoritative_backfill(request).status == "failed"
        assert service.execute_authoritative_backfill(request).status == "failed"
        incident_stage, pipeline_stage, partition_run_id, _evidence = (
            service._infer_failed_data_stage(request.partition_key)
        )
        base = PartitionIdentity(
            "research_os", "stage_source", request.partition_key
        )
        retry = ledger.get_retry_partition(base)
        assert retry is not None and retry.status is PartitionStatus.FAILED
        assert partition_run_id == retry.identity.partition_run_id
        assert pipeline_stage.value == "source"
        incident = ledger.record_incident(
            partition_key=request.partition_key,
            stage=incident_stage,
            error_code="dagster_run_failure",
            message="repair the failed successor",
            occurred_at=service._now(),
            partition_run_id=partition_run_id,
            payload={
                "dagster_run_id": request.run_id,
                "failed_step_key": "source_sync",
            },
        )
        selected = ledger.reserve_repair_successor(
            incident_id=incident.incident_id,
            dataset="stage_source",
            repair_fingerprint="f" * 64,
            created_at=service._now(),
        )
        assert selected.parent_partition_run_id == retry.identity.partition_run_id
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_explicit_incident_repair_executes_one_five_stage_successor_chain(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    service = _services(
        tmp_path,
        config,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service.production_ledger = ledger
    partition_key = "2024-01-03"
    occurred_at = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
    source_base = PartitionIdentity(
        "research_os", "stage_source", partition_key
    )
    gold_base = PartitionIdentity("research_os", "stage_gold", partition_key)
    for identity, status in (
        (source_base, PartitionStatus.SUCCEEDED),
        (gold_base, PartitionStatus.FAILED),
    ):
        ledger.ensure_partition(
            identity,
            created_at=occurred_at - timedelta(minutes=2),
            input_hash="a" * 64,
        )
        lease = ledger.claim(
            owner=f"base-{identity.dataset}",
            identity=identity,
            now=occurred_at - timedelta(minutes=2),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=status,
            completed_at=occurred_at - timedelta(minutes=1),
            output_hash=("b" * 64 if status is PartitionStatus.SUCCEEDED else None),
            error_code=(None if status is PartitionStatus.SUCCEEDED else "gold_failed"),
            error=(None if status is PartitionStatus.SUCCEEDED else "gold failed"),
        )
    incident = ledger.record_incident(
        partition_key=partition_key,
        stage=application_services_module.IncidentStage.GOLD,
        error_code="gold_failed",
        message="gold failed",
        occurred_at=occurred_at,
        partition_run_id=gold_base.partition_run_id,
    )
    calls: list[OperationName] = []

    def completed(request: OperationRequest) -> OperationResult:
        calls.append(request.operation)
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"repaired {request.operation.value}",
        )

    operations = (
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        OperationName.SHADOW_NAV_STEP,
    )
    for operation in operations:
        service._handlers[operation] = completed
    service._latest_complete_accepted_session = lambda: date.fromisoformat(
        partition_key
    )
    try:
        with pytest.raises(
            ProductionLedgerError, match="Source-to-Shadow order"
        ):
            service.execute_data_incident_repair(
                _request(
                    OperationName.SOURCE_RECONCILIATION,
                    run_id="incident-repair-run",
                ),
                incident_id=incident.incident_id,
            )

        results = [
            service.execute_data_incident_repair(
                _request(
                    operation,
                    run_id=(
                        "incident-repair-run"
                        if operation is OperationName.SOURCE_SYNC
                        else "incident-repair-resume-run"
                    ),
                ),
                incident_id=incident.incident_id,
            )
            for operation in operations
        ]
        repeated = service.execute_data_incident_repair(
            _request(
                OperationName.SHADOW_NAV_STEP,
                run_id="incident-repair-second-resume-run",
            ),
            incident_id=incident.incident_id,
        )

        assert all(result.status == "completed" for result in results)
        assert repeated.status == "completed"
        assert calls == list(operations)
        authorities = [
            ledger.get_repair_authority(
                incident.incident_id,
                {
                    OperationName.SOURCE_SYNC: "stage_source",
                    OperationName.SOURCE_RECONCILIATION: "stage_silver",
                    OperationName.DATA_QUALITY_GATE: "stage_data_quality",
                    OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH: "stage_gold",
                    OperationName.SHADOW_NAV_STEP: "stage_shadow",
                }[operation],
            )
            for operation in operations
        ]
        assert all(authority is not None for authority in authorities)
        assert len({authority.repair_fingerprint for authority in authorities}) == 1
        cohorts = {
            ledger.get_partition(authority.identity).details["repair_cohort_id"]
            for authority in authorities
        }
        assert len(cohorts) == 1
        assert ledger.get_partition(gold_base).status is PartitionStatus.FAILED
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_open_incident_repair_fails_closed_across_runtime_configuration_drift(
    tmp_path: Path,
) -> None:
    config_a = _config(tmp_path)
    config_b = json.loads(json.dumps(config_a))
    config_b["repair_runtime_marker"] = "configuration-b"
    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    service_a = _services(
        tmp_path,
        config_a,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service_b = _services(
        tmp_path,
        config_b,
        FakeGoldPublisher([]),
        catalog=catalog,
    )
    service_a.production_ledger = ledger
    service_b.production_ledger = ledger
    partition_key = "2024-01-03"
    occurred_at = datetime(2026, 8, 21, 12, tzinfo=timezone.utc)
    source_base = PartitionIdentity(
        "research_os", "stage_source", partition_key
    )
    gold_base = PartitionIdentity("research_os", "stage_gold", partition_key)
    for identity, status in (
        (source_base, PartitionStatus.SUCCEEDED),
        (gold_base, PartitionStatus.FAILED),
    ):
        ledger.ensure_partition(
            identity,
            created_at=occurred_at - timedelta(minutes=2),
            input_hash="a" * 64,
        )
        lease = ledger.claim(
            owner=f"config-drift-{identity.dataset}",
            identity=identity,
            now=occurred_at - timedelta(minutes=2),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=status,
            completed_at=occurred_at - timedelta(minutes=1),
            output_hash=("b" * 64 if status is PartitionStatus.SUCCEEDED else None),
            error_code=(None if status is PartitionStatus.SUCCEEDED else "gold_failed"),
            error=(None if status is PartitionStatus.SUCCEEDED else "gold failed"),
        )
    incident = ledger.record_incident(
        partition_key=partition_key,
        stage=application_services_module.IncidentStage.GOLD,
        error_code="gold_failed",
        message="gold failed before a configuration change",
        occurred_at=occurred_at,
        partition_run_id=gold_base.partition_run_id,
    )
    calls_a: list[OperationName] = []
    calls_b: list[OperationName] = []

    def completed_a(request: OperationRequest) -> OperationResult:
        calls_a.append(request.operation)
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"configuration A repaired {request.operation.value}",
        )

    def forbidden_b(request: OperationRequest) -> OperationResult:
        calls_b.append(request.operation)
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary="configuration B must not execute",
        )

    service_a._handlers[OperationName.SOURCE_SYNC] = completed_a
    service_a._handlers[OperationName.SOURCE_RECONCILIATION] = completed_a
    service_b._handlers[OperationName.SOURCE_RECONCILIATION] = forbidden_b
    try:
        source_result = service_a.execute_data_incident_repair(
            _request(OperationName.SOURCE_SYNC, run_id="config-a-source"),
            incident_id=incident.incident_id,
        )
        assert source_result.status == "completed"
        source_authority = ledger.get_repair_authority(
            incident.incident_id, "stage_source"
        )
        assert source_authority is not None

        with pytest.raises(
            OrchestrationFailure,
            match="OPEN data incident repair configuration drift",
        ):
            service_b.execute_data_incident_repair(
                _request(
                    OperationName.SOURCE_RECONCILIATION,
                    run_id="config-b-resume",
                ),
                incident_id=incident.incident_id,
            )

        assert calls_b == []
        assert ledger.get_repair_authority(
            incident.incident_id, "stage_source"
        ) == source_authority
        assert ledger.get_repair_authority(
            incident.incident_id, "stage_silver"
        ) is None

        resumed = service_a.execute_data_incident_repair(
            _request(
                OperationName.SOURCE_RECONCILIATION,
                run_id="config-a-resume",
            ),
            incident_id=incident.incident_id,
        )
        assert resumed.status == "completed"
        assert calls_a == [
            OperationName.SOURCE_SYNC,
            OperationName.SOURCE_RECONCILIATION,
        ]
    finally:
        ledger.close()
        engine.dispose()
        catalog.close()


def test_monthly_gate_rejects_payload_evidence_authority_and_uses_database_clock(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "monthly-input.json"
    config = _config(tmp_path)
    config["monthly"] = {"input_path": str(input_path)}
    spec = ExperimentSpec(
        snapshot=DataSnapshotRef(
            snapshot_id="monthly-gold",
            tier="gold",
            uri="iceberg://factorlab/gold#monthly-gold",
            content_hash="9" * 64,
            as_of=datetime(2026, 8, 1, tzinfo=timezone.utc),
        ),
        factor=FactorSpec(
            factor_id="monthly-factor",
            family="value",
            name="Monthly factor",
            mechanism="pre-registered mechanism",
            expression={"op": "rank", "input": "score"},
            direction="higher_is_better",
            falsification_criteria=("outer OOS fails",),
        ),
        evaluator_version="research_os.long_only.v2",
        environment=EnvironmentRef(
            code_hash="1" * 64,
            dependency_lock_hash="2" * 64,
            configuration_hash="3" * 64,
            python_version="3.11",
            platform="test",
            evaluator_build="research_os.long_only.v2",
        ),
        preregistration=Preregistration(
            hypothesis_id="monthly-hypothesis",
            economic_mechanism="pre-registered mechanism",
            direction="positive",
            falsification_criteria=("outer OOS fails",),
            stop_rules=("stop after two branches",),
        ),
    )
    spoofed = {
        "experiment": spec.model_dump(mode="json"),
        "registered_at": "1999-01-01T00:00:00+00:00",
        "evidence_class": "pristine_forward",
        "holdout_id": "invented-holdout",
    }
    input_path.write_text(json.dumps(spoofed), encoding="utf-8")
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    request = OperationRequest(
        operation=OperationName.CONFIRMATORY_BUDGET_GATE,
        cycle=CycleName.MONTHLY,
        partition_key="1999-01",
        run_id="monthly-authority",
    )
    with pytest.raises(OrchestrationFailure, match="self-assert evidence authority"):
        service._confirmatory_budget_gate(request)

    input_path.write_text(
        json.dumps({"experiment": spec.model_dump(mode="json")}), encoding="utf-8"
    )
    before = service.catalog.database_now()
    result = service._confirmatory_budget_gate(request)
    after = service.catalog.database_now()
    trial = service.catalog.get_trial(str(result.outputs["trial_id"]))
    assert trial is not None
    assert before <= trial.occurred_at <= after
    assert trial.metadata["evidence_class"] == "pseudo_oos"
    assert trial.metadata["holdout_id"] == HISTORICAL_HOLDOUT_ID


def test_monthly_gate_persists_and_reviews_llm_proposal_before_budget(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "monthly-proposal.json"
    config = _config(tmp_path)
    config["monthly"] = {"input_path": str(input_path)}
    evaluator = "research_os.long_only.v2"
    template = ExperimentSpec(
        snapshot=DataSnapshotRef(
            snapshot_id="a" * 64,
            tier=SnapshotTier.GOLD,
            uri="s3://factor-lab/gold/a",
            content_hash="a" * 64,
            as_of=datetime(2026, 8, 1, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
        ),
        universe=UniverseSpec(),
        label=LabelSpec(),
        factor=FactorSpec(
            factor_id="template-placeholder",
            family="template",
            name="Template placeholder",
            mechanism="Only reserves the factor-shaped template slot.",
            expression={
                "nodes": [{"id": "raw", "op": "field", "field": "score"}],
                "output": "raw",
            },
            direction="higher_is_better",
            falsification_criteria=("never evaluated",),
        ),
        portfolio=PortfolioPolicy(),
        validation=ValidationProtocol(
            initial_train_start=date(2017, 1, 1),
            initial_train_end=date(2020, 12, 31),
        ),
        evaluator_version=evaluator,
        environment=EnvironmentRef(
            code_hash="1" * 64,
            dependency_lock_hash="2" * 64,
            configuration_hash="3" * 64,
            dirty_patch_hash="4" * 64,
            python_version="3.12",
            platform="test",
            evaluator_build=evaluator,
        ),
        evaluation_inputs=EvaluationInputBindings(
            bootstrap_resamples=2_000,
            bootstrap_seed=0,
        ),
        preregistration=Preregistration(
            hypothesis_id="template-placeholder",
            economic_mechanism="Template placeholder only.",
            direction="positive",
            falsification_criteria=("never evaluated",),
            stop_rules=("never run",),
        ),
    )
    proposal = {
        "preregistration": {
            "hypothesis_id": "hyp-value-quality",
            "economic_mechanism": "cheap profitable firms may be underpriced",
            "direction": "positive",
            "falsification_criteria": ["outer OOS excess is non-positive"],
            "stop_rules": ["stop after two diagnostics"],
        },
        "factor": {
            "factor_id": "value-quality-v1",
            "family": "value_quality",
            "name": "Value quality",
            "mechanism": "cheap profitable firms may be underpriced",
            "expression": {
                "nodes": [
                    {"id": "raw", "op": "field", "field": "score"},
                    {"id": "ranked", "op": "rank", "input": "raw"},
                ],
                "output": "ranked",
            },
            "direction": "higher_is_better",
            "falsification_criteria": ["outer OOS excess is non-positive"],
        },
    }
    payload = {
        "proposal": {**proposal, "sharpe": 9.9},
        "experiment_template": template.model_dump(mode="json"),
        "field_specs": [
            {
                "name": "score",
                "value_type": "numeric",
                "role": "feature",
                "availability": "close",
            }
        ],
    }
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    request = OperationRequest(
        operation=OperationName.CONFIRMATORY_BUDGET_GATE,
        cycle=CycleName.MONTHLY,
        partition_key="2026-08",
        run_id="monthly-proposal",
    )

    rejected = service._confirmatory_budget_gate(request)
    assert rejected.status == "blocked"
    assert service.catalog.list_trials() == []
    assert len(service.catalog.list_runs(run_type="llm_proposal_review")) == 1

    payload["proposal"] = proposal
    input_path.write_text(json.dumps(payload), encoding="utf-8")
    accepted = service._confirmatory_budget_gate(request)
    assert accepted.status == "completed"
    assert accepted.outputs["allowed"] is True
    assert accepted.outputs["proposal_decision"]["accepted"] is True
    assert accepted.outputs["experiment_spec"]["factor"]["factor_id"] == "value-quality-v1"
    assert len(service.catalog.list_runs(run_type="llm_proposal_review")) == 2
    assert len(service.catalog.list_trials()) == 1


def test_challenger_generation_rejects_local_return_file_authority(
    tmp_path: Path, monkeypatch
) -> None:
    input_path = tmp_path / "monthly-input.json"
    input_path.write_text("{}", encoding="utf-8")
    config = _config(tmp_path)
    config["monthly"] = {
        "input_path": str(input_path),
        "challenger": {
            "historical_challenger_path": "forged-historical-a.parquet",
            "historical_champion_path": "forged-historical-b.parquet",
            "shadow_challenger_path": "forged-shadow-a.parquet",
            "shadow_champion_path": "forged-shadow-b.parquet",
        },
    }
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    monkeypatch.setattr(
        service,
        "_dependency",
        lambda *args, **kwargs: OperationResult(
            operation=OperationName.WEIGHT_REESTIMATION,
            status="completed",
            summary="test dependency",
            outputs={},
        ),
    )
    request = OperationRequest(
        operation=OperationName.CHALLENGER_GENERATION,
        cycle=CycleName.MONTHLY,
        partition_key="2026-08",
        run_id="forged-return-files",
    )
    with pytest.raises(OrchestrationFailure, match="cannot come from caller return files"):
        service._challenger_generation(request)


def test_catalog_role_challenger_generation_streams_past_lifecycle_noise(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_path = tmp_path / "monthly-catalog-role-input.json"
    input_path.write_text("{}", encoding="utf-8")
    config = _config(tmp_path)
    config["monthly"] = {
        "input_path": str(input_path),
        "challenger": {
            "authority_mode": "catalog_roles",
            "champion_role": "static_champion",
        },
    }
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    binding_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    service.catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="old-valid-challenger-walk-forward",
            sleeve_id="value_quality",
            to_state=LifecycleState.WALK_FORWARD,
            cause="fixture walk-forward state",
            occurred_at=binding_time - timedelta(microseconds=1),
        )
    )
    binding_event = service.catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="old-valid-challenger-role-binding",
            sleeve_id="value_quality",
            from_state=LifecycleState.WALK_FORWARD,
            to_state=LifecycleState.SHADOW,
            cause="challenger_shadow_account_bound",
            evidence={
                "promotion": {"experiment_id": "old-valid-challenger"},
                "shadow_account_id": "old-valid-challenger-account",
            },
            occurred_at=binding_time,
        )
    )
    for index in range(1_001):
        service.catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key=f"newer-unrelated-lifecycle-{index:04d}",
                sleeve_id=f"unrelated_sleeve_{index:04d}",
                to_state=LifecycleState.PROPOSED,
                cause="unrelated_lifecycle_noise",
                evidence={"fixture_index": index},
                occurred_at=binding_time + timedelta(days=1, seconds=index),
            )
        )
    truncated = service.catalog.list_lifecycle_events(limit=1_000)
    assert binding_event.event_id not in {event.event_id for event in truncated}

    class RoleAuthority:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str]] = []

        def active_binding(self, *, role, role_key):
            self.calls.append((str(getattr(role, "value", role)), role_key))
            if role_key == "old-valid-challenger":
                return SimpleNamespace(account_id="old-valid-challenger-account")
            return None

    role_authority = RoleAuthority()
    service.shadow_authority = role_authority
    monkeypatch.setattr(
        service,
        "_dependency",
        lambda *args, **kwargs: OperationResult(
            operation=OperationName.WEIGHT_REESTIMATION,
            status="completed",
            summary="catalog role fixture",
            outputs={},
        ),
    )

    result = service._challenger_generation(
        OperationRequest(
            operation=OperationName.CHALLENGER_GENERATION,
            cycle=CycleName.MONTHLY,
            partition_key="2026-08",
            run_id="catalog-role-lifecycle-depth",
        )
    )

    assert result.status == "completed"
    assert result.outputs["authority_blocker"] == (
        "static Champion role binding is absent"
    )
    assert ("challenger", "old-valid-challenger") in role_authority.calls


def test_authoritative_weight_mode_never_reads_local_state_return_files(
    tmp_path: Path, monkeypatch
) -> None:
    input_path = tmp_path / "monthly-input.json"
    input_path.write_text("{}", encoding="utf-8")
    config = _config(tmp_path)
    snapshot_ref = DataSnapshotRef(
        snapshot_id="authoritative-weight-gold",
        tier="gold",
        uri="iceberg://factorlab/gold#authoritative-weight-gold",
        content_hash="8" * 64,
        as_of=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )
    config["monthly"] = {
        "input_path": str(input_path),
        "weights": {
            "input_mode": "authoritative_pg",
            "state_history_path": "must-not-be-read-state.parquet",
            "sleeve_returns_path": "must-not-be-read-returns.parquet",
            "data_snapshot_id": snapshot_ref.snapshot_id,
        },
    }
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    service.catalog.register_snapshot(snapshot_ref)
    monkeypatch.setattr(
        service,
        "_dependency",
        lambda *args, **kwargs: OperationResult(
            operation=OperationName.LIMITED_DISCOVERY,
            status="skipped",
            summary="no discovery",
            outputs={},
        ),
    )
    result = service._weight_reestimation(
        OperationRequest(
            operation=OperationName.WEIGHT_REESTIMATION,
            cycle=CycleName.MONTHLY,
            partition_key="2026-08",
            run_id="authoritative-static",
        )
    )
    assert result.status == "completed"
    assert result.outputs["state_input_mode"] == "authoritative_pg"
    assert result.outputs["proposed_adaptive_scores"] == {}


def test_daily_pipeline_commits_real_gold_outputs_and_skips_only_missing_champion(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    publisher = FakeGoldPublisher([])
    service = _services(tmp_path, config, publisher)

    results = [
        execute_operation(service, _request(operation))
        for operation in (
            OperationName.SOURCE_SYNC,
            OperationName.SOURCE_RECONCILIATION,
            OperationName.DATA_QUALITY_GATE,
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
            OperationName.SHADOW_NAV_STEP,
        )
    ]

    assert [result.status for result in results] == [
        "completed",
        "completed",
        "completed",
        "completed",
        "skipped",
    ]
    gold = results[3]
    assert gold.outputs["iceberg_table"] == "factor_lab.gold_daily"
    assert gold.outputs["iceberg_snapshot_id"] == 901
    assert gold.outputs["iceberg_tag"].startswith("ros_")
    assert len(publisher.calls) == 1
    snapshot = service.catalog.get_snapshot(gold.outputs["snapshot_id"])
    assert snapshot is not None
    assert snapshot.reference.uri.endswith("#" + gold.outputs["iceberg_tag"])


def test_daily_partition_runs_reference_claimed_operation_runs_with_fk_enforced(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    source_root = tmp_path / "source"
    config["daily"]["sources"][0]["partition_cadence"] = {
        "kind": "trading_session",
        "ledger_identity": "local_daily",
    }
    pd.DataFrame(
        {"exchange": ["SSE"], "cal_date": ["2024-01-03"], "is_open": [1]}
    ).to_parquet(source_root / "trade_calendar.parquet", index=False)
    pd.DataFrame(
        {"ts_code": ["000002.SZ"], "date": ["2024-01-03"], "is_st": [1]}
    ).to_parquet(source_root / "historical_st.parquet", index=False)

    availability = {
        "mode": "session_release_time",
        "time": "08:00:00",
        "timezone": "Asia/Shanghai",
        "lag_days": 0,
    }

    def local_source(
        *,
        profile_name: str,
        dataset: str,
        filename: str,
        key_fields: list[str],
        event_time_field: str,
        fields: list[dict],
        entity_columns: list[str],
        value_columns: list[str],
    ) -> dict:
        return {
            "source": "local_file",
            "profile_name": profile_name,
            "root": str(source_root),
            "priority": 20,
            "path_templates": {dataset: filename},
            "partition_cadence": {
                "kind": "trading_session",
                "ledger_identity": profile_name,
            },
            "request": {"dataset": dataset},
            "contract": {
                "dataset": dataset,
                "key_fields": key_fields,
                "event_time_field": event_time_field,
                "release_timing": "known before the session opens",
                "fields": fields,
            },
            "canonicalization": {
                "entity_columns": entity_columns,
                "event_time_column": event_time_field,
                "value_columns": value_columns,
                "availability": availability,
            },
        }

    config["daily"]["sources"].extend(
        (
            local_source(
                profile_name="calendar",
                dataset="trade_calendar",
                filename="trade_calendar.parquet",
                key_fields=["exchange", "cal_date"],
                event_time_field="cal_date",
                fields=[
                    {"name": "exchange", "dtype": "string", "nullable": False},
                    {"name": "cal_date", "dtype": "date", "nullable": False},
                    {"name": "is_open", "dtype": "int64", "nullable": False},
                ],
                entity_columns=["exchange"],
                value_columns=["is_open"],
            ),
            local_source(
                profile_name="historical_st",
                dataset="historical_st",
                filename="historical_st.parquet",
                key_fields=["ts_code", "date"],
                event_time_field="date",
                fields=[
                    {"name": "ts_code", "dtype": "string", "nullable": False},
                    {"name": "date", "dtype": "date", "nullable": False},
                    {"name": "is_st", "dtype": "int64", "nullable": False},
                ],
                entity_columns=["ts_code"],
                value_columns=["is_st"],
            ),
        )
    )
    # Two market rows plus one calendar and one ST row form the heterogeneous
    # Silver union.  The test is about run authority, not market-field density.
    config["daily"]["data_quality"]["minimum_core_coverage"] = 0.4

    catalog, engine, ledger = _sqlite_fk_authority(tmp_path)
    try:
        service = _services(
            tmp_path, config, FakeGoldPublisher([]), catalog=catalog
        )
        service.production_ledger = ledger
        dagster_run_id = "dagster-fk-regression"
        requests = {
            operation: _request(operation, run_id=dagster_run_id)
            for operation in (
                OperationName.SOURCE_SYNC,
                OperationName.SOURCE_RECONCILIATION,
                OperationName.DATA_QUALITY_GATE,
            )
        }

        results = [service.execute(request) for request in requests.values()]

        assert [result.status for result in results] == [
            "completed",
            "completed",
            "completed",
        ], [result.to_dict() for result in results]
        source_run_id = service._operation_run_id(
            requests[OperationName.SOURCE_SYNC]
        )
        silver_run_id = service._operation_run_id(
            requests[OperationName.SOURCE_RECONCILIATION]
        )
        quality_run_id = service._operation_run_id(
            requests[OperationName.DATA_QUALITY_GATE]
        )
        expected = (
            (PartitionIdentity("local_daily", "daily", "2024-01-03"), source_run_id),
            (
                PartitionIdentity("calendar", "trade_calendar", "2024-01-03"),
                source_run_id,
            ),
            (
                PartitionIdentity("historical_st", "historical_st", "2024-01-03"),
                source_run_id,
            ),
            (
                PartitionIdentity("research_os", "stage_source", "2024-01-03"),
                source_run_id,
            ),
            (
                PartitionIdentity("research_os", "stage_silver", "2024-01-03"),
                silver_run_id,
            ),
            (
                PartitionIdentity(
                    "research_os", "accepted_trade_calendar", "2024-01-03"
                ),
                quality_run_id,
            ),
            (
                PartitionIdentity(
                    "research_os", "stage_data_quality", "2024-01-03"
                ),
                quality_run_id,
            ),
        )
        assert len(ledger.list_partitions(limit=100)) == len(expected)
        for identity, expected_run_id in expected:
            record = ledger.get_partition(identity)
            assert record is not None
            assert record.status is PartitionStatus.SUCCEEDED
            assert record.run_id == expected_run_id
            assert catalog.get_run(expected_run_id) is not None
        assert catalog.get_run(dagster_run_id) is None
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
            assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
    finally:
        catalog.close()
        engine.dispose()


def test_daily_stage_state_is_reused_across_service_processes(tmp_path: Path) -> None:
    config = _config(tmp_path)
    first_publisher = FakeGoldPublisher([])
    first = _services(tmp_path, config, first_publisher)
    for operation in (
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
    ):
        execute_operation(first, _request(operation))

    second_publisher = FakeGoldPublisher([])
    second = _services(tmp_path, config, second_publisher)
    cached = execute_operation(
        second,
        OperationRequest(
            operation=OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
            cycle=CycleName.DAILY,
            partition_key="2024-01-03",
            run_id="dagster-retry-with-new-external-id",
            metadata={
                "dagster_tags": {"dagster/retry_number": "1"},
                "authority": "production_cli",
            },
        ),
    )
    assert cached.outputs["iceberg_snapshot_id"] == 901
    assert second_publisher.calls == []
    assert not list((tmp_path / "lake").rglob("*.result.json"))
    runs = second.catalog.list_runs(limit=20)
    assert len(runs) == 4
    assert all(run.status == "completed" for run in runs)
    assert all(run.metadata["operation_result"]["summary"] for run in runs)
    assert all(run.run_type.startswith("dagster:daily:") for run in runs)
    assert second.catalog.get_run("dagster-retry-with-new-external-id") is None


def test_weekly_lifecycle_ignores_json_counters_and_persists_raw_measurement(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    weekly_path = tmp_path / "weekly.json"
    config["weekly"] = {"input_path": str(weekly_path)}
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    snapshot_id = "b" * 64
    service.catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold_daily#weekly",
            content_hash=snapshot_id,
            as_of=datetime(2026, 1, 1, 7, 0, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=("point_in_time",),
        )
    )
    service.catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="weekly-fixture-active",
            sleeve_id="value_quality",
            to_state=LifecycleState.ACTIVE,
            cause="fixture active state",
            occurred_at=datetime(2026, 1, 1, 8, tzinfo=timezone.utc),
        )
    )

    def write_weekly(day: date) -> None:
        weekly_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "trading_sessions": ["2099-01-01"] * 60,
                    "sleeves": [
                        {
                            "record": {
                                "sleeve_id": "value_quality",
                                "state": "active",
                                "target_weight": 0.25,
                                "effective_weight": 0.25,
                                "consecutive_multi_alarm_checks": 999,
                                "reduced_weeks": 999,
                                "probation_weeks": 999,
                            },
                            "observation": {
                                "as_of_date": day.isoformat(),
                                "active_ir_13w": -0.2,
                                "active_ir_26w": -0.1,
                                "ic_26w": -0.02,
                                "new_sessions_since_dormant": 999,
                            },
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    for index, day in enumerate(
        (date(2026, 1, 5), date(2026, 1, 12), date(2026, 1, 19)), 1
    ):
        write_weekly(day)
        for operation in (
            OperationName.SLEEVE_HEALTH_CHECK,
            OperationName.DRIFT_DETECTION,
            OperationName.LIFECYCLE_TRANSITION,
            OperationName.RECOVERY_SLA_CHECK,
        ):
            result = service.execute(
                OperationRequest(
                    operation=operation,
                    cycle=CycleName.WEEKLY,
                    partition_key=day.isoformat(),
                    run_id=f"weekly-{index}",
                )
            )
            if index == 2 and operation is OperationName.LIFECYCLE_TRANSITION:
                active_cases = tuple(
                    service.catalog.iter_recovery_cases(
                        statuses=(
                            RecoveryCaseStatus.OPEN,
                            RecoveryCaseStatus.DIAGNOSING,
                            RecoveryCaseStatus.OBSERVING,
                        ),
                        sleeve_id="value_quality",
                        batch_size=1_000,
                    )
                )
                assert len(active_cases) == 1
                active_case = active_cases[0]
                for terminal_index in range(1_001):
                    offset = timedelta(minutes=terminal_index + 1)
                    service.catalog.save_recovery_case(
                        RecoveryCase(
                            recovery_case_id=f"newer-terminal-{terminal_index}",
                            sleeve_id=active_case.sleeve_id,
                            status=RecoveryCaseStatus.CLOSED,
                            lifecycle_state=active_case.lifecycle_state,
                            triggered_at=active_case.triggered_at + offset,
                            drift_event_due_at=active_case.drift_event_due_at + offset,
                            diagnosis_due_at=active_case.diagnosis_due_at + offset,
                            earliest_recovery_review_at=(
                                active_case.earliest_recovery_review_at + offset
                            ),
                        )
                    )
                visible_under_old_limit = service.catalog.list_recovery_cases(
                    sleeve_id="value_quality", limit=100
                )
                assert active_case.recovery_case_id not in {
                    case.recovery_case_id for case in visible_under_old_limit
                }
        health_run = next(
            run
            for run in service.catalog.list_runs(limit=20)
            if run.run_type == "dagster:weekly:sleeve_health_check"
            and run.metadata["partition_key"] == day.isoformat()
        )
        observation = health_run.metadata["operation_result"]["outputs"][
            "evaluations"
        ][0]["observation"]
        assert observation["new_sessions_since_dormant"] == 0
        if index == 2:
            assert result.outputs["cases"][0]["persisted_shadow_sessions"] == 0
            assert (
                result.outputs["cases"][0]["checkpoints"][
                    "recovery_observation_complete"
                ]
                is False
            )
    events = service.catalog.list_lifecycle_events(sleeve_id="value_quality", limit=20)
    ticks = [event for event in events if event.cause == "weekly_health_tick"]
    measurements = [
        event for event in events if event.cause == "health_measurement_recorded"
    ]
    assert len(ticks) == len(measurements) == 3
    assert ticks[0].to_state is LifecycleState.REDUCED
    assert ticks[0].evidence["record"]["consecutive_multi_alarm_checks"] == 3
    assert measurements[0].evidence["measurement_kind"] == "raw_point_in_time"
    assert (
        "new_sessions_since_dormant"
        not in measurements[0].evidence["measurement"]
    )
    recovery = tuple(
        service.catalog.iter_recovery_cases(
            statuses=(
                RecoveryCaseStatus.OPEN,
                RecoveryCaseStatus.DIAGNOSING,
                RecoveryCaseStatus.OBSERVING,
            ),
            sleeve_id="value_quality",
            batch_size=1_000,
        )
    )
    assert len(recovery) == 1
    # Configured fake future sessions did not set the authoritative deadlines.
    assert recovery[0].drift_event_due_at.date() == date(2026, 1, 19)


def test_dormant_recovery_uses_sixty_persisted_shadow_sessions(tmp_path: Path) -> None:
    config = _config(tmp_path)
    weekly_path = tmp_path / "weekly.json"
    config["weekly"] = {"input_path": str(weekly_path)}
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    snapshot_id = "c" * 64
    service.catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold_daily#recovery",
            content_hash=snapshot_id,
            as_of=datetime(2026, 1, 1, 7, 0, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=("point_in_time",),
        )
    )
    dormant_day = date(2026, 1, 1)
    service.catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="seed:reduced:value_quality",
            sleeve_id="value_quality",
            to_state=LifecycleState.REDUCED,
            cause="legacy_catalog_migration_root",
            occurred_at=datetime(2026, 1, 1, 6, 59, tzinfo=timezone.utc),
        )
    )
    service.catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="seed:dormant:value_quality",
            sleeve_id="value_quality",
            from_state=LifecycleState.REDUCED,
            to_state=LifecycleState.DORMANT,
            cause="legacy_catalog_migration",
            occurred_at=datetime(2026, 1, 1, 7, 0, tzinfo=timezone.utc),
            evidence={
                "record": {
                    "sleeve_id": "value_quality",
                    "state": "dormant",
                    "target_weight": 0.25,
                    "effective_weight": 0.0,
                    "dormant_since": dormant_day.isoformat(),
                }
            },
        )
    )
    service.catalog.create_shadow_account(
        account_id="value-quality-shadow",
        name="Value quality shadow",
        initial_capital=50_000_000,
        opened_at=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
    )
    sessions = [stamp.date() for stamp in pd.bdate_range("2026-01-02", periods=60)]
    for session in sessions:
        service.catalog.append_shadow_event(
            account_id="value-quality-shadow",
            event_type="account_projected",
            occurred_at=datetime.combine(
                session, datetime.min.time(), tzinfo=timezone.utc
            )
            + timedelta(hours=15),
            payload={},
        )
    weekly_path.write_text(
        json.dumps(
            {
                "snapshot_id": snapshot_id,
                "sleeves": [
                    {
                        "record": {
                            "sleeve_id": "value_quality",
                            "state": "active",
                            "new_sessions_since_dormant": 0,
                        },
                        "shadow_account_id": "value-quality-shadow",
                        "observation": {
                            "as_of_date": sessions[-1].isoformat(),
                            "active_ir_13w": 0.2,
                            "active_ir_26w": 0.2,
                            "ic_26w": 0.02,
                            "active_return_20d": 0.01,
                            "active_return_60d": 0.03,
                            "new_sessions_since_dormant": 0,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = service.execute(
        OperationRequest(
            operation=OperationName.SLEEVE_HEALTH_CHECK,
            cycle=CycleName.WEEKLY,
            partition_key=sessions[-1].isoformat(),
            run_id="weekly-recovery",
        )
    )
    evaluation = result.outputs["evaluations"][0]
    assert evaluation["observation"]["new_sessions_since_dormant"] == 60
    assert evaluation["from_state"] == "dormant"
    assert evaluation["proposed_state"] == "probation"


def test_empty_historical_st_blocks_before_gold(tmp_path: Path) -> None:
    config = _config(tmp_path, empty_st=True)
    publisher = FakeGoldPublisher([])
    service = _services(tmp_path, config, publisher)
    execute_operation(service, _request(OperationName.SOURCE_SYNC))
    execute_operation(service, _request(OperationName.SOURCE_RECONCILIATION))

    quality = service.execute(_request(OperationName.DATA_QUALITY_GATE))
    assert quality.status == "blocked"
    codes = {
        issue["code"] for issue in quality.outputs["quality_report"]["issues"]
    }
    assert "st_history_unverified" in codes
    with pytest.raises(OrchestrationFailure):
        execute_operation(service, _request(OperationName.DATA_QUALITY_GATE))
    assert publisher.calls == []


def test_shadow_step_uses_explicit_prior_snapshot_and_event_ledger(tmp_path: Path) -> None:
    champion_path = tmp_path / "champion.json"
    market_bars = tmp_path / "market_bars.csv"
    prior_id = "a" * 64
    sessions = ("2024-01-02", "2024-01-03")
    calendar_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    pd.DataFrame(
        {
                "ticker": ["000001.SZ"],
                "gold_snapshot_id": [prior_id],
                "trade_date": ["2024-01-03"],
                "execution_event_time": ["2024-01-03T09:30:00+08:00"],
                "execution_available_at": ["2024-01-03T09:30:00+08:00"],
                "mark_event_time": ["2024-01-03T15:00:00+08:00"],
                "mark_available_at": ["2024-01-03T15:01:00+08:00"],
            "open_adj": [10.0],
            "close_adj": [10.2],
            "adv_20": [100_000_000.0],
            "volatility_20": [0.02],
            "is_one_price_limit_up": [False],
            "is_one_price_limit_down": [False],
            "is_suspended": [False],
            "is_delisted": [False],
        }
    ).to_csv(market_bars, index=False)
    champion_path.write_text(
        json.dumps(
            {
                "account_id": "shadow-main",
                "account_name": "Research shadow",
                "initial_capital": 50_000_000,
                "opened_at": "2024-01-01T07:00:00+00:00",
                "decision_date": "2024-01-02",
                "trade_date": "2024-01-03",
                "expected_next_session": "2024-01-03",
                "target_weights": {"000001.SZ": 0.02},
                "market_bars_path": str(market_bars),
                "snapshot_id": prior_id,
                "model_version": "champion.v1",
                "benchmark_return": 0.001,
            }
        ),
        encoding="utf-8",
    )
    config = _config(tmp_path, champion=champion_path)
    publisher = FakeGoldPublisher([])
    service = _services(tmp_path, config, publisher)
    service.catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=prior_id,
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold_daily#prior",
            content_hash=prior_id,
                as_of=datetime(2024, 1, 2, 7, 0, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=("point_in_time",),
            manifest={
                "trading_calendar": {
                    "source": "test-exchange-calendar",
                    "quality_status": "accepted",
                    "sessions": list(sessions),
                    "content_hash": calendar_hash,
                }
            },
        )
    )
    for operation in (
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        OperationName.SHADOW_NAV_STEP,
    ):
        result = execute_operation(service, _request(operation))

    assert result.status == "completed"
    assert result.outputs["snapshot_id"] == prior_id
    assert result.outputs["chain_verified"] is True
    assert service.catalog.verify_shadow_chain("shadow-main") is True


def test_default_shadow_mode_ignores_manual_champion_json(tmp_path: Path) -> None:
    manual = tmp_path / "manual.json"
    manual.write_text(
        json.dumps({"account_id": "must-not-be-created"}), encoding="utf-8"
    )
    config = _config(tmp_path, champion=manual)
    config["daily"]["shadow"].pop("input_mode")
    service = _services(tmp_path, config, FakeGoldPublisher([]))

    result = service.execute(_request(OperationName.SHADOW_NAV_STEP))

    # The production default must neither consume the manual JSON nor silently
    # skip an incompletely configured authoritative account.  Missing durable
    # account identity is a fail-closed configuration error.
    assert result.status == "failed"
    assert "authoritative daily.shadow.account must be configured" in result.summary
    assert service.catalog.get_shadow_account("must-not-be-created") is None


def test_gold_failure_persists_cash_target_but_production_rejects_file_execution(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path, empty_st=True)
    shadow = config["daily"]["shadow"]
    shadow.clear()
    bars_path = tmp_path / "execution-bars.csv"
    shadow.update(
        {
            "input_mode": "authoritative_pg",
            "account": {
                "account_id": "champion-shadow",
                "name": "Champion shadow",
                "initial_capital": 50_000_000,
                "opened_at": "2026-08-20T15:00:00+08:00",
            },
            "market_bars_path": str(bars_path),
        }
    )
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    sessions = ("2026-08-21", "2026-08-24")
    calendar_hash = hashlib.sha256(
        "\n".join(sessions).encode("ascii")
    ).hexdigest()
    snapshot_id = "9" * 64
    service.catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold#accepted-before-failure",
            content_hash=snapshot_id,
            as_of=datetime(2026, 8, 21, 7, 0, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=("point_in_time", "quality_accepted"),
            manifest={
                "trading_calendar": {
                    "source": "test-exchange-calendar",
                    "quality_status": "accepted",
                    "sessions": list(sessions),
                    "content_hash": calendar_hash,
                }
            },
        )
    )
    control = AuthoritativeChampionControl(service.catalog)
    projection = control.build_allocation(
        data_snapshot_id=snapshot_id,
        generated_at=datetime(2026, 8, 21, 8, 0, tzinfo=timezone.utc),
    )
    control.persist_allocation(projection)
    failed_request = OperationRequest(
        operation=OperationName.SHADOW_NAV_STEP,
        cycle=CycleName.DAILY,
        partition_key="2026-08-21",
        run_id="gold-failed-friday",
    )
    for operation in (
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
    ):
        service.execute(
            OperationRequest(
                operation=operation,
                cycle=CycleName.DAILY,
                partition_key="2026-08-21",
                run_id="gold-failed-friday",
            )
        )

    failure_result = service.execute(failed_request)
    assert failure_result.status == "completed"
    target = control.latest_stock_target(decision_date="2026-08-21")
    assert target is not None
    assert target.target_weights == {}
    assert target.cash_weight == 1.0
    assert failure_result.outputs["generation_reason"] == (
        "current_gold_data_failure_all_cash"
    )

    pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "gold_snapshot_id": [snapshot_id],
            "trade_date": ["2026-08-24"],
            "execution_event_time": ["2026-08-24T09:30:00+08:00"],
            "execution_available_at": ["2026-08-24T09:30:00+08:00"],
            "mark_event_time": ["2026-08-24T15:00:00+08:00"],
            "mark_available_at": ["2026-08-24T15:01:00+08:00"],
            "open_adj": [10.0],
            "close_adj": [10.0],
            "adv_20": [100_000_000.0],
            "volatility_20": [0.02],
            "is_one_price_limit_up": [False],
            "is_one_price_limit_down": [False],
            "is_suspended": [False],
            "is_delisted": [False],
        }
    ).to_csv(bars_path, index=False)
    monday = service.execute(
        OperationRequest(
            operation=OperationName.SHADOW_NAV_STEP,
            cycle=CycleName.DAILY,
            partition_key="2026-08-24",
            run_id="execute-all-cash-monday",
        )
    )
    assert monday.status == "failed"
    assert "rejects market_bars_path" in monday.summary
    assert service.catalog.get_shadow_account("champion-shadow") is None


def test_legacy_mode_consumes_explicit_cash_intent_with_trusted_bars(
    tmp_path: Path,
) -> None:
    champion_path = tmp_path / "cash-intent.json"
    market_bars = tmp_path / "cash-intent-bars.csv"
    snapshot_id = "8" * 64
    sessions = ("2024-01-02", "2024-01-03")
    calendar_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "gold_snapshot_id": [snapshot_id],
            "trade_date": ["2024-01-03"],
            "execution_event_time": ["2024-01-03T09:30:00+08:00"],
            "execution_available_at": ["2024-01-03T09:30:00+08:00"],
            "mark_event_time": ["2024-01-03T15:00:00+08:00"],
            "mark_available_at": ["2024-01-03T15:01:00+08:00"],
            "open_adj": [10.0],
            "close_adj": [10.0],
            "adv_20": [100_000_000.0],
            "volatility_20": [0.02],
            "is_one_price_limit_up": [False],
            "is_one_price_limit_down": [False],
            "is_suspended": [False],
            "is_delisted": [False],
        }
    ).to_csv(market_bars, index=False)
    champion_path.write_text(
        json.dumps(
            {
                "account_id": "legacy-cash-shadow",
                "account_name": "Legacy cash intent compatibility",
                "initial_capital": 50_000_000,
                "opened_at": "2024-01-01T07:00:00+00:00",
                "decision_date": "2024-01-02",
                "trade_date": "2024-01-03",
                "expected_next_session": "2024-01-03",
                "target_weights": {},
                "market_bars_path": str(market_bars),
                "snapshot_id": snapshot_id,
                "model_version": "legacy.cash-intent.v1",
                "benchmark_return": 0.0,
            }
        ),
        encoding="utf-8",
    )
    config = _config(tmp_path, champion=champion_path)
    service = _services(tmp_path, config, FakeGoldPublisher([]))
    service.catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier=SnapshotTier.GOLD,
            uri="iceberg://factorlab/factor_lab.gold#legacy-cash-intent",
            content_hash=snapshot_id,
            as_of=datetime(2024, 1, 2, 7, 0, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=("point_in_time",),
            manifest={
                "trading_calendar": {
                    "source": "test-exchange-calendar",
                    "quality_status": "accepted",
                    "sessions": list(sessions),
                    "content_hash": calendar_hash,
                }
            },
        )
    )
    for operation in (
        OperationName.SOURCE_SYNC,
        OperationName.SOURCE_RECONCILIATION,
        OperationName.DATA_QUALITY_GATE,
        OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        OperationName.SHADOW_NAV_STEP,
    ):
        result = execute_operation(service, _request(operation))
    assert result.status == "completed"
    account = service.catalog.get_shadow_account("legacy-cash-shadow")
    assert account is not None
    assert account.cash == pytest.approx(50_000_000)
    assert service.catalog.list_shadow_positions("legacy-cash-shadow") == []
    assert service.catalog.verify_shadow_chain("legacy-cash-shadow") is True


def test_factory_requires_explicit_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ORCHESTRATION_CONFIG_ENV, raising=False)
    with pytest.raises(ServiceNotConfigured, match=ORCHESTRATION_CONFIG_ENV):
        create_services()


class _FakeArrow:
    schema = object()


class _FakeSnapshot:
    def __init__(self, snapshot_id: int, summary: dict | None = None) -> None:
        self.snapshot_id = snapshot_id
        self.summary = summary or {}


class _FakeTagManager:
    def __init__(self, table: "_FakeTable", *, visible: bool = True) -> None:
        self.table = table
        self.visible = visible
        self.snapshot_id = None
        self.tag = None

    def create_tag(self, snapshot_id: int, tag: str) -> "_FakeTagManager":
        self.snapshot_id = snapshot_id
        self.tag = tag
        return self

    def commit(self) -> None:
        if self.visible:
            self.table.metadata.refs[self.tag] = SimpleNamespace(
                snapshot_id=self.snapshot_id
            )


class _FakeTable:
    def __init__(self, *, visible_tag: bool = True) -> None:
        self.metadata = SimpleNamespace(snapshots=[], refs={})
        self.visible_tag = visible_tag
        self.append_count = 0

    def append(self, _arrow, *, snapshot_properties: dict) -> None:
        self.append_count += 1
        self.metadata.snapshots.append(_FakeSnapshot(77, snapshot_properties))

    def current_snapshot(self):
        return self.metadata.snapshots[-1] if self.metadata.snapshots else None

    def manage_snapshots(self) -> _FakeTagManager:
        return _FakeTagManager(self, visible=self.visible_tag)

    def refresh(self) -> None:
        pass


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self.table = table

    def create_namespace_if_not_exists(self, _namespace: str) -> None:
        pass

    def table_exists(self, _identifier: str) -> bool:
        return bool(self.table.metadata.snapshots)

    def load_table(self, _identifier: str) -> _FakeTable:
        return self.table

    def create_table_if_not_exists(self, _identifier: str, *, schema) -> _FakeTable:
        assert schema is _FakeArrow.schema
        return self.table


def test_pyiceberg_publisher_is_idempotent_by_snapshot_key_and_uses_factorlab() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    loaded_names = []
    publisher = PyIcebergGoldPublisher(
        catalog_loader=lambda name: loaded_names.append(name) or catalog,
        arrow_builder=lambda _frame: _FakeArrow(),
    )
    frame = pd.DataFrame({"ticker": ["000001.SZ"], "close_adj": [10.0]})

    first = publisher.publish(
        frame,
        table_identifier="factor_lab.gold_daily",
        tag="ros_key",
        snapshot_key="key",
        partition_key="2024-01-03",
    )
    second = publisher.publish(
        frame,
        table_identifier="factor_lab.gold_daily",
        tag="ros_key",
        snapshot_key="key",
        partition_key="2024-01-03",
    )

    assert loaded_names == ["factorlab", "factorlab"]
    assert first.snapshot_id == second.snapshot_id == 77
    assert first.reused is False and second.reused is True
    assert table.append_count == 1


def test_pyiceberg_tag_must_be_visible_after_commit() -> None:
    table = _FakeTable(visible_tag=False)
    publisher = PyIcebergGoldPublisher(
        catalog_loader=lambda _name: _FakeCatalog(table),
        arrow_builder=lambda _frame: _FakeArrow(),
    )
    with pytest.raises(IcebergPublicationError, match="not visible"):
        publisher.publish(
            pd.DataFrame({"ticker": ["000001.SZ"]}),
            table_identifier="factor_lab.gold_daily",
            tag="ros_key",
            snapshot_key="key",
            partition_key="2024-01-03",
        )
