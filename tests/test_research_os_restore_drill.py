from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from factor_lab.research_os.catalog import ResearchCatalog, RunRecord
from factor_lab.research_os.application_services import ApplicationServices
from factor_lab.research_os.contracts import DataSnapshotRef
from factor_lab.research_os.fingerprint import content_fingerprint
from factor_lab.research_os.object_store import S3ImmutableArchive
from factor_lab.research_os.orchestration import OrchestrationFailure
from factor_lab.research_os.physical_canary import CANARY_OBJECT_PREFIX
from factor_lab.research_os.readiness_audit import (
    PHYSICAL_CANARY_RUN_TYPE,
    PHYSICAL_CANARY_SCHEMA_VERSION,
    RESTORE_DRILL_RUN_TYPE,
    restore_drill_evidence_hash,
    physical_canary_evidence_hash,
)
from factor_lab.research_os.restore_drill import (
    CONTROLLED_TEST_REJECTION,
    CONTROLLED_TEST_RUN_TYPE,
    PhysicalMinioRestoreDrillService,
    RestoreDrillAdmissionError,
    RestoreDrillEvidenceUnavailable,
)


class _MemoryWriter(io.BytesIO):
    def __init__(self, filesystem: "_MemoryFileSystem", path: str) -> None:
        super().__init__()
        self.filesystem = filesystem
        self.path = path

    def close(self) -> None:
        self.filesystem.objects[self.path] = self.getvalue()
        super().close()


class _MemoryFileSystem:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.read_count = 0

    def exists(self, path: str) -> bool:
        return path in self.objects

    def open(self, path: str, mode: str = "rb"):
        if mode == "rb":
            self.read_count += 1
            return io.BytesIO(self.objects[path])
        return _MemoryWriter(self, path)


def _sessions() -> tuple[str, ...]:
    current = datetime(2026, 7, 1, tzinfo=timezone.utc).date()
    values = []
    while len(values) < 21:
        if current.weekday() < 5:
            values.append(current.isoformat())
        current += timedelta(days=1)
    return tuple(values)


def _seed_physical_canary(
    *,
    catalog: ResearchCatalog,
    archive: S3ImmutableArchive,
    tmp_path: Path,
    run_id: str,
    completed_at: datetime,
    corrupt_canary_hash: bool = False,
) -> tuple[RunRecord, str]:
    payload = tmp_path / f"{run_id}.parquet"
    payload.write_bytes((f"physical-gold-mark:{run_id}:" * 128).encode("utf-8"))
    stored = archive.archive_file(
        payload,
        logical_path=f"run={run_id}/tier=gold/role=mark/date=2026-07-29",
    )
    snapshot_id = f"snapshot-{run_id}"
    labels = {
        "evidence_schema": PHYSICAL_CANARY_SCHEMA_VERSION,
        "evidence_class": "engineering_canary",
        "evidence_scope": "non_forward",
        "formal_epoch_eligible": False,
        "physical_source_attested": True,
        "controlled_test_adapter": False,
        "readiness_admission": "physical_engineering_prerequisite",
    }
    manifest = {
        **labels,
        "run_id": run_id,
        "tier": "gold",
        "role": "mark",
        "trade_date": "2026-07-29",
        "parent_snapshot_ids": [],
        "physical_object": stored.to_dict(),
    }
    content_hash = content_fingerprint(
        manifest,
        domain="factor-lab/research-os/v1/physical-canary-snapshot",
    )
    catalog.register_snapshot(
        DataSnapshotRef(
            snapshot_id=snapshot_id,
            tier="gold",
            uri=stored.uri,
            content_hash=content_hash,
            as_of=completed_at,
            quality_status="accepted",
            trust_labels=(
                "physical_engineering_canary",
                "non_forward",
                "retrospective_physical_replay",
            ),
            manifest=manifest,
        )
    )
    input_fingerprint = content_fingerprint(
        {"run_id": run_id},
        domain="tests/physical-canary-input",
    )
    snapshot_evidence = {
        "snapshot_id": snapshot_id,
        "tier": "gold",
        "role": "mark",
        "trade_date": "2026-07-29",
        "uri": stored.uri,
        "content_hash": content_hash,
        "object_sha256": stored.sha256,
        "size_bytes": stored.size_bytes,
    }
    metadata = {
        **labels,
        "run_id": run_id,
        "run_type": PHYSICAL_CANARY_RUN_TYPE,
        "input_fingerprint": input_fingerprint,
        "calendar_sessions": list(_sessions()),
        "security_count": 50,
        "projected_session_count": 20,
        "sleeve_state": "shadow",
        # This remains diagnostic and is deliberately not a restore gate.
        "opening_execution_formal_ready": False,
        "source_probe_hashes": {"tushare:daily": "1" * 64},
        "shadow_session_hashes": [
            content_fingerprint(index, domain="tests/shadow-session")
            for index in range(20)
        ],
        "shadow_account_event_hashes": [
            content_fingerprint(index, domain="tests/shadow-event")
            for index in range(20)
        ],
        "snapshot_evidence": [snapshot_evidence],
        "physical_object_count": 1,
        "bronze_object_count": 0,
        "silver_object_count": 0,
        "gold_object_count": 1,
    }
    metadata["canary_evidence_hash"] = physical_canary_evidence_hash(metadata)
    if corrupt_canary_hash:
        metadata["canary_evidence_hash"] = "0" * 64
    run = RunRecord(
        run_id=run_id,
        run_type=PHYSICAL_CANARY_RUN_TYPE,
        status="succeeded",
        input_fingerprint=input_fingerprint,
        started_at=completed_at - timedelta(minutes=1),
        completed_at=completed_at,
        metadata=metadata,
    )
    catalog.save_run(run)
    return run, f"{archive.bucket}/{stored.key}"


def _runtime(tmp_path: Path):
    catalog = ResearchCatalog(tmp_path / "catalog.db")
    catalog.initialize_schema()
    filesystem = _MemoryFileSystem()
    archive = S3ImmutableArchive(
        bucket="factor-lab",
        filesystem=filesystem,
        prefix=CANARY_OBJECT_PREFIX,
    )
    return catalog, filesystem, archive


def test_controlled_restore_downloads_deletes_and_downloads_again_without_formal_evidence(
    tmp_path: Path,
) -> None:
    catalog, filesystem, archive = _runtime(tmp_path)
    try:
        canary, _ = _seed_physical_canary(
            catalog=catalog,
            archive=archive,
            tmp_path=tmp_path,
            run_id="physical-canary-valid",
            completed_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )
        reads_before = filesystem.read_count
        service = PhysicalMinioRestoreDrillService.for_controlled_test(
            catalog=catalog,
            object_store_archive=archive,
        )

        result = service.run()

        assert filesystem.read_count - reads_before == 2
        assert result.restored_twice is True
        assert result.physical is False
        assert result.run_type == CONTROLLED_TEST_RUN_TYPE
        assert result.readiness_admission == CONTROLLED_TEST_REJECTION
        assert result.source_canary_run_id == canary.run_id
        stored = catalog.get_run(result.run_id)
        assert stored is not None
        assert stored.status == "succeeded"
        assert stored.input_fingerprint == result.restore_evidence_hash
        assert stored.metadata["restore_evidence_hash"] == restore_drill_evidence_hash(
            stored.metadata
        )
        assert stored.metadata["restored_sha256"] == stored.metadata["expected_sha256"]
        assert (
            stored.metadata["restored_size_bytes"]
            == stored.metadata["expected_size_bytes"]
        )
        assert stored.metadata["cache_deleted_before_second_restore"] is True
        assert stored.metadata["second_restore_downloaded"] is True
        assert stored.metadata["local_cache_retained"] is False
        assert catalog.list_runs(
            limit=100, status=None, run_type=RESTORE_DRILL_RUN_TYPE
        ) == []
        encoded = json.dumps(stored.metadata, sort_keys=True)
        assert str(tmp_path) not in encoded
        assert "immutable-object.cache" not in encoded
        assert "TemporaryDirectory" not in encoded
    finally:
        catalog.close()


def test_latest_invalid_canary_is_skipped_for_newest_hash_valid_canary(
    tmp_path: Path,
) -> None:
    catalog, _, archive = _runtime(tmp_path)
    try:
        now = datetime.now(timezone.utc)
        valid, _ = _seed_physical_canary(
            catalog=catalog,
            archive=archive,
            tmp_path=tmp_path,
            run_id="physical-canary-valid-older",
            completed_at=now - timedelta(minutes=10),
        )
        _seed_physical_canary(
            catalog=catalog,
            archive=archive,
            tmp_path=tmp_path,
            run_id="physical-canary-invalid-newer",
            completed_at=now - timedelta(minutes=5),
            corrupt_canary_hash=True,
        )

        result = PhysicalMinioRestoreDrillService.for_controlled_test(
            catalog=catalog,
            object_store_archive=archive,
        ).run()

        assert result.source_canary_run_id == valid.run_id
    finally:
        catalog.close()


def test_remote_corruption_cannot_persist_successful_restore_evidence(
    tmp_path: Path,
) -> None:
    catalog, filesystem, archive = _runtime(tmp_path)
    try:
        _, remote = _seed_physical_canary(
            catalog=catalog,
            archive=archive,
            tmp_path=tmp_path,
            run_id="physical-canary-corrupt-object",
            completed_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )
        filesystem.objects[remote] = b"remote-corruption"

        with pytest.raises(Exception, match="immutable object differs"):
            PhysicalMinioRestoreDrillService.for_controlled_test(
                catalog=catalog,
                object_store_archive=archive,
            ).run()

        assert catalog.list_runs(
            limit=100, status=None, run_type=CONTROLLED_TEST_RUN_TYPE
        ) == []
        assert catalog.list_runs(
            limit=100, status=None, run_type=RESTORE_DRILL_RUN_TYPE
        ) == []
    finally:
        catalog.close()


def test_no_fresh_hash_valid_canary_fails_closed(tmp_path: Path) -> None:
    catalog, _, archive = _runtime(tmp_path)
    try:
        _seed_physical_canary(
            catalog=catalog,
            archive=archive,
            tmp_path=tmp_path,
            run_id="physical-canary-only-invalid",
            completed_at=datetime.now(timezone.utc) - timedelta(minutes=5),
            corrupt_canary_hash=True,
        )

        with pytest.raises(
            RestoreDrillEvidenceUnavailable,
            match="no fresh hash-valid physical canary",
        ):
            PhysicalMinioRestoreDrillService.for_controlled_test(
                catalog=catalog,
                object_store_archive=archive,
            ).run()
    finally:
        catalog.close()


def test_production_mode_rejects_sqlite_and_memory_object_store_before_reads(
    tmp_path: Path,
) -> None:
    catalog, filesystem, archive = _runtime(tmp_path)
    try:
        service = PhysicalMinioRestoreDrillService(
            catalog=catalog,
            object_store_archive=archive,
            production_evidence=object(),  # type: ignore[arg-type]
            controlled_test=False,
        )
        reads_before = filesystem.read_count

        with pytest.raises(RestoreDrillAdmissionError, match="PostgreSQL"):
            service.run()

        assert filesystem.read_count == reads_before
        assert catalog.list_runs(
            limit=100, status=None, run_type=RESTORE_DRILL_RUN_TYPE
        ) == []
    finally:
        catalog.close()


def test_operation_api_has_no_caller_object_or_cache_path_parameters() -> None:
    run_parameters = tuple(inspect.signature(PhysicalMinioRestoreDrillService.run).parameters)
    controlled_parameters = set(
        inspect.signature(
            PhysicalMinioRestoreDrillService.for_controlled_test
        ).parameters
    )

    assert run_parameters == ("self",)
    assert not {"uri", "sha256", "path", "destination", "cache_root"}.intersection(
        controlled_parameters
    )


def test_application_services_bridge_passes_only_authoritative_dependencies(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, _, archive = _runtime(tmp_path)
    calls: dict[str, object] = {}

    class Runner:
        @staticmethod
        def run():
            return {"run_type": RESTORE_DRILL_RUN_TYPE}

    def from_production_config(cls, config_path, **kwargs):
        calls["config_path"] = config_path
        calls.update(kwargs)
        return Runner()

    monkeypatch.setattr(
        PhysicalMinioRestoreDrillService,
        "from_production_config",
        classmethod(from_production_config),
    )
    services = object.__new__(ApplicationServices)
    services.settings = SimpleNamespace(environment="production")
    services._configuration_path = tmp_path / "production.json"
    services.object_store_archive = archive
    services.catalog = catalog
    services.env = {"FACTOR_LAB_ENVIRONMENT": "production"}
    try:
        result = ApplicationServices.run_physical_minio_restore_drill(services)

        assert result == {"run_type": RESTORE_DRILL_RUN_TYPE}
        assert calls == {
            "config_path": services._configuration_path,
            "env": services.env,
            "catalog": catalog,
            "object_store_archive": archive,
        }
        services.settings = SimpleNamespace(environment="local")
        with pytest.raises(OrchestrationFailure, match="production-only"):
            ApplicationServices.run_physical_minio_restore_drill(services)
    finally:
        catalog.close()
