from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, event, text

import factor_lab.research_os.application_services as application_services_module
from factor_lab.research_os.application_services import (
    APPLICATION_SERVICES_SCHEMA_VERSION,
    ApplicationServices,
)
from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.data_quality import sha256_path
from factor_lab.research_os.data_sync import BronzeSyncResult
from factor_lab.research_os.orm import Base
from factor_lab.research_os.orchestration import (
    CycleName,
    OperationName,
    OperationRequest,
    OrchestrationFailure,
)
from factor_lab.research_os.production_ledger import (
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
)
from factor_lab.research_os.runtime import ResearchOSSettings
from factor_lab.research_os.snapshots import build_immutable_snapshot_manifest


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 22, 16, 0, tzinfo=timezone.utc)
HASHES = {
    "code_hash": "1" * 64,
    "dependency_lock_hash": "2" * 64,
    "config_hash": "3" * 64,
    "dirty_patch_hash": "4" * 64,
}


class _UnusedGoldPublisher:
    def publish(self, *_args, **_kwargs):  # pragma: no cover - safety tripwire
        raise AssertionError("calendar bootstrap must not publish Gold")


def _calendar_sources() -> list[dict[str, Any]]:
    payload = json.loads(
        (ROOT / "configs" / "research_os_orchestration.production.json").read_text(
            encoding="utf-8"
        )
    )
    return [
        deepcopy(source)
        for source in payload["daily"]["sources"]
        if source["request"]["dataset"] == "trade_calendar"
    ]


def _service(
    root: Path,
    ledger: ProductionLedger,
    *,
    catalog_name: str = "catalog.db",
    now=None,
) -> ApplicationServices:
    config = {
        "schema_version": APPLICATION_SERVICES_SCHEMA_VERSION,
        "repository": str(root),
        "path_base": str(root),
        "iceberg": {"catalog_name": "factorlab"},
        "daily": {"sources": _calendar_sources()},
    }
    settings = ResearchOSSettings(
        database_url=f"sqlite:///{root / catalog_name}",
        lake_root=root / "lake",
        snapshot_root=root / "snapshots",
        environment="test",
    )
    service = ApplicationServices(
        config,
        settings=settings,
        catalog=ResearchCatalog(settings.database_url),
        iceberg_publisher=_UnusedGoldPublisher(),
        config_base=root,
        environment_hashes_override=HASHES,
        now=now or (lambda: NOW),
    )
    # ApplicationServices intentionally creates this only for PostgreSQL.  The
    # SQLAlchemy SQLite ledger exercises the identical fresh/terminal/retry
    # state machine without requiring a network database in unit tests.
    service.production_ledger = ledger
    return service


def _calendar_frames() -> dict[str, pd.DataFrame]:
    return {
        "tushare": pd.DataFrame(
            {
                "exchange": ["SSE", "SSE", "SSE"],
                "cal_date": [
                    date(2016, 6, 1),
                    date(2016, 6, 2),
                    date(2016, 6, 3),
                ],
                "is_open": [1, 0, 1],
                "pretrade_date": [None, date(2016, 6, 1), date(2016, 6, 1)],
            }
        ),
        "diemeng": pd.DataFrame(
            {
                "exchange": ["SSE", "SSE", "SSE"],
                "cal_date": [
                    date(2016, 6, 1),
                    date(2016, 6, 2),
                    date(2016, 6, 3),
                ],
                "is_open": [1, 0, 1],
            }
        ),
    }


def _install_fake_sync(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    frames: dict[str, pd.DataFrame],
    *,
    provider_state: dict[str, Any] | None = None,
) -> list[str]:
    calls: list[str] = []
    bronze_root = root / "lake" / "fixture-bronze"
    bronze_root.mkdir(parents=True, exist_ok=True)

    def fake_sync(source: dict[str, Any], **_kwargs) -> BronzeSyncResult:
        source_id = str(source["source"])
        calls.append(source_id)
        sequence = len(calls)
        observed_at = (
            NOW
            if provider_state is None
            else provider_state.get("ingested_at", NOW)
        )
        if not isinstance(observed_at, datetime):
            raise TypeError("provider_state.ingested_at must be a datetime")
        revision = (
            f"fixture-{source_id}"
            if provider_state is None
            else str(
                (provider_state.get("revisions") or {}).get(
                    source_id, f"fixture-{source_id}"
                )
            )
        )
        data_path = bronze_root / f"bronze-{sequence}-{source_id}.parquet"
        metadata_path = bronze_root / f"bronze-{sequence}-{source_id}.json"
        frames[source_id].to_parquet(data_path, index=False)
        digest = sha256_path(data_path)
        metadata = {
            "source_id": source_id,
            "source_priority": int(source["priority"]),
            "dataset": "trade_calendar",
            "ingested_at": observed_at.isoformat(),
            "vendor_revision": revision,
            "data_sha256": digest,
            "contract": source["contract"],
            "request": source["request"],
            "lineage": {"fixture": True},
        }
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, default=str), encoding="utf-8"
        )
        return BronzeSyncResult(
            source_id=source_id,
            dataset="trade_calendar",
            rows=len(frames[source_id]),
            data_path=str(data_path),
            metadata_path=str(metadata_path),
            sha256=digest,
            vendor_revision=revision,
            ingested_at=observed_at.isoformat(),
            probe_latency_ms=1.0,
        )

    monkeypatch.setattr(application_services_module, "sync_bronze", fake_sync)
    return calls


def _ledger(tmp_path: Path):
    engine = create_engine(f"sqlite:///{tmp_path / 'catalog.db'}")

    @event.listens_for(engine, "connect")
    def enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(engine)
    return engine, ProductionLedger(engine)


def _shared_fk_ledger(tmp_path: Path):
    engine, ledger = _ledger(tmp_path)
    return "catalog.db", engine, ledger


def _bootstrap_bridge(
    service: ApplicationServices,
    ledger: ProductionLedger,
    *,
    through: str,
):
    partition = ledger.get_partition(
        PartitionIdentity("research_os", "bootstrap_trade_calendar", through)
    )
    assert partition is not None
    assert partition.run_id is not None
    bridge = service.catalog.get_run(partition.run_id)
    assert bridge is not None
    return bridge


def _silver_calendar_path(service: ApplicationServices, reference) -> Path:
    matches = [
        item
        for item in reference.manifest["files"]
        if Path(str(item["path"])).name == "accepted_calendar_silver.parquet"
    ]
    assert len(matches) == 1
    return service.settings.lake_root / Path(*str(matches[0]["path"]).split("/"))


def _crash_after_first_calendar_child(
    monkeypatch: pytest.MonkeyPatch,
    service: ApplicationServices,
    ledger: ProductionLedger,
) -> Any:
    original_finish = ledger.finish
    injected = {"raised": False}

    def crash_once(lease, **kwargs):
        if (
            lease.identity.dataset == "accepted_trade_calendar"
            and not injected["raised"]
        ):
            injected["raised"] = True
            raise RuntimeError("injected crash after Silver publication")
        return original_finish(lease, **kwargs)

    monkeypatch.setattr(ledger, "finish", crash_once)
    with pytest.raises(RuntimeError, match="injected crash"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id="calendar-crash-before-child-finish",
        )
    monkeypatch.setattr(ledger, "finish", original_finish)
    assert injected["raised"] is True
    snapshots = service.catalog.list_snapshots(
        limit=100,
        quality_status="accepted",
        tier="silver",
    )
    assert len(snapshots) == 1
    return snapshots[0].reference


def test_fresh_ledger_persists_only_dual_source_open_sessions_and_restart_reuses_terminal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    frames = _calendar_frames()
    calls = _install_fake_sync(monkeypatch, tmp_path, frames)
    service = _service(tmp_path, ledger)

    first = service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id="calendar-bootstrap-1",
    )

    assert [trigger.partition_key for trigger in first.triggers] == [
        "2016-06-01",
        "2016-06-03",
    ]
    assert ledger.accepted_calendar_partitions() == (
        "2016-06-01",
        "2016-06-03",
    )
    assert calls == ["diemeng", "tushare"]
    bridged = _bootstrap_bridge(service, ledger, through="2016-06-03")
    assert bridged.run_id.startswith("roscal_")
    assert bridged.run_type == "dagster_calendar_bootstrap"
    assert bridged.status == "succeeded"
    assert bridged.metadata["accepted_session_count"] == 2
    assert bridged.metadata["dagster_run_ids"] == ["calendar-bootstrap-1"]
    assert service.catalog.get_run("calendar-bootstrap-1") is None
    for partition_key in ("2016-06-01", "2016-06-03"):
        accepted = ledger.get_partition(
            PartitionIdentity(
                "research_os", "accepted_trade_calendar", partition_key
            )
        )
        assert accepted is not None
        assert accepted.run_id == bridged.run_id

    restarted = _service(tmp_path, ProductionLedger(engine), catalog_name="catalog.db")
    second = restarted.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id="calendar-bootstrap-after-restart",
    )

    assert [trigger.partition_key for trigger in second.triggers] == [
        "2016-06-01",
        "2016-06-03",
    ]
    assert calls == ["diemeng", "tushare"]
    restarted_bridge = _bootstrap_bridge(
        restarted, ProductionLedger(engine), through="2016-06-03"
    )
    assert restarted_bridge.run_id == bridged.run_id
    # A completed attempt is immutable.  A later Dagster observation reuses
    # the terminal partition without rewriting its historical run metadata.
    assert restarted_bridge.metadata["dagster_run_ids"] == [
        "calendar-bootstrap-1"
    ]
    assert restarted_bridge == bridged
    engine.dispose()


def test_crash_retry_reuses_bound_silver_when_provider_time_and_revision_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    clock = {"now": NOW}
    provider_state = {
        "ingested_at": NOW,
        "revisions": {"tushare": "tushare-v1", "diemeng": "diemeng-v1"},
    }
    calls = _install_fake_sync(
        monkeypatch,
        tmp_path,
        _calendar_frames(),
        provider_state=provider_state,
    )
    service = _service(tmp_path, ledger, now=lambda: clock["now"])
    snapshot = _crash_after_first_calendar_child(monkeypatch, service, ledger)
    silver_path = _silver_calendar_path(service, snapshot)
    original_bytes = silver_path.read_bytes()
    original_snapshot_ids = {
        item.reference.snapshot_id
        for item in service.catalog.list_snapshots(limit=100, tier="silver")
    }
    assert calls == ["diemeng", "tushare"]

    # A fresh provider call would produce different Bronze identities because
    # both observed time and vendor revision changed. Recovery must finish the
    # child-hash-bound attempt instead of silently substituting that new data.
    clock["now"] = NOW + pd.Timedelta(hours=2)
    provider_state["ingested_at"] = clock["now"]
    provider_state["revisions"] = {
        "tushare": "tushare-v2",
        "diemeng": "diemeng-v2",
    }
    recovered = service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id="calendar-crash-retry-new-provider-revision",
    )

    assert [item.partition_key for item in recovered.triggers] == [
        "2016-06-01",
        "2016-06-03",
    ]
    assert calls == ["diemeng", "tushare"]
    assert silver_path.read_bytes() == original_bytes
    assert {
        item.reference.snapshot_id
        for item in service.catalog.list_snapshots(limit=100, tier="silver")
    } == original_snapshot_ids
    bootstrap = ledger.get_partition(
        PartitionIdentity(
            "research_os", "bootstrap_trade_calendar", "2016-06-03"
        )
    )
    assert bootstrap is not None
    assert bootstrap.status is PartitionStatus.SUCCEEDED
    assert bootstrap.output_snapshot_id == snapshot.snapshot_id
    bridge = _bootstrap_bridge(service, ledger, through="2016-06-03")
    assert bridge.metadata["reused_failed_attempt"] is True
    assert bridge.metadata["attempt_generation"] == 2
    engine.dispose()


def test_retry_reuses_terminal_child_from_failed_prior_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger)
    original_finish = ledger.finish
    injected = {"raised": False}

    def finish_first_child_then_crash(lease, **kwargs):
        result = original_finish(lease, **kwargs)
        if (
            lease.identity.dataset == "accepted_trade_calendar"
            and not injected["raised"]
        ):
            injected["raised"] = True
            raise RuntimeError("injected crash after first terminal child")
        return result

    monkeypatch.setattr(ledger, "finish", finish_first_child_then_crash)
    with pytest.raises(RuntimeError, match="after first terminal child"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id="calendar-partial-child-attempt-1",
        )
    monkeypatch.setattr(ledger, "finish", original_finish)
    failed_bridge = _bootstrap_bridge(service, ledger, through="2016-06-03")
    assert failed_bridge.status == "failed"
    first_child = ledger.get_partition(
        PartitionIdentity(
            "research_os", "accepted_trade_calendar", "2016-06-01"
        )
    )
    assert first_child is not None
    assert first_child.status is PartitionStatus.SUCCEEDED
    assert first_child.run_id == failed_bridge.run_id

    recovered = service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id="calendar-partial-child-attempt-2",
    )

    assert [item.partition_key for item in recovered.triggers] == [
        "2016-06-01",
        "2016-06-03",
    ]
    succeeded_bridge = _bootstrap_bridge(service, ledger, through="2016-06-03")
    assert succeeded_bridge.status == "succeeded"
    assert succeeded_bridge.run_id != failed_bridge.run_id
    retained_first_child = ledger.get_partition(first_child.identity)
    assert retained_first_child == first_child
    second_child = ledger.get_partition(
        PartitionIdentity(
            "research_os", "accepted_trade_calendar", "2016-06-03"
        )
    )
    assert second_child is not None
    assert second_child.status is PartitionStatus.SUCCEEDED
    assert second_child.run_id == succeeded_bridge.run_id
    engine.dispose()


def test_prebinding_crash_retry_isolates_new_provider_revision_by_attempt_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    clock = {"now": NOW}
    provider_state = {
        "ingested_at": NOW,
        "revisions": {"tushare": "tushare-v1", "diemeng": "diemeng-v1"},
    }
    calls = _install_fake_sync(
        monkeypatch,
        tmp_path,
        _calendar_frames(),
        provider_state=provider_state,
    )
    service = _service(tmp_path, ledger, now=lambda: clock["now"])
    original_ensure = ledger.ensure_partition
    injected = {"raised": False}

    def crash_before_binding(identity, **kwargs):
        if (
            identity.dataset == "accepted_trade_calendar"
            and not injected["raised"]
        ):
            injected["raised"] = True
            raise RuntimeError("injected crash before child binding")
        return original_ensure(identity, **kwargs)

    monkeypatch.setattr(ledger, "ensure_partition", crash_before_binding)
    with pytest.raises(RuntimeError, match="before child binding"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id="calendar-prebinding-crash",
        )
    monkeypatch.setattr(ledger, "ensure_partition", original_ensure)
    first_snapshot = service.catalog.list_snapshots(
        limit=100, quality_status="accepted", tier="silver"
    )[0].reference
    first_path = _silver_calendar_path(service, first_snapshot)
    first_bytes = first_path.read_bytes()

    clock["now"] = NOW + pd.Timedelta(hours=2)
    provider_state["ingested_at"] = clock["now"]
    provider_state["revisions"] = {
        "tushare": "tushare-v2",
        "diemeng": "diemeng-v2",
    }
    retried = service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id="calendar-prebinding-retry",
    )

    assert [item.partition_key for item in retried.triggers] == [
        "2016-06-01",
        "2016-06-03",
    ]
    assert calls == ["diemeng", "tushare", "diemeng", "tushare"]
    snapshots = service.catalog.list_snapshots(
        limit=100, quality_status="accepted", tier="silver"
    )
    assert len(snapshots) == 2
    paths = {_silver_calendar_path(service, item.reference) for item in snapshots}
    assert len(paths) == 2
    assert first_path.read_bytes() == first_bytes
    assert all(path.is_file() for path in paths)
    assert len({path.parent.name for path in paths}) == 2
    engine.dispose()


def test_calendar_recovery_fails_closed_when_bound_manifest_file_is_tampered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    clock = {"now": NOW}
    calls = _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger, now=lambda: clock["now"])
    snapshot = _crash_after_first_calendar_child(monkeypatch, service, ledger)
    silver_path = _silver_calendar_path(service, snapshot)
    silver_path.write_bytes(silver_path.read_bytes() + b"tampered")
    clock["now"] = NOW + pd.Timedelta(hours=2)

    with pytest.raises(OrchestrationFailure, match="manifest or file hash"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id="calendar-tampered-retry",
        )

    assert calls == ["diemeng", "tushare"]
    assert ledger.accepted_calendar_partitions() == ()
    engine.dispose()


@pytest.mark.parametrize("binding_target", ["silver", "bronze_parent"])
def test_calendar_recovery_rejects_another_valid_manifest_inside_catalog_reference(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    binding_target: str,
) -> None:
    engine, ledger = _ledger(tmp_path)
    clock = {"now": NOW}
    calls = _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger, now=lambda: clock["now"])
    silver = _crash_after_first_calendar_child(monkeypatch, service, ledger)
    target = silver
    if binding_target == "bronze_parent":
        parent = service.catalog.get_snapshot(silver.parent_snapshot_ids[0])
        assert parent is not None
        target = parent.reference

    alternate_root = (
        service.settings.lake_root / "_orchestration" / "valid-manifest-swap"
        / binding_target
    )
    alternate_paths = []
    for index, entry in enumerate(target.manifest["files"]):
        source = service.settings.lake_root / Path(*str(entry["path"]).split("/"))
        destination = alternate_root / f"{index}-{source.name}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        alternate_paths.append(destination)
    alternate = build_immutable_snapshot_manifest(
        alternate_paths,
        base_dir=service.settings.lake_root,
        tier=target.tier.value,
        as_of=target.as_of,
        parent_snapshot_ids=target.parent_snapshot_ids,
        environment_hashes=target.manifest["environment_hashes"],
        quality_report={"status": "pass"},
        trust_labels=target.trust_labels,
        trading_calendar=target.manifest.get("trading_calendar"),
    )
    assert alternate.snapshot_id != target.snapshot_id
    payload = target.model_dump(mode="json")
    payload["manifest"] = alternate.to_dict()
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE ros_data_snapshots SET ref_json = :ref_json "
                "WHERE snapshot_id = :snapshot_id"
            ),
            {
                "ref_json": json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "snapshot_id": target.snapshot_id,
            },
        )
    clock["now"] = NOW + pd.Timedelta(hours=2)

    with pytest.raises(OrchestrationFailure, match="manifest/reference binding"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id=f"calendar-valid-manifest-swap-{binding_target}",
        )

    assert calls == ["diemeng", "tushare"]
    assert ledger.accepted_calendar_partitions() == ()
    engine.dispose()


def test_calendar_recovery_fails_closed_without_a_unique_bound_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    calls = _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger)
    ledger.ensure_partition(
        PartitionIdentity(
            "research_os", "accepted_trade_calendar", "2016-06-01"
        ),
        created_at=NOW,
        input_hash="f" * 64,
    )

    with pytest.raises(OrchestrationFailure, match="one unique verified Silver"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id="calendar-no-bound-snapshot",
        )

    assert calls == []
    assert service.catalog.list_snapshots(limit=100, tier="silver") == []
    engine.dispose()


def test_calendar_recovery_fails_closed_when_child_input_hash_is_corrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    clock = {"now": NOW}
    calls = _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger, now=lambda: clock["now"])
    snapshot = _crash_after_first_calendar_child(monkeypatch, service, ledger)
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE ros_partition_runs SET input_hash = :input_hash "
                "WHERE source_id = 'research_os' "
                "AND dataset = 'accepted_trade_calendar' "
                "AND partition_key = '2016-06-01'"
            ),
            {"input_hash": "e" * 64},
        )
    clock["now"] = NOW + pd.Timedelta(hours=2)

    with pytest.raises(OrchestrationFailure, match="one unique verified Silver"):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id="calendar-corrupt-child-hash",
        )

    assert calls == ["diemeng", "tushare"]
    assert service.catalog.get_snapshot(snapshot.snapshot_id) is not None
    assert ledger.accepted_calendar_partitions() == ()
    engine.dispose()


def test_calendar_bootstrap_uses_internal_run_with_foreign_keys_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_name, engine, ledger = _shared_fk_ledger(tmp_path)
    _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger, catalog_name=database_name)

    service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id="external-dagster-uuid",
    )

    bridge = _bootstrap_bridge(service, ledger, through="2016-06-03")
    with engine.connect() as connection:
        assert connection.scalar(text("PRAGMA foreign_keys")) == 1
        rows = connection.execute(
            text(
                "SELECT p.run_id, r.run_type "
                "FROM ros_partition_runs AS p "
                "LEFT JOIN ros_runs AS r ON r.run_id = p.run_id "
                "WHERE p.run_id IS NOT NULL"
            )
        ).all()
    assert len(rows) == 3
    assert {row.run_id for row in rows} == {bridge.run_id}
    assert {row.run_type for row in rows} == {"dagster_calendar_bootstrap"}
    assert service.catalog.get_run("external-dagster-uuid") is None
    service.catalog.close()
    engine.dispose()


def test_daily_source_stage_and_vendor_partitions_reference_rosop_run_with_fk_enabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_name, engine, ledger = _shared_fk_ledger(tmp_path)
    _install_fake_sync(monkeypatch, tmp_path, _calendar_frames())
    service = _service(tmp_path, ledger, catalog_name=database_name)
    request = OperationRequest(
        operation=OperationName.SOURCE_SYNC,
        cycle=CycleName.DAILY,
        partition_key="2016-06-03",
        run_id="external-dagster-daily-uuid",
    )

    result = service._execute_admitted(request)

    assert result.status == "completed"
    internal_run_id = service._operation_run_id(request)
    assert internal_run_id.startswith("rosop_")
    internal_run = service.catalog.get_run(internal_run_id)
    assert internal_run is not None and internal_run.status == "completed"
    assert internal_run.metadata["dagster_run_id"] == request.run_id
    assert service.catalog.get_run(request.run_id) is None
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                "SELECT p.dataset, p.run_id, r.run_type "
                "FROM ros_partition_runs AS p "
                "LEFT JOIN ros_runs AS r ON r.run_id = p.run_id "
                "WHERE p.partition_key = :partition_key AND p.run_id IS NOT NULL"
            ),
            {"partition_key": request.partition_key},
        ).all()
    assert len(rows) == 3
    assert {row.run_id for row in rows} == {internal_run_id}
    assert {row.run_type for row in rows} == {
        "dagster:daily:source_sync"
    }
    assert {row.dataset for row in rows} == {"stage_source", "trade_calendar"}
    service.catalog.close()
    engine.dispose()


def test_future_calendar_silver_manifest_is_content_addressed_and_activates_epoch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, ledger = _ledger(tmp_path)
    future_dates = (
        date(2026, 8, 24),
        date(2026, 8, 25),
        date(2026, 8, 26),
    )
    frames = {
        "tushare": pd.DataFrame(
            {
                "exchange": ["SSE"] * 3,
                "cal_date": future_dates,
                "is_open": [1, 1, 1],
                "pretrade_date": [None, *future_dates[:2]],
            }
        ),
        "diemeng": pd.DataFrame(
            {
                "exchange": ["SSE"] * 3,
                "cal_date": future_dates,
                "is_open": [1, 1, 1],
            }
        ),
    }
    _install_fake_sync(monkeypatch, tmp_path, frames)
    service = _service(tmp_path, ledger)
    service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start=future_dates[0],
        through=future_dates[-1],
        dagster_run_id="future-calendar-bootstrap",
    )

    bootstrap = ledger.get_partition(
        PartitionIdentity(
            "research_os",
            "bootstrap_trade_calendar",
            future_dates[-1].isoformat(),
        )
    )
    assert bootstrap is not None and bootstrap.output_snapshot_id
    snapshot = service.catalog.get_snapshot(bootstrap.output_snapshot_id)
    assert snapshot is not None
    calendar = snapshot.reference.manifest["trading_calendar"]
    sessions = tuple(item.isoformat() for item in future_dates)
    expected_hash = hashlib.sha256("\n".join(sessions).encode("ascii")).hexdigest()
    assert snapshot.reference.tier.value == "silver"
    assert tuple(calendar["sessions"]) == sessions
    assert calendar["content_hash"] == expected_hash
    assert calendar["quality_status"] == "accepted"
    assert calendar["source"] == "dual_source_reconciled:SSE"

    frozen = service.catalog.freeze_evidence_epoch(
        architecture_version="calendar-bootstrap-v1",
        code_hash="1" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
        frozen_at=NOW,
    )
    activated = service.catalog.activate_evidence_epoch(
        calendar_snapshot_id=snapshot.reference.snapshot_id,
        first_forward_session=future_dates[0],
        activated_at=NOW + pd.Timedelta(minutes=1),
    )
    assert activated.epoch_id == frozen.epoch_id
    assert activated.calendar_snapshot_id == snapshot.reference.snapshot_id
    assert activated.calendar_content_hash == expected_hash
    assert activated.first_forward_session == future_dates[0]
    engine.dispose()


@pytest.mark.parametrize("conflict", ["date_set", "open_flag"])
def test_calendar_disagreement_is_failed_with_no_accepted_sessions_then_retryable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conflict: str
) -> None:
    engine, ledger = _ledger(tmp_path)
    frames = _calendar_frames()
    if conflict == "date_set":
        frames["diemeng"] = frames["diemeng"].iloc[:-1].copy()
    else:
        frames["diemeng"].loc[0, "is_open"] = 0
    _install_fake_sync(monkeypatch, tmp_path, frames)
    service = _service(tmp_path, ledger)
    identity = PartitionIdentity(
        "research_os", "bootstrap_trade_calendar", "2016-06-03"
    )

    with pytest.raises(OrchestrationFailure):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id=f"calendar-conflict-{conflict}",
        )

    assert ledger.get_partition(identity).status is PartitionStatus.FAILED
    bridged = _bootstrap_bridge(service, ledger, through="2016-06-03")
    assert bridged.status == "failed"
    assert bridged.metadata["failure_type"] == "OrchestrationFailure"
    assert ledger.accepted_calendar_partitions() == ()

    frames.update(_calendar_frames())
    retried = service.bootstrap_accepted_calendar(
        exchange="SSE",
        source_start="2016-06-01",
        through="2016-06-03",
        dagster_run_id=f"calendar-retry-{conflict}",
    )
    assert [trigger.partition_key for trigger in retried.triggers] == [
        "2016-06-01",
        "2016-06-03",
    ]
    recovered_bridge = _bootstrap_bridge(service, ledger, through="2016-06-03")
    assert recovered_bridge.run_id != bridged.run_id
    assert recovered_bridge.status == "succeeded"
    assert bridged.status == "failed"
    assert bridged.metadata["attempt_generation"] == 1
    assert recovered_bridge.metadata["attempt_generation"] == 2
    assert recovered_bridge.metadata["dagster_run_ids"] == [
        f"calendar-retry-{conflict}"
    ]
    attempts = service.catalog.list_runs(
        limit=10, run_type="dagster_calendar_bootstrap"
    )
    assert {item.run_id for item in attempts} == {
        bridged.run_id,
        recovered_bridge.run_id,
    }
    assert {item.status for item in attempts} == {"failed", "succeeded"}
    engine.dispose()


@pytest.mark.parametrize("malformation", ["empty", "garbled_date", "missing_column"])
def test_calendar_empty_garbled_or_missing_required_column_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, malformation: str
) -> None:
    engine, ledger = _ledger(tmp_path)
    frames = _calendar_frames()
    if malformation == "empty":
        frames["diemeng"] = frames["diemeng"].iloc[0:0].copy()
    elif malformation == "garbled_date":
        frames["diemeng"].loc[0, "cal_date"] = "\ufffd\ufffd-invalid-date"
    else:
        frames["diemeng"] = frames["diemeng"].drop(columns=["is_open"])
    _install_fake_sync(monkeypatch, tmp_path, frames)
    service = _service(tmp_path, ledger)
    identity = PartitionIdentity(
        "research_os", "bootstrap_trade_calendar", "2016-06-03"
    )

    with pytest.raises(Exception):
        service.bootstrap_accepted_calendar(
            exchange="SSE",
            source_start="2016-06-01",
            through="2016-06-03",
            dagster_run_id=f"calendar-malformed-{malformation}",
        )

    assert ledger.get_partition(identity).status is PartitionStatus.FAILED
    assert ledger.accepted_calendar_partitions() == ()
    engine.dispose()
