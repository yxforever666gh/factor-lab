from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("sqlalchemy")
from sqlalchemy import create_engine, event
from sqlalchemy.exc import IntegrityError

from factor_lab.research_os.orm import (
    Base,
    RunModel,
    RuntimeAuthorityModel,
    ShadowSessionModel,
)
from factor_lab.research_os.production_ledger import (
    CapabilityRecord,
    CapabilityStatus,
    ImmutablePartition,
    IncidentStage,
    IncidentStatus,
    LeaseConflict,
    PartitionIdentity,
    PartitionStatus,
    ProductionLedger,
    ProductionLedgerError,
    RuntimeAuthorityError,
    load_runtime_authority_marker,
    runtime_authority_marker_hash,
    sanitize_operational_text,
)


NOW = datetime(2026, 8, 22, 15, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "value",
    [
        "Authorization: Bearer bare-marker-123456789",
        "Bearer bare-marker-123456789",
        "{'Authorization': 'Bearer bare-marker-123456789'}",
        '{"api_key":"bare-marker-123456789"}',
        "https://user:bare-marker-123456789@example.invalid/path",
    ],
)
def test_operational_text_sanitizer_removes_common_bearer_and_quoted_secrets(
    value: str,
) -> None:
    cleaned = sanitize_operational_text(value)
    assert "bare-marker-123456789" not in cleaned
    assert "***" in cleaned


@pytest.fixture
def ledger(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'ledger.db'}")
    Base.metadata.create_all(engine)
    with ProductionLedger(engine) as value:
        yield value
    engine.dispose()


def test_partition_registration_claim_and_success_are_idempotent_and_immutable(
    ledger: ProductionLedger,
) -> None:
    identity = PartitionIdentity("tushare", "daily", "2026-08-21")
    first = ledger.ensure_partition(
        identity,
        created_at=NOW,
        input_hash="a" * 64,
        details={"calendar": "accepted"},
    )
    second = ledger.ensure_partition(
        identity,
        created_at=NOW + timedelta(minutes=1),
        input_hash="a" * 64,
    )

    assert first.status is PartitionStatus.PENDING
    assert first.identity == second.identity
    lease = ledger.claim(
        owner="worker-1",
        identity=identity,
        now=NOW + timedelta(minutes=2),
        lease_for=timedelta(minutes=10),
    )
    assert lease is not None
    assert lease.record.attempts == 1
    completed = ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=NOW + timedelta(minutes=3),
        output_hash="b" * 64,
        vendor_revision="vendor-20260821",
    )

    assert completed.status is PartitionStatus.SUCCEEDED
    assert completed.lease_token is None
    assert (
        ledger.claim(
            owner="worker-2",
            identity=identity,
            now=NOW + timedelta(hours=1),
            lease_for=timedelta(minutes=10),
        )
        is None
    )
    with pytest.raises(ImmutablePartition):
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(minutes=4),
            output_hash="b" * 64,
        )


def test_finish_requires_an_existing_parent_run_even_without_sqlite_fk(
    tmp_path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'parent-run.db'}")
    Base.metadata.create_all(engine)
    with engine.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 0

    identity = PartitionIdentity("tushare", "daily", "2026-08-21")
    with ProductionLedger(engine) as ledger:
        ledger.ensure_partition(identity, created_at=NOW, input_hash="a" * 64)
        lease = ledger.claim(
            owner="worker-1",
            identity=identity,
            now=NOW + timedelta(minutes=1),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None

        with pytest.raises(ProductionLedgerError, match="existing parent run"):
            ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW + timedelta(minutes=2),
                run_id="dagster-run-1",
                output_hash="b" * 64,
            )

        unchanged = ledger.get_partition(identity)
        assert unchanged is not None
        assert unchanged.status is PartitionStatus.RUNNING
        assert unchanged.run_id is None
        assert unchanged.lease_token == lease.token

        with engine.begin() as connection:
            connection.execute(
                RunModel.__table__.insert().values(
                    run_id="dagster-run-1",
                    run_type="dagster_source_sync",
                    status="running",
                    input_fingerprint="c" * 64,
                    metadata_json={"orchestrator": "dagster"},
                    error=None,
                    started_at=NOW,
                    completed_at=None,
                )
            )

        completed = ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(minutes=2),
            run_id="dagster-run-1",
            output_hash="b" * 64,
        )
        assert completed.status is PartitionStatus.SUCCEEDED
        assert completed.run_id == "dagster-run-1"
    engine.dispose()


def test_changed_partition_input_hash_fails_closed(ledger: ProductionLedger) -> None:
    identity = PartitionIdentity("tushare", "daily_basic", "2026-08-21")
    ledger.ensure_partition(identity, created_at=NOW, input_hash="a" * 64)
    with pytest.raises(ImmutablePartition, match="input hash"):
        ledger.ensure_partition(
            identity,
            created_at=NOW + timedelta(minutes=1),
            input_hash="b" * 64,
        )


def test_backfill_plan_can_bind_input_hash_once_before_first_lease(
    ledger: ProductionLedger,
) -> None:
    identity = PartitionIdentity("primary-tushare", "daily", "2016-06-01")
    planned = ledger.ensure_partition(
        identity,
        created_at=NOW,
        details={"reason": "missing_2016_prewarm"},
    )
    assert planned.input_hash is None

    bound = ledger.ensure_partition(
        identity,
        created_at=NOW + timedelta(minutes=1),
        input_hash="a" * 64,
    )
    assert bound.input_hash == "a" * 64
    with pytest.raises(ImmutablePartition, match="input hash"):
        ledger.ensure_partition(
            identity,
            created_at=NOW + timedelta(minutes=2),
            input_hash="b" * 64,
        )


def test_expired_or_failed_partition_is_reclaimed_without_repeating_success(
    ledger: ProductionLedger,
) -> None:
    identity = PartitionIdentity("tushare", "adj_factor", "2026-08-21")
    ledger.ensure_partition(identity, created_at=NOW)
    expired = ledger.claim(
        owner="crashed-worker",
        identity=identity,
        now=NOW,
        lease_for=timedelta(minutes=1),
    )
    assert expired is not None
    replacement = ledger.claim(
        owner="recovery-worker",
        identity=identity,
        now=NOW + timedelta(minutes=2),
        lease_for=timedelta(minutes=10),
    )
    assert replacement is not None
    assert replacement.token != expired.token
    assert replacement.record.attempts == 2
    with pytest.raises(LeaseConflict):
        ledger.renew(
            expired,
            now=NOW + timedelta(minutes=2),
            lease_for=timedelta(minutes=1),
        )
    failed = ledger.finish(
        replacement,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=3),
        error_code="provider_timeout",
        error="token=do-not-persist provider timeout",
    )
    assert failed.error == "token=*** provider timeout"
    retry = ledger.claim(
        owner="retry-worker",
        identity=identity,
        now=NOW + timedelta(minutes=4),
        lease_for=timedelta(minutes=10),
    )
    assert retry is not None
    assert retry.record.attempts == 3


def test_partition_progress_and_calendar_trigger_use_only_success(
    ledger: ProductionLedger,
) -> None:
    identities = [
        PartitionIdentity("tushare", "trade_calendar", "2026-08-20"),
        PartitionIdentity("diemeng", "trade_calendar", "2026-08-20"),
        PartitionIdentity("tushare", "trade_calendar", "2026-08-21"),
    ]
    ledger.ensure_partitions(identities, created_at=NOW)
    for index, identity in enumerate(identities[:2]):
        lease = ledger.claim(
            owner=f"worker-{index}",
            identity=identity,
            now=NOW + timedelta(minutes=index),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(minutes=index + 1),
            output_hash=str(index + 1) * 64,
        )

    # Individual vendor success cannot create a dynamic partition.  The
    # reconciliation/DQ stage publishes one canonical calendar acceptance.
    assert ledger.accepted_calendar_partitions() == ()
    accepted = PartitionIdentity(
        "research_os", "accepted_trade_calendar", "2026-08-20"
    )
    ledger.ensure_partition(accepted, created_at=NOW)
    accepted_lease = ledger.claim(
        owner="calendar-reconciler",
        identity=accepted,
        now=NOW + timedelta(minutes=5),
        lease_for=timedelta(minutes=10),
    )
    assert accepted_lease is not None
    ledger.finish(
        accepted_lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=NOW + timedelta(minutes=6),
        output_hash="3" * 64,
    )

    progress = ledger.progress(dataset="trade_calendar")
    assert progress.total == 3
    assert progress.completed == 2
    assert progress.completion_ratio == pytest.approx(2 / 3)
    assert ledger.accepted_calendar_partitions() == ("2026-08-20",)


def test_capability_and_incident_records_are_deterministic_and_redacted(
    ledger: ProductionLedger,
) -> None:
    capability = ledger.upsert_capability(
        CapabilityRecord(
            source_id="tushare",
            dataset="stock_st",
            status=CapabilityStatus.ACCEPTED,
            contract_hash="a" * 64,
            probe_hash="b" * 64,
            fields=("trade_date", "ts_code", "trade_date"),
            detail="bounded real probe passed",
            probed_at=NOW,
        )
    )
    assert capability.fields == ("trade_date", "ts_code")

    first = ledger.record_incident(
        partition_key="2026-08-21",
        stage=IncidentStage.SILVER,
        error_code="reconciliation_disputed",
        message="api_key=private-value price conflict",
        occurred_at=NOW,
        source_ids=("tushare", "diemeng"),
        evidence_hashes=("c" * 64,),
        payload={"field": "close"},
    )
    second = ledger.record_incident(
        partition_key="2026-08-21",
        stage=IncidentStage.SILVER,
        error_code="reconciliation_disputed",
        message="a different presentation is not identity",
        occurred_at=NOW,
        source_ids=("diemeng", "tushare"),
        evidence_hashes=("c" * 64,),
        payload={"field": "close"},
    )
    assert first.incident_id == second.incident_id
    assert first.message == "api_key=*** price conflict"
    resolved = ledger.resolve_incident(
        first.incident_id,
        resolved_at=NOW + timedelta(days=1),
        evidence={"accepted_snapshot_hash": "d" * 64},
    )
    assert resolved.status is IncidentStatus.RESOLVED
    assert resolved.resolution_hash is not None
    assert ledger.list_incidents(status=IncidentStatus.OPEN) == ()


def test_iter_incidents_streams_all_rows_from_one_stably_ordered_query(
    ledger: ProductionLedger,
) -> None:
    incidents = tuple(
        ledger.record_incident(
            partition_key=f"2026-08-{day:02d}",
            stage=IncidentStage.SOURCE,
            error_code="source_probe_failed",
            message="bounded failure",
            occurred_at=NOW + timedelta(minutes=day),
            payload={"sequence": day},
        )
        for day in (20, 21, 22)
    )
    ledger.resolve_incident(
        incidents[1].incident_id,
        resolved_at=NOW + timedelta(days=1),
        evidence={"disposition": "test_resolution"},
    )
    incident_selects = 0

    def count_incident_selects(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        nonlocal incident_selects
        if "FROM ros_data_incidents" in statement:
            incident_selects += 1

    event.listen(ledger.engine, "before_cursor_execute", count_incident_selects)
    try:
        streamed = tuple(ledger.iter_incidents(batch_size=2))
    finally:
        event.remove(
            ledger.engine,
            "before_cursor_execute",
            count_incident_selects,
        )

    assert tuple(item.incident_id for item in streamed) == tuple(
        item.incident_id for item in reversed(incidents)
    )
    assert incident_selects == 1
    assert tuple(
        item.incident_id
        for item in ledger.iter_incidents(status=IncidentStatus.OPEN, batch_size=1)
    ) == (incidents[2].incident_id, incidents[0].incident_id)
    with pytest.raises(ValueError, match="batch_size"):
        ledger.iter_incidents(batch_size=0)
    with pytest.raises(ValueError, match="batch_size"):
        ledger.iter_incidents(batch_size=10_001)


def test_shadow_session_schema_rejects_cross_epoch_or_same_role_snapshots(
    tmp_path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'constraints.db'}")
    Base.metadata.create_all(engine)
    # The full FK fixture is intentionally unnecessary: the CHECK constraint
    # fires before any such row could be treated as evidence.
    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                ShadowSessionModel.__table__.insert().values(
                    account_id="missing",
                    trade_date="2026-08-24",
                    evidence_class="forward",
                    epoch_id=None,
                    evidence_window_hash=None,
                    execution_snapshot_id="same",
                    mark_snapshot_id="same",
                    rebalanced=False,
                    cash=50_000_000,
                    positions_value=0,
                    nav=50_000_000,
                    benchmark_nav=50_000_000,
                    position_count=0,
                    account_event_hash="e" * 64,
                    account_event_sequence=1,
                    session_hash="f" * 64,
                    created_at=NOW,
                )
            )
    engine.dispose()


def test_runtime_authority_marker_is_hash_validated_and_sqlite_is_test_only(
    tmp_path,
) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'authority.db'}")
    Base.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            RuntimeAuthorityModel.__table__.insert().values(
                marker_key="research_os",
                environment="test",
                authority_schema="research-os/runtime-authority/v1",
                marker_hash=runtime_authority_marker_hash(environment="test"),
                installed_at=NOW,
            )
        )

    marker = load_runtime_authority_marker(engine)
    assert marker is not None
    assert marker.environment == "test"
    assert marker.database_dialect == "sqlite"
    assert marker.is_production is False

    with engine.begin() as connection:
        connection.execute(
            RuntimeAuthorityModel.__table__.update().values(marker_hash="a" * 64)
        )
    with pytest.raises(RuntimeAuthorityError, match="hash"):
        load_runtime_authority_marker(engine)
    engine.dispose()


def test_non_postgresql_database_cannot_claim_production_authority(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'forged-production.db'}")
    Base.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(
            RuntimeAuthorityModel.__table__.insert().values(
                marker_key="research_os",
                environment="production",
                authority_schema="research-os/runtime-authority/v1",
                marker_hash=runtime_authority_marker_hash(environment="production"),
                installed_at=NOW,
            )
        )
    with pytest.raises(RuntimeAuthorityError, match="cannot claim production"):
        load_runtime_authority_marker(engine)
    engine.dispose()


def test_runtime_authority_missing_table_is_not_fabricated(tmp_path) -> None:
    engine = create_engine(f"sqlite:///{tmp_path / 'unmigrated.db'}")
    assert load_runtime_authority_marker(engine) is None
    engine.dispose()
