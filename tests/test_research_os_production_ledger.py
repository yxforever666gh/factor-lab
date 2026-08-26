from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier

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


def test_expired_lease_is_reclaimed_but_failed_terminal_uses_successor(
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
    assert (
        ledger.claim(
            owner="forbidden-in-place-retry",
            identity=identity,
            now=NOW + timedelta(minutes=4),
            lease_for=timedelta(minutes=10),
        )
        is None
    )
    authority = ledger.reserve_retry_successor(
        identity,
        repair_fingerprint="f" * 64,
        created_at=NOW + timedelta(minutes=4),
    )
    retry = ledger.claim(
        owner="retry-worker",
        identity=authority.identity,
        now=NOW + timedelta(minutes=4),
        lease_for=timedelta(minutes=10),
    )
    assert retry is not None
    assert retry.record.attempts == 1
    unchanged = ledger.get_partition(identity)
    assert unchanged is not None
    assert unchanged.status is PartitionStatus.FAILED
    assert unchanged.error == "token=*** provider timeout"


def test_failed_retry_successor_can_chain_without_overwriting_prior_terminal(
    ledger: ProductionLedger,
) -> None:
    identity = PartitionIdentity("tushare", "daily", "2026-08-22")
    ledger.ensure_partition(identity, created_at=NOW)
    base_lease = ledger.claim(
        owner="base-worker",
        identity=identity,
        now=NOW,
        lease_for=timedelta(minutes=10),
    )
    assert base_lease is not None
    ledger.finish(
        base_lease,
        status=PartitionStatus.QUARANTINED,
        completed_at=NOW + timedelta(minutes=1),
        error_code="source_disputed",
        error="source conflict",
    )
    first = ledger.reserve_retry_successor(
        identity,
        repair_fingerprint="a" * 64,
        created_at=NOW + timedelta(minutes=2),
    )
    first_lease = ledger.claim(
        owner="repair-one",
        identity=first.identity,
        now=NOW + timedelta(minutes=2),
        lease_for=timedelta(minutes=10),
    )
    assert first_lease is not None
    ledger.finish(
        first_lease,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=3),
        error_code="transient_retry_failure",
        error="retry failed",
    )

    second = ledger.reserve_retry_successor(
        identity,
        repair_fingerprint="b" * 64,
        created_at=NOW + timedelta(minutes=4),
    )
    repeated = ledger.reserve_retry_successor(
        identity,
        repair_fingerprint="b" * 64,
        created_at=NOW + timedelta(minutes=4),
    )

    assert second.authority_id == repeated.authority_id
    assert second.identity != first.identity
    assert second.parent_partition_run_id == first.identity.partition_run_id
    assert ledger.get_partition(first.identity).status is PartitionStatus.FAILED


def test_incident_repair_authority_is_concurrent_idempotent_and_five_stage(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-23"
    base = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(base, created_at=NOW)
    base_lease = ledger.claim(
        owner="failed-source",
        identity=base,
        now=NOW,
        lease_for=timedelta(minutes=10),
    )
    assert base_lease is not None
    ledger.finish(
        base_lease,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=1),
        error_code="source_failed",
        error="source failed",
    )
    incident = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SOURCE,
        error_code="source_failed",
        message="source failed",
        occurred_at=NOW + timedelta(minutes=1),
        partition_run_id=base.partition_run_id,
    )
    barrier = Barrier(2)

    def reserve_source(_index: int):
        barrier.wait(timeout=5)
        return ledger.reserve_repair_successor(
            incident_id=incident.incident_id,
            dataset="stage_source",
            repair_fingerprint="c" * 64,
            created_at=NOW + timedelta(minutes=2),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        reservations = tuple(pool.map(reserve_source, range(2)))
    assert reservations[0].authority_id == reservations[1].authority_id
    first_source = reservations[0]
    first_lease = ledger.claim(
        owner="source-repair-one",
        identity=first_source.identity,
        now=NOW + timedelta(minutes=2),
        lease_for=timedelta(minutes=10),
    )
    assert first_lease is not None
    ledger.finish(
        first_lease,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=3),
        error_code="repair_failed",
        error="first repair failed",
    )
    selected_source = ledger.reserve_repair_successor(
        incident_id=incident.incident_id,
        dataset="stage_source",
        repair_fingerprint="c" * 64,
        created_at=NOW + timedelta(minutes=4),
    )
    assert selected_source.parent_partition_run_id == first_source.identity.partition_run_id

    datasets = (
        "stage_source",
        "stage_silver",
        "stage_data_quality",
        "stage_gold",
        "stage_shadow",
    )
    selected = selected_source
    for index, dataset in enumerate(datasets):
        if index:
            selected = ledger.reserve_repair_successor(
                incident_id=incident.incident_id,
                dataset=dataset,
                repair_fingerprint="c" * 64,
                created_at=NOW + timedelta(minutes=4 + index * 2),
            )
        lease = ledger.claim(
            owner=f"repair-{dataset}",
            identity=selected.identity,
            now=NOW + timedelta(minutes=4 + index * 2),
            lease_for=timedelta(minutes=10),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(minutes=5 + index * 2),
            output_hash=str(index + 1) * 64,
        )

    source_chain = ledger.get_repair_chain(
        incident.incident_id, "stage_source"
    )
    assert len(source_chain) == 2
    assert source_chain[0].parent_partition_run_id == base.partition_run_id
    assert source_chain[1].parent_partition_run_id == source_chain[0].identity.partition_run_id
    previous_leaf = source_chain[-1]
    for dataset in datasets[1:]:
        chain = ledger.get_repair_chain(incident.incident_id, dataset)
        assert len(chain) == 1
        assert chain[0].repair_fingerprint == "c" * 64
        assert chain[0].parent_partition_run_id == previous_leaf.identity.partition_run_id
        previous_leaf = chain[0]


def test_shadow_revalidation_rejection_is_immutable_exact_and_linear(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-23"
    repair_fingerprint = "c" * 64
    failed_source = PartitionIdentity(
        "research_os", "stage_source", partition_key
    )
    ledger.ensure_partition(failed_source, created_at=NOW)
    failed_lease = ledger.claim(
        owner="failed-source-for-shadow-rejection",
        identity=failed_source,
        now=NOW,
        lease_for=timedelta(minutes=30),
    )
    assert failed_lease is not None
    ledger.finish(
        failed_lease,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=1),
        error_code="source_failed",
        error="source failed before the complete repair cohort",
    )
    incident = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SOURCE,
        error_code="source_failed",
        message="source failed before the complete repair cohort",
        occurred_at=NOW + timedelta(minutes=1),
        partition_run_id=failed_source.partition_run_id,
    )

    authorities = []
    terminal_records = []
    for index, dataset in enumerate(
        (
            "stage_source",
            "stage_silver",
            "stage_data_quality",
            "stage_gold",
            "stage_shadow",
        )
    ):
        minute = 2 + index * 2
        authority = ledger.reserve_repair_successor(
            incident_id=incident.incident_id,
            dataset=dataset,
            repair_fingerprint=repair_fingerprint,
            created_at=NOW + timedelta(minutes=minute),
            details=(
                {
                    "operation": "shadow_nav_step",
                    "repair_cohort_id": f"repaircohort_{'d' * 64}",
                    "repair_validation_trade_date": "2026-08-25",
                }
                if dataset == "stage_shadow"
                else None
            ),
        )
        authorities.append(authority)
        lease = ledger.claim(
            owner=f"complete-{dataset}",
            identity=authority.identity,
            now=NOW + timedelta(minutes=minute),
            lease_for=timedelta(minutes=30),
        )
        assert lease is not None
        terminal_records.append(
            ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW + timedelta(minutes=minute + 1),
                output_hash=str(index + 1) * 64,
                details=(
                    {
                        "operation": "shadow_nav_step",
                        "repair_cohort_id": f"repaircohort_{'d' * 64}",
                        "repair_validation_trade_date": "2026-08-25",
                    }
                    if dataset == "stage_shadow"
                    else None
                ),
            )
        )

    gold_authority = authorities[-2]
    shadow_authority = authorities[-1]
    succeeded_shadow = terminal_records[-1]
    rejection = ledger.record_shadow_revalidation_rejection(
        incident_id=incident.incident_id,
        rejected_partition_run_id=shadow_authority.identity.partition_run_id,
        rejection_evidence_hash="e" * 64,
    )

    assert ledger.get_partition(shadow_authority.identity) == succeeded_shadow
    assert succeeded_shadow.status is PartitionStatus.SUCCEEDED
    assert succeeded_shadow.output_hash == "5" * 64
    marker = ledger.get_partition(rejection.identity)
    assert marker is not None
    assert marker.status is PartitionStatus.FAILED
    assert marker.error_code == "shadow_revalidation_stale"
    assert marker.input_hash == "e" * 64
    assert marker.repair_incident_id == incident.incident_id
    assert (
        marker.repair_parent_partition_run_id
        == shadow_authority.identity.partition_run_id
    )
    assert marker.details["authority_kind"] == (
        "typed_shadow_revalidation_rejection"
    )
    assert marker.details["rejected_partition_run_id"] == (
        shadow_authority.identity.partition_run_id
    )
    assert rejection.parent_partition_run_id == (
        shadow_authority.identity.partition_run_id
    )

    repeated = ledger.record_shadow_revalidation_rejection(
        incident_id=incident.incident_id,
        rejected_partition_run_id=shadow_authority.identity.partition_run_id,
        rejection_evidence_hash="e" * 64,
    )
    assert repeated == rejection
    assert ledger.get_repair_chain(incident.incident_id, "stage_shadow") == (
        shadow_authority,
        rejection,
    )

    with pytest.raises(ImmutablePartition):
        ledger.record_shadow_revalidation_rejection(
            incident_id=incident.incident_id,
            rejected_partition_run_id=shadow_authority.identity.partition_run_id,
            rejection_evidence_hash="f" * 64,
        )
    with pytest.raises(ImmutablePartition, match="different successor"):
        ledger.record_shadow_revalidation_rejection(
            incident_id=incident.incident_id,
            rejected_partition_run_id=gold_authority.identity.partition_run_id,
            rejection_evidence_hash="e" * 64,
        )

    successor = ledger.reserve_repair_successor(
        incident_id=incident.incident_id,
        dataset="stage_shadow",
        repair_fingerprint=repair_fingerprint,
        created_at=NOW + timedelta(minutes=20),
        details={
            "operation": "shadow_nav_step",
            "repair_cohort_id": f"repaircohort_{'f' * 64}",
            "repair_validation_trade_date": "2026-08-26",
        },
    )
    assert successor.parent_partition_run_id == rejection.identity.partition_run_id
    pending = ledger.get_partition(successor.identity)
    assert pending is not None
    assert pending.status is PartitionStatus.PENDING
    chain = ledger.get_repair_chain(incident.incident_id, "stage_shadow")
    assert chain == (shadow_authority, rejection, successor)
    assert all(
        child.parent_partition_run_id == parent.identity.partition_run_id
        for parent, child in zip(chain, chain[1:], strict=False)
    )


def test_later_stage_incident_repairs_share_one_global_linear_parent_chain(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-24"
    source_base = PartitionIdentity(
        "research_os", "stage_source", partition_key
    )
    ledger.ensure_partition(source_base, created_at=NOW)
    source_lease = ledger.claim(
        owner="accepted-source",
        identity=source_base,
        now=NOW,
        lease_for=timedelta(minutes=30),
    )
    assert source_lease is not None
    ledger.finish(
        source_lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=NOW + timedelta(minutes=1),
        output_hash="a" * 64,
    )

    failed_bases: dict[IncidentStage, PartitionIdentity] = {}
    for index, (stage, dataset) in enumerate(
        (
            (IncidentStage.GOLD, "stage_gold"),
            (IncidentStage.SHADOW_EXECUTION, "stage_shadow"),
        ),
        start=1,
    ):
        identity = PartitionIdentity("research_os", dataset, partition_key)
        failed_bases[stage] = identity
        ledger.ensure_partition(identity, created_at=NOW)
        lease = ledger.claim(
            owner=f"failed-{dataset}",
            identity=identity,
            now=NOW,
            lease_for=timedelta(minutes=30),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.FAILED,
            completed_at=NOW + timedelta(minutes=1),
            error_code=f"{dataset}_failed",
            error=f"{dataset} failed",
        )

    first = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.GOLD,
        error_code="stage_gold_failed",
        message="first later-stage incident",
        occurred_at=NOW + timedelta(minutes=2),
        partition_run_id=failed_bases[IncidentStage.GOLD].partition_run_id,
    )
    second = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SHADOW_EXECUTION,
        error_code="stage_shadow_failed",
        message="second later-stage incident",
        occurred_at=NOW + timedelta(minutes=3),
        partition_run_id=(
            failed_bases[IncidentStage.SHADOW_EXECUTION].partition_run_id
        ),
    )
    barrier = Barrier(2)

    def reserve_source(incident_id: str):
        barrier.wait(timeout=5)
        try:
            return ledger.reserve_repair_successor(
                incident_id=incident_id,
                dataset="stage_source",
                repair_fingerprint=(
                    "b" * 64 if incident_id == first.incident_id else "c" * 64
                ),
                created_at=NOW + timedelta(minutes=4),
            )
        except ProductionLedgerError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        concurrent = tuple(
            pool.map(reserve_source, (second.incident_id, first.incident_id))
        )
    first_source = next(
        result for result in concurrent if not isinstance(result, Exception)
    )
    waiting = next(
        result for result in concurrent if isinstance(result, Exception)
    )
    assert first_source.incident_id == first.incident_id
    assert "earlier OPEN repair cohort" in str(waiting)

    datasets = (
        "stage_source",
        "stage_silver",
        "stage_data_quality",
        "stage_gold",
        "stage_shadow",
    )

    def complete_chain(
        incident_id: str,
        fingerprint: str,
        source_authority,
        minute: int,
    ):
        selected = source_authority
        authorities = []
        for index, dataset in enumerate(datasets):
            if index:
                selected = ledger.reserve_repair_successor(
                    incident_id=incident_id,
                    dataset=dataset,
                    repair_fingerprint=fingerprint,
                    created_at=NOW + timedelta(minutes=minute),
                )
            authorities.append(selected)
            lease = ledger.claim(
                owner=f"{incident_id}-{dataset}",
                identity=selected.identity,
                now=NOW + timedelta(minutes=minute),
                lease_for=timedelta(minutes=30),
            )
            assert lease is not None
            ledger.finish(
                lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=NOW + timedelta(minutes=minute + 1),
                output_hash=str((minute + index) % 10) * 64,
            )
            minute += 2
        return tuple(authorities), minute

    first_chain, next_minute = complete_chain(
        first.incident_id, "b" * 64, first_source, 5
    )
    first_predecessor = ledger.get_incident_repair_predecessor(
        first.incident_id
    )
    assert first_predecessor is not None
    assert first_predecessor.identity == source_base
    second_source = ledger.reserve_repair_successor(
        incident_id=second.incident_id,
        dataset="stage_source",
        repair_fingerprint="c" * 64,
        created_at=NOW + timedelta(minutes=next_minute),
    )
    assert (
        second_source.parent_partition_run_id
        == first_chain[-1].identity.partition_run_id
    )
    second_chain, _ = complete_chain(
        second.incident_id,
        "c" * 64,
        second_source,
        next_minute,
    )
    second_predecessor = ledger.get_incident_repair_predecessor(
        second.incident_id
    )
    assert second_predecessor is not None
    assert second_predecessor.identity == first_chain[-1].identity
    assert second_predecessor.status is PartitionStatus.SUCCEEDED

    for incident, expected_chain in (
        (first, first_chain),
        (second, second_chain),
    ):
        previous = expected_chain[0].parent_partition_run_id
        for dataset, expected in zip(datasets, expected_chain, strict=True):
            chain = ledger.get_repair_chain(incident.incident_id, dataset)
            assert chain == (expected,)
            assert expected.parent_partition_run_id == previous
            previous = expected.identity.partition_run_id


def test_source_incidents_serialize_on_one_global_repair_chain(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-25"
    failed_source = PartitionIdentity(
        "research_os", "stage_source", partition_key
    )
    ledger.ensure_partition(failed_source, created_at=NOW)
    failed_lease = ledger.claim(
        owner="failed-source",
        identity=failed_source,
        now=NOW,
        lease_for=timedelta(minutes=30),
    )
    assert failed_lease is not None
    ledger.finish(
        failed_lease,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=1),
        error_code="source_failed",
        error="source failed",
    )
    first = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SOURCE,
        error_code="source_failed_first",
        message="first Source incident",
        occurred_at=NOW + timedelta(minutes=2),
        partition_run_id=failed_source.partition_run_id,
    )
    second = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SOURCE,
        error_code="source_failed_second",
        message="second Source incident",
        occurred_at=NOW + timedelta(minutes=3),
        partition_run_id=failed_source.partition_run_id,
    )
    barrier = Barrier(2)

    def reserve_source(incident_id: str):
        barrier.wait(timeout=5)
        try:
            return ledger.reserve_repair_successor(
                incident_id=incident_id,
                dataset="stage_source",
                repair_fingerprint=(
                    "1" * 64 if incident_id == first.incident_id else "2" * 64
                ),
                created_at=NOW + timedelta(minutes=4),
            )
        except ProductionLedgerError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        concurrent = tuple(
            pool.map(reserve_source, (second.incident_id, first.incident_id))
        )
    first_source = next(
        result for result in concurrent if not isinstance(result, Exception)
    )
    waiting = next(
        result for result in concurrent if isinstance(result, Exception)
    )
    assert first_source.incident_id == first.incident_id
    assert first_source.parent_partition_run_id == failed_source.partition_run_id
    assert "earlier OPEN repair cohort" in str(waiting)

    selected = first_source
    first_chain = []
    for index, dataset in enumerate(
        (
            "stage_source",
            "stage_silver",
            "stage_data_quality",
            "stage_gold",
            "stage_shadow",
        )
    ):
        if index:
            selected = ledger.reserve_repair_successor(
                incident_id=first.incident_id,
                dataset=dataset,
                repair_fingerprint="1" * 64,
                created_at=NOW + timedelta(minutes=5 + index * 2),
            )
        first_chain.append(selected)
        lease = ledger.claim(
            owner=f"first-source-{dataset}",
            identity=selected.identity,
            now=NOW + timedelta(minutes=5 + index * 2),
            lease_for=timedelta(minutes=30),
        )
        assert lease is not None
        ledger.finish(
            lease,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(minutes=6 + index * 2),
            output_hash=str(index + 3) * 64,
        )

    second_source = ledger.reserve_repair_successor(
        incident_id=second.incident_id,
        dataset="stage_source",
        repair_fingerprint="2" * 64,
        created_at=NOW + timedelta(minutes=20),
    )
    assert (
        second_source.parent_partition_run_id
        == first_chain[-1].identity.partition_run_id
    )
    predecessor = ledger.get_incident_repair_predecessor(second.incident_id)
    assert predecessor is not None
    assert predecessor.identity == first_chain[-1].identity
    assert predecessor.status is PartitionStatus.SUCCEEDED


def test_running_partition_is_fenced_by_exact_incident_before_repair_successor(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-24"
    run_id = "dagster-hard-crash-1"
    identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        identity,
        created_at=NOW,
        input_hash="a" * 64,
        details={"dagster_run_id": run_id, "operation": "source_sync"},
    )
    abandoned = ledger.claim(
        owner="appsvc-dagster-hard-crash-1",
        identity=identity,
        now=NOW + timedelta(minutes=1),
        lease_for=timedelta(hours=1),
    )
    assert abandoned is not None
    incident = ledger.record_incident(
        partition_key=partition_key,
        stage=IncidentStage.SOURCE,
        error_code="dagster_run_failure",
        message="worker process died",
        occurred_at=NOW + timedelta(minutes=2),
        partition_run_id=identity.partition_run_id,
        payload={
            "dagster_run_id": run_id,
            "failed_step_key": "source_sync",
        },
    )

    failed = ledger.terminalize_incident_partition(incident.incident_id)
    repeated = ledger.terminalize_incident_partition(incident.incident_id)

    assert failed.status is PartitionStatus.FAILED
    assert repeated == failed
    assert failed.completed_at == incident.occurred_at
    assert failed.lease_owner is None
    terminalization = failed.details["incident_terminalization"]
    assert terminalization["incident_id"] == incident.incident_id
    assert terminalization["prior_status"] == "running"
    assert terminalization["abandoned_lease_owner"] == (
        "appsvc-dagster-hard-crash-1"
    )
    assert len(terminalization["abandoned_lease_hash"]) == 64
    with pytest.raises(ImmutablePartition, match="cannot be overwritten"):
        ledger.finish(
            abandoned,
            status=PartitionStatus.SUCCEEDED,
            completed_at=NOW + timedelta(minutes=3),
            output_hash="b" * 64,
        )

    successor = ledger.reserve_repair_successor(
        incident_id=incident.incident_id,
        dataset="stage_source",
        repair_fingerprint="c" * 64,
        input_hash="d" * 64,
        created_at=NOW + timedelta(minutes=3),
    )
    assert successor.parent_partition_run_id == identity.partition_run_id
    assert successor.identity.generation != "base"
    assert ledger.get_partition(successor.identity).status is PartitionStatus.PENDING


def test_incident_reservation_loses_atomically_to_worker_success(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-25"
    run_id = "dagster-worker-won"
    identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        identity,
        created_at=NOW,
        details={"dagster_run_id": run_id},
    )
    lease = ledger.claim(
        owner="worker-won",
        identity=identity,
        now=NOW + timedelta(minutes=1),
        lease_for=timedelta(minutes=10),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.SUCCEEDED,
        completed_at=NOW + timedelta(minutes=2),
        output_hash="e" * 64,
        details={"dagster_run_id": run_id},
    )

    with pytest.raises(ImmutablePartition, match="succeeded partition"):
        ledger.record_incident(
            partition_key=partition_key,
            stage=IncidentStage.SOURCE,
            error_code="dagster_run_failure",
            message="late failure sensor",
            occurred_at=NOW + timedelta(minutes=3),
            partition_run_id=identity.partition_run_id,
            payload={
                "dagster_run_id": run_id,
                "failed_step_key": "source_sync",
            },
        )
    assert ledger.list_incidents(status=IncidentStatus.OPEN) == ()
    assert ledger.get_partition(identity).status is PartitionStatus.SUCCEEDED


def test_incident_reservation_rejects_wrong_terminal_run_or_missing_parent(
    ledger: ProductionLedger,
) -> None:
    partition_key = "2026-08-26"
    identity = PartitionIdentity("research_os", "stage_source", partition_key)
    ledger.ensure_partition(
        identity,
        created_at=NOW,
        details={"dagster_run_id": "dagster-run-a"},
    )
    lease = ledger.claim(
        owner="worker-a",
        identity=identity,
        now=NOW + timedelta(minutes=1),
        lease_for=timedelta(minutes=10),
    )
    assert lease is not None
    ledger.finish(
        lease,
        status=PartitionStatus.FAILED,
        completed_at=NOW + timedelta(minutes=2),
        error_code="source_failed",
        error="source failed",
        details={"dagster_run_id": "dagster-run-a"},
    )

    with pytest.raises(ImmutablePartition, match="Dagster run differs"):
        ledger.record_incident(
            partition_key=partition_key,
            stage=IncidentStage.SOURCE,
            error_code="dagster_run_failure",
            message="wrong run",
            occurred_at=NOW + timedelta(minutes=3),
            partition_run_id=identity.partition_run_id,
            payload={
                "dagster_run_id": "dagster-run-b",
                "failed_step_key": "source_sync",
            },
        )

    missing = PartitionIdentity(
        "research_os", "stage_source", "2026-08-27"
    )
    with pytest.raises(ProductionLedgerError, match="partition is missing"):
        ledger.record_incident(
            partition_key=missing.partition_key,
            stage=IncidentStage.SOURCE,
            error_code="dagster_run_failure",
            message="missing parent",
            occurred_at=NOW + timedelta(minutes=4),
            partition_run_id=missing.partition_run_id,
            payload={
                "dagster_run_id": "dagster-run-missing",
                "failed_step_key": "source_sync",
            },
        )
    assert ledger.list_incidents(status=IncidentStatus.OPEN) == ()


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
    repeated = ledger.record_incident(
        partition_key="2026-08-21",
        stage=IncidentStage.SILVER,
        error_code="reconciliation_disputed",
        message="api_key=another-private-value price conflict",
        occurred_at=NOW,
        source_ids=("diemeng", "tushare"),
        evidence_hashes=("c" * 64,),
        payload={"field": "close"},
    )
    assert first.incident_id == repeated.incident_id
    assert first.message == "api_key=*** price conflict"
    with pytest.raises(ImmutablePartition, match="origin authority changed"):
        ledger.record_incident(
            partition_key="2026-08-21",
            stage=IncidentStage.SILVER,
            error_code="reconciliation_disputed",
            message="a different presentation is rejected",
            occurred_at=NOW,
            source_ids=("diemeng", "tushare"),
            evidence_hashes=("c" * 64,),
            payload={"field": "close"},
        )
    resolved = ledger.resolve_incident(
        first.incident_id,
        resolved_at=NOW + timedelta(days=1),
        evidence={"accepted_snapshot_hash": "d" * 64},
    )
    assert resolved.status is IncidentStatus.RESOLVED
    assert resolved.resolution_hash is not None
    assert ledger.list_incidents(status=IncidentStatus.OPEN) == ()


def test_incident_payload_and_resolution_are_canonical_json_before_effects(
    ledger: ProductionLedger,
) -> None:
    canonical_now = NOW.isoformat(timespec="microseconds").replace("+00:00", "Z")
    incident = ledger.record_incident(
        partition_key="2026-08-30",
        stage=IncidentStage.GOLD,
        error_code="gold_revalidation_required",
        message="canonical evidence fixture",
        occurred_at=NOW,
        payload={
            "dataset_ids": ("daily", "daily_basic"),
            "observed_at": NOW,
        },
    )
    assert incident.payload == {
        "dataset_ids": ["daily", "daily_basic"],
        "observed_at": canonical_now,
    }

    repeated = ledger.record_incident(
        partition_key="2026-08-30",
        stage=IncidentStage.GOLD,
        error_code="gold_revalidation_required",
        message="canonical evidence fixture",
        occurred_at=NOW,
        payload={
            "dataset_ids": ["daily", "daily_basic"],
            "observed_at": canonical_now,
        },
    )
    assert repeated.incident_id == incident.incident_id

    observed_payloads: list[dict[str, object]] = []

    def apply_effects(open_incident, other_open):
        assert other_open == ()
        observed_payloads.append(open_incident.payload)
        return "applied"

    terminal, result, applied = ledger.resolve_incident_with_effects(
        incident.incident_id,
        resolved_at=NOW + timedelta(minutes=1),
        evidence={"snapshot_ids": ("gold-a", "gold-b"), "verified_at": NOW},
        apply_effects=apply_effects,
    )
    assert applied is True
    assert result == "applied"
    assert observed_payloads == [incident.payload]
    assert terminal.payload["resolution"] == {
        "snapshot_ids": ["gold-a", "gold-b"],
        "verified_at": canonical_now,
    }

    retried, retry_result, retry_applied = ledger.resolve_incident_with_effects(
        incident.incident_id,
        resolved_at=NOW + timedelta(minutes=1),
        evidence={
            "snapshot_ids": ["gold-a", "gold-b"],
            "verified_at": canonical_now,
        },
        apply_effects=lambda *_args: pytest.fail(
            "canonical exact retry must not replay effects"
        ),
    )
    assert retried.resolution_hash == terminal.resolution_hash
    assert retry_result is None
    assert retry_applied is False


def test_atomic_terminal_incident_normalizes_payload_and_resolution_json(
    ledger: ProductionLedger,
) -> None:
    canonical_now = NOW.isoformat(timespec="microseconds").replace("+00:00", "Z")
    closed = ledger.record_resolved_incident(
        partition_key="2026-08-31",
        stage=IncidentStage.SOURCE,
        error_code="legacy_canary_generation_isolated",
        message="canonical terminal fixture",
        occurred_at=NOW,
        resolved_at=NOW + timedelta(minutes=1),
        payload={"source_ids": ("old", "current"), "observed_at": NOW},
        resolution={"replacement_ids": ("current",), "verified_at": NOW},
        superseded=True,
    )
    assert closed.payload == {
        "observed_at": canonical_now,
        "source_ids": ["old", "current"],
        "resolution": {
            "replacement_ids": ["current"],
            "verified_at": canonical_now,
        },
    }

    repeated = ledger.record_resolved_incident(
        partition_key="2026-08-31",
        stage=IncidentStage.SOURCE,
        error_code="legacy_canary_generation_isolated",
        message="canonical terminal fixture",
        occurred_at=NOW,
        resolved_at=NOW + timedelta(minutes=1),
        payload={
            "source_ids": ["old", "current"],
            "observed_at": canonical_now,
        },
        resolution={
            "replacement_ids": ["current"],
            "verified_at": canonical_now,
        },
        superseded=True,
    )
    assert repeated.resolution_hash == closed.resolution_hash


def test_record_incident_rejects_reserved_resolution_payload(
    ledger: ProductionLedger,
) -> None:
    with pytest.raises(ValueError, match="reserved resolution"):
        ledger.record_incident(
            partition_key="2026-09-01",
            stage=IncidentStage.SOURCE,
            error_code="source_failure",
            message="reserved field fixture",
            occurred_at=NOW,
            payload={"resolution": {"disposition": "forged"}},
        )
    assert ledger.list_incidents() == ()


def test_resolution_validation_precedes_effect_callback(
    ledger: ProductionLedger,
) -> None:
    incident = ledger.record_incident(
        partition_key="2026-09-02",
        stage=IncidentStage.SOURCE,
        error_code="source_failure",
        message="invalid resolution fixture",
        occurred_at=NOW,
    )
    effect_calls: list[str] = []

    with pytest.raises(ValueError, match="cannot precede"):
        ledger.resolve_incident_with_effects(
            incident.incident_id,
            resolved_at=NOW - timedelta(microseconds=1),
            evidence={"disposition": "impossible"},
            apply_effects=lambda *_args: effect_calls.append("predated"),
        )
    with pytest.raises(ValueError, match="canonical JSON evidence"):
        ledger.resolve_incident_with_effects(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"unsupported": object()},
            apply_effects=lambda *_args: effect_calls.append("non_json"),
        )

    authority = ledger.list_incidents()[0]
    assert authority.status is IncidentStatus.OPEN
    assert authority.resolved_at is None
    assert authority.resolution_hash is None
    assert effect_calls == []


def test_competing_open_incident_reservations_share_one_authority(tmp_path) -> None:
    database = tmp_path / "incident-reservation-race.db"
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    setup_engine = create_engine(database_url, connect_args={"timeout": 30})
    Base.metadata.create_all(setup_engine)
    setup_engine.dispose()
    origin = {
        "partition_key": "2026-08-21",
        "stage": IncidentStage.SOURCE,
        "error_code": "dagster_run_failure",
        "message": "source partition failed closed",
        "occurred_at": NOW,
        "evidence_hashes": ("9" * 64,),
        "payload": {
            "dagster_run_id": "reservation-race",
            "failed_step_key": "source_sync",
        },
    }
    barrier = Barrier(2)

    def reserve(_: int) -> tuple[str, IncidentStatus]:
        with ProductionLedger(
            database_url, connect_args={"timeout": 30}
        ) as worker:
            barrier.wait()
            record = worker.record_incident(**origin)
            return record.incident_id, record.status

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, (1, 2)))

    assert len({incident_id for incident_id, _ in results}) == 1
    assert {status for _, status in results} == {IncidentStatus.OPEN}
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as audit:
        records = audit.list_incidents()
        assert len(records) == 1
        assert records[0].status is IncidentStatus.OPEN
        assert records[0].message == origin["message"]


def test_competing_incident_messages_fail_closed_after_one_winner(tmp_path) -> None:
    database = tmp_path / "incident-message-race.db"
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    setup_engine = create_engine(database_url, connect_args={"timeout": 30})
    Base.metadata.create_all(setup_engine)
    setup_engine.dispose()
    barrier = Barrier(2)

    def reserve(candidate: str) -> tuple[str, str]:
        with ProductionLedger(
            database_url, connect_args={"timeout": 30}
        ) as worker:
            barrier.wait()
            try:
                record = worker.record_incident(
                    partition_key="2026-08-22",
                    stage=IncidentStage.SOURCE,
                    error_code="dagster_run_failure",
                    message=f"source failure presentation {candidate}",
                    occurred_at=NOW,
                    payload={"dagster_run_id": "message-race"},
                )
                return "saved", record.message
            except ImmutablePartition as exc:
                assert "origin authority changed" in str(exc)
                return "conflict", candidate

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, ("alpha", "beta")))

    assert sorted(state for state, _ in results) == ["conflict", "saved"]
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as audit:
        records = audit.list_incidents()
        assert len(records) == 1
        assert records[0].message in {
            "source failure presentation alpha",
            "source failure presentation beta",
        }


def test_resolved_incident_is_atomic_idempotent_and_origin_immutable(
    ledger: ProductionLedger,
) -> None:
    origin = {
        "partition_key": "2026-08-21",
        "stage": IncidentStage.SOURCE,
        "error_code": "legacy_canary_generation_isolated",
        "occurred_at": NOW,
        "partition_run_id": "partition_legacy_generation",
        "source_ids": ("engcan_tushare_legacy",),
        "evidence_hashes": ("a" * 64,),
        "payload": {
            "legacy_source_id": "engcan_tushare_legacy",
            "legacy_status": "failed",
            "current_source_id": "engcan_tushare_current",
            "dataset": "daily",
        },
    }
    message = "legacy generation remains isolated"
    existing = ledger.record_incident(**origin, message=message)
    resolution = {
        "disposition": "superseded_by_verified_canary_generation",
        "replacement_partition_run_id": "partition_current_generation",
        "replacement_output_snapshot_id": "snapshot-current",
        "replacement_output_hash": "b" * 64,
    }

    closed = ledger.record_resolved_incident(
        **origin,
        message=message,
        resolved_at=NOW + timedelta(minutes=1),
        resolution=resolution,
        superseded=True,
    )
    repeated = ledger.record_resolved_incident(
        **origin,
        message=message,
        resolved_at=NOW + timedelta(minutes=1),
        resolution=resolution,
        superseded=True,
    )

    assert closed.incident_id == existing.incident_id == repeated.incident_id
    assert closed.status is IncidentStatus.SUPERSEDED
    assert repeated.resolution_hash == closed.resolution_hash
    assert ledger.list_incidents(status=IncidentStatus.OPEN) == ()
    with pytest.raises(ImmutablePartition, match="origin authority changed"):
        ledger.record_resolved_incident(
            **origin,
            message="a changed presentation is not an exact retry",
            resolved_at=NOW + timedelta(minutes=1),
            resolution=resolution,
            superseded=True,
        )
    with pytest.raises(ImmutablePartition, match="already resolved"):
        ledger.record_resolved_incident(
            **origin,
            message=message,
            resolved_at=NOW + timedelta(minutes=2),
            resolution={**resolution, "replacement_output_hash": "c" * 64},
            superseded=True,
        )
    with pytest.raises(ValueError, match="reserved resolution"):
        ledger.record_resolved_incident(
            **{
                **origin,
                "payload": {**origin["payload"], "resolution": resolution},
            },
            message="reserved payload",
            resolved_at=NOW + timedelta(minutes=1),
            resolution=resolution,
            superseded=True,
        )
    assert ledger.list_incidents(status=IncidentStatus.SUPERSEDED) == (closed,)


def test_resolved_incident_can_be_created_terminal_without_open_intermediate(
    ledger: ProductionLedger,
) -> None:
    closed = ledger.record_resolved_incident(
        partition_key="2026-08-22",
        stage=IncidentStage.SILVER,
        error_code="legacy_canary_generation_isolated",
        message="atomic terminal bridge",
        occurred_at=NOW,
        resolved_at=NOW + timedelta(minutes=1),
        resolution={"disposition": "verified replacement"},
        superseded=True,
        payload={"dataset": "silver_accepted"},
    )

    assert closed.status is IncidentStatus.SUPERSEDED
    assert closed.resolution_hash is not None
    assert ledger.list_incidents(status=IncidentStatus.OPEN) == ()
    with pytest.raises(ValueError, match="cannot precede"):
        ledger.record_resolved_incident(
            partition_key="2026-08-23",
            stage=IncidentStage.SILVER,
            error_code="legacy_canary_generation_isolated",
            message="invalid interval",
            occurred_at=NOW,
            resolved_at=NOW - timedelta(seconds=1),
            resolution={"disposition": "impossible"},
        )


@pytest.mark.parametrize("precreate_open", [False, True])
def test_competing_atomic_resolutions_have_one_immutable_winner(
    tmp_path,
    precreate_open: bool,
) -> None:
    database = tmp_path / f"incident-resolution-race-{precreate_open}.db"
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    setup_engine = create_engine(database_url, connect_args={"timeout": 30})
    Base.metadata.create_all(setup_engine)
    setup_engine.dispose()
    origin = {
        "partition_key": "2026-08-24",
        "stage": IncidentStage.SOURCE,
        "error_code": "legacy_canary_generation_isolated",
        "occurred_at": NOW,
        "source_ids": ("engcan_tushare_legacy",),
        "evidence_hashes": ("d" * 64,),
        "payload": {
            "legacy_source_id": "engcan_tushare_legacy",
            "current_source_id": "engcan_tushare_current",
            "dataset": "daily",
        },
    }
    if precreate_open:
        with ProductionLedger(database_url, connect_args={"timeout": 30}) as setup:
            setup.record_incident(**origin, message="terminal authority")

    barrier = Barrier(2)

    def resolve(candidate: str) -> tuple[str, str, str | None]:
        resolution = {
            "disposition": "superseded_by_verified_canary_generation",
            "replacement_partition_run_id": f"partition_{candidate}",
            "replacement_output_hash": ("a" if candidate == "alpha" else "b")
            * 64,
        }
        with ProductionLedger(
            database_url, connect_args={"timeout": 30}
        ) as worker:
            barrier.wait()
            try:
                record = worker.record_resolved_incident(
                    **origin,
                    message="terminal authority",
                    resolved_at=NOW + timedelta(minutes=1),
                    resolution=resolution,
                    superseded=True,
                )
                return "saved", candidate, record.resolution_hash
            except ImmutablePartition as exc:
                assert "already resolved" in str(exc)
                return "conflict", candidate, None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(resolve, ("alpha", "beta")))

    assert sorted(result[0] for result in results) == ["conflict", "saved"]
    winner = next(candidate for state, candidate, _ in results if state == "saved")
    loser = "beta" if winner == "alpha" else "alpha"
    winner_hash = next(
        resolution_hash
        for state, _, resolution_hash in results
        if state == "saved"
    )
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as audit:
        records = audit.list_incidents()
        assert len(records) == 1
        authority = records[0]
        assert authority.status is IncidentStatus.SUPERSEDED
        assert authority.resolution_hash == winner_hash
        assert authority.payload["resolution"]["replacement_partition_run_id"] == (
            f"partition_{winner}"
        )
        repeated = audit.record_resolved_incident(
            **origin,
            message="terminal authority",
            resolved_at=NOW + timedelta(minutes=1),
            resolution={
                "disposition": "superseded_by_verified_canary_generation",
                "replacement_partition_run_id": f"partition_{winner}",
                "replacement_output_hash": ("a" if winner == "alpha" else "b")
                * 64,
            },
            superseded=True,
        )
        assert repeated.resolution_hash == winner_hash
        with pytest.raises(ImmutablePartition, match="already resolved"):
            audit.record_resolved_incident(
                **origin,
                message="terminal authority",
                resolved_at=NOW + timedelta(minutes=1),
                resolution={
                    "disposition": "superseded_by_verified_canary_generation",
                    "replacement_partition_run_id": f"partition_{loser}",
                    "replacement_output_hash": (
                        "a" if loser == "alpha" else "b"
                    )
                    * 64,
                },
                superseded=True,
            )


def test_competing_legacy_resolutions_have_one_immutable_winner(tmp_path) -> None:
    database = tmp_path / "legacy-incident-resolution-race.db"
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    setup_engine = create_engine(database_url, connect_args={"timeout": 30})
    Base.metadata.create_all(setup_engine)
    setup_engine.dispose()
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as setup:
        incident = setup.record_incident(
            partition_key="2026-08-25",
            stage=IncidentStage.GOLD,
            error_code="gold_market_semantics_rejected",
            message="gold semantics require one terminal authority",
            occurred_at=NOW,
            evidence_hashes=("e" * 64,),
            payload={"role": "execution"},
        )

    barrier = Barrier(2)
    resolutions = {
        "alpha": {
            "disposition": "resolved_by_verified_gold_partition",
            "replacement_output_hash": "a" * 64,
        },
        "beta": {
            "disposition": "resolved_by_verified_gold_partition",
            "replacement_output_hash": "b" * 64,
        },
    }

    def resolve(candidate: str) -> tuple[str, str, str | None]:
        with ProductionLedger(
            database_url, connect_args={"timeout": 30}
        ) as worker:
            barrier.wait()
            try:
                record = worker.resolve_incident(
                    incident.incident_id,
                    resolved_at=NOW + timedelta(minutes=1),
                    evidence=resolutions[candidate],
                    superseded=True,
                )
                return "saved", candidate, record.resolution_hash
            except ImmutablePartition as exc:
                assert "already resolved" in str(exc)
                return "conflict", candidate, None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(resolve, ("alpha", "beta")))

    assert sorted(result[0] for result in results) == ["conflict", "saved"]
    winner = next(candidate for state, candidate, _ in results if state == "saved")
    loser = "beta" if winner == "alpha" else "alpha"
    winner_hash = next(
        resolution_hash
        for state, _, resolution_hash in results
        if state == "saved"
    )
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as audit:
        records = audit.list_incidents()
        assert len(records) == 1
        authority = records[0]
        assert authority.status is IncidentStatus.SUPERSEDED
        assert authority.resolution_hash == winner_hash
        assert authority.payload["resolution"] == resolutions[winner]
        repeated = audit.resolve_incident(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence=resolutions[winner],
            superseded=True,
        )
        assert repeated.resolution_hash == winner_hash
        with pytest.raises(ImmutablePartition, match="already resolved"):
            audit.resolve_incident(
                incident.incident_id,
                resolved_at=NOW + timedelta(minutes=1),
                evidence=resolutions[loser],
                superseded=True,
            )


def test_legacy_and_atomic_resolvers_share_one_terminal_authority(tmp_path) -> None:
    database = tmp_path / "mixed-incident-resolution-race.db"
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    setup_engine = create_engine(database_url, connect_args={"timeout": 30})
    Base.metadata.create_all(setup_engine)
    setup_engine.dispose()
    origin = {
        "partition_key": "2026-08-26",
        "stage": IncidentStage.SOURCE,
        "error_code": "legacy_canary_generation_isolated",
        "occurred_at": NOW,
        "partition_run_id": None,
        "source_ids": ("engcan_tushare_legacy",),
        "evidence_hashes": ("f" * 64,),
        "payload": {
            "legacy_source_id": "engcan_tushare_legacy",
            "current_source_id": "engcan_tushare_current",
            "dataset": "daily",
        },
    }
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as setup:
        incident = setup.record_incident(
            **origin,
            message="legacy generation is open",
        )

    barrier = Barrier(2)
    resolutions = {
        "legacy": {
            "disposition": "superseded_by_verified_canary_generation",
            "replacement_partition_run_id": "partition_legacy_api_winner",
            "replacement_output_hash": "1" * 64,
        },
        "atomic": {
            "disposition": "superseded_by_verified_canary_generation",
            "replacement_partition_run_id": "partition_atomic_api_winner",
            "replacement_output_hash": "2" * 64,
        },
    }

    def resolve(api: str) -> tuple[str, str, str | None]:
        with ProductionLedger(
            database_url, connect_args={"timeout": 30}
        ) as worker:
            barrier.wait()
            try:
                if api == "legacy":
                    record = worker.resolve_incident(
                        incident.incident_id,
                        resolved_at=NOW + timedelta(minutes=1),
                        evidence=resolutions[api],
                        superseded=True,
                    )
                else:
                    record = worker.record_resolved_incident(
                        **origin,
                        message="legacy generation is open",
                        resolved_at=NOW + timedelta(minutes=1),
                        resolution=resolutions[api],
                        superseded=True,
                    )
                return "saved", api, record.resolution_hash
            except ImmutablePartition as exc:
                assert "already resolved" in str(exc)
                return "conflict", api, None

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(resolve, ("legacy", "atomic")))

    assert sorted(result[0] for result in results) == ["conflict", "saved"]
    winner = next(api for state, api, _ in results if state == "saved")
    loser = "atomic" if winner == "legacy" else "legacy"
    winner_hash = next(
        resolution_hash
        for state, _, resolution_hash in results
        if state == "saved"
    )
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as audit:
        authority = audit.list_incidents()[0]
        assert authority.status is IncidentStatus.SUPERSEDED
        assert authority.resolution_hash == winner_hash
        assert authority.payload["resolution"] == resolutions[winner]
        if winner == "legacy":
            repeated = audit.record_resolved_incident(
                **origin,
                message="legacy generation is open",
                resolved_at=NOW + timedelta(minutes=1),
                resolution=resolutions[winner],
                superseded=True,
            )
        else:
            repeated = audit.resolve_incident(
                incident.incident_id,
                resolved_at=NOW + timedelta(minutes=1),
                evidence=resolutions[winner],
                superseded=True,
            )
        assert repeated.resolution_hash == winner_hash
        with pytest.raises(ImmutablePartition, match="already resolved"):
            audit.resolve_incident(
                incident.incident_id,
                resolved_at=NOW + timedelta(minutes=1),
                evidence=resolutions[loser],
                superseded=True,
            )


def test_resolution_effect_failure_rolls_back_open_and_retry_is_exact(
    ledger: ProductionLedger,
) -> None:
    incident = ledger.record_incident(
        partition_key="2026-08-27",
        stage=IncidentStage.GOLD,
        error_code="gold_revalidation_required",
        message="accepted Gold evidence must restore lifecycle before resolution",
        occurred_at=NOW,
        payload={"fixture_incident_id": "dinc_effect_rollback"},
    )
    calls: list[str] = []

    def fail_effect(open_incident, other_open):
        assert open_incident.status is IncidentStatus.OPEN
        assert other_open == ()
        calls.append("failed")
        raise RuntimeError("fixture effect failed")

    with pytest.raises(RuntimeError, match="fixture effect failed"):
        ledger.resolve_incident_with_effects(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"snapshot_id": "gold-accepted"},
            apply_effects=fail_effect,
        )
    still_open = ledger.list_incidents()[0]
    assert still_open.status is IncidentStatus.OPEN
    assert still_open.resolved_at is None
    assert still_open.resolution_hash is None
    assert "resolution" not in still_open.payload

    def succeed_effect(open_incident, other_open):
        assert open_incident.status is IncidentStatus.OPEN
        assert other_open == ()
        calls.append("succeeded")
        return {"restored_sleeves": ["value_quality"]}

    terminal, effect_result, applied = ledger.resolve_incident_with_effects(
        incident.incident_id,
        resolved_at=NOW + timedelta(minutes=1),
        evidence={"snapshot_id": "gold-accepted"},
        apply_effects=succeed_effect,
    )
    assert terminal.status is IncidentStatus.RESOLVED
    assert effect_result == {"restored_sleeves": ["value_quality"]}
    assert applied is True
    assert calls == ["failed", "succeeded"]

    def forbidden_retry(_open_incident, _other_open):
        raise AssertionError("exact terminal retry must not apply effects")

    repeated, repeated_result, repeated_applied = (
        ledger.resolve_incident_with_effects(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"snapshot_id": "gold-accepted"},
            apply_effects=forbidden_retry,
        )
    )
    assert repeated.resolution_hash == terminal.resolution_hash
    assert repeated_result is None
    assert repeated_applied is False
    with pytest.raises(ImmutablePartition, match="already resolved"):
        ledger.resolve_incident_with_effects(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"snapshot_id": "different-gold"},
            apply_effects=forbidden_retry,
        )
    assert calls == ["failed", "succeeded"]


def test_competing_resolution_effects_apply_once_before_terminal_cas(tmp_path) -> None:
    database = tmp_path / "incident-effect-race.db"
    database_url = f"sqlite+pysqlite:///{database.as_posix()}"
    setup_engine = create_engine(database_url, connect_args={"timeout": 30})
    Base.metadata.create_all(setup_engine)
    setup_engine.dispose()
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as setup:
        incident = setup.record_incident(
            partition_key="2026-08-28",
            stage=IncidentStage.GOLD,
            error_code="gold_revalidation_required",
            message="one revalidation may restore the fleet",
            occurred_at=NOW,
            payload={"fixture_incident_id": "dinc_effect_race"},
        )
    barrier = Barrier(2)
    applied_effects: list[str] = []

    def resolve(candidate: str) -> tuple[str, str, bool]:
        with ProductionLedger(
            database_url, connect_args={"timeout": 30}
        ) as worker:
            barrier.wait()

            def apply_effect(open_incident, other_open):
                assert open_incident.status is IncidentStatus.OPEN
                assert other_open == ()
                applied_effects.append(candidate)
                return f"restored-by-{candidate}"

            try:
                _, effect_result, applied = worker.resolve_incident_with_effects(
                    incident.incident_id,
                    resolved_at=NOW + timedelta(minutes=1),
                    evidence={"snapshot_id": f"gold-{candidate}"},
                    apply_effects=apply_effect,
                )
                assert effect_result == f"restored-by-{candidate}"
                return "saved", candidate, applied
            except ImmutablePartition as exc:
                assert "already resolved" in str(exc)
                return "conflict", candidate, False

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(resolve, ("alpha", "beta")))

    assert sorted(state for state, _, _ in results) == ["conflict", "saved"]
    winner = next(candidate for state, candidate, _ in results if state == "saved")
    assert applied_effects == [winner]
    assert next(applied for state, _, applied in results if state == "saved") is True
    with ProductionLedger(database_url, connect_args={"timeout": 30}) as audit:
        authority = audit.list_incidents()[0]
        assert authority.status is IncidentStatus.RESOLVED
        assert authority.payload["resolution"] == {"snapshot_id": f"gold-{winner}"}


def test_resolution_scope_factory_observes_other_open_incidents_atomically(
    ledger: ProductionLedger,
) -> None:
    first = ledger.record_incident(
        partition_key="2026-08-29",
        stage=IncidentStage.SOURCE,
        error_code="first_domain_failure",
        message="first domain incident",
        occurred_at=NOW,
        payload={"fixture_incident_id": "dinc_first"},
    )
    second = ledger.record_incident(
        partition_key="2026-08-29",
        stage=IncidentStage.SILVER,
        error_code="second_domain_failure",
        message="second domain incident",
        occurred_at=NOW + timedelta(seconds=1),
        payload={"fixture_incident_id": "dinc_second"},
    )
    scopes: list[tuple[str, ...]] = []

    def scoped_evidence(open_incident, other_open):
        assert open_incident.incident_id == first.incident_id
        blockers = tuple(item.incident_id for item in other_open)
        scopes.append(blockers)
        return {
            "fleet_action": "remained_frozen",
            "blocking_incident_ids": list(blockers),
        }

    def scoped_effect(open_incident, other_open):
        assert open_incident.incident_id == first.incident_id
        assert tuple(item.incident_id for item in other_open) == (second.incident_id,)
        return "kept-frozen"

    terminal, result, applied = ledger.resolve_incident_with_effects(
        first.incident_id,
        resolved_at=NOW + timedelta(minutes=1),
        evidence=scoped_evidence,
        apply_effects=scoped_effect,
    )

    assert terminal.status is IncidentStatus.RESOLVED
    assert result == "kept-frozen"
    assert applied is True
    assert scopes == [(second.incident_id,)]
    assert terminal.payload["resolution"] == {
        "fleet_action": "remained_frozen",
        "blocking_incident_ids": [second.incident_id],
    }
    assert {
        item.incident_id for item in ledger.iter_incidents(status=IncidentStatus.OPEN)
    } == {second.incident_id}


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
