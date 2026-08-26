from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from factor_lab.research_os.incident_control_outbox import (
    IncidentControlActionKind,
    IncidentControlActionStatus,
    IncidentControlLeaseConflict,
    IncidentControlOutbox,
)
from factor_lab.research_os.application_services import (
    ApplicationServices,
    OrchestrationFailure,
)
from factor_lab.research_os.orm import Base
from factor_lab.research_os.production_ledger import (
    ImmutablePartition,
    IncidentStage,
    LeaseConflict,
    ProductionLedger,
)


NOW = datetime(2026, 8, 26, 8, 0, tzinfo=timezone.utc)


@pytest.fixture
def ledger(tmp_path: Path):
    database = tmp_path / "incident-control-outbox.db"
    engine = create_engine(
        f"sqlite+pysqlite:///{database.as_posix()}",
        connect_args={"timeout": 5},
    )
    Base.metadata.create_all(engine)
    store = ProductionLedger(engine)
    try:
        yield store
    finally:
        store.close()
        engine.dispose()


def _incident(ledger: ProductionLedger, *, suffix: str = "base"):
    return ledger.record_incident(
        partition_key="2026-08-26",
        stage=IncidentStage.SOURCE,
        error_code=f"source_failed_{suffix}",
        message="source failed closed",
        occurred_at=NOW,
        payload={"domain_incident_id": f"dinc_{suffix}"},
    )


def _generic_incident(ledger: ProductionLedger, *, suffix: str):
    return ledger.record_incident(
        partition_key="2026-08-26",
        stage=IncidentStage.SOURCE,
        error_code=f"generic_failed_{suffix}",
        message="generic incident fixture",
        occurred_at=NOW,
        payload={"fixture_incident_id": suffix},
    )


def test_open_latch_atomically_enqueues_one_pending_control_action(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger)
    first = ledger.incident_controls.get(incident.incident_id)
    repeated = ledger.record_incident(
        partition_key="2026-08-26",
        stage=IncidentStage.SOURCE,
        error_code="source_failed_base",
        message="source failed closed",
        occurred_at=NOW,
        payload={"domain_incident_id": "dinc_base"},
    )
    second = ledger.incident_controls.get(incident.incident_id)

    assert repeated.incident_id == incident.incident_id
    assert first is not None and second is not None
    assert first.action_id == second.action_id
    assert first.status is IncidentControlActionStatus.PENDING
    assert first.attempts == first.fencing_token == 0


def test_expired_lease_is_reclaimed_and_old_fencing_token_cannot_finalize(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="fencing")
    first = ledger.incident_controls.claim(
        incident.incident_id,
        owner="worker-a",
        now=NOW,
        lease_for=timedelta(seconds=30),
    )
    assert first is not None
    assert (
        ledger.incident_controls.claim(
            incident.incident_id,
            owner="worker-b",
            now=NOW + timedelta(seconds=29),
            lease_for=timedelta(seconds=30),
        )
        is None
    )
    replacement = ledger.incident_controls.claim(
        incident.incident_id,
        owner="worker-b",
        now=NOW + timedelta(seconds=31),
        lease_for=timedelta(seconds=30),
    )
    assert replacement is not None
    assert replacement.fencing_token == first.fencing_token + 1

    with pytest.raises(IncidentControlLeaseConflict, match="stale or expired"):
        ledger.incident_controls.complete(
            first,
            result={"worker": "stale"},
            completed_at=NOW + timedelta(seconds=31),
        )
    completed = ledger.incident_controls.complete(
        replacement,
        result={"worker": "replacement"},
        completed_at=NOW + timedelta(seconds=32),
    )
    assert completed.status is IncidentControlActionStatus.SUCCEEDED
    assert completed.result == {"worker": "replacement"}
    assert completed.attempts == completed.fencing_token == 2


def test_callback_exception_releases_current_action_for_immediate_retry(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="retry")
    with pytest.raises(RuntimeError, match="catalog unavailable"):
        with ledger.incident_control_guard(incident.incident_id):
            raise RuntimeError("catalog unavailable")

    pending = ledger.incident_controls.get(incident.incident_id)
    assert pending is not None
    assert pending.status is IncidentControlActionStatus.PENDING
    assert pending.last_error_code == "RuntimeError"
    assert pending.attempts == pending.fencing_token == 1

    with ledger.incident_control_guard(incident.incident_id):
        pass
    completed = ledger.incident_controls.get(incident.incident_id)
    assert completed is not None
    assert completed.status is IncidentControlActionStatus.SUCCEEDED
    assert completed.attempts == completed.fencing_token == 2


def test_guard_holds_no_sql_transaction_while_catalog_callback_runs(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="no_tx")

    with ledger.incident_control_guard(incident.incident_id):
        # A second writer against the same SQLite file would fail with
        # ``database is locked`` if the guard retained its claim transaction.
        with ledger.engine.begin() as connection:
            connection.execute(text("UPDATE ros_data_incidents SET message = message"))

    action = ledger.incident_controls.get(incident.incident_id)
    assert action is not None
    assert action.status is IncidentControlActionStatus.SUCCEEDED


def test_concurrent_guard_grants_only_one_materialization_authority(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="concurrent")
    entered = Event()
    release = Event()
    callbacks: list[str] = []

    def first_worker() -> str:
        with ledger.incident_control_guard(
            incident.incident_id, owner="first-worker"
        ):
            callbacks.append("first")
            entered.set()
            assert release.wait(timeout=5)
        return "completed"

    def competing_worker() -> str:
        assert entered.wait(timeout=5)
        try:
            with ledger.incident_control_guard(
                incident.incident_id, owner="competing-worker"
            ):
                callbacks.append("competing")
        except LeaseConflict:
            return "leased"
        return "unexpected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(first_worker)
        competing_future = executor.submit(competing_worker)
        assert competing_future.result(timeout=5) == "leased"
        release.set()
        assert first_future.result(timeout=5) == "completed"

    assert callbacks == ["first"]
    action = ledger.incident_controls.get(incident.incident_id)
    assert action is not None
    assert action.status is IncidentControlActionStatus.SUCCEEDED
    assert action.attempts == 1


def test_completed_action_retry_revalidates_without_new_lease(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="verify")
    with ledger.incident_control_guard(incident.incident_id):
        pass
    before = ledger.incident_controls.get(incident.incident_id)
    verified: list[str] = []

    with ledger.incident_control_guard(incident.incident_id):
        verified.append("catalog-evidence-checked")

    after = ledger.incident_controls.get(incident.incident_id)
    assert verified == ["catalog-evidence-checked"]
    assert before is not None and after is not None
    assert before == after


def test_resume_api_discovers_pending_actions_and_publishes_results(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="resume")
    callbacks: list[str] = []

    completed = ledger.resume_incident_controls(
        owner="restart-sensor",
        limit=10,
        apply_effects=lambda authority: (
            callbacks.append(authority.incident_id)
            or {
                "incident_id": authority.incident_id,
                "control_state": "materialized_after_restart",
            }
        ),
    )

    assert callbacks == [incident.incident_id]
    assert len(completed) == 1
    assert completed[0].status is IncidentControlActionStatus.SUCCEEDED
    assert completed[0].result == {
        "incident_id": incident.incident_id,
        "control_state": "materialized_after_restart",
    }
    assert (
        ledger.resume_incident_controls(
            owner="restart-sensor",
            limit=10,
            apply_effects=lambda _authority: pytest.fail(
                "completed actions must not be rediscovered"
            ),
        )
        == ()
    )


def test_application_resume_reconstructs_callback_only_from_durable_incident(
    ledger: ProductionLedger,
) -> None:
    incident = ledger.record_incident(
        partition_key="2026-08-26",
        stage=IncidentStage.SOURCE,
        error_code="source_unavailable",
        message="formal source failure",
        occurred_at=NOW,
        evidence_hashes=("a" * 64,),
        payload={
            "dagster_run_id": "durable-run",
            "failed_step_key": "source_sync",
            "domain_incident_id": "dinc_durable",
            "failed_partition_input_hash": "b" * 64,
        },
    )
    service = object.__new__(ApplicationServices)
    service.production_ledger = ledger
    observed: list[tuple[str, dict[str, object]]] = []

    def replay(partition_key: str, **kwargs):
        observed.append((partition_key, kwargs))
        return {
            "incident_id": kwargs["_locked_authority"].incident_id,
            "control_state": "materialized",
        }

    service.report_unexpected_data_failure = replay
    result = service.resume_pending_incident_controls(
        worker_id="dagster-resume-sensor",
        limit=10,
    )

    assert result["completed_count"] == 1
    assert result["incident_ids"] == [incident.incident_id]
    assert len(observed) == 1
    partition_key, kwargs = observed[0]
    assert partition_key == incident.partition_key
    assert kwargs["message"] == incident.message
    assert kwargs["occurred_at"] == incident.occurred_at
    assert kwargs["dagster_run_id"] == "durable-run"
    assert kwargs["failed_step_key"] == "source_sync"
    assert kwargs["_locked_authority"].incident_id == incident.incident_id
    assert kwargs["_failure_context"][-1] == "b" * 64


def test_revalidation_rejects_open_incident_before_controls_succeed(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="unmaterialized")
    service = object.__new__(ApplicationServices)
    service.production_ledger = ledger
    service.catalog = SimpleNamespace()

    with pytest.raises(
        OrchestrationFailure,
        match="controls are not durably materialized",
    ):
        service.revalidate_data_incident(
            incident_id=incident.incident_id,
            snapshot_id="not-relevant-before-control-gate",
            occurred_at=NOW + timedelta(minutes=1),
        )


def test_resolution_callback_holds_no_ledger_transaction(
    ledger: ProductionLedger,
) -> None:
    incident = _generic_incident(ledger, suffix="resolution_no_tx")

    def apply_effects(_authority, _other_open):
        with ledger.engine.begin() as connection:
            connection.execute(text("UPDATE ros_data_incidents SET message = message"))
        return "catalog-committed"

    terminal, result, applied = ledger.resolve_incident_with_effects(
        incident.incident_id,
        resolved_at=NOW + timedelta(minutes=1),
        evidence={"snapshot_id": "accepted-gold"},
        apply_effects=apply_effects,
    )

    assert terminal.status.value == "resolved"
    assert result == "catalog-committed"
    assert applied is True
    action = ledger.incident_controls.get(
        incident.incident_id,
        action_kind=IncidentControlActionKind.REVALIDATE_INCIDENT,
    )
    assert action is not None
    assert action.status is IncidentControlActionStatus.SUCCEEDED


def test_new_open_incident_during_resolution_prevents_terminal_cas(
    ledger: ProductionLedger,
) -> None:
    first = _generic_incident(ledger, suffix="resolution_scope")
    created: list[str] = []

    def apply_effects(_authority, other_open):
        assert other_open == ()
        second = ledger.record_incident(
            partition_key="2026-08-27",
            stage=IncidentStage.SOURCE,
            error_code="new_failure_during_revalidation",
            message="new failure committed while callback runs",
            occurred_at=NOW + timedelta(seconds=1),
            payload={"domain_incident_id": "dinc_new_scope"},
        )
        created.append(second.incident_id)
        return "effects-are-idempotent"

    with pytest.raises(LeaseConflict, match="scope changed"):
        ledger.resolve_incident_with_effects(
            first.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"snapshot_id": "accepted-before-new-fault"},
            apply_effects=apply_effects,
        )

    assert len(created) == 1
    assert {
        row.incident_id for row in ledger.list_incidents()
    } == {first.incident_id, created[0]}
    assert all(row.status.value == "open" for row in ledger.list_incidents())
    action = ledger.incident_controls.get(
        first.incident_id,
        action_kind=IncidentControlActionKind.REVALIDATE_INCIDENT,
    )
    assert action is not None
    assert action.status is IncidentControlActionStatus.PENDING
    assert action.last_error_code == "IncidentScopeChanged"


def test_production_monitor_tick_wakes_incident_control_resume(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from factor_lab.research_os import cli as cli_module

    calls: list[tuple[str, object]] = []

    class Result:
        status = "completed"

        @staticmethod
        def to_dict():
            return {"status": "completed", "operation": "fixture"}

    class Services:
        def resume_pending_incident_controls(self, *, worker_id, limit):
            calls.append((worker_id, limit))
            return {
                "status": "completed",
                "completed_count": 1,
                "action_ids": ["ica_fixture"],
            }

        @staticmethod
        def execute(_request):
            return Result()

    emitted: list[dict[str, object]] = []
    monkeypatch.setattr(cli_module, "_settings", lambda _args: SimpleNamespace())
    monkeypatch.setattr(
        cli_module, "_require_production", lambda _settings, _command: None
    )
    monkeypatch.setattr(cli_module, "_authoritative_services", Services)
    monkeypatch.setattr(cli_module, "_close_services", lambda _services: None)
    monkeypatch.setattr(cli_module, "_emit", emitted.append)

    code = cli_module._monitor_tick(
        SimpleNamespace(input=None, as_of="2026-08-26")
    )

    assert code == 0
    assert calls == [("monitor-tick:2026-08-26", 100)]
    assert emitted[0]["operations"][0] == {
        "status": "completed",
        "completed_count": 1,
        "action_ids": ["ica_fixture"],
    }


def test_postgresql_lease_clock_ignores_caller_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_now = NOW + timedelta(hours=2)

    class Dialect:
        name = "postgresql"

    class Bind:
        dialect = Dialect()

    class Session:
        @staticmethod
        def get_bind():
            return Bind()

    monkeypatch.setattr(
        IncidentControlOutbox,
        "_database_now",
        staticmethod(lambda _session: database_now),
    )

    assert IncidentControlOutbox._lease_timestamp(
        Session(),
        NOW - timedelta(days=30),
        name="completed_at",
    ) == database_now
    assert IncidentControlOutbox._lease_timestamp(
        Session(),
        NOW + timedelta(days=30),
        name="now",
    ) == database_now


def test_backdated_completion_cannot_override_authoritative_expiry(
    ledger: ProductionLedger,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incident = _incident(ledger, suffix="db_clock_expiry")
    lease = ledger.incident_controls.claim(
        incident.incident_id,
        owner="expired-worker",
        now=NOW,
        lease_for=timedelta(seconds=30),
    )
    assert lease is not None
    monkeypatch.setattr(
        ledger.incident_controls,
        "_lease_timestamp",
        lambda _session, _supplied, *, name: NOW + timedelta(seconds=31),
    )

    with pytest.raises(IncidentControlLeaseConflict, match="stale or expired"):
        ledger.incident_controls.complete(
            lease,
            result={"forged": "backdated completion"},
            completed_at=NOW,
        )


def test_complete_and_resolution_share_incident_then_action_lock_order(
    ledger: ProductionLedger,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incident = _incident(ledger, suffix="lock_order_complete")
    lease = ledger.incident_controls.claim(
        incident.incident_id,
        owner="lock-order-worker",
        now=NOW,
        lease_for=timedelta(minutes=5),
    )
    assert lease is not None
    statements: list[str] = []

    def observe_sql(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(" ".join(statement.lower().split()))

    event.listen(ledger.engine, "before_cursor_execute", observe_sql)
    try:
        ledger.incident_controls.complete(
            lease,
            result={"control_state": "materialized"},
            completed_at=NOW + timedelta(seconds=1),
        )
    finally:
        event.remove(ledger.engine, "before_cursor_execute", observe_sql)
    incident_select = next(
        index
        for index, statement in enumerate(statements)
        if "from ros_data_incidents" in statement
    )
    action_select = next(
        index
        for index, statement in enumerate(statements)
        if "from ros_incident_control_actions" in statement
    )
    assert incident_select < action_select

    generic = _generic_incident(ledger, suffix="lock_order_resolution")
    canonical_lock = ledger.incident_controls._lock_incident_then_action
    final_lock_calls: list[tuple[str, str]] = []

    def observed_lock(session, *, incident_id, action_id):
        final_lock_calls.append((incident_id, action_id))
        return canonical_lock(
            session,
            incident_id=incident_id,
            action_id=action_id,
        )

    monkeypatch.setattr(
        ledger.incident_controls,
        "_lock_incident_then_action",
        observed_lock,
    )
    ledger.resolve_incident_with_effects(
        generic.incident_id,
        resolved_at=NOW + timedelta(minutes=1),
        evidence={"fixture": "canonical lock order"},
        apply_effects=lambda *_args: "applied",
    )
    assert len(final_lock_calls) == 1
    assert final_lock_calls[0][0] == generic.incident_id


def test_concurrent_control_complete_and_resolution_cas_do_not_deadlock(
    ledger: ProductionLedger,
) -> None:
    generic = _generic_incident(ledger, suffix="lock_order_concurrent")
    control_lease = ledger.incident_controls.claim(
        generic.incident_id,
        owner="concurrent-control-worker",
        now=NOW,
        lease_for=timedelta(minutes=5),
    )
    assert control_lease is not None
    callback_entered = Event()
    release_final_cas = Event()

    def resolve_worker() -> str:
        def apply_effects(*_args) -> str:
            callback_entered.set()
            assert release_final_cas.wait(timeout=10)
            return "materialized"

        record, effect, changed = ledger.resolve_incident_with_effects(
            generic.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"fixture": "concurrent canonical lock order"},
            apply_effects=apply_effects,
        )
        assert effect == "materialized"
        assert changed is True
        return record.status.value

    def complete_worker() -> str:
        assert release_final_cas.wait(timeout=10)
        try:
            ledger.incident_controls.complete(
                control_lease,
                result={"control_state": "materialized"},
                completed_at=NOW + timedelta(seconds=1),
            )
        except IncidentControlLeaseConflict:
            # Resolution may win the incident row first.  Losing OPEN authority
            # is the expected fail-closed result, not a lock-order failure.
            return "lost_open_authority"
        return "completed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        resolution = executor.submit(resolve_worker)
        assert callback_entered.wait(timeout=10)
        completion = executor.submit(complete_worker)
        release_final_cas.set()
        assert resolution.result(timeout=10) == "resolved"
        assert completion.result(timeout=10) in {
            "completed",
            "lost_open_authority",
        }


def test_production_domain_incident_cannot_use_generic_close_paths(
    ledger: ProductionLedger,
) -> None:
    incident = _incident(ledger, suffix="direct_close_blocked")
    effect_calls: list[str] = []

    with pytest.raises(ImmutablePartition, match="typed five-stage"):
        ledger.resolve_incident(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"forged": "direct close"},
        )
    with pytest.raises(ImmutablePartition, match="typed five-stage"):
        ledger.resolve_incident_with_effects(
            incident.incident_id,
            resolved_at=NOW + timedelta(minutes=1),
            evidence={"forged": "callback close"},
            apply_effects=lambda *_args: effect_calls.append("called"),
        )
    assert effect_calls == []
    assert ledger.list_incidents()[0].status.value == "open"

    with pytest.raises(ImmutablePartition, match="cannot be atomically closed"):
        ledger.record_resolved_incident(
            partition_key="2026-08-28",
            stage=IncidentStage.GOLD,
            error_code="forged_terminal_domain_incident",
            message="must traverse typed revalidation",
            occurred_at=NOW,
            resolved_at=NOW + timedelta(minutes=1),
            payload={"domain_incident_id": "dinc_forged_terminal"},
            resolution={"forged": True},
        )
    assert len(ledger.list_incidents()) == 1
