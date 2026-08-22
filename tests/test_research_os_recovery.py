from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from factor_lab.research_os.catalog import LifecycleEvent, ResearchCatalog, ShadowEventInput
from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    FactorSpec,
    LifecycleState,
    Preregistration,
    RecoveryCase,
    RecoveryCaseStatus,
)
from factor_lab.research_os.evaluator import CANONICAL_EVALUATOR_VERSION
from factor_lab.research_os.recovery import RecoveryCoordinator, RecoveryWorkflowError


NOW = datetime(2026, 1, 2, 15, 0, tzinfo=timezone.utc)


def _snapshot(*, labels: tuple[str, ...] = ()) -> DataSnapshotRef:
    suffix = "b" if labels else "a"
    return DataSnapshotRef(
        snapshot_id=suffix * 64,
        tier="gold",
        uri=f"s3://factor-lab/gold/{suffix * 64}",
        content_hash=suffix * 64,
        as_of=NOW,
        quality_status="accepted",
        trust_labels=labels,
    )


def _experiment(snapshot: DataSnapshotRef, candidate: str) -> ExperimentSpec:
    return ExperimentSpec(
        snapshot=snapshot,
        factor=FactorSpec(
            factor_id=candidate,
            family="value_quality",
            name=candidate,
            mechanism="cash-generative cheap companies may be underpriced",
            expression={
                "schema_version": "research-os/factor-dsl/v1",
                "output_id": "value",
                "nodes": [{"id": "value", "op": "field", "field": "book_yield"}],
            },
            direction="higher_is_better",
            falsification_criteria=("outer OOS excess is non-positive",),
        ),
        evaluator_version=CANONICAL_EVALUATOR_VERSION,
        environment=EnvironmentRef(
            code_hash="c" * 64,
            dependency_lock_hash="d" * 64,
            configuration_hash="e" * 64,
            python_version="3.11",
            platform="test",
            evaluator_build=CANONICAL_EVALUATOR_VERSION,
        ),
        preregistration=Preregistration(
            hypothesis_id=f"hyp-{candidate}",
            economic_mechanism="cash-generative cheap companies may be underpriced",
            direction="positive",
            falsification_criteria=("outer OOS excess is non-positive",),
            stop_rules=("stop after two diagnostics",),
        ),
    )


def _case() -> RecoveryCase:
    return RecoveryCase(
        recovery_case_id="recovery-value-001",
        sleeve_id="value_quality",
        lifecycle_state="dormant",
        triggered_at=NOW,
        drift_event_due_at=NOW + timedelta(days=5),
        diagnosis_due_at=NOW + timedelta(days=20),
        earliest_recovery_review_at=NOW + timedelta(days=60),
    )


def _append_projection(
    catalog: ResearchCatalog,
    account_id: str,
    *,
    occurred_at: datetime,
    nav: float,
    benchmark_nav: float,
) -> None:
    account = catalog.get_shadow_account(account_id)
    assert account is not None
    catalog.append_shadow_events_atomic(
        account_id=account_id,
        events=(
            ShadowEventInput(
                event_type="account_projected",
                occurred_at=occurred_at,
                payload={
                    "account_status": "open",
                    "account_state": {
                        "cash": nav,
                        "nav": nav,
                        "benchmark_nav": benchmark_nav,
                    },
                },
            ),
        ),
        expected_previous_hash=account.last_event_hash,
    )


@pytest.fixture
def catalog(tmp_path: Path):
    with ResearchCatalog(
        tmp_path / "catalog.db",
        allowed_evaluator_versions=(CANONICAL_EVALUATOR_VERSION,),
    ) as instance:
        instance.initialize_schema()
        yield instance


def test_recovery_requires_verified_gold_and_two_diagnostic_branches(catalog) -> None:
    good = _snapshot()
    bad = _snapshot(labels=("st_history_unverified",))
    catalog.register_snapshot(good)
    catalog.register_snapshot(bad)
    catalog.save_recovery_case(_case())
    coordinator = RecoveryCoordinator(catalog)

    with pytest.raises(RecoveryWorkflowError, match="unverified"):
        coordinator.complete_diagnosis(
            "recovery-value-001",
            diagnosed_at=NOW + timedelta(days=1),
            snapshot_id=bad.snapshot_id,
            findings={"cause": "data drift"},
        )
    with pytest.raises(RecoveryWorkflowError, match="at most two"):
        coordinator.complete_diagnosis(
            "recovery-value-001",
            diagnosed_at=NOW + timedelta(days=1),
            snapshot_id=good.snapshot_id,
            findings={"cause": "regime drift"},
            diagnostic_branches=("a", "b", "c"),
        )

    updated = coordinator.complete_diagnosis(
        "recovery-value-001",
        diagnosed_at=NOW + timedelta(days=1),
        snapshot_id=good.snapshot_id,
        findings={"cause": "regime drift"},
        diagnostic_branches=("direction", "cost"),
    )

    assert updated.status.value == "diagnosing"
    replay = coordinator.complete_diagnosis(
        "recovery-value-001",
        diagnosed_at=NOW + timedelta(days=2),
        snapshot_id=good.snapshot_id,
        findings={"cause": "regime drift"},
        diagnostic_branches=("direction", "cost"),
    )
    assert replay == updated
    events = catalog.list_lifecycle_events(sleeve_id="value_quality", limit=20)
    assert sum(row.cause == "recovery_diagnosis_completed" for row in events) == 1


def test_recovery_challengers_are_monotonic_bounded_and_idempotent(catalog) -> None:
    snapshot = _snapshot()
    catalog.register_snapshot(snapshot)
    catalog.save_recovery_case(_case())
    candidates = [
        catalog.register_experiment(_experiment(snapshot, f"challenger-{number}"))
        for number in range(1, 5)
    ]
    coordinator = RecoveryCoordinator(catalog)

    with pytest.raises(RecoveryWorkflowError, match="before diagnosis"):
        coordinator.register_challengers(
            "recovery-value-001", (candidates[0].experiment_id,), registered_at=NOW
        )

    coordinator.complete_diagnosis(
        "recovery-value-001",
        diagnosed_at=NOW + timedelta(days=1),
        snapshot_id=snapshot.snapshot_id,
        findings={"cause": "signal decay"},
    )
    with pytest.raises(RecoveryWorkflowError, match="not registered"):
        coordinator.register_challengers(
            "recovery-value-001", ("missing",), registered_at=NOW + timedelta(days=2)
        )

    updated = coordinator.register_challengers(
        "recovery-value-001",
        (candidates[0].experiment_id,),
        registered_at=NOW + timedelta(days=2),
    )
    assert updated.status.value == "observing"
    assert updated.challenger_ids == (candidates[0].experiment_id,)

    replay = coordinator.register_challengers(
        "recovery-value-001",
        (candidates[0].experiment_id,),
        registered_at=NOW + timedelta(days=3),
    )
    assert replay.challenger_ids == updated.challenger_ids

    appended = coordinator.register_challengers(
        "recovery-value-001",
        (candidates[1].experiment_id, candidates[2].experiment_id),
        registered_at=NOW + timedelta(days=3),
    )
    assert set(appended.challenger_ids) == {
        candidates[0].experiment_id,
        candidates[1].experiment_id,
        candidates[2].experiment_id,
    }

    # Passing a subset is an idempotent additive call; it cannot replace or
    # delete the already registered set.
    no_deletion = coordinator.register_challengers(
        "recovery-value-001",
        (candidates[1].experiment_id,),
        registered_at=NOW + timedelta(days=4),
    )
    assert no_deletion.challenger_ids == appended.challenger_ids
    with pytest.raises(RecoveryWorkflowError, match="more than three"):
        coordinator.register_challengers(
            "recovery-value-001",
            (candidates[3].experiment_id,),
            registered_at=NOW + timedelta(days=4),
        )

    events = catalog.list_lifecycle_events(sleeve_id="value_quality", limit=100)
    registrations = [
        event for event in events if event.cause == "recovery_challenger_registered"
    ]
    assert len(registrations) == 3


def test_shadow_binding_requires_a_hash_chained_projection_baseline(catalog) -> None:
    snapshot = _snapshot()
    catalog.register_snapshot(snapshot)
    catalog.save_recovery_case(_case())
    candidate = catalog.register_experiment(_experiment(snapshot, "challenger-baseline"))
    coordinator = RecoveryCoordinator(catalog)
    coordinator.complete_diagnosis(
        "recovery-value-001",
        diagnosed_at=NOW + timedelta(days=1),
        snapshot_id=snapshot.snapshot_id,
        findings={"cause": "regime drift"},
    )
    coordinator.register_challengers(
        "recovery-value-001",
        (candidate.experiment_id,),
        registered_at=NOW + timedelta(days=2),
    )
    account = catalog.create_shadow_account(
        account_id="shadow-missing-baseline",
        name="Missing baseline",
        initial_capital=50_000_000,
        opened_at=NOW,
        currency="CNY",
    )
    with pytest.raises(RecoveryWorkflowError, match="baseline"):
        coordinator.bind_shadow_account(
            "recovery-value-001",
            candidate.experiment_id,
            account.account_id,
            bound_at=NOW + timedelta(days=2),
        )


def test_recovery_observation_uses_persisted_shadow_sessions_and_health(catalog) -> None:
    snapshot = _snapshot()
    catalog.register_snapshot(snapshot)
    catalog.save_recovery_case(_case())
    candidate = catalog.register_experiment(_experiment(snapshot, "challenger-shadow"))
    coordinator = RecoveryCoordinator(catalog)
    coordinator.complete_diagnosis(
        "recovery-value-001",
        diagnosed_at=NOW + timedelta(days=1),
        snapshot_id=snapshot.snapshot_id,
        findings={"cause": "regime drift"},
    )
    coordinator.register_challengers(
        "recovery-value-001",
        (candidate.experiment_id,),
        registered_at=NOW + timedelta(days=2),
    )
    account = catalog.create_shadow_account(
        account_id="shadow-challenger-one",
        name="Challenger one",
        initial_capital=50_000_000,
        opened_at=NOW,
        currency="CNY",
    )
    binding_time = NOW + timedelta(days=2)
    _append_projection(
        catalog,
        account.account_id,
        occurred_at=binding_time,
        nav=50_000_000.0,
        benchmark_nav=50_000_000.0,
    )
    coordinator.bind_shadow_account(
        "recovery-value-001",
        candidate.experiment_id,
        account.account_id,
        bound_at=binding_time,
    )
    # A real top-50 shadow step emits fills and position projections in
    # addition to one account projection.  Flood the ledger so the binding
    # baseline falls outside the generic 1,000-event tail; recovery must use
    # the catalog's event-type query and still see every daily NAV point.
    current_account = catalog.get_shadow_account(account.account_id)
    assert current_account is not None
    catalog.append_shadow_events_atomic(
        account_id=account.account_id,
        events=tuple(
            ShadowEventInput(
                event_type="order_blocked",
                occurred_at=binding_time + timedelta(minutes=1),
                payload={"ticker": f"noise-{number}", "reason": "test_noise"},
            )
            for number in range(1_001)
        ),
        expected_previous_hash=current_account.last_event_hash,
    )
    assert len(catalog.list_shadow_events(account_id=account.account_id, limit=1_000)) == 1_000
    for session_number in range(1, 60):
        _append_projection(
            catalog,
            account.account_id,
            occurred_at=binding_time + timedelta(days=session_number),
            nav=50_000_000.0 * (1.002**session_number),
            benchmark_nav=50_000_000.0 * (1.0005**session_number),
        )

    after_59_sessions = coordinator.evaluate_observation(
        "recovery-value-001", as_of=binding_time + timedelta(days=60)
    )
    assert after_59_sessions.observation_complete is False
    assert after_59_sessions.observations[0].session_count == 59
    assert after_59_sessions.observations[0].active_return_60 is None

    _append_projection(
        catalog,
        account.account_id,
        occurred_at=binding_time + timedelta(days=60),
        nav=50_000_000.0 * (1.002**60),
        benchmark_nav=50_000_000.0 * (1.0005**60),
    )

    coordinator.record_challenger_health(
        "recovery-value-001",
        candidate.experiment_id,
        observed_at=binding_time + timedelta(days=60, minutes=1),
        snapshot_id=snapshot.snapshot_id,
        ic_direction_restored=True,
        risk_alerts=("capacity_warning",),
    )
    blocked_by_risk = coordinator.evaluate_observation(
        "recovery-value-001", as_of=binding_time + timedelta(days=60, minutes=2)
    )
    assert blocked_by_risk.observation_complete is False
    assert blocked_by_risk.observations[0].risk_alerts == ("capacity_warning",)

    coordinator.record_challenger_health(
        "recovery-value-001",
        candidate.experiment_id,
        observed_at=binding_time + timedelta(days=61),
        snapshot_id=snapshot.snapshot_id,
        ic_direction_restored=True,
        risk_alerts=(),
    )
    result = coordinator.evaluate_observation(
        "recovery-value-001", as_of=binding_time + timedelta(days=61, minutes=1)
    )

    assert result.observation_complete is True
    assert result.eligible_challenger_ids == (candidate.experiment_id,)
    observation = result.observations[0]
    assert observation.chain_verified is True
    assert observation.session_count == 60
    assert observation.active_return_20 == pytest.approx(1.002**20 - 1.0005**20)
    assert observation.active_return_60 == pytest.approx(1.002**60 - 1.0005**60)

    persisted = catalog.get_recovery_case("recovery-value-001")
    assert persisted is not None
    assert persisted.status is RecoveryCaseStatus.RECOVERED
    assert persisted.lifecycle_state is LifecycleState.PROBATION
    lifecycle = catalog.list_lifecycle_events(sleeve_id="value_quality", limit=100)
    transitions = [
        row for row in lifecycle if row.cause == "recovery_shadow_observation_completed"
    ]
    assert len(transitions) == 1
    assert transitions[0].from_state is LifecycleState.DORMANT
    assert transitions[0].to_state is LifecycleState.PROBATION

    # A scheduler retry after the case is recovered returns the authoritative
    # evidence instead of creating a second transition or recomputing it from
    # subsequently observed data.
    replay = coordinator.evaluate_observation(
        "recovery-value-001", as_of=binding_time + timedelta(days=90)
    )
    assert replay == result
    lifecycle = catalog.list_lifecycle_events(sleeve_id="value_quality", limit=100)
    assert sum(
        row.cause == "recovery_shadow_observation_completed" for row in lifecycle
    ) == 1


def test_recovery_sessions_start_when_the_sleeve_actually_becomes_dormant(catalog) -> None:
    snapshot = _snapshot()
    catalog.register_snapshot(snapshot)
    reduced_case = _case().model_copy(update={"lifecycle_state": LifecycleState.REDUCED})
    catalog.save_recovery_case(reduced_case)
    candidate = catalog.register_experiment(_experiment(snapshot, "challenger-dormancy"))
    coordinator = RecoveryCoordinator(catalog)
    coordinator.complete_diagnosis(
        reduced_case.recovery_case_id,
        diagnosed_at=NOW + timedelta(days=1),
        snapshot_id=snapshot.snapshot_id,
        findings={"cause": "signal decay"},
    )
    coordinator.register_challengers(
        reduced_case.recovery_case_id,
        (candidate.experiment_id,),
        registered_at=NOW + timedelta(days=2),
    )
    account = catalog.create_shadow_account(
        account_id="shadow-dormancy-anchor",
        name="Dormancy anchor",
        initial_capital=50_000_000,
        opened_at=NOW,
        currency="CNY",
    )
    binding_time = NOW + timedelta(days=2)
    _append_projection(
        catalog,
        account.account_id,
        occurred_at=binding_time,
        nav=50_000_000,
        benchmark_nav=50_000_000,
    )
    coordinator.bind_shadow_account(
        reduced_case.recovery_case_id,
        candidate.experiment_id,
        account.account_id,
        bound_at=binding_time,
    )
    for session_number in range(1, 11):
        _append_projection(
            catalog,
            account.account_id,
            occurred_at=binding_time + timedelta(days=session_number),
            nav=50_000_000 * 1.001**session_number,
            benchmark_nav=50_000_000,
        )

    dormant_at = binding_time + timedelta(days=10, minutes=1)
    catalog.append_lifecycle_event(
        LifecycleEvent(
            idempotency_key="test:dormant:value-quality",
            sleeve_id=reduced_case.sleeve_id,
            from_state=LifecycleState.REDUCED,
            to_state=LifecycleState.DORMANT,
            cause="weekly_health_tick",
            occurred_at=dormant_at,
            evidence={"test": "authoritative dormancy transition"},
        )
    )
    current = catalog.get_recovery_case(reduced_case.recovery_case_id)
    assert current is not None
    catalog.save_recovery_case(
        current.model_copy(update={"lifecycle_state": LifecycleState.DORMANT})
    )
    for session_number in range(11, 70):
        _append_projection(
            catalog,
            account.account_id,
            occurred_at=binding_time + timedelta(days=session_number),
            nav=50_000_000 * 1.001**session_number,
            benchmark_nav=50_000_000,
        )

    result = coordinator.evaluate_observation(
        reduced_case.recovery_case_id,
        as_of=binding_time + timedelta(days=70),
    )
    assert result.observation_complete is False
    assert result.observations[0].observation_started_at == dormant_at
    assert result.observations[0].session_count == 59
    assert result.observations[0].active_return_60 is None
