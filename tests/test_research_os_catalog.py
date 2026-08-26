from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest

from factor_lab.research_os.catalog import (
    AuthoritativeResultExists,
    CatalogConflict,
    CatalogNotFound,
    LegacyEvidenceRecord,
    LifecycleEvent,
    ResearchCatalog,
    RunRecord,
    TrialLedgerEntry,
    UnsupportedEvaluator,
)
from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    FactorSpec,
    Preregistration,
    RecoveryCase,
    RecoveryCaseStatus,
)
from factor_lab.research_os.governance import (
    EvidenceClass,
    TrialAdmissionStatus,
    TrialKind,
    TrialRegistration,
)


NOW = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)


def trial_registration(index: int, *, family: str | None = None) -> TrialRegistration:
    return TrialRegistration(
        trial_id=f"atomic-trial-{index}",
        experiment_fingerprint=f"{index:064x}",
        hypothesis_id=f"hypothesis-{index}",
        family=family or f"family-{index}",
        kind=TrialKind.CONFIRMATORY,
        registered_at=NOW,
        holdout_id=f"holdout-{index}",
        requested_evidence_class=EvidenceClass.PSEUDO_OOS,
    )


def snapshot(snapshot_id: str = "snapshot-1") -> DataSnapshotRef:
    return DataSnapshotRef(
        snapshot_id=snapshot_id,
        tier="gold",
        uri=f"s3://factor-lab/gold/{snapshot_id}",
        content_hash="a" * 64,
        as_of=NOW,
    )


def experiment(ref: DataSnapshotRef | None = None) -> ExperimentSpec:
    return ExperimentSpec(
        snapshot=ref or snapshot(),
        factor=FactorSpec(
            factor_id="quality-value-1",
            family="value_quality",
            name="Quality value",
            mechanism="Overreaction mean reversion.",
            expression={"op": "rank", "input": "book_yield"},
            direction="higher_is_better",
            falsification_criteria=("negative outer OOS excess",),
        ),
        evaluator_version="research_os.long_only.v2",
        environment=EnvironmentRef(
            code_hash="b" * 64,
            dependency_lock_hash="c" * 64,
            configuration_hash="d" * 64,
            python_version="3.10",
            platform="Windows-AMD64",
            evaluator_build="research_os.long_only.v2",
        ),
        preregistration=Preregistration(
            hypothesis_id="hyp-1",
            economic_mechanism="Overreaction mean reversion.",
            direction="positive",
            falsification_criteria=("negative outer OOS excess",),
            stop_rules=("two diagnostics",),
        ),
    )


@pytest.fixture
def catalog(tmp_path):
    instance = ResearchCatalog(tmp_path / "research-os.sqlite")
    instance.initialize_schema()
    try:
        yield instance
    finally:
        instance.close()


def test_snapshot_and_experiment_registration_are_idempotent(catalog) -> None:
    ref = snapshot()
    first_snapshot = catalog.register_snapshot(ref)
    assert catalog.register_snapshot(ref).created_at == first_snapshot.created_at

    spec = experiment(ref)
    first = catalog.register_experiment(spec)
    second = catalog.register_experiment(spec)
    assert first.experiment_id == second.experiment_id
    assert first.fingerprint == second.fingerprint
    assert catalog.list_snapshots(tier=ref.tier)[0].reference == ref
    assert catalog.list_experiments(family="value_quality")[0].experiment_id == first.experiment_id


def test_pending_evidence_epochs_are_append_only_and_only_newest_remains_open(catalog) -> None:
    epoch = catalog.freeze_evidence_epoch(
        architecture_version="research-os-v1",
        code_hash="1" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
        frozen_at=NOW,
    )
    assert catalog.get_evidence_epoch() == epoch
    assert epoch.first_forward_session is None
    assert catalog.freeze_evidence_epoch(
        architecture_version="research-os-v1",
        code_hash="1" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
        frozen_at=NOW,
    ) == epoch
    successor = catalog.freeze_evidence_epoch(
        architecture_version="research-os-v2",
        code_hash="5" * 64,
        configuration_hash="2" * 64,
        dependency_lock_hash="3" * 64,
        dirty_patch_hash="4" * 64,
        frozen_at=NOW,
    )
    assert successor.epoch_id != epoch.epoch_id
    assert catalog.get_pending_evidence_epoch() == successor
    versions = catalog.list_evidence_epochs()
    assert len(versions) == 2
    retained = next(item for item in versions if item.epoch_id == epoch.epoch_id)
    assert retained.lifecycle_status == "closed"
    assert retained.superseded_by_epoch_id == successor.epoch_id

    registration = TrialRegistration(
        trial_id="fake-forward",
        experiment_fingerprint="9" * 64,
        hypothesis_id="fake-forward",
        family="value",
        kind=TrialKind.CONFIRMATORY,
        registered_at=NOW,
        holdout_id="caller-invented-holdout",
        requested_evidence_class=EvidenceClass.PRISTINE_FORWARD,
    )
    reservation = catalog.reserve_trial(
        registration,
        candidate_id="fake-forward",
        maximum_monthly_confirmatory_trials=3,
        maximum_monthly_confirmatory_trials_per_family=1,
        maximum_diagnostic_branches=2,
    )
    assert reservation.admission.evidence_class is EvidenceClass.PSEUDO_OOS


def test_snapshot_keyset_pagination_exhausts_more_than_one_catalog_page(
    catalog,
) -> None:
    expected_ids: set[str] = set()
    for index in range(1_005):
        snapshot_id = f"silver-page-{index:04d}"
        expected_ids.add(snapshot_id)
        catalog.register_snapshot(
            DataSnapshotRef(
                snapshot_id=snapshot_id,
                tier="silver",
                uri=f"s3://factor-lab/silver/{snapshot_id}",
                content_hash=f"{index + 1:064x}",
                as_of=NOW - timedelta(days=index // 10),
            )
        )

    first = catalog.list_snapshot_page(
        limit=1_000,
        quality_status="accepted",
        tier="silver",
    )
    assert len(first.records) == 1_000
    assert first.next_cursor is not None

    second = catalog.list_snapshot_page(
        limit=1_000,
        quality_status="accepted",
        tier="silver",
        after=first.next_cursor,
    )
    assert len(second.records) == 5
    assert second.next_cursor is None

    combined = (*first.records, *second.records)
    actual_ids = [row.reference.snapshot_id for row in combined]
    assert len(actual_ids) == len(set(actual_ids)) == 1_005
    assert set(actual_ids) == expected_ids
    ordering = [
        (
            row.reference.as_of,
            row.created_at,
            row.reference.snapshot_id,
        )
        for row in combined
    ]
    assert ordering == sorted(
        ordering,
        key=lambda item: (-item[0].timestamp(), -item[1].timestamp(), item[2]),
    )


def test_experiment_cannot_reference_an_unregistered_snapshot(catalog) -> None:
    with pytest.raises(CatalogNotFound, match="not registered"):
        catalog.register_experiment(experiment())


def test_legacy_or_mismatched_evaluators_cannot_enter_authoritative_catalog(catalog) -> None:
    ref = snapshot()
    catalog.register_snapshot(ref)
    legacy = experiment(ref).model_copy(update={"evaluator_version": "legacy-v0"})
    with pytest.raises(UnsupportedEvaluator, match="legacy evidence"):
        catalog.register_experiment(legacy)

    mismatched_environment = experiment(ref).model_copy(
        update={
            "environment": experiment(ref).environment.model_copy(
                update={"evaluator_build": "different-build"}
            )
        }
    )
    with pytest.raises(UnsupportedEvaluator, match="exactly match"):
        catalog.register_experiment(mismatched_environment)


def test_concurrent_identical_registration_produces_one_authority(catalog) -> None:
    ref = snapshot()
    catalog.register_snapshot(ref)
    spec = experiment(ref)
    with ThreadPoolExecutor(max_workers=8) as executor:
        records = list(executor.map(lambda _: catalog.register_experiment(spec), range(20)))
    assert len({record.experiment_id for record in records}) == 1
    assert len(catalog.list_experiments()) == 1


def test_only_one_authoritative_result_can_exist(catalog) -> None:
    ref = snapshot()
    catalog.register_snapshot(ref)
    record = catalog.register_experiment(experiment(ref))

    result = catalog.record_authoritative_result(
        record.experiment_id,
        outcome="rejected",
        metrics={"net_sharpe": 0.31},
        artifact_uri="s3://factor-lab/results/1.json",
        completed_at=NOW,
    )
    assert catalog.record_authoritative_result(
        record.experiment_id,
        outcome="rejected",
        metrics={"net_sharpe": 0.31},
        artifact_uri="s3://factor-lab/results/1.json",
        completed_at=NOW + timedelta(hours=1),
    ).result_id == result.result_id

    with pytest.raises(AuthoritativeResultExists):
        catalog.record_authoritative_result(
            record.experiment_id,
            outcome="promoted",
            metrics={"net_sharpe": 1.2},
            completed_at=NOW,
        )

    second_spec = experiment(ref)
    second_spec = second_spec.model_copy(
        update={
            "factor": second_spec.factor.model_copy(
                update={"factor_id": "quality-value-2"}
            )
        }
    )
    second_experiment = catalog.register_experiment(second_spec)
    second_result = catalog.record_authoritative_result(
        second_experiment.experiment_id,
        outcome="rejected",
        metrics={"net_sharpe": 0.31},
        artifact_uri="s3://factor-lab/results/1.json",
        completed_at=NOW,
    )
    assert second_result.result_id != result.result_id


def test_trial_lifecycle_recovery_run_and_summary_queries(catalog) -> None:
    trial = TrialLedgerEntry(
        trial_id="trial-1",
        family="low_risk",
        candidate_id="low-vol",
        outcome="failure",
        reason="audit period reversed",
        p_value=0.03,
        alpha_spent=0.01,
        occurred_at=NOW,
    )
    catalog.append_trial(trial)
    assert catalog.list_trials(family="low_risk") == [trial]

    initial_event = LifecycleEvent(
        event_id="event-0",
        idempotency_key="low-risk-20260822-active",
        sleeve_id="low_risk",
        to_state="active",
        cause="fixture active state",
        occurred_at=NOW - timedelta(seconds=1),
    )
    catalog.append_lifecycle_event(initial_event)
    event = LifecycleEvent(
        event_id="event-1",
        idempotency_key="low-risk-20260822-reduced",
        sleeve_id="low_risk",
        from_state="active",
        to_state="reduced",
        cause="two weekly health failures",
        occurred_at=NOW,
    )
    assert catalog.append_lifecycle_event(event) == event
    assert catalog.append_lifecycle_event(event).event_id == "event-1"
    assert catalog.latest_lifecycle_state("low_risk").value == "reduced"
    assert catalog.list_lifecycle_events(sleeve_id="low_risk") == [
        event,
        initial_event,
    ]

    with pytest.raises(CatalogConflict, match="latest durable state"):
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="low-risk-stale-active-to-dormant",
                sleeve_id="low_risk",
                from_state="active",
                to_state="dormant",
                cause="stale concurrent monitor decision",
                occurred_at=NOW + timedelta(seconds=1),
            )
        )
    assert catalog.latest_lifecycle_state("low_risk").value == "reduced"

    case = RecoveryCase(
        recovery_case_id="case-1",
        sleeve_id="low_risk",
        lifecycle_state="reduced",
        triggered_at=NOW,
        drift_event_due_at=NOW + timedelta(days=5),
        diagnosis_due_at=NOW + timedelta(days=20),
        earliest_recovery_review_at=NOW + timedelta(days=84),
    )
    case = catalog.save_recovery_case(case)
    assert catalog.list_recovery_cases(sleeve_id="low_risk") == [case]
    terminal_cases = tuple(
        RecoveryCase(
            **{
                **case.model_dump(),
                "recovery_case_id": f"closed-case-{index}",
                "projection_version": 0,
                "status": RecoveryCaseStatus.CLOSED,
                "triggered_at": NOW + timedelta(minutes=index + 1),
                "drift_event_due_at": NOW + timedelta(days=5, minutes=index + 1),
                "diagnosis_due_at": NOW + timedelta(days=20, minutes=index + 1),
                "earliest_recovery_review_at": NOW
                + timedelta(days=84, minutes=index + 1),
            }
        )
        for index in range(3)
    )
    for terminal in terminal_cases:
        catalog.save_recovery_case(terminal)
    assert tuple(
        catalog.iter_recovery_cases(
            statuses=(
                RecoveryCaseStatus.OPEN,
                RecoveryCaseStatus.DIAGNOSING,
                RecoveryCaseStatus.OBSERVING,
            ),
            sleeve_id="low_risk",
            batch_size=1,
        )
    ) == (case,)

    run = RunRecord(
        run_id="run-1",
        run_type="monitor_tick",
        status="completed",
        input_fingerprint="f" * 64,
        started_at=NOW,
        completed_at=NOW + timedelta(minutes=2),
    )
    catalog.save_run(run)
    assert catalog.get_run("run-1") == run
    assert catalog.list_runs(run_type="monitor_tick") == [run]
    summary = catalog.catalog_summary()
    assert summary.totals["trials"] == 1
    assert summary.lifecycle_states == {"reduced": 1}
    assert summary.latest_run_started_at == NOW


def test_recovery_case_projection_rejects_stale_lost_updates(catalog) -> None:
    created = catalog.save_recovery_case(
        RecoveryCase(
            recovery_case_id="case-cas",
            sleeve_id="value_quality",
            lifecycle_state="dormant",
            triggered_at=NOW,
            drift_event_due_at=NOW + timedelta(days=5),
            diagnosis_due_at=NOW + timedelta(days=20),
            earliest_recovery_review_at=NOW + timedelta(days=84),
        )
    )
    assert created.projection_version == 1
    diagnosing = created.model_copy(
        update={"status": RecoveryCaseStatus.DIAGNOSING}
    )
    observing = created.model_copy(
        update={
            "status": RecoveryCaseStatus.OBSERVING,
            "challenger_ids": ("challenger-a",),
        }
    )

    first = catalog.save_recovery_case(observing)
    assert first.projection_version == 2
    with pytest.raises(CatalogConflict, match="changed since it was read"):
        catalog.save_recovery_case(diagnosing)
    assert catalog.get_recovery_case(created.recovery_case_id) == first
    # An exact stale replay is harmless and returns the current authority.
    assert catalog.save_recovery_case(observing) == first


def test_lifecycle_requires_explicit_cas_and_strict_causal_time(catalog) -> None:
    with pytest.raises(CatalogConflict, match="latest durable state"):
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="bootstrap-from-unproven-state",
                sleeve_id="unproven_sleeve",
                from_state="walk_forward",
                to_state="shadow",
                cause="invalid bootstrap transition",
                occurred_at=NOW,
            )
        )
    first = LifecycleEvent(
        event_id="evt_z",
        idempotency_key="causal-preregistered",
        sleeve_id="causal_sleeve",
        to_state="preregistered",
        cause="preregistered",
        occurred_at=NOW,
    )
    catalog.append_lifecycle_event(first)

    with pytest.raises(CatalogConflict, match="latest durable state"):
        catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key="missing-cas-origin",
                sleeve_id="causal_sleeve",
                to_state="canary",
                cause="missing expected origin",
                occurred_at=NOW + timedelta(seconds=1),
            )
        )

    with pytest.raises(CatalogConflict, match="strictly later"):
        catalog.append_lifecycle_event(
            LifecycleEvent(
                event_id="evt_a",
                idempotency_key="same-time-tie",
                sleeve_id="causal_sleeve",
                from_state="preregistered",
                to_state="canary",
                cause="ambiguous causal timestamp",
                occurred_at=NOW,
            )
        )

    assert catalog.latest_lifecycle_state("causal_sleeve").value == "preregistered"


def test_lifecycle_path_is_atomic_and_exactly_replayable(catalog) -> None:
    bad_path = (
        LifecycleEvent(
            idempotency_key="atomic-path:preregistered",
            sleeve_id="atomic_sleeve",
            to_state="preregistered",
            cause="atomic research path",
            occurred_at=NOW,
        ),
        LifecycleEvent(
            idempotency_key="atomic-path:canary",
            sleeve_id="atomic_sleeve",
            from_state="active",
            to_state="canary",
            cause="atomic research path",
            occurred_at=NOW + timedelta(microseconds=1),
        ),
    )
    with pytest.raises(CatalogConflict, match="latest durable state"):
        catalog.append_lifecycle_path(bad_path)
    assert catalog.list_lifecycle_events(sleeve_id="atomic_sleeve") == []

    good_path = (
        bad_path[0],
        LifecycleEvent(
            idempotency_key="atomic-path:canary",
            sleeve_id="atomic_sleeve",
            from_state="preregistered",
            to_state="canary",
            cause="atomic research path",
            occurred_at=NOW + timedelta(microseconds=1),
        ),
    )
    first = catalog.append_lifecycle_path(good_path)
    replay = catalog.append_lifecycle_path(
        tuple(
            LifecycleEvent(
                idempotency_key=event.idempotency_key,
                sleeve_id=event.sleeve_id,
                from_state=event.from_state,
                to_state=event.to_state,
                cause=event.cause,
                occurred_at=event.occurred_at,
                evidence=event.evidence,
            )
            for event in good_path
        )
    )
    assert replay == first
    assert catalog.latest_lifecycle_state("atomic_sleeve").value == "canary"


def test_latest_lifecycle_events_returns_one_database_ranked_row_per_sleeve(
    catalog,
) -> None:
    for sleeve_index in range(3):
        sleeve_id = f"sleeve-{sleeve_index}"
        state = None
        for event_index in range(105):
            next_state = "preregistered" if state is None else "canary"
            catalog.append_lifecycle_event(
                LifecycleEvent(
                    idempotency_key=f"{sleeve_id}:{event_index}",
                    sleeve_id=sleeve_id,
                    from_state=state,
                    to_state=next_state,
                    cause="bounded latest-per-sleeve query fixture",
                    occurred_at=NOW
                    + timedelta(seconds=sleeve_index * 1_000 + event_index),
                )
            )
            state = next_state

    latest = catalog.list_latest_lifecycle_events(limit=10)
    selected = {event.sleeve_id: event for event in latest}
    assert set(selected) == {"sleeve-0", "sleeve-1", "sleeve-2"}
    assert all(event.to_state.value == "canary" for event in selected.values())
    assert all(event.idempotency_key.endswith(":104") for event in selected.values())


def test_run_claim_is_atomic_and_existing_identity_wins(catalog) -> None:
    run = RunRecord(
        run_id="operation-claim",
        run_type="dagster:weekly:sleeve_health_check",
        status="running",
        input_fingerprint="a" * 64,
        started_at=NOW,
        metadata={"summary": "claimed"},
    )
    first, first_claimed = catalog.claim_run(run)
    competing = RunRecord(
        **{
            **run.__dict__,
            "started_at": NOW + timedelta(seconds=1),
            "metadata": {"summary": "competing"},
        }
    )
    second, second_claimed = catalog.claim_run(competing)

    assert first_claimed is True
    assert second_claimed is False
    assert second == first
    assert catalog.get_run(run.run_id).metadata["summary"] == "claimed"


def test_terminal_run_is_immutable_and_exact_replay_is_idempotent(catalog) -> None:
    running = RunRecord(
        run_id="terminal-authority",
        run_type="physical_canary",
        status="running",
        input_fingerprint="b" * 64,
        started_at=NOW,
        metadata={"phase": "running"},
    )
    catalog.save_run(running)
    succeeded = RunRecord(
        **{
            **running.__dict__,
            "status": "succeeded",
            "metadata": {"result_hash": "c" * 64},
            "completed_at": NOW + timedelta(minutes=1),
        }
    )
    assert catalog.save_run(succeeded) == succeeded
    assert catalog.save_run(succeeded) == succeeded
    assert catalog.claim_run(succeeded) == (succeeded, False)

    for conflict in (
        RunRecord(**{**succeeded.__dict__, "status": "failed", "error": "late"}),
        RunRecord(**{**succeeded.__dict__, "metadata": {"result_hash": "d" * 64}}),
        running,
    ):
        with pytest.raises(CatalogConflict, match="terminal run.*immutable"):
            catalog.save_run(conflict)
    assert catalog.claim_run(running) == (succeeded, False)
    with pytest.raises(CatalogConflict, match="terminal run.*immutable"):
        catalog.claim_run(
            RunRecord(
                **{
                    **succeeded.__dict__,
                    "status": "failed",
                    "error": "competing terminal claim",
                }
            )
        )
    assert catalog.get_run(running.run_id) == succeeded


def test_competing_terminal_outcomes_have_one_atomic_winner(tmp_path) -> None:
    database = tmp_path / "terminal-race.sqlite"
    running = RunRecord(
        run_id="terminal-race",
        run_type="physical_canary",
        status="running",
        input_fingerprint="c" * 64,
        started_at=NOW,
    )
    with ResearchCatalog(database) as setup:
        setup.initialize_schema()
        setup.save_run(running)

    outcomes = {
        "succeeded": RunRecord(
            **{
                **running.__dict__,
                "status": "succeeded",
                "metadata": {"result": "accepted"},
                "completed_at": NOW + timedelta(minutes=1),
            }
        ),
        "failed": RunRecord(
            **{
                **running.__dict__,
                "status": "failed",
                "metadata": {"failure_type": "FixtureFailure"},
                "error": "fixture failure",
                "completed_at": NOW + timedelta(minutes=1),
            }
        ),
    }
    barrier = Barrier(2)

    def finish(status: str) -> tuple[str, str]:
        with ResearchCatalog(database) as worker:
            barrier.wait()
            try:
                worker.save_run(outcomes[status])
                return "saved", status
            except CatalogConflict as exc:
                assert "terminal run" in str(exc)
                return "conflict", status

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(finish, ("succeeded", "failed")))

    assert sorted(result[0] for result in results) == ["conflict", "saved"]
    winner = next(status for result, status in results if result == "saved")
    loser = "failed" if winner == "succeeded" else "succeeded"
    with ResearchCatalog(database) as audit:
        assert audit.get_run(running.run_id) == outcomes[winner]
        assert audit.save_run(outcomes[winner]) == outcomes[winner]
        with pytest.raises(CatalogConflict, match="terminal run.*immutable"):
            audit.save_run(outcomes[loser])


def test_legacy_evidence_is_content_deduplicated_without_relabeling(catalog) -> None:
    evidence = LegacyEvidenceRecord(
        source_uri="artifacts/expanded_long_only/round_3/results.json",
        content_hash="e" * 64,
        trust_label="regression_only",
        reasons=("historical ST was not verified",),
        imported_at=NOW,
    )
    assert catalog.import_legacy_evidence(evidence) == evidence
    assert catalog.import_legacy_evidence(evidence) == evidence
    retry = LegacyEvidenceRecord(
        source_uri=evidence.source_uri,
        content_hash=evidence.content_hash,
        trust_label=evidence.trust_label,
        reasons=evidence.reasons,
        imported_at=NOW + timedelta(days=1),
    )
    assert catalog.import_legacy_evidence(retry) == evidence
    assert catalog.list_legacy_evidence(trust_label="regression_only") == [evidence]

    conflicting = LegacyEvidenceRecord(
        source_uri=evidence.source_uri,
        content_hash=evidence.content_hash,
        trust_label="promotion_grade",
        reasons=("incorrectly upgraded",),
        imported_at=NOW,
    )
    with pytest.raises(CatalogConflict, match="conflicting trust"):
        catalog.import_legacy_evidence(conflicting)


def test_shadow_event_chain_projects_account_and_long_only_positions(catalog) -> None:
    account = catalog.create_shadow_account(
        account_id="paper-main",
        name="Main research shadow",
        initial_capital=50_000_000,
        opened_at=NOW,
    )
    fill = catalog.append_shadow_event(
        account_id=account.account_id,
        event_type="fill",
        occurred_at=NOW + timedelta(days=1),
        expected_previous_hash=account.last_event_hash,
        payload={
            "order_id": "order-1",
            "account_state": {
                "cash": 49_000_000,
                "nav": 50_010_000,
                "benchmark_nav": 49_990_000,
            },
            "position_state": {
                "ticker": "000001.SZ",
                "quantity": 100_000,
                "average_cost": 10.0,
                "market_price": 10.1,
                "market_value": 1_010_000,
            },
        },
    )
    projected = catalog.get_shadow_account(account.account_id)
    assert projected is not None
    assert catalog.list_shadow_accounts(status="active") == [projected]
    assert projected.last_event_sequence == 2
    assert projected.last_event_hash == fill.event_hash
    assert projected.nav == 50_010_000
    assert catalog.list_shadow_positions(account.account_id)[0].quantity == 100_000
    assert catalog.verify_shadow_chain(account.account_id) is True
    assert [event.sequence_number for event in catalog.list_shadow_events(account_id=account.account_id)] == [2, 1]

    with pytest.raises(CatalogConflict, match="optimistic-lock"):
        catalog.append_shadow_event(
            account_id=account.account_id,
            event_type="mark",
            occurred_at=NOW + timedelta(days=2),
            expected_previous_hash=account.last_event_hash,
            payload={},
        )


@pytest.mark.parametrize("repository_kind", ("sqlite", "sqlalchemy"))
def test_shadow_event_exact_lookup_is_not_bounded_by_recent_event_pages(
    tmp_path, repository_kind: str
) -> None:
    if repository_kind == "sqlalchemy":
        pytest.importorskip("sqlalchemy")
        database = (
            "sqlite+pysqlite:///"
            + (tmp_path / "exact-shadow-event-sqlalchemy.sqlite").as_posix()
        )
    else:
        database = tmp_path / "exact-shadow-event.sqlite"

    with ResearchCatalog(database) as catalog:
        catalog.initialize_schema()
        account = catalog.create_shadow_account(
            account_id="exact-event-paper",
            name="Exact event lookup shadow",
            initial_capital=1_000_000,
            opened_at=NOW,
        )
        target = catalog.append_shadow_event(
            account_id=account.account_id,
            event_type="audit_marker",
            occurred_at=NOW + timedelta(days=1),
            expected_previous_hash=account.last_event_hash,
            payload={"marker": "lookup-target"},
        )
        catalog.append_shadow_events_atomic(
            account_id=account.account_id,
            expected_previous_hash=target.event_hash,
            events=tuple(
                {
                    "event_type": "audit_marker",
                    "occurred_at": NOW + timedelta(days=2),
                    "payload": {"marker": f"later-{index}"},
                }
                for index in range(1_001)
            ),
        )

        assert target not in catalog.list_shadow_events(
            account_id=account.account_id, limit=1_000
        )
        assert catalog.get_shadow_event(
            account_id=account.account_id, event_hash=target.event_hash
        ) == target

        other = catalog.create_shadow_account(
            account_id="other-exact-event-paper",
            name="Other exact event lookup shadow",
            initial_capital=1_000_000,
            opened_at=NOW,
        )
        assert catalog.get_shadow_event(
            account_id=other.account_id, event_hash=target.event_hash
        ) is None
        wrong_hash = (
            ("0" if target.event_hash[0] != "0" else "1")
            + target.event_hash[1:]
        )
        assert catalog.get_shadow_event(
            account_id=account.account_id, event_hash=wrong_hash
        ) is None


def test_shadow_event_exact_lookup_validates_identity_inputs(catalog) -> None:
    account = catalog.create_shadow_account(
        account_id="validated-event-paper",
        name="Validated exact event lookup shadow",
        initial_capital=1_000_000,
        opened_at=NOW,
    )

    for invalid_account_id in ("", " padded-account ", "a" * 161):
        with pytest.raises(ValueError, match="account_id"):
            catalog.get_shadow_event(
                account_id=invalid_account_id,
                event_hash=account.last_event_hash,
            )
    for invalid_event_hash in ("", "A" * 64, "g" * 64):
        with pytest.raises(ValueError, match="event_hash"):
            catalog.get_shadow_event(
                account_id=account.account_id,
                event_hash=invalid_event_hash,
            )


def test_shadow_ledger_rejects_forward_labels_and_negative_positions(catalog) -> None:
    catalog.create_shadow_account(
        account_id="paper-main",
        name="Main research shadow",
        initial_capital=50_000_000,
        opened_at=NOW,
    )
    with pytest.raises(ValueError, match="forward-only"):
        catalog.append_shadow_event(
            account_id="paper-main",
            event_type="signal_generated",
            occurred_at=NOW + timedelta(days=1),
            payload={"forward_return_5d": 0.2},
        )
    with pytest.raises(ValueError, match="long-only"):
        catalog.append_shadow_event(
            account_id="paper-main",
            event_type="fill",
            occurred_at=NOW + timedelta(days=1),
            payload={
                "position_state": {
                    "ticker": "000001.SZ",
                    "quantity": -1,
                    "average_cost": 10,
                    "market_price": 10,
                    "market_value": -10,
                }
            },
        )


def test_shadow_step_commits_all_events_and_projections_atomically(catalog) -> None:
    account = catalog.create_shadow_account(
        account_id="atomic-paper",
        name="Atomic shadow",
        initial_capital=1_000_000,
        opened_at=NOW,
    )
    events = catalog.append_shadow_events_atomic(
        account_id=account.account_id,
        expected_previous_hash=account.last_event_hash,
        events=(
            {
                "event_type": "order_submitted",
                "occurred_at": NOW + timedelta(days=1),
                "payload": {"order_id": "order-1"},
            },
            {
                "event_type": "fill",
                "occurred_at": NOW + timedelta(days=1),
                "payload": {
                    "order_id": "order-1",
                    "account_state": {"cash": 900_000, "nav": 1_001_000},
                    "position_state": {
                        "ticker": "600000.SH",
                        "quantity": 10_000,
                        "average_cost": 10,
                        "market_price": 10.1,
                        "market_value": 101_000,
                    },
                },
            },
        ),
    )
    assert [event.sequence_number for event in events] == [2, 3]
    assert events[1].previous_event_hash == events[0].event_hash
    assert catalog.get_shadow_account(account.account_id).last_event_sequence == 3
    assert catalog.list_shadow_positions(account.account_id)[0].ticker == "600000.SH"
    assert catalog.count_shadow_sessions(
        account_id=account.account_id,
        since=NOW.date(),
        through=(NOW + timedelta(days=2)).date(),
    ) == 0


def test_shadow_step_rolls_back_earlier_events_when_a_later_event_fails(catalog) -> None:
    account = catalog.create_shadow_account(
        account_id="rollback-paper",
        name="Rollback shadow",
        initial_capital=1_000_000,
        opened_at=NOW,
    )
    with pytest.raises(CatalogConflict, match="chronological"):
        catalog.append_shadow_events_atomic(
            account_id=account.account_id,
            events=(
                {
                    "event_type": "order_submitted",
                    "occurred_at": NOW + timedelta(days=2),
                    "payload": {"order_id": "order-1"},
                },
                {
                    "event_type": "order_cancelled",
                    "occurred_at": NOW + timedelta(days=1),
                    "payload": {"order_id": "order-1"},
                },
            ),
        )
    projected = catalog.get_shadow_account(account.account_id)
    assert projected.last_event_sequence == 1
    assert len(catalog.list_shadow_events(account_id=account.account_id)) == 1
    assert catalog.verify_shadow_chain(account.account_id) is True


def test_read_queries_reject_unbounded_limits(catalog) -> None:
    with pytest.raises(ValueError, match="between 1 and 1000"):
        catalog.list_runs(limit=1001)


def test_sqlalchemy_repository_matches_catalog_semantics(tmp_path) -> None:
    pytest.importorskip("sqlalchemy")
    database_url = f"sqlite+pysqlite:///{(tmp_path / 'sqlalchemy.sqlite').as_posix()}"
    with ResearchCatalog(database_url) as catalog:
        catalog.initialize_schema()
        ref = snapshot()
        catalog.register_snapshot(ref)
        registered = catalog.register_experiment(experiment(ref))
        catalog.record_authoritative_result(
            registered.experiment_id,
            outcome="rejected",
            metrics={"net_sharpe": 0.31},
            completed_at=NOW,
        )
        account = catalog.create_shadow_account(
            account_id="sqlalchemy-paper",
            name="SQLAlchemy paper account",
            initial_capital=50_000_000,
            opened_at=NOW,
        )
        catalog.append_shadow_events_atomic(
            account_id=account.account_id,
            events=(
                {
                    "event_type": "order_submitted",
                    "occurred_at": NOW + timedelta(days=1),
                    "payload": {"order_id": "sqlalchemy-order"},
                },
                {
                    "event_type": "account_projected",
                    "occurred_at": NOW + timedelta(days=1),
                    "payload": {"account_state": {"nav": 50_100_000}},
                },
            ),
        )
        legacy = LegacyEvidenceRecord(
            source_uri="legacy/result.json",
            content_hash="e" * 64,
            trust_label="regression_only",
            reasons=("historical result",),
            imported_at=NOW,
        )
        catalog.import_legacy_evidence(legacy)
        assert catalog.import_legacy_evidence(
            LegacyEvidenceRecord(
                source_uri=legacy.source_uri,
                content_hash=legacy.content_hash,
                trust_label=legacy.trust_label,
                reasons=legacy.reasons,
                imported_at=NOW + timedelta(days=1),
            )
        ) == legacy
        claimed_run = RunRecord(
            run_id="sqlalchemy-operation-claim",
            run_type="dagster:daily:source_sync",
            status="running",
            input_fingerprint="9" * 64,
            started_at=NOW,
        )
        assert catalog.claim_run(claimed_run) == (claimed_run, True)
        assert catalog.claim_run(
            RunRecord(
                **{
                    **claimed_run.__dict__,
                    "started_at": NOW + timedelta(seconds=1),
                }
            )
        ) == (claimed_run, False)
        terminal_run = RunRecord(
            **{
                **claimed_run.__dict__,
                "status": "succeeded",
                "metadata": {"result_hash": "8" * 64},
                "completed_at": NOW + timedelta(minutes=1),
            }
        )
        assert catalog.save_run(terminal_run) == terminal_run
        assert catalog.save_run(terminal_run) == terminal_run
        with pytest.raises(CatalogConflict, match="terminal run.*immutable"):
            catalog.save_run(
                RunRecord(
                    **{
                        **terminal_run.__dict__,
                        "status": "failed",
                        "error": "competing terminal outcome",
                    }
                )
            )

        assert catalog.list_experiments(status="completed")[0].experiment_id == (
            registered.experiment_id
        )
        assert catalog.verify_shadow_chain(account.account_id) is True
        assert catalog.count_shadow_sessions(
            account_id=account.account_id,
            since=NOW.date(),
            through=(NOW + timedelta(days=2)).date(),
        ) == 1
        summary = catalog.catalog_summary()
        assert summary.totals["results"] == 1
        assert summary.totals["shadow_events"] == 3


def test_atomic_trial_reservation_never_exceeds_concurrent_month_budget(tmp_path) -> None:
    database = tmp_path / "atomic-budget.sqlite"
    with ResearchCatalog(database) as setup:
        setup.initialize_schema()

    def reserve(index: int):
        with ResearchCatalog(database) as worker:
            return worker.reserve_trial(
                trial_registration(index),
                candidate_id=f"candidate-{index}",
                maximum_monthly_confirmatory_trials=3,
                maximum_monthly_confirmatory_trials_per_family=1,
            )

    with ThreadPoolExecutor(max_workers=8) as executor:
        decisions = list(executor.map(reserve, range(1, 9)))

    assert sum(item.admission.allowed for item in decisions) == 3
    with ResearchCatalog(database) as audit:
        rows = audit.list_trials()
    assert len(rows) == 8
    assert sum(
        row.admission_status is TrialAdmissionStatus.ADMITTED for row in rows
    ) == 3
    assert sum(
        row.admission_status is TrialAdmissionStatus.REJECTED for row in rows
    ) == 5


def test_rejected_duplicate_is_persisted_and_exact_retry_is_idempotent(catalog) -> None:
    admitted = catalog.reserve_trial(
        trial_registration(41, family="value"), candidate_id="candidate-41"
    )
    duplicate_registration = TrialRegistration(
        **{
            **trial_registration(42, family="quality").__dict__,
            "experiment_fingerprint": admitted.entry.experiment_fingerprint,
        }
    )
    rejected = catalog.reserve_trial(
        duplicate_registration, candidate_id="candidate-42"
    )
    replay = catalog.reserve_trial(
        duplicate_registration, candidate_id="candidate-42"
    )

    assert admitted.admission.allowed
    assert not rejected.admission.allowed
    assert "duplicate_experiment_fingerprint" in rejected.admission.reasons
    assert replay.created is False
    assert replay.entry == rejected.entry
    assert len(catalog.list_trials()) == 2
    persisted = catalog.get_trial(duplicate_registration.trial_id)
    assert persisted is not None
    assert persisted.admission_status is TrialAdmissionStatus.REJECTED


def test_admitted_reservation_is_completed_in_place_exactly_once(catalog) -> None:
    reservation = catalog.reserve_trial(
        trial_registration(51), candidate_id="candidate-51"
    )
    assert reservation.entry.completed_at is None
    completed = catalog.complete_trial(
        reservation.entry.trial_id,
        outcome="failure",
        reason="failed frozen promotion thresholds",
        p_value=0.23,
        alpha_spent=0.01,
        metadata={"net_sharpe": 0.4},
        completed_at=NOW + timedelta(hours=1),
    )
    replay = catalog.complete_trial(
        reservation.entry.trial_id,
        outcome="failure",
        reason="failed frozen promotion thresholds",
        p_value=0.23,
        alpha_spent=0.01,
        metadata={"net_sharpe": 0.4},
        completed_at=NOW + timedelta(hours=2),
    )

    assert replay == completed
    assert completed.completed_at == NOW + timedelta(hours=1)
    assert completed.metadata["reservation_state"] == "completed"
    assert len(catalog.list_trials()) == 1
    with pytest.raises(CatalogConflict, match="already has an outcome"):
        catalog.complete_trial(
            reservation.entry.trial_id,
            outcome="failure",
            reason="failed frozen promotion thresholds",
            p_value=0.24,
            alpha_spent=0.01,
            metadata={"net_sharpe": 0.4},
        )
