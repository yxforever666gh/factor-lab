from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Event

import pandas as pd
import pytest
import factor_lab.research_os.sleeve_registry as sleeve_registry

from factor_lab.research_os.catalog import (
    CatalogConflict,
    LifecycleEvent,
    ResearchCatalog,
    ResearchSubmissionRecord,
    research_submission_lease_token,
)
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    FactorSpec,
    LifecycleState,
    Preregistration,
    RecoveryCase,
    SignalFieldSpec,
    SleeveSpec,
    SnapshotTier,
    TrialOutcome,
)
from factor_lab.research_os.cycle import ResearchCycleResult
from factor_lab.research_os.governance import (
    EvidenceClass,
    HISTORICAL_HOLDOUT_ID,
    TrialKind,
    TrialRegistration,
)
from factor_lab.research_os.monthly_research import (
    AuthoritativeResearchInputUnavailable,
    AuthoritativeResearchInputs,
    MonthlyResearchCoordinator,
    assemble_factor_sleeve,
    research_equivalence_hash,
    research_family_from_sleeve,
)
from factor_lab.research_os.recovery import RecoveryCoordinator


NOW = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)
EVALUATOR = "research_os.long_only.v2"


def _snapshot() -> DataSnapshotRef:
    return DataSnapshotRef(
        snapshot_id="a" * 64,
        tier=SnapshotTier.GOLD,
        uri="s3://factor-lab/gold/a",
        content_hash="a" * 64,
        as_of=NOW,
        quality_status=DataQualityStatus.ACCEPTED,
    )


def _environment() -> EnvironmentRef:
    return EnvironmentRef(
        code_hash="b" * 64,
        dependency_lock_hash="c" * 64,
        configuration_hash="d" * 64,
        dirty_patch_hash="e" * 64,
        python_version="3.12",
        platform="Windows-AMD64",
        evaluator_build=EVALUATOR,
    )


def _registered_sleeve() -> SleeveSpec:
    return SleeveSpec(
        sleeve_id="value_quality_v1",
        name="Value quality",
        mechanism="fixed-value-quality-mechanism",
        factor_ids=("fixed_anchor",),
        signal_expression={
            "schema_version": "research-os/factor-dsl/v1",
            "output_id": "ranked",
            "nodes": [
                {"id": "raw", "op": "field", "field": "book_to_price"},
                {"id": "ranked", "op": "rank", "input": "raw"},
            ],
        },
        signal_field_registry=(
            SignalFieldSpec(
                name="book_to_price",
                availability="close",
                available_at_column="daily_basic_available_at",
            ),
        ),
        cluster_id="mechanism-value-quality",
        falsification_criteria=("outer OOS active return is non-positive",),
    )


def _payload(*, node_ids=("raw", "ranked"), reverse_nodes=False) -> dict:
    raw, ranked = node_ids
    nodes = [
        {"id": raw, "op": "field", "field": "book_to_price"},
        {"id": ranked, "op": "rank", "input": raw},
    ]
    if reverse_nodes:
        nodes.reverse()
    return {
        "preregistration": {
            "hypothesis_id": f"hypothesis_{raw}",
            "economic_mechanism": "cheap securities may mean revert",
            "direction": "positive",
            "falsification_criteria": ["outer OOS active return is non-positive"],
            "stop_rules": ["stop after two diagnostics"],
        },
        "factor": {
            "factor_id": f"factor_{raw}",
            "family": "value_quality_v1",
            "name": f"Value {raw}",
            "mechanism": "cheap securities may mean revert",
            "expression": {
                "schema_version": "research-os/factor-dsl/v1",
                "output_id": ranked,
                "nodes": nodes,
            },
            "direction": "higher_is_better",
            "falsification_criteria": ["outer OOS active return is non-positive"],
        },
    }


def _inputs() -> AuthoritativeResearchInputs:
    return AuthoritativeResearchInputs(
        snapshot=_snapshot(),
        frame=pd.DataFrame({"date": [], "ticker": []}),
        exposure_frame=pd.DataFrame({"date": [], "ticker": []}),
        returns_history=pd.DataFrame({"date": [], "ticker": [], "return": []}),
        benchmark_weights=pd.DataFrame(
            {"date": [], "ticker": [], "benchmark_weight": [], "available_at": []}
        ),
    )


@pytest.fixture
def catalog(tmp_path):
    result = ResearchCatalog(tmp_path / "catalog.sqlite")
    result.initialize_schema()
    result.register_snapshot(_snapshot())
    result.register_research_family(
        research_family_from_sleeve(_registered_sleeve(), created_at=NOW)
    )
    try:
        yield result
    finally:
        result.close()


def _coordinator(catalog, *, resolver=lambda: _inputs(), cycle=None):
    return MonthlyResearchCoordinator(
        catalog,
        lake_root="unused-in-test",
        environment=_environment(),
        mode="test",
        input_resolver=resolver,
        cycle=cycle,
    )


def test_production_rejects_caller_supplied_input_resolver(catalog) -> None:
    with pytest.raises(ValueError, match="production cannot accept"):
        MonthlyResearchCoordinator(
            catalog,
            lake_root="unused",
            environment=_environment(),
            input_resolver=lambda: _inputs(),
        )


def test_submit_accepts_only_dsl_and_persists_reviewed_submission(catalog) -> None:
    coordinator = _coordinator(catalog)
    accepted = coordinator.submit(_payload(), family_id="value_quality_v1")
    assert accepted.accepted
    assert accepted.submission is not None
    assert accepted.submission.status == "reviewed"
    assert accepted.submission.spec.factor is None
    assert accepted.submission.spec.sleeve is not None
    assert catalog.list_experiments() == []
    assert catalog.list_trials() == []

    forged = _payload(node_ids=("forged", "forged_rank"))
    forged["metrics"] = {"sharpe": 99.0, "promote": True}
    rejected = coordinator.submit(forged, family_id="value_quality_v1")
    assert not rejected.accepted
    assert rejected.submission is None
    assert any("decision_authority_forbidden" in item for item in rejected.violations)
    assert len(catalog.list_runs(run_type="llm_proposal_review")) == 2
    assert len(catalog.list_runs(run_type="monthly_proposal_admission")) == 1


def test_fixed_family_mismatch_is_durably_rejected(catalog) -> None:
    payload = _payload()
    payload["factor"]["family"] = "caller_invented_family"
    result = _coordinator(catalog).submit(payload, family_id="value_quality_v1")
    assert not result.accepted
    assert result.submission is None
    assert "factor_family_not_in_fixed_registry" in result.violations


def _factor_experiment(*, ids=("raw", "ranked"), descriptions="first") -> ExperimentSpec:
    raw, ranked = ids
    return ExperimentSpec(
        snapshot=_snapshot(),
        factor=FactorSpec(
            factor_id=f"factor-{descriptions}",
            family="value_quality_v1",
            name=f"name-{descriptions}",
            mechanism=f"description-{descriptions}",
            expression={
                "schema_version": "research-os/factor-dsl/v1",
                "output_id": ranked,
                "nodes": [
                    {"id": ranked, "op": "rank", "input": raw},
                    {"id": raw, "op": "field", "field": "book_to_price"},
                ],
            },
            signal_field_registry=_registered_sleeve().signal_field_registry,
            direction="higher_is_better",
            falsification_criteria=(f"words-{descriptions}",),
        ),
        evaluator_version=EVALUATOR,
        environment=_environment(),
        preregistration=Preregistration(
            hypothesis_id=f"hypothesis-{descriptions}",
            economic_mechanism=f"description-{descriptions}",
            direction="positive",
            falsification_criteria=(f"words-{descriptions}",),
            stop_rules=(f"stop-{descriptions}",),
        ),
    )


def test_equivalence_hash_ignores_names_descriptions_and_dag_ids() -> None:
    family = research_family_from_sleeve(_registered_sleeve(), created_at=NOW)
    first = assemble_factor_sleeve(_factor_experiment(), family)
    second = assemble_factor_sleeve(
        _factor_experiment(ids=("different_raw", "different_rank"), descriptions="second"),
        family,
    )
    assert first.fingerprint() != second.fingerprint()
    assert research_equivalence_hash(first, family) == research_equivalence_hash(
        second, family
    )

    changed_factor = _factor_experiment(descriptions="changed").factor.model_copy(
        update={
            "expression": {
                "schema_version": "research-os/factor-dsl/v1",
                "output_id": "lagged",
                "nodes": [
                    {"id": "raw", "op": "field", "field": "book_to_price"},
                    {"id": "lagged", "op": "lag", "input": "raw", "periods": 1},
                ],
            }
        }
    )
    changed = assemble_factor_sleeve(
        _factor_experiment(descriptions="changed").model_copy(update={"factor": changed_factor}),
        family,
    )
    assert research_equivalence_hash(first, family) != research_equivalence_hash(
        changed, family
    )


def test_semantic_duplicate_is_rejected_before_second_execution(catalog) -> None:
    coordinator = _coordinator(catalog)
    first = coordinator.submit(_payload(), family_id="value_quality_v1").submission
    second = coordinator.submit(
        _payload(node_ids=("other_raw", "other_rank"), reverse_nodes=True),
        family_id="value_quality_v1",
    ).submission
    assert first is not None and second is not None
    assert first.experiment_fingerprint != second.experiment_fingerprint
    assert first.research_equivalence_hash == second.research_equivalence_hash
    assert coordinator.reserve(first.submission_id).status == "reserved"
    duplicate = coordinator.reserve(second.submission_id)
    assert duplicate.status == "failed"
    assert "duplicate_research_equivalence" in (duplicate.error or "")


def test_submission_lease_is_atomic_and_expired_work_is_recoverable(catalog) -> None:
    submission = _coordinator(catalog).submit(
        _payload(), family_id="value_quality_v1"
    ).submission
    assert submission is not None
    reserved = _coordinator(catalog).reserve(submission.submission_id)
    claimed, won = catalog.claim_research_submission(
        reserved.submission_id,
        worker_id="worker-a",
        claimed_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=5),
    )
    assert won and claimed.status == "running" and claimed.attempts == 1
    first_token = research_submission_lease_token(claimed)
    same_owner, won = catalog.claim_research_submission(
        reserved.submission_id,
        worker_id="worker-a",
        claimed_at=NOW + timedelta(minutes=1),
        lease_expires_at=NOW + timedelta(minutes=6),
    )
    assert not won and same_owner.attempts == 1
    renewed = catalog.renew_research_submission(
        reserved.submission_id,
        worker_id="worker-a",
        lease_token=first_token,
        renewed_at=NOW + timedelta(minutes=4),
        lease_expires_at=NOW + timedelta(minutes=9),
    )
    assert renewed.lease_expires_at == NOW + timedelta(minutes=9)
    observed, won = catalog.claim_research_submission(
        reserved.submission_id,
        worker_id="worker-b",
        claimed_at=NOW + timedelta(minutes=6),
        lease_expires_at=NOW + timedelta(minutes=11),
    )
    assert not won and observed.lease_owner == "worker-a"
    recovered, won = catalog.claim_research_submission(
        reserved.submission_id,
        worker_id="worker-b",
        claimed_at=NOW + timedelta(minutes=10),
        lease_expires_at=NOW + timedelta(minutes=15),
    )
    assert won and recovered.lease_owner == "worker-b" and recovered.attempts == 2
    terminal = catalog.finish_research_submission(
        recovered.submission_id,
        worker_id="worker-b",
        lease_token=research_submission_lease_token(recovered),
        status="missing_data",
        error="source unavailable",
        finished_at=NOW + timedelta(minutes=11),
    )
    assert terminal.status == "missing_data"
    assert terminal.lease_owner is None and terminal.lease_expires_at is None


def test_expired_same_owner_generation_cannot_finish_with_stale_token(catalog) -> None:
    submission = _coordinator(catalog).submit(
        _payload(), family_id="value_quality_v1"
    ).submission
    assert submission is not None
    reserved = _coordinator(catalog).reserve(submission.submission_id)
    first, won = catalog.claim_research_submission(
        reserved.submission_id,
        worker_id="stable-worker-name",
        claimed_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=5),
    )
    assert won
    stale_token = research_submission_lease_token(first)
    replacement, won = catalog.claim_research_submission(
        reserved.submission_id,
        worker_id="stable-worker-name",
        claimed_at=NOW + timedelta(minutes=6),
        lease_expires_at=NOW + timedelta(minutes=11),
    )
    assert won and replacement.attempts == 2
    assert research_submission_lease_token(replacement) != stale_token
    with pytest.raises(CatalogConflict, match="stale lease"):
        catalog.finish_research_submission(
            reserved.submission_id,
            worker_id="stable-worker-name",
            lease_token=stale_token,
            status="missing_data",
            error="stale worker",
            finished_at=NOW + timedelta(minutes=7),
        )
    terminal = catalog.finish_research_submission(
        reserved.submission_id,
        worker_id="stable-worker-name",
        lease_token=research_submission_lease_token(replacement),
        status="missing_data",
        error="replacement worker",
        finished_at=NOW + timedelta(minutes=7),
    )
    assert terminal.status == "missing_data"


class _FakeCycle:
    def __init__(self, catalog: ResearchCatalog, *, outcome="falsified_or_insufficient"):
        self.catalog = catalog
        self.outcome = outcome

    def run(self, spec, frame, **kwargs):
        experiment = self.catalog.register_experiment(spec)
        trial_id = f"trial_{spec.fingerprint()[:32]}"
        self.catalog.reserve_trial(
            TrialRegistration(
                trial_id=trial_id,
                experiment_fingerprint=spec.fingerprint(),
                hypothesis_id=spec.preregistration.hypothesis_id,
                family=kwargs["trial_family"],
                kind=TrialKind.CONFIRMATORY,
                registered_at=NOW,
                holdout_id=HISTORICAL_HOLDOUT_ID,
                requested_evidence_class=EvidenceClass.PSEUDO_OOS,
                research_equivalence_hash=kwargs["research_equivalence_hash"],
            ),
            candidate_id=spec.candidate_id,
            experiment_id=experiment.experiment_id,
        )
        self.catalog.complete_trial(
            trial_id,
            experiment_id=experiment.experiment_id,
            outcome=(
                TrialOutcome.SUCCESS
                if self.outcome == "promoted_to_shadow"
                else TrialOutcome.FAILURE
            ),
            reason="deterministic fake result",
            completed_at=NOW,
        )
        lifecycle_states = (
            LifecycleState.PREREGISTERED,
            LifecycleState.CANARY,
            LifecycleState.WALK_FORWARD,
        )
        self.catalog.append_lifecycle_path(
            tuple(
                LifecycleEvent(
                    idempotency_key=f"{spec.fingerprint()}:{state.value}",
                    sleeve_id=f"research_attempt:{spec.fingerprint()}",
                    from_state=(None if index == 0 else lifecycle_states[index - 1]),
                    to_state=state,
                    cause="deterministic_research_cycle",
                    occurred_at=NOW + timedelta(microseconds=index),
                    evidence={"experiment_fingerprint": spec.fingerprint()},
                )
                for index, state in enumerate(lifecycle_states)
            )
        )
        result = ResearchCycleResult(
            experiment_id=experiment.experiment_id,
            fingerprint=spec.fingerprint(),
            status="completed",
            promotion_verdict=(
                "promote" if self.outcome == "promoted_to_shadow" else "reject"
            ),
            lifecycle_state="walk_forward",
            metrics={},
            failures=(),
            fold_results=(),
            statistical_evidence={},
        )
        self.catalog.record_authoritative_result(
            experiment.experiment_id,
            outcome=self.outcome,
            metrics=result.to_dict(),
            completed_at=NOW + timedelta(microseconds=len(lifecycle_states)),
        )
        return result


def test_coordinator_renews_during_cycle_and_prevents_expired_takeover(
    catalog, monkeypatch
) -> None:
    renewed = Event()
    observed: dict[str, datetime] = {}
    real_renew = catalog.renew_research_submission

    def recording_renew(submission_id, **kwargs):
        before = catalog.get_research_submission(submission_id)
        assert before is not None and before.lease_expires_at is not None
        result = real_renew(submission_id, **kwargs)
        observed["previous_expiry"] = before.lease_expires_at
        renewed.set()
        return result

    monkeypatch.setattr(catalog, "renew_research_submission", recording_renew)

    class ConcurrentProbeCycle(_FakeCycle):
        def run(self, spec, frame, **kwargs):
            assert renewed.wait(3), "coordinator did not heartbeat its lease"
            previous_expiry = observed["previous_expiry"]
            current, won = catalog.claim_research_submission(
                submission.submission_id,
                worker_id="worker-b",
                claimed_at=previous_expiry + timedelta(microseconds=1),
                lease_expires_at=previous_expiry + timedelta(minutes=5),
            )
            assert not won and current.lease_owner == "worker-a"
            return super().run(spec, frame, **kwargs)

    cycle = ConcurrentProbeCycle(catalog)
    coordinator = _coordinator(catalog, cycle=cycle)
    submission = coordinator.submit(_payload(), family_id="value_quality_v1").submission
    assert submission is not None
    outcome = coordinator.run(
        submission.submission_id,
        worker_id="worker-a",
        lease_seconds=1,
    )
    assert outcome.claimed
    assert outcome.submission.status == "completed"


def test_coordinator_cannot_terminalize_after_heartbeat_loses_lease(
    catalog, monkeypatch
) -> None:
    renewal_failed = Event()

    def reject_renewal(*args, **kwargs):
        renewal_failed.set()
        raise CatalogConflict("simulated lease loss")

    monkeypatch.setattr(catalog, "renew_research_submission", reject_renewal)

    class TakeoverCycle:
        submission_id: str

        def run(self, spec, frame, **kwargs):
            assert renewal_failed.wait(3), "coordinator did not attempt lease renewal"
            current = catalog.get_research_submission(self.submission_id)
            assert current is not None and current.lease_expires_at is not None
            replacement, won = catalog.claim_research_submission(
                self.submission_id,
                worker_id="worker-b",
                claimed_at=current.lease_expires_at + timedelta(microseconds=1),
                lease_expires_at=current.lease_expires_at + timedelta(minutes=5),
            )
            assert won and replacement.attempts == 2
            return ResearchCycleResult(
                experiment_id="stale_worker_result",
                fingerprint=spec.fingerprint(),
                status="completed",
                promotion_verdict="reject",
                lifecycle_state="walk_forward",
                metrics={},
                failures=(),
                fold_results=(),
                statistical_evidence={},
            )

    cycle = TakeoverCycle()
    coordinator = _coordinator(catalog, cycle=cycle)
    submission = coordinator.submit(_payload(), family_id="value_quality_v1").submission
    assert submission is not None
    cycle.submission_id = submission.submission_id
    outcome = coordinator.run(
        submission.submission_id,
        worker_id="worker-a",
        lease_seconds=1,
    )
    assert not outcome.claimed
    assert outcome.submission.status == "running"
    assert outcome.submission.lease_owner == "worker-b"
    assert outcome.submission.experiment_id is None


def test_run_completed_without_promoting_falsified_sleeve(catalog) -> None:
    completed_coordinator = _coordinator(catalog, cycle=_FakeCycle(catalog))
    completed = completed_coordinator.submit(
        _payload(), family_id="value_quality_v1"
    ).submission
    assert completed is not None
    outcome = completed_coordinator.run(completed.submission_id, worker_id="worker")
    assert outcome.submission.status == "completed"
    assert outcome.shadow_account_id is None


def test_run_missing_gold_finishes_missing_data(catalog) -> None:
    switch = {"missing": False}

    def resolver():
        if switch["missing"]:
            raise AuthoritativeResearchInputUnavailable("Gold object disappeared")
        return _inputs()

    missing_coordinator = _coordinator(catalog, resolver=resolver)
    missing_payload = _payload(node_ids=("raw_missing", "rank_missing"))
    missing_payload["factor"]["expression"] = {
        "output_id": "lag_missing",
        "nodes": [
            {"id": "raw_missing", "op": "field", "field": "book_to_price"},
            {"id": "lag_missing", "op": "lag", "input": "raw_missing", "periods": 1},
        ],
    }
    missing = missing_coordinator.submit(
        missing_payload, family_id="value_quality_v1"
    ).submission
    assert missing is not None
    switch["missing"] = True
    outcome = missing_coordinator.run(missing.submission_id, worker_id="worker")
    assert outcome.submission.status == "missing_data"


def test_run_unexpected_evaluator_error_finishes_failed(catalog) -> None:
    class BrokenCycle:
        def run(self, spec, frame, **kwargs):
            raise RuntimeError("deterministic evaluator failure")

    failed_coordinator = _coordinator(catalog, cycle=BrokenCycle())
    failed_payload = _payload(node_ids=("raw_failed", "rank_failed"))
    failed_payload["factor"]["expression"] = {
        "output_id": "lag_failed",
        "nodes": [
            {"id": "raw_failed", "op": "field", "field": "book_to_price"},
            {"id": "lag_failed", "op": "lag", "input": "raw_failed", "periods": 2},
        ],
    }
    failed = failed_coordinator.submit(
        failed_payload, family_id="value_quality_v1"
    ).submission
    assert failed is not None
    outcome = failed_coordinator.run(failed.submission_id, worker_id="worker")
    assert outcome.submission.status == "failed"


def test_only_authoritative_promoted_sleeve_creates_challenger_shadow(catalog) -> None:
    coordinator = _coordinator(
        catalog, cycle=_FakeCycle(catalog, outcome="promoted_to_shadow")
    )
    submission = coordinator.submit(
        _payload(), family_id="value_quality_v1"
    ).submission
    assert submission is not None
    outcome = coordinator.run(submission.submission_id, worker_id="worker")
    assert outcome.submission.status == "completed"
    assert outcome.shadow_account_id is not None
    assert catalog.get_shadow_account(outcome.shadow_account_id) is not None
    events = catalog.list_lifecycle_events(
        sleeve_id=submission.spec.sleeve.sleeve_id, limit=100
    )
    assert any(event.cause == "challenger_shadow_account_bound" for event in events)


def test_completed_promotion_materialization_resumes_after_roster_crash(
    catalog, monkeypatch
) -> None:
    coordinator = _coordinator(
        catalog, cycle=_FakeCycle(catalog, outcome="promoted_to_shadow")
    )
    submission = coordinator.submit(
        _payload(), family_id="value_quality_v1"
    ).submission
    assert submission is not None
    real_persist = sleeve_registry.persist_sleeve_roster
    calls = {"count": 0}

    def persist_then_crash(*args, **kwargs):
        result = real_persist(*args, **kwargs)
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("simulated crash after roster persistence")
        return result

    monkeypatch.setattr(
        sleeve_registry, "persist_sleeve_roster", persist_then_crash
    )
    first = coordinator.run(submission.submission_id, worker_id="worker-a")

    assert first.submission.status == "completed"
    assert first.promotion_pending is True
    assert first.promotion_error == "RuntimeError"
    assert first.shadow_account_id is None
    assert catalog.get_authoritative_result(first.submission.experiment_id).outcome == (
        "promoted_to_shadow"
    )

    monkeypatch.setattr(
        sleeve_registry, "persist_sleeve_roster", real_persist
    )
    resumed = coordinator.resume(worker_id="worker-b")

    assert len(resumed) == 1
    assert resumed[0].submission.status == "completed"
    assert resumed[0].promotion_pending is False
    assert resumed[0].shadow_account_id is not None
    assert catalog.get_shadow_account(resumed[0].shadow_account_id) is not None


def test_completed_promotion_resumes_recovery_registration_and_binding(
    catalog, monkeypatch
) -> None:
    recovery_case_id = "recovery-monthly-value-quality"
    catalog.save_recovery_case(
        RecoveryCase(
            recovery_case_id=recovery_case_id,
            sleeve_id=_registered_sleeve().sleeve_id,
            lifecycle_state=LifecycleState.DORMANT,
            triggered_at=NOW - timedelta(days=2),
            drift_event_due_at=NOW - timedelta(days=1),
            diagnosis_due_at=NOW + timedelta(days=18),
            earliest_recovery_review_at=NOW + timedelta(days=58),
        )
    )
    RecoveryCoordinator(catalog).complete_diagnosis(
        recovery_case_id,
        diagnosed_at=NOW - timedelta(days=1),
        snapshot_id=_snapshot().snapshot_id,
        findings={"cause": "simulated signal decay"},
    )
    coordinator = _coordinator(
        catalog, cycle=_FakeCycle(catalog, outcome="promoted_to_shadow")
    )
    submission = coordinator.submit(
        _payload(),
        family_id="value_quality_v1",
        recovery_case_id=recovery_case_id,
    ).submission
    assert submission is not None

    real_register = RecoveryCoordinator.register_challengers
    register_calls = {"count": 0}

    def register_then_crash(self, *args, **kwargs):
        result = real_register(self, *args, **kwargs)
        register_calls["count"] += 1
        if register_calls["count"] == 1:
            raise RuntimeError("simulated crash after recovery registration")
        return result

    monkeypatch.setattr(
        RecoveryCoordinator, "register_challengers", register_then_crash
    )
    first = coordinator.run(submission.submission_id, worker_id="worker-a")
    assert first.submission.status == "completed"
    assert first.promotion_pending is True
    case_after_registration = catalog.get_recovery_case(recovery_case_id)
    assert case_after_registration is not None
    assert first.submission.experiment_id in case_after_registration.challenger_ids

    monkeypatch.setattr(
        RecoveryCoordinator, "register_challengers", real_register
    )
    resumed = coordinator.resume(worker_id="worker-b")
    assert len(resumed) == 1
    assert resumed[0].promotion_pending is False
    assert resumed[0].shadow_account_id is not None
    recovery_bindings = [
        event
        for event in catalog.list_lifecycle_events(
            sleeve_id=_registered_sleeve().sleeve_id, limit=1_000
        )
        if event.cause == "recovery_challenger_shadow_bound"
        and event.evidence.get("recovery_case_id") == recovery_case_id
        and event.evidence.get("challenger_id")
        == first.submission.experiment_id
        and event.evidence.get("account_id") == resumed[0].shadow_account_id
    ]
    assert len(recovery_bindings) == 1
