from datetime import date, timedelta

from factor_lab.research_os.lifecycle import (
    RecoveryCaseProgress,
    SleeveHealthObservation,
    SleeveLifecycleRecord,
    SleeveState,
    advance_lifecycle,
)


def _bad(day: date, *, sessions: int = 0) -> SleeveHealthObservation:
    return SleeveHealthObservation(
        as_of_date=day,
        active_ir_13w=-0.2,
        active_ir_26w=-0.1,
        ic_26w=-0.01,
        cost_ratio_to_baseline=1.0,
        active_drawdown=-0.1,
        drawdown_95_limit=-0.25,
        new_sessions_since_dormant=sessions,
    )


def test_sleeve_degrades_dormant_and_recovers_only_with_new_data():
    start = date(2026, 1, 2)
    record = SleeveLifecycleRecord(
        sleeve_id="low_risk",
        state=SleeveState.ACTIVE,
        target_weight=0.25,
        effective_weight=0.25,
    )
    first = advance_lifecycle(record, _bad(start))
    assert first.record.state == SleeveState.ACTIVE
    second = advance_lifecycle(first.record, _bad(start + timedelta(days=7)))
    assert second.record.state == SleeveState.REDUCED
    assert second.record.effective_weight == 0.125

    current = second.record
    for week in range(2, 5):
        current = advance_lifecycle(current, _bad(start + timedelta(days=7 * week))).record
    assert current.state == SleeveState.DORMANT
    assert current.effective_weight == 0

    too_early = SleeveHealthObservation(
        as_of_date=start + timedelta(days=70),
        active_ir_13w=0.2,
        active_ir_26w=0.1,
        ic_26w=0.01,
        active_return_20d=0.02,
        active_return_60d=0.05,
        new_sessions_since_dormant=59,
    )
    assert advance_lifecycle(current, too_early).record.state == SleeveState.DORMANT
    ready = SleeveHealthObservation(
        **{**too_early.__dict__, "as_of_date": start + timedelta(days=71), "new_sessions_since_dormant": 60}
    )
    probation = advance_lifecycle(current, ready).record
    assert probation.state == SleeveState.PROBATION
    assert probation.effective_weight == 0.05

    for week in range(3):
        clean = SleeveHealthObservation(
            **{**ready.__dict__, "as_of_date": ready.as_of_date + timedelta(days=7 * (week + 1))}
        )
        probation = advance_lifecycle(probation, clean).record
    assert probation.state == SleeveState.ACTIVE
    assert probation.effective_weight <= probation.target_weight


def test_data_failure_freezes_and_requires_explicit_revalidation():
    record = SleeveLifecycleRecord("value", SleeveState.ACTIVE, 0.25, 0.25)
    failed = SleeveHealthObservation(date(2026, 2, 1), 1, 1, 0.1, data_quality_ok=False)
    frozen = advance_lifecycle(record, failed).record
    assert frozen.state == SleeveState.FROZEN_DATA
    assert frozen.effective_weight == 0
    clean = SleeveHealthObservation(date(2026, 2, 8), 1, 1, 0.1)
    assert advance_lifecycle(frozen, clean).record.state == SleeveState.FROZEN_DATA
    revalidated = SleeveHealthObservation(
        date(2026, 2, 15), 1, 1, 0.1, data_revalidation_passed=True
    )
    assert advance_lifecycle(frozen, revalidated).record.state == SleeveState.DORMANT


def test_recovery_sla_is_process_not_profit_claim():
    progress = RecoveryCaseProgress(
        case_id="case-1",
        opened_session=100,
        drift_registered_session=104,
        diagnosis_session=115,
        challenger_session=119,
        shadow_start_session=120,
        current_session=180,
    ).status()
    assert progress["detection_met"] is True
    assert progress["diagnosis_met"] is True
    assert progress["challenger_met"] is True
    assert progress["recovery_observation_complete"] is True
