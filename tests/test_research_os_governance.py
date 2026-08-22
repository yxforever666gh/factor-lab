from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from factor_lab.research_os.contracts import PromotionCriteria, TrialOutcome, ValidationProtocol
from factor_lab.research_os.catalog import TrialLedgerEntry
from factor_lab.research_os.governance import (
    EvidenceClass,
    PromotionEvidence,
    PromotionMetrics,
    TrialKind,
    TrialLedger,
    TrialRecord,
    TrialRegistration,
    assess_candidate_promotion,
    evaluate_promotion,
)
from factor_lab.research_os.negative_controls import (
    NegativeControlMetric,
    evaluate_negative_control_gate,
    generate_negative_control_signals,
    reverse_direction_signal,
)


def _registration(
    suffix: str,
    *,
    family: str = "value",
    kind: TrialKind = TrialKind.CONFIRMATORY,
    holdout: str | None = None,
    day: int = 1,
    diagnostic_branch: int | None = None,
) -> TrialRegistration:
    return TrialRegistration(
        trial_id=f"trial-{suffix}",
        experiment_fingerprint=f"fingerprint-{suffix}",
        hypothesis_id="hypothesis-a",
        family=family,
        kind=kind,
        registered_at=datetime(2026, 8, day, tzinfo=timezone.utc),
        holdout_id=holdout,
        requested_evidence_class=EvidenceClass.PRISTINE_FORWARD,
        diagnostic_branch=diagnostic_branch,
    )


def _record(registration: TrialRegistration, outcome: TrialOutcome = TrialOutcome.FAILURE) -> TrialRecord:
    return TrialRecord(
        registration=registration,
        outcome=outcome,
        p_value=None if outcome is TrialOutcome.MISSING_DATA else 0.5,
    )


def test_trial_ledger_enforces_duplicate_monthly_family_and_holdout_rules() -> None:
    prior = _registration("1", holdout="forward-2026-08")
    ledger = TrialLedger([_record(prior, TrialOutcome.MISSING_DATA)])
    duplicate = ledger.admit(prior)
    assert not duplicate.allowed
    assert "duplicate_trial_id" in duplicate.reasons
    same_family = ledger.admit(_registration("2", family="value", holdout="forward-2026-09", day=2))
    assert not same_family.allowed
    assert "monthly_family_confirmation_budget_exhausted" in same_family.reasons
    reused = ledger.admit(_registration("3", family="quality", holdout="forward-2026-08", day=3))
    assert reused.allowed
    assert reused.evidence_class is EvidenceClass.OBSERVED
    assert reused.family_trial_index == 1
    assert ledger.family_p_values("value") == (1.0,)


def test_trial_ledger_limits_global_monthly_confirmations_and_diagnostics() -> None:
    ledger = TrialLedger(
        [
            _record(_registration("1", family="a")),
            _record(_registration("2", family="b")),
            _record(_registration("3", family="c")),
            _record(_registration("d1", kind=TrialKind.DIAGNOSTIC, diagnostic_branch=1)),
            _record(_registration("d2", kind=TrialKind.DIAGNOSTIC, diagnostic_branch=2)),
        ]
    )
    fourth = ledger.admit(_registration("4", family="d"))
    assert not fourth.allowed
    assert "monthly_confirmation_budget_exhausted" in fourth.reasons
    third_diagnostic = ledger.admit(_registration("d3", kind=TrialKind.DIAGNOSTIC, diagnostic_branch=3))
    assert not third_diagnostic.allowed
    assert "diagnostic_branch_budget_exhausted" in third_diagnostic.reasons


def test_trial_ledger_adapts_authoritative_catalog_entries() -> None:
    occurred_at = datetime(2026, 8, 5, tzinfo=timezone.utc)
    entry = TrialLedgerEntry(
        trial_id="catalog-trial",
        experiment_id="experiment-1",
        family="quality",
        candidate_id="quality-value",
        outcome=TrialOutcome.FAILURE,
        reason="did_not_clear_gate",
        p_value=0.2,
        alpha_spent=0.01,
        occurred_at=occurred_at,
        metadata={
            "experiment_fingerprint": "f" * 64,
            "hypothesis_id": "quality-hypothesis",
            "trial_kind": "confirmatory",
            "holdout_id": "outer-2025",
            "evidence_class": "pseudo_oos",
        },
    )
    ledger = TrialLedger.from_catalog_entries([entry])
    record = ledger.records[0]
    assert record.registration.experiment_fingerprint == "f" * 64
    assert record.registration.kind is TrialKind.CONFIRMATORY
    assert record.outcome is TrialOutcome.FAILURE
    assert ledger.family_p_values("quality") == (0.2,)


def _passing_metrics() -> PromotionMetrics:
    return PromotionMetrics(
        net_excess_annual_return=0.08,
        net_sharpe=0.9,
        information_ratio=0.6,
        max_drawdown=-0.20,
        positive_half_year_ratio=0.70,
        positive_outer_years=4,
        evaluated_outer_years=5,
        capacity_violations=0,
    )


def _passing_evidence() -> PromotionEvidence:
    return PromotionEvidence(
        statistical_budget_passed=True,
        holm_passed=True,
        deflated_sharpe_probability=0.97,
        bootstrap_probability_positive=0.96,
        negative_controls_passed=True,
    )


def test_promotion_is_all_hard_gates_with_no_rounding_override() -> None:
    criteria = PromotionCriteria()
    passed = evaluate_promotion(_passing_metrics(), criteria, _passing_evidence())
    assert passed.promoted
    assert passed.verdict == "promote"

    almost = PromotionMetrics(**{**_passing_metrics().__dict__, "net_sharpe": 0.79818766})
    rejected = evaluate_promotion(almost, criteria, _passing_evidence())
    assert not rejected.promoted
    assert rejected.failures == ("net_sharpe",)


def test_promotion_fails_closed_on_data_statistics_controls_and_diagnostic_periods() -> None:
    evidence = PromotionEvidence(
        data_audit_blockers=("st_history_unverified",),
        statistical_budget_passed=False,
        holm_passed=False,
        deflated_sharpe_probability=0.5,
        bootstrap_probability_positive=0.6,
        negative_controls_passed=False,
        diagnostic_only=True,
    )
    result = evaluate_promotion(_passing_metrics(), PromotionCriteria(), evidence)
    assert not result.promoted
    assert result.verdict == "diagnostic_only"
    assert {
        "clean_data",
        "statistical_budget",
        "holm_family_test",
        "deflated_sharpe",
        "block_bootstrap",
        "negative_controls",
        "promotion_window",
    }.issubset(result.failures)


def test_negative_controls_are_deterministic_and_preserve_cross_section_marginals() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["A", "B", "C"] * 30,
            "date": [date for date in pd.bdate_range("2024-01-02", periods=30) for _ in range(3)],
        }
    )
    signal = pd.Series(range(len(frame)), dtype=float)
    first = generate_negative_control_signals(frame, signal, seed=19)
    second = generate_negative_control_signals(frame, signal, seed=19)
    assert first.keys() == second.keys()
    for name in first:
        pd.testing.assert_series_equal(first[name], second[name])
    for _, positions in frame.groupby("date").groups.items():
        assert sorted(first["cross_section_permutation"].loc[positions]) == sorted(signal.loc[positions])
    assert reverse_direction_signal(signal).equals((-signal).rename("reverse_direction"))


def test_negative_control_gate_requires_controls_and_rejects_any_promoted_control() -> None:
    missing = evaluate_negative_control_gate([])
    assert not missing.passed
    clean = evaluate_negative_control_gate(
        [NegativeControlMetric("permutation", -0.1), NegativeControlMetric("random_rank", 0.0)]
    )
    assert clean.passed
    contaminated = evaluate_negative_control_gate(
        [NegativeControlMetric("permutation", 0.9, passed_promotion_gate=True)]
    )
    assert not contaminated.passed


def test_composite_assessment_connects_oos_ledger_statistics_and_controls() -> None:
    registration = _registration("composite", family="low-risk")
    ledger = TrialLedger(
        [
            TrialRecord(
                registration=registration,
                outcome=TrialOutcome.SUCCESS,
                p_value=1e-10,
                alpha_allocated=0.005,
            )
        ]
    )
    returns = [0.006 + (index % 5) * 0.0001 for index in range(120)]
    controls = [
        NegativeControlMetric("permutation", metric=-0.1),
        NegativeControlMetric("time_shift", metric=0.0),
    ]
    assessment = assess_candidate_promotion(
        _passing_metrics(),
        ValidationProtocol(),
        family="low-risk",
        trial_ledger=ledger,
        candidate_trial_id="trial-composite",
        stitched_outer_oos_returns=returns,
        outer_fold_ids=[f"outer-{year}" for year in range(2021, 2026)],
        within_family_p_values=[1e-10],
        lifetime_trial_sharpes=[3.0],
        negative_control_results=controls,
        bootstrap_resamples=200,
        seed=2,
    )
    assert assessment.promotion.promoted
    assert assessment.online_alpha[-1].rejected
    assert assessment.holm_adjusted_p_values == (1e-10,)
    assert assessment.deflated_sharpe is not None
    assert assessment.block_bootstrap is not None
    assert not assessment.evidence.methodology_blockers

    incomplete = assess_candidate_promotion(
        _passing_metrics(),
        ValidationProtocol(),
        family="low-risk",
        trial_ledger=ledger,
        candidate_trial_id="trial-composite",
        stitched_outer_oos_returns=returns,
        outer_fold_ids=["outer-2021", "outer-2022"],
        within_family_p_values=[1e-10],
        lifetime_trial_sharpes=[3.0],
        negative_control_results=controls,
        bootstrap_resamples=100,
    )
    assert not incomplete.promotion.promoted
    assert "methodology_integrity" in incomplete.promotion.failures
