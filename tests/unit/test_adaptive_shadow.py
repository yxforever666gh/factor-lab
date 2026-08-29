from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from typing import Any, Mapping

import pytest

from factor_lab.adaptive_shadow import (
    AdaptiveShadowError,
    CandidateSpec,
    OutcomeObservation,
    Registry,
    SelectionSpec,
    assess_pairwise_promotion,
    assess_plan_timing,
    generate_targets,
    normalize_input_rows,
)


COMMIT = "a" * 40


def _candidate(
    candidate_id: str,
    *,
    field: str = "alpha",
    direction: int = 1,
    start_after: str = "2026-08-30",
    top_n: int = 2,
    retention_n: int = 3,
) -> CandidateSpec:
    return CandidateSpec(
        candidate_id=candidate_id,
        version=1,
        formula=f"rank({field})",
        required_fields=(field,),
        direction=direction,
        selection=SelectionSpec(top_n=top_n, retention_n=retention_n),
        selection_disclosure=f"Top-{top_n}; retain through Top-{retention_n}",
        start_after=start_after,
    )


def _registry(*candidates: CandidateSpec) -> Registry:
    values = candidates or (_candidate("challenger-a"), _candidate("challenger-b"))
    return Registry(
        protocol_version="5.9-shadow-v1",
        release_tag="5.9",
        commit_oid=COMMIT,
        released_at_utc="2026-08-30T01:02:03Z",
        candidates=tuple(values),
    )


def _row(
    ticker: str,
    alpha: Any,
    *,
    row_date: str = "2026-08-31",
    eligible: bool = True,
    hidden: float = 999.0,
) -> dict[str, Any]:
    return {
        "date": row_date,
        "ticker": ticker,
        "eligible": eligible,
        "alpha": alpha,
        "hidden_future_like_field": hidden,
    }


def _outcome(
    candidate_id: str,
    net_return: float,
    *,
    signal_date: str = "2026-08-31",
    start_date: str = "2026-09-01",
    end_date: str = "2026-09-15",
) -> OutcomeObservation:
    return OutcomeObservation(
        candidate_id=candidate_id,
        signal_date=signal_date,
        start_date=start_date,
        end_date=end_date,
        net_return=net_return,
    )


def test_registry_is_frozen_order_normalized_and_content_addressed() -> None:
    alpha = _candidate("alpha", field="zeta")
    beta = _candidate("beta", field="beta")
    left = _registry(beta, alpha)
    right = _registry(alpha, beta)

    assert tuple(candidate.candidate_id for candidate in left.candidates) == ("alpha", "beta")
    assert left.sha256 == right.sha256
    assert left.to_payload()["protocol_version"] == "5.9-shadow-v1"
    assert left.to_payload()["release_tag"] == "5.9"
    assert left.to_payload()["commit_oid"] == COMMIT
    assert left.to_payload()["released_at_utc"] == "2026-08-30T01:02:03Z"
    assert alpha.to_payload() == {
        "candidate_id": "alpha",
        "direction": 1,
        "formula": "rank(zeta)",
        "required_fields": ["zeta"],
        "selection": {
            "retention_n": 3,
            "top_n": 2,
            "weighting": "equal_weight_long_only",
        },
        "selection_disclosure": "Top-2; retain through Top-3",
        "start_after": "2026-08-30",
        "version": 1,
    }
    with pytest.raises(FrozenInstanceError):
        left.release_tag = "forged"  # type: ignore[misc]


def test_every_identity_disclosure_changes_registry_sha() -> None:
    base_candidate = _candidate("alpha")
    base = _registry(base_candidate)
    candidate_variants = [
        replace(base_candidate, formula="rank(alpha) + rank(beta)"),
        replace(base_candidate, version=2),
        replace(base_candidate, required_fields=("alpha", "beta")),
        replace(base_candidate, direction=-1),
        replace(base_candidate, selection=SelectionSpec(top_n=3, retention_n=4)),
        replace(base_candidate, selection_disclosure="different disclosure"),
        replace(base_candidate, start_after="2026-08-31"),
    ]
    assert all(_registry(value).sha256 != base.sha256 for value in candidate_variants)
    registry_variants = [
        replace(base, protocol_version="5.9-shadow-v2"),
        replace(base, release_tag="6.0"),
        replace(base, commit_oid="b" * 40),
        replace(base, released_at_utc="2026-08-30T01:02:04Z"),
    ]
    assert all(value.sha256 != base.sha256 for value in registry_variants)


def test_candidate_version_and_release_tag_are_strict() -> None:
    with pytest.raises(AdaptiveShadowError, match="positive integer"):
        replace(_candidate("candidate"), version=0)
    for invalid in ("5", "5.9.0", "05.9", "5.09", " 5.9"):
        with pytest.raises(AdaptiveShadowError, match="canonical major.minor"):
            replace(_registry(_candidate("candidate")), release_tag=invalid)


def test_required_field_order_is_canonical_but_duplicates_fail() -> None:
    left = CandidateSpec(
        candidate_id="candidate",
        version=1,
        formula="generic",
        required_fields=("z", "a"),
        direction=1,
        selection=SelectionSpec(2, 3),
        selection_disclosure="generic selection",
        start_after="2026-08-30",
    )
    right = replace(left, required_fields=("a", "z"))
    assert left.required_fields == ("a", "z")
    assert left.sha256 == right.sha256
    with pytest.raises(AdaptiveShadowError, match="duplicates"):
        replace(left, required_fields=("a", "a"))


def test_registry_rejects_candidate_explosion_and_duplicate_ids() -> None:
    with pytest.raises(AdaptiveShadowError, match="more than 3"):
        _registry(*(_candidate(f"candidate-{index}") for index in range(4)))
    with pytest.raises(AdaptiveShadowError, match="IDs must be unique"):
        _registry(_candidate("same"), _candidate("same", field="other"))


def test_target_generation_is_row_order_invariant_allowlisted_and_equal_weight() -> None:
    registry = _registry(_candidate("candidate"))
    rows = [_row("CCC", 8.0), _row("AAA", 10.0), _row("BBB", 9.0)]
    observed_views: list[Mapping[str, Any]] = []

    def scorer(row: Mapping[str, Any]) -> float:
        observed_views.append(row)
        assert set(row) == {"date", "ticker", "eligible", "alpha"}
        return float(row["alpha"])

    left = generate_targets(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=rows,
        score=scorer,
    )
    right = generate_targets(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=list(reversed(rows)),
        score=lambda row: row["alpha"],
    )

    assert left == right
    assert left.selected_tickers == ("AAA", "BBB")
    assert dict(left.targets_ppm) == {"AAA": 500_000, "BBB": 500_000}
    assert all(weight > 0 for weight in dict(left.targets_ppm).values())
    assert sum(dict(left.targets_ppm).values()) == 1_000_000
    assert left.to_payload()["targets_ppm"] == {"AAA": 500_000, "BBB": 500_000}
    assert len(left.sha256) == 64
    assert len(observed_views) == 3


def test_target_ties_use_ticker_ascending_and_direction_is_applied() -> None:
    rows = [_row("BBB", 1.0), _row("AAA", 1.0), _row("CCC", 2.0)]
    positive = generate_targets(
        registry=_registry(_candidate("positive")),
        candidate_id="positive",
        signal_date="2026-08-31",
        rows=rows,
        score=lambda row: row["alpha"],
    )
    negative = generate_targets(
        registry=_registry(_candidate("negative", direction=-1)),
        candidate_id="negative",
        signal_date="2026-08-31",
        rows=rows,
        score=lambda row: row["alpha"],
    )
    assert positive.ranked_tickers == ("CCC", "AAA", "BBB")
    assert negative.ranked_tickers == ("AAA", "BBB", "CCC")


def test_integer_ppm_remainder_is_assigned_by_ticker_not_input_order() -> None:
    registry = _registry(_candidate("candidate", top_n=3, retention_n=4))
    plan = generate_targets(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=[_row("CCC", 1.0), _row("AAA", 3.0), _row("BBB", 2.0)],
        score=lambda row: row["alpha"],
    )
    assert plan.targets_ppm == (
        ("AAA", 333_334),
        ("BBB", 333_333),
        ("CCC", 333_333),
    )
    assert sum(dict(plan.targets_ppm).values()) == 1_000_000


def test_top_n_retention_keeps_prior_name_inside_retention_band() -> None:
    rows = [_row("A", 4.0), _row("B", 3.0), _row("C", 2.0), _row("D", 1.0)]
    plan = generate_targets(
        registry=_registry(_candidate("candidate")),
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=rows,
        score=lambda row: row["alpha"],
        previous_targets=("C", "D"),
    )
    assert plan.selected_tickers == ("A", "C")


def test_ineligible_rows_are_filtered_and_may_lack_candidate_features() -> None:
    rows = [
        _row("A", 3.0),
        _row("B", 2.0),
        {"date": "2026-08-31", "ticker": "BROKEN", "eligible": False},
        _row("C", 1.0, eligible=False),
    ]
    plan = generate_targets(
        registry=_registry(_candidate("candidate")),
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=rows,
        score=lambda row: row["alpha"],
    )
    assert plan.selected_tickers == ("A", "B")


@pytest.mark.parametrize("invalid", [None, float("nan"), float("inf"), "1.0", True])
def test_eligible_required_fields_and_scores_must_be_finite_numbers(invalid: Any) -> None:
    registry = _registry(_candidate("candidate"))
    rows = [_row("A", 2.0), _row("B", invalid), _row("C", 1.0)]
    with pytest.raises(AdaptiveShadowError, match="finite number"):
        generate_targets(
            registry=registry,
            candidate_id="candidate",
            signal_date="2026-08-31",
            rows=rows,
            score=lambda row: row["alpha"],
        )

    valid_rows = [_row("A", 3.0), _row("B", 2.0), _row("C", 1.0)]
    with pytest.raises(AdaptiveShadowError, match="finite number"):
        generate_targets(
            registry=registry,
            candidate_id="candidate",
            signal_date="2026-08-31",
            rows=valid_rows,
            score=lambda _row: invalid,
        )


def test_duplicate_input_identity_collapses_only_when_identical() -> None:
    row = _row("A", 1.0)
    normalized = normalize_input_rows([row, dict(row)], ("alpha",))
    assert len(normalized) == 1
    conflict = dict(row)
    conflict["alpha"] = 2.0
    with pytest.raises(AdaptiveShadowError, match="conflicting duplicate input key"):
        normalize_input_rows([row, conflict], ("alpha",))


def test_appending_future_rows_cannot_change_historical_target() -> None:
    registry = _registry(_candidate("candidate"))
    historical = [_row("A", 3.0), _row("B", 2.0), _row("C", 1.0)]
    base = generate_targets(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=historical,
        score=lambda row: row["alpha"],
    )
    future: list[Mapping[str, Any]] = [
        {"date": "2026-09-01", "ticker": "A", "eligible": True, "alpha": 1e99},
        # Only the future date is observed; even malformed future payload fields
        # cannot invalidate an already requested historical cross-section.
        {"date": "2026-09-02"},
    ]
    appended = generate_targets(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        rows=[*future, *historical],
        score=lambda row: row["alpha"],
    )
    assert appended == base


def test_plan_timing_is_strict_after_release_and_start_but_inclusive_at_deadline() -> None:
    registry = _registry(_candidate("candidate"))
    accepted = assess_plan_timing(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        created_at_utc="2026-09-01T01:15:00Z",
        deadline_at_utc="2026-09-01T01:15:00Z",
    )
    assert accepted.admissible is True
    assert accepted.backfill_forbidden is False

    release_boundary = assess_plan_timing(
        registry=registry,
        candidate_id="candidate",
        signal_date="2026-08-30",
        created_at_utc="2026-08-31T01:00:00Z",
        deadline_at_utc="2026-08-31T01:15:00Z",
    )
    assert release_boundary.reason == "signal_not_after_registry_release"
    assert release_boundary.backfill_forbidden is True

    start_boundary_registry = _registry(
        _candidate("candidate", start_after="2026-08-31")
    )
    start_boundary = assess_plan_timing(
        registry=start_boundary_registry,
        candidate_id="candidate",
        signal_date="2026-08-31",
        created_at_utc="2026-09-01T01:00:00Z",
        deadline_at_utc="2026-09-01T01:15:00Z",
    )
    assert start_boundary.reason == "signal_not_after_candidate_start"


def test_missed_deadline_is_permanent_backfill_rejection() -> None:
    decision = assess_plan_timing(
        registry=_registry(_candidate("candidate")),
        candidate_id="candidate",
        signal_date="2026-08-31",
        created_at_utc="2026-09-01T01:15:00.000001Z",
        deadline_at_utc="2026-09-01T01:15:00Z",
    )
    assert decision.admissible is False
    assert decision.missed_deadline is True
    assert decision.backfill_forbidden is True
    assert decision.reason == "missed_deadline"


def test_identical_candidate_outcomes_cannot_be_promoted() -> None:
    registry = _registry(_candidate("challenger"))
    outcomes: list[OutcomeObservation] = []
    for signal, start, end in (
        ("2026-08-31", "2026-09-01", "2026-09-15"),
        ("2026-09-10", "2026-09-11", "2026-09-25"),
        ("2026-09-20", "2026-09-21", "2026-10-05"),
    ):
        outcomes.extend(
            [
                _outcome(
                    "anchor",
                    0.02,
                    signal_date=signal,
                    start_date=start,
                    end_date=end,
                ),
                _outcome(
                    "challenger",
                    0.02,
                    signal_date=signal,
                    start_date=start,
                    end_date=end,
                ),
            ]
        )
    assessment = assess_pairwise_promotion(
        registry=registry,
        evaluation_signal_date="2026-10-20",
        anchor_id="anchor",
        challenger_id="challenger",
        outcomes=outcomes,
        minimum_complete_cohorts=3,
    )
    assert assessment.complete_cohort_count == 3
    assert assessment.mean_excess_return == 0.0
    assert assessment.positive_excess_ratio == 0.0
    assert assessment.eligible_for_major_review is False
    assert assessment.reason == "strict_pairwise_gate_failed"


def test_pairwise_promotion_requires_complete_matured_strict_superiority() -> None:
    registry = _registry(_candidate("challenger"))
    outcomes: list[OutcomeObservation] = []
    for signal, start, end in (
        ("2026-08-31", "2026-09-01", "2026-09-15"),
        ("2026-09-10", "2026-09-11", "2026-09-25"),
        ("2026-09-20", "2026-09-21", "2026-10-05"),
    ):
        outcomes.extend(
            [
                _outcome("anchor", 0.01, signal_date=signal, start_date=start, end_date=end),
                _outcome(
                    "challenger",
                    0.03,
                    signal_date=signal,
                    start_date=start,
                    end_date=end,
                ),
            ]
        )
    passed = assess_pairwise_promotion(
        registry=registry,
        evaluation_signal_date="2026-10-20",
        anchor_id="anchor",
        challenger_id="challenger",
        outcomes=list(reversed(outcomes)),
        minimum_complete_cohorts=3,
        minimum_mean_excess=0.0,
        minimum_positive_excess_ratio=0.6,
    )
    assert passed.eligible_for_major_review is True
    assert passed.complete_cohort_count == 3
    assert passed.mean_excess_return == pytest.approx(0.02)
    assert passed.positive_excess_ratio == 1.0

    insufficient = assess_pairwise_promotion(
        registry=registry,
        evaluation_signal_date="2026-10-20",
        anchor_id="anchor",
        challenger_id="challenger",
        outcomes=outcomes,
        minimum_complete_cohorts=4,
    )
    assert insufficient.eligible_for_major_review is False
    assert insufficient.reason == "insufficient_complete_cohorts"
