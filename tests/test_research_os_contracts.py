from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from factor_lab.research_os.contracts import (
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    FactorSpec,
    PortfolioPolicy,
    PromotionCriteria,
    Preregistration,
    RecoveryCase,
    StatisticalBudget,
    UniverseSpec,
    ValidationProtocol,
)


def test_portfolio_contract_cannot_loosen_fixed_exposure_limits():
    with pytest.raises(ValidationError):
        PortfolioPolicy(industry_active_weight_limit=0.06)
    with pytest.raises(ValidationError):
        PortfolioPolicy(size_active_weight_limit=0.06)
    with pytest.raises(ValidationError):
        PortfolioPolicy(minimum_beta=0.89)
    with pytest.raises(ValidationError):
        PortfolioPolicy(maximum_beta=1.11)


def test_validation_thresholds_and_trial_budget_cannot_be_weakened():
    with pytest.raises(ValidationError):
        PromotionCriteria(minimum_net_sharpe=0.79)
    with pytest.raises(ValidationError):
        PromotionCriteria(maximum_capacity_violations=1)
    with pytest.raises(ValidationError):
        StatisticalBudget(maximum_confirmatory_challengers_per_month=4)
    with pytest.raises(ValidationError):
        StatisticalBudget(maximum_diagnostic_branches=3)
    with pytest.raises(ValidationError):
        ValidationProtocol(purge_sessions=5)
    with pytest.raises(ValidationError):
        ValidationProtocol(outer_test_years=(2021, 2022, 2023, 2024))


NOW = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)


def snapshot() -> DataSnapshotRef:
    return DataSnapshotRef(
        snapshot_id="gold-a-share-20260822",
        tier="gold",
        uri="s3://factor-lab/gold/a-share/20260822",
        content_hash="a" * 64,
        as_of=NOW,
    )


def environment() -> EnvironmentRef:
    return EnvironmentRef(
        code_hash="b" * 64,
        dependency_lock_hash="c" * 64,
        configuration_hash="d" * 64,
        python_version="3.10.16",
        platform="Windows-AMD64",
        evaluator_build="portfolio-v1",
    )


def factor() -> FactorSpec:
    return FactorSpec(
        factor_id="quality_value_v1",
        family="value_quality",
        name="Quality value",
        mechanism="Cheap profitable businesses mean revert after pessimism.",
        expression={"op": "rank", "input": "book_yield"},
        direction="higher_is_better",
        falsification_criteria=("outer OOS excess return is non-positive",),
    )


def preregistration() -> Preregistration:
    return Preregistration(
        hypothesis_id="hyp-quality-value-001",
        economic_mechanism="Cheap profitable businesses mean revert after pessimism.",
        direction="positive",
        falsification_criteria=("outer OOS excess return is non-positive",),
        stop_rules=("stop after two diagnostic branches",),
    )


def test_contract_defaults_encode_the_frozen_research_policy() -> None:
    universe = UniverseSpec()
    portfolio = PortfolioPolicy()

    assert universe.target_size == 500
    assert universe.membership_lag_months == 1
    assert portfolio.mode == "long_only"
    assert portfolio.capital == 50_000_000
    assert portfolio.target_position_count == 50
    assert portfolio.maximum_stock_weight == 0.02
    assert portfolio.maximum_adv_participation == 0.05


def test_contracts_reject_unknown_fields_and_naive_snapshot_times() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        UniverseSpec(typo_target_size=500)

    with pytest.raises(ValidationError, match="timezone"):
        DataSnapshotRef(
            snapshot_id="bad",
            tier="gold",
            uri="file:///bad",
            content_hash="a" * 64,
            as_of=datetime(2026, 8, 22),
        )


def test_experiment_requires_exactly_one_factor_or_sleeve() -> None:
    common = {
        "snapshot": snapshot(),
        "environment": environment(),
        "evaluator_version": "1",
        "preregistration": preregistration(),
    }
    with pytest.raises(ValidationError, match="exactly one"):
        ExperimentSpec(**common)

    spec = ExperimentSpec(**common, factor=factor())
    assert spec.candidate_id == "quality_value_v1"
    assert spec.candidate_kind.value == "factor"
    assert len(spec.fingerprint()) == 64


def test_portfolio_bounds_cannot_silently_leave_structural_cash() -> None:
    with pytest.raises(ValidationError, match="deploy full capital"):
        PortfolioPolicy(target_position_count=40, minimum_position_count=40)

    with pytest.raises(ValidationError, match="minimum <= target <= maximum"):
        PortfolioPolicy(
            minimum_position_count=60,
            target_position_count=50,
            maximum_position_count=100,
        )


def test_recovery_case_enforces_sla_order_and_data_freeze_state() -> None:
    case = RecoveryCase(
        recovery_case_id="recovery-low-risk-001",
        sleeve_id="low_risk",
        lifecycle_state="dormant",
        triggered_at=NOW,
        drift_event_due_at=NOW + timedelta(days=5),
        diagnosis_due_at=NOW + timedelta(days=20),
        earliest_recovery_review_at=NOW + timedelta(days=84),
    )
    assert case.status.value == "open"

    with pytest.raises(ValidationError, match="frozen_data"):
        RecoveryCase.model_validate(
            {**case.model_dump(), "data_integrity_failure": True}
        )
