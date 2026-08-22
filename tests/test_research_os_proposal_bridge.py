from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from factor_lab.research_os.catalog import (
    CatalogConflict,
    ResearchCatalog,
    RunRecord,
)
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EvaluationInputBindings,
    EnvironmentRef,
    ExperimentSpec,
    FactorSpec,
    LabelSpec,
    PortfolioPolicy,
    Preregistration,
    SnapshotTier,
    UniverseSpec,
    ValidationProtocol,
)
from factor_lab.research_os.dsl import Availability, FieldSpec
from factor_lab.research_os.proposals import (
    persist_proposal_decision,
    review_llm_proposal,
)


NOW = datetime(2026, 8, 22, 8, 0, tzinfo=timezone.utc)
EVALUATOR = "research_os.long_only.v2"


def _template() -> ExperimentSpec:
    return ExperimentSpec(
        snapshot=DataSnapshotRef(
            snapshot_id="a" * 64,
            tier=SnapshotTier.GOLD,
            uri="s3://factor-lab/gold/a",
            content_hash="a" * 64,
            as_of=NOW,
            quality_status=DataQualityStatus.ACCEPTED,
        ),
        universe=UniverseSpec(),
        label=LabelSpec(),
        factor=FactorSpec(
            factor_id="template_placeholder",
            family="template",
            name="Template placeholder",
            mechanism="Only reserves the factor-shaped template slot.",
            expression={
                "nodes": [{"id": "raw", "op": "field", "field": "book_to_price"}],
                "output": "raw",
            },
            direction="higher_is_better",
            falsification_criteria=("never evaluated",),
        ),
        portfolio=PortfolioPolicy(),
        validation=ValidationProtocol(
            initial_train_start=date(2017, 1, 1),
            initial_train_end=date(2020, 12, 31),
        ),
        evaluator_version=EVALUATOR,
        environment=EnvironmentRef(
            code_hash="b" * 64,
            dependency_lock_hash="c" * 64,
            configuration_hash="d" * 64,
            dirty_patch_hash="e" * 64,
            python_version="3.12",
            platform="Windows-AMD64",
            evaluator_build=EVALUATOR,
        ),
        evaluation_inputs=EvaluationInputBindings(
            bootstrap_resamples=2_000,
            bootstrap_seed=0,
        ),
        preregistration=Preregistration(
            hypothesis_id="template_placeholder_hypothesis",
            economic_mechanism="Template placeholder only.",
            direction="positive",
            falsification_criteria=("never evaluated",),
            stop_rules=("never run",),
        ),
    )


def _payload() -> dict:
    return {
        "preregistration": {
            "hypothesis_id": "hyp_value_quality",
            "economic_mechanism": "cheap profitable firms may be underpriced",
            "direction": "positive",
            "expected_regimes": ["normal_liquidity", "risk_on"],
            "falsification_criteria": ["outer OOS excess is non-positive"],
            "allowed_variants": ["size_neutral", "industry_neutral"],
            "stop_rules": ["stop after two diagnostics"],
        },
        "factor": {
            "factor_id": "value_quality_v1",
            "family": "value_quality",
            "name": "Value quality",
            "mechanism": "cheap profitable firms may be underpriced",
            "expression": {
                "nodes": [
                    {"id": "raw", "op": "field", "field": "book_to_price"},
                    {"id": "ranked", "op": "rank", "input": "raw"},
                ],
                "output": "ranked",
            },
            "direction": "higher_is_better",
            "expected_regimes": ["risk_on", "normal_liquidity"],
            "falsification_criteria": ["outer OOS excess is non-positive"],
            "allowed_variants": ["industry_neutral", "size_neutral"],
        },
    }


def _fields() -> list[FieldSpec]:
    return [
        FieldSpec("book_to_price", availability=Availability.CLOSE),
        FieldSpec("return_on_equity", availability=Availability.POST_CLOSE, minimum_lag_sessions=1),
    ]


@pytest.mark.parametrize(
    "injection",
    (
        {"promote": True},
        {"metrics": {"sharpe": 9.9}},
        {"target_weights": {"000001.SZ": 1.0}},
        {"execute": {"orders": ["buy"]}},
    ),
)
def test_decision_metrics_weights_and_execution_authority_are_rejected(injection) -> None:
    payload = _payload()
    payload.update(injection)
    decision = review_llm_proposal(
        payload,
        experiment_template=_template(),
        field_specs=_fields(),
    )

    assert not decision.accepted
    assert decision.experiment_spec is None
    assert any(
        "decision_authority_forbidden" in item or "proposal_scope_forbidden" in item
        for item in decision.violations
    )


def test_proposal_cannot_override_frozen_template_or_trusted_registry() -> None:
    payload = _payload()
    payload["portfolio"] = {"capital": 1.0}
    payload["factor"]["signal_field_registry"] = [
        {"name": "book_to_price", "availability": "pre_open"}
    ]

    decision = review_llm_proposal(
        payload,
        experiment_template=_template(),
        field_specs=_fields(),
    )

    assert not decision.accepted
    assert "proposal_scope_forbidden:portfolio" in decision.violations
    assert (
        "proposal_scope_forbidden:factor.signal_field_registry" in decision.violations
    )


def test_future_field_is_rejected() -> None:
    payload = _payload()
    payload["factor"]["expression"]["nodes"][0]["field"] = "forward_return_5d"
    decision = review_llm_proposal(
        payload,
        experiment_template=_template(),
        field_specs=[FieldSpec("forward_return_5d")],
    )
    assert not decision.accepted
    assert any("forward" in item for item in decision.violations)


def test_metric_hidden_in_a_dsl_node_is_rejected_instead_of_ignored() -> None:
    payload = _payload()
    payload["factor"]["expression"]["nodes"][0]["net_sharpe"] = 7.0
    decision = review_llm_proposal(
        payload, experiment_template=_template(), field_specs=_fields()
    )
    assert not decision.accepted
    assert any("net_sharpe" in item for item in decision.violations)


@pytest.mark.parametrize(
    "template_update, expected",
    (
        (
            {
                "snapshot": _template().snapshot.model_copy(
                    update={"quality_status": DataQualityStatus.DISPUTED}
                )
            },
            "template_snapshot_not_accepted_gold",
        ),
        (
            {
                "snapshot": _template().snapshot.model_copy(
                    update={"tier": SnapshotTier.SILVER}
                )
            },
            "template_snapshot_not_accepted_gold",
        ),
    ),
)
def test_template_requires_accepted_gold(template_update, expected) -> None:
    template = _template().model_copy(update=template_update)
    decision = review_llm_proposal(
        _payload(), experiment_template=template, field_specs=_fields()
    )
    assert not decision.accepted
    assert expected in decision.violations


def test_template_requires_explicit_environment_and_evaluation_bindings() -> None:
    raw_template = _template().model_dump(mode="python", exclude_none=False)
    raw_template.pop("environment")
    raw_template.pop("evaluation_inputs")

    decision = review_llm_proposal(
        _payload(), experiment_template=raw_template, field_specs=_fields()
    )

    assert not decision.accepted
    assert "template_binding_missing:environment" in decision.violations
    assert "template_binding_missing:evaluation_inputs" in decision.violations


def test_valid_dsl_builds_deterministic_order_independent_experiment() -> None:
    payload = _payload()
    reordered = {
        "factor": dict(reversed(list(payload["factor"].items()))),
        "preregistration": dict(reversed(list(payload["preregistration"].items()))),
    }
    reordered["factor"]["expression"] = {
        "output": "ranked",
        "nodes": list(reversed(payload["factor"]["expression"]["nodes"])),
    }
    reordered["factor"]["allowed_variants"] = list(
        reversed(payload["factor"]["allowed_variants"])
    )
    reordered["preregistration"]["allowed_variants"] = list(
        reversed(payload["preregistration"]["allowed_variants"])
    )

    first = review_llm_proposal(
        payload, experiment_template=_template(), field_specs=_fields()
    )
    second = review_llm_proposal(
        reordered,
        experiment_template=_template(),
        field_specs=list(reversed(_fields())),
    )

    assert first.accepted and second.accepted
    assert first.experiment_fingerprint == second.experiment_fingerprint
    assert first.experiment_spec is not None
    assert first.experiment_spec.snapshot == _template().snapshot
    assert first.experiment_spec.portfolio == _template().portfolio
    assert first.experiment_spec.validation == _template().validation
    assert first.experiment_spec.environment == _template().environment
    assert tuple(
        field.name for field in first.experiment_spec.factor.signal_field_registry
    ) == ("book_to_price",)


def test_persistence_is_idempotent_and_grants_no_research_authority(tmp_path) -> None:
    accepted = review_llm_proposal(
        _payload(), experiment_template=_template(), field_specs=_fields()
    )
    rejected_payload = _payload()
    rejected_payload["sharpe"] = 8.0
    rejected = review_llm_proposal(
        rejected_payload, experiment_template=_template(), field_specs=_fields()
    )

    with ResearchCatalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.initialize_schema()
        first = persist_proposal_decision(catalog, accepted, reviewed_at=NOW)
        second = persist_proposal_decision(catalog, accepted, reviewed_at=NOW)
        persist_proposal_decision(catalog, rejected, reviewed_at=NOW)

        assert first.run_id == second.run_id == accepted.decision_id
        assert len(catalog.list_runs(run_type="llm_proposal_review")) == 2
        assert catalog.list_experiments() == []
        assert catalog.list_trials() == []


def test_same_decision_id_with_different_ledger_content_fails_closed(tmp_path) -> None:
    decision = review_llm_proposal(
        _payload(), experiment_template=_template(), field_specs=_fields()
    )
    collision = RunRecord(
        run_id=decision.decision_id,
        run_type="llm_proposal_review",
        status="completed",
        input_fingerprint=decision.input_fingerprint,
        started_at=NOW,
        completed_at=NOW,
        metadata={"proposal_decision": "tampered"},
    )
    with ResearchCatalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.initialize_schema()
        catalog.claim_run(collision)
        with pytest.raises(CatalogConflict, match="identity collision"):
            persist_proposal_decision(catalog, decision, reviewed_at=NOW)
