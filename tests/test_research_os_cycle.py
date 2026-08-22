from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EvaluationInputBindings,
    EnvironmentRef,
    ExperimentSpec,
    FactorDirection,
    FactorSpec,
    HypothesisDirection,
    Preregistration,
    SignalFieldSpec,
    SnapshotTier,
    ValidationProtocol,
)
from factor_lab.research_os.cycle import HistoricalResearchCycle, evaluation_input_hash
from factor_lab.research_os.evaluator import CANONICAL_EVALUATOR_VERSION
from factor_lab.research_os.dsl import FactorGraph, FieldNode, FieldSpec
from factor_lab.research_os.negative_controls import NegativeControlMetric
from factor_lab.research_os.snapshots import SnapshotIntegrityError


def _spec(*, trust_labels: tuple[str, ...] = ()) -> ExperimentSpec:
    return ExperimentSpec(
        snapshot=DataSnapshotRef(
            snapshot_id="a" * 64,
            tier=SnapshotTier.GOLD,
            uri="s3://factor-lab/gold/a",
            content_hash="a" * 64,
            as_of=datetime(2026, 8, 21, tzinfo=timezone.utc),
            quality_status=DataQualityStatus.ACCEPTED,
            trust_labels=trust_labels,
        ),
        factor=FactorSpec(
            factor_id="value_quality_v1",
            family="value_quality",
            name="Value quality",
            mechanism="cash-generative cheap companies may be underpriced",
            expression="legacy_python_expression",
            signal_field_registry=(SignalFieldSpec(name="book_yield"),),
            direction=FactorDirection.HIGHER_IS_BETTER,
            falsification_criteria=("stitched outer OOS excess is non-positive",),
        ),
        evaluator_version=CANONICAL_EVALUATOR_VERSION,
        environment=EnvironmentRef(
            code_hash="b" * 64,
            dependency_lock_hash="c" * 64,
            configuration_hash="d" * 64,
            dirty_patch_hash="e" * 64,
            python_version="3.10",
            platform="windows",
            evaluator_build=CANONICAL_EVALUATOR_VERSION,
        ),
        preregistration=Preregistration(
            hypothesis_id="hyp_value_quality_v1",
            economic_mechanism="quality conditions the value premium",
            direction=HypothesisDirection.POSITIVE,
            falsification_criteria=("three or more outer years fail",),
            stop_rules=("stop after two diagnostic branches",),
        ),
        validation=ValidationProtocol(
            initial_train_start=date(2017, 1, 1),
            initial_train_end=date(2020, 12, 31),
        ),
    )


def test_unverified_st_freezes_experiment_and_is_idempotent(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        cycle = HistoricalResearchCycle(catalog)
        spec = _spec(trust_labels=("st_history_unverified",))
        catalog.register_snapshot(spec.snapshot)
        first = cycle.run(spec, pd.DataFrame())
        second = cycle.run(spec, pd.DataFrame())

        assert first.status == "blocked"
        assert first.lifecycle_state == "frozen_data"
        assert "st_history_unverified" in first.failures
        assert second.to_dict() == first.to_dict()
        assert len(catalog.list_trials()) == 1


def test_legacy_expression_is_ledgered_but_cannot_run(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(_spec().snapshot)
        result = HistoricalResearchCycle(catalog).run(
            _spec(),
            pd.DataFrame({"date": pd.to_datetime(["2020-01-02"])}),
        )
        assert result.status == "blocked"
        assert any("typed DAG/DSL" in reason for reason in result.failures)
        assert len(catalog.list_trials()) == 1


def test_accepted_ref_cannot_authorize_an_unbound_in_memory_frame(tmp_path) -> None:
    spec = _spec()
    spec = spec.model_copy(
        update={
            "factor": spec.factor.model_copy(
                update={
                    "expression": FactorGraph(
                        nodes=(FieldNode("raw", "book_yield"),), output_id="raw"
                    ).to_dict()
                }
            )
        }
    )
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-02"]),
            "ticker": ["000001.SZ"],
            "book_yield": [0.2],
        }
    )
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(spec.snapshot)
        result = HistoricalResearchCycle(catalog).run(
            spec, frame, field_specs=(FieldSpec("book_yield"),)
        )
    assert result.status == "blocked"
    assert result.lifecycle_state == "frozen_data"
    assert any("snapshot_frame_binding" in reason for reason in result.failures)


def test_cycle_does_not_self_register_an_unpublished_snapshot(tmp_path) -> None:
    spec = _spec(trust_labels=("st_history_unverified",))
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        with pytest.raises(
            SnapshotIntegrityError, match="previously published catalog snapshot"
        ):
            HistoricalResearchCycle(catalog).run(spec, pd.DataFrame())
        assert catalog.get_snapshot(spec.snapshot.snapshot_id) is None


def test_cycle_persists_blocker_for_unbound_runtime_risk_input(tmp_path) -> None:
    spec = _spec()
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(spec.snapshot)
        result = HistoricalResearchCycle(catalog).run(
            spec,
            pd.DataFrame(),
            exposure_frame=pd.DataFrame({"ticker": ["000001.SZ"], "beta": [1.0]}),
        )

        assert result.status == "blocked"
        assert result.lifecycle_state == "frozen_data"
        assert "unbound_evaluation_input:exposure_frame" in result.failures
        assert catalog.list_trials()[0].metadata["reservation_state"] == "completed"


@pytest.mark.parametrize(
    ("runtime", "expected_failure"),
    (
        (
            {"negative_controls": (NegativeControlMetric("shuffle", 0.0, False),)},
            "unbound_evaluation_input:negative_controls",
        ),
        (
            {"within_family_p_values": (0.01, 0.02)},
            "unbound_evaluation_input:within_family_p_values",
        ),
        (
            {"data_audit_blockers": ("historical_st_missing",)},
            "unbound_evaluation_input:data_audit_blockers",
        ),
        (
            {"bootstrap_resamples": 500},
            "evaluation_input_mismatch:bootstrap_resamples",
        ),
        (
            {"seed": 7},
            "evaluation_input_mismatch:bootstrap_seed",
        ),
    ),
)
def test_every_result_affecting_runtime_input_must_match_fingerprint_binding(
    tmp_path, runtime, expected_failure
) -> None:
    spec = _spec()
    with ResearchCatalog(tmp_path / f"{expected_failure.split(':')[-1]}.db") as catalog:
        catalog.initialize_schema()
        catalog.register_snapshot(spec.snapshot)
        result = HistoricalResearchCycle(catalog).run(spec, pd.DataFrame(), **runtime)

    assert result.status == "blocked"
    assert result.lifecycle_state == "frozen_data"
    assert expected_failure in result.failures


def test_bound_statistical_evidence_changes_experiment_fingerprint() -> None:
    controls = (NegativeControlMetric("shuffle", 0.0, False),)
    bound = _spec().model_copy(
        update={
            "evaluation_inputs": EvaluationInputBindings(
                negative_controls_hash=evaluation_input_hash(
                    "negative_controls", controls
                )
            )
        }
    )
    assert bound.fingerprint() != _spec().fingerprint()
