from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research_os.dsl import (
    Availability,
    CrossSectionNode,
    CrossSectionOperation,
    DecisionPoint,
    DslValidationError,
    EvaluationContext,
    FactorGraph,
    FieldNode,
    FieldRole,
    FieldSpec,
    LagNode,
    NeutralizeNode,
    RollingNode,
    RollingOperation,
    ValueType,
    compile_factor_graph,
    evaluate_factor_graph,
    factor_graph_from_spec,
)
from factor_lab.research_os.contracts import FactorDirection, FactorSpec


def test_typed_graph_round_trip_and_pure_temporal_cross_section_evaluation() -> None:
    graph = FactorGraph(
        nodes=(
            FieldNode("raw", "value"),
            RollingNode("mean2", "raw", RollingOperation.MEAN, window=2),
            CrossSectionNode("rank", "mean2", CrossSectionOperation.RANK),
        ),
        output_id="rank",
    )
    restored = FactorGraph.from_dict(graph.to_dict())
    assert restored == graph
    frame = pd.DataFrame(
        {
            "ticker": ["B", "A", "B", "A", "B", "A"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02", "2024-01-03", "2024-01-03"]),
            "value": [10.0, 1.0, 20.0, 2.0, 30.0, 3.0],
        },
        index=[9, 3, 8, 2, 7, 1],
    )
    before = frame.copy(deep=True)
    result = evaluate_factor_graph(restored, frame, [FieldSpec("value")])
    pd.testing.assert_frame_equal(frame, before)
    assert result.index.tolist() == frame.index.tolist()
    assert result.loc[[9, 3]].isna().all()
    assert result.loc[8] == pytest.approx(1.0)
    assert result.loc[2] == pytest.approx(0.5)
    assert result.loc[7] == pytest.approx(1.0)
    assert result.loc[1] == pytest.approx(0.5)


@pytest.mark.parametrize(
    ("field", "spec", "expected_code"),
    [
        ("forward_return_5d", FieldSpec("forward_return_5d"), "forward_field"),
        ("outcome", FieldSpec("outcome", role=FieldRole.LABEL), "forbidden_field_role"),
        ("announcement", FieldSpec("announcement", availability=Availability.POST_CLOSE), "insufficient_lag"),
    ],
)
def test_leakage_fields_fail_closed(field: str, spec: FieldSpec, expected_code: str) -> None:
    graph = FactorGraph(nodes=(FieldNode("raw", field),), output_id="raw")
    with pytest.raises(DslValidationError) as error:
        compile_factor_graph(graph, [spec], decision_point=DecisionPoint.AFTER_CLOSE)
    assert expected_code in {item.code for item in error.value.violations}


def test_negative_lag_is_rejected() -> None:
    graph = FactorGraph(
        nodes=(FieldNode("raw", "close"), LagNode("lead", "raw", -1)),
        output_id="lead",
    )
    with pytest.raises(DslValidationError) as error:
        compile_factor_graph(graph, [FieldSpec("close")])
    assert "negative_lag" in {item.code for item in error.value.violations}


def test_lagged_post_close_field_checks_precise_availability() -> None:
    graph = FactorGraph(
        nodes=(FieldNode("raw", "announcement"), LagNode("known_next_day", "raw", 1)),
        output_id="known_next_day",
    )
    spec = FieldSpec(
        "announcement",
        availability=Availability.POST_CLOSE,
        minimum_lag_sessions=1,
        available_at_column="announcement_available_at",
    )
    frame = pd.DataFrame(
        {
            "ticker": ["A", "A", "A"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "decision_time": pd.to_datetime(["2024-01-02 15:01", "2024-01-03 15:01", "2024-01-04 15:01"]),
            "announcement_available_at": pd.to_datetime(["2024-01-02 18:00", "2024-01-03 18:00", "2024-01-04 18:00"]),
            "announcement": [10.0, 20.0, 30.0],
        }
    )
    result = evaluate_factor_graph(graph, frame, [spec])
    assert result.iloc[0] != result.iloc[0]
    assert result.iloc[1:].tolist() == [10.0, 20.0]


def test_precise_availability_violation_is_rejected_at_runtime() -> None:
    graph = FactorGraph(nodes=(FieldNode("raw", "estimate"),), output_id="raw")
    spec = FieldSpec("estimate", available_at_column="available_at")
    frame = pd.DataFrame(
        {
            "ticker": ["A"],
            "date": pd.to_datetime(["2024-01-02"]),
            "decision_time": pd.to_datetime(["2024-01-02 15:00"]),
            "available_at": pd.to_datetime(["2024-01-02 16:00"]),
            "estimate": [1.0],
        }
    )
    with pytest.raises(DslValidationError) as error:
        evaluate_factor_graph(graph, frame, [spec])
    assert error.value.violations[0].code == "not_available_at_decision"


def test_neutralization_removes_numeric_and_categorical_exposure() -> None:
    frame = pd.DataFrame(
        {
            "ticker": [f"S{i}" for i in range(8)],
            "date": pd.to_datetime(["2024-01-02"] * 8),
            "raw": [1.1, 2.0, 3.2, 4.1, 2.2, 3.0, 4.3, 5.1],
            "size": [1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0, 4.0],
            "industry": ["a"] * 4 + ["b"] * 4,
        }
    )
    graph = FactorGraph(
        nodes=(
            FieldNode("raw", "raw"),
            FieldNode("size", "size"),
            FieldNode("industry", "industry"),
            NeutralizeNode("residual", "raw", ("size", "industry"), ("industry",)),
        ),
        output_id="residual",
    )
    specs = [
        FieldSpec("raw"),
        FieldSpec("size"),
        FieldSpec("industry", value_type=ValueType.CATEGORICAL),
    ]
    residual = evaluate_factor_graph(graph, frame, specs)
    assert residual.notna().all()
    assert abs(float(residual.mean())) < 1e-10
    assert abs(float(residual.corr(frame["size"]))) < 1e-10


def test_pre_open_decision_requires_lagged_close() -> None:
    graph = FactorGraph(nodes=(FieldNode("close", "close"),), output_id="close")
    with pytest.raises(DslValidationError):
        compile_factor_graph(graph, [FieldSpec("close", availability=Availability.CLOSE)], decision_point=DecisionPoint.PRE_OPEN)
    lagged = FactorGraph(nodes=(FieldNode("close", "close"), LagNode("prior", "close", 1)), output_id="prior")
    compiled = compile_factor_graph(lagged, [FieldSpec("close", availability=Availability.CLOSE)], decision_point=DecisionPoint.PRE_OPEN)
    assert compiled.field_lags == {"close": 1}


def test_factor_spec_requires_typed_dsl_instead_of_arbitrary_python() -> None:
    spec = FactorSpec(
        factor_id="value",
        family="value",
        name="Value",
        mechanism="cheap companies may mean revert",
        expression="eval(forward_return_5d)",
        direction=FactorDirection.HIGHER_IS_BETTER,
        falsification_criteria=("outer OOS excess is non-positive",),
    )
    with pytest.raises(DslValidationError) as error:
        factor_graph_from_spec(spec)
    assert error.value.violations[0].code == "untyped_expression"

    typed = spec.model_copy(update={"expression": FactorGraph(nodes=(FieldNode("raw", "book_yield"),), output_id="raw").to_dict()})
    assert factor_graph_from_spec(typed).output_id == "raw"
