from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pandas as pd
import pytest

from factor_lab.research.pit_stock import PITStockContractError
from factor_lab.research.pit_stock_minute_stages import (
    CANDIDATE_FRAME_NAMES,
    CANDIDATE_ROLES,
    IGNORED_FRAME_STAGE_METADATA_COLUMNS,
    IGNORED_PHASE_GATE_CHECK_KEYS,
    IGNORED_PHASE_GATE_KEYS,
    MinuteDevelopmentStageDecision,
    candidate_result_identity,
    orchestrate_minute_development_stages,
)


def _phase_gate(*, stage: str, stage_1_passed: bool) -> dict:
    candidate_map = {
        "candidate_base": {"value": 1.0},
        "candidate_stress": {"value": 0.9},
    }
    return {
        "complete": stage == "stage_2",
        "positive_segments": deepcopy(candidate_map),
        "base_edge_vs_adv500": (
            {"full": True, "train": True, "validation": True}
            if stage == "stage_2"
            else {}
        ),
        "operational": {
            **deepcopy(candidate_map),
            **(
                {
                    "adv500_base": {"value": 0.8},
                    "adv500_stress": {"value": 0.7},
                }
                if stage == "stage_2"
                else {}
            ),
        },
        "size": deepcopy(candidate_map),
        "industry": deepcopy(candidate_map),
        "market_state": {
            "targets_empty": True,
            "executed_buy_count": 0,
        },
        "future_input_violation_count": 0,
        "stage_1_checks": {
            "candidate_positive_every_segment": True,
            "candidate_operational": True,
        },
        "stage_1_passed": stage_1_passed,
        "checks": {
            "candidate_positive_every_segment": True,
            "candidate_base_above_adv500_every_segment": stage == "stage_2",
            "operational": stage == "stage_2",
            "size_groups_positive": True,
            "industry_positive_fraction": True,
            "industry_leave_one_out_fraction": True,
            "positive_calendar_year_fraction": True,
            "non_both_positive_behavior": True,
        },
        "passed": stage == "stage_2",
        "stage": stage,
        "stage_metadata": {"captured_after": stage},
    }


def _frame(*, stage: str, include_benchmark: bool) -> pd.DataFrame:
    rows = [
        {
            "role": "candidate_base",
            "identity": 1,
            "event_time": pd.Timestamp("2021-01-04 09:35:00"),
            "value": 10.0,
            "optional": pd.NA,
            "stage": stage,
        },
        {
            "role": "candidate_stress",
            "identity": 2,
            "event_time": pd.Timestamp("2021-01-04 09:41:00"),
            "value": 9.5,
            "optional": None,
            "stage": stage,
        },
    ]
    if include_benchmark:
        rows.extend(
            [
                {
                    "role": "adv500_base",
                    "identity": 3,
                    "event_time": pd.Timestamp("2021-01-04 09:35:00"),
                    "value": -50.0,
                    "optional": "benchmark",
                    "stage": stage,
                },
                {
                    "role": "adv500_stress",
                    "identity": 4,
                    "event_time": pd.Timestamp("2021-01-04 09:35:00"),
                    "value": -60.0,
                    "optional": "benchmark",
                    "stage": stage,
                },
            ]
        )
    return pd.DataFrame(rows)


def _result(*, stage: str, passed: bool, include_benchmark: bool) -> SimpleNamespace:
    metrics = {
        "candidate_base": {
            "daily_cagr": 0.1,
            "requested_notional_fill_ratio": 0.99,
        },
        "candidate_stress": {
            "daily_cagr": 0.09,
            "requested_notional_fill_ratio": 0.985,
        },
        "phase_gate": _phase_gate(stage=stage, stage_1_passed=passed),
        "stage": stage,
        "stage_metadata": {"runner": stage},
    }
    if include_benchmark:
        metrics.update(
            {
                "adv500_base": {"daily_cagr": -0.01},
                "adv500_stress": {"daily_cagr": -0.02},
            }
        )
    metrics["minute_provider_protocol"] = {
        "enabled": True,
        "future_input_violation_count": 0,
        "call_count": 1 if stage == "stage_1" else 2,
    }
    frames = {
        name: _frame(stage=stage, include_benchmark=include_benchmark)
        for name in CANDIDATE_FRAME_NAMES
    }
    return SimpleNamespace(metrics=metrics, **frames)


class _FlagOnlyFailure:
    """Any access beyond the sole stage flag is a test failure."""

    metrics = {"phase_gate": {"stage_1_passed": False}}

    def __getattr__(self, name):
        raise AssertionError(f"stopped stage inspected forbidden field {name}")


def test_false_stage_1_reads_only_flag_and_never_calls_stage_2() -> None:
    calls = {"stage_1": 0, "stage_2": 0}
    result = _FlagOnlyFailure()

    def stage_1_runner():
        calls["stage_1"] += 1
        return result

    def stage_2_runner():
        calls["stage_2"] += 1
        raise AssertionError("stage 2 must not run")

    decision = orchestrate_minute_development_stages(
        stage_1_runner, stage_2_runner
    )
    assert isinstance(decision, MinuteDevelopmentStageDecision)
    assert decision.status == "stopped_after_stage_1"
    assert decision.reason == "stage_1_gate_failed"
    assert decision.stage_2_call_count == 0
    assert decision.stage_1_result is result
    assert decision.stage_2_result is None
    assert decision.candidate_identity_sha256 is None
    assert calls == {"stage_1": 1, "stage_2": 0}


def test_true_stage_1_calls_stage_2_once_and_accepts_exact_candidate_projection() -> None:
    stage_1 = _result(stage="stage_1", passed=True, include_benchmark=False)
    stage_2 = _result(stage="stage_2", passed=True, include_benchmark=True)
    calls = 0

    def stage_2_runner():
        nonlocal calls
        calls += 1
        return stage_2

    decision = orchestrate_minute_development_stages(
        lambda: stage_1, stage_2_runner
    )
    assert decision.status == "stage_2_completed"
    assert decision.reason == "stage_2_candidate_replay_exact"
    assert decision.stage_2_call_count == calls == 1
    assert decision.stage_1_result is stage_1
    assert decision.stage_2_result is stage_2
    assert decision.candidate_identity_sha256 == candidate_result_identity(stage_1)
    assert set(decision.candidate_identity_sha256) == {
        *CANDIDATE_FRAME_NAMES,
        "metrics",
        "overall",
    }


@pytest.mark.parametrize("component", [*CANDIDATE_FRAME_NAMES, "metrics"])
def test_stage_2_candidate_mismatch_fails_closed_after_one_call(component) -> None:
    stage_1 = _result(stage="stage_1", passed=True, include_benchmark=False)
    stage_2 = _result(stage="stage_2", passed=True, include_benchmark=True)
    if component == "metrics":
        stage_2.metrics["candidate_stress"]["daily_cagr"] += 0.0001
    else:
        frame = getattr(stage_2, component)
        frame.loc[frame["role"].eq("candidate_base"), "value"] += 0.01
    calls = 0

    def stage_2_runner():
        nonlocal calls
        calls += 1
        return stage_2

    with pytest.raises(PITStockContractError, match=component):
        orchestrate_minute_development_stages(lambda: stage_1, stage_2_runner)
    assert calls == 1


def test_stage_1_identity_is_frozen_before_stage_2_callback() -> None:
    stage_1 = _result(stage="stage_1", passed=True, include_benchmark=False)

    def mutating_stage_2_runner():
        stage_1.orders.loc[0, "value"] = 999.0
        return stage_1

    with pytest.raises(PITStockContractError, match="orders"):
        orchestrate_minute_development_stages(
            lambda: stage_1, mutating_stage_2_runner
        )


@pytest.mark.parametrize("value", [1, 0, "true", None])
def test_stage_1_flag_must_be_a_literal_boolean_and_stage_2_stays_zero(value) -> None:
    stage_1 = SimpleNamespace(
        metrics={"phase_gate": {"stage_1_passed": value}}
    )
    calls = 0

    def stage_2_runner():
        nonlocal calls
        calls += 1

    with pytest.raises(PITStockContractError, match="must be a boolean"):
        orchestrate_minute_development_stages(
            lambda: stage_1, stage_2_runner
        )
    assert calls == 0


def test_projection_exclusions_are_explicit_and_narrow() -> None:
    assert IGNORED_FRAME_STAGE_METADATA_COLUMNS == (
        "stage",
        "stage_id",
        "stage_name",
        "stage_metadata",
    )
    assert IGNORED_PHASE_GATE_KEYS == (
        "base_edge_vs_adv500",
        "complete",
        "passed",
        "stage",
        "stage_id",
        "stage_name",
        "stage_metadata",
    )
    assert IGNORED_PHASE_GATE_CHECK_KEYS == (
        "candidate_base_above_adv500_every_segment",
        "operational",
    )
    assert CANDIDATE_ROLES == ("candidate_base", "candidate_stress")


def test_unclassified_metric_or_phase_field_requires_projection_refreeze() -> None:
    result = _result(stage="stage_1", passed=True, include_benchmark=False)
    result.metrics["unexpected"] = 1
    with pytest.raises(PITStockContractError, match="unclassified keys"):
        candidate_result_identity(result)

    result = _result(stage="stage_1", passed=True, include_benchmark=False)
    result.metrics["phase_gate"]["unexpected"] = 1
    with pytest.raises(PITStockContractError, match="unclassified keys"):
        candidate_result_identity(result)
