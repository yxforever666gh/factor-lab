from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.research.adaptive import (
    AdaptiveExpertSpec,
    MarketOverlaySpec,
    align_expert_cohorts,
    build_market_overlay,
    combine_expert_targets,
    expert_trace_from_evaluation,
    online_wealth_allocations,
)


def _protocol() -> dict:
    path = Path(__file__).resolve().parents[2] / "protocols" / "5.0.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _result(name: str, returns: list[float]) -> dict:
    rows = []
    for index, value in enumerate(returns):
        signal = pd.Timestamp("2025-01-02") + pd.Timedelta(days=index * 10)
        rows.append(
            {
                "signal_date": signal.date().isoformat(),
                "start_date": (signal + pd.Timedelta(days=1)).date().isoformat(),
                "end_date": (signal + pd.Timedelta(days=9)).date().isoformat(),
                "net_return": value,
            }
        )
    return {"factor_name": name, "period_active_returns": rows}


def test_online_allocator_uses_only_periods_ending_strictly_before_signal() -> None:
    spec = AdaptiveExpertSpec.from_protocol(_protocol())
    traces = [
        expert_trace_from_evaluation(
            name,
            _result(name, [0.01 if index == 0 else 0.0]),
            rebalance_offset_days=0,
        )
        for index, name in enumerate(spec.ordered_experts)
    ]
    cohorts = align_expert_cohorts(traces, required_experts=spec.ordered_experts)
    first_end = cohorts[0].end_date

    decisions = online_wealth_allocations(
        [first_end, (pd.Timestamp(first_end) + pd.Timedelta(days=1)).date().isoformat()],
        cohorts,
        spec,
    )

    at_end = decisions[first_end].to_dict()
    after_end = decisions[
        (pd.Timestamp(first_end) + pd.Timedelta(days=1)).date().isoformat()
    ].to_dict()
    assert at_end["matured_cohort_count"] == 0
    assert at_end["excluded_unmatured_cohort_count"] == 1
    assert after_end["matured_cohort_count"] == 1
    assert after_end["excluded_unmatured_cohort_count"] == 0
    assert at_end["total_expert_weights"][spec.anchor_expert] == pytest.approx(0.625)
    assert after_end["total_expert_weights"][spec.ordered_experts[0]] > 0.125
    assert after_end["future_feedback_violation_count"] == 0


def test_online_decision_is_invariant_to_appended_future_cohorts() -> None:
    spec = AdaptiveExpertSpec.from_protocol(_protocol())
    short_traces = [
        expert_trace_from_evaluation(name, _result(name, [0.01]), 0)
        for name in spec.ordered_experts
    ]
    long_traces = [
        expert_trace_from_evaluation(name, _result(name, [0.01, 0.99]), 0)
        for name in spec.ordered_experts
    ]
    signal_date = "2025-01-11"

    short = online_wealth_allocations(
        [signal_date],
        align_expert_cohorts(short_traces, spec.ordered_experts),
        spec,
    )[signal_date].to_dict()
    long = online_wealth_allocations(
        [signal_date],
        align_expert_cohorts(long_traces, spec.ordered_experts),
        spec,
    )[signal_date].to_dict()

    assert long == short


def test_alignment_fails_closed_on_different_period_boundaries() -> None:
    spec = AdaptiveExpertSpec.from_protocol(_protocol())
    traces = [
        expert_trace_from_evaluation(name, _result(name, [0.01]), 0)
        for name in spec.ordered_experts
    ]
    bad_result = _result(spec.ordered_experts[-1], [0.01])
    bad_result["period_active_returns"][0]["end_date"] = "2025-01-12"
    traces[-1] = expert_trace_from_evaluation(
        spec.ordered_experts[-1], bad_result, 0
    )
    with pytest.raises(ValueError, match="identical period boundaries"):
        align_expert_cohorts(traces, required_experts=spec.ordered_experts)


def test_market_overlay_has_400_day_warmup_and_uses_current_signal_information() -> None:
    spec = MarketOverlaySpec.from_protocol(_protocol())
    dates = pd.bdate_range("2020-01-02", periods=401)
    rows = []
    for date_value in dates:
        for ticker in ("A", "B"):
            rows.append(
                {
                    "date": date_value,
                    "ticker": ticker,
                    "return_1d": 0.001,
                    "momentum_120": 1.0,
                }
            )
    features = pd.DataFrame(rows)
    early = dates[398].date().isoformat()
    ready = dates[399].date().isoformat()

    decisions = build_market_overlay(features, [early, ready], spec)

    assert decisions[early].to_dict()["status"] == "insufficient_history"
    assert decisions[early].to_dict()["exposure"] == 0.0
    observed = decisions[ready].to_dict()
    assert observed["ready"] is True
    assert observed["latest_input_date"] == ready
    assert observed["trend_positive"] is True
    assert observed["breadth_positive"] is True
    assert observed["exposure"] == pytest.approx(1.0)
    assert observed["future_overlay_violation_count"] == 0


def test_combination_adds_overlaps_without_truncation_or_renormalization() -> None:
    spec = AdaptiveExpertSpec.from_protocol(_protocol())
    targets = {
        name: {"OVERLAP": 0.1, f"ONLY-{index}": 0.1}
        for index, name in enumerate(spec.ordered_experts)
    }
    weights = {name: 0.25 for name in spec.ordered_experts}

    decision = combine_expert_targets(
        signal_date="2025-01-02",
        targets_by_expert=targets,
        expert_weights=weights,
        exposure=0.6,
        spec=spec,
    ).to_dict()

    assert decision["combined_target_weights"]["OVERLAP"] == pytest.approx(0.06)
    assert decision["combined_target_weights"]["ONLY-0"] == pytest.approx(0.015)
    assert decision["invested_weight"] == pytest.approx(0.12)
    assert decision["cash_weight"] == pytest.approx(0.88)
    assert decision["overlap_position_count"] == 1
