from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research.walk_forward import (
    WalkForwardSelectorSpec,
    build_dynamic_signal,
    causal_candidate_decisions,
    walk_forward_phase_rankings,
)


def _row(signal_date: pd.Timestamp, end_date: pd.Timestamp, value: float) -> dict[str, object]:
    return {
        "signal_date": signal_date.date().isoformat(),
        "start_date": (signal_date + pd.offsets.BDay(1)).date().isoformat(),
        "end_date": end_date.date().isoformat(),
        "net_return": value,
    }


def test_selector_excludes_period_ending_on_the_decision_date() -> None:
    dates = pd.bdate_range("2025-01-02", periods=12)
    decision_date = dates[10]
    rows = [
        _row(dates[0], dates[3], 0.01),
        _row(dates[4], decision_date, 9.0),
    ]
    spec = WalkForwardSelectorSpec(
        lookback_trading_days=10,
        minimum_completed_periods=2,
        update_every_trading_days=1,
    )

    result = causal_candidate_decisions(
        trading_dates=dates,
        signal_dates=[decision_date],
        candidate_periods={"control": rows, "candidate": rows},
        control_factor="control",
        selector=spec,
        periods_per_year=25.2,
    )

    update = result["updates"][0]
    assert update["reference_observations"] == 1
    assert update["fallback_reason"] == "insufficient_matured_history"
    assert update["selected_factor"] == "control"
    assert result["future_selection_violation_count"] == 0


def test_future_period_changes_cannot_rewrite_an_earlier_decision() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    decision_date = dates[12]
    control = [
        _row(dates[0], dates[2], 0.01),
        _row(dates[3], dates[5], -0.01),
        _row(dates[6], dates[8], 0.01),
    ]
    candidate = [
        _row(dates[0], dates[2], 0.03),
        _row(dates[3], dates[5], 0.01),
        _row(dates[6], dates[8], 0.04),
        _row(dates[10], dates[15], -99.0),
    ]
    spec = WalkForwardSelectorSpec(
        lookback_trading_days=12,
        minimum_completed_periods=3,
        update_every_trading_days=1,
        control_score_guard=0.0,
    )

    def select(rows: list[dict[str, object]]) -> dict[str, object]:
        return causal_candidate_decisions(
            trading_dates=dates,
            signal_dates=[decision_date],
            candidate_periods={"control": control, "candidate": rows},
            control_factor="control",
            selector=spec,
            periods_per_year=25.2,
        )["updates"][0]

    first = select(candidate)
    candidate[-1]["net_return"] = 99.0
    second = select(candidate)

    assert first == second
    assert first["selected_factor"] == "candidate"
    assert pd.Timestamp(first["latest_used_end_date"]) < decision_date


def test_duplicate_signal_date_prefix_poisoning_fails_closed() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    decision_date = dates[12]
    control = [
        _row(dates[0], dates[2], 0.01),
        _row(dates[3], dates[5], -0.01),
        _row(dates[6], dates[8], 0.02),
    ]
    candidate = [
        _row(dates[0], dates[2], 0.03),
        _row(dates[3], dates[5], 0.01),
        _row(dates[6], dates[8], 0.04),
        _row(dates[0], dates[15], -99.0),
    ]

    with pytest.raises(ValueError, match="duplicate signal_date"):
        causal_candidate_decisions(
            trading_dates=dates,
            signal_dates=[decision_date],
            candidate_periods={"control": control, "candidate": candidate},
            control_factor="control",
            selector=WalkForwardSelectorSpec(
                lookback_trading_days=12,
                minimum_completed_periods=3,
                update_every_trading_days=1,
                control_score_guard=0.0,
            ),
            periods_per_year=25.2,
        )


def test_future_signal_with_past_end_fails_closed() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    malformed = _row(dates[15], dates[8], 99.0)

    with pytest.raises(ValueError, match="signal_date < start_date <= end_date"):
        causal_candidate_decisions(
            trading_dates=dates,
            signal_dates=[dates[12]],
            candidate_periods={"control": [malformed]},
            control_factor="control",
            selector=WalkForwardSelectorSpec(
                lookback_trading_days=12,
                minimum_completed_periods=2,
                update_every_trading_days=1,
            ),
            periods_per_year=25.2,
        )


def test_well_formed_period_history_accepts_strict_chronology() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    rows = [
        _row(dates[0], dates[2], 0.01),
        _row(dates[3], dates[5], -0.01),
        _row(dates[6], dates[8], 0.02),
    ]

    result = causal_candidate_decisions(
        trading_dates=dates,
        signal_dates=[dates[12]],
        candidate_periods={"control": rows},
        control_factor="control",
        selector=WalkForwardSelectorSpec(
            lookback_trading_days=12,
            minimum_completed_periods=3,
            update_every_trading_days=1,
        ),
        periods_per_year=25.2,
    )

    assert result["updates"][0]["reference_observations"] == 3
    assert result["updates"][0]["latest_used_end_date"] == str(dates[8].date())
    assert result["future_selection_violation_count"] == 0


def test_control_guard_and_registry_order_are_deterministic() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    decision_date = dates[12]
    base_dates = [(dates[0], dates[2]), (dates[3], dates[5]), (dates[6], dates[8])]
    control = [_row(start, end, value) for (start, end), value in zip(base_dates, [0.01, -0.01, 0.02], strict=True)]
    near = [_row(start, end, value) for (start, end), value in zip(base_dates, [0.011, -0.009, 0.021], strict=True)]
    spec = WalkForwardSelectorSpec(
        lookback_trading_days=12,
        minimum_completed_periods=3,
        update_every_trading_days=1,
        control_score_guard=10.0,
    )

    result = causal_candidate_decisions(
        trading_dates=dates,
        signal_dates=[decision_date],
        candidate_periods={"control": control, "near": near},
        control_factor="control",
        selector=spec,
        periods_per_year=25.2,
    )

    update = result["updates"][0]
    assert update["selected_factor"] == "control"
    assert update["fallback_reason"] == "leader_below_control_guard"


def test_selector_can_choose_multiple_qualified_candidates() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    decision_date = dates[12]
    base_dates = [(dates[0], dates[2]), (dates[3], dates[5]), (dates[6], dates[8])]

    def rows(values: list[float]) -> list[dict[str, object]]:
        return [
            _row(start, end, value)
            for (start, end), value in zip(base_dates, values, strict=True)
        ]

    result = causal_candidate_decisions(
        trading_dates=dates,
        signal_dates=[decision_date],
        candidate_periods={
            "control": rows([0.01, -0.01, 0.02]),
            "candidate_a": rows([0.03, 0.01, 0.04]),
            "candidate_b": rows([0.025, 0.008, 0.03]),
        },
        control_factor="control",
        selector=WalkForwardSelectorSpec(
            lookback_trading_days=12,
            minimum_completed_periods=3,
            update_every_trading_days=1,
            control_score_guard=0.0,
            selection_count=2,
        ),
        periods_per_year=25.2,
    )

    update = result["updates"][0]
    assert len(update["selected_factors"]) == 2
    assert set(update["selected_factors"]) == {"candidate_a", "candidate_b"}
    assert sum(update["selected_weights"].values()) == pytest.approx(1.0)


def test_candidate_with_incomplete_common_history_cannot_be_selected() -> None:
    dates = pd.bdate_range("2025-01-02", periods=20)
    decision_date = dates[12]
    completed = [
        (dates[0], dates[2]),
        (dates[3], dates[5]),
        (dates[6], dates[8]),
    ]
    control = [
        _row(start, end, value)
        for (start, end), value in zip(
            completed, [0.01, -0.01, 0.02], strict=True
        )
    ]
    candidate = [
        _row(completed[0][0], completed[0][1], 0.20),
        _row(completed[2][0], completed[2][1], 0.30),
    ]
    spec = WalkForwardSelectorSpec(
        lookback_trading_days=12,
        minimum_completed_periods=3,
        update_every_trading_days=1,
        control_score_guard=0.0,
    )

    result = causal_candidate_decisions(
        trading_dates=dates,
        signal_dates=[decision_date],
        candidate_periods={"control": control, "candidate": candidate},
        control_factor="control",
        selector=spec,
        periods_per_year=25.2,
    )

    update = result["updates"][0]
    candidate_score = next(
        row for row in update["candidate_scores"] if row["factor_name"] == "candidate"
    )
    assert candidate_score == {
        "factor_name": "candidate",
        "eligible": False,
        "reason": "incomplete_matured_period_coverage",
        "observations": 2,
        "score": None,
    }
    assert update["selected_factor"] == "control"


def test_dynamic_signal_uses_selected_candidate_and_falls_back_per_stock() -> None:
    dates = pd.to_datetime(["2025-01-02", "2025-01-03"])
    frame = pd.DataFrame(
        {
            "date": [dates[0], dates[0], dates[1], dates[1]],
            "ticker": ["A", "B", "A", "B"],
        }
    )
    control = pd.Series([0.1, 0.2, 0.3, 0.4])
    candidate = pd.Series([0.9, None, 0.8, 0.7])
    decisions = {
        "selections": [
            {"signal_date": "2025-01-02", "selected_factor": "candidate"},
            {"signal_date": "2025-01-03", "selected_factor": "control"},
        ]
    }

    signal = build_dynamic_signal(
        frame,
        candidate_signals={"control": control, "candidate": candidate},
        decisions=decisions,
        control_factor="control",
    )

    assert signal.tolist() == [0.9, 0.2, 0.3, 0.4]


def test_top_k_selection_blends_signals_and_uses_control_for_missing_values() -> None:
    date = pd.Timestamp("2025-01-02")
    frame = pd.DataFrame(
        {"date": [date, date], "ticker": ["A", "B"]}
    )
    decisions = {
        "selections": [
            {
                "signal_date": date.date().isoformat(),
                "selected_factor": "candidate_a",
                "selected_factors": ["candidate_a", "candidate_b"],
                "selected_weights": {"candidate_a": 0.5, "candidate_b": 0.5},
            }
        ]
    }

    signal = build_dynamic_signal(
        frame,
        candidate_signals={
            "control": pd.Series([0.2, 0.4]),
            "candidate_a": pd.Series([0.8, None]),
            "candidate_b": pd.Series([0.6, 1.0]),
        },
        decisions=decisions,
        control_factor="control",
    )

    assert signal.tolist() == pytest.approx([0.7, 0.7])


def test_phase_ranking_excludes_any_strategy_with_an_incomplete_offset() -> None:
    def metrics(
        annual_return: float,
        *,
        period_coverage: float | None = 1.0,
        observations: int = 10,
    ) -> dict[str, float | None]:
        return {
            "observations": observations,
            "net_annual_return": annual_return,
            "net_sharpe": annual_return * 10.0,
            "information_ratio": annual_return * 5.0,
            "max_drawdown": -0.10,
            "period_coverage": period_coverage,
            "benchmark_return_coverage_min": 1.0,
            "equal_aum_account_audit_valid": True,
            "daily_nav_path_complete": True,
        }

    rankings = walk_forward_phase_rankings(
        {
            "control": [metrics(0.05), metrics(0.06), metrics(0.07)],
            "complete": [metrics(0.08), metrics(0.09), metrics(0.10)],
            # Its extreme metrics must not influence the eligible percentiles.
            "incomplete": [
                metrics(9.0),
                metrics(9.0, period_coverage=0.99),
                metrics(9.0),
            ],
            "missing": [metrics(8.0), metrics(8.0), metrics(8.0, period_coverage=None)],
        },
        control_factor="control",
        dynamic_factor="complete",
        phase_quantile=0.20,
    )

    assert [row["strategy_name"] for row in rankings[:2]] == ["complete", "control"]
    assert [row["rank"] for row in rankings[:2]] == [1, 2]
    assert all(row["phase_ranking_eligible"] for row in rankings[:2])
    assert all(not row["excluded_from_phase_ranking"] for row in rankings[:2])

    excluded = {row["strategy_name"]: row for row in rankings[2:]}
    assert excluded["incomplete"]["rank"] is None
    assert excluded["incomplete"]["phase_score"] is None
    assert excluded["incomplete"]["phase_score_percentiles"] == {}
    assert excluded["incomplete"]["excluded_from_phase_ranking"] is True
    assert excluded["incomplete"]["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "common_interval_period_coverage_not_one",
            "rebalance_offset_days": 1,
            "observed_period_coverage": 0.99,
            "required_period_coverage": 1.0,
        }
    ]
    assert excluded["missing"]["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "common_interval_period_coverage_not_one",
            "rebalance_offset_days": 2,
            "observed_period_coverage": None,
            "required_period_coverage": 1.0,
        }
    ]


@pytest.mark.parametrize(
    "invalid_metric",
    ["net_annual_return", "net_sharpe", "information_ratio", "max_drawdown"],
)
def test_phase_ranking_requires_finite_metrics_at_every_offset(
    invalid_metric: str,
) -> None:
    def metrics(value: float) -> dict[str, float | int]:
        return {
            "observations": 10,
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": 1.0,
            "equal_aum_account_audit_valid": True,
            "daily_nav_path_complete": True,
            "net_annual_return": value,
            "net_sharpe": value,
            "information_ratio": value,
            "max_drawdown": -value,
        }

    control = [metrics(0.05), metrics(0.06)]
    invalid = [metrics(0.50), metrics(0.60)]
    invalid[1][invalid_metric] = float("nan")

    rankings = walk_forward_phase_rankings(
        {"control": control, "invalid": invalid},
        control_factor="control",
        dynamic_factor="invalid",
        phase_quantile=0.20,
    )

    invalid_row = next(row for row in rankings if row["strategy_name"] == "invalid")
    assert invalid_row["excluded_from_phase_ranking"] is True
    assert invalid_row["rank"] is None
    assert invalid_row["complete_rebalance_offsets"] == [0]
    assert invalid_row["common_complete_rebalance_offsets"] == [0]
    assert invalid_row["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "common_interval_ranking_metric_not_finite",
            "rebalance_offset_days": 1,
            "metric_name": invalid_metric,
            "observed_value": None,
        }
    ]


def test_phase_ranking_requires_positive_observations_at_every_offset() -> None:
    def metrics(value: float, observations: int) -> dict[str, float | int]:
        return {
            "observations": observations,
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": 1.0,
            "equal_aum_account_audit_valid": True,
            "daily_nav_path_complete": True,
            "net_annual_return": value,
            "net_sharpe": value,
            "information_ratio": value,
            "max_drawdown": -value,
        }

    rankings = walk_forward_phase_rankings(
        {
            "control": [metrics(0.05, 10), metrics(0.06, 10)],
            "empty": [metrics(0.50, 10), metrics(0.60, 0)],
        },
        control_factor="control",
        dynamic_factor="empty",
        phase_quantile=0.20,
    )

    empty = next(row for row in rankings if row["strategy_name"] == "empty")
    assert empty["rank"] is None
    assert empty["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "common_interval_observations_not_positive",
            "rebalance_offset_days": 1,
            "observed_observations": 0.0,
            "required_observations_min_exclusive": 0.0,
        }
    ]


def test_phase_ranking_excludes_offset_count_mismatches_without_zip_failure() -> None:
    def metrics(value: float) -> dict[str, float | int]:
        return {
            "observations": 10,
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": 1.0,
            "equal_aum_account_audit_valid": True,
            "daily_nav_path_complete": True,
            "net_annual_return": value,
            "net_sharpe": value,
            "information_ratio": value,
            "max_drawdown": -value,
        }

    rankings = walk_forward_phase_rankings(
        {
            "control": [metrics(0.01), metrics(0.02), metrics(0.03)],
            "complete": [metrics(0.04), metrics(0.05), metrics(0.06)],
            "short": [metrics(0.11), metrics(0.12)],
            "long": [metrics(0.21), metrics(0.22), metrics(0.23), metrics(9.0)],
        },
        control_factor="control",
        dynamic_factor="complete",
        phase_quantile=0.20,
    )

    by_name = {row["strategy_name"]: row for row in rankings}
    assert by_name["complete"]["rank"] == 1
    assert by_name["short"]["rank"] is None
    assert by_name["short"]["common_complete_rebalance_offsets"] == [0, 1]
    assert by_name["short"]["phase_deltas_vs_control"]["net_annual_return"] == {
        "worst": 0.10,
        "q20": 0.10,
        "median": 0.10,
        "best": 0.10,
        "iqr": 0.0,
    }
    assert by_name["short"]["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "missing_configured_offset",
            "rebalance_offset_days": 2,
            "expected_offset_count": 3,
            "observed_offset_count": 2,
        }
    ]
    assert by_name["long"]["rank"] is None
    assert by_name["long"]["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "unexpected_configured_offset",
            "rebalance_offset_days": 3,
            "expected_offset_count": 3,
            "observed_offset_count": 4,
        }
    ]


def test_phase_ranking_excludes_low_benchmark_coverage_before_scoring() -> None:
    def metrics(
        value: float,
        *,
        benchmark_coverage: float = 1.0,
    ) -> dict[str, float | int]:
        return {
            "observations": 10,
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": benchmark_coverage,
            "equal_aum_account_audit_valid": True,
            "daily_nav_path_complete": True,
            "net_annual_return": value,
            "net_sharpe": value,
            "information_ratio": value,
            "max_drawdown": -0.10,
        }

    rankings = walk_forward_phase_rankings(
        {
            "control": [metrics(0.05), metrics(0.06)],
            "eligible": [metrics(0.08), metrics(0.09)],
            # Extreme returns must not enter phase percentiles when one offset's
            # benchmark is below the frozen coverage floor.
            "low_coverage": [
                metrics(9.0),
                metrics(9.0, benchmark_coverage=0.94),
            ],
        },
        control_factor="control",
        dynamic_factor="eligible",
        phase_quantile=0.20,
        benchmark_return_coverage_minimum=0.95,
    )

    by_name = {row["strategy_name"]: row for row in rankings}
    assert by_name["eligible"]["rank"] == 1
    assert by_name["control"]["rank"] == 2
    assert by_name["low_coverage"]["rank"] is None
    assert by_name["low_coverage"]["complete_rebalance_offsets"] == [0]
    assert by_name["low_coverage"]["phase_score_percentiles"] == {}
    assert by_name["low_coverage"]["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "common_interval_benchmark_coverage_below_minimum",
            "rebalance_offset_days": 1,
            "observed_benchmark_return_coverage_min": 0.94,
            "required_benchmark_return_coverage_min": 0.95,
        }
    ]


def test_phase_ranking_excludes_extreme_cash_result_with_missing_execution_inputs() -> None:
    def metrics(value: float, *, valid: bool = True) -> dict[str, object]:
        return {
            "observations": 10,
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": 1.0,
            "equal_aum_account_audit_valid": valid,
            "daily_nav_path_complete": True,
            "equal_aum_account_audit_reasons": []
            if valid
            else ["scoring_common_window_execution_input_coverage_not_one"],
            "equal_aum_account_execution_integrity": []
            if valid
            else [
                {
                    "window": "audit",
                    "observations": 10,
                    "execution_input_coverage": 0.0,
                    "execution_input_future_violation_count": 0,
                    "capacity_violation_count": 0,
                    "valid": False,
                    "exclusion_reasons": [
                        {
                            "reason": "execution_input_coverage_not_one",
                            "observed_value": 0.0,
                            "required_value": 1.0,
                        }
                    ],
                }
            ],
            "net_annual_return": value,
            "net_sharpe": value,
            "information_ratio": value,
            "max_drawdown": 0.0,
        }

    rankings = walk_forward_phase_rankings(
        {
            "control": [metrics(0.05)],
            "eligible": [metrics(0.08)],
            # The all-cash path's superficially perfect drawdown and extreme
            # score cannot enter the phase when ADV inputs were absent.
            "missing_adv": [metrics(99.0, valid=False)],
        },
        control_factor="control",
        dynamic_factor="eligible",
        phase_quantile=0.20,
    )

    by_name = {row["strategy_name"]: row for row in rankings}
    assert by_name["missing_adv"]["rank"] is None
    assert by_name["missing_adv"]["complete_rebalance_offsets"] == []
    assert by_name["missing_adv"]["phase_score_percentiles"] == {}
    assert by_name["missing_adv"]["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "equal_aum_scoring_account_audit_failed",
            "rebalance_offset_days": 0,
            "observed_valid": False,
            "audit_reasons": [
                "scoring_common_window_execution_input_coverage_not_one"
            ],
            "common_window_execution_integrity": [
                {
                    "window": "audit",
                    "observations": 10,
                    "execution_input_coverage": 0.0,
                    "execution_input_future_violation_count": 0,
                    "capacity_violation_count": 0,
                    "valid": False,
                    "exclusion_reasons": [
                        {
                            "reason": "execution_input_coverage_not_one",
                            "observed_value": 0.0,
                            "required_value": 1.0,
                        }
                    ],
                }
            ],
        }
    ]


def test_phase_ranking_explicitly_fails_closed_on_incomplete_daily_nav_path() -> None:
    def metrics(*, complete: bool) -> dict[str, object]:
        return {
            "observations": 10,
            "period_coverage": 1.0,
            "benchmark_return_coverage_min": 1.0,
            "equal_aum_account_audit_valid": True,
            "daily_nav_path_complete": complete,
            "daily_nav_observations": 42 if complete else 3,
            "net_annual_return": 99.0,
            "net_sharpe": 99.0,
            "information_ratio": 99.0,
            # Even a superficially finite endpoint value cannot bypass the
            # explicit daily-path completeness contract.
            "max_drawdown": 0.0,
        }

    rankings = walk_forward_phase_rankings(
        {
            "control": [metrics(complete=True)],
            "incomplete": [metrics(complete=False)],
        },
        control_factor="control",
        dynamic_factor="incomplete",
        phase_quantile=0.20,
    )

    incomplete = next(
        row for row in rankings if row["strategy_name"] == "incomplete"
    )
    assert incomplete["rank"] is None
    assert incomplete["complete_rebalance_offsets"] == []
    assert incomplete["phase_ranking_exclusion_reasons"] == [
        {
            "reason": "common_interval_daily_nav_path_incomplete",
            "rebalance_offset_days": 0,
            "observed_daily_nav_path_complete": False,
            "observed_daily_nav_observations": 3.0,
            "required_daily_nav_path_complete": True,
        }
    ]


@pytest.mark.parametrize(
    "payload",
    [
        {"lookback_trading_days": 0},
        {"minimum_completed_periods": 1},
        {"update_every_trading_days": 0},
        {"score_method": "mean"},
        {"control_score_guard": float("nan")},
        {"selection_count": 0},
        {"selection_weighting": "softmax"},
        {"history_policy": "end_date_at_or_before_signal_date"},
        {"missing_signal_policy": "drop"},
    ],
)
def test_selector_spec_rejects_non_causal_or_invalid_settings(payload: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="walk_forward"):
        WalkForwardSelectorSpec.from_mapping(payload)
