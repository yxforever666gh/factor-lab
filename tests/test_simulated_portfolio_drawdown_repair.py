from factor_lab.simulated_portfolio_drawdown_repair import (
    build_drawdown_repair_result,
    is_drawdown_safe_candidate,
    rank_drawdown_repair_candidates,
)


def _candidate(
    combo_id,
    *,
    max_drawdown,
    risk_adjusted_return=0.5,
    turnover_one_way_estimate=0.2,
    estimated_round_trip_cost=0.002,
):
    return {
        "combo_id": combo_id,
        "max_drawdown": max_drawdown,
        "risk_adjusted_return": risk_adjusted_return,
        "turnover_one_way_estimate": turnover_one_way_estimate,
        "estimated_round_trip_cost": estimated_round_trip_cost,
    }


def test_candidate_at_or_below_drawdown_limit_is_not_drawdown_safe():
    unsafe = _candidate("unsafe", max_drawdown=-0.35)

    assert is_drawdown_safe_candidate(unsafe, max_drawdown_limit=-0.35) is False


def test_candidate_above_drawdown_limit_with_acceptable_turnover_and_cost_is_safe():
    safe = _candidate(
        "safe",
        max_drawdown=-0.30,
        turnover_one_way_estimate=0.20,
        estimated_round_trip_cost=0.003,
    )

    assert (
        is_drawdown_safe_candidate(
            safe,
            max_drawdown_limit=-0.35,
            turnover_limit=0.35,
            round_trip_cost_limit=0.005,
        )
        is True
    )


def test_ranking_prefers_safe_candidates_then_return_then_lower_turnover_and_cost():
    unsafe_high_return = _candidate(
        "unsafe_high_return",
        max_drawdown=-0.50,
        risk_adjusted_return=9.0,
        turnover_one_way_estimate=0.01,
        estimated_round_trip_cost=0.0001,
    )
    lower_return_safe = _candidate(
        "lower_return_safe",
        max_drawdown=-0.30,
        risk_adjusted_return=0.7,
        turnover_one_way_estimate=0.10,
        estimated_round_trip_cost=0.001,
    )
    higher_return_higher_turnover_safe = _candidate(
        "higher_return_higher_turnover_safe",
        max_drawdown=-0.31,
        risk_adjusted_return=0.9,
        turnover_one_way_estimate=0.30,
        estimated_round_trip_cost=0.001,
    )
    higher_return_lower_turnover_safe = _candidate(
        "higher_return_lower_turnover_safe",
        max_drawdown=-0.32,
        risk_adjusted_return=0.9,
        turnover_one_way_estimate=0.20,
        estimated_round_trip_cost=0.001,
    )
    same_return_turnover_lower_cost_safe = _candidate(
        "same_return_turnover_lower_cost_safe",
        max_drawdown=-0.33,
        risk_adjusted_return=0.9,
        turnover_one_way_estimate=0.20,
        estimated_round_trip_cost=0.0005,
    )

    ranked = rank_drawdown_repair_candidates(
        [
            unsafe_high_return,
            lower_return_safe,
            higher_return_higher_turnover_safe,
            higher_return_lower_turnover_safe,
            same_return_turnover_lower_cost_safe,
        ],
        max_drawdown_limit=-0.35,
        turnover_limit=0.35,
        round_trip_cost_limit=0.005,
    )

    assert [row["combo_id"] for row in ranked] == [
        "same_return_turnover_lower_cost_safe",
        "higher_return_lower_turnover_safe",
        "higher_return_higher_turnover_safe",
        "lower_return_safe",
        "unsafe_high_return",
    ]


def test_blocked_result_reports_best_available_drawdown_and_gap_when_no_candidate_is_safe():
    payload = build_drawdown_repair_result(
        [
            _candidate("deep_drawdown", max_drawdown=-0.50, risk_adjusted_return=1.0),
            _candidate("near_miss", max_drawdown=-0.36, risk_adjusted_return=0.4),
        ],
        max_drawdown_limit=-0.35,
        turnover_limit=0.35,
        round_trip_cost_limit=0.005,
    )

    assert payload["repair_status"] == "blocked_no_drawdown_safe_candidate"
    assert payload["candidate_count"] == 0
    assert payload["recommended_candidate"] is None
    assert payload["best_available_max_drawdown"] == -0.36
    assert payload["drawdown_gap_to_limit"] == 0.01
