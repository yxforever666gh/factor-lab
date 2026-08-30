from __future__ import annotations

import math
from types import SimpleNamespace

import pandas as pd
import pytest

from factor_lab.research.wide_universe import (
    CAPACITY_RECONCILIATION_ABS_TOL_RMB,
    CHALLENGER_IDS,
    CONTROL_ID,
    PhaseBounds,
    UNIVERSE_IDS,
    audit_rankings,
    build_target_decisions,
    candidate_gate,
    capacity_metrics,
    select_winner,
    summarize_phase,
    target_maps,
)


def _rankings(calendar: pd.DatetimeIndex) -> pd.DataFrame:
    rows = []
    for candidate in UNIVERSE_IDS:
        for date in calendar:
            for rank in range(1, 26):
                rows.append(
                    {
                        "candidate_id": candidate,
                        "date": date,
                        "ticker": f"T{rank:03d}",
                        "rank": rank,
                        "score": 1.0 - rank / 100.0,
                    }
                )
    return pd.DataFrame(rows)


def test_rank_trace_and_targets_use_independent_absolute_sleeves() -> None:
    calendar = pd.bdate_range("2017-01-03", periods=22)
    rankings = _rankings(calendar)

    audited = audit_rankings(rankings, calendar)
    decisions = build_target_decisions(audited, calendar)

    assert len(decisions) == len(calendar) * len(UNIVERSE_IDS)
    targets0, audits0 = target_maps(decisions, CONTROL_ID, 0)
    targets1, _ = target_maps(decisions, CONTROL_ID, 1)
    assert list(targets0) == [
        calendar[0].date().isoformat(),
        calendar[10].date().isoformat(),
        calendar[20].date().isoformat(),
    ]
    assert list(targets1) == [
        calendar[1].date().isoformat(),
        calendar[11].date().isoformat(),
        calendar[21].date().isoformat(),
    ]
    assert set(next(iter(targets0.values()))) == {f"T{i:03d}" for i in range(1, 11)}
    assert all(value["sleeve"] == 0 for value in audits0.values())


def test_rank_trace_rejects_forward_columns_and_calendar_gaps() -> None:
    calendar = pd.bdate_range("2017-01-03", periods=3)
    rankings = _rankings(calendar)
    rankings["forward_return_5d"] = 0.0
    with pytest.raises(ValueError, match="forbidden columns"):
        audit_rankings(rankings, calendar)

    with pytest.raises(ValueError, match="calendar gap"):
        audit_rankings(_rankings(calendar.delete(1)), calendar)


def test_capacity_metrics_count_buys_sells_and_blocked_orders() -> None:
    periods = [{"start_date": "2024-01-03"}]
    trades = [
        {
            "date": "2024-01-03",
            "side": "buy",
            "requested_notional": 100.0,
            "executed_notional": 50.0,
            "status": "executed",
            "capacity_limited": True,
        },
        {
            "date": "2024-01-03",
            "side": "buy",
            "requested_notional": 100.0,
            "executed_notional": 0.0,
            "status": "blocked",
            "capacity_limited": False,
        },
        {
            "date": "2024-01-03",
            "side": "sell",
            "requested_notional": 200.0,
            "executed_notional": 200.0,
            "status": "executed",
            "capacity_limited": False,
        },
        {
            "date": "2024-02-01",
            "side": "buy",
            "requested_notional": 999.0,
            "executed_notional": 999.0,
            "status": "executed",
            "capacity_limited": True,
        },
    ]

    result = capacity_metrics(trades, periods)

    assert result["requested_notional_total"] == 400.0
    assert result["capacity_limited_requested_notional"] == 100.0
    assert result["executed_notional_total"] == 250.0
    assert result["capacity_limited_requested_notional_ratio"] == 0.25
    assert result["requested_notional_fill_ratio"] == 0.625
    assert result["by_side"]["sell"]["requested_notional_total"] == 200.0


def test_capacity_zero_denominator_is_vacuous_but_activity_fails() -> None:
    result = capacity_metrics([], [])
    assert result["capacity_limited_requested_notional_ratio"] == 0.0
    assert result["requested_notional_fill_ratio"] == 1.0
    assert result["activity_gate_passed"] is False


def test_capacity_metrics_use_stable_large_notional_reduction() -> None:
    periods = [{"start_date": "2024-01-03"}]
    trades = []
    for _ in range(225):
        for side, requested, executed in (
            ("buy", 1_000_000.0002, 1_000_000.0001),
            ("sell", 0.0001, 0.0001),
        ):
            trades.append(
                {
                    "date": "2024-01-03",
                    "side": side,
                    "requested_notional": requested,
                    "executed_notional": executed,
                    "status": "executed",
                    "capacity_limited": True,
                }
            )

    result = capacity_metrics(trades, periods)
    requested_values = [
        float(row["requested_notional"])
        for row in trades
    ]
    executed_values = [
        float(row["executed_notional"])
        for row in trades
    ]
    requested_naive = sum(requested_values)
    requested_by_side_naive = sum(requested_values[0::2]) + sum(
        requested_values[1::2]
    )
    executed_naive = sum(executed_values)
    executed_by_side_naive = sum(executed_values[0::2]) + sum(
        executed_values[1::2]
    )
    assert CAPACITY_RECONCILIATION_ABS_TOL_RMB == 1e-6
    assert abs(requested_naive - requested_by_side_naive) > 1e-6
    assert abs(executed_naive - executed_by_side_naive) > 1e-6
    expected_requested = math.fsum(requested_values)
    expected_executed = math.fsum(executed_values)

    assert result["order_count"] == 450
    assert result["requested_notional_total"] == expected_requested
    assert result["executed_notional_total"] == expected_executed
    assert result["capacity_limited_requested_notional"] == expected_requested
    assert result["requested_notional_fill_ratio"] == (
        expected_executed / expected_requested
    )
    assert result["capacity_limited_requested_notional_ratio"] == 1.0


def _phase(
    relative_return: float,
    *,
    observations: int = 60,
    drawdown: float = -0.20,
    capacity_ratio: float = 0.01,
    fill_ratio: float = 0.99,
) -> dict:
    base_return = 0.01
    candidate_return = (1.0 + base_return) * (1.0 + relative_return) - 1.0
    dates = [f"2020-01-{index + 1:02d}" for index in range(observations)]
    return {
        "observations": observations,
        "net_cagr": candidate_return,
        "daily_max_drawdown": drawdown,
        "mean_turnover": 0.1,
        "capacity_violation_count": 0,
        "execution_input_future_violation_count": 0,
        "execution_input_coverage": 1.0,
        "capacity": {
            "activity_gate_passed": True,
            "capacity_limited_requested_notional_ratio": capacity_ratio,
            "requested_notional_fill_ratio": fill_ratio,
        },
        "signal_dates": dates,
        "start_dates": dates,
        "outcome_end_dates": dates,
        "period_returns": [candidate_return] * observations,
    }


def test_candidate_gate_is_strict_per_offset_and_no_forced_winner() -> None:
    controls = [_phase(0.0) for _ in range(10)]
    candidates = [_phase(0.001) for _ in range(10)]
    passed = candidate_gate(candidates, controls)
    assert passed["passed"] is True

    bad_capacity = [_phase(0.001) for _ in range(10)]
    bad_capacity[7] = _phase(0.001, capacity_ratio=0.051)
    failed = candidate_gate(bad_capacity, controls)
    assert failed["passed"] is False
    assert failed["offset_checks"][7]["checks"][
        "capacity_limited_requested_notional_ratio"
    ] is False

    assert select_winner({}, {}, turnover_by_candidate={}) is None
    train = {candidate: passed for candidate in CHALLENGER_IDS}
    validation = {candidate: passed for candidate in CHALLENGER_IDS}
    winner = select_winner(
        train,
        validation,
        turnover_by_candidate={
            CHALLENGER_IDS[0]: 0.12,
            CHALLENGER_IDS[1]: 0.10,
        },
    )
    assert winner == CHALLENGER_IDS[1]


def test_summarize_phase_uses_exact_end_and_start_date_trade_attribution() -> None:
    periods = [
        {
            "signal_date": "2022-12-20",
            "start_date": "2022-12-21",
            "end_date": "2022-12-30",
            "net_return": 0.01,
            "gross_return": 0.011,
            "turnover": 0.1,
            "costs": {"total": 100.0},
            "accounting_start_nav": 1_000_000.0,
            "blocked_trade_count": 0,
            "capacity_limited_count": 0,
            "capacity_violation_count": 0,
            "forced_delist_write_down_count": 0,
            "execution_input_required_count": 1,
            "execution_input_observed_count": 1,
            "execution_input_future_violation_count": 0,
            "account_nav_path_start_sequence": 0,
            "account_nav_path_end_sequence": 1,
        },
        {
            "signal_date": "2022-12-30",
            "start_date": "2023-01-03",
            "end_date": "2023-01-16",
            "net_return": 0.02,
            "gross_return": 0.021,
            "turnover": 0.1,
            "costs": {"total": 100.0},
            "accounting_start_nav": 1_000_000.0,
            "blocked_trade_count": 0,
            "capacity_limited_count": 0,
            "capacity_violation_count": 0,
            "forced_delist_write_down_count": 0,
            "execution_input_required_count": 1,
            "execution_input_observed_count": 1,
            "execution_input_future_violation_count": 0,
            "account_nav_path_start_sequence": 1,
            "account_nav_path_end_sequence": 2,
        },
    ]
    result = SimpleNamespace(
        periods=periods,
        trades=[
            {
                "date": "2022-12-21",
                "side": "buy",
                "requested_notional": 100.0,
                "executed_notional": 100.0,
                "status": "executed",
                "capacity_limited": False,
            },
            {
                "date": "2023-01-03",
                "side": "sell",
                "requested_notional": 200.0,
                "executed_notional": 200.0,
                "status": "executed",
                "capacity_limited": False,
            },
        ],
        account_nav_path=[
            {"sequence": 0, "nav": 1_000_000.0},
            {"sequence": 1, "nav": 1_010_000.0},
            {"sequence": 2, "nav": 1_030_200.0},
        ],
    )

    phase = summarize_phase(
        result, PhaseBounds.from_values("2017-01-03", "2022-12-31")
    )

    assert phase["observations"] == 1
    assert phase["capacity"]["requested_notional_total"] == 100.0
