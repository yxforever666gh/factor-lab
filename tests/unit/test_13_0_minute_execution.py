from __future__ import annotations

from copy import deepcopy
from math import sqrt

import pandas as pd
import pytest

from factor_lab.portfolio.execution import AShareCostPolicy
from factor_lab.research.pit_stock import PITStockContractError
from factor_lab.research.pit_stock_minute_execution import (
    MINUTE_EXECUTION_BAR_COLUMNS,
    MINUTE_EXECUTION_CONTEXT_COLUMNS,
    PHASE_A,
    PHASE_B,
    PHASE_C,
    PHASE_COMPLETE,
    WINDOW_SPECS,
    SequentialMinutePolicy,
    begin_sequential_minute_rebalance,
    observe_window_a,
    observe_window_b,
    observe_window_c,
)
from factor_lab.research.pit_stock_real_account import (
    PendingShareLot,
    RawMark,
    RealShareAccount,
    RealSharePolicy,
)


DATE = "2021-01-04"
SIGNAL = "2020-12-31"


def _policy(
    *,
    max_adv: float = 0.05,
    max_window: float = 0.05,
    impact: float = 0.0,
) -> SequentialMinutePolicy:
    return SequentialMinutePolicy(
        real_share=RealSharePolicy(
            max_adv_participation=max_adv,
            max_position_weight=1.0,
            costs=AShareCostPolicy(impact_coefficient=impact),
        ),
        max_window_amount_participation=max_window,
        buy_limit_premium=0.01,
    )


def _context_row(ticker: str, **updates: float) -> dict[str, object]:
    value: dict[str, object] = {
        "ticker": ticker,
        "signal_adv20": 1_000_000_000.0,
        "signal_vol_daily": 0.01,
        "up_limit": 11.0,
        "down_limit": 9.0,
    }
    value.update(updates)
    return value


def _context(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=MINUTE_EXECUTION_CONTEXT_COLUMNS)


def _bar_row(
    ticker: str,
    window: str,
    *,
    vwap: float = 10.0,
    close: float | None = None,
    opening: float | None = None,
    high: float | None = None,
    low: float | None = None,
    amount: float = 100_000_000.0,
    volume: float | None = None,
) -> dict[str, object]:
    close = vwap if close is None else close
    opening = vwap if opening is None else opening
    if amount == 0.0:
        volume = 0.0
    elif volume is None:
        volume = amount / vwap
    high = max(opening, close, vwap) + 0.05 if high is None else high
    low = min(opening, close, vwap) - 0.05 if low is None else low
    trade, observable = WINDOW_SPECS[window]
    return {
        "ticker": ticker,
        "trade_time": f"{DATE} {trade}",
        "observable_at": f"{DATE} {observable}",
        "open": opening,
        "high": high,
        "low": low,
        "close": close,
        "volume_shares": volume,
        "amount_rmb": amount,
    }


def _bars(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=MINUTE_EXECUTION_BAR_COLUMNS)


def _marked_account(cash: float = 0.0, **positions: int) -> RealShareAccount:
    account = RealShareAccount(cash, positions=positions)
    for ticker in positions:
        account.marks[ticker] = RawMark(10.0, DATE, "open")
    return account


def _fingerprint(account: RealShareAccount) -> tuple[object, ...]:
    return (
        account.cash,
        deepcopy(account.positions),
        deepcopy(account.pending_shares),
        deepcopy(account.receivables),
        deepcopy(account.marks),
    )


def test_staged_exit_then_buy_is_whole_boundary_atomic_and_reconciles() -> None:
    account = _marked_account(OLD=1000)
    before = _fingerprint(account)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {"NEW": 1.0},
        _context(_context_row("OLD"), _context_row("NEW")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    assert state.phase == PHASE_A
    assert state.required_bar_tickers == ("OLD",)

    observe_window_a(
        state,
        account,
        _bars(_bar_row("OLD", "A", vwap=10.0, close=10.1)),
        postings=postings,
    )
    assert state.phase == PHASE_B
    assert _fingerprint(account) == before
    assert postings == []
    assert state.required_bar_tickers == ("NEW",)

    observe_window_b(
        state,
        account,
        _bars(_bar_row("NEW", "B", vwap=10.0, close=10.0)),
        postings=postings,
    )
    assert state.phase == PHASE_C
    assert _fingerprint(account) == before
    assert postings == []
    planned = state.planned_orders[0].planned_shares

    result = observe_window_c(
        state,
        account,
        _bars(
            _bar_row(
                "NEW", "C", vwap=10.0, close=10.05, high=10.10
            )
        ),
        postings=postings,
    )
    assert state.phase == PHASE_COMPLETE
    assert [order.window for order in result.orders] == ["A", "C"]
    assert result.orders[-1].planned_shares == planned
    assert account.positions["NEW"] % 100 == 0
    assert "OLD" not in account.positions
    assert account.cash >= 0.0
    assert result.reconciliation_error == pytest.approx(0.0, abs=1e-8)
    assert account.nav() - result.pretrade_nav == pytest.approx(
        sum(value.amount for value in postings), abs=1e-8
    )


def test_phase_order_repeat_and_future_columns_fail_without_mutation() -> None:
    account = _marked_account(STAY=100)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {"STAY": 1.0},
        _context(_context_row("STAY")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    before = _fingerprint(account)
    with pytest.raises(PITStockContractError, match="requires awaiting_window_B"):
        observe_window_b(
            state,
            account,
            _bars(_bar_row("STAY", "B")),
            postings=postings,
        )
    future = _bars(_bar_row("STAY", "A")).assign(b_close=99.0)
    with pytest.raises(PITStockContractError, match="future data"):
        observe_window_a(state, account, future, postings=postings)
    assert state.phase == PHASE_A
    assert _fingerprint(account) == before
    assert postings == []

    observe_window_a(
        state,
        account,
        _bars(_bar_row("STAY", "A")),
        postings=postings,
    )
    with pytest.raises(PITStockContractError, match="requires awaiting_window_A"):
        observe_window_a(
            state,
            account,
            _bars(_bar_row("STAY", "A")),
            postings=postings,
        )


def test_wrong_window_clock_or_observability_fails_closed() -> None:
    account = _marked_account(STAY=100)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {"STAY": 1.0},
        _context(_context_row("STAY")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    wrong = _bar_row("STAY", "A")
    wrong["trade_time"] = f"{DATE} 09:41:00"
    with pytest.raises(PITStockContractError, match="time/observability"):
        observe_window_a(state, account, _bars(wrong), postings=postings)
    wrong = _bar_row("STAY", "A")
    wrong["observable_at"] = f"{DATE} 09:35:00"
    with pytest.raises(PITStockContractError, match="time/observability"):
        observe_window_a(state, account, _bars(wrong), postings=postings)
    assert state.phase == PHASE_A


def test_unverified_missing_partition_fails_but_complete_no_bar_blocks() -> None:
    account = _marked_account(OLD=100)
    before = _fingerprint(account)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {},
        _context(_context_row("OLD", up_limit=float("nan"), down_limit=float("nan"))),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    empty = _bars()
    with pytest.raises(PITStockContractError, match="missing or unverified"):
        observe_window_a(state, account, empty, postings=postings)
    observe_window_a(
        state,
        account,
        empty,
        postings=postings,
        complete_no_bar_tickers={"OLD"},
    )
    assert state.orders[0].block_reason == "missing_window_A"
    assert state.working_account.positions == {"OLD": 100}
    assert _fingerprint(account) == before
    assert postings == []
    observe_window_b(
        state,
        account,
        empty,
        postings=postings,
        complete_no_bar_tickers={"OLD"},
    )
    result = observe_window_c(
        state,
        account,
        empty,
        postings=postings,
        complete_no_bar_tickers={"OLD"},
    )
    assert result.orders[0].block_reason == "missing_window_A"
    assert account.positions == {"OLD": 100}
    assert state.phase == PHASE_COMPLETE


def test_actual_sell_bar_requires_exact_price_limits() -> None:
    account = _marked_account(OLD=100)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {},
        _context(_context_row("OLD", up_limit=float("nan"), down_limit=float("nan"))),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    with pytest.raises(PITStockContractError, match="lacks exact daily"):
        observe_window_a(
            state,
            account,
            _bars(_bar_row("OLD", "A")),
            postings=postings,
        )
    assert state.phase == PHASE_A
    assert account.positions == {"OLD": 100}
    assert postings == []


def test_zero_liquidity_blocks_order_but_close_mark_is_retained_staged() -> None:
    account = _marked_account(OLD=100)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {},
        _context(_context_row("OLD")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    observe_window_a(
        state,
        account,
        _bars(_bar_row("OLD", "A", vwap=10.0, close=9.8, amount=0.0)),
        postings=postings,
    )
    assert state.orders[0].block_reason == "zero_window_liquidity"
    assert state.working_account.marks["OLD"].price == pytest.approx(9.8)
    assert account.marks["OLD"].price == pytest.approx(10.0)


def test_partial_reduction_plan_is_frozen_from_a_close_only() -> None:
    planned = []
    for b_vwap in (9.0, 11.0):
        account = _marked_account(STAY=1000)
        postings = []
        state = begin_sequential_minute_rebalance(
            account,
            {"STAY": 0.5},
            _context(_context_row("STAY")),
            trade_date=DATE,
            policy=_policy(),
            postings=postings,
            period_signal=SIGNAL,
        )
        observe_window_a(
            state,
            account,
            _bars(_bar_row("STAY", "A", vwap=10.0, close=10.0)),
            postings=postings,
        )
        plan = state.planned_orders[0]
        planned.append((plan.requested_shares, plan.planned_shares))
        observe_window_b(
            state,
            account,
            _bars(_bar_row("STAY", "B", vwap=b_vwap, close=b_vwap)),
            postings=postings,
        )
    assert planned[0] == planned[1]
    assert planned[0][1] == 500


def test_c_plan_and_cash_reservation_do_not_depend_on_c_data() -> None:
    plans = []
    executions = []
    for c_vwap in (10.0, 10.05):
        account = RealShareAccount(10_000.0)
        postings = []
        state = begin_sequential_minute_rebalance(
            account,
            {"NEW": 1.0},
            _context(_context_row("NEW")),
            trade_date=DATE,
            policy=_policy(),
            postings=postings,
            period_signal=SIGNAL,
        )
        observe_window_a(state, account, _bars(), postings=postings)
        observe_window_b(
            state,
            account,
            _bars(_bar_row("NEW", "B", close=10.0)),
            postings=postings,
        )
        plan = state.planned_orders[0]
        plans.append((plan.requested_shares, plan.planned_shares, plan.reserved_cash))
        result = observe_window_c(
            state,
            account,
            _bars(_bar_row("NEW", "C", vwap=c_vwap, close=c_vwap)),
            postings=postings,
        )
        executions.append(result.orders[0].executed_shares)
    assert plans[0] == plans[1]
    assert executions[0] == executions[1]


def test_window_amount_is_an_independent_capacity_cap() -> None:
    account = _marked_account(OLD=1000)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {},
        _context(_context_row("OLD")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    observe_window_a(
        state,
        account,
        _bars(_bar_row("OLD", "A", amount=10_000.0)),
        postings=postings,
    )
    order = state.orders[0]
    assert order.executed_shares == 0
    assert order.window_capacity_limited is True
    assert order.signal_adv_limited is False


def test_signal_adv_capacity_is_cumulative_across_b_and_c() -> None:
    account = _marked_account(STAY=1000)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {"STAY": 0.5},
        _context(_context_row("STAY", signal_adv20=110_000.0)),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    observe_window_a(
        state,
        account,
        _bars(_bar_row("STAY", "A", close=10.0)),
        postings=postings,
    )
    observe_window_b(
        state,
        account,
        _bars(
            _bar_row(
                "STAY",
                "B",
                vwap=10.0,
                close=5.0,
                high=10.05,
                low=4.95,
            )
        ),
        postings=postings,
    )
    assert dict(state.used_signal_notional)["STAY"] == pytest.approx(5000.0)
    assert state.planned_orders[0].planned_shares >= 100
    result = observe_window_c(
        state,
        account,
        _bars(_bar_row("STAY", "C", vwap=5.0, close=5.0)),
        postings=postings,
    )
    buy = next(order for order in result.orders if order.side == "buy")
    assert buy.executed_shares == 100
    assert buy.signal_adv_limited is True
    assert sum(order.executed_notional for order in result.orders) <= 5500.0 + 1e-9


def test_impact_uses_exact_window_amount_not_signal_adv() -> None:
    account = _marked_account(OLD=1000)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {},
        _context(
            _context_row(
                "OLD", signal_adv20=1_000_000_000.0, signal_vol_daily=0.04
            )
        ),
        trade_date=DATE,
        policy=_policy(impact=0.5),
        postings=postings,
        period_signal=SIGNAL,
    )
    observe_window_a(
        state,
        account,
        _bars(_bar_row("OLD", "A", vwap=10.0, amount=1_000_000.0)),
        postings=postings,
    )
    order = state.orders[0]
    expected = 10_000.0 * 0.5 * 0.04 * sqrt(10_000.0 / 1_000_000.0)
    assert order.costs["impact"] == pytest.approx(expected)


def test_blocking_one_c_order_does_not_reallocate_another_orders_reserve() -> None:
    b_shares = []
    for block_a in (False, True):
        account = RealShareAccount(10_000.0)
        postings = []
        state = begin_sequential_minute_rebalance(
            account,
            {"A": 0.5, "B": 0.5},
            _context(_context_row("A"), _context_row("B")),
            trade_date=DATE,
            policy=_policy(),
            postings=postings,
            period_signal=SIGNAL,
        )
        observe_window_a(state, account, _bars(), postings=postings)
        observe_window_b(
            state,
            account,
            _bars(_bar_row("A", "B"), _bar_row("B", "B")),
            postings=postings,
        )
        planned_b = next(plan for plan in state.planned_orders if plan.ticker == "B")
        a_bar = _bar_row("A", "C")
        if block_a:
            a_bar.update({"high": 10.2, "close": 10.1})
        result = observe_window_c(
            state,
            account,
            _bars(a_bar, _bar_row("B", "C")),
            postings=postings,
        )
        order_b = next(order for order in result.orders if order.ticker == "B")
        assert order_b.planned_shares == planned_b.planned_shares
        b_shares.append(order_b.executed_shares)
    assert b_shares[0] == b_shares[1]


def test_late_c_failure_rolls_back_entire_boundary_and_can_retry() -> None:
    account = _marked_account(OLD=1000)
    before = _fingerprint(account)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {"NEW": 1.0},
        _context(_context_row("OLD"), _context_row("NEW")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    observe_window_a(
        state,
        account,
        _bars(_bar_row("OLD", "A")),
        postings=postings,
    )
    observe_window_b(
        state,
        account,
        _bars(_bar_row("NEW", "B")),
        postings=postings,
    )
    bad = _bar_row("NEW", "C")
    bad["observable_at"] = f"{DATE} 09:49:00"
    with pytest.raises(PITStockContractError, match="time/observability"):
        observe_window_c(state, account, _bars(bad), postings=postings)
    assert state.phase == PHASE_C
    assert _fingerprint(account) == before
    assert postings == []

    result = observe_window_c(
        state,
        account,
        _bars(_bar_row("NEW", "C")),
        postings=postings,
    )
    assert result.orders[-1].status == "executed"
    assert state.phase == PHASE_COMPLETE


def test_pending_shares_are_marked_but_never_sold() -> None:
    account = _marked_account(OLD=100)
    account.pending_shares["p1"] = PendingShareLot(
        "p1", "OLD", 55, "2021-01-04", "2021-01-06"
    )
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {},
        _context(_context_row("OLD")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    observe_window_a(
        state,
        account,
        _bars(_bar_row("OLD", "A", close=10.2)),
        postings=postings,
    )
    order = state.orders[0]
    assert order.requested_shares == 100
    assert order.executed_shares == 100
    assert order.pending_limited is True
    assert state.working_account.pending_by_ticker("OLD") == 55
    assert state.working_account.marks["OLD"].price == pytest.approx(10.2)


def test_external_account_or_postings_mutation_between_stages_fails() -> None:
    account = _marked_account(STAY=100)
    postings = []
    state = begin_sequential_minute_rebalance(
        account,
        {"STAY": 1.0},
        _context(_context_row("STAY")),
        trade_date=DATE,
        policy=_policy(),
        postings=postings,
        period_signal=SIGNAL,
    )
    account.cash += 1.0
    with pytest.raises(PITStockContractError, match="account changed"):
        observe_window_a(
            state,
            account,
            _bars(_bar_row("STAY", "A")),
            postings=postings,
        )
