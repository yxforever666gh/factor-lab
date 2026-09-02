from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.portfolio.execution import AShareCostPolicy
from factor_lab.research.pit_stock_real_account import (
    RealShareAccount,
    RealShareAction,
    PendingShareLot,
    RawMark,
    RealSharePolicy,
    accrue_ex_open,
    capture_record_close,
    execute_real_share_rebalance,
    mark_owned_shares,
    prepare_real_share_actions,
    settle_pay_and_list_open,
    write_down_delists,
)


def _action(**updates):
    value = {
        "action_id": "a1",
        "ticker": "000001.SZ",
        "available_date": "2021-01-02",
        "record_date": "2021-01-04",
        "ex_date": "2021-01-05",
        "pay_session": "2021-01-05",
        "list_session": "2021-01-05",
        "stock_dividend_per_share": 0.1,
        "cash_dividend_before_tax_per_share": 1.0,
    }
    value.update(updates)
    return RealShareAction(**value)


def _market(price=10.0, **updates):
    row = {
        "ticker": "000001.SZ",
        "open": price,
        "close": price,
        "signal_adv20": 1_000_000_000.0,
        "signal_vol_daily": 0.01,
        "is_suspended": False,
        "is_one_price_limit_up": False,
        "is_one_price_limit_down": False,
    }
    row.update(updates)
    return pd.DataFrame([row])


def _policy(weight=1.0):
    return RealSharePolicy(
        max_adv_participation=0.05,
        max_position_weight=weight,
        costs=AShareCostPolicy(impact_coefficient=0.0),
    )


def test_record_ex_pay_list_lifecycle_and_pnl_reconcile() -> None:
    account = RealShareAccount(0.0, positions={"000001.SZ": 100})
    postings = []
    mark_owned_shares(
        account,
        _market(10.0),
        session="2021-01-04",
        price_column="close",
        suspended_tickers=set(),
        delisted_tickers=set(),
        postings=postings,
        period_signal="2020-12-31",
        phase="old_period_close",
    )
    start = account.nav()
    capture_record_close(account, [_action()], session="2021-01-04")
    mark_owned_shares(
        account,
        _market(9.0),
        session="2021-01-05",
        price_column="open",
        suspended_tickers=set(),
        delisted_tickers=set(),
        postings=postings,
        period_signal="2020-12-31",
        phase="old_period_open",
    )
    accrue_ex_open(
        account,
        [_action()],
        session="2021-01-05",
        cash_withholding_rate=0.20,
        postings=postings,
        period_signal="2020-12-31",
    )
    assert account.pending_by_ticker("000001.SZ") == 10
    assert sum(item.amount for item in account.receivables.values()) == 80.0
    before_transfer = account.nav()
    settle_pay_and_list_open(account, [_action()], session="2021-01-05")
    assert account.positions["000001.SZ"] == 110
    assert account.pending_shares == {}
    assert account.receivables == {}
    assert account.cash == 80.0
    assert account.nav() == pytest.approx(before_transfer)
    assert account.nav() - start == pytest.approx(sum(row.amount for row in postings))


def test_fractional_stock_dividend_floors_each_action_independently() -> None:
    account = RealShareAccount(0.0, positions={"000001.SZ": 100})
    actions = [_action(action_id="a1", stock_dividend_per_share=0.333), _action(action_id="a2", stock_dividend_per_share=0.333)]
    capture_record_close(account, actions, session="2021-01-04")
    account.marks["000001.SZ"] = __import__(
        "factor_lab.research.pit_stock_real_account", fromlist=["RawMark"]
    ).RawMark(9.0, "2021-01-05", "open")
    postings = []
    accrue_ex_open(
        account,
        actions,
        session="2021-01-05",
        cash_withholding_rate=0.2,
        postings=postings,
        period_signal="2020-12-31",
    )
    assert sorted(lot.shares for lot in account.pending_shares.values()) == [33, 33]


def test_real_share_buy_uses_100_lots_and_minimum_commission() -> None:
    account = RealShareAccount(10_000.0)
    postings = []
    result = execute_real_share_rebalance(
        account,
        {"000001.SZ": 1.0},
        _market(10.0),
        trade_date="2021-01-05",
        policy=_policy(),
        postings=postings,
        period_signal="2020-12-31",
    )
    order = result.orders[0]
    assert order.executed_shares % 100 == 0
    assert order.costs["commission"] == 5.0
    assert account.cash >= 0


def test_partial_sell_is_lot_rounded_but_full_exit_sells_odd_lot() -> None:
    account = RealShareAccount(0.0, positions={"000001.SZ": 155})
    account.marks["000001.SZ"] = __import__(
        "factor_lab.research.pit_stock_real_account", fromlist=["RawMark"]
    ).RawMark(10.0, "2021-01-04", "close")
    postings = []
    partial = execute_real_share_rebalance(
        account,
        {"000001.SZ": 0.2},
        _market(10.0),
        trade_date="2021-01-05",
        policy=_policy(),
        postings=postings,
        period_signal="2020-12-31",
    )
    assert partial.orders[0].executed_shares == 100
    full = execute_real_share_rebalance(
        account,
        {},
        _market(10.0),
        trade_date="2021-01-06",
        policy=_policy(),
        postings=postings,
        period_signal="2020-12-31",
    )
    assert account.positions == {}
    assert full.orders[-1].executed_shares % 100 != 0


def test_delist_writes_down_shares_but_preserves_confirmed_receivable() -> None:
    account = RealShareAccount(0.0, positions={"000001.SZ": 100})
    account.marks["000001.SZ"] = __import__(
        "factor_lab.research.pit_stock_real_account", fromlist=["RawMark"]
    ).RawMark(10.0, "2021-01-04", "close")
    account.receivables["a1"] = __import__(
        "factor_lab.research.pit_stock_real_account", fromlist=["CashReceivable"]
    ).CashReceivable("a1", "000001.SZ", 80.0, "2021-01-05", "2021-01-06")
    postings = []
    write_down_delists(
        account,
        {"000001.SZ"},
        session="2021-01-05",
        postings=postings,
        period_signal="2020-12-31",
    )
    assert account.positions == {}
    assert account.receivables["a1"].amount == 80.0
    assert postings[-1].amount == -1000.0


def test_zero_lot_buy_remains_in_order_ledger() -> None:
    account = RealShareAccount(500.0)
    result = execute_real_share_rebalance(
        account,
        {"000001.SZ": 1.0},
        _market(10.0),
        trade_date="2021-01-05",
        policy=_policy(),
        postings=[],
        period_signal="2020-12-31",
    )
    assert len(result.orders) == 1
    assert result.orders[0].status == "unfilled"
    assert result.orders[0].executed_shares == 0
    assert result.orders[0].requested_notional == 500.0


def test_blocked_buy_gap_uses_carrying_mark_not_intraday_raw_open() -> None:
    account = RealShareAccount(1000.0, positions={"000001.SZ": 100})
    account.marks["000001.SZ"] = RawMark(10.0, "2021-01-04", "close")
    result = execute_real_share_rebalance(
        account,
        {"000001.SZ": 1.0},
        _market(50.0, is_suspended=True),
        trade_date="2021-01-05",
        policy=_policy(),
        postings=[],
        period_signal="2020-12-31",
    )
    buy = [order for order in result.orders if order.side == "buy"]
    assert len(buy) == 1
    assert buy[0].status == "blocked"
    assert buy[0].requested_notional == 1000.0


def test_pending_only_exit_is_retained_as_unfilled_order() -> None:
    account = RealShareAccount(0.0)
    account.pending_shares["a1"] = PendingShareLot(
        "a1", "000001.SZ", 100, "2021-01-04", "2021-01-06"
    )
    account.marks["000001.SZ"] = RawMark(10.0, "2021-01-04", "close")
    result = execute_real_share_rebalance(
        account,
        {},
        _market(10.0),
        trade_date="2021-01-05",
        policy=_policy(),
        postings=[],
        period_signal="2020-12-31",
    )
    assert len(result.orders) == 1
    assert result.orders[0].status == "unfilled"
    assert result.orders[0].pending_limited is True
    assert result.orders[0].requested_notional == 1000.0


def test_prepare_actions_requires_official_record_ex_and_nonbackward_due_dates() -> None:
    frame = pd.DataFrame(
        [
            {
                "action_id": "a1",
                "ticker": "000001.SZ",
                "available_date": "2021-01-04",
                "record_date": "2021-01-04",
                "ex_date": "2021-01-05",
                "pay_date": "2021-01-03",
                "share_arrival_date": "2021-01-03",
                "stock_dividend_per_share": 0.1,
                "cash_dividend_before_tax_per_share": 1.0,
            }
        ]
    )
    with pytest.raises(ValueError, match="precedes ex"):
        prepare_real_share_actions(
            frame, pd.to_datetime(["2021-01-04", "2021-01-05"])
        )
