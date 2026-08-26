from __future__ import annotations

import subprocess
import sys

import pandas as pd
import pytest

from factor_lab.portfolio.execution import (
    AShareCostPolicy,
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    ExecutionPosition,
    apply_corporate_actions,
    calculate_trade_costs,
    execute_rebalance,
    validate_long_only_targets,
)


ZERO_COSTS = AShareCostPolicy(
    commission_rate=0.0,
    slippage_bps_per_side=0.0,
    stamp_duty_before_2023_08_28=0.0,
    stamp_duty_from_2023_08_28=0.0,
    exchange_handling_rate=0.0,
    transfer_fee_rate=0.0,
    impact_coefficient=0.0,
)


def _columns() -> ExecutionColumns:
    return ExecutionColumns(
        open="open",
        mark="close",
        adv="adv",
        volatility="volatility",
        limit_up="limit_up",
        limit_down="limit_down",
        suspended="suspended",
        delisted="delisted",
        split_ratio="split_ratio",
        cash_dividend="cash_dividend",
    )


def test_lightweight_portfolio_import_does_not_load_research_os() -> None:
    command = (
        "import sys; import factor_lab.portfolio; "
        "assert not any(n.startswith('factor_lab.research_os') for n in sys.modules)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", command],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_stamp_duty_changes_by_date_and_only_applies_to_sells() -> None:
    policy = AShareCostPolicy(
        commission_rate=0.0,
        slippage_bps_per_side=0.0,
        exchange_handling_rate=0.0,
        transfer_fee_rate=0.0,
        impact_coefficient=0.0,
    )

    buy = calculate_trade_costs(
        notional=100_000.0,
        side="buy",
        adv=10_000_000.0,
        volatility=0.02,
        trade_date="2023-08-27",
        policy=policy,
    )
    old_sell = calculate_trade_costs(
        notional=100_000.0,
        side="sell",
        adv=10_000_000.0,
        volatility=0.02,
        trade_date="2023-08-27",
        policy=policy,
    )
    new_sell = calculate_trade_costs(
        notional=100_000.0,
        side="sell",
        adv=10_000_000.0,
        volatility=0.02,
        trade_date="2023-08-28",
        policy=policy,
    )

    assert buy["stamp_duty"] == 0.0
    assert old_sell["stamp_duty"] == pytest.approx(100.0)
    assert new_sell["stamp_duty"] == pytest.approx(50.0)


def test_rebalance_enforces_capacity_without_shorting_or_borrowing() -> None:
    bars = pd.DataFrame(
        [
            {
                "ticker": "A",
                "open": 10.0,
                "close": 10.0,
                "adv": 1_000.0,
                "volatility": 0.02,
                "limit_up": False,
                "limit_down": False,
                "suspended": False,
                "delisted": False,
                "split_ratio": 1.0,
                "cash_dividend": 0.0,
            },
            {
                "ticker": "B",
                "open": 20.0,
                "close": 20.0,
                "adv": 1_000.0,
                "volatility": 0.02,
                "limit_up": True,
                "limit_down": False,
                "suspended": False,
                "delisted": False,
                "split_ratio": 1.0,
                "cash_dividend": 0.0,
            },
        ]
    )
    account = ExecutionAccount(cash=1_000.0)

    result = execute_rebalance(
        account,
        {"A": 0.5, "B": 0.5},
        bars,
        trade_date="2026-01-05",
        policy=ExecutionPolicy(
            max_adv_participation=0.05,
            max_position_weight=0.5,
            costs=ZERO_COSTS,
        ),
        columns=_columns(),
    )

    executed = [order for order in result.orders if order.status == "executed"]
    blocked = [order for order in result.orders if order.status == "blocked"]
    assert [(order.ticker, order.executed_notional) for order in executed] == [
        ("A", 50.0)
    ]
    assert [(order.ticker, order.reason) for order in blocked] == [
        ("B", "one_price_limit_up")
    ]
    assert result.capacity_violation_count == 1
    assert result.capacity_usage == pytest.approx(0.05)
    assert account.cash >= 0.0
    assert all(position.quantity >= 0.0 for position in account.positions.values())
    assert account.cash + sum(
        position.market_value for position in account.positions.values()
    ) == pytest.approx(account.nav())


def test_invalid_targets_are_rejected_before_account_mutation() -> None:
    account = ExecutionAccount(cash=1_000.0)
    bars = pd.DataFrame(
        [{"ticker": "A", "open": 10.0, "adv": 1e9, "volatility": 0.02}]
    )
    columns = ExecutionColumns(
        open="open", mark="open", adv="adv", volatility="volatility"
    )
    policy = ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS)

    with pytest.raises(ValueError, match="long-only"):
        execute_rebalance(
            account,
            {"A": -0.01},
            bars,
            trade_date="2026-01-05",
            policy=policy,
            columns=columns,
        )
    with pytest.raises(ValueError, match="100%"):
        execute_rebalance(
            account,
            {"A": 0.8, "B": 0.3},
            bars,
            trade_date="2026-01-05",
            policy=policy,
            columns=columns,
        )

    assert account.cash == 1_000.0
    assert account.positions == {}
    assert validate_long_only_targets(
        {"B": 0.25, "A": 0.5}, max_position_weight=0.5
    ) == {"A": 0.5, "B": 0.25}


def test_corporate_actions_preserve_position_value_and_credit_cash() -> None:
    account = ExecutionAccount(
        cash=0.0,
        positions={"A": ExecutionPosition("A", 10.0, 10.0, 10.0)},
    )
    actions = apply_corporate_actions(
        account,
        {
            "A": {
                "split_ratio": 2.0,
                "cash_dividend": 0.5,
            }
        },
        columns=_columns(),
    )

    assert [action.action_type for action in actions] == ["split", "dividend"]
    assert account.positions["A"].quantity == 20.0
    assert account.positions["A"].last_price == 5.0
    assert account.cash == 10.0
    assert account.nav() == 110.0
