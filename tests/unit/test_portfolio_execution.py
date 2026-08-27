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
    StalePositionViolation,
    apply_corporate_actions,
    calculate_trade_costs,
    execute_rebalance,
    mark_to_market,
    maximum_executable_notional,
    process_account_observation,
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
    assert result.capacity_violation_count == 0
    assert result.capacity_limited_order_count == 1
    assert executed[0].capacity_limited is True
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


def test_full_liquidation_removes_floating_point_position_dust() -> None:
    price = 46.4
    account = ExecutionAccount(
        cash=1_000.0,
        positions={
            "A": ExecutionPosition(
                "A",
                100_000.1,
                price,
                price,
                last_observation_date="2026-01-02",
            )
        },
    )
    market = {
        "A": {
            "open": price,
            "close": price,
            "adv": 1_000_000_000.0,
            "volatility": 0.0,
        }
    }

    result = execute_rebalance(
        account,
        {},
        market,
        trade_date="2026-01-05",
        policy=ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS),
        columns=_columns(),
    )

    assert [order.status for order in result.orders] == ["executed"]
    assert account.positions == {}


def test_stale_policy_discards_preexisting_economically_zero_position() -> None:
    account = ExecutionAccount(
        cash=1_000.0,
        positions={
            "A": ExecutionPosition(
                "A",
                1.4551915228366852e-11,
                37.601642,
                37.601642,
                last_observation_date="2020-09-28",
            )
        },
    )

    result = execute_rebalance(
        account,
        {},
        {},
        trade_date="2020-10-20",
        policy=ExecutionPolicy(
            max_position_weight=1.0,
            costs=ZERO_COSTS,
            max_stale_position_age_days=21,
        ),
        columns=_columns(),
    )

    assert result.stale_position_count == 0
    assert result.accounting_start_nav == 1_000.0
    assert account.positions == {}


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


def test_cash_scaling_keeps_capacity_clipping_diagnostic_not_violation() -> None:
    bars = pd.DataFrame(
        [
            {
                "ticker": "A",
                "open": 10.0,
                "close": 10.0,
                "adv": 19_980.0,
                "volatility": 0.02,
                "limit_up": False,
                "limit_down": False,
                "suspended": False,
                "delisted": False,
                "split_ratio": 1.0,
                "cash_dividend": 0.0,
            }
        ]
    )
    account = ExecutionAccount(cash=1_000.0)

    result = execute_rebalance(
        account,
        {"A": 1.0},
        bars,
        trade_date="2026-01-05",
        policy=ExecutionPolicy(
            max_adv_participation=0.05,
            max_position_weight=1.0,
        ),
        columns=_columns(),
    )

    fill = next(order for order in result.orders if order.status == "executed")
    assert fill.capacity_limited is True
    assert fill.executed_notional < fill.requested_notional
    assert fill.participation < 0.05
    assert result.capacity_limited_order_count == 1
    assert result.capacity_violation_count == 0
    assert account.cash >= 0.0


@pytest.mark.parametrize(
    "adv",
    [None, float("nan"), float("inf"), float("-inf"), 0.0, -1.0],
)
def test_invalid_or_nonpositive_adv_cannot_bypass_capacity(adv: object) -> None:
    account = ExecutionAccount(cash=10_000.0)
    market = pd.DataFrame(
        [
            {
                "ticker": "A",
                "open": 10.0,
                "close": 10.0,
                "adv": adv,
                "volatility": 0.02,
            }
        ]
    )

    result = execute_rebalance(
        account,
        {"A": 0.5},
        market,
        trade_date="2026-08-27",
        policy=ExecutionPolicy(
            max_adv_participation=0.05,
            max_position_weight=1.0,
            costs=ZERO_COSTS,
        ),
        columns=_columns(),
    )

    assert maximum_executable_notional(adv) == 0.0
    assert len(result.orders) == 1
    assert result.orders[0].status == "blocked"
    assert result.orders[0].reason == "missing_adv"
    assert result.orders[0].executed_notional == 0.0
    assert account.cash == 10_000.0
    assert account.positions == {}


@pytest.mark.parametrize(
    ("suspended", "delisted", "expected_reason"),
    [
        (True, False, "suspended"),
        (False, True, "delisted"),
        (True, True, "delisted"),
    ],
)
def test_nontradable_state_precedes_missing_open_for_buys(
    suspended: bool, delisted: bool, expected_reason: str
) -> None:
    account = ExecutionAccount(cash=10_000.0)
    market = pd.DataFrame(
        [
            {
                "ticker": "A",
                "open": float("nan"),
                "close": float("nan"),
                "adv": 1_000_000.0,
                "volatility": 0.02,
                "suspended": suspended,
                "delisted": delisted,
            }
        ]
    )

    result = execute_rebalance(
        account,
        {"A": 0.5},
        market,
        trade_date="2026-08-27",
        policy=ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS),
        columns=_columns(),
    )

    assert [(order.status, order.reason) for order in result.orders] == [
        ("blocked", expected_reason)
    ]
    assert account.cash == 10_000.0
    assert account.positions == {}


def test_permanently_missing_bar_is_audited_then_fails_closed() -> None:
    account = ExecutionAccount(
        cash=100.0,
        positions={
            "A": ExecutionPosition(
                "A",
                10.0,
                10.0,
                10.0,
                last_observation_date="2026-01-01",
            )
        },
    )
    policy = ExecutionPolicy(
        max_position_weight=1.0,
        costs=ZERO_COSTS,
        max_stale_position_age_days=3,
    )

    carried = execute_rebalance(
        account,
        {},
        {},
        trade_date="2026-01-03",
        policy=policy,
        columns=_columns(),
    )

    assert carried.stale_position_count == 1
    assert carried.stale_position_notional == 100.0
    assert carried.max_stale_position_age_days == 2
    assert carried.stale_position_blocked_reasons == {"missing_market_bar": 1}
    assert carried.stale_position_diagnostics[0].action == "carry"
    assert [(order.status, order.reason) for order in carried.orders] == [
        ("blocked", "missing_market_bar")
    ]

    with pytest.raises(StalePositionViolation) as exc_info:
        execute_rebalance(
            account,
            {},
            {},
            trade_date="2026-01-06",
            policy=policy,
            columns=_columns(),
        )

    violation = exc_info.value.diagnostics[0]
    assert violation.blocked_reason == "missing_market_bar"
    assert violation.age_days == 5
    assert violation.action == "violation"
    assert account.cash == 100.0
    assert account.positions["A"].market_value == 100.0


def test_suspended_position_clears_stale_state_when_trading_resumes() -> None:
    account = ExecutionAccount(
        cash=100.0,
        positions={
            "A": ExecutionPosition(
                "A",
                10.0,
                10.0,
                10.0,
                last_observation_date="2026-02-02",
            )
        },
    )
    policy = ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS)
    suspended_bar = {
        "A": {
            "open": 9.0,
            "close": 9.0,
            "adv": 1_000_000.0,
            "volatility": 0.02,
            "suspended": True,
            "delisted": False,
        }
    }

    suspended = execute_rebalance(
        account,
        {},
        suspended_bar,
        trade_date="2026-02-04",
        policy=policy,
        columns=_columns(),
    )

    assert suspended.stale_position_count == 1
    assert suspended.max_stale_position_age_days == 2
    assert suspended.stale_position_blocked_reasons == {"suspended": 1}
    assert account.positions["A"].stale_since_date == "2026-02-04"
    assert account.positions["A"].last_observation_date == "2026-02-02"

    resumed_bar = {
        "A": {
            **suspended_bar["A"],
            "open": 8.0,
            "close": 8.0,
            "suspended": False,
        }
    }
    resumed = execute_rebalance(
        account,
        {},
        resumed_bar,
        trade_date="2026-02-05",
        policy=policy,
        columns=_columns(),
    )

    assert resumed.stale_position_count == 0
    assert resumed.stale_position_blocked_reasons == {}
    assert [(order.status, order.reason) for order in resumed.orders] == [
        ("executed", None)
    ]
    assert account.positions == {}
    assert account.cash == 180.0


def test_suspended_dirty_price_is_not_a_mark_but_resume_price_is() -> None:
    account = ExecutionAccount(
        cash=100.0,
        positions={
            "A": ExecutionPosition(
                "A",
                10.0,
                10.0,
                10.0,
                last_observation_date="2026-02-02",
            )
        },
    )
    policy = ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS)
    dirty_suspended_bar = {
        "A": {
            "open": 999.0,
            "close": 999.0,
            "adv": 1_000_000.0,
            "volatility": 0.02,
            "suspended": True,
            "delisted": False,
        }
    }

    suspended = process_account_observation(
        account,
        dirty_suspended_bar,
        observation_date="2026-02-04",
        policy=policy,
        columns=_columns(),
        mark_at_open=True,
    )

    assert suspended.nav == 200.0
    assert suspended.stale_position_blocked_reasons == {"suspended": 1}
    assert account.positions["A"].last_price == 10.0
    assert account.positions["A"].last_observation_date == "2026-02-02"
    assert account.positions["A"].stale_since_date == "2026-02-04"

    resumed = process_account_observation(
        account,
        {
            "A": {
                **dirty_suspended_bar["A"],
                "open": 8.0,
                "close": 8.0,
                "suspended": False,
            }
        },
        observation_date="2026-02-05",
        policy=policy,
        columns=_columns(),
        mark_at_open=True,
    )

    assert resumed.nav == 180.0
    assert resumed.stale_position_count == 0
    assert account.positions["A"].last_price == 8.0
    assert account.positions["A"].last_observation_date == "2026-02-05"
    assert account.positions["A"].stale_since_date is None


def test_explicit_delist_is_written_down_without_cash_recovery() -> None:
    account = ExecutionAccount(
        cash=50.0,
        positions={
            "A": ExecutionPosition(
                "A",
                10.0,
                10.0,
                10.0,
                last_observation_date="2026-03-02",
            )
        },
    )
    delisted_bar = {
        "A": {
            "open": 8.0,
            "close": 8.0,
            "adv": 1_000_000.0,
            "volatility": 0.02,
            "suspended": False,
            "delisted": True,
        }
    }

    result = execute_rebalance(
        account,
        {},
        delisted_bar,
        trade_date="2026-03-06",
        policy=ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS),
        columns=_columns(),
    )

    assert result.stale_position_count == 1
    assert result.stale_position_notional == 100.0
    assert result.max_stale_position_age_days == 4
    assert result.stale_position_blocked_reasons == {"delisted": 1}
    assert result.stale_position_diagnostics[0].action == "write_down"
    assert [action.action_type for action in result.corporate_actions] == [
        "delist_write_down"
    ]
    assert result.corporate_actions[0].payload["cash_recovery"] == 0.0
    assert result.accounting_start_nav == 150.0
    assert result.pretrade_nav == result.posttrade_nav == 50.0
    assert account.cash == 50.0
    assert account.positions == {}


def test_account_observation_writes_down_delist_without_creating_orders() -> None:
    account = ExecutionAccount(
        cash=0.0,
        positions={
            "A": ExecutionPosition(
                "A",
                10.0,
                10.0,
                10.0,
                last_observation_date="2026-03-02",
            )
        },
    )
    delisted_bar = {
        "A": {
            "open": 8.0,
            "close": 8.0,
            "adv": 1_000_000.0,
            "volatility": 0.02,
            "suspended": False,
            "delisted": True,
            "split_ratio": 2.0,
            "cash_dividend": 1.0,
        }
    }

    result = process_account_observation(
        account,
        delisted_bar,
        observation_date="2026-03-06",
        policy=ExecutionPolicy(max_position_weight=1.0, costs=ZERO_COSTS),
        columns=_columns(),
    )

    assert result.nav == 0.0
    assert result.stale_position_count == 1
    assert result.stale_position_blocked_reasons == {"delisted": 1}
    assert [action.action_type for action in result.corporate_actions] == [
        "delist_write_down"
    ]
    assert result.corporate_actions[0].payload["carrying_notional"] == 100.0
    assert result.corporate_actions[0].payload["cash_recovery"] == 0.0
    assert account.cash == 0.0
    assert account.positions == {}


def test_mark_to_market_refreshes_the_last_trustworthy_observation_date() -> None:
    account = ExecutionAccount(
        cash=100.0,
        positions={
            "A": ExecutionPosition(
                "A",
                10.0,
                10.0,
                10.0,
                last_observation_date="2026-04-01",
                stale_since_date="2026-04-02",
            )
        },
    )
    healthy_bar = {
        "A": {
            "open": 11.0,
            "close": 12.0,
            "adv": 1_000_000.0,
            "volatility": 0.02,
            "suspended": False,
            "delisted": False,
        }
    }

    nav = mark_to_market(
        account,
        healthy_bar,
        columns=_columns(),
        observation_date="2026-04-07",
    )

    assert nav == 220.0
    assert account.positions["A"].last_observation_date == "2026-04-07"
    assert account.positions["A"].stale_since_date is None

    missing = execute_rebalance(
        account,
        {},
        {},
        trade_date="2026-04-10",
        policy=ExecutionPolicy(
            max_position_weight=1.0,
            costs=ZERO_COSTS,
            max_stale_position_age_days=4,
        ),
        columns=_columns(),
    )
    assert missing.max_stale_position_age_days == 3
    assert missing.stale_position_diagnostics[0].action == "carry"
