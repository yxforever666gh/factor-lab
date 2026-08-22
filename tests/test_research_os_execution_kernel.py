from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factor_lab.long_only_portfolio import (
    LongOnlyCostConfig,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)
from factor_lab.research_os.execution_kernel import (
    AShareCostPolicy,
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    execute_rebalance,
)
from factor_lab.research_os.shadow import (
    ShadowAccount,
    ShadowExecutionConfig,
    ShadowPortfolioEngine,
)


_COST_VALUES = {
    "commission_rate": 0.0003,
    "slippage_bps_per_side": 5.0,
    "stamp_duty_before_2023_08_28": 0.001,
    "stamp_duty_from_2023_08_28": 0.0005,
    "exchange_handling_rate": 0.0000341,
    "transfer_fee_rate": 0.00001,
    "impact_coefficient": 0.5,
}


def _historical_panel(
    *, adv: float, limit_up: bool = False, delisted: bool = False
) -> pd.DataFrame:
    dates = pd.bdate_range("2026-01-02", periods=7)
    rows: list[dict[str, object]] = []
    for index, session in enumerate(dates):
        rows.append(
            {
                "date": session,
                "ticker": "A",
                "open_adj": 10.0,
                "adv_20": adv,
                "volatility_20": 0.02,
                "eligible": True,
                "universe_member": True,
                "signal": 1.0,
                "is_one_price_limit_up": bool(limit_up and index == 1),
                "is_one_price_limit_down": False,
                "is_suspended": False,
                "is_delisted": bool(delisted and index == 1),
            }
        )
    return pd.DataFrame(rows)


def _shadow_bar(
    *, adv: float, limit_up: bool = False, delisted: bool = False
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-01-05",
                "execution_event_time": "2026-01-05T09:30:00+08:00",
                "execution_available_at": "2026-01-05T09:30:00+08:00",
                "mark_event_time": "2026-01-05T15:00:00+08:00",
                "mark_available_at": "2026-01-05T15:01:00+08:00",
                "gold_snapshot_id": "gold-1",
                "ticker": "A",
                "open_adj": 10.0,
                "close_adj": 10.0,
                "adv_20": adv,
                "volatility_20": 0.02,
                "is_one_price_limit_up": limit_up,
                "is_one_price_limit_down": False,
                "is_suspended": False,
                "is_delisted": delisted,
            }
        ]
    )


def _historical_execution(
    *, adv: float, limit_up: bool = False, delisted: bool = False
):
    panel = _historical_panel(adv=adv, limit_up=limit_up, delisted=delisted)
    result = evaluate_long_only_portfolio(
        panel,
        "signal",
        LongOnlyPortfolioConfig(
            capital=100_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=0.05,
            open_column="open_adj",
            costs=LongOnlyCostConfig(**_COST_VALUES),
        ),
    )
    return result, result.trades[0]


def _shadow_execution(
    *, adv: float, limit_up: bool = False, delisted: bool = False
):
    account = ShadowAccount("parity", initial_capital=100_000.0)
    engine = ShadowPortfolioEngine(
        account,
        ShadowExecutionConfig(
            max_adv_participation=0.05,
            max_position_weight=1.0,
            lot_size=0,
            costs=AShareCostPolicy(**_COST_VALUES),
        ),
    )
    engine.execute_target(
        decision_date="2026-01-02",
        trade_date="2026-01-05",
        expected_next_session="2026-01-05",
        target_weights={"A": 1.0},
        market_bars=_shadow_bar(adv=adv, limit_up=limit_up, delisted=delisted),
        snapshot_id="gold-1",
        model_version="parity-v1",
        trusted_calendar_sessions=("2026-01-02", "2026-01-05"),
    )
    event = next(
        event
        for event in account.events
        if event.event_type in {"fill", "order_blocked"}
    )
    return account, event.payload


def test_historical_and_shadow_have_identical_capacity_fill_and_fees() -> None:
    historical, historical_trade = _historical_execution(adv=1_000_000.0)
    shadow_account, shadow_fill = _shadow_execution(adv=1_000_000.0)

    assert historical_trade["status"] == shadow_fill["status"] == "executed"
    assert historical_trade["requested_notional"] == pytest.approx(
        shadow_fill["requested_notional"]
    )
    assert historical_trade["executed_notional"] == pytest.approx(
        shadow_fill["executed_notional"]
    )
    assert historical_trade["participation"] == pytest.approx(
        shadow_fill["participation"]
    )
    assert historical_trade["costs"] == pytest.approx(shadow_fill["fees"], abs=1e-6)
    assert historical.capacity_violation_count == 1
    assert shadow_account.cash >= 0.0
    assert all(position.quantity >= 0 for position in shadow_account.positions.values())


def test_historical_and_shadow_have_identical_limit_block_reason() -> None:
    historical, historical_trade = _historical_execution(
        adv=1_000_000.0, limit_up=True
    )
    shadow_account, shadow_block = _shadow_execution(
        adv=1_000_000.0, limit_up=True
    )

    assert historical_trade["status"] == shadow_block["status"] == "blocked"
    assert historical_trade["reason"] == shadow_block["reason"] == "one_price_limit_up"
    assert historical_trade["requested_notional"] == pytest.approx(
        shadow_block["requested_notional"]
    )
    assert historical_trade["executed_notional"] == shadow_block["executed_notional"] == 0.0
    assert historical.total_cost == 0.0
    assert shadow_account.cash == 100_000.0


def test_historical_and_shadow_both_refuse_to_buy_a_delisted_stock() -> None:
    historical, historical_trade = _historical_execution(
        adv=1_000_000.0, delisted=True
    )
    shadow_account, shadow_block = _shadow_execution(
        adv=1_000_000.0, delisted=True
    )

    assert historical_trade["status"] == shadow_block["status"] == "blocked"
    assert historical_trade["reason"] == shadow_block["reason"] == "delisted"
    assert historical_trade["executed_notional"] == 0.0
    assert shadow_block["executed_notional"] == 0.0
    assert historical.total_cost == 0.0
    assert shadow_account.positions == {}


@pytest.mark.parametrize("seed", range(12))
def test_kernel_property_never_shorts_or_borrows_cash(seed: int) -> None:
    rng = np.random.default_rng(seed)
    tickers = [f"S{index:02d}" for index in range(12)]
    raw = rng.random(len(tickers))
    raw = raw / raw.sum() * 0.95
    targets = dict(zip(tickers, raw, strict=True))
    bars = pd.DataFrame(
        {
            "ticker": tickers,
            "open": rng.uniform(5.0, 80.0, len(tickers)),
            "adv": rng.uniform(50_000.0, 10_000_000.0, len(tickers)),
            "volatility": rng.uniform(0.0, 0.08, len(tickers)),
            "limit_up": rng.random(len(tickers)) < 0.1,
            "limit_down": False,
            "suspended": rng.random(len(tickers)) < 0.05,
            "delisted": False,
        }
    )
    account = ExecutionAccount(cash=1_000_000.0)
    result = execute_rebalance(
        account,
        targets,
        bars,
        trade_date="2026-01-05",
        policy=ExecutionPolicy(
            max_adv_participation=0.05,
            max_position_weight=0.20,
            lot_size=100,
        ),
        columns=ExecutionColumns(
            open="open",
            mark="open",
            adv="adv",
            volatility="volatility",
            limit_up="limit_up",
            limit_down="limit_down",
            suspended="suspended",
            delisted="delisted",
            split_ratio=None,
            cash_dividend=None,
        ),
    )

    assert account.cash >= 0.0
    assert all(position.quantity >= 0.0 for position in account.positions.values())
    assert all(weight >= 0.0 for weight in result.weights.values())
    assert sum(result.weights.values()) <= 1.0 + 1e-9
    assert all(
        order.executed_notional <= (order.adv or 0.0) * 0.05 + 1e-8
        for order in result.orders
        if order.status == "executed"
    )


def test_kernel_rejects_negative_or_overfunded_targets_before_mutation() -> None:
    bars = pd.DataFrame(
        [{"ticker": "A", "open": 10.0, "adv": 1e9, "volatility": 0.02}]
    )
    account = ExecutionAccount(cash=100_000.0)
    policy = ExecutionPolicy(max_position_weight=1.0)
    columns = ExecutionColumns(
        open="open", mark="open", adv="adv", volatility="volatility"
    )

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
    assert account.cash == 100_000.0
    assert account.positions == {}
