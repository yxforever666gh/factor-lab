from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.portfolio.long_only import (
    LongOnlyCostConfig,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)


def _zero_costs() -> LongOnlyCostConfig:
    return LongOnlyCostConfig(
        commission_rate=0.0,
        slippage_bps_per_side=0.0,
        stamp_duty_before_2023_08_28=0.0,
        stamp_duty_from_2023_08_28=0.0,
        exchange_handling_rate=0.0,
        transfer_fee_rate=0.0,
        impact_coefficient=0.0,
    )


def _panel(
    dates: pd.DatetimeIndex,
    tickers: tuple[str, ...] = ("A", "B", "C"),
    *,
    adv: float = 1_000_000.0,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index, trade_date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            leader = (day_index // 5) % len(tickers)
            rows.append(
                {
                    "date": trade_date,
                    "ticker": ticker,
                    "open": 100.0 + day_index * (ticker_index - 1),
                    "adv_20": adv,
                    "volatility_20": 0.02 + ticker_index * 0.005,
                    "eligible": True,
                    "universe_member": True,
                    "signal": 10.0
                    if ticker_index == leader
                    else float(len(tickers) - ticker_index),
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "is_suspended": False,
                    "is_delisted": False,
                }
            )
    return pd.DataFrame(rows)


def test_migrated_evaluator_preserves_legacy_numerical_regression() -> None:
    frame = _panel(pd.bdate_range("2024-01-02", periods=17))
    values = {
        "capital": 100_000.0,
        "position_count": 2,
        "target_weight": 0.5,
        "max_adv_participation": 0.05,
    }

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(**values),
    )

    assert result.observations == 3
    assert result.trade_count == 7
    assert result.net_return == pytest.approx(-0.06374198)
    assert result.net_annual_return == pytest.approx(-0.66929156)
    assert result.net_sharpe == pytest.approx(-15.01676776)
    assert result.max_drawdown == pytest.approx(-0.06374198)
    assert result.actual_turnover == pytest.approx(0.68191108)
    assert result.total_cost == pytest.approx(730.7622)
    assert [period["selected_tickers"] for period in result.periods] == [
        ["A", "B"],
        ["B", "A"],
        ["C", "A"],
    ]


def test_signal_at_close_executes_next_open_and_marks_after_five_days() -> None:
    dates = pd.bdate_range("2024-02-01", periods=7)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    frame["open"] = 100.0
    frame.loc[(frame["ticker"] == "A") & (frame["date"] == dates[6]), "open"] = 110.0
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        max_adv_participation=1.0,
        costs=_zero_costs(),
    )

    result = evaluate_long_only_portfolio(frame, "signal", config)

    assert result.status == "ok"
    assert result.observations == 1
    assert result.periods[0]["signal_date"] == str(dates[0].date())
    assert result.periods[0]["start_date"] == str(dates[1].date())
    assert result.periods[0]["end_date"] == str(dates[6].date())
    assert result.net_return == pytest.approx(0.10)
    assert result.gross_return == pytest.approx(0.10)
    assert result.average_holding_count == 1.0
    assert result.max_position_weight <= 1.0


def test_limit_down_sell_stays_held_and_never_creates_negative_weight() -> None:
    dates = pd.bdate_range("2024-03-01", periods=12)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    frame["open"] = 100.0
    frame.loc[frame["date"] < dates[5], "signal"] = frame.loc[
        frame["date"] < dates[5], "ticker"
    ].map({"A": 2.0, "B": 1.0})
    frame.loc[frame["date"] >= dates[5], "signal"] = frame.loc[
        frame["date"] >= dates[5], "ticker"
    ].map({"A": 1.0, "B": 2.0})
    frame.loc[
        (frame["date"] == dates[6]) & (frame["ticker"] == "A"),
        "is_one_price_limit_down",
    ] = True

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            costs=_zero_costs(),
        ),
    )

    assert any(
        trade.get("reason") == "one_price_limit_down" for trade in result.trades
    )
    assert "A" in result.periods[1]["weights"]
    assert all(
        weight >= 0.0
        for period in result.periods
        for weight in period["weights"].values()
    )


def test_separate_pricing_frame_keeps_exited_member_available_for_sale() -> None:
    dates = pd.bdate_range("2024-04-01", periods=17)
    prices = _panel(
        dates,
        tuple(f"S{index:02d}" for index in range(12)),
        adv=1_000_000_000.0,
    )
    prices["open"] = 10.0
    prices.loc[
        (prices["ticker"] == "S11") & (prices["date"] >= dates[5]),
        ["eligible", "universe_member"],
    ] = False
    signals = prices.loc[
        prices["universe_member"], ["date", "ticker", "signal"]
    ].copy()
    signals.loc[signals["ticker"] == "S11", "signal"] = 100.0

    result = evaluate_long_only_portfolio(
        signals,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000_000.0,
            position_count=2,
            target_weight=0.5,
            max_adv_participation=1.0,
            costs=_zero_costs(),
        ),
        pricing_frame=prices,
    )

    assert result.status == "ok"
    assert any(
        trade["ticker"] == "S11"
        and trade["side"] == "sell"
        and trade["status"] == "executed"
        for trade in result.trades
    )


def test_missing_execution_column_returns_structured_insufficient_data() -> None:
    frame = _panel(pd.bdate_range("2024-05-06", periods=7)).drop(
        columns=["adv_20"]
    )
    result = evaluate_long_only_portfolio(frame, "signal")

    assert result.status == "insufficient_data"
    assert result.reason == "missing_columns"
    assert result.missing_columns == ["adv_20"]


def test_cost_config_keeps_the_established_cost_contract() -> None:
    assert tuple(LongOnlyCostConfig.__dataclass_fields__) == (
        "commission_rate",
        "slippage_bps_per_side",
        "stamp_duty_before_2023_08_28",
        "stamp_duty_from_2023_08_28",
        "exchange_handling_rate",
        "transfer_fee_rate",
        "impact_coefficient",
    )
