from __future__ import annotations

import math

import pandas as pd
import pytest

from factor_lab.long_only_portfolio import (
    LongOnlyCostConfig,
    LongOnlyPortfolioConfig,
    evaluate_long_only_portfolio,
)


def _panel(
    dates: list[pd.Timestamp],
    tickers: tuple[str, ...] = ("A", "B", "C"),
    *,
    capital_adv: float = 1_000_000.0,
) -> pd.DataFrame:
    rows = []
    for date_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "open": 100.0,
                    "adv_20": capital_adv,
                    "volatility_20": 0.02,
                    "eligible": True,
                    "universe_member": True,
                    "signal": float(len(tickers) - ticker_index),
                    "is_one_price_limit_up": False,
                    "is_one_price_limit_down": False,
                    "date_index": date_index,
                }
            )
    return pd.DataFrame(rows)


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


def test_uses_close_t_signal_and_open_t_plus_1_to_t_plus_6_return() -> None:
    dates = list(pd.bdate_range("2024-01-02", periods=7))
    frame = _panel(dates)
    frame.loc[(frame["ticker"] == "A") & (frame["date"] == dates[6]), "open"] = 110.0
    frame.loc[(frame["ticker"] == "C") & (frame["date"] == dates[6]), "open"] = 90.0
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=2,
        target_weight=0.5,
        max_adv_participation=0.05,
        costs=_zero_costs(),
    )

    result = evaluate_long_only_portfolio(frame, frame["signal"], config)
    payload = result.to_dict()

    assert result.status == "ok"
    assert result.observations == result.rebalance_count == 1
    assert payload["periods"][0]["signal_date"] == str(dates[0].date())
    assert payload["periods"][0]["start_date"] == str(dates[1].date())
    assert payload["periods"][0]["end_date"] == str(dates[6].date())
    assert payload["gross_return"] == pytest.approx(0.05)
    assert payload["net_return"] == pytest.approx(0.05)
    assert config.periods_per_year == pytest.approx(252 / 5)
    assert payload["gross_annual_return"] == pytest.approx((1.05 ** (252 / 5)) - 1, abs=1e-8)
    assert payload["benchmark_return"] == pytest.approx(0.0)
    assert payload["excess_return"] == pytest.approx(0.05)
    assert payload["average_holding_count"] == 2
    assert all(weight >= 0 for weight in payload["periods"][0]["weights"].values())


def test_executes_explicit_per_signal_date_optimized_target_weights() -> None:
    dates = list(pd.bdate_range("2024-01-02", periods=7))
    frame = _panel(dates)
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=3,
        target_weight=0.7,
        max_adv_participation=1.0,
        costs=_zero_costs(),
    )
    targets = {dates[0]: {"A": 0.6, "B": 0.3, "C": 0.1}}

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        config,
        target_weights_by_date=targets,
        optimization_audit_by_date={
            dates[0]: {"optimizer_status": "ok", "promotion_eligible": True}
        },
        require_optimized_targets=True,
    )

    period = result.periods[0]
    assert result.promotion_eligible is True
    assert result.target_weight_mode == "optimized"
    assert period["target_weights"] == pytest.approx(targets[dates[0]])
    assert period["weights"] == pytest.approx(targets[dates[0]])
    assert period["optimization_audit"]["optimizer_status"] == "ok"


def test_eligibility_is_applied_to_selection_and_equal_weight_benchmark() -> None:
    dates = list(pd.bdate_range("2024-02-01", periods=7))
    frame = _panel(dates)
    frame.loc[(frame["date"] == dates[0]) & (frame["ticker"] == "A"), "universe_member"] = False
    frame.loc[(frame["date"] == dates[0]) & (frame["ticker"] == "C"), "signal"] = float("nan")
    frame.loc[(frame["date"] == dates[6]) & (frame["ticker"] == "A"), "open"] = 200.0
    frame.loc[(frame["date"] == dates[6]) & (frame["ticker"] == "B"), "open"] = 110.0
    frame.loc[(frame["date"] == dates[6]) & (frame["ticker"] == "C"), "open"] = 90.0
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        costs=_zero_costs(),
    )

    result = evaluate_long_only_portfolio(frame, "signal", config)

    assert result.periods[0]["selected_tickers"] == ["B"]
    assert result.net_return == pytest.approx(0.10)
    assert result.benchmark_return == pytest.approx(0.0)


def test_limit_up_buy_is_skipped_and_charges_no_cost() -> None:
    dates = list(pd.bdate_range("2024-03-01", periods=7))
    frame = _panel(dates, ("A",))
    frame.loc[frame["date"] == dates[1], "is_one_price_limit_up"] = True
    config = LongOnlyPortfolioConfig(capital=1_000.0, position_count=1, target_weight=1.0)

    result = evaluate_long_only_portfolio(frame, "signal", config)

    assert result.blocked_trade_count == 1
    assert result.trade_count == 0
    assert result.total_cost == 0.0
    assert result.average_holding_count == 0
    assert result.average_cash_weight == 1.0
    assert result.trades[0]["reason"] == "one_price_limit_up"


def test_limit_down_sell_remains_held_and_all_weights_are_nonnegative() -> None:
    dates = list(pd.bdate_range("2024-04-01", periods=12))
    frame = _panel(dates, ("A", "B"))
    frame.loc[frame["date"] < dates[5], "signal"] = frame.loc[frame["date"] < dates[5], "ticker"].map({"A": 2.0, "B": 1.0})
    frame.loc[frame["date"] >= dates[5], "signal"] = frame.loc[frame["date"] >= dates[5], "ticker"].map({"A": 1.0, "B": 2.0})
    frame.loc[(frame["date"] == dates[6]) & (frame["ticker"] == "A"), "is_one_price_limit_down"] = True
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        costs=_zero_costs(),
    )

    result = evaluate_long_only_portfolio(frame, "signal", config)

    blocked = [trade for trade in result.trades if trade.get("reason") == "one_price_limit_down"]
    assert len(blocked) == 1
    assert "A" in result.periods[1]["weights"]
    assert all(
        weight >= 0
        for period in result.periods
        for weight in period["weights"].values()
    )


def test_adv_five_percent_is_a_hard_execution_cap_and_is_reported() -> None:
    dates = list(pd.bdate_range("2024-05-06", periods=7))
    frame = _panel(dates, ("A",), capital_adv=2_000.0)
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        max_adv_participation=0.05,
        costs=_zero_costs(),
    )

    result = evaluate_long_only_portfolio(frame, "signal", config)
    executed = [trade for trade in result.trades if trade["status"] == "executed"]

    assert executed[0]["requested_notional"] == pytest.approx(1_000.0)
    assert executed[0]["executed_notional"] == pytest.approx(100.0)
    assert executed[0]["participation"] == pytest.approx(0.05)
    assert result.capacity_usage <= 0.05
    assert result.capacity_violation_count == 1
    assert result.periods[0]["weights"]["A"] == pytest.approx(0.1)


def test_costs_apply_only_to_executed_notional_and_impact_uses_square_root_participation() -> None:
    dates = list(pd.bdate_range("2024-06-03", periods=7))
    frame = _panel(dates, ("A",), capital_adv=10_000.0)
    frame["volatility_20"] = 0.04
    costs = LongOnlyCostConfig(
        commission_rate=0.0003,
        slippage_bps_per_side=5.0,
        stamp_duty_before_2023_08_28=0.001,
        stamp_duty_from_2023_08_28=0.0005,
        exchange_handling_rate=0.0000341,
        transfer_fee_rate=0.00001,
        impact_coefficient=0.5,
    )
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=0.5,
        max_adv_participation=0.05,
        costs=costs,
    )

    result = evaluate_long_only_portfolio(frame, "signal", config)
    trade = next(trade for trade in result.trades if trade["status"] == "executed")
    notional = trade["executed_notional"]

    assert notional == pytest.approx(500.0)
    assert result.commission_cost == pytest.approx(notional * 0.0003, abs=1e-4)
    assert result.slippage_cost == pytest.approx(notional * 0.0005, abs=1e-4)
    assert result.stamp_duty_cost == 0.0
    assert result.impact_cost == pytest.approx(notional * 0.5 * 0.04 * math.sqrt(0.05), abs=1e-4)
    assert result.total_cost == pytest.approx(
        result.commission_cost
        + result.slippage_cost
        + result.exchange_handling_cost
        + result.transfer_fee_cost
        + result.impact_cost,
        abs=1e-4,
    )
    assert result.net_return < result.gross_return


def test_stamp_duty_switches_on_2023_08_28_and_only_applies_to_sells() -> None:
    dates = list(pd.bdate_range("2023-08-11", periods=17))
    frame = _panel(dates, ("A", "B", "C"))
    signal_orders = ["A", "B", "C"]
    for period_index, signal_date_index in enumerate((0, 5, 10)):
        leaders = {ticker: 0.0 for ticker in signal_orders}
        leaders[signal_orders[period_index]] = 10.0
        mask = (frame["date"] >= dates[signal_date_index]) & (
            frame["date"] < dates[min(signal_date_index + 5, len(dates) - 1)]
        )
        frame.loc[mask, "signal"] = frame.loc[mask, "ticker"].map(leaders)
    costs = LongOnlyCostConfig(
        commission_rate=0.0,
        slippage_bps_per_side=0.0,
        exchange_handling_rate=0.0,
        transfer_fee_rate=0.0,
        impact_coefficient=0.0,
    )
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        costs=costs,
    )

    result = evaluate_long_only_portfolio(frame, "signal", config)
    sells = [trade for trade in result.trades if trade.get("side") == "sell" and trade.get("status") == "executed"]

    assert [trade["date"] for trade in sells] == ["2023-08-21", "2023-08-28"]
    assert sells[0]["costs"]["stamp_duty"] == pytest.approx(sells[0]["executed_notional"] * 0.001, abs=1e-6)
    assert sells[1]["costs"]["stamp_duty"] == pytest.approx(sells[1]["executed_notional"] * 0.0005, abs=1e-6)
    assert all(trade["costs"]["stamp_duty"] == 0.0 for trade in result.trades if trade.get("side") == "buy" and trade.get("status") == "executed")


def test_reports_yearly_and_half_year_segments_and_required_top_level_metrics() -> None:
    dates = list(pd.bdate_range("2024-06-20", periods=12))
    frame = _panel(dates, ("A",))
    config = LongOnlyPortfolioConfig(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        costs=_zero_costs(),
    )

    payload = evaluate_long_only_portfolio(frame, "signal", config).to_dict()

    required = {
        "benchmark_return", "excess_return", "gross_annual_return", "net_annual_return",
        "net_sharpe", "max_drawdown", "actual_turnover", "capacity_usage",
        "blocked_trade_count", "yearly_segments", "half_year_segments", "observations",
        "rebalance_count", "average_holding_count", "capacity_violation_count",
    }
    assert required <= payload.keys()
    assert [row["label"] for row in payload["yearly_segments"]] == ["2024"]
    assert [row["label"] for row in payload["half_year_segments"]] == ["2024-H1", "2024-H2"]


def test_nested_project_config_mapping_and_missing_data_are_supported() -> None:
    dates = list(pd.bdate_range("2024-07-01", periods=7))
    frame = _panel(dates, ("A",))
    config = {
        "portfolio": {
            "capital": 1_000.0,
            "holding_days": 5,
            "rebalance_every_days": 5,
            "position_count": 1,
            "target_weight": 1.0,
            "max_adv_participation": 0.05,
        },
        "costs": {
            "commission_rate": 0.0,
            "slippage_bps_per_side": 0.0,
            "stamp_duty_before_2023_08_28": 0.0,
            "stamp_duty_from_2023_08_28": 0.0,
            "exchange_handling_rate": 0.0,
            "transfer_fee_rate": 0.0,
            "impact_coefficient": 0.0,
        },
    }

    ok = evaluate_long_only_portfolio(frame, "signal", config)
    missing = evaluate_long_only_portfolio(frame.drop(columns=["adv_20"]), "signal", config)

    assert ok.status == "ok"
    assert missing.status == "insufficient_data"
    assert missing.reason == "missing_columns"
    assert missing.missing_columns == ["adv_20"]


def test_separate_pricing_frame_allows_exited_member_to_be_sold() -> None:
    dates = pd.bdate_range("2024-01-02", periods=17)
    price_rows = []
    signal_rows = []
    for day_index, date in enumerate(dates):
        for ticker_index in range(12):
            ticker = f"S{ticker_index:02d}"
            is_member = not (ticker == "S11" and day_index >= 5)
            price_rows.append({
                "date": date, "ticker": ticker, "open": 10.0 + ticker_index,
                "adv_20": 1_000_000_000.0, "volatility_20": 0.02,
                "eligible": is_member, "universe_member": is_member,
                "is_one_price_limit_up": False, "is_one_price_limit_down": False,
            })
            if is_member:
                signal_rows.append({"date": date, "ticker": ticker, "signal": float(ticker_index)})
    prices = pd.DataFrame(price_rows)
    signals = pd.DataFrame(signal_rows)
    config = LongOnlyPortfolioConfig(
        capital=1_000_000.0,
        position_count=2,
        target_weight=0.5,
        costs=_zero_costs(),
    )

    result = evaluate_long_only_portfolio(signals, "signal", config, pricing_frame=prices)

    assert result.status == "ok"
    assert result.average_holding_count <= 2.0
    assert any(
        row["ticker"] == "S11" and row["side"] == "sell" and row["status"] == "executed"
        for row in result.trades
    )
