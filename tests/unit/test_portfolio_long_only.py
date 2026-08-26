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
    assert result.capacity_violation_count == 0
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


def test_capacity_limited_target_is_reported_without_actual_violation() -> None:
    dates = pd.bdate_range("2024-06-03", periods=7)
    frame = _panel(dates, ("A",), adv=19_980.0)
    frame["open"] = 10.0

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=0.05,
        ),
    )

    assert result.capacity_limited_count == 1
    assert result.capacity_violation_count == 0
    assert result.periods[0]["capacity_limited_count"] == 1
    assert result.periods[0]["capacity_violation_count"] == 0
    assert result.trades[0]["capacity_limited"] is True
    assert result.trades[0]["participation"] < 0.05


def test_t_plus_one_open_uses_previous_visible_adv_and_volatility() -> None:
    dates = pd.bdate_range("2024-07-01", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame.loc[frame["date"] == dates[0], "adv_20"] = 10_000.0
    frame.loc[frame["date"] == dates[0], "volatility_20"] = 0.20
    frame.loc[frame["date"] == dates[1], "volatility_20"] = 0.0
    costs = LongOnlyCostConfig(
        commission_rate=0.0,
        slippage_bps_per_side=0.0,
        stamp_duty_before_2023_08_28=0.0,
        stamp_duty_from_2023_08_28=0.0,
        exchange_handling_rate=0.0,
        transfer_fee_rate=0.0,
        impact_coefficient=1.0,
    )


    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=0.05,
            costs=costs,
        ),
    )

    fill = next(trade for trade in result.trades if trade["status"] == "executed")
    assert fill["adv"] == 10_000.0
    assert fill["capacity_limited"] is True
    assert fill["execution_input_date"] == str(dates[0].date())
    assert fill["costs"]["impact"] > 0.0
    assert result.capacity_limited_count == 1
    assert result.execution_input_policy == "previous_visible_ticker_row"
    assert result.periods[0]["execution_input_min_date"] == str(dates[0].date())
    assert result.periods[0]["execution_input_max_date"] == str(dates[0].date())
    assert result.periods[0]["execution_input_required_count"] == 1
    assert result.periods[0]["execution_input_observed_count"] == 1
    assert result.periods[0]["execution_input_coverage"] == 1.0
    assert fill["execution_input_complete"] is True


def test_pricing_warmup_cannot_shift_the_signal_rebalance_anchor() -> None:
    dates = pd.bdate_range("2024-04-01", periods=20)
    prices = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    signals = prices.loc[
        prices["date"] >= dates[3], ["date", "ticker", "signal"]
    ].copy()

    result = evaluate_long_only_portfolio(
        signals,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            costs=_zero_costs(),
        ),
        pricing_frame=prices,
    )

    assert result.periods[0]["signal_date"] == str(dates[3].date())
    assert result.periods[1]["signal_date"] == str(dates[8].date())


def test_benchmark_endpoint_coverage_reports_missing_start_and_end() -> None:
    dates = pd.bdate_range("2024-08-01", periods=7)
    complete = _panel(dates, ("A", "B", "C"), adv=1_000_000_000.0)
    complete["open"] = 100.0
    signals = complete[["date", "ticker", "signal"]].copy()
    prices = complete.drop(
        index=complete.index[
            ((complete["ticker"] == "C") & (complete["date"] == dates[1]))
            | ((complete["ticker"] == "B") & (complete["date"] == dates[6]))
        ]
    ).copy()

    result = evaluate_long_only_portfolio(
        signals,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            costs=_zero_costs(),
        ),
        pricing_frame=prices,
    )

    period = result.periods[0]
    assert period["benchmark_expected_endpoint_count"] == 6
    assert period["benchmark_observed_endpoint_count"] == 4
    assert period["benchmark_complete_return_count"] == 1
    assert period["benchmark_missing_start_count"] == 1
    assert period["benchmark_missing_end_count"] == 1
    assert period["benchmark_endpoint_coverage"] == pytest.approx(2 / 3)
    assert period["benchmark_return_coverage"] == pytest.approx(1 / 3)
    assert result.benchmark_expected_endpoint_count == 6
    assert result.benchmark_observed_endpoint_count == 4
    assert result.benchmark_complete_return_count == 1
    assert result.benchmark_endpoint_coverage == pytest.approx(2 / 3)
    assert result.benchmark_return_coverage == pytest.approx(1 / 3)


def test_retention_buffer_keeps_incumbent_until_it_falls_below_exit_rank() -> None:
    dates = pd.bdate_range("2024-09-02", periods=17)
    frame = _panel(dates, ("A", "B", "C", "D"), adv=1_000_000_000.0)
    frame["open"] = 100.0
    rankings = (
        {"A": 4.0, "B": 3.0, "C": 2.0, "D": 1.0},
        {"A": 3.0, "B": 2.0, "C": 4.0, "D": 1.0},
        {"A": 3.0, "B": 1.0, "C": 4.0, "D": 2.0},
    )
    for day_index, trade_date in enumerate(dates):
        mapping = rankings[min(day_index // 5, 2)]
        mask = frame["date"] == trade_date
        frame.loc[mask, "signal"] = frame.loc[mask, "ticker"].map(mapping)

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=2,
            retention_buffer=1,
            target_weight=0.5,
            max_adv_participation=1.0,
            costs=_zero_costs(),
        ),
    )

    assert [period["selected_tickers"] for period in result.periods] == [
        ["A", "B"],
        ["A", "B"],
        ["C", "A"],
    ]
    assert [period["target_entry_count"] for period in result.periods] == [2, 0, 1]
    assert [period["target_exit_count"] for period in result.periods] == [0, 0, 1]
    assert [period["retained_target_count"] for period in result.periods] == [0, 2, 1]
    assert result.retention_buffer == 1
    assert result.total_target_entry_count == 3
    assert result.total_target_exit_count == 1


def test_rebalance_offset_changes_fixed_anchor_and_is_validated() -> None:
    dates = pd.bdate_range("2024-10-01", periods=14)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    frame["open"] = 100.0

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            rebalance_offset_days=2,
            costs=_zero_costs(),
        ),
    )

    assert [period["signal_date"] for period in result.periods] == [
        str(dates[2].date()),
        str(dates[7].date()),
    ]
    assert all(period["rebalance_offset_days"] == 2 for period in result.periods)
    assert result.rebalance_offset_days == 2

    with pytest.raises(ValueError, match="rebalance_offset_days"):
        evaluate_long_only_portfolio(
            frame,
            "signal",
            LongOnlyPortfolioConfig(rebalance_offset_days=5),
        )
