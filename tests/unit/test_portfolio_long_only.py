from __future__ import annotations

import pandas as pd
import pytest

import factor_lab.portfolio.long_only as long_only_module
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


def test_row_map_preserves_iterrows_lookup_semantics() -> None:
    day = pd.DataFrame(
        {
            "ticker": ["A", "B", "A"],
            "open": [10.0, float("nan"), 11.0],
            "volume": [100, 200, 300],
            "eligible": [True, False, True],
            "date": pd.to_datetime(["2024-01-02"] * 3),
        },
        index=[7, 3, 9],
    )
    reference = {
        str(row["ticker"]): row
        for _, row in day.iterrows()
    }

    actual = long_only_module._row_map(day, "ticker")

    assert list(actual) == list(reference)
    for ticker, expected in reference.items():
        assert list(actual[ticker]) == list(expected.index)
        for column, expected_value in expected.items():
            actual_value = actual[ticker][column]
            if pd.isna(expected_value):
                assert pd.isna(actual_value)
            else:
                assert actual_value == expected_value
    assert actual["A"]["open"] == 11.0


def test_shared_period_boundary_reuses_market_row_map(monkeypatch: pytest.MonkeyPatch) -> None:
    dates = pd.bdate_range("2024-01-02", periods=17)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    mapped_dates: list[pd.Timestamp] = []
    mapped_row_counts: list[int] = []
    original = long_only_module._row_map

    def counting_row_map(day: pd.DataFrame, ticker_column: str):
        mapped_dates.append(pd.Timestamp(day["date"].iloc[0]))
        mapped_row_counts.append(len(day))
        return original(day, ticker_column)

    monkeypatch.setattr(long_only_module, "_row_map", counting_row_map)

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

    assert result.observations == 3
    assert mapped_dates == list(dates[1:])
    assert mapped_row_counts == [
        2 if index in {1, 6, 11, 16} else 1
        for index in range(1, 17)
    ]
    assert any(
        trade.get("ticker") == "B"
        and trade.get("date") == dates[6].date().isoformat()
        and trade.get("status") == "executed"
        for trade in result.trades
    )


def test_filtered_daily_observation_preserves_missing_held_bar_diagnostic() -> None:
    dates = pd.bdate_range("2024-01-02", periods=7)
    complete = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    complete["open"] = 100.0
    signals = complete[["date", "ticker", "signal"]].copy()
    pricing = complete.drop(
        index=complete.index[
            complete["ticker"].eq("A") & complete["date"].eq(dates[3])
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
        pricing_frame=pricing,
    )

    assert result.status == "ok"
    assert "unresolved_stale_position_observed" in result.promotion_blockers
    assert result.periods[0]["stale_position_blocked_reasons"] == {
        "missing_market_bar": 1
    }


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


def test_t_plus_one_open_uses_previous_valid_adv_and_volatility() -> None:
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
    assert result.execution_input_policy == "previous_valid_ticker_observation"
    assert result.periods[0]["execution_input_min_date"] == str(dates[0].date())
    assert result.periods[0]["execution_input_max_date"] == str(dates[0].date())
    assert result.periods[0]["execution_input_required_count"] == 1
    assert result.periods[0]["execution_input_observed_count"] == 1
    assert result.periods[0]["execution_input_coverage"] == 1.0
    assert fill["execution_input_complete"] is True


def test_resume_after_event_only_gap_uses_last_valid_execution_input() -> None:
    dates = pd.bdate_range("2026-07-01", periods=12)
    signal_date = dates[5]
    resume_date = dates[6]
    last_valid_date = dates[0]
    pricing_rows: list[dict[str, object]] = []
    for index, trade_date in enumerate(dates):
        event_only = 1 <= index <= 5
        pricing_rows.append(
            {
                "date": trade_date,
                "ticker": "A",
                "open": float("nan") if event_only else 10.0,
                "adv_20": float("nan") if event_only else (
                    20_000.0 if index == 0 else 1_000_000_000.0
                ),
                "volatility_20": float("nan") if event_only else (
                    0.03 if index == 0 else 0.01
                ),
                "is_suspended": event_only,
                "is_delisted": False,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
            }
        )
    pricing = pd.DataFrame(pricing_rows)
    signals = pd.DataFrame(
        {
            "date": [signal_date],
            "ticker": ["A"],
            "signal": [1.0],
        }
    )

    result = evaluate_long_only_portfolio(
        signals,
        "signal",
        LongOnlyPortfolioConfig(
            capital=100_000.0,
            holding_days=5,
            rebalance_every_days=5,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=0.05,
            open_column="open",
            costs=_zero_costs(),
        ),
        pricing_frame=pricing,
    )

    assert result.status == "ok"
    fills = [row for row in result.trades if row.get("status") == "executed"]
    assert len(fills) == 1
    fill = fills[0]
    assert fill["date"] == resume_date.date().isoformat()
    assert fill["execution_input_date"] == last_valid_date.date().isoformat()
    assert fill["adv"] == 20_000.0
    assert fill["executed_notional"] == pytest.approx(1_000.0)
    assert fill["capacity_limited"] is True
    assert fill["execution_input_complete"] is True
    assert result.max_execution_input_age_days == (
        resume_date - last_valid_date
    ).days


def test_resume_ignores_dirty_execution_inputs_on_suspended_price_rows() -> None:
    dates = pd.bdate_range("2026-07-01", periods=12)
    signal_date = dates[5]
    resume_date = dates[6]
    last_tradable_date = dates[0]
    pricing_rows: list[dict[str, object]] = []
    for index, trade_date in enumerate(dates):
        suspended = 1 <= index <= 5
        pricing_rows.append(
            {
                "date": trade_date,
                "ticker": "A",
                # The authoritative suspension overlay may coincide with a
                # vendor price row.  Its positive fields are not tradable
                # observations and must not enter the as-of ADV history.
                "open": 999.0 if suspended else 10.0,
                "adv_20": 999_000_000.0
                if suspended
                else (20_000.0 if index == 0 else 1_000_000_000.0),
                "volatility_20": 0.99 if suspended else 0.01,
                "is_suspended": suspended,
                "is_delisted": False,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
            }
        )
    pricing = pd.DataFrame(pricing_rows)
    signals = pd.DataFrame(
        {
            "date": [signal_date],
            "ticker": ["A"],
            "signal": [1.0],
        }
    )

    result = evaluate_long_only_portfolio(
        signals,
        "signal",
        LongOnlyPortfolioConfig(
            capital=100_000.0,
            holding_days=5,
            rebalance_every_days=5,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=0.05,
            open_column="open",
            costs=_zero_costs(),
        ),
        pricing_frame=pricing,
    )

    assert result.status == "ok"
    fill = next(row for row in result.trades if row.get("status") == "executed")
    assert fill["date"] == resume_date.date().isoformat()
    assert fill["execution_input_date"] == last_tradable_date.date().isoformat()
    assert fill["adv"] == 20_000.0
    assert fill["executed_notional"] == pytest.approx(1_000.0)
    assert fill["capacity_limited"] is True
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


def test_benchmark_rejects_dirty_positive_price_on_suspended_endpoint() -> None:
    dates = pd.bdate_range("2026-01-05", periods=7)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    frame["open"] = 10.0
    dirty_endpoint = (frame["date"] == dates[-1]) & frame["ticker"].eq("B")
    frame.loc[dirty_endpoint, "open"] = 999.0
    frame.loc[dirty_endpoint, "is_suspended"] = True

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

    period = result.periods[0]
    assert result.net_return == 0.0
    assert period["benchmark_return"] == 0.0
    assert period["benchmark_expected_endpoint_count"] == 4
    assert period["benchmark_observed_endpoint_count"] == 3
    assert period["benchmark_complete_return_count"] == 1
    assert period["benchmark_missing_start_count"] == 0
    assert period["benchmark_missing_end_count"] == 1
    assert period["benchmark_endpoint_coverage"] == pytest.approx(0.75)
    assert period["benchmark_return_coverage"] == pytest.approx(0.5)
    assert result.benchmark_return == 0.0
    assert result.benchmark_endpoint_coverage == pytest.approx(0.75)
    assert result.benchmark_return_coverage == pytest.approx(0.5)


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


def test_holding_and_rebalance_intervals_must_share_accounting_boundaries() -> None:
    frame = _panel(pd.bdate_range("2024-01-02", periods=17))

    with pytest.raises(ValueError, match="must equal rebalance_every_days"):
        evaluate_long_only_portfolio(
            frame,
            "signal",
            LongOnlyPortfolioConfig(holding_days=4, rebalance_every_days=5),
        )


def test_evaluation_start_date_skips_prestart_signals_without_reanchoring() -> None:
    dates = pd.bdate_range("2024-11-01", periods=22)
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
            evaluation_start_date=str(dates[4].date()),
            costs=_zero_costs(),
        ),
    )

    assert [period["signal_date"] for period in result.periods] == [
        str(dates[7].date()),
        str(dates[12].date()),
    ]
    assert all(trade["date"] >= str(dates[8].date()) for trade in result.trades)
    assert result.evaluation_start_date == str(dates[7].date())
    assert result.initial_nav == 1_000.0
    assert result.first_pretrade_nav == 1_000.0
    assert result.end_nav == 1_000.0
    assert {
        key: result.to_dict()[key]
        for key in (
            "evaluation_start_date",
            "initial_nav",
            "first_pretrade_nav",
            "end_nav",
        )
    } == {
        "evaluation_start_date": str(dates[7].date()),
        "initial_nav": 1_000.0,
        "first_pretrade_nav": 1_000.0,
        "end_nav": 1_000.0,
    }

    with pytest.raises(ValueError, match="evaluation_start_date"):
        evaluate_long_only_portfolio(
            frame,
            "signal",
            LongOnlyPortfolioConfig(evaluation_start_date="2024/11/07"),
        )


def test_evaluation_start_date_resets_different_prehistories_to_equal_aum() -> None:
    dates = pd.bdate_range("2025-01-02", periods=22)
    first_history = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    second_history = first_history.copy()
    day_number = first_history["date"].map({date: i for i, date in enumerate(dates)})
    first_history.loc[first_history["ticker"] == "A", "open"] = (
        100.0 + 5.0 * day_number[first_history["ticker"] == "A"]
    )
    first_history.loc[first_history["ticker"] == "B", "open"] = 100.0
    second_history["open"] = first_history["open"]
    before_start = first_history["date"] < dates[10]
    first_history.loc[before_start, "signal"] = first_history.loc[
        before_start, "ticker"
    ].map({"A": 2.0, "B": 1.0})
    second_history.loc[before_start, "signal"] = second_history.loc[
        before_start, "ticker"
    ].map({"A": 1.0, "B": 2.0})
    for history in (first_history, second_history):
        history.loc[~before_start, "signal"] = history.loc[
            ~before_start, "ticker"
        ].map({"A": 2.0, "B": 1.0})

    common = dict(
        capital=1_000.0,
        position_count=1,
        target_weight=1.0,
        max_adv_participation=1.0,
        costs=_zero_costs(),
    )
    unreset_first = evaluate_long_only_portfolio(
        first_history, "signal", LongOnlyPortfolioConfig(**common)
    )
    unreset_second = evaluate_long_only_portfolio(
        second_history, "signal", LongOnlyPortfolioConfig(**common)
    )
    assert unreset_first.end_nav != pytest.approx(unreset_second.end_nav)

    start_date = str(dates[10].date())
    reset_first = evaluate_long_only_portfolio(
        first_history,
        "signal",
        LongOnlyPortfolioConfig(**common, evaluation_start_date=start_date),
    )
    reset_second = evaluate_long_only_portfolio(
        second_history,
        "signal",
        LongOnlyPortfolioConfig(**common, evaluation_start_date=start_date),
    )

    assert reset_first.evaluation_start_date == start_date
    assert reset_second.evaluation_start_date == start_date
    assert reset_first.initial_nav == reset_second.initial_nav == 1_000.0
    assert reset_first.first_pretrade_nav == reset_second.first_pretrade_nav == 1_000.0
    assert reset_first.end_nav == pytest.approx(reset_second.end_nav)
    assert reset_first.net_return == pytest.approx(reset_second.net_return)
    assert reset_first.account_nav_path[0] == {
        "date": start_date,
        "phase": "accounting_boundary",
        "nav": 1_000.0,
        "sequence": 0,
    }
    assert reset_second.account_nav_path[0] == reset_first.account_nav_path[0]


def test_delist_write_down_is_in_period_return_and_reconciles_to_account_nav() -> None:
    dates = pd.bdate_range("2026-01-05", periods=7)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    frame["open"] = 100.0
    frame["signal"] = frame["ticker"].map({"A": 2.0, "B": 1.0})
    frame.loc[
        (frame["ticker"] == "A") & (frame["date"] == dates[6]),
        "is_delisted",
    ] = True

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=2,
            target_weight=0.5,
            max_adv_participation=1.0,
            costs=_zero_costs(),
        ),
    )

    assert result.status == "ok"
    assert result.forced_delist_write_down_count == 1
    assert result.forced_delist_write_down_notional == pytest.approx(500.0)
    assert result.end_nav == pytest.approx(500.0)
    assert result.net_return == pytest.approx(-0.5)
    assert result.account_nav_reconciliation_error == pytest.approx(0.0)
    assert len(result.periods) == 1
    assert result.periods[0]["accounting_start_nav"] == pytest.approx(1_000.0)
    assert result.periods[0]["end_nav"] == pytest.approx(500.0)
    assert result.periods[0]["net_return"] == pytest.approx(-0.5)
    assert result.periods[0]["forced_delist_write_down_count"] == 1
    assert all(trade["date"] != str(dates[6].date()) for trade in result.trades)
    compounded = 1.0
    for period in result.periods:
        compounded *= 1.0 + float(period["net_return"])
    assert compounded - 1.0 == pytest.approx(result.net_return, abs=1e-9)


def test_daily_observation_carries_last_normal_mark_into_end_suspension() -> None:
    dates = pd.bdate_range("2026-01-05", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame.loc[
        frame["date"].between(dates[2], dates[5]),
        "open",
    ] = 20.0
    frame.loc[frame["date"] == dates[6], "open"] = float("nan")
    frame.loc[frame["date"] == dates[6], "is_suspended"] = True

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

    assert result.status == "ok"
    assert result.end_nav == pytest.approx(2_000.0)
    assert result.net_return == pytest.approx(1.0)
    assert result.account_nav_reconciliation_error == pytest.approx(0.0)
    period = result.periods[0]
    assert period["end_nav"] == pytest.approx(2_000.0)
    assert period["net_return"] == pytest.approx(1.0)
    assert period["stale_position_blocked_reasons"] == {"suspended": 1}
    diagnostic = period["stale_position_diagnostics"][0]
    assert diagnostic["carrying_notional"] == pytest.approx(2_000.0)
    assert diagnostic["last_observation_date"] == dates[5].date().isoformat()


def test_daily_account_nav_path_exposes_round_trip_drawdown_and_links_period() -> None:
    dates = pd.bdate_range("2026-01-19", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame.loc[frame["date"] == dates[2], "open"] = 5.0

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

    assert result.status == "ok"
    assert result.net_return == pytest.approx(0.0)
    assert result.max_drawdown == pytest.approx(-0.5)
    assert [row["phase"] for row in result.account_nav_path] == [
        "accounting_boundary",
        "posttrade",
        "daily_end",
        "daily_end",
        "daily_end",
        "daily_end",
        "daily_end",
    ]
    assert [row["nav"] for row in result.account_nav_path] == pytest.approx(
        [1_000.0, 1_000.0, 500.0, 1_000.0, 1_000.0, 1_000.0, 1_000.0]
    )
    assert [row["sequence"] for row in result.account_nav_path] == list(range(7))
    assert (
        result.account_nav_path[-1]["nav"] / result.account_nav_path[0]["nav"]
        - 1.0
    ) == pytest.approx(result.net_return)
    period = result.periods[0]
    assert period["account_nav_path_start_sequence"] == 0
    assert period["account_nav_path_end_sequence"] == 6
    assert period["daily_nav_observation_count"] == 5
    assert period["max_drawdown"] == pytest.approx(-0.5)
    assert period["max_drawdown_basis"] == "daily_account_nav"
    assert result.yearly_segments[0]["max_drawdown"] == pytest.approx(-0.5)
    assert result.half_year_segments[0]["max_drawdown"] == pytest.approx(-0.5)


def test_missing_adv_leaves_cash_but_records_incomplete_execution_input() -> None:
    dates = pd.bdate_range("2026-01-19", periods=7)
    frame = _panel(dates, ("A",), adv=float("nan"))
    frame["open"] = 10.0

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

    assert result.status == "ok"
    assert result.end_nav == pytest.approx(1_000.0)
    assert result.net_return == pytest.approx(0.0)
    assert result.average_cash_weight == pytest.approx(1.0)
    assert result.blocked_trade_count == 1
    assert result.capacity_violation_count == 0
    assert result.periods[0]["execution_input_required_count"] == 1
    assert result.periods[0]["execution_input_observed_count"] == 0
    assert result.periods[0]["execution_input_coverage"] == 0.0
    assert {row["nav"] for row in result.account_nav_path} == {1_000.0}


def test_mid_period_delist_is_written_down_on_its_event_session_once() -> None:
    dates = pd.bdate_range("2026-02-02", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame.loc[frame["date"] >= dates[3], "open"] = float("nan")
    frame.loc[frame["date"] >= dates[3], "is_delisted"] = True

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

    assert result.status == "ok"
    assert result.forced_delist_write_down_count == 1
    assert result.forced_delist_write_down_notional == pytest.approx(1_000.0)
    assert result.end_nav == 0.0
    assert result.net_return == -1.0
    assert result.periods[0]["forced_delist_write_down_count"] == 1
    assert result.periods[0]["forced_delist_write_down_notional"] == pytest.approx(
        1_000.0
    )
    assert result.account_nav_reconciliation_error == pytest.approx(0.0)


def test_mid_period_split_and_dividend_are_applied_on_the_event_session_once() -> None:
    dates = pd.bdate_range("2026-02-16", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame["split_ratio"] = 1.0
    frame["cash_dividend"] = 0.0
    frame.loc[frame["date"] >= dates[3], "open"] = 5.0
    frame.loc[frame["date"] == dates[3], "split_ratio"] = 2.0
    frame.loc[frame["date"] == dates[3], "cash_dividend"] = 1.0

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            price_basis="raw_with_actions",
            price_source="synthetic_raw_open",
            costs=_zero_costs(),
        ),
    )

    # 100 shares split to 200 at 5 yuan, then receive one yuan per post-split
    # share exactly once: 1,000 holdings + 200 cash.
    assert result.status == "ok"
    assert result.end_nav == pytest.approx(1_200.0)
    assert result.net_return == pytest.approx(0.2)
    assert result.periods[0]["end_nav"] == pytest.approx(1_200.0)
    assert result.periods[0]["net_return"] == pytest.approx(0.2)
    assert result.account_nav_reconciliation_error == pytest.approx(0.0)


def test_shared_boundary_dividend_is_counted_once_in_ending_period() -> None:
    dates = pd.bdate_range("2026-03-02", periods=12)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 100.0
    frame["cash_dividend"] = 0.0
    frame.loc[frame["date"] == dates[6], "cash_dividend"] = 10.0

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            price_basis="raw_with_actions",
            price_source="synthetic_raw_open",
            costs=_zero_costs(),
        ),
    )

    assert result.status == "ok"
    assert len(result.periods) == 2
    first, second = result.periods
    assert first["end_date"] == second["start_date"] == str(dates[6].date())
    assert first["end_nav"] == pytest.approx(1_100.0)
    assert first["net_return"] == pytest.approx(0.1)
    assert second["accounting_start_nav"] == pytest.approx(1_100.0)
    assert second["pretrade_nav"] == pytest.approx(1_100.0)
    assert second["end_nav"] == pytest.approx(1_100.0)
    assert second["net_return"] == pytest.approx(0.0)
    assert result.end_nav == pytest.approx(1_100.0)
    assert result.net_return == pytest.approx(0.1)
    assert result.account_nav_reconciliation_error == pytest.approx(0.0)
    boundary_path = [
        row
        for row in result.account_nav_path
        if row["date"] == str(dates[6].date())
    ]
    assert [row["phase"] for row in boundary_path] == [
        "daily_end",
        "accounting_boundary",
        "posttrade",
    ]
    assert [row["nav"] for row in boundary_path] == pytest.approx(
        [1_100.0, 1_100.0, 1_100.0]
    )
    assert first["account_nav_path_end_sequence"] + 1 == second[
        "account_nav_path_start_sequence"
    ]


def test_shared_boundary_split_preserves_nav_without_reapplying_event() -> None:
    dates = pd.bdate_range("2026-04-01", periods=12)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 100.0
    frame["split_ratio"] = 1.0
    frame.loc[frame["date"] >= dates[6], "open"] = 50.0
    frame.loc[frame["date"] == dates[6], "split_ratio"] = 2.0

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            price_basis="raw_with_actions",
            price_source="synthetic_raw_open",
            costs=_zero_costs(),
        ),
    )

    assert result.status == "ok"
    assert [period["net_return"] for period in result.periods] == pytest.approx(
        [0.0, 0.0]
    )
    assert [period["end_nav"] for period in result.periods] == pytest.approx(
        [1_000.0, 1_000.0]
    )
    assert result.end_nav == pytest.approx(1_000.0)
    assert result.net_return == pytest.approx(0.0)
    assert result.account_nav_reconciliation_error == pytest.approx(0.0)


@pytest.mark.parametrize(
    ("column", "value"),
    (("split_ratio", 2.0), ("cash_dividend", 1.0)),
)
def test_adjusted_total_return_rejects_non_neutral_corporate_actions(
    column: str,
    value: float,
) -> None:
    dates = pd.bdate_range("2026-05-04", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame["split_ratio"] = 1.0
    frame["cash_dividend"] = 0.0
    frame.loc[frame["date"] == dates[3], column] = value

    with pytest.raises(
        ValueError,
        match="adjusted_total_return forbids non-neutral split/dividend",
    ):
        evaluate_long_only_portfolio(
            frame,
            "signal",
            LongOnlyPortfolioConfig(
                capital=1_000.0,
                position_count=1,
                target_weight=1.0,
                max_adv_participation=1.0,
                price_source="synthetic_hfq",
                costs=_zero_costs(),
            ),
        )


def test_adjusted_total_return_allows_neutral_event_columns_without_enabling_events() -> None:
    dates = pd.bdate_range("2026-06-01", periods=7)
    frame = _panel(dates, ("A",), adv=1_000_000_000.0)
    frame["open"] = 10.0
    frame["split_ratio"] = 1.0
    frame["cash_dividend"] = 0.0

    result = evaluate_long_only_portfolio(
        frame,
        "signal",
        LongOnlyPortfolioConfig(
            capital=1_000.0,
            position_count=1,
            target_weight=1.0,
            max_adv_participation=1.0,
            price_source="synthetic_hfq",
            costs=_zero_costs(),
        ),
    )

    assert result.status == "ok"
    assert result.end_nav == pytest.approx(1_000.0)
    assert result.price_basis == "adjusted_total_return"
    assert result.price_source == "synthetic_hfq"
    assert result.execution_price_column == "open"
    assert result.corporate_action_mode == "embedded_in_adjusted_prices"
    assert result.lot_size == 0


def test_raw_with_actions_rejects_adjusted_execution_column() -> None:
    frame = _panel(pd.bdate_range("2026-07-01", periods=7), ("A",))

    with pytest.raises(
        ValueError,
        match="raw_with_actions requires a non-adjusted execution price column",
    ):
        evaluate_long_only_portfolio(
            frame,
            "signal",
            LongOnlyPortfolioConfig(
                price_basis="raw_with_actions",
                open_column="open_adj",
            ),
        )


def test_empty_scheduled_signal_cross_section_fails_closed() -> None:
    dates = pd.bdate_range("2026-02-02", periods=17)
    frame = _panel(dates, ("A", "B"), adv=1_000_000_000.0)
    frame.loc[frame["date"] == dates[10], "signal"] = float("nan")

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

    assert result.status == "insufficient_data"
    assert result.reason == f"empty_signal_cross_section:{dates[10].date()}"
    assert result.observations == 0
    assert result.end_nav == 0.0
