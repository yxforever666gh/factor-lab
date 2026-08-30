from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factor_lab.data.opportunity_set import OpportunitySetDataError
from factor_lab.data.wide_pricing import CALENDAR_SENTINEL, SparsePricingBuilder


def _daily(date: pd.Timestamp, ticker: str = "000001.SZ", amount: float = 100.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": [ticker],
            "trade_date": [date.strftime("%Y%m%d")],
            "open": [10.0],
            "high": [10.2],
            "low": [9.8],
            "pre_close": [9.9],
            "pct_chg": [1.0],
            "amount": [amount],
        }
    )


def _factor(date: pd.Timestamp, ticker: str = "000001.SZ") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": [ticker],
            "trade_date": [date.strftime("%Y%m%d")],
            "adj_factor": [2.0],
        }
    )


def test_sparse_pricing_adv_is_exactly_trailing_20_and_future_amount_cannot_change_t() -> None:
    calendar = pd.bdate_range("2024-01-02", periods=21)
    securities = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "list_date": ["19910403"], "delist_date": [None]}
    )
    builder = SparsePricingBuilder(calendar, securities, ["000001.SZ"])
    outputs = []
    for index, date in enumerate(calendar):
        amount = 100.0 if index < 20 else 2_100.0
        outputs.append(
            builder.push_day(date, daily=_daily(date, amount=amount), adj_factor=_factor(date))
        )

    day20 = outputs[19].frame.set_index("ticker").loc["000001.SZ"]
    day21 = outputs[20].frame.set_index("ticker").loc["000001.SZ"]
    assert day20["open_adj"] == 20.0
    assert day20["adv_20"] == 100_000.0
    assert day21["adv_20"] == 200_000.0
    assert outputs[0].frame.set_index("ticker").loc["000001.SZ", "adv_20"] is np.nan or pd.isna(
        outputs[0].frame.set_index("ticker").loc["000001.SZ", "adv_20"]
    )
    assert all(CALENDAR_SENTINEL in set(day.frame["ticker"]) for day in outputs)


def test_missing_bar_requires_suspension_and_failed_push_is_retryable() -> None:
    calendar = pd.bdate_range("2024-01-02", periods=2)
    securities = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "list_date": ["19910403"], "delist_date": [None]}
    )
    builder = SparsePricingBuilder(calendar, securities, ["000001.SZ"])
    builder.push_day(calendar[0], daily=_daily(calendar[0]), adj_factor=_factor(calendar[0]))
    empty_daily = _daily(calendar[1]).iloc[0:0]
    empty_factor = _factor(calendar[1]).iloc[0:0]
    with pytest.raises(OpportunitySetDataError, match="lacks daily bar"):
        builder.push_day(calendar[1], daily=empty_daily, adj_factor=empty_factor)

    suspension = pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "date": [calendar[1]],
            "suspend_type": ["S"],
            "suspend_timing": [pd.NA],
        }
    )
    retried = builder.push_day(
        calendar[1],
        daily=empty_daily,
        adj_factor=empty_factor,
        suspensions=suspension,
    )
    row = retried.frame.set_index("ticker").loc["000001.SZ"]
    assert row["is_suspended"]
    assert pd.isna(row["open_adj"])


def test_sparse_pricing_carries_suspend_event_until_observed_bar() -> None:
    calendar = pd.bdate_range("2024-01-02", periods=4)
    ticker = "000001.SZ"
    securities = pd.DataFrame(
        {"ts_code": [ticker], "list_date": ["19910403"], "delist_date": [None]}
    )
    builder = SparsePricingBuilder(calendar, securities, [ticker])
    suspension = pd.DataFrame(
        {
            "ticker": [ticker],
            "date": [calendar[0]],
            "suspend_type": ["S"],
            "suspend_timing": [pd.NA],
        }
    )

    first = builder.push_day(
        calendar[0],
        daily=_daily(calendar[0]).iloc[0:0],
        adj_factor=_factor(calendar[0]).iloc[0:0],
        suspensions=suspension,
    )
    second = builder.push_day(
        calendar[1],
        daily=_daily(calendar[1]).iloc[0:0],
        adj_factor=_factor(calendar[1]).iloc[0:0],
    )
    assert bool(first.frame.set_index("ticker").loc[ticker, "is_suspended"])
    assert bool(second.frame.set_index("ticker").loc[ticker, "is_suspended"])

    resumed = builder.push_day(
        calendar[2], daily=_daily(calendar[2]), adj_factor=_factor(calendar[2])
    )
    assert not bool(resumed.frame.set_index("ticker").loc[ticker, "is_suspended"])
    with pytest.raises(OpportunitySetDataError, match="lacks daily bar"):
        builder.push_day(
            calendar[3],
            daily=_daily(calendar[3]).iloc[0:0],
            adj_factor=_factor(calendar[3]).iloc[0:0],
        )


def test_intraday_suspension_after_open_does_not_block_open_execution() -> None:
    date = pd.Timestamp("2024-01-02")
    securities = pd.DataFrame(
        {"ts_code": ["000001.SZ"], "list_date": ["19910403"], "delist_date": [None]}
    )
    suspension = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000001.SZ"],
            "date": [date, date],
            "suspend_type": ["S", "R"],
            "suspend_timing": ["09:46:07-09:56:07", pd.NA],
        }
    )
    result = SparsePricingBuilder([date], securities, ["000001.SZ"]).push_day(
        date,
        daily=_daily(date),
        adj_factor=_factor(date),
        suspensions=suspension,
    )
    assert not result.frame.set_index("ticker").loc["000001.SZ", "is_suspended"]

    with pytest.raises(OpportunitySetDataError, match="lacks daily bar"):
        SparsePricingBuilder([date], securities, ["000001.SZ"]).push_day(
            date,
            daily=_daily(date).iloc[0:0],
            adj_factor=_factor(date).iloc[0:0],
            suspensions=suspension.iloc[[0]],
        )


def test_delist_event_is_emitted_on_first_official_session_at_or_after_date() -> None:
    calendar = pd.to_datetime(["2024-01-02", "2024-01-04"])
    securities = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "list_date": ["19910403"],
            "delist_date": ["20240103"],
        }
    )
    builder = SparsePricingBuilder(calendar, securities, ["000001.SZ"])
    builder.push_day(calendar[0], daily=_daily(calendar[0]), adj_factor=_factor(calendar[0]))
    result = builder.push_day(
        calendar[1],
        daily=_daily(calendar[1]).iloc[0:0],
        adj_factor=_factor(calendar[1]).iloc[0:0],
    )
    row = result.frame.set_index("ticker").loc["000001.SZ"]
    assert row["is_delisted"]
    assert not row["eligible"]
    assert pd.isna(row["open_adj"])


def test_adj_factor_alias_prefers_historical_vendor_normalization() -> None:
    dates = pd.bdate_range("2019-12-13", periods=2)
    canonical = "001914.SZ"
    vendor = "000043.SZ"
    securities = pd.DataFrame(
        {
            "ts_code": [canonical],
            "list_date": [pd.Timestamp("1990-01-01")],
            "delist_date": [pd.NaT],
        }
    )
    aliases = [
        {
            "canonical_ts_code": canonical,
            "vendor_ts_code": vendor,
            "effective_from": "1900-01-01",
            "effective_to": "2019-12-15",
            "source": "verified exchange notice",
        }
    ]
    builder = SparsePricingBuilder(
        dates, securities, [canonical], aliases=aliases
    )
    daily = _daily(dates[0], ticker=vendor)
    factors = pd.DataFrame(
        {
            "ts_code": [vendor, canonical],
            "trade_date": [dates[0], dates[0]],
            "adj_factor": [8.333, 8.334],
        }
    )

    result = builder.push_day(dates[0], daily=daily, adj_factor=factors)

    row = result.frame.loc[result.frame["ticker"].eq(canonical)].iloc[0]
    assert row["open_adj"] == pytest.approx(float(daily.iloc[0]["open"]) * 8.333)


def test_missing_stock_st_bar_does_not_invent_suspension_or_price() -> None:
    date = pd.Timestamp("2024-01-02")
    ticker = "000001.SZ"
    securities = pd.DataFrame(
        {"ts_code": [ticker], "list_date": ["19910403"], "delist_date": [None]}
    )
    stock_st = pd.DataFrame(
        {
            "ts_code": [ticker],
            "trade_date": [date],
            "name": ["*ST TEST"],
            "type": ["ST"],
            "type_name": ["risk warning"],
        }
    )

    result = SparsePricingBuilder([date], securities, [ticker]).push_day(
        date,
        daily=_daily(date).iloc[0:0],
        adj_factor=_factor(date).iloc[0:0],
        stock_st=stock_st,
    )

    row = result.frame.set_index("ticker").loc[ticker]
    assert pd.isna(row["open_adj"])
    assert not bool(row["is_suspended"])
    assert pd.isna(row["adv_20"])
