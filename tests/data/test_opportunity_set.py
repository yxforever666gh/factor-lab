from __future__ import annotations

from collections.abc import Sequence
import json

import numpy as np
import pandas as pd
import pytest

import factor_lab.data.opportunity_set as opportunity_module
from factor_lab.data.opportunity_set import (
    DailyOpportunitySetBuilder,
    OpportunitySetDataError,
    UNIVERSE_ADV_GE_100M,
    UNIVERSE_TOP1500,
    UNIVERSE_TOP500,
    normalize_security_aliases,
)


def _calendar(periods: int = 140) -> pd.DatetimeIndex:
    return pd.bdate_range("2024-01-02", periods=periods)


def _securities(
    tickers: Sequence[str],
    calendar: Sequence[pd.Timestamp],
    *,
    list_index: int = 0,
    delist_dates: dict[str, pd.Timestamp] | None = None,
) -> pd.DataFrame:
    delist_dates = delist_dates or {}
    return pd.DataFrame(
        {
            "ts_code": list(tickers),
            "list_date": [calendar[list_index]] * len(tickers),
            "delist_date": [delist_dates.get(ticker, pd.NaT) for ticker in tickers],
        }
    )


def _daily(
    date: pd.Timestamp,
    tickers: Sequence[str],
    *,
    amounts: float | Sequence[float] = 100_000.0,
    pct_chg: float | Sequence[float] = 1.0,
) -> pd.DataFrame:
    if np.isscalar(amounts):
        amounts = [float(amounts)] * len(tickers)
    if np.isscalar(pct_chg):
        pct_chg = [float(pct_chg)] * len(tickers)
    return pd.DataFrame(
        {
            "trade_date": [date] * len(tickers),
            "ts_code": list(tickers),
            "amount": list(amounts),
            "pct_chg": list(pct_chg),
        }
    )


def _daily_basic(
    date: pd.Timestamp,
    tickers: Sequence[str],
    *,
    pe_ttm: float | Sequence[float] = 10.0,
    pb: float | Sequence[float] = 1.0,
) -> pd.DataFrame:
    if np.isscalar(pe_ttm):
        pe_ttm = [float(pe_ttm)] * len(tickers)
    if np.isscalar(pb):
        pb = [float(pb)] * len(tickers)
    return pd.DataFrame(
        {
            "trade_date": [date] * len(tickers),
            "ts_code": list(tickers),
            "pe_ttm": list(pe_ttm),
            "pb": list(pb),
        }
    )


def _known_empty_st() -> pd.DataFrame:
    return pd.DataFrame(columns=["trade_date", "ts_code", "is_st"])


def _push_normal_day(
    builder: DailyOpportunitySetBuilder,
    date: pd.Timestamp,
    tickers: Sequence[str],
    *,
    amounts: float | Sequence[float] = 100_000.0,
    pct_chg: float | Sequence[float] = 1.0,
    pe_ttm: float | Sequence[float] = 10.0,
    pb: float | Sequence[float] = 1.0,
    stock_st: pd.DataFrame | None = None,
    suspensions: pd.DataFrame | None = None,
):
    return builder.push_day(
        date,
        daily=_daily(date, tickers, amounts=amounts, pct_chg=pct_chg),
        daily_basic=_daily_basic(date, tickers, pe_ttm=pe_ttm, pb=pb),
        stock_st=_known_empty_st() if stock_st is None else stock_st,
        suspensions=suspensions,
    )


def test_alias_normalization_merges_identical_overlap_and_rejects_conflict() -> None:
    date = pd.Timestamp("2019-12-15")
    aliases = [
        {
            "canonical_ts_code": "001914.SZ",
            "vendor_ts_code": "000043.SZ",
            "effective_from": "1990-01-01",
            "effective_to": "2019-12-15",
            "source": "verified exchange notice",
        }
    ]
    identical = pd.DataFrame(
        {
            "trade_date": [date, date],
            "ts_code": ["001914.SZ", "000043.SZ"],
            "amount": [123.0, 123.0],
            "pct_chg": [1.5, 1.5],
        }
    )

    normalized = normalize_security_aliases(identical, date, aliases, role="daily")

    assert len(normalized) == 1
    assert normalized.loc[0, "ticker"] == "001914.SZ"
    assert normalized.loc[0, "amount"] == pytest.approx(123.0)

    conflict = identical.copy()
    conflict.loc[1, "amount"] = 124.0
    with pytest.raises(OpportunitySetDataError, match="inconsistent content"):
        normalize_security_aliases(conflict, date, aliases, role="daily")


def test_alias_effective_to_is_inclusive_and_next_day_is_not_aliased() -> None:
    aliases = [
        {
            "canonical_ts_code": "001914.SZ",
            "vendor_ts_code": "000043.SZ",
            "effective_from": "1990-01-01",
            "effective_to": "2019-12-15",
        }
    ]
    at_boundary = pd.DataFrame(
        {"date": ["2019-12-15"], "ticker": ["000043.SZ"], "amount": [1.0]}
    )
    next_day = pd.DataFrame(
        {"date": ["2019-12-16"], "ticker": ["000043.SZ"], "amount": [1.0]}
    )

    assert normalize_security_aliases(
        at_boundary, "2019-12-15", aliases
    ).loc[0, "ticker"] == "001914.SZ"
    assert normalize_security_aliases(next_day, "2019-12-16", aliases).loc[
        0, "ticker"
    ] == "000043.SZ"


def test_listing_session_119_is_excluded_and_120_is_included() -> None:
    calendar = _calendar()
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )

    result_119 = None
    for date in calendar[99:119]:
        result_119 = _push_normal_day(builder, date, [ticker])
    assert result_119 is not None and result_119.history_ready
    assert result_119.base_eligible_count == 0

    result_120 = _push_normal_day(builder, calendar[119], [ticker])

    assert result_120.base_eligible_count == 1
    assert result_120.universe(UNIVERSE_TOP500).tickers == (ticker,)


def test_cached_listing_indices_match_exact_full_calendar_search() -> None:
    calendar = pd.bdate_range("2016-07-07", periods=2063)
    securities = pd.DataFrame(
        {
            "ts_code": ["A.SZ", "B.SZ", "C.SZ", "D.SZ"],
                "list_date": [
                    "1991-01-01",
                    calendar[500].date().isoformat(),
                    (calendar[500] + pd.Timedelta(days=1)).date().isoformat(),
                    (calendar[-1] + pd.Timedelta(days=10)).date().isoformat(),
            ],
            "delist_date": [pd.NaT] * 4,
        }
    )

    builder = DailyOpportunitySetBuilder(calendar, securities)
    expected = np.searchsorted(
        np.asarray(calendar, dtype="datetime64[ns]"),
        pd.to_datetime(securities["list_date"]).to_numpy(dtype="datetime64[ns]"),
        side="left",
    )

    assert builder._securities["_listing_first_calendar_index"].tolist() == (
        expected.tolist()
    )


def test_future_delist_does_not_affect_t_and_delist_day_is_excluded() -> None:
    calendar = _calendar()
    tickers = ["000001.SZ", "000002.SZ"]
    builder = DailyOpportunitySetBuilder(
        calendar,
        _securities(
            tickers,
            calendar,
            delist_dates={"000001.SZ": calendar[120]},
        ),
    )
    result_before = None
    for date in calendar[100:120]:
        result_before = _push_normal_day(builder, date, tickers)

    assert result_before is not None
    assert set(result_before.universe(UNIVERSE_TOP500).tickers) == set(tickers)

    result_on_delist = _push_normal_day(builder, calendar[120], ["000002.SZ"])

    assert result_on_delist.base_eligible_count == 1
    assert result_on_delist.universe(UNIVERSE_TOP500).tickers == ("000002.SZ",)


def test_stock_st_is_effective_on_t_and_t_plus_one_cannot_change_prior_result() -> None:
    calendar = _calendar()
    tickers = ["000001.SZ", "000002.SZ"]
    builder = DailyOpportunitySetBuilder(
        calendar, _securities(tickers, calendar)
    )
    result_t = None
    for date in calendar[100:120]:
        result_t = _push_normal_day(builder, date, tickers)
    assert result_t is not None
    prior_tickers = result_t.universe(UNIVERSE_TOP500).tickers
    assert set(prior_tickers) == set(tickers)

    st_t_plus_one = pd.DataFrame(
        {
            "trade_date": [calendar[120]],
            "ts_code": ["000001.SZ"],
            "is_st": [True],
        }
    )
    result_t_plus_one = _push_normal_day(
        builder, calendar[120], tickers, stock_st=st_t_plus_one
    )

    assert result_t.universe(UNIVERSE_TOP500).tickers == prior_tickers
    assert result_t_plus_one.universe(UNIVERSE_TOP500).tickers == ("000002.SZ",)


def test_missing_or_unknown_daily_stock_st_partition_fails_without_state_commit() -> None:
    calendar = _calendar(3)
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    daily = _daily(calendar[0], [ticker])
    basic = _daily_basic(calendar[0], [ticker])

    with pytest.raises(OpportunitySetDataError, match="missing/unknown"):
        builder.push_day(
            calendar[0], daily=daily, daily_basic=basic, stock_st=None
        )
    assert builder.buffered_sessions == 0

    unknown = pd.DataFrame(
        {
            "trade_date": [calendar[0]],
            "ts_code": [ticker],
            "is_st": [pd.NA],
        }
    )
    with pytest.raises(OpportunitySetDataError, match="unknown status"):
        builder.push_day(
            calendar[0], daily=daily, daily_basic=basic, stock_st=unknown
        )
    assert builder.buffered_sessions == 0


def test_adv20_uses_exactly_twenty_official_sessions_and_amount_times_1000() -> None:
    calendar = _calendar()
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    result = None
    pct_changes = np.arange(1.0, 21.0)
    for offset, date in enumerate(calendar[100:120], start=1):
        result = _push_normal_day(
            builder,
            date,
            [ticker],
            amounts=float(offset),
            pct_chg=float(offset),
        )

    assert result is not None and result.history_ready
    ranked = result.universe(UNIVERSE_TOP500).top25[0]
    assert ranked.adv20_rmb == pytest.approx(sum(range(1, 21)) * 1000.0 / 20.0)
    assert ranked.volatility_20 == pytest.approx(
        np.std(pct_changes / 100.0, ddof=1)
    )


def test_missing_daily_bar_requires_suspension_and_contributes_zero() -> None:
    calendar = _calendar()
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    for date in calendar[100:119]:
        _push_normal_day(builder, date, [ticker], amounts=100.0, pct_chg=1.0)

    empty_daily = pd.DataFrame(
        columns=["trade_date", "ts_code", "amount", "pct_chg"]
    )
    basic = _daily_basic(calendar[119], [ticker])
    with pytest.raises(OpportunitySetDataError, match="lack a daily bar"):
        builder.push_day(
            calendar[119],
            daily=empty_daily,
            daily_basic=basic,
            stock_st=_known_empty_st(),
        )
    assert builder.buffered_sessions == 19

    suspension = pd.DataFrame(
        {
            "date": [calendar[119]],
            "ticker": [ticker],
            "is_suspended": [True],
        }
    )
    result = builder.push_day(
        calendar[119],
        daily=empty_daily,
        daily_basic=basic,
        stock_st=_known_empty_st(),
        suspensions=suspension,
    )

    row = result.universe(UNIVERSE_TOP500).top25[0]
    assert row.adv20_rmb == pytest.approx(19 * 100.0 * 1000.0 / 20.0)
    assert row.volatility_20 == pytest.approx(
        np.std([0.01] * 19 + [0.0], ddof=1)
    )


def test_nonpositive_adv_is_excluded_from_every_candidate_arm() -> None:
    calendar = _calendar()
    tickers = ("LIVE", "ZERO_ADV")
    builder = DailyOpportunitySetBuilder(
        calendar, _securities(tickers, calendar)
    )

    result = None
    for date in calendar[100:120]:
        result = _push_normal_day(
            builder,
            date,
            tickers,
            amounts=(100_000.0, 0.0),
        )

    assert result is not None
    assert result.base_eligible_count == 1
    for universe in result.universes:
        assert universe.tickers == ("LIVE",)


def test_amount_conversion_overflow_fails_before_adv_construction() -> None:
    calendar = _calendar()
    ticker = "OVERFLOW"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )

    with pytest.raises(OpportunitySetDataError, match="amount overflows"):
        _push_normal_day(
            builder,
            calendar[100],
            [ticker],
            amounts=1e308,
        )


def test_same_day_suspend_and_resume_are_valid_missing_bar_proof() -> None:
    calendar = _calendar()
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )

    result = builder.push_day(
        calendar[0],
        daily=pd.DataFrame(
            columns=["trade_date", "ts_code", "amount", "pct_chg"]
        ),
        daily_basic=_daily_basic(calendar[0], [ticker]),
        stock_st=_known_empty_st(),
        suspensions=pd.DataFrame(
            {
                "ticker": [ticker, ticker],
                "date": [calendar[0], calendar[0]],
                "suspend_type": ["S", "R"],
                "suspend_timing": [pd.NA, pd.NA],
            }
        ),
    )

    assert result.observed_window_sessions == 1
    assert result.base_eligible_count == 0

    with pytest.raises(OpportunitySetDataError, match="lack a daily bar"):
        builder.push_day(
            calendar[1],
            daily=pd.DataFrame(
                columns=["trade_date", "ts_code", "amount", "pct_chg"]
            ),
            daily_basic=_daily_basic(calendar[1], [ticker]),
            stock_st=_known_empty_st(),
        )


def test_full_day_suspend_event_is_carried_until_a_bar_proves_resume() -> None:
    calendar = _calendar(4)
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    empty_daily = _daily(calendar[0], [ticker]).iloc[0:0]
    suspended = pd.DataFrame(
        {
            "ticker": [ticker],
            "date": [calendar[0]],
            "suspend_type": ["S"],
            "suspend_timing": [pd.NA],
        }
    )

    builder.push_day(
        calendar[0],
        daily=empty_daily,
        daily_basic=_daily_basic(calendar[0], [ticker]),
        stock_st=_known_empty_st(),
        suspensions=suspended,
    )
    carried = builder.push_day(
        calendar[1],
        daily=_daily(calendar[1], [ticker]).iloc[0:0],
        daily_basic=_daily_basic(calendar[1], [ticker]),
        stock_st=_known_empty_st(),
    )
    assert carried.observed_window_sessions == 2
    assert [row.ticker for row in carried.carried_suspension_evidence] == [ticker]
    evidence = carried.carried_suspension_evidence[0]
    assert evidence.source_suspend_date == calendar[0].date().isoformat()
    assert evidence.official_session_age == 2

    builder.push_day(
        calendar[2],
        daily=_daily(calendar[2], [ticker]),
        daily_basic=_daily_basic(calendar[2], [ticker]),
        stock_st=_known_empty_st(),
    )
    with pytest.raises(OpportunitySetDataError, match="lack a daily bar"):
        builder.push_day(
            calendar[3],
            daily=_daily(calendar[3], [ticker]).iloc[0:0],
            daily_basic=_daily_basic(calendar[3], [ticker]),
            stock_st=_known_empty_st(),
        )


def test_intraday_s_is_not_carried_and_r_clears_prior_proof() -> None:
    calendar = _calendar(2)
    ticker = "000001.SZ"
    empty = _daily(calendar[0], [ticker]).iloc[0:0]

    intraday_builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    intraday = pd.DataFrame(
        {
            "ticker": [ticker],
            "date": [calendar[0]],
            "suspend_type": ["S"],
            "suspend_timing": ["09:31-10:31"],
        }
    )
    with pytest.raises(OpportunitySetDataError, match="lack a daily bar"):
        intraday_builder.push_day(
            calendar[0],
            daily=empty,
            daily_basic=_daily_basic(calendar[0], [ticker]),
            stock_st=_known_empty_st(),
            suspensions=intraday,
        )
    assert intraday_builder.buffered_sessions == 0

    observed_intraday_builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    observed_intraday_builder.push_day(
        calendar[0],
        daily=_daily(calendar[0], [ticker]),
        daily_basic=_daily_basic(calendar[0], [ticker]),
        stock_st=_known_empty_st(),
        suspensions=intraday,
    )
    with pytest.raises(OpportunitySetDataError, match="lack a daily bar"):
        observed_intraday_builder.push_day(
            calendar[1],
            daily=_daily(calendar[1], [ticker]).iloc[0:0],
            daily_basic=_daily_basic(calendar[1], [ticker]),
            stock_st=_known_empty_st(),
        )

    resume_builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    resume_builder.push_day(
        calendar[0],
        daily=empty,
        daily_basic=_daily_basic(calendar[0], [ticker]),
        stock_st=_known_empty_st(),
        suspensions=pd.DataFrame(
            {
                "ticker": [ticker],
                "date": [calendar[0]],
                "suspend_type": ["S"],
                "suspend_timing": [pd.NA],
            }
        ),
    )
    with pytest.raises(OpportunitySetDataError, match="lack a daily bar"):
        resume_builder.push_day(
            calendar[1],
            daily=_daily(calendar[1], [ticker]).iloc[0:0],
            daily_basic=_daily_basic(calendar[1], [ticker]),
            stock_st=_known_empty_st(),
            suspensions=pd.DataFrame(
                {
                    "ticker": [ticker],
                    "date": [calendar[1]],
                    "suspend_type": ["R"],
                    "suspend_timing": [pd.NA],
                }
            ),
        )
    retried = resume_builder.push_day(
        calendar[1],
        daily=_daily(calendar[1], [ticker]).iloc[0:0],
        daily_basic=_daily_basic(calendar[1], [ticker]),
        stock_st=_known_empty_st(),
    )
    assert retried.carried_suspension_evidence[0].ticker == ticker


def test_stock_st_ignores_known_inactive_identity_but_rejects_unknown() -> None:
    calendar = _calendar(2)
    active = "000001.SZ"
    future = "000002.SZ"
    securities = pd.DataFrame(
        {
            "ts_code": [active, future],
            "list_date": [calendar[0], calendar[1]],
            "delist_date": [pd.NaT, pd.NaT],
        }
    )
    builder = DailyOpportunitySetBuilder(calendar, securities)
    inactive_st = pd.DataFrame(
        {
            "trade_date": [calendar[0]],
            "ts_code": [future],
            "is_st": [True],
        }
    )

    first = builder.push_day(
        calendar[0],
        daily=_daily(calendar[0], [active]),
        daily_basic=_daily_basic(calendar[0], [active]),
        stock_st=inactive_st,
    )
    assert first.observed_window_sessions == 1
    assert first.inactive_stock_st_ignored_count == 1

    unknown_st = pd.DataFrame(
        {
            "trade_date": [calendar[1]],
            "ts_code": ["999999.SZ"],
            "is_st": [True],
        }
    )
    with pytest.raises(OpportunitySetDataError, match="unknown securities"):
        builder.push_day(
            calendar[1],
            daily=_daily(calendar[1], [active, future]),
            daily_basic=_daily_basic(calendar[1], [active, future]),
            stock_st=unknown_st,
        )


def test_stock_st_alias_positive_list_unions_different_display_names() -> None:
    calendar = _calendar()
    canonical = "001914.SZ"
    vendor = "000043.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar,
        _securities([canonical], calendar),
        aliases=[
            {
                "canonical_ts_code": canonical,
                "vendor_ts_code": vendor,
                "effective_from": "1900-01-01",
                "effective_to": "2024-12-31",
                "source": "verified exchange notice",
            }
        ],
    )
    for date in calendar[:119]:
        _push_normal_day(builder, date, [vendor])
    signal_date = calendar[119]
    stock_st = pd.DataFrame(
        {
            "ts_code": [vendor, canonical],
            "trade_date": [signal_date, signal_date],
            "name": ["*ST OLD", "ST NEW"],
            "type": ["ST", "ST"],
            "type_name": ["特别处理", "其他风险警示"],
        }
    )

    result = builder.push_day(
        signal_date,
        daily=_daily(signal_date, [vendor]),
        daily_basic=_daily_basic(signal_date, [vendor]),
        stock_st=stock_st,
    )

    assert result.base_eligible_count == 0


def test_missing_stock_st_bar_is_excluded_without_inventing_suspension() -> None:
    calendar = _calendar()
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    for date in calendar[:119]:
        _push_normal_day(builder, date, [ticker])
    signal_date = calendar[119]
    stock_st = pd.DataFrame(
        {
            "ts_code": [ticker],
            "trade_date": [signal_date],
            "name": ["*ST TEST"],
            "type": ["ST"],
            "type_name": ["risk warning"],
        }
    )

    result = builder.push_day(
        signal_date,
        daily=_daily(signal_date, [ticker]).iloc[0:0],
        daily_basic=_daily_basic(signal_date, [ticker]),
        stock_st=stock_st,
    )

    assert result.base_eligible_count == 0
    assert result.observed_window_sessions == 20


def test_builder_materializes_only_the_stage_candidate_subset() -> None:
    calendar = _calendar()
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar,
        _securities([ticker], calendar),
        universe_names=(UNIVERSE_TOP500, UNIVERSE_TOP1500),
    )

    result = _push_normal_day(builder, calendar[0], [ticker])

    assert tuple(universe.name for universe in result.universes) == (
        UNIVERSE_TOP500,
        UNIVERSE_TOP1500,
    )


def test_universe_ranking_classifies_source_fundamental_missingness() -> None:
    calendar = _calendar()
    tickers = (
        "GOOD",
        "PE_SOURCE_NULL",
        "SUSPENDED_NO_SNAPSHOT",
        "BAR_NO_SNAPSHOT",
        "PB_NULL",
        "INVALID_TEXT",
        "INVALID_ZERO",
    )
    builder = DailyOpportunitySetBuilder(
        calendar, _securities(tickers, calendar)
    )
    for date in calendar[100:119]:
        _push_normal_day(builder, date, tickers, amounts=200_000.0)

    signal_date = calendar[119]
    actual_bar_tickers = tuple(
        ticker for ticker in tickers if ticker != "SUSPENDED_NO_SNAPSHOT"
    )
    daily_basic = pd.DataFrame(
        {
            "trade_date": [signal_date] * 5,
            "ts_code": [
                "GOOD",
                "PE_SOURCE_NULL",
                "PB_NULL",
                "INVALID_TEXT",
                "INVALID_ZERO",
            ],
            "pe_ttm": [10.0, np.nan, 10.0, "not-a-number", 10.0],
            "pb": [1.0, 1.0, np.nan, 1.0, 0.0],
        }
    )
    result = builder.push_day(
        signal_date,
        daily=_daily(signal_date, actual_bar_tickers, amounts=200_000.0),
        daily_basic=daily_basic,
        stock_st=_known_empty_st(),
        suspensions=pd.DataFrame(
            {
                "date": [signal_date],
                "ticker": ["SUSPENDED_NO_SNAPSHOT"],
                "is_suspended": [True],
            }
        ),
    )

    diagnostic_fields = (
        "daily_basic_row_absent_with_daily_bar_count",
        "daily_basic_row_absent_with_proven_no_daily_bar_count",
        "pe_ttm_null_count",
        "pb_null_count",
        "invalid_non_null_fundamental_count",
        "expected_finite_score_count",
        "unexpected_score_mismatch_count",
        "arithmetic_nonfinite_count",
        "classified_unscoreable_count",
        "unclassified_unscoreable_count",
    )
    expected = {
        "daily_basic_row_absent_with_daily_bar_count": 1,
        "daily_basic_row_absent_with_proven_no_daily_bar_count": 1,
        "pe_ttm_null_count": 1,
        "pb_null_count": 1,
        "invalid_non_null_fundamental_count": 2,
        "expected_finite_score_count": 1,
        "unexpected_score_mismatch_count": 0,
        "arithmetic_nonfinite_count": 0,
        "classified_unscoreable_count": 6,
        "unclassified_unscoreable_count": 0,
    }
    assert result.base_eligible_count == len(tickers)
    for universe in result.universes:
        assert universe.member_count == len(tickers)
        assert universe.finite_score_count == 1
        assert universe.tickers == ("GOOD",)
        assert {
            field: getattr(universe, field) for field in diagnostic_fields
        } == expected
        frame = universe.to_frame()
        assert set(diagnostic_fields).issubset(frame.columns)
        assert frame.loc[0, list(diagnostic_fields)].to_dict() == expected
        json.dumps(
            frame.loc[:, diagnostic_fields].to_dict(orient="records"),
            allow_nan=False,
        )


def test_invalid_infinity_is_counted_and_score_mismatch_is_observable() -> None:
    calendar = _calendar()
    ticker = "INFINITE_PE"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    for date in calendar[100:119]:
        _push_normal_day(builder, date, [ticker], amounts=200_000.0)

    result = _push_normal_day(
        builder,
        calendar[119],
        [ticker],
        amounts=200_000.0,
        pe_ttm=np.inf,
    )

    for universe in result.universes:
        # Preserve the frozen score path: it currently produces a finite score
        # for infinite PE, while the independent input classifier rejects it.
        assert universe.finite_score_count == 1
        assert universe.invalid_non_null_fundamental_count == 1
        assert universe.expected_finite_score_count == 0
        assert universe.unexpected_score_mismatch_count == 1
        assert universe.arithmetic_nonfinite_count == 0
        assert universe.classified_unscoreable_count == 1
        assert universe.unclassified_unscoreable_count == 0
        assert universe.tickers == (ticker,)


def test_finite_nonzero_inputs_with_nonfinite_arithmetic_are_classified() -> None:
    calendar = _calendar()
    ticker = "ARITHMETIC_OVERFLOW"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    for date in calendar[100:119]:
        _push_normal_day(builder, date, [ticker], amounts=200_000.0)

    signal_date = calendar[119]
    result = builder.push_day(
        signal_date,
        daily=_daily(signal_date, [ticker], amounts=200_000.0),
        daily_basic=pd.DataFrame(
            {
                "trade_date": [signal_date],
                "ts_code": [ticker],
                "pe_ttm": [1e-308],
                "pb": [1e-308],
            }
        ),
        stock_st=_known_empty_st(),
        suspensions=None,
    )

    for universe in result.universes:
        assert universe.invalid_non_null_fundamental_count == 0
        assert universe.arithmetic_nonfinite_count == 1
        assert universe.expected_finite_score_count == 0
        assert universe.classified_unscoreable_count == 1
        assert universe.unclassified_unscoreable_count == 0


def test_universe_boundaries_ties_threshold_and_independent_fixed_core_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calendar = _calendar()
    tickers = [f"T{index:04d}" for index in range(1502)]
    amounts = [100_000.0] * 1501 + [99_999.999]
    pe = [10.0] * len(tickers)
    pb = [1.0] * len(tickers)
    # T0500 is just outside Top500 by the frozen ticker tie-break.  T1500 is
    # just outside Top1500 but is exactly on the >= RMB100m threshold.
    pe[500], pb[500] = 1.0, 0.10
    pe[1500], pb[1500] = 0.5, 0.05

    calls: list[int] = []
    real_fixed_core_score = opportunity_module.fixed_core_score

    def recording_score(frame: pd.DataFrame):
        calls.append(len(frame))
        return real_fixed_core_score(frame)

    monkeypatch.setattr(opportunity_module, "fixed_core_score", recording_score)
    builder = DailyOpportunitySetBuilder(
        calendar, _securities(tickers, calendar)
    )
    result = None
    for date in calendar[100:120]:
        result = _push_normal_day(
            builder,
            date,
            tickers,
            amounts=amounts,
            pe_ttm=pe,
            pb=pb,
        )

    assert result is not None
    assert calls[-3:] == [500, 1501, 1500]
    top500 = result.universe(UNIVERSE_TOP500)
    threshold = result.universe(UNIVERSE_ADV_GE_100M)
    top1500 = result.universe(UNIVERSE_TOP1500)
    assert (top500.member_count, threshold.member_count, top1500.member_count) == (
        500,
        1501,
        1500,
    )
    assert "T0500" not in top500.tickers
    assert top1500.tickers[0] == "T0500"
    assert threshold.tickers[0] == "T1500"
    assert "T1500" not in top1500.tickers
    assert all(len(universe.top25) <= 25 for universe in result.universes)


def test_forbidden_columns_and_official_calendar_gaps_fail_closed() -> None:
    calendar = _calendar(4)
    ticker = "000001.SZ"
    builder = DailyOpportunitySetBuilder(
        calendar, _securities([ticker], calendar)
    )
    forbidden = _daily(calendar[0], [ticker])
    forbidden["forward_return_5d"] = 0.5

    with pytest.raises(OpportunitySetDataError, match="forbidden"):
        builder.push_day(
            calendar[0],
            daily=forbidden,
            daily_basic=_daily_basic(calendar[0], [ticker]),
            stock_st=_known_empty_st(),
        )
    assert builder.buffered_sessions == 0

    _push_normal_day(builder, calendar[0], [ticker])
    with pytest.raises(OpportunitySetDataError, match="official calendar gap"):
        _push_normal_day(builder, calendar[2], [ticker])
    assert builder.buffered_sessions == 1


@pytest.mark.parametrize(
    "calendar",
    [
        ["2024-01-02", "2024-01-02"],
        ["2024-01-03", "2024-01-02"],
    ],
)
def test_official_calendar_itself_must_be_unique_and_increasing(calendar) -> None:
    securities = pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "list_date": ["2020-01-01"],
            "delist_date": [pd.NaT],
        }
    )
    with pytest.raises(OpportunitySetDataError, match="unique and strictly increasing"):
        DailyOpportunitySetBuilder(calendar, securities)
