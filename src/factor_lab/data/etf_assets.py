"""Strict point-in-time inputs for the fixed 7.0 multi-asset ETF route.

The module deliberately does not use vendor adjustment factors to manufacture
returns.  It reconstructs a cash-dividend total-return index from raw closes
and fails closed when the next ``pre_close`` cannot be explained by the prior
raw close and the dividend effective on that session.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
from typing import Any, Mapping, Protocol
import uuid

import numpy as np
import pandas as pd


STAGE_CONTRACT_ID = "factor-lab/multi-asset-etf-stage/1"
PRE_CLOSE_ABS_TOLERANCE = 0.0011
CALENDAR_LOOKAHEAD_DAYS = 31

FUND_DAILY_FIELDS = (
    "ts_code",
    "trade_date",
    "pre_close",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)
FUND_DIV_FIELDS = (
    "ts_code",
    "ann_date",
    "imp_anndate",
    "div_proc",
    "record_date",
    "ex_date",
    "pay_date",
    "div_cash",
)
FUND_ADJ_FIELDS = ("ts_code", "trade_date", "adj_factor")
TRADE_CAL_FIELDS = ("exchange", "cal_date", "is_open", "pretrade_date")
FINAL_DIVIDEND_PROCESSES = frozenset({"实施", "已实施"})
UNIT_ADJUSTMENT_RATIO_REL_TOLERANCE = 1e-3
REFERENCE_PRICE_RESET_RELATIVE_LIMIT = 0.02
OFFICIAL_UNIT_EVENTS: Mapping[str, Mapping[str, Any]] = {
    "513100.SH": {
        "split_date": "2022-01-13",
        "first_resumed_trade_date": "2022-01-14",
        "unit_multiplier": 5.0,
        "source_url": (
            "https://www.sse.com.cn/disclosure/fund/announcement/c/new/"
            "2022-01-04/513100_20220104_1_0U96WMU8.pdf"
        ),
        "source_sha256": (
            "7dd4e55669fcd2ec0645a61e2dba648e000a2947e6bd160b43ea6cf3898abef1"
        ),
        "source_size_bytes": 303_782,
    }
}
REFERENCE_PRICE_RESET_DATES = frozenset(
    {("513100.SH", pd.Timestamp("2021-07-26"))}
)


def _visible_unit_event(
    ticker: str, cutoff: pd.Timestamp
) -> Mapping[str, Any] | None:
    event = OFFICIAL_UNIT_EVENTS.get(ticker)
    if event is None:
        return None
    return (
        event
        if pd.Timestamp(str(event["split_date"])).normalize()
        <= pd.Timestamp(cutoff).normalize()
        else None
    )

DAILY_COLUMNS = (
    "ticker",
    "trade_date",
    "pre_close",
    "open",
    "high",
    "low",
    "close",
    "volume_shares",
    "amount_rmb",
)
DIVIDEND_COLUMNS = (
    "ticker",
    "ann_date",
    "imp_anndate",
    "div_proc",
    "record_date",
    "ex_date",
    "pay_date",
    "div_cash",
)
ADJUSTMENT_COLUMNS = ("ticker", "trade_date", "adj_factor")
CALENDAR_COLUMNS = ("trade_date", "previous_open_date")
HISTORY_COLUMNS = (
    *DAILY_COLUMNS,
    "dividend_cash",
    "dividend_pay_date",
    "adj_factor_diagnostic",
    "unit_multiplier",
    "reference_price_reset",
    "total_return_index",
    "adv20_rmb",
)


@dataclass(frozen=True, slots=True)
class ETFAsset:
    ticker: str
    asset: str
    target_weight: float


ETF_ASSETS = (
    ETFAsset("510300.SH", "mainland_china_large_equity", 0.30),
    ETFAsset("159920.SZ", "hong_kong_equity", 0.10),
    ETFAsset("513100.SH", "united_states_equity", 0.10),
    ETFAsset("518880.SH", "gold", 0.20),
    ETFAsset("511010.SH", "five_year_government_bond", 0.30),
    ETFAsset("511880.SH", "exchange_traded_money_market_cash_proxy", 0.0),
)
ETF_TICKERS = tuple(asset.ticker for asset in ETF_ASSETS)
ETF_TARGET_WEIGHTS = {asset.ticker: asset.target_weight for asset in ETF_ASSETS}
_ASSET_BY_TICKER = {asset.ticker: asset for asset in ETF_ASSETS}


class FundDataClient(Protocol):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame: ...


@dataclass(frozen=True)
class MultiAssetStage:
    path: Path
    manifest: Mapping[str, Any]
    calendar: pd.DataFrame
    assets: Mapping[str, pd.DataFrame]


def _empty(columns: tuple[str, ...], date_columns: tuple[str, ...]) -> pd.DataFrame:
    frame = pd.DataFrame(columns=list(columns))
    for column in date_columns:
        frame[column] = pd.Series(dtype="datetime64[ns]")
    return frame


def _require_frame(value: Any, *, role: str) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise TypeError(f"{role} must be a pandas DataFrame")
    return value.copy()


def _require_exact_columns(
    frame: pd.DataFrame, expected: tuple[str, ...], *, role: str
) -> None:
    actual = set(map(str, frame.columns))
    required = set(expected)
    if actual != required:
        missing = sorted(required - actual)
        extra = sorted(actual - required)
        raise ValueError(f"{role} columns mismatch; missing={missing}, extra={extra}")


def _vendor_dates(values: pd.Series, *, field: str) -> pd.Series:
    text = values.astype("string")
    if text.isna().any() or not text.str.fullmatch(r"\d{8}").all():
        raise ValueError(f"{field} must contain exact YYYYMMDD dates")
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    if parsed.isna().any():
        raise ValueError(f"{field} contains invalid dates")
    return parsed.dt.normalize()


def _optional_vendor_dates(values: pd.Series, *, field: str) -> pd.Series:
    text = values.astype("string").str.strip()
    missing = text.isna() | text.eq("") | text.str.casefold().isin({"none", "nan", "nat"})
    present = text.loc[~missing]
    if not present.str.fullmatch(r"\d{8}").all():
        raise ValueError(f"{field} must be empty or contain exact YYYYMMDD dates")
    output = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    if not present.empty:
        parsed = pd.to_datetime(present, format="%Y%m%d", errors="coerce")
        if parsed.isna().any():
            raise ValueError(f"{field} contains invalid dates")
        output.loc[present.index] = parsed.dt.normalize()
    return output


def _finite_numeric(values: pd.Series, *, field: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype(float)
    if numeric.isna().any() or not np.isfinite(numeric.to_numpy(dtype=float)).all():
        raise ValueError(f"{field} must contain finite numeric values")
    return numeric


def _validate_tickers(
    values: pd.Series, *, role: str, expected_ticker: str | None
) -> pd.Series:
    tickers = values.astype("string")
    if tickers.isna().any():
        raise ValueError(f"{role} contains missing ts_code")
    unknown = sorted(set(tickers.astype(str)) - set(ETF_TICKERS))
    if unknown:
        raise ValueError(f"{role} contains unsupported ETF codes: {unknown}")
    if expected_ticker is not None:
        if expected_ticker not in _ASSET_BY_TICKER:
            raise ValueError(f"unsupported expected ETF code: {expected_ticker}")
        observed = set(tickers.astype(str))
        if observed and observed != {expected_ticker}:
            raise ValueError(
                f"{role} expected only {expected_ticker}, observed {sorted(observed)}"
            )
    return tickers.astype(str)


def normalize_fund_daily(
    rows: pd.DataFrame, *, expected_ticker: str | None = None
) -> pd.DataFrame:
    """Normalize Tushare ``fund_daily`` rows into raw-price RMB units.

    Tushare fund volume is expressed in 100-share lots and ``amount`` in
    thousands of RMB.  Prices are deliberately kept raw and unadjusted.
    """

    work = _require_frame(rows, role="fund_daily")
    if work.empty and not len(work.columns):
        return _empty(DAILY_COLUMNS, ("trade_date",))
    _require_exact_columns(work, FUND_DAILY_FIELDS, role="fund_daily")
    if work.empty:
        return _empty(DAILY_COLUMNS, ("trade_date",))

    output = pd.DataFrame()
    output["ticker"] = _validate_tickers(
        work["ts_code"], role="fund_daily", expected_ticker=expected_ticker
    )
    output["trade_date"] = _vendor_dates(work["trade_date"], field="trade_date")
    for column in ("pre_close", "open", "high", "low", "close"):
        output[column] = _finite_numeric(work[column], field=column)
        if not output[column].gt(0.0).all():
            raise ValueError(f"fund_daily {column} must be strictly positive")
    volume = _finite_numeric(work["vol"], field="vol")
    amount = _finite_numeric(work["amount"], field="amount")
    if not volume.ge(0.0).all() or not amount.ge(0.0).all():
        raise ValueError("fund_daily vol and amount must be non-negative")
    output["volume_shares"] = volume * 100.0
    output["amount_rmb"] = amount * 1_000.0
    if not np.isfinite(
        output[["volume_shares", "amount_rmb"]].to_numpy(dtype=float)
    ).all():
        raise ValueError("fund_daily unit conversion produced a non-finite value")

    if (
        output.duplicated(["ticker", "trade_date"]).any()
        or not output["high"].ge(output[["open", "close", "low"]].max(axis=1)).all()
        or not output["low"].le(output[["open", "close", "high"]].min(axis=1)).all()
    ):
        raise ValueError("fund_daily contains duplicate dates or invalid OHLC bounds")
    return output.sort_values(["ticker", "trade_date"], kind="mergesort").reset_index(
        drop=True
    )[list(DAILY_COLUMNS)]


def normalize_trade_calendar(
    rows: pd.DataFrame,
    *,
    start: str,
    end: str,
    exchange: str = "SSE",
) -> pd.DataFrame:
    """Validate a complete official calendar and return its open sessions."""

    work = _require_frame(rows, role="trade_cal")
    _require_exact_columns(work, TRADE_CAL_FIELDS, role="trade_cal")
    start_date, _ = _boundary(start, field="calendar start")
    end_date, _ = _boundary(end, field="calendar end")
    if start_date > end_date or work.empty:
        raise ValueError("trade_cal range is invalid or empty")
    work["cal_date"] = _vendor_dates(work["cal_date"], field="cal_date")
    if work["cal_date"].duplicated().any():
        raise ValueError("trade_cal contains duplicate calendar dates")
    expected = pd.date_range(start_date, end_date, freq="D")
    observed = pd.DatetimeIndex(work["cal_date"].sort_values())
    if not observed.equals(expected):
        raise ValueError("trade_cal must cover every requested calendar date")
    exchanges = work["exchange"].astype("string").str.strip()
    if exchanges.isna().any() or not exchanges.eq(exchange).all():
        raise ValueError("trade_cal contains an unexpected exchange")
    numeric_open = pd.to_numeric(work["is_open"], errors="coerce")
    text_open = work["is_open"].astype("string").str.strip().str.casefold()
    valid_open = numeric_open.isin([0, 1]) | text_open.isin({"false", "true"})
    if not valid_open.all():
        raise ValueError("trade_cal contains invalid is_open values")
    work["is_open"] = numeric_open.eq(1) | text_open.eq("true")
    work["pretrade_date"] = _optional_vendor_dates(
        work["pretrade_date"], field="pretrade_date"
    )
    opened = work.loc[work["is_open"], ["cal_date", "pretrade_date"]].copy()
    if opened.empty:
        raise ValueError("trade_cal contains no official open sessions")
    opened = opened.sort_values("cal_date", kind="mergesort").reset_index(drop=True)
    if (
        not pd.isna(opened.loc[0, "pretrade_date"])
        and opened.loc[0, "pretrade_date"] >= opened.loc[0, "cal_date"]
    ):
        raise ValueError("trade_cal first pretrade_date must precede its open session")
    if len(opened) > 1:
        prior = opened["cal_date"].shift(1)
        mismatch = opened.index.to_series().gt(0) & opened["pretrade_date"].ne(prior)
        if mismatch.any():
            raise ValueError("trade_cal pretrade_date does not link open sessions")
    return opened.rename(
        columns={"cal_date": "trade_date", "pretrade_date": "previous_open_date"}
    )[list(CALENDAR_COLUMNS)]


def normalize_fund_div(
    rows: pd.DataFrame, *, expected_ticker: str | None = None
) -> pd.DataFrame:
    """Normalize final executable cash distributions and their payment dates."""

    work = _require_frame(rows, role="fund_div")
    if work.empty and not len(work.columns):
        return _empty(
            DIVIDEND_COLUMNS,
            ("ann_date", "imp_anndate", "record_date", "ex_date", "pay_date"),
        )
    _require_exact_columns(work, FUND_DIV_FIELDS, role="fund_div")
    if work.empty:
        return _empty(
            DIVIDEND_COLUMNS,
            ("ann_date", "imp_anndate", "record_date", "ex_date", "pay_date"),
        )

    processes = work["div_proc"].astype("string").str.strip()
    work = work.loc[processes.isin(FINAL_DIVIDEND_PROCESSES)].copy()
    if work.empty:
        return _empty(
            DIVIDEND_COLUMNS,
            ("ann_date", "imp_anndate", "record_date", "ex_date", "pay_date"),
        )
    normalized = pd.DataFrame(index=work.index)
    normalized["ticker"] = _validate_tickers(
        work["ts_code"], role="fund_div", expected_ticker=expected_ticker
    )
    normalized["ann_date"] = _vendor_dates(work["ann_date"], field="ann_date")
    normalized["imp_anndate"] = _optional_vendor_dates(
        work["imp_anndate"], field="imp_anndate"
    )
    normalized["div_proc"] = work["div_proc"].astype("string").str.strip().astype(str)
    normalized["record_date"] = _vendor_dates(
        work["record_date"], field="record_date"
    )
    normalized["ex_date"] = _vendor_dates(work["ex_date"], field="ex_date")
    normalized["pay_date"] = _vendor_dates(work["pay_date"], field="pay_date")
    normalized["div_cash"] = _finite_numeric(work["div_cash"], field="div_cash")
    if not normalized["div_cash"].gt(0.0).all():
        raise ValueError("fund_div div_cash must be strictly positive")
    chronology = (
        normalized["ann_date"].le(normalized["record_date"])
        & normalized["record_date"].le(normalized["ex_date"])
        & normalized["ex_date"].le(normalized["pay_date"])
    )
    implemented = normalized["imp_anndate"].isna() | (
        normalized["ann_date"].le(normalized["imp_anndate"])
        & normalized["imp_anndate"].le(normalized["record_date"])
    )
    if not chronology.all() or not implemented.all():
        raise ValueError(
            "fund_div dates must satisfy ann<=record<=ex<=pay and optional "
            "ann<=imp_anndate<=record"
        )

    conflict_fields = ("record_date", "ex_date", "pay_date", "div_cash")
    conflict_counts = normalized.groupby(["ticker", "ex_date"], sort=False)[
        list(conflict_fields)
    ].nunique(dropna=False)
    if conflict_counts.gt(1).any(axis=None):
        raise ValueError("fund_div contains conflicting events on one ex_date")
    collapsed_rows: list[dict[str, Any]] = []
    for (ticker, ex_date), group in normalized.groupby(
        ["ticker", "ex_date"], sort=True
    ):
        implementation_dates = group["imp_anndate"].dropna()
        collapsed_rows.append(
            {
                "ticker": ticker,
                "ann_date": group["ann_date"].min(),
                "imp_anndate": (
                    implementation_dates.min()
                    if not implementation_dates.empty
                    else pd.NaT
                ),
                "div_proc": str(group["div_proc"].iloc[0]),
                "record_date": group["record_date"].iloc[0],
                "ex_date": ex_date,
                "pay_date": group["pay_date"].iloc[0],
                "div_cash": float(group["div_cash"].iloc[0]),
            }
        )
    return pd.DataFrame(collapsed_rows, columns=list(DIVIDEND_COLUMNS)).sort_values(
        ["ticker", "ex_date"], kind="mergesort"
    ).reset_index(drop=True)


def normalize_fund_adj(
    rows: pd.DataFrame, *, expected_ticker: str | None = None
) -> pd.DataFrame:
    """Normalize vendor fund adjustment factors for diagnostics only."""

    work = _require_frame(rows, role="fund_adj")
    if work.empty and not len(work.columns):
        return _empty(ADJUSTMENT_COLUMNS, ("trade_date",))
    _require_exact_columns(work, FUND_ADJ_FIELDS, role="fund_adj")
    if work.empty:
        return _empty(ADJUSTMENT_COLUMNS, ("trade_date",))
    output = pd.DataFrame()
    output["ticker"] = _validate_tickers(
        work["ts_code"], role="fund_adj", expected_ticker=expected_ticker
    )
    output["trade_date"] = _vendor_dates(work["trade_date"], field="trade_date")
    output["adj_factor"] = _finite_numeric(work["adj_factor"], field="adj_factor")
    if not output["adj_factor"].gt(0.0).all():
        raise ValueError("fund_adj adj_factor must be strictly positive")
    conflicts = output.groupby(["ticker", "trade_date"], sort=False)[
        "adj_factor"
    ].nunique(dropna=False)
    if conflicts.gt(1).any():
        raise ValueError("fund_adj contains conflicting factors on one date")
    return (
        output.drop_duplicates(["ticker", "trade_date", "adj_factor"])
        .sort_values(["ticker", "trade_date"], kind="mergesort")
        .drop_duplicates(["ticker", "trade_date"], keep="first")
        .reset_index(drop=True)[list(ADJUSTMENT_COLUMNS)]
    )


def _require_normalized_daily(daily: pd.DataFrame) -> pd.DataFrame:
    work = _require_frame(daily, role="normalized fund_daily")
    _require_exact_columns(work, DAILY_COLUMNS, role="normalized fund_daily")
    if work.empty:
        raise ValueError("normalized fund_daily must not be empty")
    if not pd.api.types.is_datetime64_any_dtype(work["trade_date"]):
        raise ValueError("normalized fund_daily trade_date must be datetime64")
    if work["trade_date"].isna().any() or work.duplicated(["ticker", "trade_date"]).any():
        raise ValueError("normalized fund_daily contains invalid or duplicate dates")
    if len(set(work["ticker"].astype(str))) != 1:
        raise ValueError("total-return history must contain exactly one ETF")
    ticker = str(work["ticker"].iloc[0])
    _validate_tickers(work["ticker"], role="normalized fund_daily", expected_ticker=ticker)
    for column in ("pre_close", "open", "high", "low", "close"):
        values = _finite_numeric(work[column], field=column)
        if not values.gt(0.0).all():
            raise ValueError(f"normalized fund_daily {column} must be positive")
        work[column] = values
    for column in ("volume_shares", "amount_rmb"):
        values = _finite_numeric(work[column], field=column)
        if not values.ge(0.0).all():
            raise ValueError(f"normalized fund_daily {column} must be non-negative")
        work[column] = values
    return work.sort_values("trade_date", kind="mergesort").reset_index(drop=True)


def build_total_return_history(
    daily: pd.DataFrame,
    dividends: pd.DataFrame | None = None,
    adjustments: pd.DataFrame | None = None,
    *,
    pre_close_abs_tolerance: float = PRE_CLOSE_ABS_TOLERANCE,
) -> pd.DataFrame:
    """Build a raw-price cash-dividend total-return series for one ETF.

    The adjustment factor is diagnostic: it can validate the one registered
    unit event or known reference-price reset, but never supplies a price or
    silently repairs an otherwise unexplained corporate action.
    """

    if float(pre_close_abs_tolerance) != PRE_CLOSE_ABS_TOLERANCE:
        raise ValueError(
            f"pre_close_abs_tolerance is frozen at {PRE_CLOSE_ABS_TOLERANCE}"
        )
    work = _require_normalized_daily(daily)
    ticker = str(work["ticker"].iloc[0])
    if dividends is None:
        divs = _empty(
            DIVIDEND_COLUMNS,
            ("ann_date", "imp_anndate", "record_date", "ex_date", "pay_date"),
        )
    else:
        divs = _require_frame(dividends, role="normalized fund_div")
        _require_exact_columns(divs, DIVIDEND_COLUMNS, role="normalized fund_div")
        if not divs.empty:
            date_columns = (
                "ann_date",
                "imp_anndate",
                "record_date",
                "ex_date",
                "pay_date",
            )
            if any(
                not pd.api.types.is_datetime64_any_dtype(divs[column])
                for column in date_columns
            ):
                raise ValueError("normalized fund_div dates must be datetime64")
            _validate_tickers(divs["ticker"], role="normalized fund_div", expected_ticker=ticker)
            divs["div_cash"] = _finite_numeric(divs["div_cash"], field="div_cash")
            chronology = (
                divs["ann_date"].le(divs["record_date"])
                & divs["record_date"].le(divs["ex_date"])
                & divs["ex_date"].le(divs["pay_date"])
            )
            implemented = divs["imp_anndate"].isna() | (
                divs["ann_date"].le(divs["imp_anndate"])
                & divs["imp_anndate"].le(divs["record_date"])
            )
            if (
                divs.duplicated(["ticker", "ex_date"]).any()
                or not divs["div_cash"].gt(0.0).all()
                or not divs["div_proc"].isin(FINAL_DIVIDEND_PROCESSES).all()
                or not chronology.all()
                or not implemented.all()
            ):
                raise ValueError("normalized fund_div violates its event contract")

    date_set = set(pd.DatetimeIndex(work["trade_date"]))
    inside = divs.loc[
        divs["ex_date"].between(work["trade_date"].min(), work["trade_date"].max())
    ].copy()
    missing_ex_dates = sorted(set(pd.DatetimeIndex(inside["ex_date"])) - date_set)
    if missing_ex_dates:
        raise ValueError("fund_div ex_date is absent from fund_daily history")
    dividend_by_date = (
        inside.set_index("ex_date")["div_cash"].astype(float).to_dict()
        if not inside.empty
        else {}
    )
    pay_date_by_ex_date = (
        inside.set_index("ex_date")["pay_date"].to_dict()
        if not inside.empty
        else {}
    )
    work["dividend_cash"] = work["trade_date"].map(dividend_by_date).fillna(0.0).astype(float)
    work["dividend_pay_date"] = pd.to_datetime(
        work["trade_date"].map(pay_date_by_ex_date), errors="coerce"
    )

    if adjustments is None:
        adjusted = _empty(ADJUSTMENT_COLUMNS, ("trade_date",))
    else:
        adjusted = _require_frame(adjustments, role="normalized fund_adj")
        _require_exact_columns(
            adjusted, ADJUSTMENT_COLUMNS, role="normalized fund_adj"
        )
        if not adjusted.empty:
            if not pd.api.types.is_datetime64_any_dtype(adjusted["trade_date"]):
                raise ValueError("normalized fund_adj trade_date must be datetime64")
            _validate_tickers(
                adjusted["ticker"],
                role="normalized fund_adj",
                expected_ticker=ticker,
            )
            adjusted["adj_factor"] = _finite_numeric(
                adjusted["adj_factor"], field="adj_factor"
            )
            if (
                adjusted.duplicated(["ticker", "trade_date"]).any()
                or not adjusted["adj_factor"].gt(0.0).all()
            ):
                raise ValueError("normalized fund_adj violates its diagnostic contract")
    adjustment_by_date = (
        adjusted.set_index("trade_date")["adj_factor"].astype(float).to_dict()
        if not adjusted.empty
        else {}
    )
    work["adj_factor_diagnostic"] = pd.to_numeric(
        work["trade_date"].map(adjustment_by_date), errors="coerce"
    )

    closes = work["close"].to_numpy(dtype=float)
    pre_closes = work["pre_close"].to_numpy(dtype=float)
    cash = work["dividend_cash"].to_numpy(dtype=float)
    diagnostic_factors = work["adj_factor_diagnostic"].to_numpy(dtype=float)
    total_return = np.ones(len(work), dtype=float)
    unit_multipliers = np.ones(len(work), dtype=float)
    reference_resets = np.zeros(len(work), dtype=bool)
    unit_event = OFFICIAL_UNIT_EVENTS.get(ticker)
    unit_event_date = (
        pd.Timestamp(unit_event["first_resumed_trade_date"])
        if unit_event is not None
        else None
    )
    for index in range(1, len(work)):
        trade_date = pd.Timestamp(work.loc[index, "trade_date"])
        expected_pre_close = closes[index - 1] - cash[index]
        if unit_event_date is not None and trade_date == unit_event_date:
            multiplier = float(unit_event["unit_multiplier"])
            if cash[index] != 0.0:
                raise ValueError("registered unit event cannot share a cash distribution")
            prior_adj = diagnostic_factors[index - 1]
            current_adj = diagnostic_factors[index]
            adj_ratio = current_adj / prior_adj if prior_adj > 0.0 else float("nan")
            expected_pre_close = closes[index - 1] / multiplier
            if (
                not np.isfinite(adj_ratio)
                or not math.isclose(
                    adj_ratio,
                    multiplier,
                    rel_tol=UNIT_ADJUSTMENT_RATIO_REL_TOLERANCE,
                    abs_tol=0.0,
                )
                or not math.isclose(
                    pre_closes[index],
                    expected_pre_close,
                    rel_tol=0.0,
                    abs_tol=PRE_CLOSE_ABS_TOLERANCE,
                )
            ):
                raise ValueError("registered 513100 unit event does not reconcile")
            unit_multipliers[index] = multiplier
            factor = closes[index] * multiplier / closes[index - 1]
        elif (ticker, trade_date) in REFERENCE_PRICE_RESET_DATES:
            prior_adj = diagnostic_factors[index - 1]
            current_adj = diagnostic_factors[index]
            adj_ratio = current_adj / prior_adj if prior_adj > 0.0 else float("nan")
            relative_reset = (
                abs(pre_closes[index] - expected_pre_close) / expected_pre_close
                if expected_pre_close > 0.0
                else float("inf")
            )
            if (
                cash[index] != 0.0
                or not np.isfinite(adj_ratio)
                or not math.isclose(adj_ratio, 1.0, rel_tol=0.0, abs_tol=1e-12)
                or relative_reset >= REFERENCE_PRICE_RESET_RELATIVE_LIMIT
            ):
                raise ValueError("registered 513100 reference-price reset does not reconcile")
            reference_resets[index] = True
            factor = closes[index] / closes[index - 1]
        else:
            if expected_pre_close <= 0.0 or not math.isclose(
                pre_closes[index],
                expected_pre_close,
                rel_tol=0.0,
                abs_tol=PRE_CLOSE_ABS_TOLERANCE,
            ):
                date = trade_date.date().isoformat()
                raise ValueError(
                    "unexplained fund corporate action at "
                    f"{ticker} {date}: pre_close={pre_closes[index]!r}, "
                    f"expected={expected_pre_close!r}"
                )
            factor = (closes[index] + cash[index]) / closes[index - 1]
        if not np.isfinite(factor) or factor <= 0.0:
            raise ValueError("fund total-return factor must be finite and positive")
        total_return[index] = total_return[index - 1] * factor
        if not np.isfinite(total_return[index]) or total_return[index] <= 0.0:
            raise ValueError("fund total_return_index overflowed or became non-positive")
    work["unit_multiplier"] = unit_multipliers
    work["reference_price_reset"] = reference_resets
    work["total_return_index"] = total_return
    work["adv20_rmb"] = work["amount_rmb"].rolling(20, min_periods=20).mean()
    finite_adv = work["adv20_rmb"].dropna().to_numpy(dtype=float)
    if not np.isfinite(finite_adv).all():
        raise ValueError("fund adv20_rmb aggregation produced a non-finite value")
    return work[list(HISTORY_COLUMNS)].reset_index(drop=True)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _payload_sha256(value: Mapping[str, Any]) -> str:
    payload = dict(value)
    payload.pop("payload_sha256", None)
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _frame_content_sha256(frame: pd.DataFrame) -> str:
    rows: list[list[Any]] = []
    for values in frame.itertuples(index=False, name=None):
        row: list[Any] = []
        for value in values:
            if value is None or bool(pd.isna(value)):
                row.append(None)
            elif isinstance(value, pd.Timestamp):
                row.append(value.date().isoformat())
            elif isinstance(value, (np.integer,)):
                row.append(int(value))
            elif isinstance(value, (np.floating,)):
                row.append(float(value))
            else:
                row.append(value)
        rows.append(row)
    return hashlib.sha256(
        _canonical_bytes({"columns": list(frame.columns), "rows": rows})
    ).hexdigest()


def _call(client: Any, endpoint: str, **kwargs: Any) -> pd.DataFrame:
    direct = getattr(client, endpoint, None)
    if callable(direct):
        value = direct(**kwargs)
    else:
        query = getattr(client, "query", None)
        if not callable(query):
            raise TypeError(f"fund data client has no {endpoint!r} endpoint or query")
        value = query(endpoint, **kwargs)
    if value is None:
        return pd.DataFrame()
    if not isinstance(value, pd.DataFrame):
        raise TypeError(f"fund data endpoint {endpoint!r} did not return a DataFrame")
    return value.copy()


def _boundary(value: str, *, field: str) -> tuple[pd.Timestamp, str]:
    text = str(value).strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}|\d{8}", text):
        raise ValueError(f"{field} must be YYYY-MM-DD or YYYYMMDD")
    parsed = pd.to_datetime(text, format="%Y-%m-%d" if "-" in text else "%Y%m%d", errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"{field} is not a valid date")
    timestamp = pd.Timestamp(parsed).normalize()
    return timestamp, timestamp.strftime("%Y%m%d")


def _validate_stage_name(stage: str) -> str:
    value = str(stage).strip()
    if not value or value in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", value):
        raise ValueError("stage must be a safe non-empty identifier")
    return value


def _natural_year_windows(
    start: pd.Timestamp, end: pd.Timestamp
) -> tuple[tuple[pd.Timestamp, pd.Timestamp], ...]:
    return tuple(
        (
            max(start, pd.Timestamp(year=year, month=1, day=1)),
            min(end, pd.Timestamp(year=year, month=12, day=31)),
        )
        for year in range(start.year, end.year + 1)
    )


def _suspicious_ex_dates(
    frames: Mapping[str, pd.DataFrame],
) -> tuple[str, ...]:
    dates: set[str] = set()
    for frame in frames.values():
        prior_close = pd.to_numeric(frame["close"], errors="coerce").shift(1)
        observed_pre = pd.to_numeric(frame["pre_close"], errors="coerce")
        suspicious = prior_close.notna() & observed_pre.sub(prior_close).abs().gt(
            PRE_CLOSE_ABS_TOLERANCE
        )
        dates.update(
            pd.DatetimeIndex(frame.loc[suspicious, "trade_date"])
            .strftime("%Y-%m-%d")
            .tolist()
        )
    return tuple(sorted(dates))


def _audit_calendar(
    frame: pd.DataFrame,
    *,
    start_date: pd.Timestamp,
    price_end_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> None:
    _require_exact_columns(frame, CALENDAR_COLUMNS, role="official open calendar")
    if frame.empty or any(
        not pd.api.types.is_datetime64_any_dtype(frame[column])
        for column in CALENDAR_COLUMNS
    ):
        raise ValueError("official open calendar is empty or has invalid dtypes")
    if (
        frame["trade_date"].isna().any()
        or frame["trade_date"].duplicated().any()
        or not frame["trade_date"].is_monotonic_increasing
        or frame["trade_date"].min() < start_date
        or frame["trade_date"].max() > end_date
    ):
        raise ValueError("official open calendar dates violate the stage cutoff")
    if frame["trade_date"].max() <= price_end_date:
        raise ValueError("official calendar does not contain a post-cutoff session")
    if len(frame) > 1 and not frame["previous_open_date"].iloc[1:].reset_index(
        drop=True
    ).equals(frame["trade_date"].iloc[:-1].reset_index(drop=True)):
        raise ValueError("official open calendar previous-session links are invalid")


def _audit_history(
    frame: pd.DataFrame,
    ticker: str,
    *,
    price_start_date: pd.Timestamp,
    price_end_date: pd.Timestamp,
    expected_price_sessions: pd.DatetimeIndex,
) -> None:
    _require_exact_columns(frame, HISTORY_COLUMNS, role=f"{ticker} history")
    if frame.empty or set(frame["ticker"].astype(str)) != {ticker}:
        raise ValueError(f"{ticker} history is empty or has the wrong ticker")
    if not pd.api.types.is_datetime64_any_dtype(frame["trade_date"]):
        raise ValueError(f"{ticker} history trade_date must be datetime64")
    if frame["trade_date"].isna().any() or frame["trade_date"].duplicated().any() or not frame["trade_date"].is_monotonic_increasing:
        raise ValueError(f"{ticker} history dates must be unique and increasing")
    if (
        frame["trade_date"].min() < price_start_date
        or frame["trade_date"].max() > price_end_date
    ):
        raise ValueError(f"{ticker} history exceeds the physical price cutoff")
    dates = pd.DatetimeIndex(frame["trade_date"])
    if (
        expected_price_sessions.empty
        or dates.min() != expected_price_sessions.min()
        or dates.max() != expected_price_sessions.max()
        or not dates.isin(expected_price_sessions).all()
    ):
        raise ValueError(f"{ticker} history does not cover the full price boundary")
    for column in ("pre_close", "open", "high", "low", "close", "total_return_index"):
        values = _finite_numeric(frame[column], field=column)
        if not values.gt(0.0).all():
            raise ValueError(f"{ticker} history {column} must be positive")
    for column in ("volume_shares", "amount_rmb", "dividend_cash"):
        values = _finite_numeric(frame[column], field=column)
        if not values.ge(0.0).all():
            raise ValueError(f"{ticker} history {column} must be non-negative")
    if not pd.api.types.is_datetime64_any_dtype(frame["dividend_pay_date"]):
        raise ValueError(f"{ticker} dividend_pay_date must be datetime64")
    has_dividend = frame["dividend_cash"].gt(0.0)
    has_pay_date = frame["dividend_pay_date"].notna()
    if (
        not has_dividend.eq(has_pay_date).all()
        or not frame.loc[has_dividend, "dividend_pay_date"]
        .ge(frame.loc[has_dividend, "trade_date"])
        .all()
    ):
        raise ValueError(f"{ticker} dividend payment dates do not reconcile")

    diagnostic = pd.to_numeric(frame["adj_factor_diagnostic"], errors="coerce")
    if (
        (frame["adj_factor_diagnostic"].notna() & diagnostic.isna()).any()
        or not diagnostic.dropna().gt(0.0).all()
    ):
        raise ValueError(f"{ticker} diagnostic adjustment factors must be positive")
    unit_multiplier = pd.to_numeric(frame["unit_multiplier"], errors="coerce")
    if unit_multiplier.isna().any() or not unit_multiplier.gt(0.0).all():
        raise ValueError(f"{ticker} unit multipliers must be positive")
    if not pd.api.types.is_bool_dtype(frame["reference_price_reset"]):
        raise ValueError(f"{ticker} reference_price_reset must be boolean")

    reconstructed_dividends = pd.DataFrame(
        {
            "ticker": ticker,
            "ann_date": frame.loc[has_dividend, "trade_date"].to_numpy(),
            "imp_anndate": pd.NaT,
            "div_proc": "实施",
            "record_date": frame.loc[has_dividend, "trade_date"].to_numpy(),
            "ex_date": frame.loc[has_dividend, "trade_date"].to_numpy(),
            "pay_date": frame.loc[has_dividend, "dividend_pay_date"].to_numpy(),
            "div_cash": frame.loc[has_dividend, "dividend_cash"].to_numpy(),
        },
        columns=list(DIVIDEND_COLUMNS),
    )
    for column in ("ann_date", "imp_anndate", "record_date", "ex_date", "pay_date"):
        reconstructed_dividends[column] = pd.to_datetime(
            reconstructed_dividends[column], errors="coerce"
        )
    reconstructed_adjustments = frame.loc[
        diagnostic.notna(), ["ticker", "trade_date", "adj_factor_diagnostic"]
    ].rename(columns={"adj_factor_diagnostic": "adj_factor"})
    rebuilt = build_total_return_history(
        frame.loc[:, list(DAILY_COLUMNS)],
        reconstructed_dividends,
        reconstructed_adjustments,
    )
    for column, absolute_tolerance in (
        ("adj_factor_diagnostic", 0.0),
        ("unit_multiplier", 0.0),
        ("total_return_index", 1e-12),
        ("adv20_rmb", 1e-9),
    ):
        if not np.allclose(
            pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float),
            pd.to_numeric(rebuilt[column], errors="coerce").to_numpy(dtype=float),
            equal_nan=True,
            rtol=0.0,
            atol=absolute_tolerance,
        ):
            raise ValueError(f"{ticker} {column} does not reconcile")
    if not frame["reference_price_reset"].reset_index(drop=True).equals(
        rebuilt["reference_price_reset"].reset_index(drop=True)
    ):
        raise ValueError(f"{ticker} reference_price_reset does not reconcile")


def _load_stage_directory(directory: Path, *, expected_stage: str) -> MultiAssetStage:
    if directory.is_symlink() or not directory.is_dir():
        raise FileNotFoundError(f"multi-asset stage is missing or symlinked: {directory}")
    manifest_path = directory / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise FileNotFoundError("multi-asset stage manifest is missing or symlinked")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError("multi-asset stage manifest is unreadable") from exc
    if not isinstance(manifest, Mapping):
        raise ValueError("multi-asset stage manifest must be an object")
    if manifest.get("payload_sha256") != _payload_sha256(manifest):
        raise ValueError("multi-asset stage manifest payload hash mismatch")
    if (
        manifest.get("schema_version") != 1
        or manifest.get("contract_id") != STAGE_CONTRACT_ID
        or manifest.get("stage") != expected_stage
    ):
        raise ValueError("multi-asset stage manifest identity mismatch")
    assets = manifest.get("assets")
    if not isinstance(assets, Mapping) or set(assets) != set(ETF_TICKERS):
        raise ValueError("multi-asset stage manifest asset set mismatch")
    price_start_date, _ = _boundary(
        str(manifest.get("price_start_date") or ""), field="manifest price start"
    )
    price_end_date, _ = _boundary(
        str(manifest.get("price_end_date") or ""), field="manifest price end"
    )
    calendar_end_date, _ = _boundary(
        str(manifest.get("calendar_end_date") or ""), field="manifest calendar end"
    )
    if (
        price_start_date > price_end_date
        or calendar_end_date < price_end_date
        or calendar_end_date
        > price_end_date + pd.Timedelta(days=CALENDAR_LOOKAHEAD_DAYS)
    ):
        raise ValueError("multi-asset stage manifest date range is invalid")
    dividend_query = manifest.get("fund_div_ex_date_query")
    recorded_ex_dates = (
        dividend_query.get("queried_ex_dates")
        if isinstance(dividend_query, Mapping)
        else None
    )
    if (
        not isinstance(dividend_query, Mapping)
        or set(dividend_query)
        != {"bounded_by", "queried_ex_dates", "query_count", "unbounded_query_count"}
        or dividend_query.get("bounded_by") != "ex_date"
        or not isinstance(recorded_ex_dates, list)
        or recorded_ex_dates != sorted(set(map(str, recorded_ex_dates)))
        or dividend_query.get("query_count") != len(recorded_ex_dates)
        or dividend_query.get("unbounded_query_count") != 0
    ):
        raise ValueError("multi-asset stage dividend query boundary mismatch")
    for value in recorded_ex_dates:
        observed_date, _ = _boundary(str(value), field="queried ex date")
        if not price_start_date <= observed_date <= price_end_date:
            raise ValueError("multi-asset stage queried ex date exceeds price cutoff")
    expected_files = {
        "manifest.json",
        "calendar.parquet",
        *(f"{ticker}.parquet" for ticker in ETF_TICKERS),
    }
    observed_files = {path.name for path in directory.iterdir()}
    if observed_files != expected_files:
        raise ValueError("multi-asset stage contains an unexpected file set")

    calendar_entry = manifest.get("calendar")
    if (
        not isinstance(calendar_entry, Mapping)
        or calendar_entry.get("path") != "calendar.parquet"
        or calendar_entry.get("exchange") != "SSE"
        or calendar_entry.get("price_start_date")
        != price_start_date.date().isoformat()
        or calendar_entry.get("calendar_end_date")
        != calendar_end_date.date().isoformat()
    ):
        raise ValueError("multi-asset stage calendar manifest is invalid")
    calendar_path = directory / "calendar.parquet"
    if calendar_path.is_symlink() or not calendar_path.is_file():
        raise FileNotFoundError("official calendar file is missing or symlinked")
    if (
        calendar_path.stat().st_size != calendar_entry.get("size_bytes")
        or _file_sha256(calendar_path) != calendar_entry.get("file_sha256")
    ):
        raise ValueError("official calendar file hash or size mismatch")
    calendar = pd.read_parquet(calendar_path)
    if len(calendar) != calendar_entry.get("row_count"):
        raise ValueError("official calendar row count mismatch")
    _audit_calendar(
        calendar,
        start_date=price_start_date,
        price_end_date=price_end_date,
        end_date=calendar_end_date,
    )
    if calendar_entry.get("content_sha256") != _frame_content_sha256(calendar):
        raise ValueError("official calendar content hash mismatch")

    expected_price_sessions = pd.DatetimeIndex(
        calendar.loc[
            calendar["trade_date"].le(price_end_date), "trade_date"
        ]
    )
    if (
        expected_price_sessions.empty
        or expected_price_sessions.max() != price_end_date
    ):
        raise ValueError("official calendar does not end the price stage on its cutoff")

    loaded: dict[str, pd.DataFrame] = {}
    for definition in ETF_ASSETS:
        entry = assets.get(definition.ticker)
        if not isinstance(entry, Mapping):
            raise ValueError(f"manifest entry missing for {definition.ticker}")
        expected_path = f"{definition.ticker}.parquet"
        if (
            entry.get("path") != expected_path
            or entry.get("asset") != definition.asset
            or entry.get("target_weight") != definition.target_weight
            or entry.get("official_unit_event")
            != _visible_unit_event(definition.ticker, price_end_date)
        ):
            raise ValueError(f"manifest contract mismatch for {definition.ticker}")
        path = directory / expected_path
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(f"asset file missing or symlinked: {expected_path}")
        if path.stat().st_size != entry.get("size_bytes") or _file_sha256(path) != entry.get("file_sha256"):
            raise ValueError(f"asset file hash or size mismatch: {definition.ticker}")
        frame = pd.read_parquet(path)
        if len(frame) != entry.get("row_count"):
            raise ValueError(f"asset row count mismatch: {definition.ticker}")
        _audit_history(
            frame,
            definition.ticker,
            price_start_date=price_start_date,
            price_end_date=price_end_date,
            expected_price_sessions=expected_price_sessions,
        )
        loaded[definition.ticker] = frame
    if list(_suspicious_ex_dates(loaded)) != recorded_ex_dates:
        raise ValueError("multi-asset stage dividend ex-date query set mismatch")
    return MultiAssetStage(
        path=directory.resolve(),
        manifest=manifest,
        calendar=calendar,
        assets=loaded,
    )


def capture_multi_asset_stage(
    client: FundDataClient,
    root: str | Path,
    start: str,
    end: str,
    stage: str,
) -> MultiAssetStage:
    """Capture all fixed ETFs into one create-only, self-hashed stage."""

    stage_name = _validate_stage_name(stage)
    start_date, start_compact = _boundary(start, field="start")
    end_date, end_compact = _boundary(end, field="end")
    if start_date > end_date:
        raise ValueError("start must be no later than end")
    calendar_end_date = end_date + pd.Timedelta(days=CALENDAR_LOOKAHEAD_DAYS)
    calendar_end_compact = calendar_end_date.strftime("%Y%m%d")
    root_path = Path(root).expanduser().resolve()
    root_path.mkdir(parents=True, exist_ok=True)
    destination = root_path / f"stage={stage_name}"
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"multi-asset stage already exists: {destination}")
    staging = root_path / f".stage={stage_name}.partial-{uuid.uuid4().hex}"
    staging.mkdir()

    try:
        calendar = normalize_trade_calendar(
            _call(
                client,
                "trade_cal",
                exchange="SSE",
                start_date=start_compact,
                end_date=calendar_end_compact,
                fields=",".join(TRADE_CAL_FIELDS),
            ),
            start=start_compact,
            end=calendar_end_compact,
        )
        calendar_path = staging / "calendar.parquet"
        calendar.to_parquet(calendar_path, index=False, compression="zstd")
        calendar_manifest = {
            "exchange": "SSE",
            "price_start_date": start_date.date().isoformat(),
            "calendar_end_date": calendar_end_date.date().isoformat(),
            "path": calendar_path.name,
            "row_count": len(calendar),
            "size_bytes": calendar_path.stat().st_size,
            "file_sha256": _file_sha256(calendar_path),
            "content_sha256": _frame_content_sha256(calendar),
        }

        daily_by_ticker: dict[str, pd.DataFrame] = {}
        adjustments_by_ticker: dict[str, pd.DataFrame] = {}
        adjustment_query_counts: dict[str, int] = {}
        for definition in ETF_ASSETS:
            ticker = definition.ticker
            daily = normalize_fund_daily(
                _call(
                    client,
                    "fund_daily",
                    ts_code=ticker,
                    start_date=start_compact,
                    end_date=end_compact,
                    fields=",".join(FUND_DAILY_FIELDS),
                ),
                expected_ticker=ticker,
            )
            if daily.empty or daily["trade_date"].min() < start_date or daily["trade_date"].max() > end_date:
                raise ValueError(f"fund_daily returned no rows or rows outside cutoff for {ticker}")
            adjustment_parts: list[pd.DataFrame] = []
            adjustment_windows = _natural_year_windows(start_date, end_date)
            for window_start, window_end in adjustment_windows:
                part = normalize_fund_adj(
                    _call(
                        client,
                        "fund_adj",
                        ts_code=ticker,
                        start_date=window_start.strftime("%Y%m%d"),
                        end_date=window_end.strftime("%Y%m%d"),
                        fields=",".join(FUND_ADJ_FIELDS),
                    ),
                    expected_ticker=ticker,
                )
                if not part.empty and (
                    part["trade_date"].min() < window_start
                    or part["trade_date"].max() > window_end
                ):
                    raise ValueError(
                        f"fund_adj returned rows outside its natural-year window for {ticker}"
                    )
                adjustment_parts.append(part)
            nonempty_adjustment_parts = [
                part for part in adjustment_parts if not part.empty
            ]
            adjustments = (
                pd.concat(nonempty_adjustment_parts, ignore_index=True)
                if nonempty_adjustment_parts
                else _empty(ADJUSTMENT_COLUMNS, ("trade_date",))
            )
            if not adjustments.empty:
                conflicts = adjustments.groupby(["ticker", "trade_date"], sort=False)[
                    "adj_factor"
                ].nunique(dropna=False)
                if conflicts.gt(1).any():
                    raise ValueError(
                        f"fund_adj natural-year chunks conflict for {ticker}"
                    )
                adjustments = (
                    adjustments.drop_duplicates(
                        ["ticker", "trade_date", "adj_factor"]
                    )
                    .sort_values(["ticker", "trade_date"], kind="mergesort")
                    .drop_duplicates(["ticker", "trade_date"], keep="first")
                    .reset_index(drop=True)
                )
            daily_by_ticker[ticker] = daily
            adjustments_by_ticker[ticker] = adjustments
            adjustment_query_counts[ticker] = len(adjustment_windows)

        queried_ex_dates = _suspicious_ex_dates(daily_by_ticker)
        dividend_source_parts: list[pd.DataFrame] = []
        for ex_date_text in queried_ex_dates:
            ex_date = pd.Timestamp(ex_date_text)
            raw_dividends = _call(
                client,
                "fund_div",
                ex_date=ex_date.strftime("%Y%m%d"),
                fields=",".join(FUND_DIV_FIELDS),
            )
            if raw_dividends.empty and not len(raw_dividends.columns):
                continue
            _require_exact_columns(
                raw_dividends, FUND_DIV_FIELDS, role="bounded fund_div"
            )
            if raw_dividends.empty:
                continue
            observed_ex_dates = _vendor_dates(
                raw_dividends["ex_date"], field="bounded fund_div ex_date"
            )
            if not observed_ex_dates.eq(ex_date).all():
                raise ValueError(
                    "fund_div returned rows outside the requested ex date"
                )
            selected = raw_dividends.loc[
                raw_dividends["ts_code"].astype("string").isin(ETF_TICKERS)
            ].copy()
            if not selected.empty:
                dividend_source_parts.append(selected)
        all_dividends = normalize_fund_div(
            (
                pd.concat(dividend_source_parts, ignore_index=True)
                if dividend_source_parts
                else pd.DataFrame(columns=list(FUND_DIV_FIELDS))
            )
        )
        if not all_dividends.empty:
            all_dividends = all_dividends.loc[
                (
                    all_dividends["imp_anndate"].isna()
                    | all_dividends["imp_anndate"].le(end_date)
                )
                & all_dividends["ex_date"].between(start_date, end_date)
            ].reset_index(drop=True)

        manifest_assets: dict[str, Any] = {}
        for definition in ETF_ASSETS:
            ticker = definition.ticker
            daily = daily_by_ticker[ticker]
            adjustments = adjustments_by_ticker[ticker]
            dividends = all_dividends.loc[
                all_dividends["ticker"].eq(ticker)
            ].reset_index(drop=True)

            history = build_total_return_history(daily, dividends, adjustments)
            path = staging / f"{ticker}.parquet"
            history.to_parquet(path, index=False, compression="zstd")
            manifest_assets[ticker] = {
                "asset": definition.asset,
                "target_weight": definition.target_weight,
                "path": path.name,
                "row_count": len(history),
                "size_bytes": path.stat().st_size,
                "file_sha256": _file_sha256(path),
                "source_content_sha256": {
                    "fund_daily": _frame_content_sha256(daily),
                    "fund_div": _frame_content_sha256(dividends),
                    "fund_adj_diagnostic_only": _frame_content_sha256(adjustments),
                },
                "fund_adj_diagnostic_row_count": len(adjustments),
                "fund_adj_diagnostic_query_count": adjustment_query_counts[ticker],
                "official_unit_event": _visible_unit_event(ticker, end_date),
            }

        manifest: dict[str, Any] = {
            "schema_version": 1,
            "contract_id": STAGE_CONTRACT_ID,
            "stage": stage_name,
            "price_start_date": start_date.date().isoformat(),
            "price_end_date": end_date.date().isoformat(),
            "calendar_end_date": calendar_end_date.date().isoformat(),
            "calendar": calendar_manifest,
            "fund_div_ex_date_query": {
                "bounded_by": "ex_date",
                "queried_ex_dates": list(queried_ex_dates),
                "query_count": len(queried_ex_dates),
                "unbounded_query_count": 0,
            },
            "assets": manifest_assets,
        }
        manifest["payload_sha256"] = _payload_sha256(manifest)
        (staging / "manifest.json").write_bytes(
            json.dumps(
                manifest,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            ).encode("utf-8")
            + b"\n"
        )
        _load_stage_directory(staging, expected_stage=stage_name)
        os.rename(staging, destination)
        return _load_stage_directory(destination, expected_stage=stage_name)
    except BaseException:
        if staging.exists():
            shutil.rmtree(staging)
        raise


def load_multi_asset_stage(
    root: str | Path, stage: str
) -> MultiAssetStage:
    """Verify and load a previously captured fixed multi-asset stage."""

    stage_name = _validate_stage_name(stage)
    directory = Path(root).expanduser().resolve() / f"stage={stage_name}"
    return _load_stage_directory(directory, expected_stage=stage_name)


__all__ = [
    "ADJUSTMENT_COLUMNS",
    "CALENDAR_COLUMNS",
    "CALENDAR_LOOKAHEAD_DAYS",
    "DAILY_COLUMNS",
    "DIVIDEND_COLUMNS",
    "ETF_ASSETS",
    "ETF_TARGET_WEIGHTS",
    "ETF_TICKERS",
    "FUND_ADJ_FIELDS",
    "FUND_DAILY_FIELDS",
    "FUND_DIV_FIELDS",
    "TRADE_CAL_FIELDS",
    "HISTORY_COLUMNS",
    "MultiAssetStage",
    "OFFICIAL_UNIT_EVENTS",
    "PRE_CLOSE_ABS_TOLERANCE",
    "REFERENCE_PRICE_RESET_DATES",
    "build_total_return_history",
    "capture_multi_asset_stage",
    "load_multi_asset_stage",
    "normalize_fund_adj",
    "normalize_fund_daily",
    "normalize_fund_div",
    "normalize_trade_calendar",
]
