"""Streaming sparse execution-pricing construction for the 6.1 experiment."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from numbers import Number
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from .opportunity_set import (
    ADV_WINDOW_SESSIONS,
    OpportunitySetDataError,
    SecurityAliasInterval,
    normalize_security_aliases,
)


CALENDAR_SENTINEL = "__CALENDAR__"


def _timestamp(value: Any, *, field: str) -> pd.Timestamp:
    text = str(value).strip()
    if not text:
        raise OpportunitySetDataError(f"{field} must be known")
    parsed = pd.to_datetime(
        text,
        format="%Y%m%d" if len(text) == 8 and text.isdigit() else None,
        errors="coerce",
    )
    if pd.isna(parsed):
        raise OpportunitySetDataError(f"invalid {field}: {value!r}")
    result = pd.Timestamp(parsed)
    if result.tzinfo is not None:
        result = result.tz_convert(None)
    return result.normalize()


def _date_column(values: pd.Series, *, field: str) -> pd.Series:
    output = []
    for value in values:
        if value is None or value is pd.NaT or bool(pd.isna(value)):
            output.append(pd.NaT)
        else:
            output.append(_timestamp(value, field=field))
    return pd.Series(output, index=values.index, dtype="datetime64[ns]")


def _alias_values(
    values: Iterable[SecurityAliasInterval | Mapping[str, Any]],
) -> tuple[SecurityAliasInterval, ...]:
    aliases = tuple(SecurityAliasInterval.from_value(value) for value in values)
    by_vendor: dict[str, list[SecurityAliasInterval]] = {}
    for alias in aliases:
        by_vendor.setdefault(alias.vendor_ticker, []).append(alias)
    for vendor, rows in by_vendor.items():
        for index, left in enumerate(rows):
            for right in rows[index + 1 :]:
                overlaps = (
                    left.effective_from <= right.effective_to
                    and right.effective_from <= left.effective_to
                )
                if overlaps and left.canonical_ticker != right.canonical_ticker:
                    raise OpportunitySetDataError(
                        f"ambiguous security aliases for {vendor}"
                    )
    return aliases


def _canonical_ticker(
    ticker: str,
    date: pd.Timestamp,
    aliases: Sequence[SecurityAliasInterval],
) -> str:
    matches = {
        alias.canonical_ticker
        for alias in aliases
        if alias.vendor_ticker == ticker
        and alias.effective_from <= date <= alias.effective_to
    }
    if len(matches) > 1:
        raise OpportunitySetDataError(
            f"ambiguous alias for {ticker} on {date.date()}"
        )
    return next(iter(matches), ticker)


def _prefer_historical_vendor_factor_rows(
    frame: pd.DataFrame,
    date: pd.Timestamp,
    aliases: Sequence[SecurityAliasInterval],
) -> pd.DataFrame:
    """Choose the interval-authoritative vendor factor before alias collapse.

    Tushare sometimes back-publishes both historical and renamed codes with
    slightly different adjustment-factor normalizations.  During an explicit
    verified alias interval, the historical vendor code is the authoritative
    point-in-time identity.  Outside that interval the canonical row remains.
    """

    if not isinstance(frame, pd.DataFrame):
        raise TypeError("adj_factor must be a pandas DataFrame")
    work = frame.copy()
    if "ticker" in work.columns and "ts_code" in work.columns:
        left = work["ticker"].astype("string").str.strip()
        right = work["ts_code"].astype("string").str.strip()
        if not (left.eq(right) | (left.isna() & right.isna())).all():
            raise OpportunitySetDataError(
                "adj_factor contains inconsistent ticker and ts_code columns"
            )
        identifier = "ticker"
    elif "ticker" in work.columns:
        identifier = "ticker"
    elif "ts_code" in work.columns:
        identifier = "ts_code"
    else:
        raise OpportunitySetDataError("adj_factor requires ticker or ts_code")
    codes = work[identifier].astype("string").str.strip()
    for alias in aliases:
        if not alias.effective_from <= date <= alias.effective_to:
            continue
        has_vendor = bool(codes.eq(alias.vendor_ticker).any())
        has_canonical = bool(codes.eq(alias.canonical_ticker).any())
        if has_vendor and has_canonical:
            work = work.loc[~codes.eq(alias.canonical_ticker)].copy()
            codes = work[identifier].astype("string").str.strip()
    return work


def _positive_stock_st_tickers(
    frame: pd.DataFrame | None,
    date: pd.Timestamp,
    aliases: Sequence[SecurityAliasInterval],
    target_tickers: set[str],
) -> set[str]:
    """Normalize the daily positive list while ignoring display-name variants."""

    if frame is None:
        return set()
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("stock_st must be a pandas DataFrame or None")
    if frame.empty:
        return set()
    if "ticker" in frame.columns:
        identifier = "ticker"
    elif "ts_code" in frame.columns:
        identifier = "ts_code"
    else:
        raise OpportunitySetDataError("stock_st requires ticker or ts_code")
    if "date" in frame.columns:
        date_column = "date"
    elif "trade_date" in frame.columns:
        date_column = "trade_date"
    else:
        raise OpportunitySetDataError("stock_st requires date or trade_date")
    minimal = frame[[identifier, date_column]].copy()
    minimal["is_st"] = True
    normalized = normalize_security_aliases(
        minimal, date, aliases, role="sparse pricing stock_st"
    )
    return set(normalized.loc[normalized["ticker"].isin(target_tickers), "ticker"])


def _metadata(
    securities: pd.DataFrame, target_tickers: set[str]
) -> pd.DataFrame:
    if not isinstance(securities, pd.DataFrame):
        raise TypeError("securities must be a pandas DataFrame")
    work = securities.copy()
    if "ticker" not in work:
        if "ts_code" not in work:
            raise OpportunitySetDataError("securities require ticker or ts_code")
        work = work.rename(columns={"ts_code": "ticker"})
    required = {"ticker", "list_date", "delist_date"}
    missing = sorted(required - set(work.columns))
    if missing:
        raise OpportunitySetDataError(f"securities missing columns: {missing}")
    work["ticker"] = work["ticker"].astype("string").str.strip()
    work["list_date"] = _date_column(work["list_date"], field="list_date")
    work["delist_date"] = _date_column(work["delist_date"], field="delist_date")
    if work["ticker"].isna().any() or work["list_date"].isna().any():
        raise OpportunitySetDataError("securities contain unknown identity/list date")
    if work.duplicated("ticker").any():
        raise OpportunitySetDataError("securities contain duplicate tickers")
    selected = work.loc[work["ticker"].isin(target_tickers)].copy()
    absent = sorted(target_tickers - set(selected["ticker"].astype(str)))
    if absent:
        raise OpportunitySetDataError(f"targets missing from security master: {absent[:10]}")
    invalid = selected["delist_date"].notna() & selected["delist_date"].lt(
        selected["list_date"]
    )
    if invalid.any():
        raise OpportunitySetDataError("security delist precedes list date")
    return selected[["ticker", "list_date", "delist_date"]].sort_values(
        "ticker", kind="mergesort"
    ).reset_index(drop=True)


def _blocking_suspension(value: Any) -> bool:
    if value is None or value is pd.NA or bool(pd.isna(value)):
        return True
    text = str(value).strip()
    if not text:
        return True
    first = text.split(",", 1)[0].split("-", 1)[0].strip()
    try:
        parsed = pd.Timestamp(f"2000-01-01 {first}")
    except (TypeError, ValueError):
        raise OpportunitySetDataError(f"invalid suspension timing: {value!r}")
    return (parsed.hour, parsed.minute, parsed.second) <= (9, 30, 0)


@dataclass(frozen=True)
class SparsePricingDay:
    date: str
    active_ticker_count: int
    suspended_ticker_count: int
    frame: pd.DataFrame


class SparsePricingBuilder:
    """Build only ever-targeted ticker rows while preserving every session."""

    def __init__(
        self,
        official_calendar: Sequence[Any],
        securities: pd.DataFrame,
        target_tickers: Iterable[str],
        *,
        aliases: Iterable[SecurityAliasInterval | Mapping[str, Any]] = (),
    ) -> None:
        calendar = tuple(
            _timestamp(value, field="official_calendar") for value in official_calendar
        )
        if not calendar or list(calendar) != sorted(calendar) or len(set(calendar)) != len(calendar):
            raise OpportunitySetDataError(
                "official_calendar must be non-empty, unique, and increasing"
            )
        targets = {str(value).strip() for value in target_tickers if str(value).strip()}
        if not targets or CALENDAR_SENTINEL in targets:
            raise OpportunitySetDataError("target_tickers must be non-empty and real")
        self._calendar = calendar
        self._calendar_index = {value: index for index, value in enumerate(calendar)}
        self._targets = targets
        self._aliases = _alias_values(aliases)
        self._securities = _metadata(securities, targets)
        self._history: deque[pd.DataFrame] = deque(maxlen=ADV_WINDOW_SESSIONS)
        self._last_index: int | None = None
        self._carried_suspensions: set[str] = set()

    @property
    def target_tickers(self) -> frozenset[str]:
        return frozenset(self._targets)

    def _active(self, date: pd.Timestamp) -> pd.DataFrame:
        return self._securities.loc[
            self._securities["list_date"].le(date)
            & (
                self._securities["delist_date"].isna()
                | self._securities["delist_date"].gt(date)
            )
        ].copy()

    def _suspensions(
        self, suspensions: pd.DataFrame | None, date: pd.Timestamp
    ) -> tuple[set[str], set[str], set[str], set[str]]:
        if suspensions is None or suspensions.empty:
            return set(), set(), set(), set()
        if not isinstance(suspensions, pd.DataFrame):
            raise TypeError("suspensions must be a DataFrame or None")
        work = suspensions.copy()
        if "ticker" not in work:
            if "ts_code" not in work:
                raise OpportunitySetDataError("suspensions require ticker or ts_code")
            work = work.rename(columns={"ts_code": "ticker"})
        work["ticker"] = [
            _canonical_ticker(str(value).strip(), date, self._aliases)
            for value in work["ticker"]
        ]
        work = work.loc[work["ticker"].isin(self._targets)].copy()
        resumed: set[str] = set()
        if "suspend_type" in work:
            types = work["suspend_type"].astype("string").str.strip().str.upper()
            unknown = ~types.isin(["S", "R"])
            if unknown.any():
                raise OpportunitySetDataError("suspensions contain unknown event type")
            resumed = set(work.loc[types.eq("R"), "ticker"].astype(str))
            work = work.loc[types.eq("S")].copy()
        all_s = set(work["ticker"].astype(str))
        if "suspend_timing" in work:
            timing = work["suspend_timing"].astype("string").str.strip()
            blocking = {
                str(row.ticker)
                for row in work[["ticker", "suspend_timing"]].itertuples(index=False)
                if _blocking_suspension(row.suspend_timing)
            }
            carryable = set(
                work.loc[timing.isna() | timing.eq(""), "ticker"].astype(str)
            )
        else:
            blocking = set(all_s)
            carryable = set(all_s)
        return all_s, blocking, resumed, carryable

    def push_day(
        self,
        trade_date: Any,
        *,
        daily: pd.DataFrame,
        adj_factor: pd.DataFrame,
        stock_st: pd.DataFrame | None = None,
        suspensions: pd.DataFrame | None = None,
    ) -> SparsePricingDay:
        date = _timestamp(trade_date, field="trade_date")
        if date not in self._calendar_index:
            raise OpportunitySetDataError(f"{date.date()} is not an official session")
        index = self._calendar_index[date]
        if self._last_index is not None and index != self._last_index + 1:
            raise OpportunitySetDataError("sparse pricing stream has a calendar gap/replay")
        active = self._active(date)
        active_tickers = set(active["ticker"].astype(str))
        (
            all_suspensions,
            blocking_suspensions,
            resumed_suspensions,
            carryable_suspensions,
        ) = self._suspensions(suspensions, date)
        st_tickers = _positive_stock_st_tickers(
            stock_st, date, self._aliases, self._targets
        )

        market = normalize_security_aliases(
            daily, date, self._aliases, role="sparse pricing daily"
        )
        market = market.loc[market["ticker"].isin(active_tickers)].copy()
        required = {"ticker", "open", "high", "low", "pre_close", "pct_chg", "amount"}
        missing_columns = sorted(required - set(market.columns))
        if missing_columns:
            raise OpportunitySetDataError(f"daily missing pricing columns: {missing_columns}")
        numeric_columns = ["open", "high", "low", "pre_close", "pct_chg", "amount"]
        market[numeric_columns] = market[numeric_columns].apply(
            pd.to_numeric, errors="coerce"
        )
        if not np.isfinite(market[numeric_columns]).all().all():
            raise OpportunitySetDataError("daily pricing values must be finite")
        if market["amount"].lt(0.0).any():
            raise OpportunitySetDataError("daily amount must be non-negative")
        observed = set(market["ticker"].astype(str))
        carried_at_open = self._carried_suspensions & active_tickers
        suspension_proof = (
            carried_at_open - resumed_suspensions
        ) | carryable_suspensions
        unexplained = sorted((active_tickers - observed) - suspension_proof - st_tickers)
        if unexplained:
            raise OpportunitySetDataError(
                f"active target lacks daily bar/suspension proof: {unexplained[:10]}"
            )

        factors = normalize_security_aliases(
            _prefer_historical_vendor_factor_rows(
                adj_factor, date, self._aliases
            ),
            date,
            self._aliases,
            role="sparse pricing adj_factor",
        )
        if "adj_factor" not in factors:
            raise OpportunitySetDataError("adj_factor partition lacks adj_factor")
        factors = factors.loc[factors["ticker"].isin(observed), ["ticker", "adj_factor"]]
        factors["adj_factor"] = pd.to_numeric(factors["adj_factor"], errors="coerce")
        if factors["ticker"].duplicated().any() or not np.isfinite(
            factors["adj_factor"]
        ).all() or factors["adj_factor"].le(0.0).any():
            raise OpportunitySetDataError("adj_factor rows are duplicate/non-finite/non-positive")
        market = market.merge(factors, on="ticker", how="left", validate="one_to_one")
        if market["adj_factor"].isna().any():
            raise OpportunitySetDataError("a priced target lacks contemporaneous adj_factor")

        observations = market[["ticker", "amount", "pct_chg"]].copy()
        observations["amount_rmb"] = observations.pop("amount") * 1000.0
        observations["return_1d"] = observations.pop("pct_chg") / 100.0
        missing_active = sorted((active_tickers - observed) - st_tickers)
        if missing_active:
            observations = pd.concat(
                [
                    observations,
                    pd.DataFrame(
                        {
                            "ticker": missing_active,
                            "amount_rmb": 0.0,
                            "return_1d": 0.0,
                        }
                    ),
                ],
                ignore_index=True,
            )
        candidate_history = [*self._history, observations][
            -ADV_WINDOW_SESSIONS:
        ]
        rolling = pd.concat(candidate_history, ignore_index=True)
        metrics = (
            rolling.groupby("ticker", sort=False)
            .agg(
                observation_count=("return_1d", "size"),
                amount_sum_rmb=("amount_rmb", "sum"),
                volatility_20=("return_1d", "std"),
            )
            .reset_index()
        )
        metrics["adv_20"] = metrics["amount_sum_rmb"] / ADV_WINDOW_SESSIONS

        output = active[["ticker"]].merge(
            market[
                [
                    "ticker",
                    "open",
                    "high",
                    "low",
                    "pre_close",
                    "adj_factor",
                ]
            ],
            on="ticker",
            how="left",
            validate="one_to_one",
        ).merge(
            metrics[["ticker", "observation_count", "adv_20", "volatility_20"]],
            on="ticker",
            how="left",
            validate="one_to_one",
        )
        output["date"] = date
        output["open_adj"] = output["open"] * output["adj_factor"]
        flat = output["high"].eq(output["low"])
        output["is_one_price_limit_up"] = flat & output["open"].gt(
            output["pre_close"]
        )
        output["is_one_price_limit_down"] = flat & output["open"].lt(
            output["pre_close"]
        )
        # An actual daily bar is direct evidence that a stale carried halt no
        # longer prevents execution at the session's first tradable price.
        # A current S event can still block that price according to its timing.
        effective_blocking = blocking_suspensions | (
            carried_at_open - resumed_suspensions - observed
        )
        output["is_suspended"] = output["ticker"].isin(effective_blocking)
        output["is_delisted"] = False
        output["eligible"] = True
        output["universe_member"] = True
        not_ready = output["observation_count"].ne(ADV_WINDOW_SESSIONS)
        output.loc[not_ready, ["adv_20", "volatility_20"]] = np.nan

        previous = self._calendar[index - 1] if index > 0 else pd.Timestamp.min
        delisted = self._securities.loc[
            self._securities["delist_date"].notna()
            & self._securities["delist_date"].gt(previous)
            & self._securities["delist_date"].le(date),
            "ticker",
        ].astype(str)
        if not delisted.empty:
            output = pd.concat(
                [
                    output,
                    pd.DataFrame(
                        {
                            "ticker": delisted.tolist(),
                            "date": date,
                            "open_adj": np.nan,
                            "adv_20": np.nan,
                            "volatility_20": np.nan,
                            "is_one_price_limit_up": False,
                            "is_one_price_limit_down": False,
                            "is_suspended": False,
                            "is_delisted": True,
                            "eligible": False,
                            "universe_member": False,
                        }
                    ),
                ],
                ignore_index=True,
            )
        output = output[
            [
                "ticker",
                "date",
                "open_adj",
                "adv_20",
                "volatility_20",
                "eligible",
                "universe_member",
                "is_one_price_limit_up",
                "is_one_price_limit_down",
                "is_suspended",
                "is_delisted",
            ]
        ]
        output = pd.concat(
            [
                output,
                pd.DataFrame(
                    {
                        "ticker": [CALENDAR_SENTINEL],
                        "date": [date],
                        "open_adj": [1.0],
                        "adv_20": [1.0],
                        "volatility_20": [0.0],
                        "eligible": [False],
                        "universe_member": [False],
                        "is_one_price_limit_up": [False],
                        "is_one_price_limit_down": [False],
                        "is_suspended": [False],
                        "is_delisted": [False],
                    }
                ),
            ],
            ignore_index=True,
        ).sort_values("ticker", kind="mergesort").reset_index(drop=True)
        if output.duplicated(["date", "ticker"]).any():
            raise OpportunitySetDataError("sparse pricing emitted duplicate keys")

        self._history.append(observations)
        self._carried_suspensions = (
            (self._carried_suspensions | carryable_suspensions)
            - observed
            - resumed_suspensions
        ) & active_tickers
        self._last_index = index
        return SparsePricingDay(
            date=date.date().isoformat(),
            active_ticker_count=len(active_tickers),
            suspended_ticker_count=len(effective_blocking & active_tickers),
            frame=output,
        )


__all__ = ["CALENDAR_SENTINEL", "SparsePricingBuilder", "SparsePricingDay"]
