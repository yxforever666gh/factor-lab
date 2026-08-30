"""Causal, streaming construction of widened A-share opportunity sets.

The builder consumes one *official* trading session at a time.  It keeps only
the latest twenty normalized daily observations in memory and returns only the
Top-25 fixed-core ranking for each frozen universe arm.  Raw I/O, manifests,
and execution are deliberately outside this module.

``stock_st`` is a complete positive-list partition: a non-``None`` empty frame
means that no security is ST on that session, while ``None`` means the daily
partition is unknown and therefore fails closed.  Suspension input is also a
positive list, but is consulted only to explain a missing daily bar.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from numbers import Number
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from factor_lab.strategy import fixed_core_score


ADV_WINDOW_SESSIONS = 20
MIN_LISTING_SESSIONS = 120
MIN_ADV_RMB = 100_000_000.0
RANKING_LIMIT = 25

UNIVERSE_TOP500 = "daily_adv20_top500_control"
UNIVERSE_ADV_GE_100M = "daily_adv20_ge_100m"
UNIVERSE_TOP1500 = "daily_adv20_top1500"
UNIVERSE_NAMES = (
    UNIVERSE_TOP500,
    UNIVERSE_ADV_GE_100M,
    UNIVERSE_TOP1500,
)

FORBIDDEN_INPUT_COLUMN_FRAGMENTS = ("forward", "future", "label", "outcome")


class OpportunitySetDataError(ValueError):
    """Raised when a point-in-time input is incomplete or ambiguous."""


def _timestamp(value: Any, *, field: str = "date") -> pd.Timestamp:
    if value is None or value is pd.NaT or pd.isna(value):
        raise OpportunitySetDataError(f"{field} must be known")
    text = str(value).strip()
    if not text:
        raise OpportunitySetDataError(f"{field} must be known")
    if len(text) == 8 and text.isdigit():
        parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        raise OpportunitySetDataError(f"invalid {field}: {value!r}")
    result = pd.Timestamp(parsed)
    if result.tzinfo is not None:
        result = result.tz_convert(None)
    return result.normalize()


def _date_series(values: pd.Series, *, field: str) -> pd.Series:
    text = values.astype("string").str.strip()
    missing = text.isna() | text.eq("")
    compact = text.str.fullmatch(r"\d{8}", na=False)
    result = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    if compact.any():
        result.loc[compact] = pd.to_datetime(
            text.loc[compact], format="%Y%m%d", errors="coerce"
        ).dt.normalize()
    ordinary = ~missing & ~compact
    if ordinary.any():
        parsed = pd.to_datetime(text.loc[ordinary], errors="coerce")
        if isinstance(parsed.dtype, pd.DatetimeTZDtype):
            parsed = parsed.dt.tz_convert(None)
        result.loc[ordinary] = parsed.dt.normalize()
    return result


def _reject_forbidden_columns(frame: pd.DataFrame, *, role: str) -> None:
    forbidden = sorted(
        str(column)
        for column in frame.columns
        if any(
            fragment in str(column).strip().casefold()
            for fragment in FORBIDDEN_INPUT_COLUMN_FRAGMENTS
        )
    )
    if forbidden:
        raise OpportunitySetDataError(
            f"{role} contains forbidden forward/label/future/outcome columns: "
            f"{forbidden}"
        )


def _standardize_identifier_columns(
    frame: pd.DataFrame,
    *,
    signal_date: pd.Timestamp | None,
    role: str,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"{role} must be a pandas DataFrame")
    _reject_forbidden_columns(frame, role=role)
    work = frame.copy()

    if "ticker" not in work.columns:
        if "ts_code" not in work.columns:
            raise OpportunitySetDataError(
                f"{role} must contain ticker or ts_code"
            )
        work = work.rename(columns={"ts_code": "ticker"})
    elif "ts_code" in work.columns:
        left = work["ticker"].astype("string").str.strip()
        right = work["ts_code"].astype("string").str.strip()
        if ~(left.eq(right) | (left.isna() & right.isna())).all():
            raise OpportunitySetDataError(
                f"{role} contains inconsistent ticker and ts_code columns"
            )
        work = work.drop(columns="ts_code")
    work["ticker"] = work["ticker"].astype("string").str.strip()
    if work["ticker"].isna().any() or work["ticker"].eq("").any():
        raise OpportunitySetDataError(f"{role} contains an unknown ticker")

    if "date" not in work.columns:
        if "trade_date" in work.columns:
            work = work.rename(columns={"trade_date": "date"})
        elif signal_date is not None:
            work["date"] = signal_date
        elif not work.empty:
            raise OpportunitySetDataError(f"{role} must contain date or trade_date")
        else:
            work["date"] = pd.Series(dtype="datetime64[ns]")
    elif "trade_date" in work.columns:
        left = _date_series(work["date"], field=f"{role}.date")
        right = _date_series(work["trade_date"], field=f"{role}.trade_date")
        if ~(left.eq(right) | (left.isna() & right.isna())).all():
            raise OpportunitySetDataError(
                f"{role} contains inconsistent date and trade_date columns"
            )
        work = work.drop(columns="trade_date")
    work["date"] = _date_series(work["date"], field=f"{role}.date")
    if work["date"].isna().any():
        raise OpportunitySetDataError(f"{role} contains an unknown date")
    if signal_date is not None and not work["date"].eq(signal_date).all():
        seen = sorted(work["date"].dt.date.astype(str).unique().tolist())
        raise OpportunitySetDataError(
            f"{role} is not the exact signal-date partition {signal_date.date()}: "
            f"{seen}"
        )
    return work


def _scalar_equal(left: Any, right: Any) -> bool:
    try:
        left_missing = bool(pd.isna(left))
    except (TypeError, ValueError):
        left_missing = False
    try:
        right_missing = bool(pd.isna(right))
    except (TypeError, ValueError):
        right_missing = False
    if left_missing or right_missing:
        return left_missing and right_missing
    if isinstance(left, Number) and isinstance(right, Number):
        return float(left) == float(right)
    if isinstance(left, (pd.Timestamp, np.datetime64)) or isinstance(
        right, (pd.Timestamp, np.datetime64)
    ):
        return pd.Timestamp(left) == pd.Timestamp(right)
    try:
        return bool(left == right)
    except (TypeError, ValueError):
        return False


def _merge_identical_duplicates(
    frame: pd.DataFrame,
    *,
    keys: Sequence[str],
    source_column: str,
    role: str,
) -> pd.DataFrame:
    if frame.empty:
        return frame.drop(columns=[source_column], errors="ignore").reset_index(drop=True)
    duplicate_mask = frame.duplicated(list(keys), keep=False)
    payload_columns = [
        column
        for column in frame.columns
        if column not in {*keys, source_column, "_input_order"}
    ]
    if duplicate_mask.any():
        duplicates = frame.loc[duplicate_mask]
        for key, group in duplicates.groupby(list(keys), sort=False, dropna=False):
            first = group.iloc[0]
            for row_index in range(1, len(group)):
                candidate = group.iloc[row_index]
                inconsistent = [
                    column
                    for column in payload_columns
                    if not _scalar_equal(first[column], candidate[column])
                ]
                if inconsistent:
                    raise OpportunitySetDataError(
                        f"{role} aliases overlap at {key!r} with inconsistent "
                        f"content columns: {inconsistent}"
                    )
    ordered = frame.sort_values(
        [*keys, source_column, "_input_order"],
        kind="mergesort",
    )
    merged = ordered.drop_duplicates(list(keys), keep="first")
    return merged.drop(
        columns=[source_column, "_input_order"], errors="ignore"
    ).reset_index(drop=True)


@dataclass(frozen=True)
class SecurityAliasInterval:
    """One explicit inclusive vendor-code interval, ``[from, to]``."""

    canonical_ticker: str
    vendor_ticker: str
    effective_from: pd.Timestamp
    effective_to: pd.Timestamp
    source: str | None = None

    @classmethod
    def from_value(
        cls, value: "SecurityAliasInterval | Mapping[str, Any]"
    ) -> "SecurityAliasInterval":
        if isinstance(value, cls):
            return value
        raw = dict(value)
        canonical = raw.get("canonical_ticker", raw.get("canonical_ts_code"))
        vendor = raw.get("vendor_ticker", raw.get("vendor_ts_code"))
        missing = [
            name
            for name, item in (
                ("canonical_ticker", canonical),
                ("vendor_ticker", vendor),
                ("effective_from", raw.get("effective_from")),
                ("effective_to", raw.get("effective_to")),
            )
            if item is None or not str(item).strip()
        ]
        if missing:
            raise OpportunitySetDataError(
                f"security alias interval missing explicit fields: {missing}"
            )
        return cls(
            canonical_ticker=str(canonical).strip(),
            vendor_ticker=str(vendor).strip(),
            effective_from=_timestamp(
                raw["effective_from"], field="alias.effective_from"
            ),
            effective_to=_timestamp(raw["effective_to"], field="alias.effective_to"),
            source=(
                str(raw["source"]).strip()
                if raw.get("source") is not None
                else None
            ),
        )

    def __post_init__(self) -> None:
        object.__setattr__(self, "canonical_ticker", str(self.canonical_ticker).strip())
        object.__setattr__(self, "vendor_ticker", str(self.vendor_ticker).strip())
        object.__setattr__(
            self,
            "effective_from",
            _timestamp(self.effective_from, field="alias.effective_from"),
        )
        object.__setattr__(
            self,
            "effective_to",
            _timestamp(self.effective_to, field="alias.effective_to"),
        )
        if self.source is not None:
            object.__setattr__(self, "source", str(self.source).strip())
        if not self.canonical_ticker or not self.vendor_ticker:
            raise OpportunitySetDataError("security alias tickers must be explicit")
        if self.canonical_ticker == self.vendor_ticker:
            raise OpportunitySetDataError(
                "security alias canonical and vendor tickers must differ"
            )
        if self.effective_from > self.effective_to:
            raise OpportunitySetDataError(
                f"invalid security alias interval for {self.vendor_ticker}"
            )


def _compile_aliases(
    values: Iterable[SecurityAliasInterval | Mapping[str, Any]],
) -> tuple[SecurityAliasInterval, ...]:
    aliases = sorted(
        {SecurityAliasInterval.from_value(value) for value in values},
        key=lambda item: (
            item.vendor_ticker,
            item.effective_from,
            item.effective_to,
            item.canonical_ticker,
        ),
    )
    by_vendor: dict[str, list[SecurityAliasInterval]] = {}
    for alias in aliases:
        by_vendor.setdefault(alias.vendor_ticker, []).append(alias)
    for vendor, intervals in by_vendor.items():
        canonical_identities = {item.canonical_ticker for item in intervals}
        if len(canonical_identities) != 1:
            raise OpportunitySetDataError(
                f"vendor ticker {vendor} maps to multiple canonical identities"
            )
        for index, left in enumerate(intervals):
            for right in intervals[index + 1 :]:
                overlaps = (
                    left.effective_from <= right.effective_to
                    and right.effective_from <= left.effective_to
                )
                if overlaps:
                    if left.canonical_ticker != right.canonical_ticker:
                        raise OpportunitySetDataError(
                            f"ambiguous overlapping alias intervals for {vendor}"
                        )
    return tuple(aliases)


def _resolve_ticker(
    ticker: str,
    signal_date: pd.Timestamp,
    aliases: Sequence[SecurityAliasInterval],
) -> str:
    matches = {
        alias.canonical_ticker
        for alias in aliases
        if alias.vendor_ticker == ticker
        and alias.effective_from <= signal_date <= alias.effective_to
    }
    if len(matches) > 1:
        raise OpportunitySetDataError(
            f"ticker {ticker} has ambiguous aliases on {signal_date.date()}"
        )
    return next(iter(matches), ticker)


def normalize_security_aliases(
    frame: pd.DataFrame,
    signal_date: Any,
    aliases: Iterable[SecurityAliasInterval | Mapping[str, Any]] = (),
    *,
    role: str = "security rows",
) -> pd.DataFrame:
    """Normalize interval-bounded aliases and merge only identical overlaps.

    When vendor and canonical records collide at the same canonical/date key,
    every non-identifier cell must agree.  Identical rows are merged with a
    stable source-ticker tie-break; conflicting content fails closed.
    """

    date = _timestamp(signal_date, field="signal_date")
    compiled = _compile_aliases(aliases)
    work = _standardize_identifier_columns(
        frame, signal_date=date, role=role
    )
    if work.empty:
        return work.reset_index(drop=True)
    work["_source_ticker"] = work["ticker"].astype(str)
    work["_input_order"] = np.arange(len(work), dtype=np.int64)
    work["ticker"] = [
        _resolve_ticker(str(ticker), date, compiled)
        for ticker in work["_source_ticker"].tolist()
    ]
    return _merge_identical_duplicates(
        work,
        keys=("date", "ticker"),
        source_column="_source_ticker",
        role=role,
    )


def _strict_bool(value: Any, *, field: str) -> bool:
    if value is None or value is pd.NA or pd.isna(value):
        raise OpportunitySetDataError(f"{field} contains an unknown status")
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, Number):
        numeric = float(value)
        if numeric in (0.0, 1.0):
            return bool(numeric)
        raise OpportunitySetDataError(f"{field} must be boolean, zero, or one")
    text = str(value).strip().casefold()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    raise OpportunitySetDataError(f"{field} contains an unknown status: {value!r}")


@dataclass(frozen=True)
class RankedSecurity:
    rank: int
    ticker: str
    fixed_core_score: float
    adv20_rmb: float
    volatility_20: float


@dataclass(frozen=True)
class UniverseRanking:
    name: str
    member_count: int
    finite_score_count: int
    daily_basic_row_absent_with_daily_bar_count: int
    daily_basic_row_absent_with_proven_no_daily_bar_count: int
    pe_ttm_null_count: int
    pb_null_count: int
    invalid_non_null_fundamental_count: int
    expected_finite_score_count: int
    unexpected_score_mismatch_count: int
    arithmetic_nonfinite_count: int
    classified_unscoreable_count: int
    unclassified_unscoreable_count: int
    top25: tuple[RankedSecurity, ...]

    @property
    def finite_score_coverage(self) -> float:
        return (
            self.finite_score_count / self.member_count
            if self.member_count
            else 0.0
        )

    @property
    def tickers(self) -> tuple[str, ...]:
        return tuple(row.ticker for row in self.top25)

    def to_frame(self) -> pd.DataFrame:
        """Materialize only the retained ranking rows for downstream scripts."""

        diagnostics = {
            "daily_basic_row_absent_with_daily_bar_count": (
                self.daily_basic_row_absent_with_daily_bar_count
            ),
            "daily_basic_row_absent_with_proven_no_daily_bar_count": (
                self.daily_basic_row_absent_with_proven_no_daily_bar_count
            ),
            "pe_ttm_null_count": self.pe_ttm_null_count,
            "pb_null_count": self.pb_null_count,
            "invalid_non_null_fundamental_count": (
                self.invalid_non_null_fundamental_count
            ),
            "expected_finite_score_count": self.expected_finite_score_count,
            "unexpected_score_mismatch_count": (
                self.unexpected_score_mismatch_count
            ),
            "arithmetic_nonfinite_count": self.arithmetic_nonfinite_count,
            "classified_unscoreable_count": self.classified_unscoreable_count,
            "unclassified_unscoreable_count": (
                self.unclassified_unscoreable_count
            ),
        }
        return pd.DataFrame(
            [
                {
                    "rank": row.rank,
                    "ticker": row.ticker,
                    "fixed_core_score": row.fixed_core_score,
                    "adv20_rmb": row.adv20_rmb,
                    "volatility_20": row.volatility_20,
                    **diagnostics,
                }
                for row in self.top25
            ],
            columns=(
                "rank",
                "ticker",
                "fixed_core_score",
                "adv20_rmb",
                "volatility_20",
                *diagnostics,
            ),
        )


@dataclass(frozen=True)
class CarriedSuspensionEvidence:
    """One missing bar explained only by a prior explicit full-day S event."""

    ticker: str
    source_suspend_date: str
    official_session_age: int


@dataclass(frozen=True)
class DailyOpportunitySet:
    signal_date: str
    observed_window_sessions: int
    base_eligible_count: int
    inactive_stock_st_ignored_count: int
    carried_suspension_evidence: tuple[CarriedSuspensionEvidence, ...]
    universes: tuple[UniverseRanking, ...]

    @property
    def history_ready(self) -> bool:
        return self.observed_window_sessions == ADV_WINDOW_SESSIONS

    def universe(self, name: str) -> UniverseRanking:
        for result in self.universes:
            if result.name == name:
                return result
        raise KeyError(name)


def _normalize_metadata(
    securities: pd.DataFrame,
    aliases: Sequence[SecurityAliasInterval],
) -> pd.DataFrame:
    if not isinstance(securities, pd.DataFrame):
        raise TypeError("securities must be a pandas DataFrame")
    _reject_forbidden_columns(securities, role="securities")
    work = securities.copy()
    if "ticker" not in work.columns:
        if "ts_code" not in work.columns:
            raise OpportunitySetDataError("securities must contain ticker or ts_code")
        work = work.rename(columns={"ts_code": "ticker"})
    elif "ts_code" in work.columns:
        left = work["ticker"].astype("string").str.strip()
        right = work["ts_code"].astype("string").str.strip()
        if ~(left.eq(right) | (left.isna() & right.isna())).all():
            raise OpportunitySetDataError(
                "securities contains inconsistent ticker and ts_code columns"
            )
        work = work.drop(columns="ts_code")
    required = {"ticker", "list_date", "delist_date"}
    missing = sorted(required - set(work.columns))
    if missing:
        raise OpportunitySetDataError(
            f"securities missing required columns: {missing}"
        )
    work["ticker"] = work["ticker"].astype("string").str.strip()
    if work["ticker"].isna().any() or work["ticker"].eq("").any():
        raise OpportunitySetDataError("securities contains an unknown ticker")
    work["list_date"] = _date_series(
        work["list_date"], field="securities.list_date"
    )
    work["delist_date"] = _date_series(
        work["delist_date"], field="securities.delist_date"
    )
    if work["list_date"].isna().any():
        raise OpportunitySetDataError("securities contains an unknown list_date")
    invalid_delist = work["delist_date"].notna() & work["delist_date"].lt(
        work["list_date"]
    )
    if invalid_delist.any():
        raise OpportunitySetDataError("securities contains delist_date before list_date")

    global_identity: dict[str, str] = {}
    for alias in aliases:
        prior = global_identity.setdefault(
            alias.vendor_ticker, alias.canonical_ticker
        )
        if prior != alias.canonical_ticker:
            raise OpportunitySetDataError(
                f"vendor ticker {alias.vendor_ticker} has ambiguous canonical identity"
            )
    work["_source_ticker"] = work["ticker"].astype(str)
    work["_input_order"] = np.arange(len(work), dtype=np.int64)
    work["ticker"] = work["_source_ticker"].map(
        lambda ticker: global_identity.get(ticker, ticker)
    )
    return _merge_identical_duplicates(
        work,
        keys=("ticker",),
        source_column="_source_ticker",
        role="securities",
    ).sort_values("ticker", kind="mergesort").reset_index(drop=True)


class DailyOpportunitySetBuilder:
    """Stateful one-session-at-a-time opportunity-set builder.

    Calls must follow adjacent entries of ``official_calendar``.  Validation
    and ranking finish before state is committed, so a rejected partition can
    be corrected and retried without corrupting the rolling window.
    """

    def __init__(
        self,
        official_calendar: Sequence[Any],
        securities: pd.DataFrame,
        *,
        aliases: Iterable[SecurityAliasInterval | Mapping[str, Any]] = (),
        universe_names: Sequence[str] = UNIVERSE_NAMES,
    ) -> None:
        calendar = tuple(
            _timestamp(value, field="official_calendar")
            for value in official_calendar
        )
        if not calendar:
            raise OpportunitySetDataError("official_calendar must not be empty")
        if len(set(calendar)) != len(calendar) or list(calendar) != sorted(calendar):
            raise OpportunitySetDataError(
                "official_calendar must be unique and strictly increasing"
            )
        self._calendar = calendar
        self._calendar_array = np.asarray(calendar, dtype="datetime64[ns]")
        self._calendar_index = {date: index for index, date in enumerate(calendar)}
        self._aliases = _compile_aliases(aliases)
        self._securities = _normalize_metadata(securities, self._aliases)
        self._securities["_listing_first_calendar_index"] = np.searchsorted(
            self._calendar_array,
            self._securities["list_date"].to_numpy(dtype="datetime64[ns]"),
            side="left",
        ).astype(np.int64)
        selected_universes = tuple(map(str, universe_names))
        if (
            not selected_universes
            or len(set(selected_universes)) != len(selected_universes)
            or not set(selected_universes).issubset(UNIVERSE_NAMES)
        ):
            raise OpportunitySetDataError(
                "universe_names must be a unique non-empty subset of frozen arms"
            )
        self._universe_names = selected_universes
        self._history: deque[pd.DataFrame] = deque(maxlen=ADV_WINDOW_SESSIONS)
        self._last_calendar_index: int | None = None
        # ``suspend_d`` is an event stream, not a guaranteed one-row-per-
        # suspended-session roster.  Once a full-day absence has an explicit S
        # event, carry that causal state until an R event or an observed daily
        # bar proves that trading resumed.  The candidate state is committed
        # only after the entire day validates.
        self._carried_suspensions: dict[str, pd.Timestamp] = {}

    @property
    def buffered_sessions(self) -> int:
        return len(self._history)

    def _active_securities(self, date: pd.Timestamp) -> pd.DataFrame:
        metadata = self._securities
        active = metadata["list_date"].le(date) & (
            metadata["delist_date"].isna() | metadata["delist_date"].gt(date)
        )
        return metadata.loc[active].copy()

    def _listing_age(self, list_date: pd.Timestamp, calendar_index: int) -> int:
        # When metadata predates calendar coverage, the count is deliberately
        # conservative: it begins at the first supplied official session.
        first = int(np.searchsorted(self._calendar_array, list_date, side="left"))
        first = max(first, 0)
        return max(calendar_index - first + 1, 0)

    def _normalize_day(
        self, frame: pd.DataFrame, date: pd.Timestamp, *, role: str
    ) -> tuple[pd.DataFrame, set[str]]:
        return normalize_security_aliases(
            frame, date, self._aliases, role=role
        )

    def _daily_observations(
        self,
        date: pd.Timestamp,
        daily: pd.DataFrame,
        active_tickers: set[str],
        suspended_tickers: set[str],
        excluded_tickers: set[str],
    ) -> pd.DataFrame:
        work = self._normalize_day(daily, date, role="daily")
        required = {"amount", "pct_chg"}
        missing_columns = sorted(required - set(work.columns))
        if missing_columns:
            raise OpportunitySetDataError(
                f"daily missing required columns: {missing_columns}"
            )
        for column in ("amount", "pct_chg"):
            work[column] = pd.to_numeric(work[column], errors="coerce")
            invalid = work[column].isna() | ~np.isfinite(work[column])
            if invalid.any():
                raise OpportunitySetDataError(
                    f"daily contains an unknown/non-finite {column}"
                )
        if work["amount"].lt(0).any():
            raise OpportunitySetDataError("daily amount must be non-negative")

        observed = set(work["ticker"].astype(str))
        extra = sorted(observed - active_tickers)
        if extra:
            raise OpportunitySetDataError(
                f"daily contains not-yet-listed or delisted securities: {extra[:10]}"
            )
        missing = active_tickers - observed
        unexplained = sorted(missing - suspended_tickers - excluded_tickers)
        if unexplained:
            raise OpportunitySetDataError(
                "listed non-delisted securities lack a daily bar and explicit "
                f"suspension evidence: {unexplained[:10]}"
            )

        result = work.loc[:, ["ticker", "amount", "pct_chg"]].copy()
        result["ticker"] = result["ticker"].astype(str)
        with np.errstate(over="ignore", invalid="ignore"):
            result["amount_rmb"] = result["amount"].astype(float) * 1000.0
        result["return_1d"] = result["pct_chg"].astype(float) / 100.0
        if not np.isfinite(result["amount_rmb"]).all():
            raise OpportunitySetDataError(
                "daily amount overflows after conversion from thousand RMB"
            )
        suspension_zeros = missing - excluded_tickers
        if suspension_zeros:
            zero_rows = pd.DataFrame(
                {
                    "ticker": sorted(suspension_zeros),
                    "amount_rmb": 0.0,
                    "return_1d": 0.0,
                }
            )
            result = pd.concat(
                [result[["ticker", "amount_rmb", "return_1d"]], zero_rows],
                ignore_index=True,
            )
        else:
            result = result[["ticker", "amount_rmb", "return_1d"]]
        return (
            result.sort_values("ticker", kind="mergesort").reset_index(drop=True),
            observed,
        )

    def _stock_st_tickers(
        self,
        date: pd.Timestamp,
        stock_st: pd.DataFrame | None,
        active_tickers: set[str],
    ) -> tuple[set[str], int]:
        if stock_st is None:
            raise OpportunitySetDataError(
                f"stock_st partition is missing/unknown for {date.date()}"
            )
        if not isinstance(stock_st, pd.DataFrame):
            raise TypeError("stock_st must be a pandas DataFrame or None")
        if stock_st.empty:
            _reject_forbidden_columns(stock_st, role="stock_st")
            return set(), 0
        # ``stock_st`` is a positive list.  A renamed security may appear under
        # both provider codes with different display names; after an explicit
        # interval alias they are one identity and positive-list membership is
        # reduced with OR rather than payload equality.
        work = _standardize_identifier_columns(
            stock_st, signal_date=date, role="stock_st"
        )
        work["ticker"] = [
            _resolve_ticker(str(ticker), date, self._aliases)
            for ticker in work["ticker"].tolist()
        ]
        if "is_st" in work.columns:
            statuses = [
                _strict_bool(value, field="stock_st.is_st")
                for value in work["is_st"].tolist()
            ]
        else:
            # Tushare stock_st is itself the complete positive-list endpoint.
            statuses = [True] * len(work)
        work["_is_st"] = statuses
        observed_tickers = set(work["ticker"].astype(str))
        known_tickers = set(self._securities["ticker"].astype(str))
        unknown = sorted(observed_tickers - known_tickers)
        if unknown:
            raise OpportunitySetDataError(
                f"stock_st contains unknown securities: {unknown[:10]}"
            )
        # The positive-list endpoint can publish a status immediately before a
        # relisting date or on a delisting date.  Those known identities are
        # outside today's active roster and cannot affect today's opportunity
        # set, so ignore them rather than treating a provider timing mismatch as
        # a market-data contradiction.
        inactive_count = int((~work["ticker"].isin(active_tickers)).sum())
        work = work.loc[work["ticker"].isin(active_tickers)].copy()
        positive = {
            str(ticker)
            for ticker, is_st in zip(
                work["ticker"].tolist(), work["_is_st"].tolist()
            )
            if is_st
        }
        return positive, inactive_count

    def _suspended_tickers(
        self,
        date: pd.Timestamp,
        suspensions: pd.DataFrame | None,
        active_tickers: set[str],
    ) -> tuple[set[str], set[str], set[str]]:
        if suspensions is None:
            return set(), set(), set()
        if not isinstance(suspensions, pd.DataFrame):
            raise TypeError("suspensions must be a pandas DataFrame or None")
        if suspensions.empty:
            _reject_forbidden_columns(suspensions, role="suspensions")
            return set(), set(), set()
        # ``suspend_d`` can legitimately publish both S and R events for one
        # ticker/session.  For opportunity-set completeness, any S event is
        # sufficient proof for a missing daily bar; the generic alias loader's
        # one-row-per-key conflict rule therefore must not collapse these event
        # rows before their status is reduced.
        work = _standardize_identifier_columns(
            suspensions, signal_date=date, role="suspensions"
        )
        work["ticker"] = [
            _resolve_ticker(str(ticker), date, self._aliases)
            for ticker in work["ticker"].tolist()
        ]
        if "is_suspended" in work.columns:
            statuses = [
                _strict_bool(value, field="suspensions.is_suspended")
                for value in work["is_suspended"].tolist()
            ]
        elif "suspended" in work.columns:
            statuses = [
                _strict_bool(value, field="suspensions.suspended")
                for value in work["suspended"].tolist()
            ]
        elif "suspend_type" in work.columns:
            statuses = []
            for value in work["suspend_type"].tolist():
                code = "" if value is None or pd.isna(value) else str(value).strip().upper()
                if code == "S":
                    statuses.append(True)
                elif code == "R":
                    statuses.append(False)
                else:
                    raise OpportunitySetDataError(
                        f"suspensions.suspend_type is unknown: {value!r}"
                    )
        else:
            statuses = [True] * len(work)
        work["_is_suspended_event"] = statuses
        if "suspend_timing" in work.columns:
            timing = work["suspend_timing"].astype("string").str.strip()
            work["_carryable_full_day_s"] = (
                work["_is_suspended_event"] & (timing.isna() | timing.eq(""))
            )
        else:
            work["_carryable_full_day_s"] = work["_is_suspended_event"]
        observed_tickers = set(work["ticker"].astype(str))
        known_tickers = set(self._securities["ticker"].astype(str))
        unknown = sorted(observed_tickers - known_tickers)
        if unknown:
            raise OpportunitySetDataError(
                f"suspensions contains unknown securities: {unknown[:10]}"
            )
        work = work.loc[work["ticker"].isin(active_tickers)].copy()
        suspended = {
            str(ticker)
            for ticker, suspended in zip(
                work["ticker"].tolist(), work["_is_suspended_event"].tolist()
            )
            if suspended
        }
        resumed = {
            str(ticker)
            for ticker, suspended in zip(
                work["ticker"].tolist(), work["_is_suspended_event"].tolist()
            )
            if not suspended
        }
        carryable = set(
            work.loc[work["_carryable_full_day_s"], "ticker"].astype(str)
        )
        return suspended, resumed, carryable

    def _fundamentals(
        self,
        date: pd.Timestamp,
        daily_basic: pd.DataFrame,
        active_tickers: set[str],
    ) -> pd.DataFrame:
        work = self._normalize_day(daily_basic, date, role="daily_basic")
        required = {"pe_ttm", "pb"}
        missing = sorted(required - set(work.columns))
        if missing:
            raise OpportunitySetDataError(
                f"daily_basic missing required columns: {missing}"
            )
        unknown = sorted(set(work["ticker"].astype(str)) - active_tickers)
        if unknown:
            raise OpportunitySetDataError(
                f"daily_basic contains inactive/unknown securities: {unknown[:10]}"
            )
        result = work.loc[:, ["ticker", "pe_ttm", "pb"]].copy()
        result["ticker"] = result["ticker"].astype(str)
        for column in ("pe_ttm", "pb"):
            source_null = result[column].isna()
            numeric = pd.to_numeric(result[column], errors="coerce")
            numeric_values = numeric.to_numpy(dtype=float, na_value=np.nan)
            result[f"{column}_source_null"] = source_null.to_numpy(dtype=bool)
            result[f"{column}_invalid_non_null"] = (
                ~source_null.to_numpy(dtype=bool)
                & (~np.isfinite(numeric_values) | (numeric_values == 0.0))
            )
            result[column] = numeric_values
        result["daily_basic_row_present"] = True
        return result

    @staticmethod
    def _rank_universe(name: str, members: pd.DataFrame) -> UniverseRanking:
        signal = members.copy().reset_index(drop=True)
        signal["eligible"] = True
        signal["universe_member"] = True
        row_present = signal["daily_basic_row_present"].astype(bool)
        actual_daily_bar = signal["actual_daily_bar"].astype(bool)
        pe_source_null = signal["pe_ttm_source_null"].astype(bool)
        pb_source_null = signal["pb_source_null"].astype(bool)
        invalid_non_null = (
            signal["pe_ttm_invalid_non_null"].astype(bool)
            | signal["pb_invalid_non_null"].astype(bool)
        )
        with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
            expected_control = (1.0 / signal["pe_ttm"]) / signal["pb"]
        expected_finite_score = (
            row_present
            & ~pe_source_null
            & ~pb_source_null
            & ~invalid_non_null
            & np.isfinite(expected_control)
        )
        arithmetic_nonfinite = (
            row_present
            & ~pe_source_null
            & ~pb_source_null
            & ~invalid_non_null
            & ~np.isfinite(expected_control)
        )
        classified_unscoreable = (
            ~row_present
            | pe_source_null
            | pb_source_null
            | invalid_non_null
            | arithmetic_nonfinite
        )
        unclassified_unscoreable = ~(
            expected_finite_score | classified_unscoreable
        )
        scores = fixed_core_score(
            signal[
                [
                    "date",
                    "ticker",
                    "eligible",
                    "universe_member",
                    "earnings_yield",
                    "pb",
                    "book_yield",
                    "volatility_20",
                ]
            ]
        )
        signal["fixed_core_score"] = pd.to_numeric(scores, errors="coerce").to_numpy()
        actual_finite_score = np.isfinite(signal["fixed_core_score"])
        ranked = signal.loc[actual_finite_score].sort_values(
            ["fixed_core_score", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        ).head(RANKING_LIMIT)
        rows = tuple(
            RankedSecurity(
                rank=rank,
                ticker=str(row.ticker),
                fixed_core_score=float(row.fixed_core_score),
                adv20_rmb=float(row.adv20_rmb),
                volatility_20=float(row.volatility_20),
            )
            for rank, row in enumerate(ranked.itertuples(index=False), start=1)
        )
        return UniverseRanking(
            name=name,
            member_count=int(len(signal)),
            finite_score_count=int(actual_finite_score.sum()),
            daily_basic_row_absent_with_daily_bar_count=int(
                ((~row_present) & actual_daily_bar).sum()
            ),
            daily_basic_row_absent_with_proven_no_daily_bar_count=int(
                ((~row_present) & ~actual_daily_bar).sum()
            ),
            pe_ttm_null_count=int((row_present & pe_source_null).sum()),
            pb_null_count=int((row_present & pb_source_null).sum()),
            invalid_non_null_fundamental_count=int(
                (row_present & invalid_non_null).sum()
            ),
            expected_finite_score_count=int(expected_finite_score.sum()),
            unexpected_score_mismatch_count=int(
                (actual_finite_score != expected_finite_score).sum()
            ),
            arithmetic_nonfinite_count=int(arithmetic_nonfinite.sum()),
            classified_unscoreable_count=int(classified_unscoreable.sum()),
            unclassified_unscoreable_count=int(unclassified_unscoreable.sum()),
            top25=rows,
        )

    def push_day(
        self,
        signal_date: Any,
        *,
        daily: pd.DataFrame,
        daily_basic: pd.DataFrame,
        stock_st: pd.DataFrame | None,
        suspensions: pd.DataFrame | None = None,
    ) -> DailyOpportunitySet:
        """Validate and consume exactly one adjacent official session."""

        date = _timestamp(signal_date, field="signal_date")
        if date not in self._calendar_index:
            raise OpportunitySetDataError(
                f"signal_date {date.date()} is not in official_calendar"
            )
        calendar_index = self._calendar_index[date]
        if self._last_calendar_index is not None:
            if calendar_index <= self._last_calendar_index:
                raise OpportunitySetDataError(
                    "input day sequence must be unique and strictly increasing"
                )
            if calendar_index != self._last_calendar_index + 1:
                missing = self._calendar[self._last_calendar_index + 1]
                raise OpportunitySetDataError(
                    "official calendar gap in input stream; expected "
                    f"{missing.date()} before {date.date()}"
                )

        active = self._active_securities(date)
        active_tickers = set(active["ticker"].astype(str))
        st_tickers, inactive_st_ignored = self._stock_st_tickers(
            date, stock_st, active_tickers
        )
        suspended_events, resumed_events, carryable_suspensions = self._suspended_tickers(
            date, suspensions, active_tickers
        )
        carried_tickers = set(self._carried_suspensions) & active_tickers
        suspension_proof = (
            carried_tickers - resumed_events
        ) | carryable_suspensions
        observations, observed_tickers = self._daily_observations(
            date,
            daily,
            active_tickers,
            suspension_proof,
            st_tickers,
        )
        used_carried = sorted(
            (carried_tickers - observed_tickers)
            - resumed_events
            - carryable_suspensions
            - st_tickers
        )
        carried_evidence = tuple(
            CarriedSuspensionEvidence(
                ticker=ticker,
                source_suspend_date=self._carried_suspensions[
                    ticker
                ].date().isoformat(),
                official_session_age=(
                    calendar_index
                    - self._calendar_index[self._carried_suspensions[ticker]]
                    + 1
                ),
            )
            for ticker in used_carried
        )
        fundamentals = self._fundamentals(date, daily_basic, active_tickers)

        candidate_history = [*self._history, observations]
        candidate_history = candidate_history[-ADV_WINDOW_SESSIONS:]
        if candidate_history:
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
            metrics["adv20_rmb"] = (
                metrics["amount_sum_rmb"] / float(ADV_WINDOW_SESSIONS)
            )
            if not np.isfinite(metrics["adv20_rmb"]).all():
                raise OpportunitySetDataError(
                    "rolling ADV20 is non-finite after aggregation"
                )
        else:  # pragma: no cover - a successfully consumed day is never empty here
            metrics = pd.DataFrame(
                columns=("ticker", "observation_count", "adv20_rmb", "volatility_20")
            )

        active["listing_sessions"] = np.maximum(
            calendar_index
            - active["_listing_first_calendar_index"].to_numpy(dtype=np.int64)
            + 1,
            0,
        )
        base = active[["ticker", "listing_sessions"]].merge(
            metrics[["ticker", "observation_count", "adv20_rmb", "volatility_20"]],
            on="ticker",
            how="left",
            validate="one_to_one",
        )
        base = base.loc[
            base["listing_sessions"].ge(MIN_LISTING_SESSIONS)
            & base["observation_count"].eq(ADV_WINDOW_SESSIONS)
            & base["adv20_rmb"].gt(0.0)
            & ~base["ticker"].isin(st_tickers)
        ].copy()
        base["actual_daily_bar"] = base["ticker"].isin(observed_tickers)
        base = base.merge(
            fundamentals,
            on="ticker",
            how="left",
            validate="one_to_one",
        )
        for marker in (
            "daily_basic_row_present",
            "pe_ttm_source_null",
            "pb_source_null",
            "pe_ttm_invalid_non_null",
            "pb_invalid_non_null",
        ):
            base[marker] = base[marker].eq(True)
        base["date"] = date
        with np.errstate(divide="ignore", invalid="ignore"):
            base["earnings_yield"] = 1.0 / base["pe_ttm"]
            base["book_yield"] = 1.0 / base["pb"]
        for column in ("earnings_yield", "book_yield"):
            base[column] = base[column].where(np.isfinite(base[column]))

        adv_order = base.sort_values(
            ["adv20_rmb", "ticker"],
            ascending=[False, True],
            kind="mergesort",
        )
        members_by_name = {
            UNIVERSE_TOP500: adv_order.head(500).copy(),
            UNIVERSE_ADV_GE_100M: adv_order.loc[
                adv_order["adv20_rmb"].ge(MIN_ADV_RMB)
            ].copy(),
            UNIVERSE_TOP1500: adv_order.head(1500).copy(),
        }
        # Deliberately call fixed_core_score independently for every arm.
        rankings = tuple(
            self._rank_universe(name, members_by_name[name])
            for name in self._universe_names
        )
        result = DailyOpportunitySet(
            signal_date=date.date().isoformat(),
            observed_window_sessions=min(
                len(candidate_history), ADV_WINDOW_SESSIONS
            ),
            base_eligible_count=int(len(base)),
            inactive_stock_st_ignored_count=inactive_st_ignored,
            carried_suspension_evidence=carried_evidence,
            universes=rankings,
        )

        # Commit only after every validation and all three rankings succeed.
        self._history.append(observations)
        next_carried = {
            ticker: source_date
            for ticker, source_date in self._carried_suspensions.items()
            if ticker in active_tickers
            and ticker not in observed_tickers
            and ticker not in resumed_events
        }
        for ticker in sorted(
            carryable_suspensions - observed_tickers - resumed_events
        ):
            next_carried.setdefault(ticker, date)
        self._carried_suspensions = next_carried
        self._last_calendar_index = calendar_index
        return result


OpportunitySetBuilder = DailyOpportunitySetBuilder


__all__ = [
    "ADV_WINDOW_SESSIONS",
    "CarriedSuspensionEvidence",
    "DailyOpportunitySet",
    "DailyOpportunitySetBuilder",
    "FORBIDDEN_INPUT_COLUMN_FRAGMENTS",
    "MIN_ADV_RMB",
    "MIN_LISTING_SESSIONS",
    "OpportunitySetBuilder",
    "OpportunitySetDataError",
    "RANKING_LIMIT",
    "RankedSecurity",
    "SecurityAliasInterval",
    "UNIVERSE_ADV_GE_100M",
    "UNIVERSE_NAMES",
    "UNIVERSE_TOP1500",
    "UNIVERSE_TOP500",
    "UniverseRanking",
    "normalize_security_aliases",
]
