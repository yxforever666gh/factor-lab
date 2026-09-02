"""Strict, cost-bounded access to Diemeng historical A-share minutes.

The module deliberately captures one ticker and one explicit time range at a
time.  It does not infer a universal history start, bulk-download the market,
or expose the API key in receipts and exceptions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from math import isfinite
import time
from typing import Any, Callable, Mapping, Protocol

import pandas as pd


DIEMENG_LEVELS = ("1min", "5min", "15min")
DIEMENG_UNIT_CONTRACT_ID = "diemeng-a-share-per-slice-unique-vwap-unit-v5"
DIEMENG_HISTORY_URL = "https://data.diemeng.chat/api/stock/history"
_FORMAL_EXECUTION_WINDOWS = {
    "A": frozenset(f"09:{minute:02d}:00" for minute in range(31, 36)),
    "B": frozenset(f"09:{minute:02d}:00" for minute in range(37, 42)),
    "C": frozenset(f"09:{minute:02d}:00" for minute in range(43, 48)),
}
_FORMAL_EXECUTION_CLOCKS = frozenset().union(
    *_FORMAL_EXECUTION_WINDOWS.values()
)
_UNIT_VWAP_OHLC_TOLERANCE_RMB = 0.01
DIEMENG_RAW_COLUMNS = (
    "stock_code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
)
DIEMENG_MINUTE_COLUMNS = (
    "ticker",
    "trade_time",
    "observable_at",
    "level",
    "open",
    "high",
    "low",
    "close",
    "provider_volume",
    "volume_multiplier_to_shares",
    "volume_shares",
    "provider_amount",
    "amount_multiplier_to_rmb",
    "amount_rmb",
)


class DiemengMinuteDataError(ValueError):
    """Raised when a provider response cannot prove the minute contract."""


class DiemengMinuteTransportError(RuntimeError):
    """Raised for exhausted retryable HTTP failures."""


class DiemengHistoryClient(Protocol):
    def query_history(self, **payload: Any) -> Mapping[str, Any]: ...


def _canonical_sha256(value: Any) -> str:
    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=str,
    )
    return sha256(text.encode("utf-8")).hexdigest()


def _timestamp(value: Any, *, field: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise DiemengMinuteDataError(f"{field} is not a valid timestamp")
    result = pd.Timestamp(parsed)
    if result.tzinfo is not None:
        result = result.tz_convert("Asia/Shanghai").tz_localize(None)
    return result


def _ticker(value: Any) -> str:
    ticker = str(value).strip().upper()
    if not (
        len(ticker) == 9
        and ticker[:6].isdigit()
        and ticker[6:] in {".SH", ".SZ"}
    ):
        raise DiemengMinuteDataError("Diemeng ticker is not canonical")
    return ticker


def _level(value: Any) -> str:
    level = str(value).strip().lower()
    if level not in DIEMENG_LEVELS:
        raise DiemengMinuteDataError("Diemeng minute level is unsupported")
    return level


def _is_formal_one_minute_execution_slice(
    level: str, start: pd.Timestamp, end: pd.Timestamp
) -> bool:
    return bool(
        level == "1min"
        and start.normalize() == end.normalize()
        and start.strftime("%H:%M:%S") == "09:30:00"
        and end.strftime("%H:%M:%S") == "09:47:00"
    )


def normalize_diemeng_minutes(
    rows: pd.DataFrame,
    *,
    ticker: str,
    level: str,
    start_time: Any,
    end_time: Any,
) -> pd.DataFrame:
    """Normalize one exact provider slice and freeze its units.

    Diemeng volume units vary across ticker/date slices.  The only accepted
    conversion is the unique pair from volume multipliers ``{1,100}`` and amount
    multipliers ``{1,1000}`` whose A/B/C aggregate VWAPs lie inside their
    aggregate OHLC ranges.  For the formal 09:30--09:47 slice, individual bars
    still require valid OHLC geometry and volume/amount states, but only the
    three five-minute windows consumed by the strategy validate units.  The
    09:30 price anchor and 09:36/09:42 decision-overlap rows remain raw context.
    No global unit is inferred from another ticker.
    """

    if not isinstance(rows, pd.DataFrame):
        raise TypeError("Diemeng rows must be a pandas DataFrame")
    if tuple(map(str, rows.columns)) != DIEMENG_RAW_COLUMNS:
        raise DiemengMinuteDataError("Diemeng minute columns differ")
    expected_ticker = _ticker(ticker)
    expected_level = _level(level)
    start = _timestamp(start_time, field="start_time")
    end = _timestamp(end_time, field="end_time")
    if end < start:
        raise DiemengMinuteDataError("end_time precedes start_time")
    if rows.empty:
        result = pd.DataFrame(columns=DIEMENG_MINUTE_COLUMNS)
        result["ticker"] = result["ticker"].astype("string")
        result["level"] = result["level"].astype("string")
        result["trade_time"] = pd.to_datetime(result["trade_time"])
        result["observable_at"] = pd.to_datetime(result["observable_at"])
        for column in (
            "open",
            "high",
            "low",
            "close",
            "provider_volume",
            "volume_multiplier_to_shares",
            "volume_shares",
            "provider_amount",
            "amount_multiplier_to_rmb",
            "amount_rmb",
        ):
            result[column] = result[column].astype(float)
        return result

    work = rows.copy()
    work["stock_code"] = work["stock_code"].astype("string").str.strip().str.upper()
    if not work["stock_code"].eq(expected_ticker).all():
        raise DiemengMinuteDataError("Diemeng response contains another ticker")
    times = pd.to_datetime(work["trade_time"], errors="coerce")
    if times.isna().any():
        raise DiemengMinuteDataError("Diemeng response contains invalid trade_time")
    if getattr(times.dt, "tz", None) is not None:
        times = times.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    if not times.between(start, end, inclusive="both").all():
        raise DiemengMinuteDataError("Diemeng response escapes requested time range")
    if times.dt.second.ne(0).any() or times.dt.microsecond.ne(0).any():
        raise DiemengMinuteDataError("Diemeng minute timestamp is off-grid")
    minute_step = int(expected_level[:-3])
    if expected_level != "1min" and times.dt.minute.mod(minute_step).ne(0).any():
        raise DiemengMinuteDataError("Diemeng minute timestamp has wrong level grid")

    numeric_columns = ("open", "high", "low", "close", "vol", "amount")
    for column in numeric_columns:
        numeric = pd.to_numeric(work[column], errors="coerce").astype(float)
        if not numeric.map(isfinite).all():
            raise DiemengMinuteDataError(
                f"Diemeng response contains invalid {column}"
            )
        work[column] = numeric
    if work[["open", "high", "low", "close"]].le(0.0).any().any():
        raise DiemengMinuteDataError("Diemeng prices must be positive")
    if work[["vol", "amount"]].lt(0.0).any().any():
        raise DiemengMinuteDataError("Diemeng volume/amount cannot be negative")
    if (
        work["high"].lt(work[["open", "close", "low"]].max(axis=1)).any()
        or work["low"].gt(work[["open", "close", "high"]].min(axis=1)).any()
    ):
        raise DiemengMinuteDataError("Diemeng OHLC geometry differs")
    clocks = times.dt.strftime("%H:%M:%S")
    formal_execution_slice = _is_formal_one_minute_execution_slice(
        expected_level, start, end
    )
    if formal_execution_slice:
        for window_clocks in _FORMAL_EXECUTION_WINDOWS.values():
            observed = clocks.isin(window_clocks)
            observed_clocks = set(clocks.loc[observed])
            if observed_clocks and (
                int(observed.sum()) != 5
                or observed_clocks != set(window_clocks)
            ):
                raise DiemengMinuteDataError(
                    "Diemeng execution window is partial"
                )
        for context_clock in ("09:30:00", "09:36:00", "09:42:00"):
            if int(clocks.eq(context_clock).sum()) > 1:
                raise DiemengMinuteDataError(
                    "Diemeng execution context clock is duplicate"
                )
    if work["vol"].eq(0.0).ne(work["amount"].eq(0.0)).any():
        raise DiemengMinuteDataError("Diemeng zero volume/amount state differs")
    unit_rows = (
        clocks.isin(_FORMAL_EXECUTION_CLOCKS)
        if formal_execution_slice
        else ~clocks.eq("09:30:00")
        if expected_level == "1min"
        else pd.Series(True, index=work.index, dtype=bool)
    )
    positive_volume = unit_rows & work["vol"].gt(0.0)
    unit_candidates: list[tuple[float, float]] = []
    if positive_volume.any():
        for volume_multiplier in (1.0, 100.0):
            for amount_multiplier in (1.0, 1000.0):
                valid = True
                if formal_execution_slice:
                    for window_clocks in _FORMAL_EXECUTION_WINDOWS.values():
                        window = clocks.isin(window_clocks)
                        provider_volume = float(work.loc[window, "vol"].sum())
                        if provider_volume <= 0.0:
                            continue
                        vwap = (
                            float(work.loc[window, "amount"].sum())
                            * amount_multiplier
                            / (provider_volume * volume_multiplier)
                        )
                        low = float(work.loc[window, "low"].min())
                        high = float(work.loc[window, "high"].max())
                        if (
                            vwap
                            < low - _UNIT_VWAP_OHLC_TOLERANCE_RMB - 1e-12
                            or vwap
                            > high + _UNIT_VWAP_OHLC_TOLERANCE_RMB + 1e-12
                        ):
                            valid = False
                            break
                else:
                    vwap = (
                        work.loc[positive_volume, "amount"]
                        * amount_multiplier
                    ) / (
                        work.loc[positive_volume, "vol"]
                        * volume_multiplier
                    )
                    valid = bool(
                        not vwap.lt(
                            work.loc[positive_volume, "low"]
                            - _UNIT_VWAP_OHLC_TOLERANCE_RMB
                            - 1e-12
                        ).any()
                        and not vwap.gt(
                            work.loc[positive_volume, "high"]
                            + _UNIT_VWAP_OHLC_TOLERANCE_RMB
                            + 1e-12
                        ).any()
                    )
                if valid:
                    unit_candidates.append(
                        (volume_multiplier, amount_multiplier)
                    )
        if len(unit_candidates) != 1:
            raise DiemengMinuteDataError(
                "Diemeng slice does not have one unique volume/amount unit"
            )
        volume_multiplier, amount_multiplier = unit_candidates[0]
    else:
        volume_multiplier = float("nan")
        amount_multiplier = float("nan")

    result = pd.DataFrame(
        {
            "ticker": expected_ticker,
            "trade_time": times,
            "observable_at": times + pd.Timedelta(minutes=1),
            "level": expected_level,
            "open": work["open"],
            "high": work["high"],
            "low": work["low"],
            "close": work["close"],
            "provider_volume": work["vol"],
            "volume_multiplier_to_shares": volume_multiplier,
            "volume_shares": work["vol"] * volume_multiplier,
            "provider_amount": work["amount"],
            "amount_multiplier_to_rmb": amount_multiplier,
            "amount_rmb": work["amount"] * amount_multiplier,
        },
        columns=DIEMENG_MINUTE_COLUMNS,
    )
    result = result.sort_values(
        ["trade_time", "ticker"], kind="mergesort"
    ).reset_index(drop=True)
    if result.duplicated(["ticker", "trade_time"]).any():
        raise DiemengMinuteDataError("Diemeng response contains duplicate bars")
    return result


@dataclass(frozen=True)
class DiemengMinuteCapture:
    frame: pd.DataFrame
    receipt: dict[str, Any]


def capture_diemeng_minutes(
    client: DiemengHistoryClient,
    *,
    ticker: str,
    level: str,
    start_time: Any,
    end_time: Any,
    page_size: int = 10_000,
    maximum_pages: int = 100_000,
    require_nonempty: bool = True,
) -> DiemengMinuteCapture:
    """Fetch every page exactly once and return a deterministic receipt."""

    expected_ticker = _ticker(ticker)
    expected_level = _level(level)
    start = _timestamp(start_time, field="start_time")
    end = _timestamp(end_time, field="end_time")
    if end < start:
        raise DiemengMinuteDataError("end_time precedes start_time")
    if isinstance(page_size, bool) or not 1 <= int(page_size) <= 10_000:
        raise DiemengMinuteDataError("page_size must be in [1,10000]")
    if isinstance(maximum_pages, bool) or int(maximum_pages) <= 0:
        raise DiemengMinuteDataError("maximum_pages must be positive")
    page_size = int(page_size)
    expected_total: int | None = None
    pages: list[pd.DataFrame] = []
    page_receipts: list[dict[str, Any]] = []
    page = 0
    while True:
        if page >= int(maximum_pages):
            raise DiemengMinuteDataError("Diemeng pagination exceeded maximum_pages")
        response = client.query_history(
            stock_code=expected_ticker,
            level=expected_level,
            start_time=start.strftime("%Y-%m-%d %H:%M:%S"),
            end_time=end.strftime("%Y-%m-%d %H:%M:%S"),
            page=page,
            page_size=page_size,
        )
        if not isinstance(response, Mapping) or response.get("code") != 200:
            raise DiemengMinuteDataError("Diemeng response code differs")
        data = response.get("data")
        if not isinstance(data, Mapping):
            raise DiemengMinuteDataError("Diemeng response lacks data mapping")
        total = data.get("total")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise DiemengMinuteDataError("Diemeng response total is invalid")
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise DiemengMinuteDataError("Diemeng total changed during pagination")
        raw_rows = data.get("list")
        if not isinstance(raw_rows, list):
            raise DiemengMinuteDataError("Diemeng response list differs")
        if len(raw_rows) > page_size:
            raise DiemengMinuteDataError("Diemeng page exceeded page_size")
        if not raw_rows:
            if sum(len(value) for value in pages) != expected_total:
                raise DiemengMinuteDataError(
                    "Diemeng pagination ended before declared total"
                )
            page_receipts.append(
                {
                    "page": page,
                    "row_count": 0,
                    "payload_sha256": _canonical_sha256([]),
                    "sentinel": True,
                }
            )
            break
        raw = pd.DataFrame(raw_rows)
        if tuple(map(str, raw.columns)) != DIEMENG_RAW_COLUMNS:
            raise DiemengMinuteDataError("Diemeng minute columns differ")
        pages.append(raw.loc[:, DIEMENG_RAW_COLUMNS])
        page_receipts.append(
            {
                "page": page,
                "row_count": len(raw_rows),
                "payload_sha256": _canonical_sha256(raw_rows),
                "sentinel": False,
            }
        )
        captured = sum(len(value) for value in pages)
        if captured > expected_total:
            raise DiemengMinuteDataError("Diemeng pages exceed declared total")
        if captured < expected_total and len(raw_rows) != page_size:
            raise DiemengMinuteDataError(
                "Diemeng nonterminal page is shorter than page_size"
            )
        page += 1
    assert expected_total is not None
    raw_frame = (
        pd.concat(pages, ignore_index=True)
        if pages
        else pd.DataFrame(columns=DIEMENG_RAW_COLUMNS)
    )
    frame = normalize_diemeng_minutes(
        raw_frame,
        ticker=expected_ticker,
        level=expected_level,
        start_time=start,
        end_time=end,
    )
    if len(frame) != expected_total:
        raise DiemengMinuteDataError("Diemeng canonical row count differs from total")
    if require_nonempty and frame.empty:
        raise DiemengMinuteDataError("required Diemeng minute slice is empty")
    payload = frame.copy()
    payload["trade_time"] = payload["trade_time"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    payload["observable_at"] = payload["observable_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    payload = payload.astype(object).where(pd.notna(payload), None)
    request_identity = {
        "stock_code": expected_ticker,
        "level": expected_level,
        "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
        "page_origin": 0,
        "page_size": page_size,
    }
    volume_multipliers = sorted(
        set(
            float(value)
            for value in frame["volume_multiplier_to_shares"].dropna()
        )
    )
    amount_multipliers = sorted(
        set(
            float(value)
            for value in frame["amount_multiplier_to_rmb"].dropna()
        )
    )
    if len(volume_multipliers) > 1 or len(amount_multipliers) > 1:
        raise DiemengMinuteDataError("Diemeng canonical slice units are unstable")
    receipt = {
        "schema_version": 1,
        "provider": "diemeng",
        "historical_vintage_class": "reconstructed_from_current_provider_history",
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "ticker": expected_ticker,
        "level": expected_level,
        "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
        "page_size": page_size,
        "page_count": len(pages),
        "request_count": len(page_receipts),
        "page_origin": 0,
        "sentinel_empty_page_verified": True,
        "request_payload_sha256": _canonical_sha256(request_identity),
        "pages": page_receipts,
        "row_count": len(frame),
        "provider_total": expected_total,
        "provider_volume_unit": (
            "shares"
            if volume_multipliers == [1.0]
            else "hands"
            if volume_multipliers == [100.0]
            else "not_inferable_zero_liquidity"
        ),
        "volume_multiplier_to_shares": (
            volume_multipliers[0] if volume_multipliers else None
        ),
        "canonical_volume_unit": "shares",
        "provider_amount_unit": (
            "RMB"
            if amount_multipliers == [1.0]
            else "thousand_RMB"
            if amount_multipliers == [1000.0]
            else "not_inferable_zero_liquidity"
        ),
        "amount_multiplier_to_rmb": (
            amount_multipliers[0] if amount_multipliers else None
        ),
        "canonical_amount_unit": "RMB",
        "unit_contract_id": DIEMENG_UNIT_CONTRACT_ID,
        "unit_inference_scope": (
            "formal_1min_A_B_C_five_minute_consumed_window_aggregates"
            if _is_formal_one_minute_execution_slice(
                expected_level, start, end
            )
            else "positive_volume_bars_except_exact_1min_09:30_price_anchor"
        ),
        "observable_at_rule": "trade_time_plus_one_minute",
        "payload_sha256": _canonical_sha256(payload.to_dict("records")),
    }
    return DiemengMinuteCapture(frame=frame, receipt=receipt)


def first_five_minute_audit_bar(
    frame: pd.DataFrame,
    *,
    ticker: str,
    session: Any,
    daily_open: float,
    level: str = "5min",
) -> dict[str, float | str]:
    """Validate the first interval without claiming exact-open auction fill."""

    expected_level = _level(level)
    if expected_level != "5min":
        raise DiemengMinuteDataError("13.0 execution bar is frozen at 5min")
    if tuple(map(str, frame.columns)) != DIEMENG_MINUTE_COLUMNS:
        raise DiemengMinuteDataError("canonical Diemeng minute columns differ")
    expected_ticker = _ticker(ticker)
    date = _timestamp(session, field="session").normalize()
    rows = frame.loc[
        frame["trade_time"].dt.normalize().eq(date)
        & frame["level"].eq(expected_level)
        & frame["ticker"].eq(expected_ticker)
    ].sort_values("trade_time", kind="mergesort")
    if rows.empty or rows.iloc[0]["trade_time"] != date + pd.Timedelta(
        hours=9, minutes=35
    ):
        raise DiemengMinuteDataError("Diemeng first 5min execution bar is absent")
    row = rows.iloc[0]
    opening = float(daily_open)
    if not isfinite(opening) or opening <= 0.0:
        raise DiemengMinuteDataError("daily open is invalid")
    if abs(float(row["open"]) - opening) > 0.005 + 1e-12:
        raise DiemengMinuteDataError("Diemeng first bar open differs from daily open")
    volume = float(row["volume_shares"])
    amount = float(row["amount_rmb"])
    if volume <= 0.0 or amount <= 0.0:
        raise DiemengMinuteDataError("Diemeng first execution bar lacks liquidity")
    vwap = amount / volume
    if not float(row["low"]) - 0.005 <= vwap <= float(row["high"]) + 0.005:
        raise DiemengMinuteDataError("Diemeng first-bar VWAP escapes OHLC")
    return {
        "ticker": str(row["ticker"]),
        "trade_time": row["trade_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "observable_at": row["observable_at"].strftime("%Y-%m-%d %H:%M:%S"),
        "open": float(row["open"]),
        "vwap": vwap,
        "volume_shares": volume,
        "amount_rmb": amount,
    }


def audit_diemeng_full_day(
    frame: pd.DataFrame,
    *,
    ticker: str,
    session: Any,
    daily_open: float,
    daily_volume_shares: float,
    daily_amount_rmb: float,
    relative_tolerance: float = 0.001,
) -> dict[str, Any]:
    """Prove one normal full-day 5-minute slice against canonical daily data."""

    expected_ticker = _ticker(ticker)
    date = _timestamp(session, field="session").normalize()
    tolerance = float(relative_tolerance)
    if not isfinite(tolerance) or not 0.0 < tolerance <= 0.01:
        raise ValueError("relative_tolerance must be in (0,0.01]")
    if tuple(map(str, frame.columns)) != DIEMENG_MINUTE_COLUMNS:
        raise DiemengMinuteDataError("canonical Diemeng minute columns differ")
    rows = frame.loc[
        frame["ticker"].eq(expected_ticker)
        & frame["level"].eq("5min")
        & frame["trade_time"].dt.normalize().eq(date)
    ].sort_values("trade_time", kind="mergesort")
    if (
        len(rows) != 48
        or rows.iloc[0]["trade_time"]
        != date + pd.Timedelta(hours=9, minutes=35)
        or rows.iloc[-1]["trade_time"]
        != date + pd.Timedelta(hours=15)
    ):
        raise DiemengMinuteDataError(
            "Diemeng calibration day is not a complete 48-bar session"
        )
    first = first_five_minute_audit_bar(
        rows,
        ticker=expected_ticker,
        session=date,
        daily_open=daily_open,
    )
    expected_volume = float(daily_volume_shares)
    expected_amount = float(daily_amount_rmb)
    if not all(
        isfinite(value) and value > 0.0
        for value in (expected_volume, expected_amount)
    ):
        raise DiemengMinuteDataError("daily calibration totals are invalid")
    volume_ratio = float(rows["volume_shares"].sum()) / expected_volume
    amount_ratio = float(rows["amount_rmb"].sum()) / expected_amount
    if (
        abs(volume_ratio - 1.0) > tolerance
        or abs(amount_ratio - 1.0) > tolerance
    ):
        raise DiemengMinuteDataError(
            "Diemeng minute totals do not reconcile to canonical daily totals"
        )
    identity = rows.copy()
    identity["trade_time"] = identity["trade_time"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    identity["observable_at"] = identity["observable_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return {
        "ticker": expected_ticker,
        "session": date.date().isoformat(),
        "exchange": expected_ticker[-2:],
        "year": int(date.year),
        "row_count": len(rows),
        "first_trade_time": first["trade_time"],
        "last_trade_time": rows.iloc[-1]["trade_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "daily_open_exact": float(first["open"]) == float(daily_open),
        "volume_ratio": volume_ratio,
        "amount_ratio": amount_ratio,
        "volume_multiplier_to_shares": float(
            rows["volume_multiplier_to_shares"].iloc[0]
        ),
        "amount_multiplier_to_rmb": float(
            rows["amount_multiplier_to_rmb"].iloc[0]
        ),
        "relative_tolerance": tolerance,
        "minute_payload_sha256": _canonical_sha256(identity.to_dict("records")),
    }


def audit_diemeng_one_minute_day(
    frame: pd.DataFrame,
    *,
    ticker: str,
    session: Any,
    daily_open: float,
    daily_volume_shares: float,
    daily_amount_rmb: float,
    relative_tolerance: float = 0.01,
) -> dict[str, Any]:
    """Calibrate one 1min day while explicitly excluding its 09:30 auction bar."""

    expected_ticker = _ticker(ticker)
    date = _timestamp(session, field="session").normalize()
    tolerance = float(relative_tolerance)
    if not isfinite(tolerance) or not 0.0 < tolerance <= 0.02:
        raise ValueError("relative_tolerance must be in (0,0.02]")
    if tuple(map(str, frame.columns)) != DIEMENG_MINUTE_COLUMNS:
        raise DiemengMinuteDataError("canonical Diemeng minute columns differ")
    rows = frame.loc[
        frame["ticker"].eq(expected_ticker)
        & frame["level"].eq("1min")
        & frame["trade_time"].dt.normalize().eq(date)
    ].sort_values("trade_time", kind="mergesort")
    required_execution_clocks = {
        f"09:{minute:02d}:00" for minute in range(31, 48)
    }
    clocks = set(rows["trade_time"].dt.strftime("%H:%M:%S"))
    if (
        len(rows) < 200
        or rows.iloc[0]["trade_time"] != date + pd.Timedelta(hours=9, minutes=30)
        or rows.iloc[-1]["trade_time"] != date + pd.Timedelta(hours=15)
        or not required_execution_clocks.issubset(clocks)
    ):
        raise DiemengMinuteDataError("Diemeng 1min calibration day is incomplete")
    opening = float(daily_open)
    if not isfinite(opening) or abs(float(rows.iloc[0]["open"]) - opening) > 0.005 + 1e-12:
        raise DiemengMinuteDataError("Diemeng 09:30 auction anchor differs")
    expected_volume = float(daily_volume_shares)
    expected_amount = float(daily_amount_rmb)
    volume_ratio = float(rows["volume_shares"].sum()) / expected_volume
    amount_ratio = float(rows["amount_rmb"].sum()) / expected_amount
    if (
        abs(volume_ratio - 1.0) > tolerance
        or abs(amount_ratio - 1.0) > tolerance
    ):
        raise DiemengMinuteDataError("Diemeng 1min totals differ from daily")
    identity = rows.copy()
    identity["trade_time"] = identity["trade_time"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    identity["observable_at"] = identity["observable_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return {
        "ticker": expected_ticker,
        "session": date.date().isoformat(),
        "exchange": expected_ticker[-2:],
        "year": int(date.year),
        "level": "1min",
        "row_count": len(rows),
        "first_trade_time": rows.iloc[0]["trade_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "last_trade_time": rows.iloc[-1]["trade_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "daily_open_exact": abs(float(rows.iloc[0]["open"]) - opening) <= 1e-12,
        "auction_0930_excluded_from_execution": True,
        "execution_0931_0947_complete": True,
        "volume_ratio": volume_ratio,
        "amount_ratio": amount_ratio,
        "volume_multiplier_to_shares": float(
            rows["volume_multiplier_to_shares"].iloc[0]
        ),
        "amount_multiplier_to_rmb": float(
            rows["amount_multiplier_to_rmb"].iloc[0]
        ),
        "relative_tolerance": tolerance,
        "minute_payload_sha256": _canonical_sha256(identity.to_dict("records")),
    }


def audit_diemeng_execution_slice(
    frame: pd.DataFrame,
    *,
    ticker: str,
    session: Any,
    daily_open: float,
) -> dict[str, Any]:
    """Freeze the exact 09:30 anchor plus 09:31..09:47 execution slice."""

    expected_ticker = _ticker(ticker)
    date = _timestamp(session, field="session").normalize()
    if tuple(map(str, frame.columns)) != DIEMENG_MINUTE_COLUMNS:
        raise DiemengMinuteDataError("canonical Diemeng minute columns differ")
    rows = frame.loc[
        frame["ticker"].eq(expected_ticker)
        & frame["level"].eq("1min")
        & frame["trade_time"].dt.normalize().eq(date)
    ].sort_values("trade_time", kind="mergesort")
    expected_times = tuple(
        date + pd.Timedelta(hours=9, minutes=minute)
        for minute in range(30, 48)
    )
    if tuple(rows["trade_time"]) != expected_times:
        raise DiemengMinuteDataError("Diemeng execution slice is incomplete")
    opening = float(daily_open)
    if not isfinite(opening) or abs(float(rows.iloc[0]["open"]) - opening) > 0.005 + 1e-12:
        raise DiemengMinuteDataError("Diemeng execution auction anchor differs")
    window_aggregates = {}
    clocks = rows["trade_time"].dt.strftime("%H:%M:%S")
    for name, window_clocks in _FORMAL_EXECUTION_WINDOWS.items():
        window = rows.loc[clocks.isin(window_clocks)]
        volume = float(window["volume_shares"].sum())
        amount = float(window["amount_rmb"].sum())
        window_aggregates[name] = {
            "bar_count": len(window),
            "first_trade_time": window.iloc[0]["trade_time"].strftime(
                "%H:%M:%S"
            ),
            "last_trade_time": window.iloc[-1]["trade_time"].strftime(
                "%H:%M:%S"
            ),
            "low": float(window["low"].min()),
            "high": float(window["high"].max()),
            "volume_shares": volume,
            "amount_rmb": amount,
            "vwap": amount / volume if volume > 0.0 else None,
        }
    identity = rows.copy()
    identity["trade_time"] = identity["trade_time"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    identity["observable_at"] = identity["observable_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return {
        "ticker": expected_ticker,
        "session": date.date().isoformat(),
        "exchange": expected_ticker[-2:],
        "year": int(date.year),
        "level": "1min",
        "row_count": len(rows),
        "first_trade_time": rows.iloc[0]["trade_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "last_trade_time": rows.iloc[-1]["trade_time"].strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "daily_open_exact": abs(float(rows.iloc[0]["open"]) - opening) <= 1e-12,
        "auction_0930_excluded_from_execution": True,
        "execution_0931_0947_complete": True,
        "execution_window_aggregates": window_aggregates,
        "volume_multiplier_to_shares": float(
            rows["volume_multiplier_to_shares"].iloc[0]
        ),
        "amount_multiplier_to_rmb": float(
            rows["amount_multiplier_to_rmb"].iloc[0]
        ),
        "minute_payload_sha256": _canonical_sha256(identity.to_dict("records")),
    }


def freeze_diemeng_unit_contract(
    audits: list[Mapping[str, Any]],
) -> dict[str, Any]:
    """Freeze a cross-exchange/multi-year unit and bar-label calibration."""

    if len(audits) < 8:
        raise DiemengMinuteDataError("Diemeng unit calibration needs 8 samples")
    normalized = [dict(value) for value in audits]
    tickers = [str(value.get("ticker")) for value in normalized]
    if len(set((value.get("ticker"), value.get("session")) for value in normalized)) != len(
        normalized
    ):
        raise DiemengMinuteDataError("Diemeng calibration samples are duplicate")
    if {ticker[-2:] for ticker in tickers} != {"SH", "SZ"}:
        raise DiemengMinuteDataError("Diemeng calibration lacks both exchanges")
    if len({int(value.get("year")) for value in normalized}) < 4:
        raise DiemengMinuteDataError("Diemeng calibration lacks four years")
    for value in normalized:
        if (
            int(value.get("row_count", 0)) != 18
            or value.get("level") != "1min"
            or not str(value.get("first_trade_time", "")).endswith("09:30:00")
            or not str(value.get("last_trade_time", "")).endswith("09:47:00")
            or value.get("daily_open_exact") is not True
            or value.get("auction_0930_excluded_from_execution") is not True
            or value.get("execution_0931_0947_complete") is not True
        ):
            raise DiemengMinuteDataError("Diemeng calibration sample differs")
        if float(value.get("volume_multiplier_to_shares")) not in {1.0, 100.0}:
            raise DiemengMinuteDataError("Diemeng calibration volume unit differs")
        if float(value.get("amount_multiplier_to_rmb")) not in {1.0, 1000.0}:
            raise DiemengMinuteDataError("Diemeng calibration amount unit differs")
        aggregates = value.get("execution_window_aggregates")
        if not isinstance(aggregates, Mapping) or set(aggregates) != {
            "A",
            "B",
            "C",
        }:
            raise DiemengMinuteDataError("Diemeng calibration windows differ")
        for window in aggregates.values():
            if int(window.get("bar_count", 0)) != 5:
                raise DiemengMinuteDataError(
                    "Diemeng calibration window bar count differs"
                )
            volume = float(window.get("volume_shares", 0.0))
            amount = float(window.get("amount_rmb", 0.0))
            if volume > 0.0:
                vwap = float(window.get("vwap"))
                if (
                    vwap
                    < float(window.get("low"))
                    - _UNIT_VWAP_OHLC_TOLERANCE_RMB
                    - 1e-12
                    or vwap
                    > float(window.get("high"))
                    + _UNIT_VWAP_OHLC_TOLERANCE_RMB
                    + 1e-12
                ):
                    raise DiemengMinuteDataError(
                        "Diemeng calibration aggregate VWAP differs"
                    )
            elif amount != 0.0 or window.get("vwap") is not None:
                raise DiemengMinuteDataError(
                    "Diemeng calibration empty window differs"
                )
    volume_multipliers = sorted(
        {float(value["volume_multiplier_to_shares"]) for value in normalized}
    )
    amount_multipliers = sorted(
        {float(value["amount_multiplier_to_rmb"]) for value in normalized}
    )
    if volume_multipliers != [1.0, 100.0]:
        raise DiemengMinuteDataError(
            "Diemeng calibration did not expose both volume regimes"
        )
    normalized.sort(key=lambda value: (str(value["session"]), str(value["ticker"])))
    contract = {
        "schema_version": 1,
        "contract_id": DIEMENG_UNIT_CONTRACT_ID,
        "unit_scope": "infer_uniquely_per_ticker_date_slice",
        "allowed_volume_multipliers_to_shares": volume_multipliers,
        "allowed_amount_multipliers_to_rmb": amount_multipliers,
        "bar_level": "1min",
        "bar_label": "interval_end",
        "auction_anchor_time": "09:30:00",
        "auction_anchor_excluded_from_execution": True,
        "auction_anchor_excluded_from_unit_inference_and_validation": True,
        "decision_overlap_clocks_excluded_from_unit_inference_and_validation": [
            "09:36:00",
            "09:42:00",
        ],
        "unit_inference_and_validation_clocks": sorted(
            _FORMAL_EXECUTION_CLOCKS
        ),
        "unit_inference_and_validation_windows": {
            name: sorted(clocks)
            for name, clocks in _FORMAL_EXECUTION_WINDOWS.items()
        },
        "unit_validation_granularity": "consumed_five_minute_window_aggregate",
        "aggregate_vwap_ohlc_tolerance_rmb": (
            _UNIT_VWAP_OHLC_TOLERANCE_RMB
        ),
        "execution_capture_range": "09:31:00 through 09:47:00",
        "observable_at_rule": "trade_time_plus_one_minute",
        "sample_count": len(normalized),
        "samples": normalized,
    }
    contract["payload_sha256"] = _canonical_sha256(contract)
    return contract


class DiemengMinuteHTTPClient:
    """Minimal paced/retrying HTTP transport with secret-safe errors."""

    def __init__(
        self,
        api_key: str,
        *,
        request_rate_per_minute: float = 60.0,
        timeout_seconds: float = 30.0,
        post_fn: Callable[..., Any] | None = None,
        monotonic_fn: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
    ) -> None:
        key = str(api_key).strip()
        if not key:
            raise ValueError("Diemeng api_key is empty")
        rate = float(request_rate_per_minute)
        timeout = float(timeout_seconds)
        if not isfinite(rate) or rate <= 0.0:
            raise ValueError("Diemeng request rate must be positive/finite")
        if not isfinite(timeout) or timeout <= 0.0:
            raise ValueError("Diemeng timeout must be positive/finite")
        if post_fn is None:
            try:
                import requests
            except ImportError as exc:
                raise RuntimeError(
                    "Diemeng HTTP access requires the optional data dependency"
                ) from exc
            session = requests.Session()
            session.trust_env = False
            post_fn = session.post
            self._session = session
        else:
            self._session = None
        self._api_key = key
        self._url = DIEMENG_HISTORY_URL
        self._minimum_interval = 60.0 / rate
        self._timeout = timeout
        self._post = post_fn
        self._monotonic = monotonic_fn
        self._sleep = sleep_fn
        self._last_attempt: float | None = None

    def _pace(self) -> None:
        if self._last_attempt is not None:
            remaining = self._minimum_interval - (
                float(self._monotonic()) - self._last_attempt
            )
            if remaining > 0.0:
                self._sleep(remaining)
        self._last_attempt = float(self._monotonic())

    def query_history(self, **payload: Any) -> Mapping[str, Any]:
        if set(payload) != {
            "stock_code",
            "level",
            "start_time",
            "end_time",
            "page",
            "page_size",
        }:
            raise DiemengMinuteDataError("Diemeng request payload fields differ")
        for attempt in range(3):
            self._pace()
            try:
                response = self._post(
                    self._url,
                    headers={
                        "apiKey": self._api_key,
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=self._timeout,
                    allow_redirects=False,
                )
                status = int(getattr(response, "status_code", 0))
                if status == 429 or 500 <= status <= 599:
                    raise DiemengMinuteTransportError(
                        f"temporary Diemeng HTTP status {status}"
                    )
                if status in {401, 403}:
                    raise PermissionError(
                        f"Diemeng permission denied with HTTP status {status}"
                    )
                if status != 200:
                    raise DiemengMinuteDataError(
                        f"Diemeng HTTP status differs: {status}"
                    )
                value = response.json()
                if not isinstance(value, Mapping):
                    raise DiemengMinuteDataError("Diemeng JSON root differs")
                return value
            except PermissionError as exc:
                raise exc from None
            except DiemengMinuteDataError as exc:
                raise exc from None
            except Exception as exc:
                retryable = isinstance(
                    exc, (TimeoutError, ConnectionError, DiemengMinuteTransportError)
                ) or any(
                    marker in type(exc).__name__.lower()
                    for marker in ("timeout", "connection")
                )
                if not retryable or attempt == 2:
                    if isinstance(exc, DiemengMinuteTransportError):
                        raise exc from None
                    raise DiemengMinuteTransportError(
                        "Diemeng transport failed without exposing credentials"
                    ) from None
                self._sleep((1.0, 2.0)[attempt])
        raise AssertionError("unreachable Diemeng retry state")


__all__ = [
    "DIEMENG_LEVELS",
    "DIEMENG_HISTORY_URL",
    "DIEMENG_MINUTE_COLUMNS",
    "DIEMENG_RAW_COLUMNS",
    "DIEMENG_UNIT_CONTRACT_ID",
    "DiemengMinuteCapture",
    "DiemengMinuteDataError",
    "DiemengMinuteHTTPClient",
    "DiemengMinuteTransportError",
    "capture_diemeng_minutes",
    "audit_diemeng_execution_slice",
    "audit_diemeng_full_day",
    "audit_diemeng_one_minute_day",
    "first_five_minute_audit_bar",
    "freeze_diemeng_unit_contract",
    "normalize_diemeng_minutes",
]
