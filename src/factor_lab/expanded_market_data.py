from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


DAILY_RAW_DATASETS = ("daily", "daily_basic", "adj_factor")
STOCK_LIST_STATUSES = ("L", "P", "D")
RAW_CHECKPOINT_SCHEMA_VERSION = 1
SHA256_MANIFEST_SCHEMA_VERSION = 1

_DAILY_FIELDS = {
    "daily": "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
    "daily_basic": (
        "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,"
        "dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
    ),
    "adj_factor": "ts_code,trade_date,adj_factor",
}

_STOCK_BASIC_FIELDS = (
    "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,"
    "curr_type,list_status,list_date,delist_date,is_hs"
)


@dataclass
class HistoricalSTSnapshot:
    """Historical ST-name intervals and whether the optional source was usable."""

    records: pd.DataFrame
    available: bool
    degraded: bool
    reason: str | None = None


@dataclass
class MonthlyMembershipResult:
    """PIT monthly universe membership plus selection diagnostics."""

    membership: pd.DataFrame
    audit: dict[str, Any]


def _as_iso_date(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError(f"invalid date: {value!r}")
    return timestamp.normalize().strftime("%Y-%m-%d")


def _compact_date(value: Any) -> str:
    return pd.Timestamp(value).strftime("%Y%m%d")


def normalize_trade_calendar(calendar: pd.DataFrame | Sequence[Any]) -> pd.DatetimeIndex:
    """Return sorted, unique open trading dates from a frame or a date sequence."""

    if isinstance(calendar, pd.DataFrame):
        date_column = next(
            (column for column in ("cal_date", "trade_date", "date") if column in calendar.columns),
            None,
        )
        if date_column is None:
            raise KeyError("trade calendar requires cal_date, trade_date, or date")
        frame = calendar
        if "is_open" in frame.columns:
            open_flag = pd.to_numeric(frame["is_open"], errors="coerce").eq(1)
            open_flag |= frame["is_open"].astype(str).str.lower().eq("true")
            frame = frame.loc[open_flag]
        values = frame[date_column]
    else:
        values = pd.Series(list(calendar), dtype="object")

    parsed = pd.to_datetime(values, errors="coerce")
    dates = pd.DatetimeIndex(parsed[~pd.isna(parsed)]).normalize().unique().sort_values()
    if dates.empty:
        raise ValueError("trade calendar contains no open dates")
    return dates


def calculate_warmup_start_date(
    calendar: pd.DataFrame | Sequence[Any],
    analysis_start: Any,
    *,
    warmup_sessions: int = 120,
) -> str:
    """Return the date that contributes exactly ``warmup_sessions`` prior sessions.

    ``analysis_start`` may be a non-trading day; the first open date on or after it
    is treated as the first analysis session.  Insufficient history is an error so
    a caller cannot silently run a shorter warm-up.
    """

    if warmup_sessions < 0:
        raise ValueError("warmup_sessions must be non-negative")
    dates = normalize_trade_calendar(calendar)
    requested = pd.Timestamp(analysis_start).normalize()
    positions = np.flatnonzero(dates >= requested)
    if not len(positions):
        raise ValueError("analysis_start is after the supplied trade calendar")
    first_position = int(positions[0])
    if first_position < warmup_sessions:
        raise ValueError(
            f"trade calendar has only {first_position} sessions before analysis_start; "
            f"{warmup_sessions} required"
        )
    return dates[first_position - warmup_sessions].strftime("%Y-%m-%d")


def raw_partition_key(dataset: str, trade_date: Any) -> str:
    if dataset not in DAILY_RAW_DATASETS:
        raise ValueError(f"unsupported daily raw dataset: {dataset}")
    return f"{dataset}/{_as_iso_date(trade_date)}"


def raw_partition_path(raw_root: str | Path, dataset: str, trade_date: Any) -> Path:
    """Return the deterministic Hive-style path for one full-market day."""

    date_text = _as_iso_date(trade_date)
    raw_partition_key(dataset, date_text)
    return Path(raw_root) / dataset / f"trade_date={date_text}" / "part-000.parquet"


def plan_daily_raw_partitions(
    trading_dates: pd.DataFrame | Sequence[Any],
    *,
    raw_root: str | Path,
    checkpoint: Mapping[str, Any] | None = None,
    datasets: Sequence[str] = DAILY_RAW_DATASETS,
) -> list[dict[str, Any]]:
    """Purely plan daily full-market requests; do not inspect disk or call a client."""

    dates = normalize_trade_calendar(trading_dates)
    completed = (checkpoint or {}).get("partitions") or {}
    if not isinstance(completed, Mapping):
        raise TypeError("checkpoint.partitions must be a mapping")

    plans: list[dict[str, Any]] = []
    for date in dates:
        date_text = date.strftime("%Y-%m-%d")
        for dataset in datasets:
            if dataset not in DAILY_RAW_DATASETS:
                raise ValueError(f"unsupported daily raw dataset: {dataset}")
            key = raw_partition_key(dataset, date_text)
            path = str(raw_partition_path(raw_root, dataset, date_text))
            existing = completed.get(key)
            is_complete = (
                isinstance(existing, Mapping)
                and existing.get("status") == "complete"
                and existing.get("path") == path
                and bool(re.fullmatch(r"[0-9a-f]{64}", str(existing.get("sha256") or "")))
            )
            plans.append(
                {
                    "key": key,
                    "dataset": dataset,
                    "trade_date": date_text,
                    "path": path,
                    "status": "complete" if is_complete else "pending",
                    "request": {
                        "trade_date": _compact_date(date_text),
                        "fields": _DAILY_FIELDS[dataset],
                    },
                }
            )
    return plans


def build_expanded_market_data_plan(
    calendar: pd.DataFrame | Sequence[Any],
    *,
    analysis_start: Any,
    analysis_end: Any,
    raw_root: str | Path,
    checkpoint: Mapping[str, Any] | None = None,
    warmup_sessions: int = 120,
    forward_label_sessions: int = 6,
    datasets: Sequence[str] = DAILY_RAW_DATASETS,
) -> dict[str, Any]:
    """Plan the warm-up, label tail, and all daily raw partitions for a run."""

    dates = normalize_trade_calendar(calendar)
    requested_start = pd.Timestamp(analysis_start).normalize()
    requested_end = pd.Timestamp(analysis_end).normalize()
    if warmup_sessions < 0 or forward_label_sessions < 0:
        raise ValueError("warmup_sessions and forward_label_sessions must be non-negative")
    if requested_end < requested_start:
        raise ValueError("analysis_end must be on or after analysis_start")

    analysis_positions = np.flatnonzero((dates >= requested_start) & (dates <= requested_end))
    if not len(analysis_positions):
        raise ValueError("analysis range contains no trading sessions")
    first_position = int(analysis_positions[0])
    last_position = int(analysis_positions[-1])
    if first_position < warmup_sessions:
        raise ValueError("trade calendar does not contain the requested warm-up history")
    if last_position + forward_label_sessions >= len(dates):
        raise ValueError("trade calendar does not contain the requested forward-label tail")

    fetch_dates = dates[
        first_position - warmup_sessions : last_position + forward_label_sessions + 1
    ]
    partitions = plan_daily_raw_partitions(
        fetch_dates,
        raw_root=raw_root,
        checkpoint=checkpoint,
        datasets=datasets,
    )
    return {
        "schema_version": 1,
        "analysis_start": dates[first_position].strftime("%Y-%m-%d"),
        "analysis_end": dates[last_position].strftime("%Y-%m-%d"),
        "fetch_start": fetch_dates[0].strftime("%Y-%m-%d"),
        "fetch_end": fetch_dates[-1].strftime("%Y-%m-%d"),
        "warmup_sessions": warmup_sessions,
        "forward_label_sessions": forward_label_sessions,
        "partition_count": len(partitions),
        "pending_partition_count": sum(row["status"] == "pending" for row in partitions),
        "partitions": partitions,
    }


def advance_raw_checkpoint(
    checkpoint: Mapping[str, Any] | None,
    partition: Mapping[str, Any],
    *,
    sha256: str,
    row_count: int,
    size_bytes: int,
    completed_at_utc: str | None = None,
) -> dict[str, Any]:
    """Return a new checkpoint with one verified partition marked complete."""

    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise ValueError("sha256 must be a lowercase 64-character hex digest")
    key = str(partition.get("key") or "")
    expected_key = raw_partition_key(str(partition.get("dataset")), partition.get("trade_date"))
    if key != expected_key:
        raise ValueError("partition key does not match dataset/trade_date")
    if row_count < 0 or size_bytes < 0:
        raise ValueError("row_count and size_bytes must be non-negative")

    result = deepcopy(dict(checkpoint or {}))
    result["schema_version"] = RAW_CHECKPOINT_SCHEMA_VERSION
    entries = dict(result.get("partitions") or {})
    entry = {
        "status": "complete",
        "dataset": partition["dataset"],
        "trade_date": _as_iso_date(partition["trade_date"]),
        "path": str(partition["path"]),
        "sha256": sha256,
        "row_count": int(row_count),
        "size_bytes": int(size_bytes),
    }
    if completed_at_utc is not None:
        entry["completed_at_utc"] = completed_at_utc
    entries[key] = entry
    result["partitions"] = entries
    return result


def filter_verified_raw_checkpoint(
    checkpoint: Mapping[str, Any] | None,
    *,
    verify_hashes: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return only checkpoint entries whose files still match recorded evidence.

    Planning used to trust a syntactically valid digest even when the file was
    missing or had changed.  The filtered checkpoint makes those partitions
    pending again without deleting or overwriting any cached data.
    """

    payload = dict(checkpoint or {})
    entries = payload.get("partitions") or {}
    if not isinstance(entries, Mapping):
        raise TypeError("checkpoint.partitions must be a mapping")
    verified: dict[str, Any] = {}
    failures: dict[str, list[str]] = {
        "invalid_entry": [],
        "key_mismatch": [],
        "file_missing": [],
        "empty_partition": [],
        "size_mismatch": [],
        "hash_missing": [],
        "hash_mismatch": [],
    }
    for key, raw_entry in entries.items():
        if not isinstance(raw_entry, Mapping) or raw_entry.get("status") != "complete":
            failures["invalid_entry"].append(str(key))
            continue
        entry = dict(raw_entry)
        try:
            expected_key = raw_partition_key(entry.get("dataset"), entry.get("trade_date"))
        except (TypeError, ValueError):
            failures["invalid_entry"].append(str(key))
            continue
        if str(key) != expected_key:
            failures["key_mismatch"].append(str(key))
            continue
        path = Path(str(entry.get("path") or ""))
        if not path.is_file():
            failures["file_missing"].append(str(key))
            continue
        if int(entry.get("row_count") or 0) <= 0:
            failures["empty_partition"].append(str(key))
            continue
        if path.stat().st_size != int(entry.get("size_bytes") or -1):
            failures["size_mismatch"].append(str(key))
            continue
        expected_hash = str(entry.get("sha256") or "")
        if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
            failures["hash_missing"].append(str(key))
            continue
        if verify_hashes and sha256_file(path) != expected_hash:
            failures["hash_mismatch"].append(str(key))
            continue
        verified[str(key)] = entry
    filtered = {
        **{key: value for key, value in payload.items() if key != "partitions"},
        "schema_version": RAW_CHECKPOINT_SCHEMA_VERSION,
        "partitions": verified,
    }
    counts = {name: len(values) for name, values in failures.items()}
    return filtered, {
        "status": "pass" if not any(counts.values()) else "degraded",
        "input_count": len(entries),
        "verified_count": len(verified),
        "failure_counts": counts,
        "failure_samples": {name: values[:10] for name, values in failures.items() if values},
        "hashes_verified": bool(verify_hashes),
    }


def _call_tushare(client: Any, endpoint: str, **kwargs: Any) -> pd.DataFrame:
    """Call an injected raw Pro client, provider.pro, or query-style fixture."""

    target = getattr(client, endpoint, None)
    if not callable(target) and getattr(client, "pro", None) is not None:
        target = getattr(client.pro, endpoint, None)
    if callable(target):
        value = target(**kwargs)
    else:
        query = getattr(client, "query", None)
        if not callable(query) and getattr(client, "pro", None) is not None:
            query = getattr(client.pro, "query", None)
        if not callable(query):
            raise TypeError(f"injected Tushare client has no {endpoint!r} endpoint")
        value = query(endpoint, **kwargs)

    if value is None:
        return pd.DataFrame()
    if not isinstance(value, pd.DataFrame):
        raise TypeError(f"Tushare endpoint {endpoint!r} did not return a DataFrame")
    return value.copy()


def fetch_trade_calendar(
    client: Any,
    *,
    start_date: Any,
    end_date: Any,
    exchange: str = "SSE",
) -> pd.DataFrame:
    """Fetch a bounded exchange calendar through an injected client."""

    frame = _call_tushare(
        client,
        "trade_cal",
        exchange=exchange,
        start_date=_compact_date(start_date),
        end_date=_compact_date(end_date),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    if "is_open" in frame.columns:
        open_flag = pd.to_numeric(frame["is_open"], errors="coerce").eq(1)
        open_flag |= frame["is_open"].astype(str).str.lower().eq("true")
        frame = frame.loc[open_flag]
    return frame.sort_values("cal_date").reset_index(drop=True) if "cal_date" in frame else frame


def fetch_daily_raw_partition(client: Any, partition: Mapping[str, Any]) -> pd.DataFrame:
    """Fetch exactly one planned all-market partition through an injected client."""

    dataset = str(partition.get("dataset") or "")
    if dataset not in DAILY_RAW_DATASETS:
        raise ValueError(f"unsupported daily raw dataset: {dataset}")
    request = dict(partition.get("request") or {})
    expected_date = _compact_date(partition.get("trade_date"))
    if request.get("trade_date") != expected_date:
        raise ValueError("partition request date does not match trade_date")
    return _call_tushare(client, dataset, **request)


def fetch_stock_metadata(
    client: Any,
    *,
    statuses: Sequence[str] = STOCK_LIST_STATUSES,
) -> pd.DataFrame:
    """Fetch and union stock_basic snapshots for listed, paused, and delisted stocks."""

    parts: list[pd.DataFrame] = []
    for status in statuses:
        if status not in STOCK_LIST_STATUSES:
            raise ValueError(f"unsupported list_status: {status}")
        frame = _call_tushare(
            client,
            "stock_basic",
            exchange="",
            list_status=status,
            fields=_STOCK_BASIC_FIELDS,
        )
        if frame.empty:
            continue
        if "list_status" not in frame.columns:
            frame["list_status"] = status
        else:
            frame["list_status"] = frame["list_status"].fillna(status)
        frame["queried_list_status"] = status
        parts.append(frame)
    if not parts:
        return pd.DataFrame(columns=_STOCK_BASIC_FIELDS.split(",") + ["queried_list_status"])
    result = pd.concat(parts, ignore_index=True).drop_duplicates().reset_index(drop=True)
    if "ts_code" in result.columns:
        result = result.sort_values(["ts_code", "queried_list_status"]).reset_index(drop=True)
    return result


def filter_mainland_common_a_shares(metadata: pd.DataFrame) -> pd.DataFrame:
    """Keep ordinary Shanghai/Shenzhen A shares; exclude B shares and Beijing."""

    if "ts_code" not in metadata.columns:
        raise KeyError("stock metadata requires ts_code")
    result = metadata.copy()
    code = (
        result["symbol"].astype("string")
        if "symbol" in result.columns
        else result["ts_code"].astype("string").str.split(".").str[0]
    )
    ts_code = result["ts_code"].astype("string")
    ordinary_sh = ts_code.str.endswith(".SH", na=False) & code.str.fullmatch(r"6\d{5}", na=False)
    ordinary_sz = ts_code.str.endswith(".SZ", na=False) & code.str.fullmatch(r"[03]\d{5}", na=False)
    return result.loc[ordinary_sh | ordinary_sz].copy().reset_index(drop=True)


def filter_stock_metadata_as_of(
    metadata: pd.DataFrame,
    as_of_date: Any,
    *,
    min_listing_days: int = 0,
) -> pd.DataFrame:
    """Apply PIT list/delist dates and an optional minimum listing age."""

    if not {"ts_code", "list_date"}.issubset(metadata.columns):
        raise KeyError("stock metadata requires ts_code and list_date")
    if min_listing_days < 0:
        raise ValueError("min_listing_days must be non-negative")
    result = filter_mainland_common_a_shares(metadata)
    as_of = pd.Timestamp(as_of_date).normalize()
    listed = pd.to_datetime(result["list_date"], format="%Y%m%d", errors="coerce")
    if "delist_date" in result.columns:
        delisted = pd.to_datetime(result["delist_date"], format="%Y%m%d", errors="coerce")
    else:
        delisted = pd.Series(pd.NaT, index=result.index, dtype="datetime64[ns]")
    listing_age_days = (as_of - listed).dt.days
    eligible = (
        listed.notna()
        & (listed <= as_of)
        & (delisted.isna() | (delisted >= as_of))
        & listing_age_days.ge(min_listing_days)
    )
    return result.loc[eligible].copy().reset_index(drop=True)


def fetch_historical_st_history(
    client: Any,
    *,
    start_date: Any,
    end_date: Any,
    allow_degraded: bool = True,
) -> HistoricalSTSnapshot:
    """Fetch historical ST name intervals, optionally degrading when unavailable."""

    try:
        frame = _call_tushare(
            client,
            "namechange",
            ts_code="",
            start_date=_compact_date(start_date),
            end_date=_compact_date(end_date),
            fields="ts_code,name,start_date,end_date,change_reason",
        )
    except Exception as exc:
        if not allow_degraded:
            raise
        return HistoricalSTSnapshot(
            records=pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date", "change_reason"]),
            available=False,
            degraded=True,
            reason=f"{type(exc).__name__}: {exc}",
        )

    missing_required = sorted({"ts_code", "name", "start_date"} - set(frame.columns))
    if missing_required:
        error = ValueError(f"namechange response missing columns: {missing_required}")
        if not allow_degraded:
            raise error
        return HistoricalSTSnapshot(
            records=pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date", "change_reason"]),
            available=False,
            degraded=True,
            reason=str(error),
        )
    if frame.empty:
        error = ValueError("namechange response is empty; historical ST coverage is unverified")
        if not allow_degraded:
            raise error
        return HistoricalSTSnapshot(
            records=pd.DataFrame(columns=["ts_code", "name", "start_date", "end_date", "change_reason"]),
            available=False,
            degraded=True,
            reason="historical_st_empty_response",
        )
    for column in ("end_date", "change_reason"):
        if column not in frame.columns:
            frame[column] = pd.NA
    st_name = frame["name"].astype("string").str.strip().str.match(r"^(?:S)?\*?ST", case=False, na=False)
    records = frame.loc[st_name].copy().reset_index(drop=True)
    if records.empty:
        error = ValueError("namechange response contains no recognizable historical ST records")
        if not allow_degraded:
            raise error
        return HistoricalSTSnapshot(
            records=records,
            available=False,
            degraded=True,
            reason="historical_st_no_matching_records",
        )
    return HistoricalSTSnapshot(records=records, available=True, degraded=False)


def normalize_historical_st_snapshot(snapshot: HistoricalSTSnapshot) -> HistoricalSTSnapshot:
    """Prevent cached empty/invalid ST tables from being treated as applied evidence."""

    records = snapshot.records.copy()
    if snapshot.available and records.empty:
        return HistoricalSTSnapshot(
            records=records,
            available=False,
            degraded=True,
            reason=snapshot.reason or "historical_st_empty_cached_table",
        )
    required = {"ts_code", "start_date", "end_date"}
    if snapshot.available and not required.issubset(records.columns):
        return HistoricalSTSnapshot(
            records=records,
            available=False,
            degraded=True,
            reason=f"historical_st_cached_columns_missing:{sorted(required - set(records.columns))}",
        )
    return snapshot


def apply_historical_st_filter(
    candidates: pd.DataFrame,
    history: HistoricalSTSnapshot | pd.DataFrame | None,
    *,
    date_column: str = "as_of_date",
    allow_degraded: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Exclude securities that were ST as of each row's PIT selection date."""

    if "ts_code" not in candidates.columns or date_column not in candidates.columns:
        raise KeyError(f"candidates require ts_code and {date_column}")
    if isinstance(history, pd.DataFrame):
        snapshot = HistoricalSTSnapshot(
            records=history,
            available=not history.empty,
            degraded=history.empty,
            reason="historical_st_empty_dataframe" if history.empty else None,
        )
    elif history is None:
        snapshot = HistoricalSTSnapshot(
            records=pd.DataFrame(), available=False, degraded=True, reason="historical_st_not_provided"
        )
    else:
        snapshot = normalize_historical_st_snapshot(history)

    result = candidates.copy()
    if not snapshot.available:
        if not allow_degraded:
            raise RuntimeError(snapshot.reason or "historical ST source unavailable")
        result["historical_st_known"] = False
        result["is_st_at_asof"] = pd.Series(pd.NA, index=result.index, dtype="boolean")
        result["st_filter_status"] = "degraded_unavailable"
        return result, {
            "status": "degraded_unavailable",
            "available": False,
            "input_count": int(len(result)),
            "excluded_count": 0,
            "reason": snapshot.reason,
        }

    records = snapshot.records.copy()
    if records.empty:
        is_st = pd.Series(False, index=result.index, dtype="boolean")
    else:
        required = {"ts_code", "start_date", "end_date"}
        if not required.issubset(records.columns):
            raise KeyError(f"historical ST records require {sorted(required)}")
        records["_start"] = pd.to_datetime(records["start_date"], format="%Y%m%d", errors="coerce")
        records["_end"] = pd.to_datetime(records["end_date"], format="%Y%m%d", errors="coerce")
        intervals = {
            code: list(zip(group["_start"], group["_end"]))
            for code, group in records.loc[records["_start"].notna()].groupby("ts_code")
        }

        def _row_is_st(row: pd.Series) -> bool:
            as_of = pd.Timestamp(row[date_column]).normalize()
            return any(
                start <= as_of and (pd.isna(end) or as_of <= end)
                for start, end in intervals.get(row["ts_code"], [])
            )

        is_st = result.apply(_row_is_st, axis=1).astype("boolean")

    result["historical_st_known"] = True
    result["is_st_at_asof"] = is_st
    result["st_filter_status"] = "applied"
    excluded_count = int(is_st.fillna(False).sum())
    result = result.loc[~is_st.fillna(False)].copy().reset_index(drop=True)
    return result, {
        "status": "applied",
        "available": True,
        "input_count": int(len(candidates)),
        "excluded_count": excluded_count,
        "reason": None,
    }


def build_monthly_top_n_membership(
    daily_amount: pd.DataFrame,
    metadata: pd.DataFrame,
    calendar: pd.DataFrame | Sequence[Any],
    *,
    start_date: Any,
    end_date: Any,
    top_n: int = 500,
    lookback_sessions: int = 60,
    min_amount_observations: int = 1,
    min_listing_days: int = 180,
    historical_st: HistoricalSTSnapshot | pd.DataFrame | None = None,
    allow_st_degraded: bool = True,
) -> MonthlyMembershipResult:
    """Build monthly PIT membership from the prior month-end's 60-session median amount."""

    required = {"ts_code", "trade_date", "amount"}
    if not required.issubset(daily_amount.columns):
        raise KeyError(f"daily_amount requires {sorted(required)}")
    if top_n <= 0 or lookback_sessions <= 0 or min_amount_observations <= 0:
        raise ValueError("top_n, lookback_sessions, and min_amount_observations must be positive")
    if min_listing_days < 0:
        raise ValueError("min_listing_days must be non-negative")

    dates = normalize_trade_calendar(calendar)
    requested_start = pd.Timestamp(start_date).normalize()
    requested_end = pd.Timestamp(end_date).normalize()
    effective_dates = dates[(dates >= requested_start) & (dates <= requested_end)]
    if effective_dates.empty:
        raise ValueError("membership range contains no trading sessions")

    amounts = daily_amount.copy()
    amounts["trade_date"] = pd.to_datetime(amounts["trade_date"], errors="coerce").dt.normalize()
    amounts["amount"] = pd.to_numeric(amounts["amount"], errors="coerce")
    amounts = amounts.dropna(subset=["ts_code", "trade_date"])
    if amounts.duplicated(["ts_code", "trade_date"]).any():
        raise ValueError("daily_amount contains duplicate ts_code/trade_date rows")

    rows: list[pd.DataFrame] = []
    month_audits: list[dict[str, Any]] = []
    periods = pd.PeriodIndex(effective_dates, freq="M").unique().sort_values()
    for period in periods:
        month_dates = effective_dates[pd.PeriodIndex(effective_dates, freq="M") == period]
        effective_start = month_dates[0]
        effective_end = month_dates[-1]
        prior_dates = dates[dates < period.start_time]
        if prior_dates.empty:
            raise ValueError(f"no prior month-end trading date available for {period}")
        if len(prior_dates) < lookback_sessions:
            raise ValueError(
                f"only {len(prior_dates)} prior sessions available for {period}; "
                f"{lookback_sessions} required"
            )
        as_of = prior_dates[-1]
        liquidity_dates = prior_dates[-lookback_sessions:]

        listed_metadata = filter_stock_metadata_as_of(metadata, as_of, min_listing_days=0)
        eligible_metadata = filter_stock_metadata_as_of(
            metadata,
            as_of,
            min_listing_days=min_listing_days,
        )
        listed_codes = set(listed_metadata["ts_code"].dropna())
        eligible_codes = set(eligible_metadata["ts_code"].dropna())
        excluded_recent_listing_count = len(listed_codes - eligible_codes)
        candidates = eligible_metadata[["ts_code"]].drop_duplicates().copy()
        candidates["as_of_date"] = as_of
        candidates, st_audit = apply_historical_st_filter(
            candidates,
            historical_st,
            date_column="as_of_date",
            allow_degraded=allow_st_degraded,
        )

        window = amounts.loc[amounts["trade_date"].isin(liquidity_dates)]
        liquidity = (
            window.groupby("ts_code", as_index=False)["amount"]
            .agg(median_amount_60d="median", liquidity_observations="count")
        )
        ranked = candidates.merge(liquidity, on="ts_code", how="inner")
        ranked = ranked.loc[
            ranked["median_amount_60d"].notna()
            & (ranked["liquidity_observations"] >= min_amount_observations)
        ].copy()
        rankable_security_count = int(len(ranked))
        ranked = ranked.sort_values(
            ["median_amount_60d", "ts_code"], ascending=[False, True], kind="mergesort"
        ).head(top_n)
        ranked["liquidity_rank"] = np.arange(1, len(ranked) + 1)
        ranked["membership_month"] = str(period)
        ranked["effective_start_date"] = effective_start
        ranked["effective_end_date"] = effective_end
        ranked["liquidity_window_start"] = liquidity_dates[0]
        ranked["liquidity_window_end"] = liquidity_dates[-1]
        rows.append(ranked)
        month_audits.append(
            {
                "membership_month": str(period),
                "as_of_date": as_of.strftime("%Y-%m-%d"),
                "liquidity_window_start": liquidity_dates[0].strftime("%Y-%m-%d"),
                "liquidity_window_end": liquidity_dates[-1].strftime("%Y-%m-%d"),
                "eligible_security_count": int(len(eligible_metadata)),
                "excluded_recent_listing_count": excluded_recent_listing_count,
                "rankable_security_count": rankable_security_count,
                "selected_count": int(len(ranked)),
                "st_filter": st_audit,
            }
        )

    membership = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    return MonthlyMembershipResult(
        membership=membership,
        audit={
            "schema_version": 1,
            "selection_rule": f"prior_month_end_{lookback_sessions}d_median_amount_top{top_n}",
            "top_n": top_n,
            "lookback_sessions": lookback_sessions,
            "min_amount_observations": min_amount_observations,
            "min_listing_days": min_listing_days,
            "months": month_audits,
            "degraded_st_month_count": sum(
                row["st_filter"]["status"] == "degraded_unavailable" for row in month_audits
            ),
        },
    )


def build_monthly_top500_membership(
    daily_amount: pd.DataFrame,
    metadata: pd.DataFrame,
    calendar: pd.DataFrame | Sequence[Any],
    **kwargs: Any,
) -> MonthlyMembershipResult:
    """Convenience wrapper for the production top-500 rule."""

    if "top_n" in kwargs:
        raise TypeError("build_monthly_top500_membership fixes top_n=500")
    return build_monthly_top_n_membership(
        daily_amount,
        metadata,
        calendar,
        top_n=500,
        **kwargs,
    )


def apply_monthly_membership(
    market_frame: pd.DataFrame,
    membership: MonthlyMembershipResult | pd.DataFrame,
) -> pd.DataFrame:
    """Inner-join daily market rows to their precomputed PIT monthly universe."""

    if not {"ts_code", "trade_date"}.issubset(market_frame.columns):
        raise KeyError("market_frame requires ts_code and trade_date")
    member_frame = membership.membership if isinstance(membership, MonthlyMembershipResult) else membership
    if not {"ts_code", "membership_month"}.issubset(member_frame.columns):
        raise KeyError("membership requires ts_code and membership_month")
    market = market_frame.copy()
    market["trade_date"] = pd.to_datetime(market["trade_date"], errors="coerce").dt.normalize()
    market["membership_month"] = market["trade_date"].dt.to_period("M").astype(str)
    member_columns = [
        column
        for column in member_frame.columns
        if column not in market.columns or column in {"ts_code", "membership_month"}
    ]
    return market.merge(
        member_frame[member_columns],
        on=["ts_code", "membership_month"],
        how="inner",
        validate="many_to_one",
    )


def add_adjusted_open_close(
    daily: pd.DataFrame,
    adj_factors: pd.DataFrame,
    *,
    code_column: str = "ts_code",
    date_column: str = "trade_date",
) -> pd.DataFrame:
    """Add split/dividend-consistent open/close as raw price times adj_factor.

    No latest-date normalization is used, avoiding a future-dependent qfq base.
    Missing adjustment factors stay missing rather than silently falling back to raw.
    """

    daily_required = {code_column, date_column, "open", "close"}
    factor_required = {code_column, date_column, "adj_factor"}
    if not daily_required.issubset(daily.columns):
        raise KeyError(f"daily requires {sorted(daily_required)}")
    if not factor_required.issubset(adj_factors.columns):
        raise KeyError(f"adj_factors requires {sorted(factor_required)}")
    if daily.duplicated([code_column, date_column]).any():
        raise ValueError("daily contains duplicate security/date rows")
    if adj_factors.duplicated([code_column, date_column]).any():
        raise ValueError("adj_factors contains duplicate security/date rows")

    prices = daily.copy()
    factors = adj_factors[[code_column, date_column, "adj_factor"]].copy()
    prices[date_column] = pd.to_datetime(prices[date_column], errors="coerce").dt.normalize()
    factors[date_column] = pd.to_datetime(factors[date_column], errors="coerce").dt.normalize()
    result = prices.merge(
        factors,
        on=[code_column, date_column],
        how="left",
        validate="one_to_one",
    )
    factor = pd.to_numeric(result["adj_factor"], errors="coerce").where(lambda value: value > 0)
    result["open_adj"] = pd.to_numeric(result["open"], errors="coerce") * factor
    result["close_adj"] = pd.to_numeric(result["close"], errors="coerce") * factor
    return result


def add_t_plus_1_to_t_plus_6_open_label(
    frame: pd.DataFrame,
    calendar: pd.DataFrame | Sequence[Any],
    *,
    code_column: str = "ts_code",
    date_column: str = "trade_date",
    adjusted_open_column: str = "open_adj",
    label_column: str = "forward_return_5d_open",
) -> pd.DataFrame:
    """Add the five-session return bought at t+1 open and sold at t+6 open.

    Entry/exit dates come from the market calendar, not the security's next
    observed row.  A suspension or missing adjusted open therefore yields a
    missing label instead of shortening the horizon.
    """

    required = {code_column, date_column, adjusted_open_column}
    if not required.issubset(frame.columns):
        raise KeyError(f"frame requires {sorted(required)}")
    if frame.duplicated([code_column, date_column]).any():
        raise ValueError("frame contains duplicate security/date rows")

    dates = normalize_trade_calendar(calendar)
    date_map = pd.DataFrame({date_column: dates})
    date_map["label_entry_date"] = date_map[date_column].shift(-1)
    date_map["label_exit_date"] = date_map[date_column].shift(-6)

    result = frame.copy()
    result[date_column] = pd.to_datetime(result[date_column], errors="coerce").dt.normalize()
    result = result.merge(date_map, on=date_column, how="left", validate="many_to_one")
    price_lookup = result[[code_column, date_column, adjusted_open_column]].copy()
    entry_lookup = price_lookup.rename(
        columns={date_column: "label_entry_date", adjusted_open_column: "label_entry_open"}
    )
    exit_lookup = price_lookup.rename(
        columns={date_column: "label_exit_date", adjusted_open_column: "label_exit_open"}
    )
    result = result.merge(
        entry_lookup,
        on=[code_column, "label_entry_date"],
        how="left",
        validate="many_to_one",
    )
    result = result.merge(
        exit_lookup,
        on=[code_column, "label_exit_date"],
        how="left",
        validate="many_to_one",
    )
    entry = pd.to_numeric(result["label_entry_open"], errors="coerce")
    exit_price = pd.to_numeric(result["label_exit_open"], errors="coerce")
    valid = entry.gt(0) & exit_price.gt(0)
    result[label_column] = np.where(valid, exit_price / entry - 1.0, np.nan)
    return result


def build_factor_panels(
    frame: pd.DataFrame,
    factor_columns: Sequence[str],
    *,
    label_column: str = "forward_return_5d_open",
    required_columns: Sequence[str] = ("ts_code", "trade_date"),
) -> dict[str, pd.DataFrame]:
    """Drop missing values per factor, never across the union of factor columns."""

    missing = [
        column
        for column in [*required_columns, label_column, *factor_columns]
        if column not in frame.columns
    ]
    if missing:
        raise KeyError(f"frame is missing columns: {sorted(set(missing))}")
    panels: dict[str, pd.DataFrame] = {}
    for factor in factor_columns:
        needed = list(dict.fromkeys([*required_columns, factor, label_column]))
        mask = frame[needed].notna().all(axis=1)
        panels[factor] = frame.loc[mask].copy().reset_index(drop=True)
    return panels


def audit_raw_partition(frame: pd.DataFrame, partition: Mapping[str, Any]) -> dict[str, Any]:
    dataset = str(partition.get("dataset") or "")
    requested_date = _as_iso_date(partition.get("trade_date"))
    required_columns = {"ts_code", "trade_date"}
    requested_fields = str((partition.get("request") or {}).get("fields") or "")
    required_columns.update(field.strip() for field in requested_fields.split(",") if field.strip())
    missing_columns = sorted(required_columns - set(frame.columns))
    date_mismatch_count = None
    duplicate_key_count = None
    if not missing_columns:
        dates = pd.to_datetime(frame["trade_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        date_mismatch_count = int((dates != requested_date).sum())
        duplicate_key_count = int(frame.duplicated(["ts_code", "trade_date"]).sum())
    issues = []
    if frame.empty:
        issues.append("empty_open_market_partition")
    if missing_columns:
        issues.append("missing_required_columns")
    if date_mismatch_count:
        issues.append("trade_date_mismatch")
    if duplicate_key_count:
        issues.append("duplicate_security_date")
    return {
        "dataset": dataset,
        "trade_date": requested_date,
        "row_count": int(len(frame)),
        "security_count": int(frame["ts_code"].nunique()) if "ts_code" in frame else 0,
        "missing_columns": missing_columns,
        "date_mismatch_count": date_mismatch_count,
        "duplicate_key_count": duplicate_key_count,
        "status": "pass" if not issues else "fail",
        "issues": issues,
    }


def audit_expanded_market_data(
    frame: pd.DataFrame,
    *,
    factor_columns: Sequence[str] = (),
    label_column: str = "forward_return_5d_open",
    code_column: str = "ts_code",
    date_column: str = "trade_date",
) -> dict[str, Any]:
    """Create a serializable coverage, PIT, duplicate, and label audit."""

    row_count = int(len(frame))
    missingness = {
        column: {
            "missing_count": int(frame[column].isna().sum()),
            "coverage": round(float(frame[column].notna().mean()), 6) if row_count else 0.0,
        }
        for column in frame.columns
    }
    factor_coverage = {}
    for factor in factor_columns:
        summary = dict(missingness.get(factor, {"missing_count": row_count, "coverage": 0.0}))
        if factor in frame.columns and label_column in frame.columns:
            usable = frame[[factor, label_column]].notna().all(axis=1)
            summary["usable_with_label_count"] = int(usable.sum())
            summary["usable_with_label_coverage"] = round(float(usable.mean()), 6) if row_count else 0.0
        else:
            summary["usable_with_label_count"] = 0
            summary["usable_with_label_coverage"] = 0.0
        factor_coverage[factor] = summary
    duplicate_key_count = (
        int(frame.duplicated([code_column, date_column]).sum())
        if {code_column, date_column}.issubset(frame.columns)
        else None
    )

    pit_violation_count = None
    if {"as_of_date", "effective_start_date"}.issubset(frame.columns):
        as_of = pd.to_datetime(frame["as_of_date"], errors="coerce")
        effective_start = pd.to_datetime(frame["effective_start_date"], errors="coerce")
        pit_violation_count = int((as_of >= effective_start).fillna(False).sum())
    liquidity_future_leak_count = None
    if {"liquidity_window_end", "as_of_date"}.issubset(frame.columns):
        window_end = pd.to_datetime(frame["liquidity_window_end"], errors="coerce")
        as_of = pd.to_datetime(frame["as_of_date"], errors="coerce")
        liquidity_future_leak_count = int((window_end > as_of).fillna(False).sum())

    issues: list[str] = []
    if duplicate_key_count:
        issues.append("duplicate_security_date")
    if pit_violation_count:
        issues.append("membership_not_point_in_time")
    if liquidity_future_leak_count:
        issues.append("liquidity_window_after_as_of")
    if label_column not in frame.columns:
        issues.append("label_column_missing")
    historical_st_degraded_row_count = (
        int((frame["st_filter_status"] == "degraded_unavailable").sum())
        if "st_filter_status" in frame.columns
        else None
    )
    if historical_st_degraded_row_count:
        issues.append("st_history_unverified")

    parsed_dates = (
        pd.to_datetime(frame[date_column], errors="coerce")
        if date_column in frame.columns
        else pd.Series(dtype="datetime64[ns]")
    )
    return {
        "schema_version": 1,
        "row_count": row_count,
        "security_count": int(frame[code_column].nunique()) if code_column in frame else 0,
        "min_date": parsed_dates.min().strftime("%Y-%m-%d") if parsed_dates.notna().any() else None,
        "max_date": parsed_dates.max().strftime("%Y-%m-%d") if parsed_dates.notna().any() else None,
        "duplicate_key_count": duplicate_key_count,
        "pit_violation_count": pit_violation_count,
        "liquidity_future_leak_count": liquidity_future_leak_count,
        "label": missingness.get(label_column, {"missing_count": row_count, "coverage": 0.0}),
        "factor_coverage": factor_coverage,
        "missingness": missingness,
        "historical_st_degraded_row_count": historical_st_degraded_row_count,
        "status": "pass" if not issues else "fail",
        "issues": issues,
    }


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _expand_manifest_paths(paths: Iterable[str | Path]) -> list[Path]:
    expanded: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            expanded.extend(candidate for candidate in path.rglob("*") if candidate.is_file())
        elif path.is_file():
            expanded.append(path)
        else:
            raise FileNotFoundError(path)
    return sorted(set(expanded), key=lambda item: item.as_posix())


def build_sha256_manifest(
    paths: Iterable[str | Path],
    *,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Hash files deterministically and include a digest of the canonical entry list."""

    files = _expand_manifest_paths(paths)
    base = Path(base_dir).resolve() if base_dir is not None else None
    entries: list[dict[str, Any]] = []
    for path in files:
        resolved = path.resolve()
        if base is not None:
            try:
                display_path = resolved.relative_to(base).as_posix()
            except ValueError as exc:
                raise ValueError(f"manifest file is outside base_dir: {resolved}") from exc
        else:
            display_path = resolved.as_posix()
        entries.append(
            {
                "path": display_path,
                "size_bytes": resolved.stat().st_size,
                "sha256": sha256_file(resolved),
            }
        )
    entries.sort(key=lambda row: row["path"])
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "schema_version": SHA256_MANIFEST_SCHEMA_VERSION,
        "algorithm": "sha256",
        "file_count": len(entries),
        "files": entries,
        "manifest_sha256": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
    }


def verify_sha256_manifest(
    manifest: Mapping[str, Any],
    *,
    base_dir: str | Path | None = None,
) -> dict[str, Any]:
    base = Path(base_dir).resolve() if base_dir is not None else None
    entries = list(manifest.get("files") or [])
    canonical = json.dumps(entries, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    actual_manifest_digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    expected_manifest_digest = str(manifest.get("manifest_sha256") or "")
    manifest_digest_valid = (
        bool(re.fullmatch(r"[0-9a-f]{64}", expected_manifest_digest))
        and actual_manifest_digest == expected_manifest_digest
    )
    missing: list[str] = []
    mismatches: list[dict[str, str]] = []
    size_mismatches: list[dict[str, Any]] = []
    invalid_entries: list[dict[str, Any]] = []
    stored_paths = [str(entry.get("path") or "") for entry in entries if isinstance(entry, Mapping)]
    duplicate_paths = sorted({path for path in stored_paths if stored_paths.count(path) > 1})
    for entry in entries:
        if not isinstance(entry, Mapping):
            invalid_entries.append({"reason": "entry_not_mapping"})
            continue
        stored_path = str(entry.get("path") or "")
        pure_parts = Path(stored_path).parts
        if not stored_path or (base is not None and (Path(stored_path).is_absolute() or ".." in pure_parts)):
            invalid_entries.append({"path": stored_path, "reason": "unsafe_path"})
            continue
        path = ((base / stored_path).resolve()) if base is not None else Path(stored_path)
        if base is not None:
            try:
                path.relative_to(base)
            except ValueError:
                invalid_entries.append({"path": stored_path, "reason": "path_escape"})
                continue
        if not path.exists():
            missing.append(stored_path)
            continue
        actual_size = path.stat().st_size
        raw_size = entry.get("size_bytes")
        expected_size = int(raw_size) if raw_size is not None else -1
        if actual_size != expected_size:
            size_mismatches.append(
                {"path": stored_path, "expected": expected_size, "actual": actual_size}
            )
        actual = sha256_file(path)
        expected = str(entry.get("sha256") or "")
        if actual != expected:
            mismatches.append({"path": stored_path, "expected": expected, "actual": actual})
    metadata_valid = (
        manifest.get("schema_version") == SHA256_MANIFEST_SCHEMA_VERSION
        and manifest.get("algorithm") == "sha256"
        and (
            int(manifest.get("file_count"))
            if manifest.get("file_count") is not None
            else -1
        )
        == len(entries)
        and not duplicate_paths
        and not invalid_entries
    )
    return {
        "valid": (
            manifest_digest_valid
            and metadata_valid
            and not missing
            and not mismatches
            and not size_mismatches
        ),
        "manifest_digest_valid": manifest_digest_valid,
        "metadata_valid": metadata_valid,
        "checked_count": len(entries),
        "missing": missing,
        "mismatches": mismatches,
        "size_mismatches": size_mismatches,
        "duplicate_paths": duplicate_paths,
        "invalid_entries": invalid_entries,
    }


__all__ = [
    "DAILY_RAW_DATASETS",
    "HistoricalSTSnapshot",
    "MonthlyMembershipResult",
    "STOCK_LIST_STATUSES",
    "add_adjusted_open_close",
    "add_t_plus_1_to_t_plus_6_open_label",
    "advance_raw_checkpoint",
    "apply_historical_st_filter",
    "apply_monthly_membership",
    "audit_expanded_market_data",
    "audit_raw_partition",
    "build_expanded_market_data_plan",
    "build_factor_panels",
    "build_monthly_top500_membership",
    "build_monthly_top_n_membership",
    "build_sha256_manifest",
    "calculate_warmup_start_date",
    "fetch_daily_raw_partition",
    "fetch_historical_st_history",
    "fetch_stock_metadata",
    "fetch_trade_calendar",
    "filter_mainland_common_a_shares",
    "filter_stock_metadata_as_of",
    "normalize_trade_calendar",
    "normalize_historical_st_snapshot",
    "plan_daily_raw_partitions",
    "raw_partition_key",
    "raw_partition_path",
    "sha256_file",
    "filter_verified_raw_checkpoint",
    "verify_sha256_manifest",
]
