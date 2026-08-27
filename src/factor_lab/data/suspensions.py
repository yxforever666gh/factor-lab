"""Compact, resumable Tushare ``suspend_d`` synchronization."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Any, Mapping

import pandas as pd

from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file
from .sources import (
    MarketDataClient,
    _call,
    _compact,
    _configured_tushare_client,
    _date,
    _write_json_atomic,
    _write_parquet_atomic,
)


SUSPENSION_COLUMNS = ("ticker", "date", "suspend_type", "suspend_timing")
SUSPENSION_FIELDS = "ts_code,trade_date,suspend_type,suspend_timing"
SUSPENSION_PAGE_SIZE = 5_000


def _calendar_year_windows(start: str, end: str) -> list[tuple[str, str]]:
    cursor = pd.Timestamp(start)
    finish = pd.Timestamp(end)
    windows: list[tuple[str, str]] = []
    while cursor <= finish:
        year_end = pd.Timestamp(year=cursor.year, month=12, day=31)
        window_end = min(year_end, finish)
        windows.append((cursor.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")))
        cursor = window_end + pd.Timedelta(days=1)
    return windows


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": pd.Series(dtype="string"),
            "date": pd.Series(dtype="datetime64[ns]"),
            "suspend_type": pd.Series(dtype="string"),
            "suspend_timing": pd.Series(dtype="string"),
        }
    )


def _normalize_page(frame: pd.DataFrame, *, start: str, end: str) -> pd.DataFrame:
    if frame.empty:
        return _empty_frame()
    required = {"ts_code", "trade_date", "suspend_type", "suspend_timing"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"suspend_d response missing columns: {missing}")
    normalized = frame.loc[:, list(SUSPENSION_FIELDS.split(","))].rename(
        columns={"ts_code": "ticker", "trade_date": "date"}
    )
    normalized["ticker"] = normalized["ticker"].astype("string").str.strip()
    raw_dates = normalized["date"].astype("string").str.replace("-", "", regex=False)
    normalized["date"] = pd.to_datetime(raw_dates, format="%Y%m%d", errors="coerce")
    normalized["suspend_type"] = (
        normalized["suspend_type"].astype("string").str.strip().str.upper()
    )
    normalized["suspend_timing"] = (
        normalized["suspend_timing"].astype("string").str.strip()
    )
    if normalized["ticker"].isna().any() or normalized["ticker"].eq("").any():
        raise ValueError("suspend_d response contains an empty ticker")
    if normalized["date"].isna().any():
        raise ValueError("suspend_d response contains an invalid trade_date")
    if not normalized["suspend_type"].isin({"S", "R"}).all():
        values = sorted(normalized.loc[~normalized["suspend_type"].isin({"S", "R"}), "suspend_type"].dropna().unique())
        raise ValueError(f"suspend_d response contains unsupported suspend_type: {values}")
    if bool(normalized["date"].lt(pd.Timestamp(start)).any()) or bool(
        normalized["date"].gt(pd.Timestamp(end)).any()
    ):
        raise ValueError("suspend_d response contains dates outside its query window")
    return normalized


def _canonicalize(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return _empty_frame()
    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        return _empty_frame()
    return (
        combined.drop_duplicates(list(SUSPENSION_COLUMNS), keep="last")
        .sort_values(["date", "ticker", "suspend_type", "suspend_timing"], na_position="last")
        .reset_index(drop=True)
        .loc[:, list(SUSPENSION_COLUMNS)]
    )


def _frame_stats(frame: pd.DataFrame) -> dict[str, Any]:
    dates = pd.to_datetime(frame["date"], errors="coerce")
    types = frame["suspend_type"].astype("string").str.upper()
    return {
        "rows": int(len(frame)),
        "date": {
            "min": str(dates.min().date()) if len(frame) else None,
            "max": str(dates.max().date()) if len(frame) else None,
        },
        "security": int(frame["ticker"].nunique()),
        "S": int(types.eq("S").sum()),
        "R": int(types.eq("R").sum()),
    }


def _audit_canonical(frame: pd.DataFrame, *, query_start: str, query_end: str) -> None:
    if len(frame.columns) != len(SUSPENSION_COLUMNS) or set(frame.columns) != set(
        SUSPENSION_COLUMNS
    ):
        raise ValueError("suspensions parquet has a non-canonical schema")
    canonical = frame.loc[:, list(SUSPENSION_COLUMNS)].reset_index(drop=True)
    if bool(canonical.duplicated(list(SUSPENSION_COLUMNS)).any()):
        raise ValueError("suspensions parquet contains duplicate rows")
    tickers = canonical["ticker"].astype("string").str.strip()
    types = canonical["suspend_type"].astype("string")
    dates = pd.to_datetime(canonical["date"], errors="coerce")
    if tickers.isna().any() or tickers.eq("").any():
        raise ValueError("suspensions parquet contains an empty ticker")
    if dates.isna().any():
        raise ValueError("suspensions parquet contains invalid dates")
    if not dates.eq(dates.dt.normalize()).all():
        raise ValueError("suspensions parquet dates must be normalized sessions")
    if not types.isin({"S", "R"}).all():
        raise ValueError("suspensions parquet contains unsupported suspend_type values")
    if len(frame) and (
        bool(dates.lt(pd.Timestamp(query_start)).any())
        or bool(dates.gt(pd.Timestamp(query_end)).any())
    ):
        raise ValueError("suspensions parquet falls outside its attested query range")
    expected = _canonicalize([canonical])
    if not canonical.equals(expected):
        raise ValueError("suspensions parquet is not canonically sorted")


def _resume_metadata(
    output_path: Path,
    metadata_path: Path,
    *,
    requested_start: str | None,
    requested_end: str | None,
) -> dict[str, Any] | None:
    if not output_path.exists() and not metadata_path.exists():
        return None
    if not output_path.is_file() or not metadata_path.is_file():
        raise ValueError("resume requires both suspensions.parquet and suspensions.meta.json")
    try:
        import json

        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable suspensions metadata: {metadata_path}") from exc
    if not isinstance(metadata, Mapping):
        raise ValueError("invalid suspensions metadata contract")
    query = metadata.get("query")
    file_info = metadata.get("file")
    try:
        schema_version = int(metadata.get("schema_version") or 0)
        query_limit = int(query.get("limit") or 0) if isinstance(query, Mapping) else 0
        file_size = (
            int(file_info.get("size_bytes") or -1)
            if isinstance(file_info, Mapping)
            else -1
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid suspensions metadata contract") from exc
    if (
        schema_version != 1
        or metadata.get("status") != "complete"
        or metadata.get("source") != "tushare"
        or metadata.get("endpoint") != "suspend_d"
        or not isinstance(query, Mapping)
        or not isinstance(file_info, Mapping)
    ):
        raise ValueError("invalid suspensions metadata contract")
    query_start = _date(str(query.get("start_date") or ""))
    query_end = _date(str(query.get("end_date") or ""))
    if query_end < query_start:
        raise ValueError("invalid suspensions metadata query range")
    if (
        requested_start is not None
        and requested_end is not None
        and (query_start > requested_start or query_end < requested_end)
    ):
        raise ValueError("existing suspensions query range does not cover the requested range")
    if query.get("window") != "calendar_year" or query_limit != SUSPENSION_PAGE_SIZE:
        raise ValueError("existing suspensions metadata uses a different pagination protocol")
    expected_path = str(output_path.resolve())
    if str(file_info.get("path") or "") != expected_path:
        raise ValueError("suspensions metadata path does not match the requested output")
    if str(metadata.get("metadata_path") or "") != str(metadata_path.resolve()):
        raise ValueError("suspensions metadata path does not match its own location")
    retrieved_at = pd.to_datetime(
        metadata.get("retrieved_at_utc"), errors="coerce", utc=True
    )
    if pd.isna(retrieved_at):
        raise ValueError("suspensions metadata has an invalid retrieval timestamp")
    if file_size != output_path.stat().st_size:
        raise ValueError("suspensions parquet size does not match metadata")
    try:
        actual_hash = sha256_file(output_path)
        frame = pd.read_parquet(output_path)
    except Exception as exc:
        raise ValueError(f"unreadable suspensions parquet: {output_path}") from exc
    if str(file_info.get("sha256") or "") != actual_hash:
        raise ValueError("suspensions parquet hash does not match metadata")
    _audit_canonical(frame, query_start=query_start, query_end=query_end)
    stats = _frame_stats(frame)
    if any(metadata.get(key) != value for key, value in stats.items()):
        raise ValueError("suspensions parquet statistics do not match metadata")
    return metadata


def _result(metadata: Mapping[str, Any], *, resumed: bool, request_count: int) -> dict[str, Any]:
    file_info = dict(metadata["file"])
    return {
        "status": "complete",
        "source": metadata["source"],
        "endpoint": metadata["endpoint"],
        "query": dict(metadata["query"]),
        "rows": metadata["rows"],
        "date": dict(metadata["date"]),
        "security": metadata["security"],
        "S": metadata["S"],
        "R": metadata["R"],
        "hash": file_info["sha256"],
        "path": file_info["path"],
        "metadata_path": metadata["metadata_path"],
        "resumed": resumed,
        "request_count": request_count,
    }


def audit_suspensions_snapshot(
    path: str | Path,
    metadata_path: str | Path | None = None,
    *,
    requested_start: str | None = None,
    requested_end: str | None = None,
) -> dict[str, Any]:
    """Fail closed on any suspension snapshot or provenance mismatch.

    This function is strictly local and read-only: it never constructs a data
    client and never performs a network request.
    """

    if (requested_start is None) != (requested_end is None):
        raise ValueError("requested_start and requested_end must be provided together")
    start = _date(requested_start) if requested_start is not None else None
    end = _date(requested_end) if requested_end is not None else None
    if start is not None and end is not None and end < start:
        raise ValueError("requested_end must be on or after requested_start")
    target = Path(path).expanduser().resolve()
    resolved_metadata = (
        Path(metadata_path).expanduser().resolve()
        if metadata_path is not None
        else target.with_name("suspensions.meta.json")
    )
    metadata = _resume_metadata(
        target,
        resolved_metadata,
        requested_start=start,
        requested_end=end,
    )
    if metadata is None:
        raise ValueError("missing suspensions parquet and metadata")
    result = _result(metadata, resumed=True, request_count=0)
    result.pop("resumed")
    result.pop("request_count")
    return result


def sync_suspensions(
    start_date: str,
    end_date: str,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    client: MarketDataClient | Any | None = None,
    output_path: str | Path | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    """Fetch ``suspend_d`` by calendar year and publish one attested Parquet."""

    start = _date(start_date)
    end = _date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    target = (
        Path(output_path).expanduser().resolve()
        if output_path is not None
        else (resolved_layout.top500_root / "suspensions.parquet").resolve()
    )
    metadata_path = target.with_name("suspensions.meta.json")
    if resume:
        try:
            existing = audit_suspensions_snapshot(
                target,
                metadata_path,
                requested_start=start,
                requested_end=end,
            )
        except ValueError:
            if target.exists() or metadata_path.exists():
                raise
            existing = None
        if existing is not None:
            return {
                **existing,
                "resumed": True,
                "request_count": 0,
            }

    sync_config = dict(config.get("sync") or {})
    resolved_client = client or _configured_tushare_client(sync_config, resolved_layout)
    rate = max(0.0, float(sync_config.get("request_rate_per_minute") or 0.0))
    delay = 60.0 / rate if rate else 0.0
    pages: list[pd.DataFrame] = []
    request_count = 0
    for window_start, window_end in _calendar_year_windows(start, end):
        offset = 0
        seen_pages: set[tuple[tuple[Any, ...], ...]] = set()
        while True:
            if delay and request_count:
                time.sleep(delay)
            page = _call(
                resolved_client,
                "suspend_d",
                start_date=_compact(window_start),
                end_date=_compact(window_end),
                fields=SUSPENSION_FIELDS,
                limit=SUSPENSION_PAGE_SIZE,
                offset=offset,
            )
            request_count += 1
            normalized = _normalize_page(page, start=window_start, end=window_end)
            if not normalized.empty:
                signature = tuple(
                    tuple(
                        None
                        if pd.isna(value)
                        else value.isoformat()
                        if isinstance(value, pd.Timestamp)
                        else str(value)
                        for value in row
                    )
                    for row in normalized.itertuples(index=False, name=None)
                )
                if signature in seen_pages:
                    raise RuntimeError("suspend_d pagination repeated a page without advancing")
                seen_pages.add(signature)
                pages.append(normalized)
            if len(page) < SUSPENSION_PAGE_SIZE:
                break
            offset += SUSPENSION_PAGE_SIZE

    frame = _canonicalize(pages)
    _audit_canonical(frame, query_start=start, query_end=end)
    _write_parquet_atomic(target, frame)
    stats = _frame_stats(frame)
    metadata: dict[str, Any] = {
        "schema_version": 1,
        "status": "complete",
        "source": "tushare",
        "endpoint": "suspend_d",
        "query": {
            "start_date": start,
            "end_date": end,
            "window": "calendar_year",
            "limit": SUSPENSION_PAGE_SIZE,
        },
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        **stats,
        "file": {
            "path": str(target),
            "size_bytes": int(target.stat().st_size),
            "sha256": sha256_file(target),
        },
        "metadata_path": str(metadata_path),
    }
    _write_json_atomic(metadata_path, metadata)
    return _result(metadata, resumed=False, request_count=request_count)


__all__ = [
    "SUSPENSION_COLUMNS",
    "SUSPENSION_FIELDS",
    "SUSPENSION_PAGE_SIZE",
    "audit_suspensions_snapshot",
    "sync_suspensions",
]
