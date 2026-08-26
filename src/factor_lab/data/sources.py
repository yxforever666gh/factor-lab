"""Small, resumable raw-market synchronisation adapters."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Mapping, Protocol, Sequence

import pandas as pd

from .catalog import DEFAULT_CONFIG_PATH, RuntimeLayout, load_data_config, sha256_file


DATASET_FIELDS = {
    "daily": "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
    "daily_basic": (
        "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,"
        "ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
    ),
    "adj_factor": "ts_code,trade_date,adj_factor",
}


class MarketDataClient(Protocol):
    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame: ...


class TushareClient:
    """Lazy Tushare Pro wrapper so status/audit commands need no data extra."""

    def __init__(self, token: str | None = None, *, token_env: str = "TUSHARE_TOKEN") -> None:
        resolved_token = token or os.environ.get(token_env)
        if not resolved_token:
            raise RuntimeError(f"missing Tushare token in {token_env}")
        try:
            import tushare as ts
        except ImportError as exc:  # pragma: no cover - depends on optional extra
            raise RuntimeError("Tushare support requires the data optional dependency") from exc
        self._pro = ts.pro_api(resolved_token)

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        target = getattr(self._pro, endpoint, None)
        value = target(**kwargs) if callable(target) else self._pro.query(endpoint, **kwargs)
        if value is None:
            return pd.DataFrame()
        if not isinstance(value, pd.DataFrame):
            raise TypeError(f"Tushare endpoint {endpoint!r} did not return a DataFrame")
        return value.copy()


def _call(client: Any, endpoint: str, **kwargs: Any) -> pd.DataFrame:
    target = getattr(client, endpoint, None)
    if callable(target):
        value = target(**kwargs)
    else:
        query = getattr(client, "query", None)
        if not callable(query):
            raise TypeError(f"data client has no {endpoint!r} endpoint or query method")
        value = query(endpoint, **kwargs)
    if value is None:
        return pd.DataFrame()
    if not isinstance(value, pd.DataFrame):
        raise TypeError(f"data endpoint {endpoint!r} did not return a DataFrame")
    return value.copy()


def _date(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", str(value)):
        raise ValueError(f"date must use YYYY-MM-DD: {value!r}")
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError(f"invalid date: {value!r}")
    return timestamp.strftime("%Y-%m-%d")


def _compact(value: str) -> str:
    return _date(value).replace("-", "")


def _partition_path(raw_root: Path, dataset: str, trade_date: str) -> Path:
    return raw_root / dataset / f"trade_date={_date(trade_date)}" / "part-000.parquet"


def _read_checkpoint(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"schema_version": 1, "partitions": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable data checkpoint: {path}") from exc
    if not isinstance(payload.get("partitions"), Mapping):
        raise ValueError("data checkpoint partitions must be a mapping")
    return payload


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )
    temporary.replace(path)


def _write_parquet_atomic(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _checkpoint_entry_is_valid(entry: Any, path: Path, *, verify_hash: bool) -> bool:
    if not isinstance(entry, Mapping) or entry.get("status") != "complete" or not path.is_file():
        return False
    if int(entry.get("row_count") or 0) <= 0 or int(entry.get("size_bytes") or -1) != path.stat().st_size:
        return False
    expected_hash = str(entry.get("sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        return False
    return not verify_hash or sha256_file(path) == expected_hash


def _audit_partition(frame: pd.DataFrame, dataset: str, trade_date: str) -> None:
    required = {field.strip() for field in DATASET_FIELDS[dataset].split(",") if field.strip()}
    missing = sorted(required - set(frame.columns))
    if frame.empty:
        raise ValueError(f"{dataset}/{trade_date} returned no rows")
    if missing:
        raise ValueError(f"{dataset}/{trade_date} missing columns: {missing}")
    compact_dates = frame["trade_date"].astype("string").str.replace("-", "", regex=False)
    dates = pd.to_datetime(compact_dates, format="%Y%m%d", errors="coerce").dt.strftime(
        "%Y-%m-%d"
    )
    if dates.isna().any() or bool((dates != trade_date).any()):
        raise ValueError(f"{dataset}/{trade_date} contains mismatched trade dates")
    if bool(frame.duplicated(["ts_code", "trade_date"]).any()):
        raise ValueError(f"{dataset}/{trade_date} contains duplicate securities")


def sync_data(
    start_date: str,
    end_date: str,
    *,
    config_path: str | Path = DEFAULT_CONFIG_PATH,
    layout: RuntimeLayout | None = None,
    client: MarketDataClient | Any | None = None,
    datasets: Sequence[str] | None = None,
    resume: bool = True,
    max_partitions: int | None = None,
) -> dict[str, Any]:
    """Synchronise full-market daily Parquet partitions with a local checkpoint."""

    start = _date(start_date)
    end = _date(end_date)
    if end < start:
        raise ValueError("end_date must be on or after start_date")
    config = load_data_config(config_path)
    resolved_layout = layout or RuntimeLayout.from_config(config, config_path=config_path)
    resolved_layout.ensure_directories()
    sync_config = dict(config.get("sync") or {})
    selected_datasets = tuple(datasets or sync_config.get("datasets") or DATASET_FIELDS)
    unknown = sorted(set(selected_datasets) - set(DATASET_FIELDS))
    if unknown:
        raise ValueError(f"unsupported datasets: {unknown}")
    resolved_client = client or TushareClient(token_env=str(sync_config.get("token_env") or "TUSHARE_TOKEN"))
    calendar = _call(
        resolved_client,
        "trade_cal",
        exchange=str(sync_config.get("exchange") or "SSE"),
        start_date=_compact(start),
        end_date=_compact(end),
        fields="exchange,cal_date,is_open,pretrade_date",
    )
    if not {"cal_date", "is_open"}.issubset(calendar.columns):
        raise ValueError("trade_cal response requires cal_date and is_open")
    open_flag = pd.to_numeric(calendar["is_open"], errors="coerce").eq(1)
    open_flag |= calendar["is_open"].astype(str).str.lower().eq("true")
    dates = (
        pd.to_datetime(calendar.loc[open_flag, "cal_date"], errors="coerce")
        .dropna()
        .dt.strftime("%Y-%m-%d")
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    if not dates:
        raise ValueError("trade calendar contains no open dates")

    checkpoint = _read_checkpoint(resolved_layout.checkpoint_path) if resume else {
        "schema_version": 1,
        "partitions": {},
    }
    entries = dict(checkpoint.get("partitions") or {})
    verify_hash = bool(sync_config.get("verify_hashes_on_resume", False))
    pending: list[tuple[str, str, Path, str]] = []
    completed_before = 0
    for trade_date in dates:
        for dataset in selected_datasets:
            key = f"{dataset}/{trade_date}"
            path = _partition_path(resolved_layout.raw_root, dataset, trade_date)
            if resume and _checkpoint_entry_is_valid(entries.get(key), path, verify_hash=verify_hash):
                completed_before += 1
            else:
                pending.append((dataset, trade_date, path, key))
    requested_count = len(pending) if max_partitions is None else min(len(pending), max(0, max_partitions))
    rate = max(0.0, float(sync_config.get("request_rate_per_minute") or 0.0))
    delay = 60.0 / rate if rate else 0.0
    completed_now = 0
    for dataset, trade_date, path, key in pending[:requested_count]:
        frame = _call(
            resolved_client,
            dataset,
            trade_date=_compact(trade_date),
            fields=DATASET_FIELDS[dataset],
        )
        _audit_partition(frame, dataset, trade_date)
        _write_parquet_atomic(path, frame)
        entries[key] = {
            "status": "complete",
            "dataset": dataset,
            "trade_date": trade_date,
            "path": str(path),
            "row_count": int(len(frame)),
            "size_bytes": int(path.stat().st_size),
            "sha256": sha256_file(path),
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        checkpoint = {"schema_version": 1, "partitions": entries}
        _write_json_atomic(resolved_layout.checkpoint_path, checkpoint)
        completed_now += 1
        if delay and completed_now < requested_count:
            time.sleep(delay)
    return {
        "schema_version": 1,
        "status": "complete" if requested_count == len(pending) else "partial",
        "source": "tushare",
        "start_date": dates[0],
        "end_date": dates[-1],
        "open_day_count": len(dates),
        "dataset_count": len(selected_datasets),
        "partition_count": len(dates) * len(selected_datasets),
        "completed_before": completed_before,
        "completed_this_run": completed_now,
        "remaining_partition_count": len(pending) - completed_now,
        "checkpoint_path": str(resolved_layout.checkpoint_path),
        "raw_root": str(resolved_layout.raw_root),
    }


__all__ = ["DATASET_FIELDS", "MarketDataClient", "TushareClient", "sync_data"]
