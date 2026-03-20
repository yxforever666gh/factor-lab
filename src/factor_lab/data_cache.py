from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from factor_lab.data import SampleDataset
from factor_lab.timing import WorkflowTiming
from factor_lab.tushare_provider import TushareDataProvider, TushareRequest
from factor_lab.universe import default_universe_name, ensure_universe_snapshot


WARMUP_DAYS = 30
FORWARD_LABEL_DAYS = 5


def _feature_dir(cache_dir: str | Path) -> Path:
    return Path(cache_dir).parent / "feature_store"


def feature_store_path(universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> Path:
    return _feature_dir(cache_dir) / f"{universe_name}_master.parquet"


def feature_store_meta_path(universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> Path:
    return _feature_dir(cache_dir) / f"{universe_name}_master.meta.json"


def read_feature_meta(universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> dict | None:
    path = feature_store_meta_path(universe_name, cache_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _write_feature_store(frame: pd.DataFrame, universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> None:
    store_path = feature_store_path(universe_name, cache_dir)
    meta_path = feature_store_meta_path(universe_name, cache_dir)
    store_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = frame.sort_values(["date", "ticker"]).reset_index(drop=True)
    ordered.to_parquet(store_path, index=False)
    meta = {
        "universe_name": universe_name,
        "min_date": ordered["date"].min().strftime("%Y-%m-%d") if not ordered.empty else None,
        "max_date": ordered["date"].max().strftime("%Y-%m-%d") if not ordered.empty else None,
        "row_count": int(len(ordered)),
        "columns": list(ordered.columns),
        "updated_at_utc": datetime.utcnow().isoformat() + "Z",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def _read_feature_store(universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> pd.DataFrame:
    path = feature_store_path(universe_name, cache_dir)
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    if not frame.empty:
        frame["date"] = pd.to_datetime(frame["date"])
    return frame


def ensure_feature_coverage(
    *,
    provider: TushareDataProvider,
    universe_limit: int,
    start_date: str,
    end_date: str,
    cache_dir: str = "artifacts/tushare_cache",
    universe_name: str | None = None,
    timing: WorkflowTiming | None = None,
) -> str:
    resolved_universe_name = universe_name or default_universe_name(universe_limit)
    snapshot = ensure_universe_snapshot(resolved_universe_name, universe_limit, cache_dir=cache_dir, provider=provider)
    tickers = snapshot["tickers"]

    existing = _read_feature_store(resolved_universe_name, cache_dir)
    meta = read_feature_meta(resolved_universe_name, cache_dir)
    req_start = pd.Timestamp(start_date)
    req_end = pd.Timestamp(end_date)

    if meta and meta.get("min_date") and meta.get("max_date"):
        covered = pd.Timestamp(meta["min_date"]) <= req_start and pd.Timestamp(meta["max_date"]) >= req_end
        if covered:
            if timing:
                timing.set_counter("cache_hit_type", "feature_master_exact")
            return resolved_universe_name

    fetch_start = req_start - timedelta(days=WARMUP_DAYS)
    fetch_end = req_end + timedelta(days=FORWARD_LABEL_DAYS)
    if meta and meta.get("min_date") and meta.get("max_date"):
        current_min = pd.Timestamp(meta["min_date"])
        current_max = pd.Timestamp(meta["max_date"])
        fetch_start = min(fetch_start, current_min - timedelta(days=WARMUP_DAYS)) if req_start < current_min else current_min
        fetch_end = max(fetch_end, current_max + timedelta(days=FORWARD_LABEL_DAYS)) if req_end > current_max else current_max
        if timing:
            timing.set_counter("cache_hit_type", "feature_master_extend")
    elif timing:
        timing.set_counter("cache_hit_type", "none")

    request = TushareRequest(
        start_date=fetch_start.strftime("%Y-%m-%d"),
        end_date=fetch_end.strftime("%Y-%m-%d"),
        universe_limit=universe_limit,
        cache_dir=cache_dir,
        universe_codes=tickers,
        use_request_cache=True,
    )
    frame = provider.load_dataset(request, timing=timing).frame

    merged = frame if existing.empty else pd.concat([existing, frame], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged.drop_duplicates(subset=["date", "ticker"], keep="last")
    _write_feature_store(merged, resolved_universe_name, cache_dir)
    return resolved_universe_name


def slice_feature_store(
    universe_name: str,
    start_date: str,
    end_date: str,
    cache_dir: str = "artifacts/tushare_cache",
) -> SampleDataset:
    frame = _read_feature_store(universe_name, cache_dir)
    if frame.empty:
        return SampleDataset(frame=frame)
    req_start = pd.Timestamp(start_date)
    req_end = pd.Timestamp(end_date)
    sliced = frame[(frame["date"] >= req_start) & (frame["date"] <= req_end)].copy().reset_index(drop=True)
    return SampleDataset(frame=sliced)
