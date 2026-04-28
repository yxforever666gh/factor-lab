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


def _enrich_derived_features(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"])
    if "roe" not in out.columns and {"earnings_yield", "book_yield"}.issubset(out.columns):
        denom = out["book_yield"].replace(0, pd.NA)
        out["roe"] = out["earnings_yield"] / denom
    if "momentum_60" not in out.columns and {"ticker", "close"}.issubset(out.columns):
        out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
        out["momentum_60"] = out.groupby("ticker")["close"].transform(lambda s: s / s.shift(60) - 1.0)
    if "momentum_120" not in out.columns and {"ticker", "close"}.issubset(out.columns):
        out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
        out["momentum_120"] = out.groupby("ticker")["close"].transform(lambda s: s / s.shift(120) - 1.0)
    if "momentum_60_skip_5" not in out.columns and {"ticker", "close"}.issubset(out.columns):
        out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
        out["momentum_60_skip_5"] = out.groupby("ticker")["close"].transform(lambda s: s.shift(5) / s.shift(60) - 1.0)
    return out


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
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        # Corrupt meta files should not keep the workflow in a deterministic-failure loop.
        # Best effort: quarantine the bad meta and force downstream code to treat coverage as unknown.
        corrupt_path = path.with_suffix(path.suffix + f".corrupt.{int(datetime.utcnow().timestamp())}")
        try:
            path.rename(corrupt_path)
        except Exception:
            try:
                path.unlink()
            except Exception:
                pass
        return None


def inspect_feature_store_coverage(
    universe_name: str,
    start_date: str,
    end_date: str,
    cache_dir: str | Path = "artifacts/tushare_cache",
) -> dict[str, object]:
    meta = read_feature_meta(universe_name, cache_dir)
    if not meta:
        return {
            "available": False,
            "covers_exact": False,
            "covers_start": False,
            "min_date": None,
            "max_date": None,
            "stale_days": None,
            "effective_end_date": None,
        }

    min_date = meta.get("min_date")
    max_date = meta.get("max_date")
    try:
        min_ts = pd.Timestamp(min_date) if min_date else None
        max_ts = pd.Timestamp(max_date) if max_date else None
        req_start = pd.Timestamp(start_date)
        req_end = pd.Timestamp(end_date)
    except Exception:
        return {
            "available": False,
            "covers_exact": False,
            "covers_start": False,
            "min_date": min_date,
            "max_date": max_date,
            "stale_days": None,
            "effective_end_date": None,
        }

    covers_start = bool(min_ts is not None and min_ts <= req_start)
    covers_exact = bool(covers_start and max_ts is not None and max_ts >= req_end)
    stale_days = None
    effective_end_date = None
    if max_ts is not None:
        effective_end = min(req_end, max_ts)
        effective_end_date = effective_end.strftime("%Y-%m-%d")
        if max_ts < req_end:
            stale_days = max(0, int((req_end - max_ts).days))
        else:
            stale_days = 0

    return {
        "available": True,
        "covers_exact": covers_exact,
        "covers_start": covers_start,
        "min_date": min_date,
        "max_date": max_date,
        "stale_days": stale_days,
        "effective_end_date": effective_end_date,
    }


def _write_feature_store(frame: pd.DataFrame, universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> None:
    store_path = feature_store_path(universe_name, cache_dir)
    meta_path = feature_store_meta_path(universe_name, cache_dir)
    store_path.parent.mkdir(parents=True, exist_ok=True)

    if frame.empty and not {"date", "ticker"}.issubset(frame.columns):
        ordered = frame.reset_index(drop=True)
    else:
        ordered = frame.sort_values(["date", "ticker"]).reset_index(drop=True)

    ordered.to_parquet(store_path, index=False)
    min_date = ordered["date"].min().strftime("%Y-%m-%d") if (not ordered.empty and "date" in ordered.columns) else None
    max_date = ordered["date"].max().strftime("%Y-%m-%d") if (not ordered.empty and "date" in ordered.columns) else None
    meta = {
        "universe_name": universe_name,
        "min_date": min_date,
        "max_date": max_date,
        "row_count": int(len(ordered)),
        "columns": list(ordered.columns),
        "updated_at_utc": datetime.utcnow().isoformat() + "Z",
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def _restore_corrupt_feature_store(universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> pd.DataFrame:
    store_path = feature_store_path(universe_name, cache_dir)
    prefix = f"{store_path.name}.corrupt."
    candidates = sorted(
        [path for path in store_path.parent.glob(f"{store_path.name}.corrupt.*") if path.name.startswith(prefix)],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for candidate in candidates:
        try:
            frame = pd.read_parquet(candidate)
        except Exception:
            continue
        frame = _enrich_derived_features(frame)
        _write_feature_store(frame, universe_name, cache_dir)
        return frame
    return pd.DataFrame()



def _read_feature_store(universe_name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> pd.DataFrame:
    path = feature_store_path(universe_name, cache_dir)
    if not path.exists():
        return _restore_corrupt_feature_store(universe_name, cache_dir)
    try:
        frame = pd.read_parquet(path)
    except Exception:
        # If the master parquet is corrupted, quarantine it and try the newest readable backup first.
        corrupt_path = path.with_suffix(path.suffix + f".corrupt.{int(datetime.utcnow().timestamp())}")
        try:
            path.rename(corrupt_path)
        except Exception:
            pass
        meta_path = feature_store_meta_path(universe_name, cache_dir)
        if meta_path.exists():
            try:
                meta_path.unlink()
            except Exception:
                pass
        return _restore_corrupt_feature_store(universe_name, cache_dir)
    return _enrich_derived_features(frame)


def _coverage_segments(req_start: pd.Timestamp, req_end: pd.Timestamp, current_min: pd.Timestamp | None, current_max: pd.Timestamp | None) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    if current_min is None or current_max is None:
        return [(req_start - timedelta(days=WARMUP_DAYS), req_end + timedelta(days=FORWARD_LABEL_DAYS))]

    segments: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    if req_start < current_min:
        segments.append((req_start - timedelta(days=WARMUP_DAYS), min(req_end, current_min) + timedelta(days=FORWARD_LABEL_DAYS)))
    if req_end > current_max:
        segments.append((max(req_start, current_max) - timedelta(days=WARMUP_DAYS), req_end + timedelta(days=FORWARD_LABEL_DAYS)))
    return segments


def _merge_feature_frames(existing: pd.DataFrame, incoming_frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in [existing, *incoming_frames] if not frame.empty]
    if not frames:
        return pd.DataFrame()
    merged = pd.concat(frames, ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged.drop_duplicates(subset=["date", "ticker"], keep="last")
    merged = merged.sort_values(["date", "ticker"]).reset_index(drop=True)
    return _enrich_derived_features(merged)


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

    current_min = pd.Timestamp(meta["min_date"]) if meta and meta.get("min_date") else None
    current_max = pd.Timestamp(meta["max_date"]) if meta and meta.get("max_date") else None
    if current_min is not None and current_max is not None and current_min <= req_start and current_max >= req_end:
        if timing:
            timing.set_counter("cache_hit_type", "feature_master_exact")
            timing.set_counter("coverage_fetch_segments", 0)
        return resolved_universe_name

    segments = _coverage_segments(req_start, req_end, current_min, current_max)
    if timing:
        timing.set_counter("cache_hit_type", "none" if current_min is None else "feature_master_incremental")
        timing.set_counter("coverage_fetch_segments", len(segments))

    incoming_frames: list[pd.DataFrame] = []
    for idx, (fetch_start, fetch_end) in enumerate(segments, start=1):
        request = TushareRequest(
            start_date=fetch_start.strftime("%Y-%m-%d"),
            end_date=fetch_end.strftime("%Y-%m-%d"),
            universe_limit=universe_limit,
            cache_dir=cache_dir,
            universe_codes=tickers,
            use_request_cache=True,
        )
        frame = provider.load_dataset(request, timing=timing).frame
        incoming_frames.append(frame)
        if timing:
            timing.set_counter(f"coverage_segment_{idx}", f"{request.start_date}:{request.end_date}")

    merged = _merge_feature_frames(existing, incoming_frames)
    if merged.empty and not existing.empty:
        # Empty incremental pulls should not wipe a readable master store.
        merged = existing.copy()
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
