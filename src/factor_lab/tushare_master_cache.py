from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd


@dataclass
class MasterCacheSpec:
    universe_limit: int
    start_date: str
    end_date: str
    cache_dir: str = 'artifacts/tushare_cache'


def master_cache_path(spec: MasterCacheSpec) -> Path:
    root = Path(spec.cache_dir)
    root.mkdir(parents=True, exist_ok=True)
    return root / f'master_tushare_{spec.start_date}_{spec.end_date}_{spec.universe_limit}.csv'


def find_covering_master_cache(cache_dir: str | Path, universe_limit: int, start_date: str, end_date: str) -> Path | None:
    cache_dir = Path(cache_dir)
    req_start = pd.Timestamp(start_date)
    req_end = pd.Timestamp(end_date)
    best: tuple[int, Path] | None = None
    for path in cache_dir.glob('master_tushare_*.csv'):
        parts = path.stem.split('_')
        if len(parts) < 5:
            continue
        try:
            start = pd.Timestamp(parts[2])
            end = pd.Timestamp(parts[3])
            limit = int(parts[4])
        except Exception:
            continue
        if limit != universe_limit:
            continue
        if start <= req_start and end >= req_end:
            span = int((end - start).days)
            if best is None or span < best[0]:
                best = (span, path)
    return best[1] if best else None


def slice_master_cache(path: str | Path, start_date: str, end_date: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    frame['date'] = pd.to_datetime(frame['date'])
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    return frame[(frame['date'] >= start) & (frame['date'] <= end)].copy().reset_index(drop=True)
