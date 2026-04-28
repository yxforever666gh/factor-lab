from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.tushare_master_cache import MasterCacheSpec, master_cache_path


ROOT = Path(__file__).resolve().parents[2]


def maybe_materialize_master_from_run(output_dir: str | Path, universe_limit: int = 20, cache_dir: str = 'artifacts/tushare_cache') -> Path | None:
    output_dir = Path(output_dir)
    dataset_path = output_dir / 'dataset.csv'
    if not dataset_path.exists():
        return None

    frame = pd.read_csv(dataset_path)
    if 'date' not in frame.columns or frame.empty:
        return None

    frame['date'] = pd.to_datetime(frame['date'])
    start_date = frame['date'].min().strftime('%Y-%m-%d')
    end_date = frame['date'].max().strftime('%Y-%m-%d')

    spec = MasterCacheSpec(
        universe_limit=universe_limit,
        start_date=start_date,
        end_date=end_date,
        cache_dir=cache_dir,
    )
    path = master_cache_path(spec)
    if not path.exists():
        frame.to_csv(path, index=False)
    return path


def materialize_recent_generated_runs(db_path: str | Path, limit: int = 20) -> list[str]:
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT output_dir, universe_limit
            FROM workflow_runs
            WHERE status='finished' AND output_dir LIKE 'artifacts/generated_%'
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    created = []
    for output_dir, universe_limit in rows:
        path = maybe_materialize_master_from_run(output_dir, universe_limit or 20)
        if path is not None:
            created.append(str(path))
    return created
