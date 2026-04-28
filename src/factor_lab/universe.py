from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab.tushare_provider import TushareDataProvider


def _universe_dir(cache_dir: str | Path) -> Path:
    return Path(cache_dir).parent / "universes"


def default_universe_name(limit: int) -> str:
    return f"tushare_top_{int(limit)}"


def universe_snapshot_path(name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> Path:
    return _universe_dir(cache_dir) / f"{name}.json"


def load_universe_snapshot(name: str, cache_dir: str | Path = "artifacts/tushare_cache") -> dict | None:
    path = universe_snapshot_path(name, cache_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_universe_snapshot(name: str, tickers: list[str], metadata: dict | None = None, cache_dir: str | Path = "artifacts/tushare_cache") -> Path:
    path = universe_snapshot_path(name, cache_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "universe_name": name,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ticker_count": len(tickers),
        "tickers": tickers,
        "metadata": metadata or {},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def ensure_universe_snapshot(
    universe_name: str | None,
    universe_limit: int,
    cache_dir: str | Path = "artifacts/tushare_cache",
    provider: TushareDataProvider | None = None,
) -> dict:
    name = universe_name or default_universe_name(universe_limit)
    existing = load_universe_snapshot(name, cache_dir)
    if existing:
        return existing

    provider = provider or TushareDataProvider()
    stock_basic = provider.fetch_stock_basic()
    stock_basic = stock_basic.sort_values("list_date_dt")
    selected = stock_basic.head(universe_limit).copy()
    tickers = selected["ts_code"].tolist()
    metadata = {
        "selection_rule": "earliest_listed_equities",
        "universe_limit": universe_limit,
    }
    save_universe_snapshot(name, tickers, metadata=metadata, cache_dir=cache_dir)
    return load_universe_snapshot(name, cache_dir) or {"universe_name": name, "tickers": tickers, "metadata": metadata}
