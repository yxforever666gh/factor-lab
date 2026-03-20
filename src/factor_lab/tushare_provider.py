from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import time

import numpy as np
import pandas as pd
import tushare as ts

from factor_lab.data import SampleDataset
from factor_lab.settings import get_required_env
from factor_lab.timing import WorkflowTiming


@dataclass
class TushareRequest:
    start_date: str
    end_date: str
    universe_limit: int = 80
    cache_dir: str = "artifacts/tushare_cache"
    universe_codes: list[str] | None = None
    use_request_cache: bool = True


class TushareDataProvider:
    def __init__(self, token: str | None = None) -> None:
        self.token = token or get_required_env("TUSHARE_TOKEN")
        ts.set_token(self.token)
        self.pro = ts.pro_api(self.token)

    def _query_with_retry(self, api_name: str, retries: int = 4, sleep_seconds: float = 1.5, timing: WorkflowTiming | None = None, **kwargs):
        last_error = None
        if timing:
            timing.add_counter("api_call_count", 1)
        for attempt in range(1, retries + 1):
            try:
                return getattr(self.pro, api_name)(**kwargs)
            except Exception as exc:
                last_error = exc
                if attempt == retries:
                    raise
                time.sleep(sleep_seconds * attempt)
        raise last_error

    def fetch_stock_basic(self, timing: WorkflowTiming | None = None) -> pd.DataFrame:
        started_at = time.perf_counter()
        stock_basic = self._query_with_retry(
            "stock_basic",
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date",
            timing=timing,
        )
        stock_basic = stock_basic[stock_basic["ts_code"].str.endswith((".SH", ".SZ"))].copy()
        stock_basic["list_date_dt"] = pd.to_datetime(stock_basic["list_date"], format="%Y%m%d", errors="coerce")
        if timing:
            timing.metrics_ms["stock_basic_ms"] = round((time.perf_counter() - started_at) * 1000, 3)
        return stock_basic

    def _cache_candidates(self, cache_dir: Path, universe_limit: int):
        pattern = re.compile(r"tushare_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(\d+)\.csv")
        for path in cache_dir.glob("tushare_*.csv"):
            m = pattern.fullmatch(path.name)
            if not m:
                continue
            start_date, end_date, limit = m.group(1), m.group(2), int(m.group(3))
            if limit != universe_limit:
                continue
            yield path, start_date, end_date

    def _find_covering_cache(self, cache_dir: Path, request: TushareRequest):
        matches = []
        req_start = pd.Timestamp(request.start_date)
        req_end = pd.Timestamp(request.end_date)
        for path, start_date, end_date in self._cache_candidates(cache_dir, request.universe_limit):
            start_ts = pd.Timestamp(start_date)
            end_ts = pd.Timestamp(end_date)
            if start_ts <= req_start and end_ts >= req_end:
                span = (end_ts - start_ts).days
                matches.append((span, path))
        if not matches:
            return None
        matches.sort(key=lambda item: item[0])
        return matches[0][1]

    def _date_chunks(self, start_date: str, end_date: str):
        start = pd.Timestamp(start_date)
        end = pd.Timestamp(end_date)
        current = start
        while current <= end:
            chunk_end = min(current + pd.Timedelta(days=89), end)
            yield current.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")
            current = chunk_end + pd.Timedelta(days=1)

    def _fetch_market_data(self, universe_codes: list[str], start_date: str, end_date: str, timing: WorkflowTiming | None = None):
        daily_parts = []
        daily_basic_parts = []
        daily_started_at = time.perf_counter()
        for ts_code in universe_codes:
            for chunk_start, chunk_end in self._date_chunks(start_date, end_date):
                daily_parts.append(
                    self._query_with_retry(
                        "daily",
                        ts_code=ts_code,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        fields="ts_code,trade_date,open,high,low,close,vol,amount,pct_chg",
                        timing=timing,
                    )
                )
                daily_basic_parts.append(
                    self._query_with_retry(
                        "daily_basic",
                        ts_code=ts_code,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        fields="ts_code,trade_date,turnover_rate,pe_ttm,pb,total_mv",
                        timing=timing,
                    )
                )
        if timing:
            elapsed_ms = round((time.perf_counter() - daily_started_at) * 1000, 3)
            timing.metrics_ms["daily_fetch_ms"] = elapsed_ms
            timing.metrics_ms["daily_basic_fetch_ms"] = elapsed_ms
        return pd.concat(daily_parts, ignore_index=True), pd.concat(daily_basic_parts, ignore_index=True)

    def _build_feature_frame(self, daily: pd.DataFrame, daily_basic: pd.DataFrame, universe_meta: pd.DataFrame, request: TushareRequest, timing: WorkflowTiming | None = None) -> pd.DataFrame:
        started_at = time.perf_counter()
        frame = daily.merge(daily_basic, on=["ts_code", "trade_date"], how="inner")
        frame = frame.merge(universe_meta[["ts_code", "industry", "list_date"]], on="ts_code", how="left")
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["list_date"] = pd.to_datetime(frame["list_date"], format="%Y%m%d", errors="coerce")
        frame = frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        frame = frame[(frame["close"] > 0) & (frame["pb"] > 0) & (frame["pe_ttm"] > 0) & (frame["total_mv"] > 0)]
        frame["days_since_list"] = (frame["trade_date"] - frame["list_date"]).dt.days
        frame = frame[frame["days_since_list"] >= 180]

        avg_mv = frame.groupby("ts_code")["total_mv"].mean().sort_values(ascending=False)
        universe = request.universe_codes or avg_mv.head(request.universe_limit).index.tolist()
        frame = frame[frame["ts_code"].isin(universe)].copy()

        frame["return_1d"] = frame.groupby("ts_code")["close"].pct_change().fillna(0.0)
        frame["forward_return_5d"] = frame.groupby("ts_code")["close"].transform(lambda s: s.shift(-5) / s.shift(-1) - 1.0)
        frame["momentum_20"] = frame.groupby("ts_code")["close"].transform(lambda s: s / s.shift(20) - 1.0)
        frame["turnover_ma5"] = frame.groupby("ts_code")["turnover_rate"].transform(lambda s: s.rolling(5).mean())
        frame["turnover_ma20"] = frame.groupby("ts_code")["turnover_rate"].transform(lambda s: s.rolling(20).mean())
        frame["turnover_shock_5_20"] = frame["turnover_ma5"] / frame["turnover_ma20"] - 1.0
        frame["earnings_yield"] = 1.0 / frame["pe_ttm"]
        frame["book_yield"] = 1.0 / frame["pb"]
        frame["size_inv"] = -np.log(frame["total_mv"])

        frame = frame.rename(columns={"trade_date": "date", "ts_code": "ticker", "turnover_rate": "turnover"})
        frame = frame[
            [
                "date",
                "ticker",
                "industry",
                "close",
                "return_1d",
                "forward_return_5d",
                "turnover",
                "momentum_20",
                "turnover_shock_5_20",
                "earnings_yield",
                "book_yield",
                "size_inv",
                "pe_ttm",
                "pb",
                "total_mv",
            ]
        ].dropna().reset_index(drop=True)
        if timing:
            timing.metrics_ms["merge_clean_ms"] = round((time.perf_counter() - started_at) * 1000, 3)
            timing.metrics_ms["feature_build_ms"] = timing.metrics_ms["merge_clean_ms"]
        return frame

    def load_dataset(self, request: TushareRequest, timing: WorkflowTiming | None = None) -> SampleDataset:
        cache_dir = Path(request.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key = f"tushare_{request.start_date}_{request.end_date}_{request.universe_limit}.csv"
        cache_path = cache_dir / cache_key

        if request.use_request_cache and cache_path.exists():
            frame = pd.read_csv(cache_path)
            frame["date"] = pd.to_datetime(frame["date"])
            if timing:
                timing.set_counter("cache_hit_type", "request_exact")
            return SampleDataset(frame=frame)

        if request.use_request_cache:
            covering_cache = self._find_covering_cache(cache_dir, request)
            if covering_cache is not None:
                frame = pd.read_csv(covering_cache)
                frame["date"] = pd.to_datetime(frame["date"])
                req_start = pd.Timestamp(request.start_date)
                req_end = pd.Timestamp(request.end_date)
                frame = frame[(frame["date"] >= req_start) & (frame["date"] <= req_end)].copy().reset_index(drop=True)
                frame.to_csv(cache_path, index=False)
                if timing:
                    timing.set_counter("cache_hit_type", "request_covering")
                return SampleDataset(frame=frame)

        if request.universe_codes:
            stock_basic = self.fetch_stock_basic(timing=timing)
            universe_meta = stock_basic[stock_basic["ts_code"].isin(request.universe_codes)].copy()
            universe_codes = list(request.universe_codes)
        else:
            stock_basic = self.fetch_stock_basic(timing=timing)
            universe_meta = stock_basic.sort_values("list_date_dt").head(request.universe_limit).copy()
            universe_codes = universe_meta["ts_code"].tolist()

        daily, daily_basic = self._fetch_market_data(
            universe_codes=universe_codes,
            start_date=request.start_date,
            end_date=request.end_date,
            timing=timing,
        )
        frame = self._build_feature_frame(daily, daily_basic, universe_meta, request, timing=timing)

        if request.use_request_cache:
            cache_write_started_at = time.perf_counter()
            frame.to_csv(cache_path, index=False)
            if timing:
                timing.metrics_ms["cache_write_ms"] = round((time.perf_counter() - cache_write_started_at) * 1000, 3)
        return SampleDataset(frame=frame)
