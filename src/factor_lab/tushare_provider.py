from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time

import numpy as np
import pandas as pd
import tushare as ts

from factor_lab.data import SampleDataset
from factor_lab.settings import get_required_env


@dataclass
class TushareRequest:
    start_date: str
    end_date: str
    universe_limit: int = 80
    cache_dir: str = "artifacts/tushare_cache"


class TushareDataProvider:
    def __init__(self, token: str | None = None) -> None:
        self.token = token or get_required_env("TUSHARE_TOKEN")
        ts.set_token(self.token)
        self.pro = ts.pro_api(self.token)

    def _query_with_retry(self, api_name: str, retries: int = 4, sleep_seconds: float = 1.5, **kwargs):
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                return getattr(self.pro, api_name)(**kwargs)
            except Exception as exc:  # network / transient API failures
                last_error = exc
                if attempt == retries:
                    raise
                time.sleep(sleep_seconds * attempt)
        raise last_error

    def load_dataset(self, request: TushareRequest) -> SampleDataset:
        cache_dir = Path(request.cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_key = f"tushare_{request.start_date}_{request.end_date}_{request.universe_limit}.csv"
        cache_path = cache_dir / cache_key
        if cache_path.exists():
            frame = pd.read_csv(cache_path)
            frame["date"] = pd.to_datetime(frame["date"])
            return SampleDataset(frame=frame)

        stock_basic = self._query_with_retry(
            "stock_basic",
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date",
        )
        stock_basic = stock_basic[stock_basic["ts_code"].str.endswith((".SH", ".SZ"))].copy()
        stock_basic["list_date_dt"] = pd.to_datetime(stock_basic["list_date"], format="%Y%m%d", errors="coerce")
        universe_meta = stock_basic.sort_values("list_date_dt").head(request.universe_limit).copy()
        universe_codes = universe_meta["ts_code"].tolist()

        daily_parts = []
        daily_basic_parts = []
        start = request.start_date.replace("-", "")
        end = request.end_date.replace("-", "")
        for ts_code in universe_codes:
            daily_parts.append(
                self._query_with_retry(
                    "daily",
                    ts_code=ts_code,
                    start_date=start,
                    end_date=end,
                    fields="ts_code,trade_date,open,high,low,close,vol,amount,pct_chg",
                )
            )
            daily_basic_parts.append(
                self._query_with_retry(
                    "daily_basic",
                    ts_code=ts_code,
                    start_date=start,
                    end_date=end,
                    fields="ts_code,trade_date,turnover_rate,pe_ttm,pb,total_mv",
                )
            )

        daily = pd.concat(daily_parts, ignore_index=True)
        daily_basic = pd.concat(daily_basic_parts, ignore_index=True)
        frame = daily.merge(daily_basic, on=["ts_code", "trade_date"], how="inner")
        frame = frame.merge(universe_meta[["ts_code", "industry", "list_date"]], on="ts_code", how="left")
        frame["trade_date"] = pd.to_datetime(frame["trade_date"])
        frame["list_date"] = pd.to_datetime(frame["list_date"], format="%Y%m%d", errors="coerce")
        frame = frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        frame = frame[(frame["close"] > 0) & (frame["pb"] > 0) & (frame["pe_ttm"] > 0) & (frame["total_mv"] > 0)]
        frame["days_since_list"] = (frame["trade_date"] - frame["list_date"]).dt.days
        frame = frame[frame["days_since_list"] >= 180]

        avg_mv = frame.groupby("ts_code")["total_mv"].mean().sort_values(ascending=False)
        universe = avg_mv.head(request.universe_limit).index
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

        frame.to_csv(cache_path, index=False)
        return SampleDataset(frame=frame)
