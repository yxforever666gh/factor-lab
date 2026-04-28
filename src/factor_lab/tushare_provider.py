from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import os
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


@dataclass
class TushareRoutePolicy:
    requested_mode: str
    resolved_mode: str
    proxy_url: str | None
    no_proxy_hosts: list[str]
    connect_timeout_seconds: float
    read_timeout_seconds: float
    request_timeout_seconds: float
    max_retries: int
    probe_on_start: bool
    last_good_route_ttl_seconds: int


def _workspace_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _route_status_path() -> Path:
    return _workspace_root() / "artifacts" / "tushare_route_status.json"


def _split_hosts(raw: str | None) -> list[str]:
    if not raw:
        return ["api.waditu.com"]
    seen: set[str] = set()
    hosts: list[str] = []
    for part in str(raw).split(","):
        host = part.strip()
        if not host or host in seen:
            continue
        seen.add(host)
        hosts.append(host)
    if "api.waditu.com" not in seen:
        hosts.append("api.waditu.com")
    return hosts


def _dedupe_csv(raw: str | None, extra_hosts: list[str]) -> str:
    seen: set[str] = set()
    items: list[str] = []
    for source in [raw or "", ",".join(extra_hosts)]:
        for token in source.split(","):
            value = token.strip()
            if not value or value in seen:
                continue
            seen.add(value)
            items.append(value)
    return ",".join(items)


def _load_route_status() -> dict:
    path = _route_status_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_route_status(payload: dict) -> None:
    path = _route_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class TushareDataProvider:
    PROXY_KEYS = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
    ]

    def __init__(self, token: str | None = None) -> None:
        self.token = token or get_required_env("TUSHARE_TOKEN")
        ts.set_token(self.token)
        self.pro = ts.pro_api(self.token)
        self.route_policy = self._build_route_policy()
        self._apply_request_timeout()
        self._route_status = self._initialize_route_status()

    def _build_route_policy(self) -> TushareRoutePolicy:
        requested_mode = (os.getenv("FACTOR_LAB_TUSHARE_ROUTE_MODE") or "auto").strip().lower()
        if requested_mode not in {"auto", "direct", "proxy", "hybrid"}:
            requested_mode = "auto"
        proxy_url = (os.getenv("FACTOR_LAB_TUSHARE_PROXY_URL") or "").strip() or None
        connect_timeout = max(1.0, float(os.getenv("FACTOR_LAB_TUSHARE_CONNECT_TIMEOUT_SECONDS") or 5))
        read_timeout = max(1.0, float(os.getenv("FACTOR_LAB_TUSHARE_READ_TIMEOUT_SECONDS") or 15))
        max_retries = max(1, int(float(os.getenv("FACTOR_LAB_TUSHARE_MAX_RETRIES") or 2)))
        probe_on_start = (os.getenv("FACTOR_LAB_TUSHARE_ROUTE_PROBE_ON_START") or "1").strip().lower() not in {"0", "false", "no", "off"}
        ttl_seconds = max(60, int(float(os.getenv("FACTOR_LAB_TUSHARE_LAST_GOOD_ROUTE_TTL_SECONDS") or 1800)))
        no_proxy_hosts = _split_hosts(os.getenv("FACTOR_LAB_TUSHARE_NO_PROXY_HOSTS"))

        resolved_mode = requested_mode
        if requested_mode == "hybrid":
            resolved_mode = "hybrid"
        elif requested_mode == "direct":
            resolved_mode = "direct"
        elif requested_mode == "proxy":
            resolved_mode = "proxy" if self._proxy_available(proxy_url) else "direct"
        else:
            resolved_mode = "auto"

        return TushareRoutePolicy(
            requested_mode=requested_mode,
            resolved_mode=resolved_mode,
            proxy_url=proxy_url,
            no_proxy_hosts=no_proxy_hosts,
            connect_timeout_seconds=connect_timeout,
            read_timeout_seconds=read_timeout,
            request_timeout_seconds=max(connect_timeout, read_timeout),
            max_retries=max_retries,
            probe_on_start=probe_on_start,
            last_good_route_ttl_seconds=ttl_seconds,
        )

    def _apply_request_timeout(self) -> None:
        timeout_seconds = float(self.route_policy.request_timeout_seconds)
        for attr in ("_DataApi__timeout", "timeout"):
            try:
                setattr(self.pro, attr, timeout_seconds)
            except Exception:
                continue

    def _proxy_available(self, proxy_url: str | None = None) -> bool:
        proxy = proxy_url or os.getenv("HTTP_PROXY") or os.getenv("http_proxy") or os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        return bool(str(proxy or "").strip())

    def _initialize_route_status(self) -> dict:
        base = {
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "requested_mode": self.route_policy.requested_mode,
            "resolved_mode": self.route_policy.resolved_mode,
            "healthy": True,
            "last_error": None,
            "last_probe_ms": None,
            "proxy_available": self._proxy_available(self.route_policy.proxy_url),
        }
        if self.route_policy.requested_mode != "auto":
            _write_route_status(base)
            return base

        cached = self._load_cached_good_route()
        if cached:
            payload = {
                **base,
                "resolved_mode": cached.get("resolved_mode") or "direct",
                "healthy": True,
                "cached_route": True,
                "last_probe_ms": cached.get("last_probe_ms"),
            }
            self.route_policy.resolved_mode = payload["resolved_mode"]
            _write_route_status(payload)
            return payload

        if not self.route_policy.probe_on_start:
            payload = {
                **base,
                "resolved_mode": "direct",
                "healthy": True,
                "cached_route": False,
                "probe_skipped": True,
            }
            self.route_policy.resolved_mode = "direct"
            _write_route_status(payload)
            return payload

        best = self._probe_best_route()
        if best:
            payload = {
                **base,
                "resolved_mode": best["resolved_mode"],
                "healthy": True,
                "cached_route": False,
                "last_probe_ms": best.get("elapsed_ms"),
                "probe_results": best.get("probe_results") or [],
            }
            self.route_policy.resolved_mode = payload["resolved_mode"]
            _write_route_status(payload)
            return payload

        payload = {
            **base,
            "resolved_mode": "direct",
            "healthy": False,
            "cached_route": False,
            "last_error": "route_probe_failed",
        }
        self.route_policy.resolved_mode = "direct"
        _write_route_status(payload)
        return payload

    def _load_cached_good_route(self) -> dict | None:
        payload = _load_route_status()
        if not payload:
            return None
        if not payload.get("healthy"):
            return None
        resolved_mode = str(payload.get("resolved_mode") or "").strip().lower()
        if resolved_mode not in {"direct", "proxy", "hybrid"}:
            return None
        updated_at = payload.get("updated_at_utc")
        try:
            updated = datetime.fromisoformat(str(updated_at)) if updated_at else None
        except Exception:
            updated = None
        if updated is None:
            return None
        if datetime.now(timezone.utc) - updated > timedelta(seconds=self.route_policy.last_good_route_ttl_seconds):
            return None
        if resolved_mode == "proxy" and not self._proxy_available(self.route_policy.proxy_url):
            return None
        return payload

    def _probe_best_route(self) -> dict | None:
        candidates = ["direct"]
        if self._proxy_available(self.route_policy.proxy_url):
            candidates.append("proxy")
        results: list[dict] = []
        for mode in candidates:
            started = time.perf_counter()
            try:
                self._run_route_probe(mode)
            except Exception as exc:
                results.append({
                    "mode": mode,
                    "ok": False,
                    "error": str(exc),
                })
                continue
            elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
            results.append({"mode": mode, "ok": True, "elapsed_ms": elapsed_ms})
        successes = [row for row in results if row.get("ok")]
        if not successes:
            _write_route_status(
                {
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "requested_mode": self.route_policy.requested_mode,
                    "resolved_mode": "direct",
                    "healthy": False,
                    "proxy_available": self._proxy_available(self.route_policy.proxy_url),
                    "probe_results": results,
                    "last_error": "route_probe_failed",
                }
            )
            return None
        successes.sort(key=lambda row: float(row.get("elapsed_ms") or 0.0))
        best = dict(successes[0])
        best["resolved_mode"] = best["mode"]
        best["probe_results"] = [dict(row) for row in results]
        return best

    def _run_route_probe(self, mode: str) -> None:
        with self._route_env(mode):
            self._apply_request_timeout()
            probe = getattr(self.pro, "stock_basic")(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,area,industry,list_date",
            )
        if probe is None or getattr(probe, "empty", False):
            raise RuntimeError(f"tushare route probe returned empty result via {mode}")

    @contextmanager
    def _route_env(self, mode: str | None = None):
        route_mode = (mode or self.route_policy.resolved_mode or "direct").strip().lower()
        original = {key: os.environ.get(key) for key in self.PROXY_KEYS}
        try:
            if route_mode == "direct":
                for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
                    os.environ.pop(key, None)
                no_proxy = _dedupe_csv(None, self.route_policy.no_proxy_hosts)
                os.environ["NO_PROXY"] = no_proxy
                os.environ["no_proxy"] = no_proxy
            elif route_mode == "hybrid":
                no_proxy = _dedupe_csv(original.get("NO_PROXY") or original.get("no_proxy"), self.route_policy.no_proxy_hosts)
                os.environ["NO_PROXY"] = no_proxy
                os.environ["no_proxy"] = no_proxy
            elif route_mode == "proxy":
                proxy = self.route_policy.proxy_url or original.get("HTTP_PROXY") or original.get("http_proxy") or original.get("HTTPS_PROXY") or original.get("https_proxy")
                if proxy:
                    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
                        os.environ[key] = proxy
                else:
                    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
                        os.environ.pop(key, None)
                no_proxy = _dedupe_csv(None, [])
                if self.route_policy.no_proxy_hosts:
                    no_proxy = _dedupe_csv(None, self.route_policy.no_proxy_hosts)
                if no_proxy:
                    os.environ["NO_PROXY"] = no_proxy
                    os.environ["no_proxy"] = no_proxy
                else:
                    os.environ.pop("NO_PROXY", None)
                    os.environ.pop("no_proxy", None)
            yield
        finally:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def route_status(self) -> dict:
        payload = dict(self._route_status)
        payload.setdefault("resolved_mode", self.route_policy.resolved_mode)
        payload.setdefault("requested_mode", self.route_policy.requested_mode)
        return payload

    def route_healthy(self) -> bool:
        return bool(self.route_status().get("healthy", True))

    def _note_route_success(self, route_mode: str | None = None, elapsed_ms: float | None = None) -> None:
        payload = {
            **self.route_status(),
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "healthy": True,
            "resolved_mode": (route_mode or self.route_policy.resolved_mode),
            "last_error": None,
        }
        if elapsed_ms is not None:
            payload["last_probe_ms"] = round(float(elapsed_ms), 3)
        self._route_status = payload
        _write_route_status(payload)

    def _note_route_error(self, exc: Exception, route_mode: str | None = None) -> None:
        payload = {
            **self.route_status(),
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "healthy": False if self.route_policy.requested_mode == "auto" else self.route_status().get("healthy", True),
            "resolved_mode": (route_mode or self.route_policy.resolved_mode),
            "last_error": str(exc),
        }
        self._route_status = payload
        _write_route_status(payload)

    def _quarantine_cache_file(self, path: Path) -> None:
        quarantine = path.with_suffix(path.suffix + f".invalid.{int(time.time())}")
        try:
            path.rename(quarantine)
        except Exception:
            pass

    def _read_cached_frame(self, path: Path) -> pd.DataFrame | None:
        try:
            return pd.read_csv(path)
        except FileNotFoundError:
            return None
        except Exception:
            self._quarantine_cache_file(path)
            return None

    def _query_with_retry(
        self,
        api_name: str,
        retries: int | None = None,
        sleep_seconds: float = 1.5,
        timing: WorkflowTiming | None = None,
        *,
        route_mode: str | None = None,
        count_metrics: bool = True,
        note_route_status: bool = True,
        **kwargs,
    ):
        last_error = None
        if timing and count_metrics:
            timing.add_counter("api_call_count", 1)
        max_retries = max(1, int(retries or self.route_policy.max_retries))
        effective_mode = route_mode or self.route_policy.resolved_mode
        for attempt in range(1, max_retries + 1):
            started = time.perf_counter()
            try:
                with self._route_env(effective_mode):
                    self._apply_request_timeout()
                    result = getattr(self.pro, api_name)(**kwargs)
                if note_route_status:
                    self._note_route_success(effective_mode, elapsed_ms=(time.perf_counter() - started) * 1000)
                return result
            except Exception as exc:
                last_error = exc
                if note_route_status:
                    self._note_route_error(exc, effective_mode)
                if attempt == max_retries:
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
        frame["momentum_60"] = frame.groupby("ts_code")["close"].transform(lambda s: s / s.shift(60) - 1.0)
        frame["momentum_120"] = frame.groupby("ts_code")["close"].transform(lambda s: s / s.shift(120) - 1.0)
        frame["momentum_60_skip_5"] = frame.groupby("ts_code")["close"].transform(lambda s: s.shift(5) / s.shift(60) - 1.0)
        frame["turnover_ma5"] = frame.groupby("ts_code")["turnover_rate"].transform(lambda s: s.rolling(5).mean())
        frame["turnover_ma20"] = frame.groupby("ts_code")["turnover_rate"].transform(lambda s: s.rolling(20).mean())
        frame["turnover_shock_5_20"] = frame["turnover_ma5"] / frame["turnover_ma20"] - 1.0
        frame["earnings_yield"] = 1.0 / frame["pe_ttm"]
        frame["book_yield"] = 1.0 / frame["pb"]
        frame["roe"] = frame["earnings_yield"] / frame["book_yield"]
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
                "momentum_60",
                "momentum_120",
                "momentum_60_skip_5",
                "turnover_shock_5_20",
                "earnings_yield",
                "book_yield",
                "roe",
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
            frame = self._read_cached_frame(cache_path)
            if frame is not None:
                if frame.empty:
                    self._quarantine_cache_file(cache_path)
                else:
                    frame["date"] = pd.to_datetime(frame["date"])
                    if timing:
                        timing.set_counter("cache_hit_type", "request_exact")
                    return SampleDataset(frame=frame)

        if request.use_request_cache:
            covering_cache = self._find_covering_cache(cache_dir, request)
            if covering_cache is not None:
                frame = self._read_cached_frame(covering_cache)
                if frame is not None:
                    if frame.empty:
                        self._quarantine_cache_file(covering_cache)
                    else:
                        frame["date"] = pd.to_datetime(frame["date"])
                        req_start = pd.Timestamp(request.start_date)
                        req_end = pd.Timestamp(request.end_date)
                        frame = frame[(frame["date"] >= req_start) & (frame["date"] <= req_end)].copy().reset_index(drop=True)
                        if not frame.empty:
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

        if request.use_request_cache and not frame.empty:
            cache_write_started_at = time.perf_counter()
            frame.to_csv(cache_path, index=False)
            if timing:
                timing.metrics_ms["cache_write_ms"] = round((time.perf_counter() - cache_write_started_at) * 1000, 3)
        return SampleDataset(frame=frame)
