from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import json
from pathlib import Path
import threading
import time
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import requests

from factor_lab.evaluation import evaluate_factor
from factor_lab.expanded_factor_research import (
    compute_preregistered_factors,
    fixed_sample_labels,
    run_expanded_factor_research,
)
from factor_lab.expanded_market_data import (
    HistoricalSTSnapshot,
    add_adjusted_open_close,
    add_t_plus_1_to_t_plus_6_open_label,
    advance_raw_checkpoint,
    apply_monthly_membership,
    audit_expanded_market_data,
    audit_raw_partition,
    build_expanded_market_data_plan,
    build_monthly_top500_membership,
    build_sha256_manifest,
    fetch_daily_raw_partition,
    fetch_historical_st_history,
    fetch_stock_metadata,
    fetch_trade_calendar,
    filter_verified_raw_checkpoint,
    normalize_historical_st_snapshot,
    normalize_trade_calendar,
    sha256_file,
)
from factor_lab.factors import FactorDefinition, apply_factor, expand_factor_family_config
from factor_lab.long_only_portfolio import LongOnlyPortfolioConfig, evaluate_long_only_portfolio
from factor_lab.research_os.data_quality import DataQualityGate
from factor_lab.tushare_provider import TushareDataProvider


CURRENT_FACTOR_COLUMNS = (
    "momentum_20",
    "momentum_60",
    "momentum_120",
    "momentum_60_skip_5",
    "book_yield",
    "earnings_yield",
    "roe",
)
CORE_COVERAGE_COLUMNS = (
    "date",
    "ticker",
    "open_adj",
    "close_adj",
    "adv_20",
    "volatility_20",
    "forward_return_5d_open",
)


class RetryingTushareClient:
    """Adapt the existing provider retry/route policy to the injected-client API."""

    def __init__(self, provider: TushareDataProvider):
        self.provider = provider

    def query(self, endpoint: str, **kwargs: Any) -> pd.DataFrame:
        return self.provider._query_with_retry(endpoint, **kwargs)


class SessionTushareClient:
    """Tushare HTTP client with connection reuse and bounded retries."""

    def __init__(self, data_api: Any, *, max_retries: int = 3):
        self.url = str(data_api._DataApi__http_url).rstrip("/")
        self.token = str(data_api._DataApi__token)
        self.timeout = int(data_api._DataApi__timeout)
        self.max_retries = max(1, int(max_retries))
        self.session = requests.Session()

    def query(self, endpoint: str, fields: str = "", **kwargs: Any) -> pd.DataFrame:
        params = dict(kwargs)
        params.setdefault("ts_type_name", self.url)
        body = {"api_name": endpoint, "token": self.token, "params": params, "fields": fields}
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(f"{self.url}/{endpoint}", json=body, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if int(payload.get("code") or 0) != 0:
                    raise RuntimeError(str(payload.get("msg") or f"Tushare error {payload.get('code')}"))
                data = payload.get("data") or {}
                return pd.DataFrame(data.get("items") or [], columns=data.get("fields") or [])
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(0.5 * attempt)
        assert last_error is not None
        raise last_error


def load_expanded_config(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if str(payload.get("data_source") or "") != "tushare_market_daily":
        raise ValueError("expanded workflow requires data_source=tushare_market_daily")
    portfolio = payload.get("portfolio") or {}
    if portfolio.get("mode") != "long_only":
        raise ValueError("expanded workflow only supports portfolio.mode=long_only")
    if float(portfolio.get("capital") or 0.0) != 50_000_000.0:
        raise ValueError("expanded workflow is frozen to capital=50000000")
    if int(portfolio.get("rebalance_every_days") or 0) != 5:
        raise ValueError("expanded workflow is frozen to weekly rebalancing")
    return payload


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    last_error: OSError | None = None
    for attempt in range(6):
        try:
            temporary.replace(path)
            return
        except OSError as exc:
            last_error = exc
            time.sleep(0.05 * (attempt + 1))
    assert last_error is not None
    raise last_error


def _write_parquet_atomic(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def resolve_expanded_plan(
    client: Any,
    config: Mapping[str, Any],
    *,
    today: Any | None = None,
    analysis_start: Any | None = None,
    analysis_open_day_limit: int | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Resolve latest completed data date and the last date with a t+6 label."""

    now = pd.Timestamp(today or datetime.now()).normalize()
    calendar_start = pd.Timestamp(config.get("fetch_start_date") or "2016-06-01") - pd.Timedelta(days=45)
    calendar = fetch_trade_calendar(client, start_date=calendar_start, end_date=now)
    open_dates = normalize_trade_calendar(calendar)
    completed = open_dates[open_dates <= now]
    if len(completed) < 127:
        raise RuntimeError("trade calendar does not contain enough completed sessions")
    requested_start = pd.Timestamp(analysis_start or config["start_date"]).normalize()
    analysis_dates = completed[completed >= requested_start]
    if analysis_open_day_limit is not None:
        if analysis_open_day_limit <= 0:
            raise ValueError("analysis_open_day_limit must be positive")
        analysis_dates = analysis_dates[:analysis_open_day_limit]
        completed_for_plan = completed[completed <= analysis_dates[-1] + pd.Timedelta(days=20)]
        if len(completed_for_plan[completed_for_plan > analysis_dates[-1]]) < 6:
            raise RuntimeError("canary calendar does not include six completed label-tail sessions")
        effective_end = analysis_dates[-1]
    else:
        effective_end = completed[-7]
    checkpoint = _read_json(Path(str(config["checkpoint_path"])), {})
    checkpoint, checkpoint_verification = filter_verified_raw_checkpoint(
        checkpoint,
        verify_hashes=bool(config.get("verify_raw_checkpoint_hashes", True)),
    )
    plan = build_expanded_market_data_plan(
        calendar,
        analysis_start=requested_start,
        analysis_end=effective_end,
        raw_root=config["raw_cache_dir"],
        checkpoint=checkpoint,
        warmup_sessions=120,
        forward_label_sessions=6,
    )
    plan.update({
        "latest_completed_trade_date": completed[-1].strftime("%Y-%m-%d"),
        "requested_end": config.get("end_date"),
        "resolved_at_utc": datetime.now(timezone.utc).isoformat(),
        "checkpoint_verification": checkpoint_verification,
    })
    return calendar, plan


def download_raw_partitions(
    client: Any,
    plan: Mapping[str, Any],
    *,
    checkpoint_path: str | Path,
    requests_per_minute: float = 120.0,
    max_partitions: int | None = None,
    progress_every: int = 25,
    workers: int = 1,
    checkpoint_flush_every: int = 25,
) -> dict[str, Any]:
    """Download pending daily partitions with atomic files and resumable hashes."""

    checkpoint_file = Path(checkpoint_path)
    checkpoint = _read_json(checkpoint_file, {})
    pending = [row for row in plan.get("partitions", []) if row.get("status") != "complete"]
    if max_partitions is not None:
        pending = pending[:max(0, int(max_partitions))]
    minimum_interval = 60.0 / max(float(requests_per_minute), 1.0)
    workers = max(1, int(workers))
    checkpoint_flush_every = max(1, int(checkpoint_flush_every))
    rate_lock = threading.Lock()
    next_request_at = [time.monotonic()]

    def fetch_one(partition: Mapping[str, Any]) -> tuple[Mapping[str, Any], pd.DataFrame, dict[str, Any]]:
        with rate_lock:
            now = time.monotonic()
            wait_for = max(0.0, next_request_at[0] - now)
            next_request_at[0] = max(now, next_request_at[0]) + minimum_interval
        if wait_for > 0:
            time.sleep(wait_for)
        frame = fetch_daily_raw_partition(client, partition)
        audit = audit_raw_partition(frame, partition)
        if audit["status"] != "pass":
            raise RuntimeError(f"raw partition audit failed: {audit}")
        return partition, frame, audit

    started = time.monotonic()
    completed_count = 0
    total_rows = 0
    executor = ThreadPoolExecutor(max_workers=workers) if workers > 1 else None
    results = executor.map(fetch_one, pending) if executor else map(fetch_one, pending)
    try:
        for partition, frame, _audit in results:
            path = Path(str(partition["path"]))
            _write_parquet_atomic(path, frame)
            checkpoint = advance_raw_checkpoint(
                checkpoint,
                partition,
                sha256=sha256_file(path),
                row_count=len(frame),
                size_bytes=path.stat().st_size,
                completed_at_utc=datetime.now(timezone.utc).isoformat(),
            )
            completed_count += 1
            total_rows += len(frame)
            if completed_count % checkpoint_flush_every == 0 or completed_count == len(pending):
                _write_json(checkpoint_file, checkpoint)
            if progress_every and completed_count % progress_every == 0:
                print(json.dumps({
                    "event": "download_progress",
                    "completed_this_run": completed_count,
                    "pending_this_run": len(pending),
                    "last_partition": partition["key"],
                    "rows_this_run": total_rows,
                    "workers": workers,
                }, ensure_ascii=False), flush=True)
    finally:
        if completed_count:
            _write_json(checkpoint_file, checkpoint)
        if executor:
            executor.shutdown(wait=False, cancel_futures=True)
    return {
        "status": "complete" if completed_count == len(pending) else "partial",
        "completed_this_run": completed_count,
        "planned_this_run": len(pending),
        "rows_this_run": total_rows,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "checkpoint_path": str(checkpoint_file),
    }


def cache_reference_data(
    client: Any,
    config: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> tuple[pd.DataFrame, HistoricalSTSnapshot, dict[str, Any]]:
    root = Path(str(config["output_dir"])) / "reference"
    metadata_path = root / "stock_metadata.parquet"
    st_path = root / "historical_st.parquet"
    st_status_path = root / "historical_st_status.json"
    if metadata_path.exists():
        metadata = pd.read_parquet(metadata_path)
    else:
        metadata = fetch_stock_metadata(client)
        _write_parquet_atomic(metadata_path, metadata)
    if st_status_path.exists() and st_path.exists():
        status = _read_json(st_status_path, {})
        st_snapshot = normalize_historical_st_snapshot(HistoricalSTSnapshot(
            records=pd.read_parquet(st_path),
            available=bool(status.get("available")),
            degraded=bool(status.get("degraded")),
            reason=status.get("reason"),
        ))
    else:
        st_snapshot = fetch_historical_st_history(
            client,
            start_date=plan["analysis_start"],
            end_date=plan["analysis_end"],
            allow_degraded=True,
        )
        _write_parquet_atomic(st_path, st_snapshot.records)
        _write_json(st_status_path, {
            "available": st_snapshot.available,
            "degraded": st_snapshot.degraded,
            "reason": st_snapshot.reason,
        })
    reference_gate = DataQualityGate()
    reference_gate.check_text_encoding(
        metadata,
        ["name", "area", "industry", "fullname", "market"],
    )
    reference_gate.check_historical_st(
        st_snapshot.records,
        available=st_snapshot.available,
        degraded=st_snapshot.degraded,
        reason=st_snapshot.reason,
    )
    reference_quality = reference_gate.report()
    return metadata, st_snapshot, {
        "metadata_path": str(metadata_path),
        "historical_st_path": str(st_path),
        "historical_st_degraded": st_snapshot.degraded,
        "historical_st_reason": st_snapshot.reason,
        "quality": reference_quality.to_dict(),
        "promotion_allowed": reference_quality.promotion_allowed,
        "trust_labels": ["st_history_unverified"]
        if not st_snapshot.available or st_snapshot.degraded
        else [],
    }


def _partition_lookup(plan: Mapping[str, Any]) -> dict[tuple[str, str], Path]:
    return {
        (str(row["dataset"]), str(row["trade_date"])): Path(str(row["path"]))
        for row in plan.get("partitions", [])
    }


def _read_partition(path: Path, columns: Sequence[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    return pd.read_parquet(path, columns=list(columns) if columns else None)


def build_membership_from_raw_daily(
    config: Mapping[str, Any],
    calendar: pd.DataFrame,
    plan: Mapping[str, Any],
    metadata: pd.DataFrame,
    historical_st: HistoricalSTSnapshot,
) -> Any:
    """Build and persist membership using only all-market Tushare daily data."""

    lookup = _partition_lookup(plan)
    trade_dates = sorted({date for dataset, date in lookup if dataset == "daily"})
    amount_parts = [
        _read_partition(lookup[("daily", date)], columns=["ts_code", "trade_date", "amount"])
        for date in trade_dates
    ]
    daily_amount = pd.concat(amount_parts, ignore_index=True)
    universe_cfg = config.get("universe") or {}
    result = build_monthly_top500_membership(
        daily_amount,
        metadata,
        calendar,
        start_date=plan["analysis_start"],
        end_date=plan["analysis_end"],
        lookback_sessions=int(universe_cfg.get("liquidity_lookback_days") or 60),
        min_amount_observations=int(universe_cfg.get("min_liquidity_observations") or 40),
        min_listing_days=int(universe_cfg.get("min_listing_days") or 180),
        historical_st=historical_st,
        allow_st_degraded=bool(universe_cfg.get("st_filter_degraded_allowed", True)),
    )
    feature_root = Path(str(config["feature_store_dir"]))
    _write_parquet_atomic(feature_root / "monthly_top500_membership.parquet", result.membership)
    _write_json(Path(str(config["output_dir"])) / "membership_audit.json", result.audit)
    return result


def _akshare_hfq_for_ticker(ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    import akshare as ak

    symbol = str(ts_code).split(".", 1)[0]
    prefix = "sh" if str(ts_code).endswith(".SH") else "sz"
    last_error: Exception | None = None
    # Sina has been materially more stable than Eastmoney for long historical
    # windows in this runtime.  Prefer it and retain Eastmoney as fallback.
    for attempt in range(1, 3):
        try:
            frame = ak.stock_zh_a_daily(
                symbol=prefix + symbol,
                start_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                end_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
                adjust="hfq",
            )
            if frame is not None and not frame.empty:
                frame = frame.rename(columns={
                    "date": "trade_date", "open": "open_hfq", "close": "close_hfq",
                    "high": "high_hfq", "low": "low_hfq", "amount": "amount_akshare",
                    "turnover": "turnover_akshare",
                })
                frame["ts_code"] = ts_code
                frame["price_source"] = "akshare_sina_hfq"
                frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
                keep = [column for column in ["ts_code", "trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq", "amount_akshare", "turnover_akshare", "price_source"] if column in frame.columns]
                return frame[keep].dropna(subset=["trade_date"]).drop_duplicates(["ts_code", "trade_date"])
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(float(attempt))
    for attempt in range(1, 4):
        try:
            frame = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                end_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
                adjust="hfq",
            )
            if frame is None or frame.empty:
                return pd.DataFrame(columns=["ts_code", "trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq"])
            aliases = {
                "日期": "trade_date",
                "股票代码": "symbol",
                "开盘": "open_hfq",
                "收盘": "close_hfq",
                "最高": "high_hfq",
                "最低": "low_hfq",
                "成交量": "volume_akshare",
                "成交额": "amount_akshare",
                "换手率": "turnover_akshare",
            }
            frame = frame.rename(columns=aliases)
            required = {"trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq"}
            if not required.issubset(frame.columns):
                raise ValueError(f"AkShare history missing fields: {sorted(required - set(frame.columns))}")
            frame["ts_code"] = ts_code
            frame["price_source"] = "akshare_eastmoney_hfq"
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
            keep = [column for column in ["ts_code", "trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq", "amount_akshare", "turnover_akshare", "price_source"] if column in frame.columns]
            return frame[keep].dropna(subset=["trade_date"]).drop_duplicates(["ts_code", "trade_date"])
        except Exception as exc:
            last_error = exc
            if attempt < 3:
                time.sleep(float(attempt))
    for attempt in range(1, 3):
        try:
            frame = ak.stock_zh_a_daily(
                symbol=prefix + symbol,
                start_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                end_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
                adjust="hfq",
            )
            if frame is None or frame.empty:
                return pd.DataFrame(columns=["ts_code", "trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq", "price_source"])
            frame = frame.rename(columns={
                "date": "trade_date", "open": "open_hfq", "close": "close_hfq",
                "high": "high_hfq", "low": "low_hfq", "amount": "amount_akshare",
                "turnover": "turnover_akshare",
            })
            frame["ts_code"] = ts_code
            frame["price_source"] = "akshare_sina_hfq"
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
            keep = [column for column in ["ts_code", "trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq", "amount_akshare", "turnover_akshare", "price_source"] if column in frame.columns]
            return frame[keep].dropna(subset=["trade_date"]).drop_duplicates(["ts_code", "trade_date"])
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(float(attempt))
    assert last_error is not None
    raise last_error


def _akshare_worker(args: tuple[str, str, str]) -> tuple[str, pd.DataFrame, str | None]:
    code, start_date, end_date = args
    try:
        frame = _akshare_hfq_for_ticker(code, start_date, end_date)
        if frame.empty:
            return code, frame, "empty_history"
        return code, frame, None
    except Exception as exc:
        return code, pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def download_hybrid_supplements(
    client: Any,
    config: Mapping[str, Any],
    plan: Mapping[str, Any],
    membership: Any,
    *,
    max_tickers: int | None = None,
) -> dict[str, Any]:
    """Fetch selected-union valuation data from Tushare and HFQ prices from AkShare."""

    codes = sorted(set(membership.membership["ts_code"].astype(str)))
    if max_tickers is not None:
        codes = codes[:max(0, int(max_tickers))]
    root = Path(str(config["output_dir"])) / "supplements"
    basic_root = root / "tushare_daily_basic"
    akshare_root = root / "akshare_hfq"
    checkpoint_path = root / "supplement_checkpoint.json"
    checkpoint = _read_json(checkpoint_path, {"tushare_daily_basic": {}, "akshare_hfq": {}})
    start_date = str(plan["fetch_start"])
    end_date = str(plan["fetch_end"])
    fields = "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv"
    summary = {"security_count": len(codes), "tushare_completed": 0, "akshare_completed": 0, "failures": []}

    def save_supplement(source: str, code: str, frame: pd.DataFrame, path: Path) -> None:
        _write_parquet_atomic(path, frame)
        checkpoint.setdefault(source, {})[code] = {
            "status": "complete",
            "path": str(path),
            "rows": int(len(frame)),
            "sha256": sha256_file(path),
            "start_date": start_date,
            "end_date": end_date,
        }

    def covers(entry: Mapping[str, Any]) -> bool:
        return (
            entry.get("status") == "complete"
            and str(entry.get("start_date") or "9999-12-31") <= start_date
            and str(entry.get("end_date") or "0000-01-01") >= end_date
            and Path(str(entry.get("path") or "")).exists()
        )

    existing_akshare_coverage = sum(covers(entry) for entry in (checkpoint.get("akshare_hfq") or {}).values()) / max(len(codes), 1)
    retry_missing_akshare = bool(config.get("retry_missing_akshare", True))
    if not retry_missing_akshare and existing_akshare_coverage >= float(config.get("akshare_minimum_initial_coverage") or 0.90):
        pending_ak = []
    else:
        pending_ak = [code for code in codes if not covers((checkpoint.get("akshare_hfq") or {}).get(code, {}))]
    ak_workers = max(1, int(config.get("akshare_workers") or 8))

    with ProcessPoolExecutor(max_workers=ak_workers) as executor:
        jobs = [(code, start_date, end_date) for code in pending_ak]
        akshare_errors: dict[str, str] = {}
        for index, (code, frame, error) in enumerate(executor.map(_akshare_worker, jobs), start=1):
            if error:
                akshare_errors[code] = error
            else:
                path = akshare_root / f"ticker={code}" / "history.parquet"
                save_supplement("akshare_hfq", code, frame, path)
                summary["akshare_completed"] += 1
            if index % 25 == 0:
                _write_json(checkpoint_path, checkpoint)
                print(json.dumps({"event": "akshare_supplement_progress", "completed": index, "total": len(pending_ak)}), flush=True)
    _write_json(checkpoint_path, checkpoint)

    # Delisted or intermittently unavailable symbols may be absent from both
    # AkShare public routes.  Fall back to Tushare per-symbol daily + adj_factor
    # so the final price panel remains fully adjusted and explicitly sourced.
    uncovered_prices = [code for code in codes if not covers((checkpoint.get("akshare_hfq") or {}).get(code, {}))]
    fallback_fields = "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount"
    for index, code in enumerate(uncovered_prices, start=1):
        try:
            daily = client.query(
                "daily", ts_code=code,
                start_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                end_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
                fields=fallback_fields,
            )
            factors = client.query(
                "adj_factor", ts_code=code,
                start_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                end_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
                fields="ts_code,trade_date,adj_factor",
            )
            if daily.empty or factors.empty:
                raise RuntimeError("empty_tushare_price_fallback")
            adjusted = add_adjusted_open_close(daily, factors).rename(columns={
                "open_adj": "open_hfq", "close_adj": "close_hfq",
            })
            adjusted["high_hfq"] = pd.to_numeric(adjusted["high"], errors="coerce") * pd.to_numeric(adjusted["adj_factor"], errors="coerce")
            adjusted["low_hfq"] = pd.to_numeric(adjusted["low"], errors="coerce") * pd.to_numeric(adjusted["adj_factor"], errors="coerce")
            adjusted["price_source"] = "tushare_adj_factor_fallback"
            keep = ["ts_code", "trade_date", "open_hfq", "close_hfq", "high_hfq", "low_hfq", "price_source"]
            path = akshare_root / f"ticker={code}" / "history.parquet"
            save_supplement("akshare_hfq", code, adjusted[keep], path)
            summary["akshare_completed"] += 1
        except Exception as exc:
            summary["failures"].append({
                "source": "adjusted_price_all_sources",
                "ticker": code,
                "error": f"akshare={akshare_errors.get(code)}; tushare={type(exc).__name__}: {exc}",
            })
        if index % 25 == 0:
            _write_json(checkpoint_path, checkpoint)
            print(json.dumps({"event": "price_fallback_progress", "completed": index, "total": len(uncovered_prices)}), flush=True)
    _write_json(checkpoint_path, checkpoint)

    pending_basic = [code for code in codes if not covers((checkpoint.get("tushare_daily_basic") or {}).get(code, {}))]
    supplement_workers = max(1, int(config.get("tushare_supplement_workers") or 1))
    supplement_rate = max(1.0, float(config.get("request_rate_per_minute") or 300))
    supplement_interval = 60.0 / supplement_rate
    supplement_lock = threading.Lock()
    supplement_next_at = [time.monotonic()]

    def fetch_basic(code: str) -> tuple[str, pd.DataFrame, str | None]:
        try:
            with supplement_lock:
                now = time.monotonic()
                wait_for = max(0.0, supplement_next_at[0] - now)
                supplement_next_at[0] = max(now, supplement_next_at[0]) + supplement_interval
            if wait_for > 0:
                time.sleep(wait_for)
            frame = client.query(
                "daily_basic",
                ts_code=code,
                start_date=pd.Timestamp(start_date).strftime("%Y%m%d"),
                end_date=pd.Timestamp(end_date).strftime("%Y%m%d"),
                fields=fields,
            )
            if frame.empty:
                raise RuntimeError("empty_history")
            return code, frame, None
        except Exception as exc:
            return code, pd.DataFrame(), f"{type(exc).__name__}: {exc}"

    with ThreadPoolExecutor(max_workers=supplement_workers) as executor:
        for index, (code, frame, error) in enumerate(executor.map(fetch_basic, pending_basic), start=1):
            if error:
                summary["failures"].append({"source": "tushare_daily_basic", "ticker": code, "error": error})
            else:
                frame["trade_date"] = pd.to_datetime(frame.get("trade_date"), errors="coerce")
                path = basic_root / f"ticker={code}" / "history.parquet"
                save_supplement("tushare_daily_basic", code, frame, path)
                summary["tushare_completed"] += 1
            if index % 25 == 0:
                _write_json(checkpoint_path, checkpoint)
                print(json.dumps({"event": "tushare_basic_supplement_progress", "completed": index, "total": len(pending_basic), "workers": supplement_workers}), flush=True)
    _write_json(checkpoint_path, checkpoint)
    summary["failure_ratio"] = len(summary["failures"]) / max(len(codes) * 2, 1)
    _write_json(root / "supplement_download_summary.json", summary)
    if summary["failure_ratio"] > 0.02:
        raise RuntimeError(f"hybrid supplement failure ratio too high: {summary['failure_ratio']:.2%}")
    return summary


def _read_supplement_frames(root: Path, codes: set[str]) -> pd.DataFrame:
    frames = []
    for code in sorted(codes):
        path = root / f"ticker={code}" / "history.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _derive_market_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    grouped = work.groupby("ts_code", sort=False)
    work["raw_close"] = pd.to_numeric(work["close"], errors="coerce")
    work["close"] = pd.to_numeric(work["close_adj"], errors="coerce")
    work["return_1d"] = grouped["close_adj"].pct_change(fill_method=None)
    work["momentum_20"] = grouped["close_adj"].pct_change(20, fill_method=None)
    work["momentum_60"] = grouped["close_adj"].pct_change(60, fill_method=None)
    work["momentum_120"] = grouped["close_adj"].pct_change(120, fill_method=None)
    work["momentum_60_skip_5"] = grouped["close_adj"].shift(5) / grouped["close_adj"].shift(60) - 1.0
    pe = pd.to_numeric(work.get("pe_ttm"), errors="coerce").replace(0.0, np.nan)
    pb = pd.to_numeric(work.get("pb"), errors="coerce").replace(0.0, np.nan)
    work["earnings_yield"] = 1.0 / pe
    work["book_yield"] = 1.0 / pb
    work["roe"] = work["earnings_yield"] / work["book_yield"].replace(0.0, np.nan)
    amount_rmb = pd.to_numeric(work.get("amount"), errors="coerce") * 1000.0
    work["amount_rmb"] = amount_rmb
    work["adv_20"] = amount_rmb.groupby(work["ts_code"]).transform(lambda values: values.rolling(20, min_periods=10).mean())
    work["volatility_20"] = work["return_1d"].groupby(work["ts_code"]).transform(lambda values: values.rolling(20, min_periods=10).std())
    one_price = (
        pd.to_numeric(work.get("open"), errors="coerce").eq(pd.to_numeric(work.get("high"), errors="coerce"))
        & pd.to_numeric(work.get("high"), errors="coerce").eq(pd.to_numeric(work.get("low"), errors="coerce"))
        & pd.to_numeric(work.get("low"), errors="coerce").eq(pd.to_numeric(work.get("raw_close"), errors="coerce"))
    )
    pct = pd.to_numeric(work.get("pct_chg"), errors="coerce")
    work["is_one_price_limit_up"] = one_price & pct.ge(9.5)
    work["is_one_price_limit_down"] = one_price & pct.le(-9.5)
    work["size_inv"] = -np.log(pd.to_numeric(work.get("total_mv"), errors="coerce").where(lambda value: value > 0))
    return work


def _build_hybrid_selected_features(
    config: Mapping[str, Any],
    calendar: pd.DataFrame,
    plan: Mapping[str, Any],
    metadata: pd.DataFrame,
    membership_result: Any,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Compute full-history features ticker-by-ticker, then retain PIT member months."""

    codes = sorted(set(membership_result.membership["ts_code"].astype(str)))
    supplement_root = Path(str(config["output_dir"])) / "supplements"
    metadata_columns = [column for column in ["ts_code", "industry", "list_date", "delist_date"] if column in metadata.columns]
    metadata_frame = metadata[metadata_columns].drop_duplicates("ts_code").rename(columns={"industry": "industry_current"})
    selected_parts: list[pd.DataFrame] = []
    execution_parts: list[pd.DataFrame] = []
    source_counts: dict[str, int] = {}
    missing_price_codes: list[str] = []
    missing_basic_codes: list[str] = []
    for index, code in enumerate(codes, start=1):
        price_path = supplement_root / "akshare_hfq" / f"ticker={code}" / "history.parquet"
        basic_path = supplement_root / "tushare_daily_basic" / f"ticker={code}" / "history.parquet"
        if not price_path.exists():
            missing_price_codes.append(code)
            continue
        if not basic_path.exists():
            missing_basic_codes.append(code)
            continue
        price = pd.read_parquet(price_path)
        basic = pd.read_parquet(basic_path)
        if price.empty or basic.empty:
            (missing_price_codes if price.empty else missing_basic_codes).append(code)
            continue
        price["trade_date"] = pd.to_datetime(price["trade_date"], errors="coerce").dt.normalize()
        basic["trade_date"] = pd.to_datetime(basic["trade_date"], errors="coerce").dt.normalize()
        work = price.merge(basic, on=["ts_code", "trade_date"], how="left", validate="one_to_one")
        work["open"] = pd.to_numeric(work["open_hfq"], errors="coerce")
        work["high"] = pd.to_numeric(work["high_hfq"], errors="coerce")
        work["low"] = pd.to_numeric(work["low_hfq"], errors="coerce")
        work["close"] = pd.to_numeric(work["close_hfq"], errors="coerce")
        work["open_adj"] = work["open"]
        work["close_adj"] = work["close"]
        work["pct_chg"] = work["close_adj"].pct_change(fill_method=None) * 100.0
        ak_amount = pd.to_numeric(work["amount_akshare"], errors="coerce") if "amount_akshare" in work else pd.Series(np.nan, index=work.index)
        turnover = pd.to_numeric(work["turnover_rate"], errors="coerce") if "turnover_rate" in work else pd.Series(np.nan, index=work.index)
        circ_mv_rmb = (pd.to_numeric(work["circ_mv"], errors="coerce") if "circ_mv" in work else pd.Series(np.nan, index=work.index)) * 10_000.0
        estimated_amount = circ_mv_rmb * turnover / 100.0
        work["amount"] = ak_amount.combine_first(estimated_amount)
        work = _derive_market_features(work)
        work = add_t_plus_1_to_t_plus_6_open_label(work, calendar)
        member_rows = membership_result.membership[membership_result.membership["ts_code"].astype(str) == code]
        member_months = set(member_rows["membership_month"].astype(str))
        work["membership_month"] = work["trade_date"].dt.to_period("M").astype(str)
        work["eligible"] = work["membership_month"].isin(member_months)
        work["universe_member"] = work["eligible"]
        execution_parts.append(work[[
            "trade_date", "ts_code", "open_adj", "adv_20", "volatility_20",
            "eligible", "universe_member", "is_one_price_limit_up", "is_one_price_limit_down",
        ]].copy())
        selected = apply_monthly_membership(work, member_rows)
        selected = selected[
            (selected["trade_date"] >= pd.Timestamp(plan["analysis_start"]))
            & (selected["trade_date"] <= pd.Timestamp(plan["analysis_end"]))
        ]
        if not selected.empty:
            selected_parts.append(selected)
        source = str(price.get("price_source", pd.Series(["unknown"])).iloc[0])
        source_counts[source] = source_counts.get(source, 0) + 1
        if index % 250 == 0:
            print(json.dumps({"event": "hybrid_feature_progress", "tickers": index, "total": len(codes)}), flush=True)
    if not selected_parts:
        raise RuntimeError("hybrid feature builder produced no selected rows")
    selected_market = pd.concat(selected_parts, ignore_index=True)
    execution_market = pd.concat(execution_parts, ignore_index=True)
    selected_market = selected_market.merge(metadata_frame, on="ts_code", how="left", validate="many_to_one")
    audit = {
        "detail_source": "hybrid_tushare_basic_akshare_hfq",
        "price_source_security_counts": source_counts,
        "requested_security_count": len(codes),
        "missing_price_codes": missing_price_codes,
        "missing_basic_codes": missing_basic_codes,
        "price_security_coverage": round((len(codes) - len(missing_price_codes)) / max(len(codes), 1), 6),
        "basic_security_coverage": round((len(codes) - len(missing_basic_codes)) / max(len(codes), 1), 6),
    }
    return selected_market, execution_market, audit


def build_expanded_feature_store(
    config: Mapping[str, Any],
    calendar: pd.DataFrame,
    plan: Mapping[str, Any],
    metadata: pd.DataFrame,
    historical_st: HistoricalSTSnapshot,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Build the PIT top-500 feature store from verified raw partitions."""

    detail_source = str(config.get("market_detail_source") or "tushare_daily_partitions")
    required_datasets = (
        {"daily"}
        if detail_source == "hybrid_tushare_basic_akshare_hfq"
        else {"daily", "daily_basic", "adj_factor"}
    )
    checkpoint = _read_json(Path(str(config["checkpoint_path"])), {})
    partition_gate = DataQualityGate().check_partition_coverage(
        plan,
        checkpoint,
        required_datasets=required_datasets,
        verify_hashes=bool(config.get("verify_raw_checkpoint_hashes", True)),
    )
    partition_quality = partition_gate.raise_if_blocked()
    historical_st = normalize_historical_st_snapshot(historical_st)
    reference_gate = DataQualityGate()
    reference_gate.check_text_encoding(
        metadata,
        ["name", "area", "industry", "fullname", "market"],
    )
    reference_gate.check_historical_st(
        historical_st.records,
        available=historical_st.available,
        degraded=historical_st.degraded,
        reason=historical_st.reason,
    )
    reference_quality = reference_gate.report()

    lookup = _partition_lookup(plan)
    trade_dates = sorted({date for dataset, date in lookup if dataset == "daily"})
    membership_result = build_membership_from_raw_daily(config, calendar, plan, metadata, historical_st)
    selected_codes = set(membership_result.membership["ts_code"].astype(str))
    cross_source_audit: dict[str, Any] = {"detail_source": detail_source}
    if detail_source == "hybrid_tushare_basic_akshare_hfq":
        market, execution_market, cross_source_audit = _build_hybrid_selected_features(
            config, calendar, plan, metadata, membership_result,
        )
    else:
        market_parts: list[pd.DataFrame] = []
        for index, date in enumerate(trade_dates, start=1):
            daily = _read_partition(lookup[("daily", date)])
            market_parts.append(daily[daily["ts_code"].astype(str).isin(selected_codes)])
            if index % 250 == 0:
                print(json.dumps({"event": "feature_build_progress", "dates": index, "total_dates": len(trade_dates)}), flush=True)
        market = pd.concat(market_parts, ignore_index=True)
        adjusted_parts: list[pd.DataFrame] = []
        for date in trade_dates:
            daily = market[market["trade_date"].astype(str).str[:10] == date]
            basic = _read_partition(lookup[("daily_basic", date)])
            basic = basic[basic["ts_code"].astype(str).isin(selected_codes)]
            basic["trade_date"] = pd.to_datetime(basic["trade_date"], errors="coerce").dt.normalize()
            factors = _read_partition(lookup[("adj_factor", date)])
            factors = factors[factors["ts_code"].astype(str).isin(selected_codes)]
            adjusted = add_adjusted_open_close(daily, factors)
            adjusted_parts.append(adjusted.merge(basic, on=["ts_code", "trade_date"], how="left", validate="one_to_one"))
        market = pd.concat(adjusted_parts, ignore_index=True)
        metadata_columns = [column for column in ["ts_code", "industry", "list_date", "delist_date"] if column in metadata.columns]
        metadata_for_merge = metadata[metadata_columns].drop_duplicates("ts_code").rename(columns={"industry": "industry_current"})
        market = market.merge(metadata_for_merge, on="ts_code", how="left", validate="many_to_one")
        market["trade_date"] = pd.to_datetime(market["trade_date"], errors="coerce")
        market = _derive_market_features(market)
        market = add_t_plus_1_to_t_plus_6_open_label(market, calendar)
        market = apply_monthly_membership(market, membership_result)
        market = market[
            (market["trade_date"] >= pd.Timestamp(plan["analysis_start"]))
            & (market["trade_date"] <= pd.Timestamp(plan["analysis_end"]))
        ]
        execution_market = market[[
            "trade_date", "ts_code", "open_adj", "adv_20", "volatility_20",
            "eligible", "universe_member", "is_one_price_limit_up", "is_one_price_limit_down",
        ]].copy()
    selected = market.copy()
    selected["eligible"] = True
    selected["universe_member"] = True
    selected["forward_return_5d"] = selected["forward_return_5d_open"]
    selected = selected.rename(columns={"trade_date": "date", "ts_code": "ticker"})
    selected = selected.sort_values(["date", "ticker"]).reset_index(drop=True)
    factor_columns = list(CURRENT_FACTOR_COLUMNS)
    audit = audit_expanded_market_data(
        selected.rename(columns={"date": "trade_date", "ticker": "ts_code"}),
        factor_columns=factor_columns,
        label_column="forward_return_5d_open",
    )
    core_coverage = {
        column: round(float(selected[column].notna().mean()), 6) if column in selected and len(selected) else 0.0
        for column in CORE_COVERAGE_COLUMNS
    }
    months = membership_result.audit.get("months") or []
    adequate_months = sum(int(row.get("selected_count") or 0) >= 450 for row in months)
    adequate_month_ratio = adequate_months / len(months) if months else 0.0
    acceptance = {
        "duplicate_key_count": audit.get("duplicate_key_count"),
        "core_coverage": core_coverage,
        "minimum_core_coverage": min(core_coverage.values()) if core_coverage else 0.0,
        "adequate_month_ratio": round(adequate_month_ratio, 6),
        "month_count": len(months),
        "partition_quality": partition_quality.to_dict(),
        "reference_quality": reference_quality.to_dict(),
        "passes": (
            audit.get("status") == "pass"
            and all(value >= 0.95 for value in core_coverage.values())
            and adequate_month_ratio >= 0.90
            and partition_quality.promotion_allowed
            and reference_quality.promotion_allowed
        ),
    }
    output_root = Path(str(config["output_dir"]))
    feature_path = Path(str(config["feature_store_dir"])) / "expanded_top500_features.parquet"
    execution_path = Path(str(config["feature_store_dir"])) / "expanded_execution_prices.parquet"
    membership_path = Path(str(config["feature_store_dir"])) / "monthly_top500_membership.parquet"
    audit_path = output_root / "data_audit.json"
    membership_audit_path = output_root / "membership_audit.json"
    _write_parquet_atomic(feature_path, selected)
    execution_market = execution_market.rename(columns={"trade_date": "date", "ts_code": "ticker"})
    execution_market = execution_market[
        (execution_market["date"] >= pd.Timestamp(plan["analysis_start"]))
        & (execution_market["date"] <= pd.Timestamp(plan["latest_completed_trade_date"]))
    ].sort_values(["date", "ticker"]).reset_index(drop=True)
    _write_parquet_atomic(execution_path, execution_market)
    _write_parquet_atomic(membership_path, membership_result.membership)
    audit = {
        **audit,
        "industry_classification_status": "current_snapshot_non_pit",
        "industry_adjusted_factor_promotion_allowed": False,
        "trust_labels": ["st_history_unverified"]
        if not historical_st.available or historical_st.degraded
        else [],
        "cross_source_audit": cross_source_audit,
        "acceptance": acceptance,
    }
    _write_json(audit_path, audit)
    _write_json(membership_audit_path, membership_result.audit)
    manifest = build_sha256_manifest(
        [feature_path, execution_path, membership_path, audit_path, membership_audit_path],
        base_dir=Path.cwd(),
    )
    _write_json(output_root / "data_snapshot_manifest.json", manifest)
    if not acceptance["passes"]:
        raise RuntimeError(f"expanded data acceptance failed: {acceptance}")
    return selected, audit, manifest


def _rank_ic_diagnostics(frame: pd.DataFrame, signal: pd.Series) -> dict[str, Any]:
    rows = pd.DataFrame({
        "date": frame["date"],
        "signal": pd.to_numeric(signal, errors="coerce"),
        "return": pd.to_numeric(frame["forward_return_5d_open"], errors="coerce"),
    }).dropna()
    daily = []
    spreads = []
    for _, group in rows.groupby("date", sort=True):
        if len(group) < 10 or group["signal"].nunique() < 5:
            continue
        daily.append(group["signal"].corr(group["return"], method="spearman"))
        ranked = group.assign(bucket=pd.qcut(group["signal"].rank(method="first"), 5, labels=False))
        spreads.append(ranked.loc[ranked["bucket"] == 4, "return"].mean() - ranked.loc[ranked["bucket"] == 0, "return"].mean())
    return {
        "rank_ic_mean": round(float(pd.Series(daily).mean()), 8) if daily else None,
        "rank_ic_std": round(float(pd.Series(daily).std(ddof=0)), 8) if daily else None,
        "top_bottom_spread_mean": round(float(pd.Series(spreads).mean()), 8) if spreads else None,
        "date_count": len(daily),
    }


def _current_factor_definitions(config_path: str | Path = "configs/factor_families_v1.json") -> list[dict[str, Any]]:
    return expand_factor_family_config(config_path)


def _factor_signal(frame: pd.DataFrame, row: Mapping[str, Any]) -> pd.Series:
    definition = FactorDefinition(str(row["name"]), str(row["expression"]))
    return pd.to_numeric(apply_factor(frame, definition), errors="coerce") * int(row.get("direction") or 1)


def _compact_portfolio_result(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in {"periods", "trades"}}


def run_expanded_rounds(
    frame: pd.DataFrame,
    config: Mapping[str, Any],
    *,
    output_dir: str | Path | None = None,
    execution_frame: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Run the frozen baseline, corrected engine, and six-factor validation rounds."""

    root = Path(output_dir or config["output_dir"])
    definitions = _current_factor_definitions()
    portfolio_config = LongOnlyPortfolioConfig.from_mapping(config)

    round1_dir = root / "round_1_expanded_baseline"
    round1_path = round1_dir / "results.json"
    if round1_path.exists():
        round1_rows = _read_json(round1_path, [])
    else:
        round1_rows = []
        for row in definitions:
            signal = _factor_signal(frame, row)
            panel = frame[["date", "ticker", "forward_return_5d"]].copy()
            panel["factor_value"] = signal
            legacy = evaluate_factor(
                panel,
                str(row["name"]),
                str(row["expression"]),
                {"min_rank_ic": 0.02, "min_top_bottom_spread": 0.0005, "min_sharpe_net": 1.0},
            ).to_dict()
            legacy.update({"family": row.get("family"), "round": "expanded_legacy_baseline", "investment_claim_allowed": False})
            round1_rows.append(legacy)
        _write_json(round1_path, round1_rows)

    round2_rows = []
    composite_parts = []
    round2_dir = root / "round_2_corrected_long_only_v2"
    round2_audit_dir = round2_dir / "strategy_audits"
    for row in definitions:
        signal = _factor_signal(frame, row)
        diagnostics = _rank_ic_diagnostics(frame, signal)
        if row.get("allow_in_long_only", row.get("allow_in_portfolio", True)):
            audit_path = round2_audit_dir / f"{row['name']}.json"
            cached_evaluation = _read_json(audit_path, {})
            if cached_evaluation.get("max_holding_count") is not None:
                full_evaluation = cached_evaluation
            else:
                full_evaluation = evaluate_long_only_portfolio(
                    frame, signal, portfolio_config, pricing_frame=execution_frame,
                ).to_dict()
                _write_json(audit_path, full_evaluation)
            evaluation = _compact_portfolio_result(full_evaluation)
            normalized = signal.groupby(frame["date"]).transform(
                lambda values: (values - values.mean()) / values.std(ddof=0) if values.std(ddof=0) not in (0, 0.0) else 0.0
            )
            composite_parts.append(normalized)
        else:
            evaluation = {"status": "diagnostic_only", "reason": "allow_in_long_only=false"}
        round2_rows.append({
            "name": row["name"], "family": row.get("family"), "direction": row.get("direction", 1),
            "diagnostics": diagnostics, "portfolio": evaluation,
        })
        _write_json(round2_dir / "results.partial.json", round2_rows)
    if composite_parts:
        composite = pd.concat(composite_parts, axis=1).mean(axis=1, skipna=True)
        composite_audit_path = round2_audit_dir / "current_allowed_factors_equal_weight.json"
        cached_composite = _read_json(composite_audit_path, {})
        if cached_composite.get("max_holding_count") is not None:
            full_composite = cached_composite
        else:
            full_composite = evaluate_long_only_portfolio(
                frame, composite, portfolio_config, pricing_frame=execution_frame,
            ).to_dict()
            _write_json(composite_audit_path, full_composite)
        round2_rows.append({
            "name": "current_allowed_factors_equal_weight",
            "family": "composite",
            "direction": 1,
            "diagnostics": _rank_ic_diagnostics(frame, composite),
            "portfolio": _compact_portfolio_result(full_composite),
        })
    _write_json(round2_dir / "results.json", round2_rows)

    research = run_expanded_factor_research(
        frame,
        forward_return_column="forward_return_5d_open",
        q_threshold=float((config.get("multiple_testing") or {}).get("max_q_value") or 0.10),
        non_overlapping_step=5,
    )
    labels = fixed_sample_labels(frame["date"])
    q_lookup = {str(row["name"]): row for row in research.family_trial_ledger}
    round3_rows = []
    round3_dir = root / "round_3_preregistered_validation_v2"
    round3_audit_dir = round3_dir / "strategy_audits"
    for definition in research.definitions:
        signal = research.computation.series_by_name.get(definition.name)
        frozen = research.frozen_directions.get(definition.name)
        if signal is None or frozen is None or frozen.multiplier == 0:
            round3_rows.append({"name": definition.name, "family": definition.family, "status": "unavailable"})
            continue
        windows: dict[str, Any] = {}
        for window in ("train", "validation", "observed_audit"):
            mask = labels == window
            part = frame.loc[mask].copy()
            part_signal = signal.loc[mask] * frozen.multiplier
            if execution_frame is not None:
                bounds = {
                    "train": ("2017-01-01", "2022-12-31"),
                    "validation": ("2023-01-01", "2024-12-31"),
                    "observed_audit": ("2025-01-01", "2100-12-31"),
                }[window]
                execution_part = execution_frame[
                    (pd.to_datetime(execution_frame["date"]) >= pd.Timestamp(bounds[0]))
                    & (pd.to_datetime(execution_frame["date"]) <= pd.Timestamp(bounds[1]))
                ]
            else:
                execution_part = None
            audit_path = round3_audit_dir / definition.name / f"{window}.json"
            cached_window = _read_json(audit_path, {})
            if cached_window.get("max_holding_count") is not None:
                full_window = cached_window
            else:
                full_window = evaluate_long_only_portfolio(
                    part, part_signal, portfolio_config, pricing_frame=execution_part,
                ).to_dict()
                _write_json(audit_path, full_window)
            windows[window] = _compact_portfolio_result(full_window)
        validation = windows["validation"]
        fdr = q_lookup.get(definition.name) or {}
        gate_cfg = config.get("promotion_gate") or {}
        validation_drawdown = validation.get("max_drawdown")
        gate_checks = {
            "fdr": bool(fdr.get("passes_fdr")),
            "net_excess_annual_return": float(validation.get("net_excess_annual_return") or 0.0) > float(gate_cfg.get("validation_net_excess_annual_return_min") or 0.0),
            "net_sharpe": float(validation.get("net_sharpe") or 0.0) >= float(gate_cfg.get("validation_net_sharpe_min") or 0.8),
            "max_drawdown": float(validation_drawdown if validation_drawdown is not None else -1.0) >= float(gate_cfg.get("validation_max_drawdown_min") or -0.25),
            "positive_half_year_ratio": float(validation.get("positive_half_year_ratio") or 0.0) >= float(gate_cfg.get("positive_half_year_ratio_min") or 0.6),
            "average_holding_count": float(validation.get("average_holding_count") or 0.0) >= float(gate_cfg.get("average_holding_count_min") or 40),
            "capacity": int(validation.get("capacity_violation_count") or 0) <= int(gate_cfg.get("capacity_violation_count_max") or 0),
        }
        passed = all(gate_checks.values())
        round3_rows.append({
            "name": definition.name,
            "family": definition.family,
            "frozen_direction": frozen.to_dict(),
            "fdr": fdr,
            "windows": windows,
            "promotion_gate": gate_checks,
            "status": "paper_candidate" if passed else "rejected",
            "investment_claim_allowed": False,
        })
        _write_json(round3_dir / "results.partial.json", round3_rows)
    _write_json(round3_dir / "factor_research.json", research.to_dict())
    _write_json(round3_dir / "results.json", round3_rows)

    comparison = build_round_comparison(round1_rows, round2_rows, round3_rows)
    _write_json(root / "three_round_comparison.json", comparison)
    (root / "three_round_report.md").write_text(render_round_report(comparison), encoding="utf-8")
    artifact_paths = [
        round1_dir / "results.json",
        round2_dir / "results.json",
        round2_audit_dir,
        round3_dir / "factor_research.json",
        round3_dir / "results.json",
        round3_audit_dir,
        root / "three_round_comparison.json",
        root / "three_round_report.md",
    ]
    run_manifest = build_sha256_manifest(artifact_paths, base_dir=Path.cwd())
    _write_json(root / "three_round_manifest.json", run_manifest)
    return comparison


def build_round_comparison(
    round1: Sequence[Mapping[str, Any]],
    round2: Sequence[Mapping[str, Any]],
    round3: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    corrected = sorted(
        [dict(row) for row in round2 if (row.get("portfolio") or {}).get("status") == "ok"],
        key=lambda row: float((row.get("portfolio") or {}).get("net_excess_annual_return") or -999),
        reverse=True,
    )
    validation = sorted(
        [dict(row) for row in round3 if row.get("status") != "unavailable"],
        key=lambda row: float(((row.get("windows") or {}).get("validation") or {}).get("net_excess_annual_return") or -999),
        reverse=True,
    )
    candidates = [row for row in validation if row.get("status") == "paper_candidate"]
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "portfolio_mode": "long_only",
        "capital": 50_000_000,
        "rebalance": "weekly_5_sessions",
        "shorting_used": False,
        "round_1_factor_count": len(round1),
        "round_2_strategy_count": len(corrected),
        "round_3_variant_count": len(round3),
        "paper_candidate_count": len(candidates),
        "best_corrected_strategy": corrected[0] if corrected else None,
        "best_validation_variant": validation[0] if validation else None,
        "paper_candidates": candidates,
        "stopped_without_threshold_relaxation": not bool(candidates),
    }


def render_round_report(comparison: Mapping[str, Any]) -> str:
    lines = [
        "# Expanded Long-Only Research Report",
        "",
        "- Portfolio mode: long only (no shorting)",
        "- Capital: CNY 50,000,000",
        "- Rebalance: every 5 trading sessions",
        f"- Paper candidates: {comparison.get('paper_candidate_count', 0)}",
        "- This report does not claim live profitability.",
        "",
        "## Best corrected current-factor strategy",
        "",
        "```json",
        json.dumps(comparison.get("best_corrected_strategy"), ensure_ascii=False, indent=2, default=str),
        "```",
        "",
        "## Best frozen validation variant",
        "",
        "```json",
        json.dumps(comparison.get("best_validation_variant"), ensure_ascii=False, indent=2, default=str),
        "```",
    ]
    if comparison.get("stopped_without_threshold_relaxation"):
        lines.extend(["", "No variant passed all frozen gates. Research stopped without lowering thresholds."])
    return "\n".join(lines).rstrip() + "\n"


def build_offline_canary_frame(*, stocks: int = 50, analysis_days: int = 20, seed: int = 17) -> pd.DataFrame:
    """Create a deterministic 50-stock/20-rebalance integration fixture."""

    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2021-01-04", periods=max(analysis_days * 5 + 7, 120))
    rows = []
    for ticker_index in range(stocks):
        shocks = rng.normal(0.0002 + ticker_index / 1_000_000, 0.012, len(dates))
        close = 20.0 * np.cumprod(1.0 + shocks)
        for date_index, date in enumerate(dates):
            rows.append({
                "date": date,
                "ticker": f"T{ticker_index:04d}",
                "open_adj": close[date_index],
                "close_adj": close[date_index] * (1 + shocks[date_index] / 4),
                "close": close[date_index],
                "adv_20": 200_000_000.0,
                "volatility_20": 0.02,
                "eligible": True,
                "universe_member": True,
                "is_one_price_limit_up": False,
                "is_one_price_limit_down": False,
                "forward_return_5d_open": close[min(date_index + 6, len(close) - 1)] / close[min(date_index + 1, len(close) - 1)] - 1.0 if date_index + 6 < len(close) else np.nan,
                "forward_return_5d": close[min(date_index + 6, len(close) - 1)] / close[min(date_index + 1, len(close) - 1)] - 1.0 if date_index + 6 < len(close) else np.nan,
            })
    frame = pd.DataFrame(rows)
    frame["signal"] = frame.groupby("date")["ticker"].transform(lambda values: np.arange(len(values), dtype=float))
    return frame


def run_offline_canary(config: Mapping[str, Any]) -> dict[str, Any]:
    frame = build_offline_canary_frame()
    result = evaluate_long_only_portfolio(frame, "signal", LongOnlyPortfolioConfig.from_mapping(config))
    payload = result.to_dict()
    checks = {
        "status_ok": payload["status"] == "ok",
        "no_shorting": all(weight >= 0 for period in payload["periods"] for weight in period["weights"].values()),
        "capacity_respected": payload["capacity_usage"] <= float((config.get("portfolio") or {}).get("max_adv_participation") or 0.05) + 1e-12,
        "has_rebalances": payload["rebalance_count"] >= 10,
    }
    output = {"checks": checks, "passes": all(checks.values()), "evaluation": payload}
    _write_json(Path(str(config["output_dir"])) / "canary" / "offline_canary.json", output)
    if not output["passes"]:
        raise RuntimeError(f"offline canary failed: {checks}")
    return output


__all__ = [
    "RetryingTushareClient",
    "SessionTushareClient",
    "load_expanded_config",
    "resolve_expanded_plan",
    "download_raw_partitions",
    "cache_reference_data",
    "build_membership_from_raw_daily",
    "download_hybrid_supplements",
    "build_expanded_feature_store",
    "run_expanded_rounds",
    "build_round_comparison",
    "render_round_report",
    "build_offline_canary_frame",
    "run_offline_canary",
]
