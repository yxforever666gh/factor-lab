#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.earnings_event_source import (
    asof_merge_earnings_event_features,
    build_earnings_event_features,
    build_earnings_event_report,
    build_earnings_event_statement_features,
    event_preflight,
    normalize_event_frame,
)
from factor_lab.margin_source_mvp import choose_feature_cache, load_feature_cache
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/earnings_event_source_mvp")
JSON_OUT = ARTIFACT_DIR / "earnings_event_source_mvp.json"
MD_OUT = ARTIFACT_DIR / "earnings_event_source_mvp.md"
RAW_OUT = ARTIFACT_DIR / "earnings_event_raw_sample.csv"
STATEMENT_OUT = ARTIFACT_DIR / "earnings_event_statement_features.csv"
ASOF_OUT = ARTIFACT_DIR / "earnings_event_daily_asof_features.csv"
KNOWLEDGE_OUT = Path("knowledge/earnings_event_source_mvp.md")
ENDPOINTS = ("forecast", "express")
MAX_SAMPLE_CODES = 60
DEFAULT_CODES = ["000001.SZ", "000002.SZ", "000333.SZ", "600000.SH", "600519.SH", "601318.SH", "300750.SZ"]


def _json_safe(obj: Any) -> Any:
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return str(obj)


def choose_sample_codes() -> list[str]:
    path = choose_feature_cache()
    if path is None:
        return DEFAULT_CODES
    try:
        tickers = pd.read_csv(path, usecols=["ticker"])["ticker"].dropna().astype(str).drop_duplicates().tolist()
        return tickers[:MAX_SAMPLE_CODES] or DEFAULT_CODES
    except Exception:
        return DEFAULT_CODES


def _bounded_call(pro: Any, endpoint: str, code: str) -> pd.DataFrame:
    fn = getattr(pro, endpoint)
    # Tushare forecast is safer with per-ticker calls; express also accepts ts_code.
    variants = ({"ts_code": code},)
    for params in variants:
        try:
            df = fn(**params)
            if df is not None and len(df):
                return normalize_event_frame(df, endpoint=endpoint).head(120)
        except Exception:
            pass
        time.sleep(0.03)
    return pd.DataFrame()


def fetch_tushare_events(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    meta: dict[str, Any] = {"auth_ready": bool(token), "sample_codes": sample_codes, "endpoints": list(ENDPOINTS), "errors": {}}
    if not token:
        meta["error"] = "missing_tushare_token"
        return {e: pd.DataFrame() for e in ENDPOINTS}, meta
    try:
        import tushare as ts  # type: ignore
        pro = ts.pro_api(token)
    except Exception as exc:
        meta["error"] = f"tushare_import_or_auth_failed: {type(exc).__name__}: {exc}"[:500]
        return {e: pd.DataFrame() for e in ENDPOINTS}, meta
    frames: dict[str, list[pd.DataFrame]] = {e: [] for e in ENDPOINTS}
    errors: dict[str, str] = {}
    for endpoint in ENDPOINTS:
        if not hasattr(pro, endpoint):
            errors[endpoint] = "method_not_found"
            continue
        for code in sample_codes:
            try:
                df = _bounded_call(pro, endpoint, code)
                if len(df):
                    frames[endpoint].append(df)
            except Exception as exc:
                errors[f"{endpoint}:{code}"] = f"{type(exc).__name__}: {exc}"[:500]
    out = {endpoint: (pd.concat(parts, ignore_index=True).drop_duplicates() if parts else pd.DataFrame()) for endpoint, parts in frames.items()}
    meta["errors"] = errors
    meta["rows_by_endpoint"] = {k: int(len(v)) for k, v in out.items()}
    return out, meta


def load_or_fetch_events(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    frames = {"forecast": pd.DataFrame(), "express": pd.DataFrame()}
    sources: dict[str, str] = {}
    if RAW_OUT.exists():
        raw = pd.read_csv(RAW_OUT)
        for endpoint in ENDPOINTS:
            if "endpoint" in raw.columns:
                part = raw[raw["endpoint"].astype(str) == endpoint].copy()
            else:
                part = pd.DataFrame()
            if len(part):
                frames[endpoint] = normalize_event_frame(part, endpoint=endpoint)
                sources[endpoint] = str(RAW_OUT)
    express_cache = Path("artifacts/express_event_mvp/express_raw.csv")
    if express_cache.exists() and frames["express"].empty:
        frames["express"] = normalize_event_frame(pd.read_csv(express_cache), endpoint="express")
        sources["express"] = str(express_cache)
    # No forecast cache exists in the latest state; bounded per-ticker pull only if needed/available.
    forecast_cache = Path("artifacts/forecast_event_mvp/forecast_raw.csv")
    if forecast_cache.exists():
        frames["forecast"] = normalize_event_frame(pd.read_csv(forecast_cache), endpoint="forecast")
        sources["forecast"] = str(forecast_cache)
    missing = [k for k, v in frames.items() if v.empty]
    fetch_meta: dict[str, Any] = {"source": "cache_plus_bounded_fetch", "cache_sources": sources}
    if missing:
        fetched, meta = fetch_tushare_events(sample_codes)
        fetch_meta["bounded_fetch_meta"] = meta
        for endpoint in missing:
            if len(fetched.get(endpoint, pd.DataFrame())):
                frames[endpoint] = fetched[endpoint]
    return frames, fetch_meta


def _write_md(report: dict[str, Any]) -> str:
    diag = report["diagnostic"]
    decision = diag.get("decision", {})
    cov = diag.get("coverage", {})
    best = diag.get("best_signal", {})
    baseline = diag.get("diagnostics", {}).get("baseline", {})
    lines = [
        "# 第 9 轮：业绩预告 / 业绩快报事件数据源 read-only diagnostic",
        "",
        "Scope: bounded per-ticker/cache reuse + PIT daily as-of diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.",
        "",
        "## Decision",
        f"- Decision: `{decision.get('decision')}`",
        f"- Reasons: {', '.join(decision.get('reasons') or [])}",
        f"- Best signal: `{best.get('name')}` spread={best.get('spread_mean')}",
        f"- Benchmark value_quality_no_distress bucket spread: {diag.get('benchmark', {}).get('value_quality_no_distress_bucket_spread')}",
        "",
        "## Coverage",
        f"- Rows: {cov.get('rows')}",
        f"- Dates: {cov.get('dates')}",
        f"- Tickers: {cov.get('tickers')}",
        f"- Signal columns: {len(cov.get('signal_columns') or [])}",
        "",
        "## Key spreads",
        f"- Local baseline Q3-Q0: {baseline.get('spread_mean')} (obs={baseline.get('observations')})",
    ]
    diagnostics = diag.get("diagnostics", {})
    ranked = []
    for name, item in diagnostics.items():
        if name == "baseline" or not isinstance(item, dict):
            continue
        ranked.append((item.get("spread_mean"), name, item))
    for _, name, item in sorted(ranked, key=lambda x: (-999 if x[0] is None else -float(x[0])))[:20]:
        lines.append(f"- {name}: spread={item.get('spread_mean')}, positive_rate={item.get('spread_positive_rate')}, obs={item.get('observations')}")
    lines.extend([
        "",
        "## PIT / source preflight",
        f"- Raw source rows: {report.get('source_preflight', {}).get('rows')}",
        f"- Raw source tickers: {report.get('source_preflight', {}).get('tickers')}",
        f"- PIT-safe endpoints: {report.get('source_preflight', {}).get('pit_safe_endpoints')}",
        f"- Daily as-of rows before feature dropna: {report.get('daily_asof_rows_before_feature_dropna')}",
        "",
        "## Interpretation",
        "- 所有事件/财务数据均要求 `ann_date <= trade_date` 的 daily as-of；未使用 end_date 直接前视。",
        "- 若 best signal 未超过 0.0062253011 benchmark，则事件路线不进入 controlled workflow。",
    ])
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")
    return text


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    sample_codes = choose_sample_codes()
    frames, meta = load_or_fetch_events(sample_codes)
    raw_parts = [df for df in frames.values() if len(df)]
    if raw_parts:
        pd.concat(raw_parts, ignore_index=True, sort=False).to_csv(RAW_OUT, index=False)
    source_preflight = event_preflight(frames)
    statement = build_earnings_event_statement_features(frames)
    if len(statement):
        statement.to_csv(STATEMENT_OUT, index=False)
    feature_cache_path = choose_feature_cache()
    asof = pd.DataFrame()
    features = pd.DataFrame()
    if feature_cache_path is not None and len(statement):
        feature_cache = load_feature_cache(feature_cache_path)
        asof = asof_merge_earnings_event_features(feature_cache, statement)
        features = build_earnings_event_features(asof)
        if len(features):
            features.to_csv(ASOF_OUT, index=False)
    diagnostic = build_earnings_event_report(features)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded_earnings_event_forecast_express_readonly_daily_asof",
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_workflow_run": True,
        "no_full_market_backfill": True,
        "fetch_meta": meta,
        "feature_cache": str(feature_cache_path) if feature_cache_path else None,
        "source_preflight": source_preflight,
        "statement_feature_rows": int(len(statement)),
        "daily_asof_rows_before_feature_dropna": int(len(asof)),
        "diagnostic": diagnostic,
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_safe), encoding="utf-8")
    _write_md(report)
    print(json.dumps({"artifact": str(JSON_OUT), "decision": diagnostic.get("decision"), "best_signal": diagnostic.get("best_signal"), "coverage": diagnostic.get("coverage")}, ensure_ascii=False, indent=2, default=_json_safe))


if __name__ == "__main__":
    main()
