#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.dragon_tiger_source import (
    asof_merge_dragon_tiger_features,
    build_dragon_tiger_daily_events,
    build_dragon_tiger_readonly_features,
    build_dragon_tiger_report,
    dragon_tiger_preflight,
    normalize_top_list_frame,
)
from factor_lab.margin_source_mvp import choose_feature_cache, load_feature_cache
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/dragon_tiger_source_mvp")
JSON_OUT = ARTIFACT_DIR / "dragon_tiger_source_mvp.json"
MD_OUT = ARTIFACT_DIR / "dragon_tiger_source_mvp.md"
RAW_OUT = ARTIFACT_DIR / "dragon_tiger_top_list_raw_sample.csv"
DAILY_OUT = ARTIFACT_DIR / "dragon_tiger_daily_events.csv"
ASOF_OUT = ARTIFACT_DIR / "dragon_tiger_daily_asof_features.csv"
KNOWLEDGE_OUT = Path("knowledge/dragon_tiger_source_mvp.md")
MAX_TRADE_DATES = 90
MAX_SAMPLE_CODES = 80


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


def choose_trade_dates_and_codes() -> tuple[list[str], list[str], str | None]:
    path = choose_feature_cache()
    if path is None:
        return [], [], None
    try:
        usecols = ["date", "ticker"]
        df = pd.read_csv(path, usecols=usecols)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
        dates = sorted(df["date"].dropna().astype(str).unique().tolist())[-MAX_TRADE_DATES:]
        tickers = df.loc[df["date"].isin(dates), "ticker"].dropna().astype(str).drop_duplicates().tolist()[:MAX_SAMPLE_CODES]
        return dates, tickers, str(path)
    except Exception:
        return [], [], str(path)


def fetch_tushare_top_list(trade_dates: list[str], sample_codes: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    meta: dict[str, Any] = {
        "auth_ready": bool(token),
        "trade_dates_requested": trade_dates,
        "sample_codes": sample_codes,
        "max_trade_dates": MAX_TRADE_DATES,
        "max_sample_codes": MAX_SAMPLE_CODES,
        "errors": {},
    }
    if not token:
        meta["error"] = "missing_tushare_token"
        return pd.DataFrame(), meta
    try:
        import tushare as ts  # type: ignore
        pro = ts.pro_api(token)
    except Exception as exc:
        meta["error"] = f"tushare_import_or_auth_failed: {type(exc).__name__}: {exc}"[:500]
        return pd.DataFrame(), meta
    if not hasattr(pro, "top_list"):
        meta["error"] = "method_not_found: top_list"
        return pd.DataFrame(), meta
    frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}
    sample_set = set(sample_codes)
    for trade_date in trade_dates:
        try:
            df = pro.top_list(trade_date=trade_date)
            if df is not None and len(df):
                nd = normalize_top_list_frame(df)
                if sample_set and "ts_code" in nd.columns:
                    nd = nd[nd["ts_code"].astype(str).isin(sample_set)].copy()
                if len(nd):
                    frames.append(nd)
        except Exception as exc:
            errors[trade_date] = f"{type(exc).__name__}: {exc}"[:500]
        time.sleep(0.05)
    meta["errors"] = errors
    out = pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()
    meta["rows"] = int(len(out))
    return out, meta


def load_or_fetch_top_list(trade_dates: list[str], sample_codes: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    if RAW_OUT.exists():
        df = pd.read_csv(RAW_OUT)
        return normalize_top_list_frame(df), {"source": "existing_cache", "path": str(RAW_OUT)}
    legacy_candidates = sorted(Path("artifacts").glob("**/*top_list*raw*.csv")) + sorted(Path("artifacts").glob("**/*dragon*tiger*raw*.csv"))
    for path in legacy_candidates:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if {"trade_date", "ts_code"}.issubset(df.columns):
            nd = normalize_top_list_frame(df)
            if len(nd):
                return nd, {"source": "legacy_cache", "path": str(path)}
    return fetch_tushare_top_list(trade_dates, sample_codes)


def _write_md(report: dict[str, Any]) -> str:
    diag = report.get("diagnostic", {})
    decision = diag.get("decision", {})
    cov = diag.get("coverage", {})
    best = diag.get("best_signal", {})
    baseline = diag.get("diagnostics", {}).get("baseline", {})
    lines = [
        "# dragon_tiger / 龙虎榜事件流 bounded read-only source MVP",
        "",
        "Scope: bounded Tushare top_list sample + event-date PIT/read-only diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.",
        "",
        "## Decision",
        f"- Decision: `{decision.get('decision')}`",
        f"- Reasons: {', '.join(decision.get('reasons') or [])}",
        f"- Best signal: `{best.get('name')}` spread={best.get('spread_mean')}, positive_rate={best.get('positive_rate')}, obs={best.get('observations')}",
        f"- Benchmark value_quality_no_distress bucket spread: {diag.get('benchmark', {}).get('value_quality_no_distress_bucket_spread')}",
        f"- Local baseline Q3-Q0: {baseline.get('spread_mean')} (obs={baseline.get('observations')})",
        "",
        "## Coverage",
        f"- Diagnostic rows: {cov.get('rows')}",
        f"- Dates: {cov.get('dates')}",
        f"- Tickers: {cov.get('tickers')}",
        f"- Event-active rows: {cov.get('event_active_rows')}",
        f"- Signal columns: {len(cov.get('signal_columns') or [])}",
        f"- Raw rows: {report.get('source_preflight', {}).get('rows')}",
        f"- Raw dates: {report.get('source_preflight', {}).get('dates')}",
        f"- Raw tickers: {report.get('source_preflight', {}).get('tickers')}",
        "",
        "## Top spreads",
    ]
    diagnostics = diag.get("diagnostics", {})
    ranked = []
    for name, item in diagnostics.items():
        if name == "baseline" or not isinstance(item, dict) or item.get("spread_mean") is None:
            continue
        ranked.append((name, item))
    ranked.sort(key=lambda kv: float(kv[1].get("spread_mean") or -999), reverse=True)
    for name, item in ranked[:12]:
        lines.append(f"- {name}: spread={item.get('spread_mean')}, positive_rate={item.get('spread_positive_rate')}, obs={item.get('observations')}")
    lines.extend([
        "",
        "## PIT interpretation",
        "- 龙虎榜是 trade_date 事件流；本诊断只把 `trade_date <= feature date` 的事件滚动进入 5/20 日窗口，未使用未来事件。",
        "- 若 best spread 未同时超过 local baseline 与 benchmark 0.0062253011，则停止该路线，不进入 controlled workflow。",
    ])
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")
    return text


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    trade_dates, sample_codes, feature_cache_hint = choose_trade_dates_and_codes()
    raw, meta = load_or_fetch_top_list(trade_dates, sample_codes)
    if len(raw):
        raw.to_csv(RAW_OUT, index=False)
    preflight = dragon_tiger_preflight(raw)
    daily = build_dragon_tiger_daily_events(raw)
    if len(daily):
        daily.to_csv(DAILY_OUT, index=False)
    feature_cache_path = choose_feature_cache()
    asof = pd.DataFrame()
    features = pd.DataFrame()
    if feature_cache_path is not None:
        cache = load_feature_cache(feature_cache_path)
        asof = asof_merge_dragon_tiger_features(cache, daily)
        features = build_dragon_tiger_readonly_features(asof)
        if len(features):
            features.to_csv(ASOF_OUT, index=False)
    diagnostic = build_dragon_tiger_report(features)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded_dragon_tiger_top_list_readonly_event_date_asof",
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_workflow_run": True,
        "no_full_market_backfill": True,
        "fetch_meta": meta,
        "feature_cache": str(feature_cache_path) if feature_cache_path else feature_cache_hint,
        "source_preflight": preflight,
        "daily_event_rows": int(len(daily)),
        "daily_asof_rows_before_feature_dropna": int(len(asof)),
        "diagnostic": diagnostic,
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_safe), encoding="utf-8")
    _write_md(report)
    print(json.dumps({"artifact": str(JSON_OUT), "decision": diagnostic.get("decision"), "best_signal": diagnostic.get("best_signal"), "coverage": diagnostic.get("coverage")}, ensure_ascii=False, indent=2, default=_json_safe))


if __name__ == "__main__":
    main()
