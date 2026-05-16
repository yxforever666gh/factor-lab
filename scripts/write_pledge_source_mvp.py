#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_source_mvp import choose_feature_cache, load_feature_cache
from factor_lab.pledge_source import (
    asof_merge_pledge_features,
    build_pledge_readonly_features,
    build_pledge_report,
    build_pledge_statement_features,
    normalize_pledge_frame,
    pledge_preflight,
)
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/pledge_source_mvp")
JSON_OUT = ARTIFACT_DIR / "pledge_source_mvp.json"
MD_OUT = ARTIFACT_DIR / "pledge_source_mvp.md"
RAW_OUT = ARTIFACT_DIR / "pledge_raw_sample.csv"
STATEMENT_OUT = ARTIFACT_DIR / "pledge_statement_features.csv"
ASOF_OUT = ARTIFACT_DIR / "pledge_daily_asof_features.csv"
KNOWLEDGE_OUT = Path("knowledge/pledge_source_mvp.md")
ENDPOINTS = ("pledge_stat", "pledge_detail")
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


def _call_pledge_endpoint(pro: Any, endpoint: str, code: str) -> pd.DataFrame:
    fn = getattr(pro, endpoint)
    variants = ({"ts_code": code},)
    frames: list[pd.DataFrame] = []
    for params in variants:
        try:
            df = fn(**params)
            if df is not None and len(df):
                frames.append(normalize_pledge_frame(df, endpoint=endpoint).head(120))
        except Exception:
            pass
        time.sleep(0.04)
    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()


def fetch_tushare_pledge(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    meta: dict[str, Any] = {"auth_ready": bool(token), "sample_codes": sample_codes, "endpoints": list(ENDPOINTS), "max_sample_codes": MAX_SAMPLE_CODES, "errors": {}}
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
                df = _call_pledge_endpoint(pro, endpoint, code)
                if len(df):
                    frames[endpoint].append(df)
            except Exception as exc:
                errors[f"{endpoint}:{code}"] = f"{type(exc).__name__}: {exc}"[:500]
    out = {endpoint: (pd.concat(parts, ignore_index=True).drop_duplicates() if parts else pd.DataFrame()) for endpoint, parts in frames.items()}
    meta["errors"] = errors
    meta["rows_by_endpoint"] = {k: int(len(v)) for k, v in out.items()}
    return out, meta


def load_or_fetch_pledge(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    if RAW_OUT.exists():
        df = pd.read_csv(RAW_OUT)
        frames = {e: normalize_pledge_frame(df[df.get("endpoint") == e].copy(), endpoint=e) if "endpoint" in df.columns else pd.DataFrame() for e in ENDPOINTS}
        if any(len(v) for v in frames.values()):
            return frames, {"source": "existing_cache", "path": str(RAW_OUT)}
    legacy_candidates = sorted(Path("artifacts").glob("**/*pledge*raw*.csv"))
    for path in legacy_candidates:
        if path == RAW_OUT:
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if "ts_code" in df.columns:
            endpoint = "pledge_detail" if "start_date" in df.columns else "pledge_stat"
            return {"pledge_stat": pd.DataFrame(), "pledge_detail": normalize_pledge_frame(df, endpoint=endpoint)}, {"source": "legacy_cache", "path": str(path)}
    return fetch_tushare_pledge(sample_codes)


def _write_md(report: dict[str, Any]) -> str:
    diag = report.get("diagnostic", {})
    decision = diag.get("decision", {})
    cov = diag.get("coverage", {})
    best = diag.get("best_signal", {})
    baseline = diag.get("diagnostics", {}).get("baseline", {})
    lines = [
        "# pledge / 股权质押 bounded PIT read-only source MVP",
        "",
        "Scope: bounded Tushare pledge_stat/pledge_detail sample + PIT daily as-of read-only diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.",
        "",
        "## Decision",
        f"- Decision: `{decision.get('decision')}`",
        f"- Reasons: {', '.join(decision.get('reasons') or [])}",
        f"- Best signal: `{best.get('name')}` spread={best.get('spread_mean')}, positive_rate={best.get('positive_rate')}, obs={best.get('observations')}",
        f"- Benchmark value_quality_no_distress bucket spread: {diag.get('benchmark', {}).get('value_quality_no_distress_bucket_spread')}",
        f"- Local baseline Q3-Q0: {baseline.get('spread_mean')} (obs={baseline.get('observations')})",
        "",
        "## Coverage",
        f"- Raw source rows: {report.get('source_preflight', {}).get('rows')}",
        f"- Raw source tickers: {report.get('source_preflight', {}).get('tickers')}",
        f"- PIT-safe endpoints: {report.get('source_preflight', {}).get('pit_safe_endpoints')}",
        f"- Statement rows: {report.get('statement_feature_rows')}",
        f"- Daily as-of rows before dropna: {report.get('daily_asof_rows_before_feature_dropna')}",
        f"- Diagnostic rows: {cov.get('rows')}",
        f"- Diagnostic dates: {cov.get('dates')}",
        f"- Diagnostic tickers: {cov.get('tickers')}",
        f"- Signal columns: {len(cov.get('signal_columns') or [])}",
        "",
        "## Top spreads",
    ]
    ranked = []
    for name, item in diag.get("diagnostics", {}).items():
        if name == "baseline" or not isinstance(item, dict) or item.get("spread_mean") is None:
            continue
        ranked.append((name, item))
    ranked.sort(key=lambda kv: float(kv[1].get("spread_mean") or -999), reverse=True)
    for name, item in ranked[:12]:
        lines.append(f"- {name}: spread={item.get('spread_mean')}, positive_rate={item.get('spread_positive_rate')}, obs={item.get('observations')}")
    lines.extend([
        "",
        "## PIT interpretation",
        "- 质押数据只用 `ann_date <= feature date` 的 daily as-of；无公告日时 detail 仅用 start_date 作为保守可见日期，不使用未来 release/end date。",
        "- 若 best spread 未同时超过 local baseline 与 benchmark 0.0062253011，则停止该路线，不进入 controlled workflow。",
    ])
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")
    return text


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    sample_codes = choose_sample_codes()
    frames, meta = load_or_fetch_pledge(sample_codes)
    raw_parts = [df for df in frames.values() if len(df)]
    if raw_parts:
        pd.concat(raw_parts, ignore_index=True).to_csv(RAW_OUT, index=False)
    source_preflight = pledge_preflight(frames)
    statement = build_pledge_statement_features(frames)
    if len(statement):
        statement.to_csv(STATEMENT_OUT, index=False)
    feature_cache_path = choose_feature_cache()
    asof = pd.DataFrame()
    features = pd.DataFrame()
    if feature_cache_path is not None and len(statement):
        cache = load_feature_cache(feature_cache_path)
        asof = asof_merge_pledge_features(cache, statement)
        features = build_pledge_readonly_features(asof)
        if len(features):
            features.to_csv(ASOF_OUT, index=False)
    diagnostic = build_pledge_report(features)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded_pledge_readonly_daily_asof",
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
