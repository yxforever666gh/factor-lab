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
from factor_lab.ownership_concentration_source import (
    asof_merge_top10_features,
    build_ownership_concentration_features,
    build_ownership_concentration_report,
    build_top10_statement_features,
    normalize_top10_frame,
    top10_preflight,
)
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/ownership_concentration_source_mvp")
JSON_OUT = ARTIFACT_DIR / "ownership_concentration_source_mvp.json"
MD_OUT = ARTIFACT_DIR / "ownership_concentration_source_mvp.md"
RAW_OUT = ARTIFACT_DIR / "ownership_top10_raw_sample.csv"
STATEMENT_OUT = ARTIFACT_DIR / "ownership_top10_statement_features.csv"
ASOF_OUT = ARTIFACT_DIR / "ownership_top10_daily_asof_features.csv"
KNOWLEDGE_OUT = Path("knowledge/ownership_concentration_source_mvp.md")
ENDPOINTS = ("top10_holders", "top10_floatholders")
PERIODS = ("20231231", "20230930", "20230630", "20230331", "20221231", "20220930", "20220630", "20220331")
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


def _call_endpoint_periods(pro: Any, endpoint: str, code: str) -> pd.DataFrame:
    fn = getattr(pro, endpoint)
    frames: list[pd.DataFrame] = []
    for period in PERIODS:
        variants = ({"ts_code": code, "period": period}, {"ts_code": code, "end_date": period})
        for params in variants:
            try:
                df = fn(**params)
                if df is not None and len(df):
                    frames.append(normalize_top10_frame(df, endpoint=endpoint))
                    break
            except Exception:
                pass
            time.sleep(0.03)
    if frames:
        return pd.concat(frames, ignore_index=True).drop_duplicates()
    # last fallback: bounded per-code call, still sample-only
    try:
        df = fn(ts_code=code)
        if df is not None and len(df):
            return normalize_top10_frame(df, endpoint=endpoint).head(80)
    except Exception:
        pass
    return pd.DataFrame()


def fetch_tushare_top10(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    meta: dict[str, Any] = {"auth_ready": bool(token), "sample_codes": sample_codes, "periods": list(PERIODS), "endpoints": list(ENDPOINTS), "errors": {}}
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
                df = _call_endpoint_periods(pro, endpoint, code)
                if len(df):
                    frames[endpoint].append(df)
            except Exception as exc:
                errors[f"{endpoint}:{code}"] = f"{type(exc).__name__}: {exc}"[:500]
    out = {endpoint: (pd.concat(parts, ignore_index=True).drop_duplicates() if parts else pd.DataFrame()) for endpoint, parts in frames.items()}
    meta["errors"] = errors
    meta["rows_by_endpoint"] = {k: int(len(v)) for k, v in out.items()}
    return out, meta


def load_or_fetch_top10(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
    # Reuse existing bounded cache if present to avoid unnecessary API calls.
    legacy_raw = Path("artifacts/ownership_top10_mvp/top10_floatholders_raw.csv")
    if legacy_raw.exists():
        df = pd.read_csv(legacy_raw)
        return {"top10_holders": pd.DataFrame(), "top10_floatholders": normalize_top10_frame(df, endpoint="top10_floatholders")}, {"source": "legacy_cache", "path": str(legacy_raw)}
    return fetch_tushare_top10(sample_codes)


def _write_md(report: dict[str, Any]) -> str:
    diag = report["diagnostic"]
    decision = diag["decision"]
    cov = diag.get("coverage", {})
    best = diag.get("best_signal", {})
    baseline = diag.get("diagnostics", {}).get("baseline", {})
    lines = [
        "# 第 8 轮：十大股东 / ownership concentration read-only diagnostic",
        "",
        "Scope: bounded top10_holders/top10_floatholders sample + PIT daily as-of diagnostic only. No workflow enqueue, no daemon start, no full-market backfill.",
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
    for name, item in sorted(diagnostics.items()):
        if name == "baseline" or not isinstance(item, dict):
            continue
        lines.append(f"- {name}: spread={item.get('spread_mean')}, positive_rate={item.get('spread_positive_rate')}, obs={item.get('observations')}")
    lines.extend([
        "",
        "## PIT / source preflight",
        f"- Raw source rows: {report.get('source_preflight', {}).get('rows')}",
        f"- Raw source tickers: {report.get('source_preflight', {}).get('tickers')}",
        f"- PIT-safe endpoints: {report.get('source_preflight', {}).get('pit_safe_endpoints')}",
        "",
        "## Interpretation",
        "- 若 best signal 未超过 0.0062253011 benchmark，则该 ownership-concentration 路线停止，不进入 controlled workflow。",
        "- 本轮所有持仓数据均要求 `ann_date <= trade_date` 的 daily as-of；未使用 end_date 直接前视。",
    ])
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")
    return text


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    sample_codes = choose_sample_codes()
    frames, meta = load_or_fetch_top10(sample_codes)
    raw_parts = [df for df in frames.values() if len(df)]
    if raw_parts:
        pd.concat(raw_parts, ignore_index=True).to_csv(RAW_OUT, index=False)
    source_preflight = top10_preflight(frames)
    statement = build_top10_statement_features(frames)
    if len(statement):
        statement.to_csv(STATEMENT_OUT, index=False)
    feature_cache_path = choose_feature_cache()
    asof = pd.DataFrame()
    features = pd.DataFrame()
    if feature_cache_path is not None and len(statement):
        feature_cache = load_feature_cache(feature_cache_path)
        asof = asof_merge_top10_features(feature_cache, statement)
        features = build_ownership_concentration_features(asof)
        if len(features):
            features.to_csv(ASOF_OUT, index=False)
    diagnostic = build_ownership_concentration_report(features)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded_ownership_concentration_top10_readonly_daily_asof",
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
