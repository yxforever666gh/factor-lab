#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_source_mvp import choose_feature_cache, is_stock_like_ts_code, load_feature_cache
from factor_lab.settings import load_env_file
from factor_lab.shareholder_count_builder import (
    build_daily_asof_shareholder_frame,
    build_shareholder_crowding_features,
    build_shareholder_probe_report,
)

load_env_file()

ARTIFACT_DIR = Path("artifacts/shareholder_count_mvp")
RAW_OUT = ARTIFACT_DIR / "shareholder_count_raw.csv"
FEATURE_OUT = ARTIFACT_DIR / "shareholder_count_daily_asof_features.csv"
JSON_OUT = ARTIFACT_DIR / "shareholder_count_mvp.json"
MD_OUT = ARTIFACT_DIR / "shareholder_count_mvp.md"
KNOWLEDGE_OUT = Path("knowledge/shareholder_count_mvp.md")
START_DATE = "20180101"
END_DATE = "20231231"
MAX_TICKERS = 100


def _json_error(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"[:500]


def choose_sample_tickers(base: pd.DataFrame, max_tickers: int = MAX_TICKERS) -> list[str]:
    if "ticker" in base.columns:
        codes = list(pd.Series(base["ticker"].dropna().astype(str).unique()).sort_values())
    elif "ts_code" in base.columns:
        codes = list(pd.Series(base["ts_code"].dropna().astype(str).unique()).sort_values())
    else:
        return []
    stocks = [c for c in codes if is_stock_like_ts_code(c)]
    return stocks[:max_tickers]


def fetch_holdernumber(tickers: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        return pd.DataFrame(), {"ready": False, "error": "missing_tushare_token"}
    import tushare as ts  # type: ignore

    pro = ts.pro_api(token)
    frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}
    for idx, code in enumerate(tickers, start=1):
        try:
            df = pro.stk_holdernumber(ts_code=code, start_date=START_DATE, end_date=END_DATE)
            if df is not None and len(df):
                frames.append(df)
        except Exception as exc:
            errors[code] = _json_error(exc)
        if idx % 10 == 0:
            time.sleep(0.2)
    raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return raw, {"ready": True, "requested_tickers": len(tickers), "successful_ticker_frames": len(frames), "error_count": len(errors), "sample_errors": dict(list(errors.items())[:5])}


def summarize_raw(raw: pd.DataFrame) -> dict[str, Any]:
    if raw.empty:
        return {"rows": 0}
    date_fields = [c for c in ["ann_date", "end_date"] if c in raw.columns]
    field_stats = {}
    for col in ["ann_date", "end_date", "holder_num"]:
        if col in raw.columns:
            field_stats[col] = {"nonnull_rate": round(float(raw[col].notna().mean()), 4), "unique": int(raw[col].nunique(dropna=True))}
    return {
        "rows": int(len(raw)),
        "tickers": int(raw["ts_code"].nunique()) if "ts_code" in raw.columns else 0,
        "columns": list(raw.columns),
        "date_fields": date_fields,
        "ann_date_min": str(raw["ann_date"].dropna().min()) if "ann_date" in raw.columns and raw["ann_date"].notna().any() else None,
        "ann_date_max": str(raw["ann_date"].dropna().max()) if "ann_date" in raw.columns and raw["ann_date"].notna().any() else None,
        "field_stats": field_stats,
    }


def to_markdown(payload: dict[str, Any]) -> str:
    d = payload.get("decision", {})
    raw = payload.get("raw_summary", {})
    c = payload.get("coverage", {})
    diag = payload.get("diagnostics", {})
    lines = [
        "# Shareholder Count / Ownership-crowding MVP",
        "",
        "Scope: bounded Tushare stk_holdernumber sample + daily as-of read-only diagnostic. No queue write, no daemon start, no full workflow.",
        "",
        f"Decision: `{d.get('decision')}`",
        f"Reasons: {', '.join(d.get('reasons') or [])}",
        "",
        "## Raw source",
        f"- Rows: {raw.get('rows')}",
        f"- Tickers: {raw.get('tickers')}",
        f"- Date fields: {', '.join(raw.get('date_fields') or [])}",
        f"- Ann date range: {raw.get('ann_date_min')} to {raw.get('ann_date_max')}",
        "",
        "## Daily as-of feature coverage",
        f"- Rows: {c.get('rows')}",
        f"- Dates: {c.get('dates')}",
        f"- Tickers: {c.get('tickers')}",
        f"- QoQ non-null rate: {c.get('qoq_nonnull_rate')}",
        f"- YoY non-null rate: {c.get('yoy_nonnull_rate')}",
        "",
        "## Bucket diagnostics",
        "| Score | Spread mean | Positive rate | Observations |",
        "|---|---:|---:|---:|",
    ]
    for key in ["baseline", "low_shareholder_crowding_qoq", "shareholder_confirmation_qoq", "low_shareholder_crowding_yoy", "shareholder_confirmation_yoy"]:
        rec = diag.get(key, {})
        lines.append(f"| {key} | {rec.get('spread_mean')} | {rec.get('spread_positive_rate')} | {rec.get('observations')} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    cache = choose_feature_cache()
    if not cache:
        raise SystemExit("missing feature cache")
    base = load_feature_cache(cache)
    tickers = choose_sample_tickers(base)
    raw, fetch_status = fetch_holdernumber(tickers)
    raw.to_csv(RAW_OUT, index=False)
    raw_summary = summarize_raw(raw)
    asof = build_daily_asof_shareholder_frame(base, raw)
    features = build_shareholder_crowding_features(asof)
    features.to_csv(FEATURE_OUT, index=False)
    report = build_shareholder_probe_report(features)
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "shareholder_count_stk_holdernumber_bounded_mvp",
        "feature_cache": str(cache),
        "raw_csv": str(RAW_OUT),
        "feature_csv": str(FEATURE_OUT),
        "sample_params": {"start_date": START_DATE, "end_date": END_DATE, "max_tickers": MAX_TICKERS, "tickers_used": tickers[:10]},
        "fetch_status": fetch_status,
        "raw_summary": raw_summary,
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        **report,
    }
    JSON_OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    md = to_markdown(payload)
    MD_OUT.write_text(md, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(md, encoding="utf-8")
    print(json.dumps({"json": str(JSON_OUT), "markdown": str(MD_OUT), "knowledge": str(KNOWLEDGE_OUT), "raw_csv": str(RAW_OUT), "feature_csv": str(FEATURE_OUT), "fetch_status": fetch_status, "raw_summary": raw_summary, "coverage": payload.get("coverage"), "diagnostics": payload.get("diagnostics"), "correlations": payload.get("correlations"), "decision": payload.get("decision")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
