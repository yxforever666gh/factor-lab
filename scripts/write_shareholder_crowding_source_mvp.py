#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_source_mvp import choose_feature_cache, load_feature_cache
from factor_lab.settings import load_env_file
from factor_lab.shareholder_crowding_source import (
    asof_merge_holdernumber_features,
    build_holdernumber_statement_features,
    build_shareholder_crowding_features,
    build_shareholder_crowding_report,
    holdernumber_preflight,
)

load_env_file()

ARTIFACT_DIR = Path("artifacts/shareholder_crowding_source_mvp")
JSON_OUT = ARTIFACT_DIR / "shareholder_crowding_source_mvp.json"
CSV_RAW_OUT = ARTIFACT_DIR / "stk_holdernumber_raw_sample.csv"
CSV_FEATURE_OUT = ARTIFACT_DIR / "shareholder_crowding_daily_asof_features.csv"
MD_OUT = ARTIFACT_DIR / "shareholder_crowding_source_mvp.md"
KNOWLEDGE_OUT = Path("knowledge/shareholder_crowding_source_mvp.md")

DEFAULT_SAMPLE_CODES = ["000001.SZ", "000002.SZ", "000333.SZ", "600000.SH", "600519.SH", "601318.SH", "300750.SZ"]
START_DATE = "20180101"
END_DATE = "20231231"
MAX_SAMPLE_CODES = 60


def choose_sample_codes(feature_cache_path: Path | None) -> list[str]:
    if feature_cache_path is None:
        return DEFAULT_SAMPLE_CODES
    try:
        tickers = pd.read_csv(feature_cache_path, usecols=["ticker"])["ticker"].dropna().astype(str).drop_duplicates().tolist()
        return tickers[:MAX_SAMPLE_CODES] or DEFAULT_SAMPLE_CODES
    except Exception:
        return DEFAULT_SAMPLE_CODES


def _json_safe(obj: Any) -> Any:
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)


def fetch_tushare_holdernumber(sample_codes: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
    token = os.environ.get("TUSHARE_TOKEN")
    meta: dict[str, Any] = {"auth_ready": bool(token), "endpoint": "stk_holdernumber", "sample_codes": sample_codes, "start_date": START_DATE, "end_date": END_DATE}
    if not token:
        meta["error"] = "missing_tushare_token"
        return pd.DataFrame(), meta
    try:
        import tushare as ts  # type: ignore
        pro = ts.pro_api(token)
    except Exception as exc:
        meta["error"] = f"tushare_import_or_auth_failed: {type(exc).__name__}: {exc}"
        return pd.DataFrame(), meta
    frames: list[pd.DataFrame] = []
    errors: dict[str, str] = {}
    for code in sample_codes:
        try:
            df = pro.stk_holdernumber(ts_code=code, start_date=START_DATE, end_date=END_DATE)
            if df is not None and len(df):
                frames.append(df)
        except Exception as exc:
            errors[code] = f"{type(exc).__name__}: {exc}"[:500]
    meta["errors"] = errors
    if not frames:
        meta["error"] = "empty_result"
        return pd.DataFrame(), meta
    raw = pd.concat(frames, ignore_index=True).drop_duplicates()
    meta["rows"] = int(len(raw))
    meta["columns"] = list(map(str, raw.columns))
    return raw, meta


def _write_md(report: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# Shareholder Crowding Source MVP")
    lines.append("")
    lines.append("Scope: bounded Tushare `stk_holdernumber` sample + PIT daily as-of read-only diagnostic. No workflow enqueue, no daemon start.")
    lines.append("")
    decision = report.get("diagnostic", {}).get("decision", {})
    lines.append("## Decision")
    lines.append(f"- Decision: `{decision.get('decision')}`")
    lines.append(f"- Reasons: {', '.join(decision.get('reasons') or [])}")
    lines.append("")
    lines.append("## Preflight")
    pre = report.get("preflight", {})
    lines.append(f"- Rows: {pre.get('rows')}")
    lines.append(f"- Tickers: {pre.get('tickers')}")
    lines.append(f"- Required fields present: {pre.get('required_fields_present')}")
    lines.append(f"- Missing fields: {pre.get('missing_required_fields')}")
    lines.append(f"- ann_date range: {pre.get('ann_date_min')} to {pre.get('ann_date_max')}")
    lines.append("")
    lines.append("## Daily as-of coverage")
    cov = report.get("diagnostic", {}).get("coverage", {})
    lines.append(f"- Rows: {cov.get('rows')}")
    lines.append(f"- Dates: {cov.get('dates')}")
    lines.append(f"- Tickers: {cov.get('tickers')}")
    lines.append("")
    lines.append("## Bucket diagnostics")
    for name, item in (report.get("diagnostic", {}).get("diagnostics") or {}).items():
        lines.append(f"- {name}: spread={item.get('spread_mean')}, observations={item.get('observations')}, positive_rate={item.get('spread_positive_rate')}")
    lines.append("")
    lines.append("## Correlations")
    for k, v in (report.get("diagnostic", {}).get("correlations") or {}).items():
        lines.append(f"- {k}: {v}")
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    feature_cache_path = choose_feature_cache()
    sample_codes = choose_sample_codes(feature_cache_path)
    raw, fetch_meta = fetch_tushare_holdernumber(sample_codes)
    if not raw.empty:
        raw.to_csv(CSV_RAW_OUT, index=False)
    preflight = holdernumber_preflight(raw)
    holder_features = build_holdernumber_statement_features(raw)
    diagnostic: dict[str, Any]
    feature_rows = 0
    if feature_cache_path is None:
        diagnostic = {"decision": {"decision": "stop_shareholder_crowding_no_feature_cache", "reasons": ["no_tushare_feature_cache"]}, "coverage": {"rows": 0, "dates": 0, "tickers": 0}}
    elif holder_features.empty:
        diagnostic = {"decision": {"decision": "stop_shareholder_crowding_no_holder_features", "reasons": ["holder_features_empty"]}, "coverage": {"rows": 0, "dates": 0, "tickers": 0}}
    else:
        cache = load_feature_cache(feature_cache_path)
        merged = asof_merge_holdernumber_features(cache, holder_features)
        features = build_shareholder_crowding_features(merged)
        feature_rows = int(len(features))
        if not features.empty:
            features.to_csv(CSV_FEATURE_OUT, index=False)
        diagnostic = build_shareholder_crowding_report(features)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded_shareholder_crowding_source_mvp_readonly",
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_workflow_run": True,
        "fetch_meta": fetch_meta,
        "preflight": preflight,
        "feature_cache_path": str(feature_cache_path) if feature_cache_path else None,
        "statement_feature_rows": int(len(holder_features)),
        "daily_asof_feature_rows": feature_rows,
        "diagnostic": diagnostic,
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_safe), encoding="utf-8")
    _write_md(report)
    print(json.dumps({"decision": diagnostic.get("decision"), "coverage": diagnostic.get("coverage"), "artifact": str(JSON_OUT)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
