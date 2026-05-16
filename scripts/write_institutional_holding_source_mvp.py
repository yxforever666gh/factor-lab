#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.institutional_holding_source_mvp import build_institutional_holding_source_report, normalize_holding_frame
from factor_lab.margin_source_mvp import choose_feature_cache
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/institutional_holding_source_mvp")
JSON_OUT = ARTIFACT_DIR / "institutional_holding_source_mvp.json"
MD_OUT = ARTIFACT_DIR / "institutional_holding_source_mvp.md"
RAW_OUT = ARTIFACT_DIR / "institutional_holding_raw_sample.csv"
KNOWLEDGE_OUT = Path("knowledge/institutional_holding_source_mvp.md")
ENDPOINTS = ("top10_holders", "top10_floatholders")
START_DATE = "20230101"
END_DATE = "20231231"
SAMPLE_PERIOD = "20231231"
MAX_SAMPLE_CODES = 20
DEFAULT_CODES = ["000001.SZ", "000002.SZ", "000333.SZ", "600000.SH", "600519.SH", "601318.SH", "300750.SZ"]


def _json_safe(obj: Any) -> Any:
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    if pd.isna(obj):
        return None
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


def _call_variants(pro: Any, endpoint: str, code: str) -> pd.DataFrame:
    fn = getattr(pro, endpoint)
    variants = [
        {"ts_code": code, "start_date": START_DATE, "end_date": END_DATE},
        {"ts_code": code, "end_date": SAMPLE_PERIOD},
        {"ts_code": code},
    ]
    frames: list[pd.DataFrame] = []
    for params in variants:
        try:
            df = fn(**params)
            if df is not None and len(df):
                frames.append(normalize_holding_frame(df, endpoint=endpoint))
        except Exception:
            pass
        time.sleep(0.05)
    return pd.concat(frames, ignore_index=True).drop_duplicates() if frames else pd.DataFrame()


def fetch_tushare_top_holders(sample_codes: list[str]) -> tuple[dict[str, pd.DataFrame], dict[str, Any]]:
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
                df = _call_variants(pro, endpoint, code)
                if len(df):
                    frames[endpoint].append(df)
            except Exception as exc:
                errors[f"{endpoint}:{code}"] = f"{type(exc).__name__}: {exc}"[:500]
    out = {endpoint: (pd.concat(parts, ignore_index=True).drop_duplicates() if parts else pd.DataFrame()) for endpoint, parts in frames.items()}
    meta["errors"] = errors
    meta["rows_by_endpoint"] = {k: int(len(v)) for k, v in out.items()}
    return out, meta


def _write_md(report: dict[str, Any]) -> None:
    decision = report["diagnostic"]["decision"]
    cov = report["diagnostic"]["coverage"]
    lines = [
        "# Institutional Holding / Top10 Holders Source MVP",
        "",
        "Scope: bounded Tushare top10_holders/top10_floatholders sample only. No workflow enqueue, no daemon start, no full-market backfill.",
        "",
        "## Decision",
        f"- Decision: `{decision['decision']}`",
        f"- Reasons: {', '.join(decision.get('reasons') or [])}",
        "",
        "## Coverage",
        f"- Rows: {cov.get('rows')}",
        f"- Tickers: {cov.get('tickers')}",
        f"- Endpoints with rows: {cov.get('endpoints_with_rows')}",
        "",
        "## Endpoint schema / PIT audit",
    ]
    for endpoint, item in report["diagnostic"].get("endpoint_reports", {}).items():
        lines.extend([
            f"### {endpoint}",
            f"- Rows: {item.get('rows')}",
            f"- Tickers: {item.get('tickers')}",
            f"- Date fields: {', '.join(item.get('date_fields') or [])}",
            f"- PIT control: `{item.get('pit_control')}`",
            f"- Columns: {', '.join(item.get('columns') or [])}",
        ])
    lines.extend([
        "",
        "## Interpretation",
        "- 若只有 `end_date` 而没有 `ann_date`/`f_ann_date`，该源不能直接用于 PIT 因子研究。",
        "- 本轮为只读 MVP；未写队列、未启动 daemon、未运行 workflow。",
    ])
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    sample_codes = choose_sample_codes()
    frames, meta = fetch_tushare_top_holders(sample_codes)
    raw_parts = [df for df in frames.values() if len(df)]
    if raw_parts:
        pd.concat(raw_parts, ignore_index=True).to_csv(RAW_OUT, index=False)
    diagnostic = build_institutional_holding_source_report(frames)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded_institutional_holding_top10_source_mvp_readonly",
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_workflow_run": True,
        "no_full_market_backfill": True,
        "fetch_meta": meta,
        "diagnostic": diagnostic,
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_safe), encoding="utf-8")
    _write_md(report)
    print(json.dumps({"artifact": str(JSON_OUT), "decision": diagnostic.get("decision"), "coverage": diagnostic.get("coverage")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
