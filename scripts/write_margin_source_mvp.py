#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_source_mvp import (
    choose_feature_cache,
    classify_tickers,
    correlation_precheck,
    decide_margin_mvp,
    field_sanity,
    load_feature_cache,
    merge_margin_with_features,
    normalize_margin_frame,
)
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/margin_source_mvp")
JSON_OUT = ARTIFACT_DIR / "margin_source_mvp.json"
MD_OUT = ARTIFACT_DIR / "margin_source_mvp.md"
KNOWLEDGE_OUT = Path("knowledge/margin_source_mvp.md")
SAMPLE_TRADE_DATES = ("20181228", "20191231", "20201231", "20211231", "20221230", "20231229")
MARGIN_FIELDS = ("rzye", "rqye", "rzmre", "rzche", "rzrqye", "rqyl", "rqchl", "rqmcl")


def _fetch_margin_detail(token: str | None) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not token:
        return pd.DataFrame(), {"error": "missing_tushare_token"}
    try:
        import tushare as ts  # type: ignore
    except Exception as exc:
        return pd.DataFrame(), {"error": f"import_failed: {type(exc).__name__}: {exc}"}
    pro = ts.pro_api(token)
    frames: list[pd.DataFrame] = []
    by_date: dict[str, Any] = {}
    for trade_date in SAMPLE_TRADE_DATES:
        try:
            df = pro.margin_detail(trade_date=trade_date)
            df = normalize_margin_frame(df if df is not None else pd.DataFrame())
            frames.append(df)
            by_date[trade_date] = classify_tickers(df)
        except Exception as exc:
            by_date[trade_date] = {"error": f"{type(exc).__name__}: {exc}"}
        time.sleep(0.2)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    combined = normalize_margin_frame(combined)
    return combined, {"by_trade_date": by_date}


def _write_md(report: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# Margin Source MVP")
    lines.append("")
    lines.append("Scope: bounded sample data quality only. No factor run, no queue write, no daemon start.")
    lines.append("")
    decision = report["decision"]
    lines.append("## Decision")
    lines.append(f"- Decision: `{decision['decision']}`")
    lines.append(f"- Reasons: {', '.join(decision.get('reasons', []))}")
    lines.append("")
    lines.append("## Coverage")
    overall = report.get("coverage", {}).get("overall", {})
    lines.append(f"- Rows: {overall.get('rows')}")
    lines.append(f"- Unique tickers: {overall.get('unique_tickers')}")
    lines.append(f"- Stock-like tickers: {overall.get('stock_like_tickers')}")
    lines.append(f"- Stock-like ratio: {overall.get('stock_like_ratio')}")
    lines.append(f"- Feature overlap rows: {report.get('coverage', {}).get('feature_overlap_rows')}")
    lines.append(f"- Feature cache: {report.get('feature_cache')}")
    lines.append("")
    lines.append("## Sample date coverage")
    lines.append("| trade_date | rows | unique tickers | stock-like ratio |")
    lines.append("|---|---:|---:|---:|")
    for date, rec in report.get("coverage", {}).get("by_trade_date", {}).items():
        if rec.get("error"):
            lines.append(f"| {date} | error | error | error |")
        else:
            lines.append(f"| {date} | {rec.get('rows')} | {rec.get('unique_tickers')} | {rec.get('stock_like_ratio')} |")
    lines.append("")
    lines.append("## Redundancy pre-check")
    corr = report.get("correlation_precheck", {})
    lines.append(f"- Available: {corr.get('available')}")
    lines.append(f"- Rows: {corr.get('rows')}")
    lines.append(f"- Max abs turnover-like corr: {corr.get('max_abs_turnover_like_corr')}")
    lines.append(f"- Redundancy flag: {corr.get('redundancy_flag')}")
    lines.append("")
    lines.append("## Interpretation")
    if decision["decision"] == "proceed_margin_factor_probe_plan":
        lines.append("融资融券数据通过 MVP 质量门槛。下一轮可以写 controlled probe 计划，把 low-crowding 从 turnover 弱代理升级为 margin-based 机制。")
    elif decision["decision"] == "margin_data_usable_but_needs_feature_store":
        lines.append("融资融券接口可用，但还需要接入 feature store 后再跑因子。")
    elif decision["decision"] == "margin_redundant_with_turnover":
        lines.append("融资融券与已有 turnover/volatility 高度重复，暂不值得进入因子探针。")
    else:
        lines.append("融资融券 MVP 尚未通过，先处理 blockers。")
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    token = os.environ.get("TUSHARE_TOKEN")
    margin, fetch_meta = _fetch_margin_detail(token)
    feature_cache = choose_feature_cache()
    features = load_feature_cache(feature_cache) if feature_cache else pd.DataFrame()
    merged = merge_margin_with_features(margin, features)
    sanity = field_sanity(margin, MARGIN_FIELDS)
    coverage = {
        **fetch_meta,
        "overall": classify_tickers(margin),
        "feature_overlap_rows": int(len(merged)),
        "feature_overlap_tickers": int(merged["ts_code"].nunique()) if not merged.empty and "ts_code" in merged.columns else 0,
    }
    corr = correlation_precheck(merged)
    decision = decide_margin_mvp(coverage, sanity, corr)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded margin source MVP data-quality evaluation",
        "credentials_redacted": True,
        "sample_trade_dates": list(SAMPLE_TRADE_DATES),
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        "feature_cache": str(feature_cache) if feature_cache else None,
        "coverage": coverage,
        "field_sanity": sanity,
        "correlation_precheck": corr,
        "decision": decision,
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_md(report)
    print(json.dumps({
        "json": str(JSON_OUT),
        "markdown": str(MD_OUT),
        "knowledge": str(KNOWLEDGE_OUT),
        "decision": decision,
        "coverage": coverage["overall"],
        "feature_overlap_rows": coverage["feature_overlap_rows"],
        "correlation_precheck": corr,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
