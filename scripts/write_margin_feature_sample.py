#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import build_margin_feature_sample
from factor_lab.margin_source_mvp import choose_feature_cache, normalize_margin_frame
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/margin_feature_sample")
JSON_OUT = ARTIFACT_DIR / "margin_feature_sample.json"
CSV_OUT = ARTIFACT_DIR / "margin_feature_sample.csv"
MD_OUT = ARTIFACT_DIR / "margin_feature_sample.md"
KNOWLEDGE_OUT = Path("knowledge/margin_low_crowding_probe_readiness.md")
DEFAULT_SAMPLE_TRADE_DATES = ("20200630", "20201231", "20210630", "20211231", "20220630", "20221230", "20230630", "20231229")


def derive_month_end_trade_dates(feature_cache: Path, *, start: str = "2020-01-01", end: str = "2023-12-31", max_dates: int = 48) -> tuple[str, ...]:
    """Pick month-end-like dates from the existing feature cache to maximize margin/cache overlap.

    This is still bounded sampling, not full daily margin backfill. It avoids hundreds of Tushare calls
    while giving a stronger read-only diagnostic than quarter-end-only sampling.
    """
    try:
        dates = pd.read_csv(feature_cache, usecols=["date"])
    except Exception:
        return DEFAULT_SAMPLE_TRADE_DATES
    ds = pd.to_datetime(dates["date"], errors="coerce").dropna()
    ds = ds[(ds >= pd.Timestamp(start)) & (ds <= pd.Timestamp(end))]
    if ds.empty:
        return DEFAULT_SAMPLE_TRADE_DATES
    month_end = ds.drop_duplicates().sort_values().groupby(ds.dt.to_period("M")).max().dt.strftime("%Y%m%d").tolist()
    if max_dates and len(month_end) > max_dates:
        month_end = month_end[-max_dates:]
    return tuple(month_end) or DEFAULT_SAMPLE_TRADE_DATES


def fetch_margin_detail(token: str | None, trade_dates: tuple[str, ...]) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not token:
        return pd.DataFrame(), {"error": "missing_tushare_token"}
    try:
        import tushare as ts  # type: ignore
    except Exception as exc:
        return pd.DataFrame(), {"error": f"import_failed: {type(exc).__name__}: {exc}"}
    pro = ts.pro_api(token)
    frames: list[pd.DataFrame] = []
    by_date: dict[str, Any] = {}
    for trade_date in trade_dates:
        try:
            df = pro.margin_detail(trade_date=trade_date)
            df = normalize_margin_frame(df if df is not None else pd.DataFrame())
            frames.append(df)
            by_date[trade_date] = {"rows": int(len(df)), "tickers": int(df["ts_code"].nunique()) if "ts_code" in df.columns else 0}
        except Exception as exc:
            by_date[trade_date] = {"error": f"{type(exc).__name__}: {exc}"}
        time.sleep(0.2)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return normalize_margin_frame(combined), {"by_trade_date": by_date}


def to_markdown(payload: dict[str, Any]) -> str:
    d = payload.get("decision", {})
    c = payload.get("coverage", {})
    diag = payload.get("diagnostics", {})
    corr = payload.get("correlations", {})
    lines = [
        "# Margin Low-crowding Probe Readiness",
        "",
        "Scope: read-only diagnostic. No workflow run, no queue write, no daemon start.",
        "",
        "## Decision",
        f"- Decision: `{d.get('decision')}`",
        f"- Reasons: {', '.join(d.get('reasons', []))}",
        "",
        "## Coverage",
        f"- Feature rows after dropna: {c.get('rows')}",
        f"- Dates: {c.get('dates')}",
        f"- Tickers: {c.get('tickers')}",
        f"- Raw margin rows: {payload.get('raw_margin_rows')}",
        f"- Merged overlap before dropna: {payload.get('merged_overlap_rows_before_feature_dropna')}",
        f"- Feature cache: {payload.get('feature_cache')}",
        "",
        "## Bucket diagnostics",
        "| Score | Spread mean | Positive rate | Observations |",
        "|---|---:|---:|---:|",
    ]
    for key in ["baseline", "low_margin_crowding", "confirmation"]:
        rec = diag.get(key, {})
        lines.append(f"| {key} | {rec.get('spread_mean')} | {rec.get('spread_positive_rate')} | {rec.get('observations')} |")
    lines += [
        "",
        "## Correlations",
        f"- low_margin_vs_baseline: {corr.get('low_margin_vs_baseline')}",
        f"- confirmation_vs_baseline: {corr.get('confirmation_vs_baseline')}",
        f"- low_margin_vs_turnover: {corr.get('low_margin_vs_turnover')}",
        f"- low_margin_vs_turnover_shock_5_20: {corr.get('low_margin_vs_turnover_shock_5_20')}",
        "",
        "## Interpretation",
    ]
    if d.get("decision") == "proceed_controlled_margin_low_crowding_probe":
        lines.append("融资余额低拥挤特征在只读样本上有正向且非重复迹象，可以进入下一轮 controlled workflow 计划。")
    elif d.get("decision") == "need_margin_feature_store_extension":
        lines.append("信号没有被否定，但当前只读样本/覆盖不足或还未超过既有 benchmark；下一步应扩展 margin feature store，而不是直接跑完整因子。")
    elif d.get("decision") == "stop_margin_low_crowding_non_incremental":
        lines.append("融资余额低拥挤与现有 baseline 高度重复，暂不作为新增机制。")
    else:
        lines.append("只读诊断未显示足够正向增量，暂不进入 controlled workflow。")
    return "\n".join(lines) + "\n"


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    feature_cache = choose_feature_cache()
    if not feature_cache:
        raise SystemExit("missing feature cache")
    trade_dates = derive_month_end_trade_dates(feature_cache)
    margin, fetch_meta = fetch_margin_detail(os.environ.get("TUSHARE_TOKEN"), trade_dates)
    sample, report = build_margin_feature_sample(margin, feature_cache)
    report.update({
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_trade_dates": list(trade_dates),
        "sample_mode": "feature_cache_month_end_bounded",
        "fetch_meta": fetch_meta,
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
    })
    sample.to_csv(CSV_OUT, index=False)
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md = to_markdown(report)
    MD_OUT.write_text(md, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(md, encoding="utf-8")
    print(json.dumps({
        "json": str(JSON_OUT),
        "csv": str(CSV_OUT),
        "markdown": str(MD_OUT),
        "knowledge": str(KNOWLEDGE_OUT),
        "decision": report.get("decision"),
        "coverage": report.get("coverage"),
        "diagnostics": report.get("diagnostics"),
        "correlations": report.get("correlations"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
