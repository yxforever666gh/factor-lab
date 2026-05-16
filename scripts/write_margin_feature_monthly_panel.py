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

ARTIFACT_DIR = Path("artifacts/margin_feature_monthly_panel")
RAW_CSV_OUT = ARTIFACT_DIR / "margin_raw_panel.csv"
FEATURE_CSV_OUT = ARTIFACT_DIR / "margin_feature_monthly_panel.csv"
JSON_OUT = ARTIFACT_DIR / "margin_feature_monthly_panel.json"
MD_OUT = ARTIFACT_DIR / "margin_feature_monthly_panel.md"
KNOWLEDGE_OUT = Path("knowledge/margin_low_crowding_monthly_panel.md")
BENCHMARK = 0.0062253011


def month_end_trade_dates(feature_cache_path: str | Path, *, start: str = "20200601", end: str = "20231231") -> list[str]:
    df = pd.read_csv(feature_cache_path, usecols=["date"])
    s = pd.to_datetime(df["date"], errors="coerce")
    dates = pd.DataFrame({"dt": s.dropna().drop_duplicates()})
    dates = dates[(dates["dt"] >= pd.to_datetime(start)) & (dates["dt"] <= pd.to_datetime(end))]
    if dates.empty:
        return []
    dates["month"] = dates["dt"].dt.to_period("M")
    out = dates.groupby("month")["dt"].max().sort_values().dt.strftime("%Y%m%d").tolist()
    return out


def fetch_margin_panel(token: str | None, trade_dates: list[str]) -> tuple[pd.DataFrame, dict[str, Any]]:
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
        time.sleep(0.12)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return normalize_margin_frame(combined), {"by_trade_date": by_date}


def monthly_decision(report: dict[str, Any]) -> dict[str, Any]:
    coverage = report.get("coverage", {})
    diag = report.get("diagnostics", {})
    corr = report.get("correlations", {})
    reasons: list[str] = []
    if (coverage.get("dates") or 0) < 24:
        reasons.append("dates_below_24")
    if (coverage.get("rows") or 0) < 500:
        reasons.append("rows_below_500")
    low_spread = diag.get("low_margin_crowding", {}).get("spread_mean")
    baseline_spread = diag.get("baseline", {}).get("spread_mean")
    confirmation_spread = diag.get("confirmation", {}).get("spread_mean")
    if low_spread is None or low_spread <= 0:
        reasons.append("low_margin_spread_not_positive")
    if confirmation_spread is None or baseline_spread is None or confirmation_spread <= baseline_spread:
        reasons.append("confirmation_not_incremental_vs_local_baseline")
    if confirmation_spread is None or confirmation_spread <= BENCHMARK:
        reasons.append("confirmation_not_above_value_quality_benchmark")
    for key in ["low_margin_vs_baseline", "low_margin_vs_turnover"]:
        v = corr.get(key)
        if v is not None and abs(float(v)) >= 0.85:
            reasons.append(f"{key}_too_high")
    if any(r in reasons for r in ["dates_below_24", "rows_below_500"]):
        decision = "need_more_margin_panel_coverage"
    elif any(r.endswith("too_high") for r in reasons):
        decision = "stop_margin_low_crowding_non_incremental"
    elif "low_margin_spread_not_positive" in reasons or "confirmation_not_incremental_vs_local_baseline" in reasons or "confirmation_not_above_value_quality_benchmark" in reasons:
        decision = "stop_margin_low_crowding_sample_unstable"
    else:
        decision = "proceed_margin_controlled_workflow_config"
    return {"decision": decision, "reasons": reasons or ["monthly_panel_passed_controlled_workflow_precheck"]}


def to_markdown(payload: dict[str, Any]) -> str:
    d = payload.get("monthly_decision", {})
    c = payload.get("coverage", {})
    diag = payload.get("diagnostics", {})
    corr = payload.get("correlations", {})
    lines = [
        "# Margin Low-crowding Monthly Panel",
        "",
        "Scope: read-only monthly panel diagnostic. No workflow run, no queue write, no daemon start.",
        "",
        "## Decision",
        f"- Decision: `{d.get('decision')}`",
        f"- Reasons: {', '.join(d.get('reasons', []))}",
        "",
        "## Coverage",
        f"- Rows: {c.get('rows')}",
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
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    feature_cache = choose_feature_cache()
    if not feature_cache:
        raise SystemExit("missing feature cache")
    dates = month_end_trade_dates(feature_cache)
    margin, fetch_meta = fetch_margin_panel(os.environ.get("TUSHARE_TOKEN"), dates)
    margin.to_csv(RAW_CSV_OUT, index=False)
    sample, report = build_margin_feature_sample(margin, feature_cache)
    report.update({
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "trade_dates": dates,
        "trade_date_count": len(dates),
        "fetch_meta": fetch_meta,
        "benchmark": {"value_quality_no_distress_bucket_spread": BENCHMARK},
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
    })
    report["monthly_decision"] = monthly_decision(report)
    sample.to_csv(FEATURE_CSV_OUT, index=False)
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md = to_markdown(report)
    MD_OUT.write_text(md, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(md, encoding="utf-8")
    print(json.dumps({
        "json": str(JSON_OUT),
        "feature_csv": str(FEATURE_CSV_OUT),
        "raw_csv": str(RAW_CSV_OUT),
        "markdown": str(MD_OUT),
        "knowledge": str(KNOWLEDGE_OUT),
        "monthly_decision": report.get("monthly_decision"),
        "coverage": report.get("coverage"),
        "diagnostics": report.get("diagnostics"),
        "correlations": report.get("correlations"),
        "trade_date_count": len(dates),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
