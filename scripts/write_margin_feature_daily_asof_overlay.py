#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import build_margin_low_crowding_features, build_readonly_margin_probe_report
from factor_lab.margin_source_mvp import choose_feature_cache, load_feature_cache, normalize_margin_frame

ARTIFACT_DIR = Path("artifacts/margin_feature_daily_asof_overlay")
FEATURE_CSV_OUT = ARTIFACT_DIR / "margin_feature_daily_asof_overlay.csv"
JSON_OUT = ARTIFACT_DIR / "margin_feature_daily_asof_overlay.json"
MD_OUT = ARTIFACT_DIR / "margin_feature_daily_asof_overlay.md"
KNOWLEDGE_OUT = Path("knowledge/margin_low_crowding_daily_asof_overlay.md")
RAW_MARGIN_PANEL = Path("artifacts/margin_feature_monthly_panel/margin_raw_panel.csv")
START_DATE = "2020-06-01"
END_DATE = "2023-12-31"


def build_daily_asof_margin_frame(base_features: pd.DataFrame, raw_margin: pd.DataFrame) -> pd.DataFrame:
    base = base_features.copy()
    base["date"] = pd.to_datetime(base["date"], errors="coerce")
    base["ts_code"] = base["ticker"].astype(str)
    base = base[(base["date"] >= pd.to_datetime(START_DATE)) & (base["date"] <= pd.to_datetime(END_DATE))].copy()
    margin = normalize_margin_frame(raw_margin)
    margin["date"] = pd.to_datetime(margin["trade_date"], format="%Y%m%d", errors="coerce")
    keep = [c for c in ["date", "ts_code", "rzye", "rqye", "rzmre", "rzche", "rzrqye"] if c in margin.columns]
    margin = margin[keep].dropna(subset=["date", "ts_code"]).sort_values(["date", "ts_code"])
    parts: list[pd.DataFrame] = []
    for ticker, g in base.sort_values("date").groupby("ts_code", sort=False):
        mg = margin[margin["ts_code"] == ticker].sort_values("date")
        if mg.empty:
            gg = g.copy()
            for col in ["rzye", "rqye", "rzmre", "rzche", "rzrqye"]:
                if col not in gg.columns:
                    gg[col] = pd.NA
            parts.append(gg)
            continue
        merged = pd.merge_asof(g.sort_values("date"), mg, on="date", by="ts_code", direction="backward", suffixes=("", "_margin"))
        parts.append(merged)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def decision_from_report(report: dict[str, Any]) -> dict[str, Any]:
    c = report.get("coverage", {})
    diag = report.get("diagnostics", {})
    corr = report.get("correlations", {})
    reasons: list[str] = []
    if (c.get("rows") or 0) < 10000:
        reasons.append("daily_asof_rows_too_low")
    if (c.get("dates") or 0) < 500:
        reasons.append("daily_asof_dates_too_low")
    if (diag.get("low_margin_crowding", {}).get("spread_mean") or 0) <= 0:
        reasons.append("low_margin_spread_not_positive")
    conf = diag.get("confirmation", {}).get("spread_mean")
    base = diag.get("baseline", {}).get("spread_mean")
    if conf is None or base is None or conf <= base:
        reasons.append("confirmation_not_incremental_vs_local_baseline")
    if conf is None or conf <= 0.0062253011:
        reasons.append("confirmation_not_above_benchmark")
    for key in ["low_margin_vs_baseline", "low_margin_vs_turnover"]:
        v = corr.get(key)
        if v is not None and abs(float(v)) >= 0.85:
            reasons.append(f"{key}_too_high")
    if reasons:
        if any(r.endswith("too_low") for r in reasons):
            decision = "need_more_margin_daily_coverage"
        else:
            decision = "stop_margin_low_crowding_daily_asof_not_incremental"
    else:
        decision = "proceed_single_controlled_workflow_with_daily_asof_overlay"
    return {"decision": decision, "reasons": reasons or ["daily_asof_overlay_passed_precheck"]}


def to_markdown(payload: dict[str, Any]) -> str:
    d = payload.get("daily_asof_decision", {})
    c = payload.get("coverage", {})
    diag = payload.get("diagnostics", {})
    lines = ["# Margin Low-crowding Daily As-of Overlay", "", f"Decision: `{d.get('decision')}`", f"Reasons: {', '.join(d.get('reasons') or [])}", "", "## Coverage", f"- Rows: {c.get('rows')}", f"- Dates: {c.get('dates')}", f"- Tickers: {c.get('tickers')}", "", "## Bucket diagnostics", "| Score | Spread mean | Positive rate | Observations |", "|---|---:|---:|---:|"]
    for key in ["baseline", "low_margin_crowding", "confirmation"]:
        rec = diag.get(key, {})
        lines.append(f"| {key} | {rec.get('spread_mean')} | {rec.get('spread_positive_rate')} | {rec.get('observations')} |")
    return "\n".join(lines) + "\n"


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    feature_cache = choose_feature_cache()
    if not feature_cache:
        raise SystemExit("missing feature cache")
    if not RAW_MARGIN_PANEL.exists():
        raise SystemExit(f"missing raw margin panel: {RAW_MARGIN_PANEL}")
    base = load_feature_cache(feature_cache)
    raw = pd.read_csv(RAW_MARGIN_PANEL)
    asof = build_daily_asof_margin_frame(base, raw)
    sample = build_margin_low_crowding_features(asof)
    report = build_readonly_margin_probe_report(sample)
    report.update({"schema_version": 1, "generated_at": datetime.now(timezone.utc).isoformat(), "feature_cache": str(feature_cache), "raw_margin_panel": str(RAW_MARGIN_PANEL), "raw_daily_asof_rows_before_dropna": int(len(asof)), "no_factor_run": True, "no_queue_write": True, "no_daemon_start": True})
    report["daily_asof_decision"] = decision_from_report(report)
    sample.to_csv(FEATURE_CSV_OUT, index=False)
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md = to_markdown(report)
    MD_OUT.write_text(md, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(md, encoding="utf-8")
    print(json.dumps({"json": str(JSON_OUT), "feature_csv": str(FEATURE_CSV_OUT), "markdown": str(MD_OUT), "knowledge": str(KNOWLEDGE_OUT), "daily_asof_decision": report.get("daily_asof_decision"), "coverage": report.get("coverage"), "diagnostics": report.get("diagnostics"), "correlations": report.get("correlations")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
