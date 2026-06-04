from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]

OVERLAY_FIELDS = ["profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]


def build_pit_overlay_diagnostic(
    *,
    run_id: str,
    base_frame: pd.DataFrame,
    pit_frame: pd.DataFrame,
    base_path: str,
    pit_path: str,
    min_overlay_coverage: float = 0.60,
) -> dict[str, Any]:
    if base_frame.empty:
        decision = "blocked_empty_base_frame"
        merged = base_frame.copy()
    elif pit_frame.empty:
        decision = "blocked_empty_pit_frame"
        merged = base_frame.copy()
    else:
        base = base_frame.copy()
        pit = pit_frame.copy()
        base["date"] = pd.to_datetime(base["date"])
        pit["date"] = pd.to_datetime(pit["date"])
        pit_cols = [c for c in ["ticker", "date", *OVERLAY_FIELDS, "pit_feature_validated", "pit_source_ann_date", "pit_source_end_date"] if c in pit.columns]
        merged = base.merge(pit[pit_cols], on=["ticker", "date"], how="left", suffixes=("", "__pit"))
        for field in OVERLAY_FIELDS:
            pit_col = f"{field}__pit" if f"{field}__pit" in merged.columns else field
            if pit_col in merged.columns:
                if f"{field}__pit" in merged.columns:
                    pit_values = merged[f"{field}__pit"]
                    if field in merged.columns:
                        base_values = merged[field]
                        merged[f"{field}__overlay"] = pit_values.where(pit_values.notna(), base_values)
                    else:
                        merged[f"{field}__overlay"] = pit_values
                else:
                    merged[f"{field}__overlay"] = merged[pit_col]
            elif field in merged.columns:
                merged[f"{field}__overlay"] = merged[field]
        decision = "computed"

    overlay_coverage = {
        field: float(merged.get(f"{field}__overlay", pd.Series(dtype="float64")).notna().mean()) if not merged.empty else 0.0
        for field in OVERLAY_FIELDS
    }
    base_coverage = {
        field: float(base_frame[field].notna().mean()) if field in base_frame.columns and not base_frame.empty else 0.0
        for field in OVERLAY_FIELDS
    }
    pit_coverage = {
        field: float(pit_frame[field].notna().mean()) if field in pit_frame.columns and not pit_frame.empty else 0.0
        for field in OVERLAY_FIELDS
    }
    overlap_rows = int(merged["profit_yoy__overlay"].notna().sum()) if "profit_yoy__overlay" in merged.columns else 0
    base_tickers = int(base_frame["ticker"].nunique()) if "ticker" in base_frame.columns and not base_frame.empty else 0
    pit_tickers = int(pit_frame["ticker"].nunique()) if "ticker" in pit_frame.columns and not pit_frame.empty else 0
    overlap_tickers = int(len(set(base_frame.get("ticker", pd.Series(dtype=str)).dropna()).intersection(set(pit_frame.get("ticker", pd.Series(dtype=str)).dropna())))) if not base_frame.empty and not pit_frame.empty else 0
    low_after_overlay = [field for field, cov in overlay_coverage.items() if cov < min_overlay_coverage]
    if decision == "computed":
        if low_after_overlay:
            decision = "extend_pit_cache_coverage"
            recommended_next_step = "extend_pit_financial_cache_to_base_universe_and_window"
            next_allowed_actions = ["extend_pit_cache", "rerun_pit_overlay_diagnostic"]
        else:
            decision = "prepare_proxy_pit_alignment_review"
            recommended_next_step = "prove_proxy_report_date_alignment"
            next_allowed_actions = ["pit_safety_preflight"]
    else:
        recommended_next_step = "inspect_base_or_pit_cache"
        next_allowed_actions = ["inspect_cache"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "pit_overlay_diagnostic",
        "base_path": base_path,
        "pit_path": pit_path,
        "decision": decision,
        "recommended_next_step": recommended_next_step,
        "min_overlay_coverage": min_overlay_coverage,
        "base_rows": int(len(base_frame)),
        "pit_rows": int(len(pit_frame)),
        "overlap_rows": overlap_rows,
        "base_tickers": base_tickers,
        "pit_tickers": pit_tickers,
        "overlap_tickers": overlap_tickers,
        "base_coverage": base_coverage,
        "pit_coverage": pit_coverage,
        "overlay_coverage": overlay_coverage,
        "low_after_overlay": low_after_overlay,
        "root_cause": (
            "The base Tushare feature cache contains placeholder financial columns with 0 coverage; separate PIT financial caches contain data, "
            "but the autonomous-strategy field-resolution path did not overlay those PIT caches before computing coverage."
        ),
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
    }


def pit_overlay_diagnostic_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# PIT Overlay Diagnostic",
        "",
        f"decision: {report.get('decision')}",
        f"recommended_next_step: {report.get('recommended_next_step')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Cache overlap",
        f"- base_rows: {report.get('base_rows')}",
        f"- pit_rows: {report.get('pit_rows')}",
        f"- overlap_rows: {report.get('overlap_rows')}",
        f"- base_tickers: {report.get('base_tickers')}",
        f"- pit_tickers: {report.get('pit_tickers')}",
        f"- overlap_tickers: {report.get('overlap_tickers')}",
        "",
        "## Coverage",
        "| Field | Base | PIT | Overlay |",
        "|---|---:|---:|---:|",
    ]
    for field in OVERLAY_FIELDS:
        lines.append(
            f"| {field} | {report.get('base_coverage', {}).get(field)} | "
            f"{report.get('pit_coverage', {}).get(field)} | {report.get('overlay_coverage', {}).get(field)} |"
        )
    lines += ["", "## Root cause", str(report.get("root_cause"))]
    return "\n".join(lines).rstrip() + "\n"


def write_pit_overlay_diagnostic(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "pit_overlay_diagnostic.json"
    markdown_path = out / "pit_overlay_diagnostic.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(pit_overlay_diagnostic_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
