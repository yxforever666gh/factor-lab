from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PROXY_FIELDS = ["debt_to_asset", "operating_cashflow_to_profit", "profit_yoy", "netprofit_yoy"]


def build_distress_pit_preflight(
    *,
    run_id: str,
    field_resolution: dict[str, Any],
    pit_frame: pd.DataFrame,
    min_ticker_coverage: float = 0.50,
) -> dict[str, Any]:
    frame = pit_frame.copy()
    ticker_count = int(frame["ticker"].nunique()) if "ticker" in frame.columns else 0
    field_coverage = []
    for field in PROXY_FIELDS:
        exists = field in frame.columns
        non_null_rows = int(frame[field].notna().sum()) if exists else 0
        ticker_with_field = int(frame.loc[frame[field].notna(), "ticker"].nunique()) if exists and "ticker" in frame.columns else 0
        ticker_coverage = ticker_with_field / ticker_count if ticker_count else 0.0
        field_coverage.append({
            "field": field,
            "exists": exists,
            "non_null_rows": non_null_rows,
            "ticker_with_field": ticker_with_field,
            "ticker_coverage": round(ticker_coverage, 6),
            "coverage_pass": exists and ticker_coverage >= min_ticker_coverage,
        })
    pit_validated = bool(frame.get("pit_feature_validated", pd.Series([False])).fillna(False).astype(bool).any()) if len(frame) else False
    has_ann_date = "pit_source_ann_date" in frame.columns and frame["pit_source_ann_date"].notna().any()
    has_end_date = "pit_source_end_date" in frame.columns and frame["pit_source_end_date"].notna().any()
    required_proxy_pass = all(item["coverage_pass"] for item in field_coverage if item["field"] in {"debt_to_asset", "operating_cashflow_to_profit"})
    interest_missing = any(row.get("field") == "interest_coverage" and row.get("resolution_status") == "missing_external_or_derivation_required" for row in field_resolution.get("field_resolutions") or [])
    pit_safe = pit_validated and has_ann_date and has_end_date
    if pit_safe and required_proxy_pass:
        decision = "use_proxy_distress_screen_without_interest_coverage" if interest_missing else "use_proxy_distress_screen"
        ready = True
    else:
        decision = "request_data_or_fix_pit_alignment"
        ready = False
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "distress_pit_safety_preflight",
        "route_id": "quality_cashflow_distress_filter",
        "decision": decision,
        "ready_for_proxy_distress_screen": ready,
        "pit_validated": pit_validated,
        "has_announcement_dates": bool(has_ann_date),
        "has_report_end_dates": bool(has_end_date),
        "ticker_count": ticker_count,
        "row_count": int(len(frame)),
        "min_ticker_coverage": float(min_ticker_coverage),
        "field_coverage": field_coverage,
        "interest_coverage_missing": interest_missing,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "next_allowed_actions": ["run_proxy_distress_cheap_screen"] if ready else ["request_data_for_missing_or_unaligned_pit_fields"],
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def distress_pit_preflight_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Quality Cashflow Distress PIT Preflight",
        "",
        f"decision: {report.get('decision')}",
        f"ready_for_proxy_distress_screen: {report.get('ready_for_proxy_distress_screen')}",
        f"pit_validated: {report.get('pit_validated')}",
        f"has_announcement_dates: {report.get('has_announcement_dates')}",
        f"interest_coverage_missing: {report.get('interest_coverage_missing')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Field coverage",
    ]
    for item in report.get("field_coverage") or []:
        lines.append(f"- {item.get('field')}: exists={item.get('exists')}, ticker_coverage={item.get('ticker_coverage')}, pass={item.get('coverage_pass')}")
    return "\n".join(lines).rstrip() + "\n"


def write_distress_pit_preflight(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_cashflow_distress_pit_preflight.json"
    md_path = out / "quality_cashflow_distress_pit_preflight.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(distress_pit_preflight_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
