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

PIT_FIELDS_TO_EXTEND = ["profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]


def _date_bounds(frame: pd.DataFrame) -> dict[str, str | None]:
    if frame.empty or "date" not in frame.columns:
        return {"start_date": None, "end_date": None}
    dates = pd.to_datetime(frame["date"])
    return {"start_date": dates.min().strftime("%Y-%m-%d"), "end_date": dates.max().strftime("%Y-%m-%d")}


def build_pit_cache_extension_plan(
    *,
    run_id: str,
    overlay_diagnostic: dict[str, Any],
    base_frame: pd.DataFrame,
    pit_frame: pd.DataFrame,
    base_path: str,
    pit_path: str,
    target_overlay_coverage: float = 0.60,
) -> dict[str, Any]:
    base_tickers = sorted(str(t) for t in base_frame.get("ticker", pd.Series(dtype=str)).dropna().unique())
    pit_tickers = sorted(str(t) for t in pit_frame.get("ticker", pd.Series(dtype=str)).dropna().unique())
    base_set = set(base_tickers)
    pit_set = set(pit_tickers)
    missing_tickers = sorted(base_set - pit_set)
    overlap_tickers = sorted(base_set.intersection(pit_set))
    base_bounds = _date_bounds(base_frame)
    pit_bounds = _date_bounds(pit_frame)
    base_dates = pd.to_datetime(base_frame["date"]) if "date" in base_frame.columns and not base_frame.empty else pd.Series(dtype="datetime64[ns]")
    pit_dates = pd.to_datetime(pit_frame["date"]) if "date" in pit_frame.columns and not pit_frame.empty else pd.Series(dtype="datetime64[ns]")
    missing_early_window = None
    missing_late_window = None
    if not base_dates.empty and not pit_dates.empty:
        if base_dates.min() < pit_dates.min():
            missing_early_window = {"start_date": base_dates.min().strftime("%Y-%m-%d"), "end_date": (pit_dates.min() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")}
        if base_dates.max() > pit_dates.max():
            missing_late_window = {"start_date": (pit_dates.max() + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), "end_date": base_dates.max().strftime("%Y-%m-%d")}
    overlay_coverage = overlay_diagnostic.get("overlay_coverage") or {}
    low_fields = [field for field in PIT_FIELDS_TO_EXTEND if float(overlay_coverage.get(field, 0.0)) < target_overlay_coverage]
    current_min_coverage = min((float(overlay_coverage.get(field, 0.0)) for field in PIT_FIELDS_TO_EXTEND), default=0.0)
    needs_extension = bool(low_fields)
    estimated_rows_for_full_base_overlay = int(len(base_frame))
    estimated_current_overlay_rows = int(overlay_diagnostic.get("overlap_rows") or 0)
    estimated_missing_overlay_rows = max(0, estimated_rows_for_full_base_overlay - estimated_current_overlay_rows)
    if needs_extension:
        decision = "await_human_approval_for_pit_cache_extension"
        recommended_next_step = "approve_or_decline_pit_cache_extension"
        human_required = True
        next_allowed_actions = ["manual_review", "approve_pit_cache_extension", "stop_proxy_route"]
    else:
        decision = "no_extension_needed"
        recommended_next_step = "rerun_proxy_field_resolution_with_overlay"
        human_required = False
        next_allowed_actions = ["rerun_proxy_field_resolution_with_overlay"]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "pit_cache_extension_plan",
        "base_path": base_path,
        "pit_path": pit_path,
        "decision": decision,
        "recommended_next_step": recommended_next_step,
        "human_required": human_required,
        "target_overlay_coverage": target_overlay_coverage,
        "current_min_overlay_coverage": current_min_coverage,
        "low_fields": low_fields,
        "base_rows": int(len(base_frame)),
        "pit_rows": int(len(pit_frame)),
        "estimated_current_overlay_rows": estimated_current_overlay_rows,
        "estimated_missing_overlay_rows": estimated_missing_overlay_rows,
        "base_ticker_count": len(base_tickers),
        "pit_ticker_count": len(pit_tickers),
        "overlap_ticker_count": len(overlap_tickers),
        "missing_ticker_count": len(missing_tickers),
        "missing_tickers": missing_tickers,
        "base_date_window": base_bounds,
        "pit_date_window": pit_bounds,
        "missing_early_window": missing_early_window,
        "missing_late_window": missing_late_window,
        "extension_requirements": {
            "tickers": "cover all base tickers or explicitly approve reduced universe",
            "date_window": "cover base date window or explicitly approve shorter PIT-aligned evaluation window",
            "fields": PIT_FIELDS_TO_EXTEND,
            "minimum_overlay_coverage": target_overlay_coverage,
            "reuse_existing_pit_cache": True,
        },
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
    }


def pit_cache_extension_plan_to_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# PIT Cache Extension Plan",
        "",
        f"decision: {plan.get('decision')}",
        f"recommended_next_step: {plan.get('recommended_next_step')}",
        f"human_required: {plan.get('human_required')}",
        f"controlled_execution_allowed: {plan.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {plan.get('queue_write_allowed')}",
        "",
        "## Coverage target",
        f"- target_overlay_coverage: {plan.get('target_overlay_coverage')}",
        f"- current_min_overlay_coverage: {plan.get('current_min_overlay_coverage')}",
        f"- low_fields: {', '.join(plan.get('low_fields') or [])}",
        "",
        "## Cache scope",
        f"- base_rows: {plan.get('base_rows')}",
        f"- pit_rows: {plan.get('pit_rows')}",
        f"- estimated_current_overlay_rows: {plan.get('estimated_current_overlay_rows')}",
        f"- estimated_missing_overlay_rows: {plan.get('estimated_missing_overlay_rows')}",
        f"- base_ticker_count: {plan.get('base_ticker_count')}",
        f"- pit_ticker_count: {plan.get('pit_ticker_count')}",
        f"- overlap_ticker_count: {plan.get('overlap_ticker_count')}",
        f"- missing_ticker_count: {plan.get('missing_ticker_count')}",
        f"- base_date_window: {plan.get('base_date_window')}",
        f"- pit_date_window: {plan.get('pit_date_window')}",
        f"- missing_early_window: {plan.get('missing_early_window')}",
        f"- missing_late_window: {plan.get('missing_late_window')}",
        "",
        "## Missing tickers",
    ]
    missing_tickers = plan.get("missing_tickers") or []
    lines.extend(f"- {ticker}" for ticker in missing_tickers[:50])
    if len(missing_tickers) > 50:
        lines.append(f"- ... {len(missing_tickers) - 50} more")
    lines += ["", "## Next allowed actions"]
    lines.extend(f"- {action}" for action in plan.get("next_allowed_actions") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in plan.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_pit_cache_extension_plan(plan: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "pit_cache_extension_plan.json"
    markdown_path = out / "pit_cache_extension_plan.md"
    json_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(pit_cache_extension_plan_to_markdown(plan), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
