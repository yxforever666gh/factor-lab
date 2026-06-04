from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import add_historical_valuation_screen_features
from factor_lab.autonomous_strategy_risk_filter_probe import _candidate_metrics, _quantile_filter

BLOCKED_ACTIONS = ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"]
REQUIRED_FIELDS = ["date", "ticker", "pb", "pe_ttm", "forward_return_5d", "roe", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]


def build_proxy_feature_frame(*, base_frame: pd.DataFrame, pit_frame: pd.DataFrame) -> pd.DataFrame:
    base = base_frame.copy()
    pit = pit_frame.copy()
    base["date"] = pd.to_datetime(base["date"])
    pit["date"] = pd.to_datetime(pit["date"])
    fields = ["ticker", "date", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]
    merged = base.merge(pit[[c for c in fields if c in pit.columns]], on=["ticker", "date"], how="left", suffixes=("", "__pit"))
    for field in ["profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]:
        pit_col = f"{field}__pit"
        if pit_col in merged.columns:
            pit_values = merged[pit_col]
            if field in merged.columns:
                merged[field] = pit_values.where(pit_values.notna(), merged[field])
            else:
                merged[field] = pit_values
            merged = merged.drop(columns=[pit_col])
    return merged


def build_proxy_cheap_screen(
    *,
    run_id: str,
    frame: pd.DataFrame,
    plan: dict[str, Any],
    source_path: str,
    window: int = 756,
    min_periods: int = 756,
    max_drawdown_limit: float = -0.35,
    min_usable_rows: int = 1000,
) -> dict[str, Any]:
    missing = [field for field in REQUIRED_FIELDS if field not in frame.columns]
    if missing:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "proxy_cheap_screen",
            "overall_status": "blocked",
            "recommended_next_step": "repair_proxy_feature_frame",
            "missing_fields": missing,
            "controlled_execution_allowed": False,
            "queue_write_allowed": False,
            "timer_enable_allowed": False,
            "blocked_actions": BLOCKED_ACTIONS,
        }
    if plan.get("decision") != "prepare_proxy_cheap_screen_execution":
        return {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "proxy_cheap_screen",
            "overall_status": "blocked",
            "recommended_next_step": "respect_proxy_cheap_screen_plan",
            "plan_decision": plan.get("decision"),
            "controlled_execution_allowed": False,
            "queue_write_allowed": False,
            "timer_enable_allowed": False,
            "blocked_actions": BLOCKED_ACTIONS,
        }
    featured = add_historical_valuation_screen_features(frame, window=window, min_periods=min_periods)
    usable = featured.dropna(subset=["valuation_bucket", "forward_return_5d", "roe", "profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]).copy()
    pair = usable[usable["valuation_bucket"].isin(["cheap", "expensive"])].copy()
    candidates = []
    candidates.append(_candidate_metrics("cheap_baseline_pb_pe", pair, max_drawdown_limit=max_drawdown_limit))
    roe = pair[_quantile_filter(pair, "roe", 0.50, direction="gte")].copy()
    candidates.append(_candidate_metrics("cheap_plus_roe_top50", roe, max_drawdown_limit=max_drawdown_limit))
    profit = pair[pair["profit_yoy"] > 0].copy()
    candidates.append(_candidate_metrics("cheap_plus_profit_yoy_positive", profit, max_drawdown_limit=max_drawdown_limit))
    debt = pair[_quantile_filter(pair, "debt_to_asset", 0.50, direction="lte")].copy()
    candidates.append(_candidate_metrics("cheap_plus_debt_to_asset_bottom50", debt, max_drawdown_limit=max_drawdown_limit))
    cashflow = pair[pair["operating_cashflow_to_profit"] > 0].copy()
    candidates.append(_candidate_metrics("cheap_plus_operating_cashflow_to_profit_positive", cashflow, max_drawdown_limit=max_drawdown_limit))
    combined = pair[
        _quantile_filter(pair, "roe", 0.50, direction="gte")
        & (pair["profit_yoy"] > 0)
        & _quantile_filter(pair, "debt_to_asset", 0.50, direction="lte")
        & (pair["operating_cashflow_to_profit"] > 0)
    ].copy()
    candidates.append(_candidate_metrics("combined_quality_profit_proxy", combined, max_drawdown_limit=max_drawdown_limit))
    valid = [c for c in candidates if c["usable_row_count"] >= min_usable_rows and c["mean_daily_spread"] is not None]
    best = sorted(valid, key=lambda item: (item["risk_pass"], item["max_drawdown"], item["mean_daily_spread"] or -999), reverse=True)[0] if valid else None
    if best and best["risk_pass"]:
        overall_status = "manual_review"
        recommended_next_step = "manual_review_proxy_cheap_screen"
    else:
        overall_status = "fail"
        recommended_next_step = "stop_proxy_route"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "proxy_cheap_screen",
        "mechanism_id": "quality_profit_proxy_value_repair_v1",
        "source_path": source_path,
        "candidate_results": candidates,
        "best_candidate": best,
        "overall_status": overall_status,
        "recommended_next_step": recommended_next_step,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def write_proxy_cheap_screen(screen: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "proxy_cheap_screen_result.json"
    markdown_path = out / "proxy_cheap_screen_result.md"
    json_path.write_text(json.dumps(screen, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    lines = [
        "# Proxy Cheap Screen Result",
        "",
        f"overall_status: {screen.get('overall_status')}",
        f"recommended_next_step: {screen.get('recommended_next_step')}",
        f"best_candidate: {screen.get('best_candidate')}",
        f"controlled_execution_allowed: {screen.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {screen.get('queue_write_allowed')}",
        "",
        "## Candidates",
    ]
    for c in screen.get("candidate_results") or []:
        lines.append(f"- {c.get('candidate')}: rows={c.get('usable_row_count')}, mean={c.get('mean_daily_spread')}, mdd={c.get('max_drawdown')}, risk_pass={c.get('risk_pass')}")
    markdown_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
