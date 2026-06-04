from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import add_historical_valuation_screen_features
from factor_lab.autonomous_strategy_risk_filter_probe import _candidate_metrics, _quantile_filter


def build_distress_cheap_screen(
    *,
    run_id: str,
    frame: pd.DataFrame,
    pit_preflight: dict[str, Any],
    source_path: str,
    window: int = 756,
    min_periods: int | None = None,
    max_drawdown_limit: float = -0.35,
    min_usable_rows: int = 1000,
) -> dict[str, Any]:
    if not pit_preflight.get("ready_for_proxy_distress_screen"):
        return {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "distress_cheap_screen",
            "overall_status": "blocked",
            "recommended_next_step": "request_data_or_fix_pit_alignment",
            "controlled_execution_allowed": False,
            "queue_write_allowed": False,
        }
    featured = add_historical_valuation_screen_features(frame, window=window, min_periods=min_periods or window)
    usable = featured.dropna(subset=["historical_valuation_cheapness", "valuation_bucket", "forward_return_5d"]).copy()
    candidates: list[dict[str, Any]] = []
    base = usable[usable["valuation_bucket"].isin(["cheap", "expensive"])].copy()
    candidates.append(_candidate_metrics("baseline", base, max_drawdown_limit=max_drawdown_limit))

    filters: list[tuple[str, pd.Series]] = []
    if "debt_to_asset" in usable.columns:
        debt_frame = usable[usable["debt_to_asset"].notna()].copy()
        filters.append(("exclude_high_debt_to_asset_top30", debt_frame.index[(_quantile_filter(debt_frame, "debt_to_asset", 0.70, direction="lte"))]))
    if "operating_cashflow_to_profit" in usable.columns:
        cf_frame = usable[usable["operating_cashflow_to_profit"].notna()].copy()
        filters.append(("exclude_weak_cashflow_to_profit_bottom30", cf_frame.index[(_quantile_filter(cf_frame, "operating_cashflow_to_profit", 0.30, direction="gte"))]))
    if "roe" in usable.columns:
        roe_frame = usable[usable["roe"].notna()].copy()
        filters.append(("exclude_low_roe_bottom30", roe_frame.index[(_quantile_filter(roe_frame, "roe", 0.30, direction="gte"))]))

    for name, idx in filters:
        filtered = usable.loc[idx]
        candidates.append(_candidate_metrics(name, filtered[filtered["valuation_bucket"].isin(["cheap", "expensive"])], max_drawdown_limit=max_drawdown_limit))

    if filters:
        common_idx = set(filters[0][1])
        for _, idx in filters[1:]:
            common_idx &= set(idx)
        combined = usable.loc[sorted(common_idx)] if common_idx else usable.iloc[0:0]
        candidates.append(_candidate_metrics("combined_debt_cashflow_roe_proxy_filter", combined[combined["valuation_bucket"].isin(["cheap", "expensive"])], max_drawdown_limit=max_drawdown_limit))

    valid = [c for c in candidates if c["usable_row_count"] >= min_usable_rows and c["mean_daily_spread"] is not None]
    best = sorted(valid, key=lambda item: (item["risk_pass"], item["max_drawdown"], item["mean_daily_spread"] or -999), reverse=True)[0] if valid else None
    if best and best["risk_pass"]:
        overall_status = "manual_review"
        recommended_next_step = "manual_review_distress_repaired_screen"
    else:
        overall_status = "fail"
        recommended_next_step = "stop_route"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "distress_cheap_screen",
        "route_id": "quality_cashflow_distress_filter",
        "source_path": source_path,
        "candidate_results": candidates,
        "best_candidate": best,
        "overall_status": overall_status,
        "recommended_next_step": recommended_next_step,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def distress_cheap_screen_to_markdown(screen: dict[str, Any]) -> str:
    lines = [
        "# Quality Cashflow Distress Cheap Screen",
        "",
        f"overall_status: {screen.get('overall_status')}",
        f"recommended_next_step: {screen.get('recommended_next_step')}",
        f"best_candidate: {screen.get('best_candidate')}",
        f"controlled_execution_allowed: {screen.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {screen.get('queue_write_allowed')}",
        "",
        "## Candidates",
    ]
    for item in screen.get("candidate_results") or []:
        lines.append(f"- {item.get('candidate')}: rows={item.get('usable_row_count')}, mean={item.get('mean_daily_spread')}, mdd={item.get('max_drawdown')}, risk_pass={item.get('risk_pass')}")
    return "\n".join(lines).rstrip() + "\n"


def write_distress_cheap_screen(screen: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_cashflow_distress_cheap_screen.json"
    md_path = out / "quality_cashflow_distress_cheap_screen.md"
    json_path.write_text(json.dumps(screen, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(distress_cheap_screen_to_markdown(screen), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
