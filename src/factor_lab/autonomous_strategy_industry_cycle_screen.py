from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import add_historical_valuation_screen_features
from factor_lab.autonomous_strategy_risk_filter_probe import _candidate_metrics, _quantile_filter


def build_industry_cycle_cheap_screen(
    *,
    run_id: str,
    frame: pd.DataFrame,
    source_path: str,
    window: int = 756,
    min_periods: int = 756,
    max_drawdown_limit: float = -0.35,
    min_usable_rows: int = 1000,
) -> dict[str, Any]:
    if "industry_return_60d" not in frame.columns:
        return {"schema_version": 1, "run_id": run_id, "mode": "industry_cycle_cheap_screen", "overall_status": "blocked", "recommended_next_step": "derive_industry_return_60d", "controlled_execution_allowed": False, "queue_write_allowed": False}
    featured = add_historical_valuation_screen_features(frame, window=window, min_periods=min_periods)
    usable = featured.dropna(subset=["historical_valuation_cheapness", "valuation_bucket", "forward_return_5d", "industry_return_60d"]).copy()
    candidates = []
    base = usable[usable["valuation_bucket"].isin(["cheap", "expensive"])].copy()
    candidates.append(_candidate_metrics("baseline_cheap_vs_expensive", base, max_drawdown_limit=max_drawdown_limit))
    pos = usable[usable["industry_return_60d"] > 0].copy()
    candidates.append(_candidate_metrics("industry_return_60d_positive", pos[pos["valuation_bucket"].isin(["cheap", "expensive"])], max_drawdown_limit=max_drawdown_limit))
    top50 = usable[_quantile_filter(usable, "industry_return_60d", 0.50, direction="gte")].copy()
    candidates.append(_candidate_metrics("industry_return_60d_top50_by_date", top50[top50["valuation_bucket"].isin(["cheap", "expensive"])], max_drawdown_limit=max_drawdown_limit))
    top30 = usable[_quantile_filter(usable, "industry_return_60d", 0.70, direction="gte")].copy()
    candidates.append(_candidate_metrics("industry_return_60d_top30_by_date", top30[top30["valuation_bucket"].isin(["cheap", "expensive"])], max_drawdown_limit=max_drawdown_limit))
    if "industry_relative_pb" in usable.columns:
        anchor = usable[(usable["industry_return_60d"] > 0) & (usable["industry_relative_pb"] <= 0)].copy()
        candidates.append(_candidate_metrics("cycle_positive_and_industry_relative_pb_cheap", anchor[anchor["valuation_bucket"].isin(["cheap", "expensive"])], max_drawdown_limit=max_drawdown_limit))
    valid = [c for c in candidates if c["usable_row_count"] >= min_usable_rows and c["mean_daily_spread"] is not None]
    best = sorted(valid, key=lambda item: (item["risk_pass"], item["max_drawdown"], item["mean_daily_spread"] or -999), reverse=True)[0] if valid else None
    if best and best["risk_pass"]:
        overall_status = "manual_review"
        recommended_next_step = "manual_review_industry_cycle_screen"
    else:
        overall_status = "fail"
        recommended_next_step = "stop_route"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "industry_cycle_cheap_screen",
        "route_id": "industry_cycle_inflection_value_anchor_v1",
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


def write_industry_cycle_cheap_screen(screen: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out=Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    jp=out/'industry_cycle_cheap_screen.json'; mp=out/'industry_cycle_cheap_screen.md'
    jp.write_text(json.dumps(screen,ensure_ascii=False,indent=2,sort_keys=True),encoding='utf-8')
    lines=["# Industry Cycle Cheap Screen", "", f"overall_status: {screen.get('overall_status')}", f"recommended_next_step: {screen.get('recommended_next_step')}", f"best_candidate: {screen.get('best_candidate')}", f"controlled_execution_allowed: {screen.get('controlled_execution_allowed')}", f"queue_write_allowed: {screen.get('queue_write_allowed')}", "", "## Candidates"]
    for c in screen.get('candidate_results') or []:
        lines.append(f"- {c.get('candidate')}: rows={c.get('usable_row_count')}, mean={c.get('mean_daily_spread')}, mdd={c.get('max_drawdown')}, risk_pass={c.get('risk_pass')}")
    mp.write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return {'json':jp,'markdown':mp}
