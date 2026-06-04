from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import add_historical_valuation_screen_features
from factor_lab.autonomous_strategy_risk_diagnostic import _max_drawdown_details


def _daily_spread(frame: pd.DataFrame) -> pd.Series:
    daily = frame.pivot_table(index="date", columns="valuation_bucket", values="forward_return_5d", aggfunc="mean")
    if "cheap" not in daily.columns or "expensive" not in daily.columns:
        return pd.Series(dtype=float)
    return daily["cheap"] - daily["expensive"]


def _candidate_metrics(name: str, frame: pd.DataFrame, *, max_drawdown_limit: float) -> dict[str, Any]:
    spread = _daily_spread(frame)
    drawdown = _max_drawdown_details(spread)
    rank_ic = None
    if len(frame) >= 3 and frame["historical_valuation_cheapness"].nunique() > 1:
        rank_ic = float(frame["historical_valuation_cheapness"].corr(frame["forward_return_5d"], method="spearman"))
    mean_daily_spread = float(spread.mean()) if not spread.empty else None
    risk_pass = bool(mean_daily_spread is not None and mean_daily_spread > 0 and drawdown["max_drawdown"] >= max_drawdown_limit)
    return {
        "candidate": name,
        "usable_row_count": int(len(frame)),
        "usable_ticker_count": int(frame["ticker"].nunique()) if len(frame) else 0,
        "daily_count": int(spread.notna().sum()),
        "mean_daily_spread": mean_daily_spread,
        "rank_ic": rank_ic,
        "max_drawdown": drawdown["max_drawdown"],
        "drawdown_peak_date": drawdown["peak_date"],
        "drawdown_bottom_date": drawdown["bottom_date"],
        "drawdown_recovery_date": drawdown["recovery_date"],
        "risk_pass": risk_pass,
    }


def _quantile_filter(frame: pd.DataFrame, column: str, q: float, *, direction: str) -> pd.Series:
    thresholds = frame.groupby("date")[column].transform(lambda s: s.quantile(q))
    if direction == "lte":
        return frame[column] <= thresholds
    if direction == "gte":
        return frame[column] >= thresholds
    raise ValueError(f"unknown direction: {direction}")


def build_value_trap_risk_filter_probe(
    *,
    run_id: str,
    frame: pd.DataFrame,
    source_path: str,
    route_verdict: dict[str, Any],
    window: int = 756,
    min_periods: int | None = None,
    max_drawdown_limit: float = -0.35,
    min_usable_rows: int = 1000,
) -> dict[str, Any]:
    if route_verdict.get("verdict") != "design_risk_filter_one_probe":
        return {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "value_trap_risk_filter_probe",
            "overall_status": "blocked",
            "recommended_next_step": "respect_route_verdict",
            "route_verdict": route_verdict.get("verdict"),
            "controlled_execution_allowed": False,
            "queue_write_allowed": False,
        }

    featured = add_historical_valuation_screen_features(frame, window=window, min_periods=min_periods or window)
    usable = featured.dropna(subset=["historical_valuation_cheapness", "valuation_bucket", "forward_return_5d"]).copy()
    candidates: list[dict[str, Any]] = []

    base = usable[usable["valuation_bucket"].isin(["cheap", "expensive"])].copy()
    candidates.append(_candidate_metrics("baseline_cheap_vs_expensive", base, max_drawdown_limit=max_drawdown_limit))

    moderate = usable[(usable["historical_valuation_cheapness"] >= 0.6) & (usable["historical_valuation_cheapness"] < 0.8)].copy()
    expensive = usable[usable["valuation_bucket"] == "expensive"].copy()
    moderate_labeled = pd.concat([
        moderate.assign(valuation_bucket="cheap"),
        expensive.assign(valuation_bucket="expensive"),
    ], ignore_index=True)
    candidates.append(_candidate_metrics("moderate_cheap_only_0.6_to_0.8", moderate_labeled, max_drawdown_limit=max_drawdown_limit))

    if "volatility_20" in usable.columns:
        vol = usable[usable["volatility_20"].notna()].copy()
        vol_filtered = vol[_quantile_filter(vol, "volatility_20", 0.70, direction="lte")]
        candidates.append(_candidate_metrics("exclude_top_30pct_daily_volatility_20", vol_filtered, max_drawdown_limit=max_drawdown_limit))

    if "turnover" in usable.columns:
        turn = usable[usable["turnover"].notna()].copy()
        turn_filtered = turn[_quantile_filter(turn, "turnover", 0.30, direction="gte")]
        candidates.append(_candidate_metrics("exclude_bottom_30pct_daily_turnover", turn_filtered, max_drawdown_limit=max_drawdown_limit))

    if "roe" in usable.columns and "debt_to_asset" in usable.columns:
        quality = usable[usable["roe"].notna() & usable["debt_to_asset"].notna()].copy()
        roe_mask = _quantile_filter(quality, "roe", 0.30, direction="gte")
        debt_mask = _quantile_filter(quality, "debt_to_asset", 0.70, direction="lte")
        quality_filtered = quality[roe_mask & debt_mask]
        candidates.append(_candidate_metrics("quality_overlay_roe_top70_debt_bottom70", quality_filtered, max_drawdown_limit=max_drawdown_limit))

    valid_candidates = [item for item in candidates if item["usable_row_count"] >= min_usable_rows and item["mean_daily_spread"] is not None]
    best = None
    if valid_candidates:
        best = sorted(valid_candidates, key=lambda item: (item["risk_pass"], item["max_drawdown"], item["mean_daily_spread"] or -999), reverse=True)[0]
    if best and best["risk_pass"]:
        overall_status = "manual_review"
        recommended_next_step = "manual_review_repaired_screen"
    else:
        overall_status = "fail"
        recommended_next_step = "stop_route"

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "value_trap_risk_filter_probe",
        "route_id": "historical_relative_valuation_repair",
        "source_path": source_path,
        "route_verdict": route_verdict.get("verdict"),
        "max_drawdown_limit": float(max_drawdown_limit),
        "min_usable_rows": int(min_usable_rows),
        "candidate_results": candidates,
        "best_candidate": best,
        "overall_status": overall_status,
        "recommended_next_step": recommended_next_step,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "auto_promotion_allowed": False,
        "blocked_actions": ["queue_write", "full_backtest", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def risk_filter_probe_to_markdown(probe: dict[str, Any]) -> str:
    lines = [
        "# Value Trap Risk Filter Probe",
        "",
        f"run_id: {probe.get('run_id')}",
        f"overall_status: {probe.get('overall_status')}",
        f"recommended_next_step: {probe.get('recommended_next_step')}",
        f"route_verdict: {probe.get('route_verdict')}",
        f"best_candidate: {probe.get('best_candidate')}",
        f"controlled_execution_allowed: {probe.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {probe.get('queue_write_allowed')}",
        "",
        "## Candidates",
    ]
    for item in probe.get("candidate_results") or []:
        lines.append(f"- {item.get('candidate')}: rows={item.get('usable_row_count')}, mean={item.get('mean_daily_spread')}, mdd={item.get('max_drawdown')}, risk_pass={item.get('risk_pass')}")
    return "\n".join(lines).rstrip() + "\n"


def write_risk_filter_probe(probe: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "value_trap_risk_filter_probe.json"
    md_path = out / "value_trap_risk_filter_probe.md"
    json_path.write_text(json.dumps(probe, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(risk_filter_probe_to_markdown(probe), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
