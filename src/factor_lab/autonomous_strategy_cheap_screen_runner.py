from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BUCKET_ORDER = ["expensive", "middle", "cheap"]


def _last_value_percentile(values: np.ndarray) -> float:
    values = values[~np.isnan(values)]
    if len(values) == 0:
        return np.nan
    return float((values <= values[-1]).sum() / len(values))


def add_historical_valuation_screen_features(
    frame: pd.DataFrame,
    *,
    window: int = 756,
    min_periods: int | None = None,
) -> pd.DataFrame:
    required = {"date", "ticker", "pb", "pe_ttm", "forward_return_5d"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"missing required columns for historical valuation cheap screen: {missing}")
    min_periods = int(min_periods or window)
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values(["ticker", "date"]).reset_index(drop=True)
    for source, target in [("pb", "pb_history_percentile"), ("pe_ttm", "pe_ttm_history_percentile")]:
        out[target] = (
            out.groupby("ticker", group_keys=False)[source]
            .rolling(window=window, min_periods=min_periods)
            .apply(_last_value_percentile, raw=True)
            .reset_index(level=0, drop=True)
        )
    out["historical_valuation_percentile"] = out[["pb_history_percentile", "pe_ttm_history_percentile"]].mean(axis=1)
    out["historical_valuation_cheapness"] = 1.0 - out["historical_valuation_percentile"]
    out["valuation_bucket"] = pd.cut(
        out["historical_valuation_cheapness"],
        bins=[-np.inf, 0.4, 0.6, np.inf],
        labels=["expensive", "middle", "cheap"],
    ).astype("string")
    return out


def _max_drawdown(values: pd.Series) -> float:
    if values.empty:
        return 0.0
    cumulative = values.fillna(0).cumsum()
    drawdown = cumulative - cumulative.cummax()
    return float(drawdown.min())


def build_historical_valuation_cheap_screen_result(
    *,
    run_id: str,
    frame: pd.DataFrame,
    source_path: str,
    window: int = 756,
    min_periods: int | None = None,
    min_rows: int = 100,
    min_spread: float = 0.0,
    min_rank_ic: float = 0.0,
    max_drawdown_limit: float = -0.35,
) -> dict[str, Any]:
    featured = add_historical_valuation_screen_features(frame, window=window, min_periods=min_periods)
    usable = featured.dropna(subset=["historical_valuation_cheapness", "forward_return_5d", "valuation_bucket"]).copy()
    bucket_summary = []
    for bucket in BUCKET_ORDER:
        rows = usable[usable["valuation_bucket"] == bucket]
        bucket_summary.append({
            "bucket": bucket,
            "row_count": int(len(rows)),
            "mean_forward_return_5d": float(rows["forward_return_5d"].mean()) if len(rows) else None,
            "median_forward_return_5d": float(rows["forward_return_5d"].median()) if len(rows) else None,
        })
    bucket_by_name = {item["bucket"]: item for item in bucket_summary}
    cheap_mean = bucket_by_name["cheap"].get("mean_forward_return_5d")
    expensive_mean = bucket_by_name["expensive"].get("mean_forward_return_5d")
    cheap_expensive_spread = None
    if cheap_mean is not None and expensive_mean is not None:
        cheap_expensive_spread = float(cheap_mean - expensive_mean)
    rank_ic = float(usable["historical_valuation_cheapness"].corr(usable["forward_return_5d"], method="spearman")) if len(usable) >= 3 else None

    daily = usable.pivot_table(index="date", columns="valuation_bucket", values="forward_return_5d", aggfunc="mean")
    if "cheap" in daily.columns and "expensive" in daily.columns:
        spread_series = daily["cheap"] - daily["expensive"]
    else:
        spread_series = pd.Series(dtype=float)
    drawdown_proxy = _max_drawdown(spread_series)

    information_pass = (
        len(usable) >= min_rows
        and cheap_expensive_spread is not None
        and cheap_expensive_spread > min_spread
        and rank_ic is not None
        and rank_ic > min_rank_ic
    )
    risk_pass = drawdown_proxy >= max_drawdown_limit
    if information_pass and risk_pass:
        recommended_next_step = "allow_one_controlled_backtest"
        overall_status = "pass"
    elif len(usable) < min_rows:
        recommended_next_step = "request_data_or_lower_min_rows"
        overall_status = "blocked"
    elif not information_pass:
        recommended_next_step = "stop_route_or_switch_mechanism"
        overall_status = "fail"
    else:
        recommended_next_step = "manual_review_risk"
        overall_status = "manual_review"

    industry_summary: list[dict[str, Any]] = []
    if "industry" in usable.columns:
        for industry, rows in usable.groupby("industry"):
            if rows["valuation_bucket"].nunique() < 2:
                continue
            means = rows.groupby("valuation_bucket")["forward_return_5d"].mean()
            if "cheap" in means and "expensive" in means:
                industry_summary.append({
                    "industry": str(industry),
                    "cheap_expensive_spread": float(means["cheap"] - means["expensive"]),
                    "row_count": int(len(rows)),
                })

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "metric_bearing_cheap_screen",
        "route_id": "historical_relative_valuation_repair",
        "source_path": source_path,
        "row_count": int(len(frame)),
        "usable_row_count": int(len(usable)),
        "ticker_count": int(featured["ticker"].nunique()),
        "usable_ticker_count": int(usable["ticker"].nunique()) if len(usable) else 0,
        "window": int(window),
        "min_periods": int(min_periods or window),
        "bucket_summary": bucket_summary,
        "cheap_expensive_spread": cheap_expensive_spread,
        "rank_ic": rank_ic,
        "drawdown_proxy": drawdown_proxy,
        "max_drawdown_limit": float(max_drawdown_limit),
        "information_screen_status": "pass" if information_pass else "fail",
        "risk_screen_status": "pass" if risk_pass else "fail",
        "overall_status": overall_status,
        "recommended_next_step": recommended_next_step,
        "industry_summary": industry_summary[:30],
        "controlled_execution_allowed": overall_status == "pass",
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "auto_promotion_allowed": False,
        "blocked_actions": ["queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def cheap_screen_result_to_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Historical Valuation Cheap Screen Result",
        "",
        f"run_id: {result.get('run_id')}",
        f"route_id: {result.get('route_id')}",
        f"overall_status: {result.get('overall_status')}",
        f"recommended_next_step: {result.get('recommended_next_step')}",
        f"information_screen_status: {result.get('information_screen_status')}",
        f"risk_screen_status: {result.get('risk_screen_status')}",
        f"cheap_expensive_spread: {result.get('cheap_expensive_spread')}",
        f"rank_ic: {result.get('rank_ic')}",
        f"drawdown_proxy: {result.get('drawdown_proxy')}",
        f"controlled_execution_allowed: {result.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {result.get('queue_write_allowed')}",
        "",
        "## Bucket summary",
    ]
    for item in result.get("bucket_summary") or []:
        lines.append(f"- {item.get('bucket')}: rows={item.get('row_count')}, mean={item.get('mean_forward_return_5d')}, median={item.get('median_forward_return_5d')}")
    lines.append("")
    lines.append("## Blocked actions")
    lines.extend(f"- {action}" for action in result.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_cheap_screen_result(result: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "cheap_screen_result.json"
    md_path = out / "cheap_screen_result.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(cheap_screen_result_to_markdown(result), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
