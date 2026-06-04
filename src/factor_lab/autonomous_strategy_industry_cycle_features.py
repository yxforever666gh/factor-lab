from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


def add_industry_cycle_features(frame: pd.DataFrame, *, window: int = 60, min_periods: int = 40) -> pd.DataFrame:
    required = {"date", "industry", "return_1d"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"missing required fields for industry cycle features: {missing}")
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"])
    industry_daily = (
        out.groupby(["industry", "date"], as_index=False)["return_1d"].mean()
        .sort_values(["industry", "date"])
        .rename(columns={"return_1d": "industry_return_1d"})
    )
    industry_daily["industry_return_60d"] = (
        industry_daily.groupby("industry", group_keys=False)["industry_return_1d"]
        .rolling(window=window, min_periods=min_periods)
        .sum()
        .reset_index(level=0, drop=True)
    )
    return out.merge(industry_daily[["industry", "date", "industry_return_1d", "industry_return_60d"]], on=["industry", "date"], how="left")


def build_industry_cycle_feature_derivation_report(*, run_id: str, frame: pd.DataFrame, source_path: str, feature_frame_path: str, window: int = 60, min_periods: int = 40) -> dict[str, Any]:
    featured = add_industry_cycle_features(frame, window=window, min_periods=min_periods)
    non_null = int(featured["industry_return_60d"].notna().sum())
    row_count = int(len(featured))
    ticker_count = int(featured["ticker"].nunique()) if "ticker" in featured.columns else 0
    coverage = non_null / row_count if row_count else 0.0
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "industry_cycle_feature_derivation",
        "source_path": source_path,
        "feature_frame_path": feature_frame_path,
        "derived_fields": [
            {
                "field": "industry_return_60d",
                "source_fields": ["industry", "date", "return_1d"],
                "derivation": f"industry_mean_return_1d_then_rolling_sum:{window}d",
                "window": window,
                "min_periods": min_periods,
                "non_null_rows": non_null,
                "row_count": row_count,
                "coverage_ratio": round(coverage, 6),
            }
        ],
        "row_count": row_count,
        "ticker_count": ticker_count,
        "coverage_ratio": round(coverage, 6),
        "ready_for_industry_cycle_screen": coverage >= 0.50,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "next_allowed_actions": ["run_industry_cycle_cheap_screen"] if coverage >= 0.50 else ["request_more_history_or_lower_min_periods"],
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def write_industry_cycle_feature_derivation(report: dict[str, Any], featured: pd.DataFrame, output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    csv_path = out / "industry_cycle_feature_frame.csv"
    jp = out / "industry_cycle_feature_derivation.json"
    mp = out / "industry_cycle_feature_derivation.md"
    featured.to_csv(csv_path, index=False)
    report = {**report, "feature_frame_path": str(csv_path)}
    jp.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    lines = ["# Industry Cycle Feature Derivation", "", f"coverage_ratio: {report.get('coverage_ratio')}", f"ready_for_industry_cycle_screen: {report.get('ready_for_industry_cycle_screen')}", f"feature_frame_path: {csv_path}", f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}", f"queue_write_allowed: {report.get('queue_write_allowed')}"]
    mp.write_text("\n".join(lines)+"\n", encoding="utf-8")
    return {"csv": csv_path, "json": jp, "markdown": mp}
