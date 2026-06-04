from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import add_historical_valuation_screen_features


def _max_drawdown_details(spread: pd.Series) -> dict[str, Any]:
    spread = spread.dropna().sort_index()
    if spread.empty:
        return {"max_drawdown": 0.0, "peak_date": None, "bottom_date": None, "recovery_date": None}
    cumulative = spread.cumsum()
    running_peak = cumulative.cummax()
    drawdown = cumulative - running_peak
    bottom_date = drawdown.idxmin()
    peak_level = running_peak.loc[bottom_date]
    peak_candidates = cumulative.loc[:bottom_date][cumulative.loc[:bottom_date] == peak_level]
    peak_date = peak_candidates.index[-1] if not peak_candidates.empty else cumulative.index[0]
    recovery = cumulative.loc[bottom_date:][cumulative.loc[bottom_date:] >= peak_level]
    recovery_date = recovery.index[0] if not recovery.empty else None
    return {
        "max_drawdown": float(drawdown.loc[bottom_date]),
        "peak_date": str(pd.Timestamp(peak_date).date()),
        "bottom_date": str(pd.Timestamp(bottom_date).date()),
        "recovery_date": str(pd.Timestamp(recovery_date).date()) if recovery_date is not None else None,
    }


def _daily_spread(usable: pd.DataFrame) -> pd.Series:
    daily = usable.pivot_table(index="date", columns="valuation_bucket", values="forward_return_5d", aggfunc="mean")
    if "cheap" not in daily.columns or "expensive" not in daily.columns:
        return pd.Series(dtype=float)
    return daily["cheap"] - daily["expensive"]


def _screen_metrics(usable: pd.DataFrame) -> dict[str, Any]:
    spread = _daily_spread(usable)
    return {
        "mean_daily_spread": float(spread.mean()) if not spread.empty else None,
        "max_drawdown": _max_drawdown_details(spread)["max_drawdown"],
        "daily_count": int(spread.notna().sum()),
    }


def build_historical_valuation_risk_diagnostic(
    *,
    run_id: str,
    frame: pd.DataFrame,
    source_path: str,
    cheap_screen_result: dict[str, Any],
    window: int = 756,
    min_periods: int | None = None,
    max_drawdown_limit: float = -0.35,
) -> dict[str, Any]:
    featured = add_historical_valuation_screen_features(frame, window=window, min_periods=min_periods or window)
    usable = featured.dropna(subset=["historical_valuation_cheapness", "forward_return_5d", "valuation_bucket"]).copy()
    spread = _daily_spread(usable)
    drawdown = _max_drawdown_details(spread)
    worst_dates = [
        {"date": str(pd.Timestamp(idx).date()), "spread": float(value)}
        for idx, value in spread.sort_values().head(10).items()
    ]

    industry_summary: list[dict[str, Any]] = []
    if "industry" in usable.columns:
        for industry, rows in usable.groupby("industry"):
            means = rows.groupby("valuation_bucket")["forward_return_5d"].mean()
            if "cheap" not in means or "expensive" not in means:
                continue
            industry_summary.append({
                "industry": str(industry),
                "cheap_expensive_spread": float(means["cheap"] - means["expensive"]),
                "cheap_rows": int((rows["valuation_bucket"] == "cheap").sum()),
                "expensive_rows": int((rows["valuation_bucket"] == "expensive").sum()),
                "row_count": int(len(rows)),
            })
    negative_industries = sorted(
        [item for item in industry_summary if item["cheap_expensive_spread"] < 0],
        key=lambda item: item["cheap_expensive_spread"],
    )
    positive_industries = sorted(
        [item for item in industry_summary if item["cheap_expensive_spread"] >= 0],
        key=lambda item: item["cheap_expensive_spread"],
        reverse=True,
    )

    repair_candidates: list[dict[str, Any]] = []
    if negative_industries and "industry" in usable.columns:
        excluded = {item["industry"] for item in negative_industries}
        repaired = usable[~usable["industry"].astype(str).isin(excluded)].copy()
        metrics = _screen_metrics(repaired)
        repair_candidates.append({
            "candidate": "exclude_negative_industries",
            "excluded_industry_count": len(excluded),
            "remaining_rows": int(len(repaired)),
            **metrics,
            "risk_pass": metrics["max_drawdown"] >= max_drawdown_limit,
        })

    moderate = usable[(usable["historical_valuation_cheapness"] >= 0.6) & (usable["historical_valuation_cheapness"] < 0.8)].copy()
    expensive = usable[usable["valuation_bucket"] == "expensive"].copy()
    moderate_labeled = pd.concat([
        moderate.assign(valuation_bucket="cheap"),
        expensive.assign(valuation_bucket="expensive"),
    ], ignore_index=True)
    metrics = _screen_metrics(moderate_labeled)
    repair_candidates.append({
        "candidate": "moderate_cheap_only_0.6_to_0.8",
        "remaining_rows": int(len(moderate_labeled)),
        **metrics,
        "risk_pass": metrics["max_drawdown"] >= max_drawdown_limit,
    })

    by_year = []
    if not spread.empty:
        yearly = spread.groupby(pd.to_datetime(spread.index).year)
        for year, values in yearly:
            by_year.append({
                "year": int(year),
                "mean_daily_spread": float(values.mean()),
                "max_drawdown": _max_drawdown_details(values)["max_drawdown"],
                "daily_count": int(values.notna().sum()),
            })

    best_repair = None
    if repair_candidates:
        best_repair = sorted(repair_candidates, key=lambda item: (item.get("risk_pass") is True, item.get("mean_daily_spread") or -999, item.get("max_drawdown") or -999), reverse=True)[0]
    if best_repair and best_repair.get("risk_pass") and (best_repair.get("mean_daily_spread") or 0) > 0:
        recommended_next_step = "manual_review_repaired_screen"
        overall_status = "manual_review"
    else:
        recommended_next_step = "stop_route_or_design_risk_filter"
        overall_status = "fail"

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "cheap_screen_risk_diagnostic",
        "route_id": "historical_relative_valuation_repair",
        "source_path": source_path,
        "cheap_screen_overall_status": cheap_screen_result.get("overall_status"),
        "cheap_screen_recommended_next_step": cheap_screen_result.get("recommended_next_step"),
        "row_count": int(len(frame)),
        "usable_row_count": int(len(usable)),
        "usable_ticker_count": int(usable["ticker"].nunique()) if len(usable) else 0,
        "original_drawdown": drawdown,
        "worst_dates": worst_dates,
        "negative_industries": negative_industries[:20],
        "positive_industries": positive_industries[:20],
        "yearly_spread_summary": by_year,
        "repair_candidates": repair_candidates,
        "best_repair_candidate": best_repair,
        "overall_status": overall_status,
        "recommended_next_step": recommended_next_step,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "auto_promotion_allowed": False,
        "blocked_actions": ["queue_write", "full_backtest", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
    }


def risk_diagnostic_to_markdown(diagnostic: dict[str, Any]) -> str:
    lines = [
        "# Historical Valuation Cheap Screen Risk Diagnostic",
        "",
        f"run_id: {diagnostic.get('run_id')}",
        f"overall_status: {diagnostic.get('overall_status')}",
        f"recommended_next_step: {diagnostic.get('recommended_next_step')}",
        f"original_drawdown: {diagnostic.get('original_drawdown')}",
        f"best_repair_candidate: {diagnostic.get('best_repair_candidate')}",
        f"controlled_execution_allowed: {diagnostic.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {diagnostic.get('queue_write_allowed')}",
        "",
        "## Worst dates",
    ]
    for item in diagnostic.get("worst_dates") or []:
        lines.append(f"- {item.get('date')}: {item.get('spread')}")
    lines.append("")
    lines.append("## Negative industries")
    for item in diagnostic.get("negative_industries") or []:
        lines.append(f"- {item.get('industry')}: spread={item.get('cheap_expensive_spread')}, rows={item.get('row_count')}")
    lines.append("")
    lines.append("## Repair candidates")
    for item in diagnostic.get("repair_candidates") or []:
        lines.append(f"- {item.get('candidate')}: mean={item.get('mean_daily_spread')}, max_drawdown={item.get('max_drawdown')}, risk_pass={item.get('risk_pass')}")
    return "\n".join(lines).rstrip() + "\n"


def write_risk_diagnostic(diagnostic: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "cheap_screen_risk_diagnostic.json"
    md_path = out / "cheap_screen_risk_diagnostic.md"
    json_path.write_text(json.dumps(diagnostic, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(risk_diagnostic_to_markdown(diagnostic), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
