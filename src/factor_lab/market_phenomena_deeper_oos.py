from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.market_phenomena_controlled_execution import _apply_risk_cost_constraints, _with_groups

GATE_KEYS = ["live_trading_allowed", "queue_write_allowed", "timer_enable_allowed", "daemon_restore_allowed", "auto_promotion_allowed"]
DEFAULT_HORIZONS = [5, 20, 60, 120]
DEFAULT_SPLIT_YEARS = {
    "train": [2017, 2018, 2019],
    "validation": [2020, 2021],
    "oos": [2022, 2023],
}


def _closed_gates() -> dict[str, bool]:
    return {key: False for key in GATE_KEYS}


def _ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    return out


def _return_col_for(df: pd.DataFrame, horizon: int) -> tuple[pd.DataFrame, str]:
    existing = f"forward_return_{horizon}d"
    if existing in df.columns:
        return df, existing
    existing2 = f"future_{horizon}d_return"
    if existing2 in df.columns:
        return df, existing2
    out = df.sort_values(["ticker", "date"]).copy() if {"ticker", "date"}.issubset(df.columns) else df.copy()
    col = f"future_{horizon}d_return"
    if "ticker" in out.columns:
        future_close = out.groupby("ticker")["close"].shift(-horizon)
    else:
        future_close = out["close"].shift(-horizon)
    out[col] = future_close / out["close"] - 1.0
    return out, col


def _split_frame(df: pd.DataFrame, years: list[int]) -> pd.DataFrame:
    if not years:
        return df.iloc[0:0].copy()
    return df[df["date"].dt.year.isin(years)].copy()


def _evaluate_split_horizon(*, base_frame: pd.DataFrame, split: str, years: list[int], horizon: int, target_group: str | None, constraints: dict[str, Any]) -> dict[str, Any]:
    with_returns, return_col = _return_col_for(base_frame, horizon)
    part = _split_frame(with_returns, years)
    constrained, application = _apply_risk_cost_constraints(part, constraints)
    grouped = _with_groups(constrained).dropna(subset=[return_col]).copy()
    target = grouped[grouped["_phenomenon_group"] == target_group][return_col].dropna() if target_group else pd.Series(dtype="float64")
    control = grouped[grouped["_phenomenon_group"] != target_group][return_col].dropna() if target_group else grouped[return_col].dropna()
    turnover_col = "turnover_rate" if "turnover_rate" in grouped.columns else "turnover" if "turnover" in grouped.columns else None
    spread = float(target.mean() - control.mean()) if not target.empty and not control.empty else None
    target_cost_adjusted = None
    control_cost_adjusted = None
    cost_adjusted_spread = None
    if turnover_col and not target.empty and not control.empty:
        target_rows = grouped[grouped["_phenomenon_group"] == target_group].dropna(subset=[return_col, turnover_col])
        control_rows = grouped[grouped["_phenomenon_group"] != target_group].dropna(subset=[return_col, turnover_col])
        if not target_rows.empty and not control_rows.empty:
            target_net = target_rows[return_col] - target_rows[turnover_col].abs() * 0.001
            control_net = control_rows[return_col] - control_rows[turnover_col].abs() * 0.001
            target_cost_adjusted = float(target_net.mean())
            control_cost_adjusted = float(control_net.mean())
            cost_adjusted_spread = float(target_net.mean() - control_net.mean())
    worst = float(target.min()) if not target.empty else None
    downside = float((target < 0).mean()) if not target.empty else None
    usable_rows = int(grouped.shape[0])
    status = "pass" if spread is not None and spread > 0 and (cost_adjusted_spread is None or cost_adjusted_spread > 0) and (worst is None or worst > -0.25) else "fail"
    if usable_rows < 20 or target.empty or control.empty:
        status = "insufficient_sample"
    return {
        "split": split,
        "years": years,
        "horizon": horizon,
        "return_column": return_col,
        "status": status,
        "usable_row_count": usable_rows,
        "target_row_count": int(target.shape[0]),
        "control_row_count": int(control.shape[0]),
        "spread_vs_control": spread,
        "target_mean_return": float(target.mean()) if not target.empty else None,
        "control_mean_return": float(control.mean()) if not control.empty else None,
        "target_cost_adjusted_mean_return": target_cost_adjusted,
        "control_cost_adjusted_mean_return": control_cost_adjusted,
        "cost_adjusted_spread_vs_control": cost_adjusted_spread,
        "target_downside_frequency": downside,
        "target_worst_forward_return": worst,
        "constraint_application": application,
    }


def _decision(results: list[dict[str, Any]]) -> tuple[str, list[str]]:
    oos = [r for r in results if r.get("split") == "oos"]
    if not oos:
        return "request_oos_split", ["missing_oos_result"]
    if all(r.get("status") == "pass" for r in oos):
        return "continue_to_strategy_design_review", ["oos_horizons_supported", "strategy_phase_still_requires_human_review"]
    if any(r.get("status") == "pass" for r in oos):
        return "continue_research_with_supported_horizons", ["partial_oos_horizon_support"]
    return "mutate_or_reject", ["oos_horizons_failed"]


def build_deeper_oos_horizon_report(*, run_id: str, iteration_plan: dict[str, Any], feature_frame: pd.DataFrame, horizons: list[int] | None = None, split_years: dict[str, list[int]] | None = None) -> dict[str, Any]:
    horizons = horizons or DEFAULT_HORIZONS
    split_years = split_years or DEFAULT_SPLIT_YEARS
    base = _ensure_datetime(feature_frame)
    constraints = iteration_plan.get("risk_cost_constraints") or {}
    target_group = iteration_plan.get("target_group")
    results = []
    for horizon in horizons:
        for split, years in split_years.items():
            results.append(_evaluate_split_horizon(base_frame=base, split=split, years=years, horizon=horizon, target_group=target_group, constraints=constraints))
    counts = Counter(item["status"] for item in results)
    decision, reason_codes = _decision(results)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "deeper_oos_horizon_report",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_iteration_plan_run_id": iteration_plan.get("run_id"),
        "phenomenon_id": iteration_plan.get("phenomenon_id"),
        "target_group": target_group,
        "horizons": horizons,
        "split_years": split_years,
        "summary": {
            "horizon_count": len(horizons),
            "split_count": len(split_years),
            "result_count": len(results),
            **dict(sorted(counts.items())),
        },
        "decision": decision,
        "reason_codes": reason_codes,
        "results": results,
        "strategy_generation_allowed": False,
        "human_approval_required_for_strategy_phase": True,
        **_closed_gates(),
    }


def validate_deeper_oos_horizon_report(report: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    if not any(item.get("split") == "oos" for item in report.get("results") or []):
        reason_codes.append("missing_oos_result")
    if not report.get("horizons"):
        reason_codes.append("missing_horizons")
    for key in GATE_KEYS:
        if report.get(key) is not False:
            reason_codes.append(f"gate_not_closed_{key}")
    if report.get("strategy_generation_allowed") is not False:
        reason_codes.append("strategy_generation_not_closed")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def deeper_oos_horizon_report_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Deeper OOS / Holding Horizon Report",
        "",
        f"run_id: {report.get('run_id')}",
        f"phenomenon_id: {report.get('phenomenon_id')}",
        f"decision: {report.get('decision')}",
        f"strategy_generation_allowed: {report.get('strategy_generation_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Summary",
    ]
    for key, value in (report.get("summary") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Results"])
    for item in report.get("results") or []:
        lines.append(f"- {item.get('split')} horizon={item.get('horizon')}: status={item.get('status')} spread={item.get('spread_vs_control')} cost_adj_spread={item.get('cost_adjusted_spread_vs_control')} rows={item.get('usable_row_count')}")
    return "\n".join(lines).rstrip() + "\n"


def write_deeper_oos_horizon_report(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "deeper_oos_horizon_report.json"
    markdown_path = out / "deeper_oos_horizon_report.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(deeper_oos_horizon_report_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
