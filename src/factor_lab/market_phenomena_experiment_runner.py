from __future__ import annotations

import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.market_phenomena_schema import SAFETY_FLAGS


def _horizon_from_target(target_variables: list[str]) -> int:
    for target in target_variables:
        match = re.search(r"future_(\d+)d_return", target)
        if match:
            return int(match.group(1))
    return 60


def compute_future_return(feature_frame: pd.DataFrame, *, horizon_days: int) -> pd.DataFrame:
    df = feature_frame.copy()
    if "date" in df.columns:
        df = df.sort_values(["ticker", "date"] if "ticker" in df.columns else ["date"])
    col = f"future_{horizon_days}d_return"
    if col in df.columns:
        return df
    if "ticker" in df.columns:
        future_close = df.groupby("ticker")["close"].shift(-horizon_days)
    else:
        future_close = df["close"].shift(-horizon_days)
    df[col] = future_close / df["close"] - 1.0
    return df


def _median(df: pd.DataFrame, col: str) -> float:
    value = df[col].median(skipna=True)
    if pd.isna(value):
        return math.nan
    return float(value)


def derive_condition_fields(feature_frame: pd.DataFrame, *, required_fields: list[str]) -> pd.DataFrame:
    df = feature_frame.copy()
    if "date" in df.columns:
        df = df.sort_values(["ticker", "date"] if "ticker" in df.columns else ["date"])
    if "debt_to_asset_delta" in required_fields and "debt_to_asset_delta" not in df.columns and "debt_to_asset" in df.columns:
        if "ticker" in df.columns:
            df["debt_to_asset_delta"] = df.groupby("ticker")["debt_to_asset"].diff()
        else:
            df["debt_to_asset_delta"] = df["debt_to_asset"].diff()
    if "industry_return_60d" in required_fields and "industry_return_60d" not in df.columns and {"ticker", "industry", "close"}.issubset(df.columns):
        # Research diagnostic derivation: ticker-level trailing return averaged
        # by industry/date. This is an observable past-state proxy, not a target.
        ticker_ret = df.groupby("ticker")["close"].pct_change(60)
        if ticker_ret.notna().sum() == 0:
            ticker_ret = df.groupby("ticker")["close"].pct_change(2)
        df["industry_return_60d"] = ticker_ret.groupby([df["industry"], df["date"]]).transform("mean")
    return df


def _assign_groups(plan: dict[str, Any], df: pd.DataFrame) -> pd.Series:
    pid = plan.get("phenomenon_id")
    groups = pd.Series("other", index=df.index, dtype="object")
    pb_low = df["pb"] <= _median(df, "pb") if "pb" in df.columns else pd.Series(False, index=df.index)
    if pid == "value_trap_escape_after_balance_sheet_repair_v1":
        debt_repair = df["debt_to_asset_delta"] <= _median(df, "debt_to_asset_delta") if "debt_to_asset_delta" in df.columns else pd.Series(False, index=df.index)
        cashflow_good = df["operating_cashflow_to_profit"] >= _median(df, "operating_cashflow_to_profit") if "operating_cashflow_to_profit" in df.columns else pd.Series(False, index=df.index)
        repair = debt_repair & cashflow_good
        groups[pb_low & repair] = "balance_sheet_repair_low_valuation"
        groups[pb_low & ~repair] = "low_valuation_no_repair"
        groups[~pb_low & repair] = "balance_sheet_repair_not_low_valuation"
        return groups

    profit_good = df["profit_yoy"] >= _median(df, "profit_yoy") if "profit_yoy" in df.columns else pd.Series(False, index=df.index)
    roe_good = df["roe"] >= _median(df, "roe") if "roe" in df.columns else pd.Series(False, index=df.index)
    debt_safe = df["debt_to_asset"] <= _median(df, "debt_to_asset") if "debt_to_asset" in df.columns else pd.Series(True, index=df.index)
    quality = profit_good & roe_good & debt_safe
    groups[pb_low & quality] = "quality_repair_low_valuation"
    groups[pb_low & ~quality] = "low_quality_low_valuation"
    groups[~pb_low & quality] = "quality_repair_not_low_valuation"
    return groups


def _group_metrics(df: pd.DataFrame, group_col: str, return_col: str, comparison_groups: list[str]) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    for group in comparison_groups:
        part = df[df[group_col] == group][return_col].dropna()
        metrics[group] = {
            "row_count": int(part.shape[0]),
            "mean_return": float(part.mean()) if not part.empty else None,
            "median_return": float(part.median()) if not part.empty else None,
            "downside_risk": float((part < 0).mean()) if not part.empty else None,
            "max_drawdown_proxy": float(part.min()) if not part.empty else None,
        }
    return metrics


def run_conditional_distribution_experiment(plan: dict[str, Any], feature_frame: pd.DataFrame) -> dict[str, Any]:
    target_variables = plan.get("target_variables") or []
    horizon = _horizon_from_target(target_variables)
    return_col = f"future_{horizon}d_return"
    required = ["date", "ticker", "close", *list(plan.get("condition_variables") or [])]
    feature_frame = derive_condition_fields(feature_frame, required_fields=required)
    missing_columns = [col for col in required if col not in feature_frame.columns]
    if missing_columns:
        return {
            "phenomenon_id": plan.get("phenomenon_id"),
            "title": plan.get("title"),
            "experiment_type": plan.get("experiment_type"),
            "result_status": "blocked_missing_columns",
            "missing_columns": missing_columns,
            **SAFETY_FLAGS,
        }

    df = compute_future_return(feature_frame, horizon_days=horizon)
    df = df.dropna(subset=[return_col, *list(plan.get("condition_variables") or [])]).copy()
    if df.empty:
        return {
            "phenomenon_id": plan.get("phenomenon_id"),
            "title": plan.get("title"),
            "experiment_type": plan.get("experiment_type"),
            "result_status": "insufficient_sample",
            "missing_columns": [],
            "usable_row_count": 0,
            "usable_ticker_count": 0,
            **SAFETY_FLAGS,
        }
    df["_phenomenon_group"] = _assign_groups(plan, df)
    comparison_groups = list(plan.get("comparison_groups") or [])
    metrics = _group_metrics(df, "_phenomenon_group", return_col, comparison_groups)
    target_group = comparison_groups[0] if comparison_groups else None
    control_groups = comparison_groups[1:]
    target_mean = metrics.get(target_group or "", {}).get("mean_return")
    control_means = [metrics[g]["mean_return"] for g in control_groups if metrics.get(g, {}).get("mean_return") is not None]
    control_mean = sum(control_means) / len(control_means) if control_means else None
    spread_vs_control = target_mean - control_mean if target_mean is not None and control_mean is not None else None

    criteria = plan.get("success_criteria") or {}
    min_rows = int(criteria.get("minimum_usable_rows") or 250)
    min_tickers = int(criteria.get("minimum_usable_tickers") or 50)
    usable_tickers = int(df["ticker"].nunique()) if "ticker" in df.columns else 0
    usable_rows = int(df.shape[0])
    if usable_rows < min_rows or usable_tickers < min_tickers:
        status = "insufficient_sample"
    elif spread_vs_control is not None and spread_vs_control > 0:
        status = "pass"
    else:
        status = "fail"

    return {
        "phenomenon_id": plan.get("phenomenon_id"),
        "title": plan.get("title"),
        "experiment_type": plan.get("experiment_type"),
        "result_status": status,
        "missing_columns": [],
        "target_return_column": return_col,
        "usable_row_count": usable_rows,
        "usable_ticker_count": usable_tickers,
        "groups": metrics,
        "target_group": target_group,
        "spread_vs_control": spread_vs_control,
        "success_criteria": criteria,
        **SAFETY_FLAGS,
    }


def build_minimal_verification_result(*, run_id: str, plan_report: dict[str, Any], feature_frame: pd.DataFrame) -> dict[str, Any]:
    results = [run_conditional_distribution_experiment(plan, feature_frame) for plan in plan_report.get("experiments") or []]
    status_counts: dict[str, int] = {}
    for item in results:
        status_counts[item.get("result_status")] = status_counts.get(item.get("result_status"), 0) + 1
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "minimal_verification_result_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_plan_run_id": plan_report.get("run_id"),
        "summary": {"experiment_count": len(results), **status_counts},
        "results": results,
        **SAFETY_FLAGS,
    }


def minimal_verification_result_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Minimal Verification Result",
        "",
        f"run_id: {report.get('run_id')}",
        f"mode: {report.get('mode')}",
        f"strategy_generation_allowed: {report.get('strategy_generation_allowed')}",
        f"backtest_allowed: {report.get('backtest_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Summary",
    ]
    for key, value in (report.get("summary") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Results")
    for item in report.get("results") or []:
        lines.extend([
            "",
            f"### {item.get('phenomenon_id')}: {item.get('title')}",
            f"- result_status: {item.get('result_status')}",
            f"- usable_row_count: {item.get('usable_row_count')}",
            f"- usable_ticker_count: {item.get('usable_ticker_count')}",
            f"- target_group: {item.get('target_group')}",
            f"- spread_vs_control: {item.get('spread_vs_control')}",
        ])
    return "\n".join(lines).rstrip() + "\n"


def write_minimal_verification_result(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "minimal_verification_result.json"
    markdown_path = out / "minimal_verification_result.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(minimal_verification_result_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
