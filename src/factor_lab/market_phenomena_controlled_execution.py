from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

PRODUCTION_GATE_KEYS = [
    "live_trading_allowed",
    "queue_write_allowed",
    "timer_enable_allowed",
    "daemon_restore_allowed",
    "auto_promotion_allowed",
]

CHECK_REQUIREMENTS: dict[str, list[list[str]]] = {
    "industry_split_robustness": [["industry"]],
    "size_split_robustness": [["total_mv"], ["market_cap"], ["size_bucket"]],
    "regime_split_robustness": [["market_regime"], ["date", "close"]],
    "turnover_sensitivity": [["turnover_rate"], ["turnover"]],
    "drawdown_sensitivity": [["ticker", "date", "close"]],
    "cost_sensitivity_probe": [["turnover_rate"], ["turnover"]],
}


def _closed_boundaries() -> dict[str, bool]:
    return {key: False for key in PRODUCTION_GATE_KEYS}


def _check_kind(check_name: str) -> str:
    if "industry" in check_name:
        return "bucket_spread"
    if "size" in check_name:
        return "bucket_spread"
    if "regime" in check_name:
        return "bucket_spread"
    if "turnover" in check_name:
        return "sensitivity"
    if "drawdown" in check_name:
        return "downside_diagnostic"
    if "cost" in check_name:
        return "cost_sensitivity"
    return "diagnostic"


def build_controlled_execution_plan(*, run_id: str, execution_request: dict[str, Any], iteration_plan: dict[str, Any]) -> dict[str, Any]:
    steps = []
    for index, check_name in enumerate(execution_request.get("requested_checks") or [], start=1):
        steps.append(
            {
                "step_id": f"step_{index:02d}",
                "check_name": check_name,
                "check_kind": _check_kind(check_name),
                "required_column_alternatives": CHECK_REQUIREMENTS.get(check_name, []),
                "artifact_only": True,
            }
        )
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "controlled_research_execution_plan",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_execution_request_run_id": execution_request.get("run_id"),
        "source_iteration_plan_run_id": iteration_plan.get("run_id"),
        "phenomenon_id": execution_request.get("phenomenon_id") or iteration_plan.get("phenomenon_id"),
        "target_group": iteration_plan.get("target_group"),
        "controlled_research_backtest_allowed": execution_request.get("controlled_research_backtest_allowed") is True,
        "production_execution_allowed": False,
        "live_trading_allowed": False,
        "queue_write_allowed": False,
        "production_boundaries": _closed_boundaries(),
        "risk_cost_constraints": execution_request.get("risk_cost_constraints") or iteration_plan.get("risk_cost_constraints") or {},
        "execution_steps": steps,
        "stop_conditions": execution_request.get("stop_conditions") or iteration_plan.get("stop_conditions") or [],
    }


def validate_controlled_execution_plan(plan: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    if plan.get("controlled_research_backtest_allowed") is not True:
        reason_codes.append("controlled_research_backtest_not_allowed")
    if not plan.get("execution_steps"):
        reason_codes.append("missing_execution_steps")
    if plan.get("production_execution_allowed") is not False:
        reason_codes.append("production_execution_not_closed")
    if plan.get("live_trading_allowed") is not False:
        reason_codes.append("live_trading_not_closed")
    if plan.get("queue_write_allowed") is not False:
        reason_codes.append("queue_write_not_closed")
    gates = plan.get("production_boundaries") or {}
    for key in PRODUCTION_GATE_KEYS:
        if gates.get(key) is not False:
            reason_codes.append(f"production_gate_not_closed_{key}")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def _missing_columns_for(alternatives: list[list[str]], columns: set[str]) -> list[str]:
    if not alternatives:
        return []
    for alternative in alternatives:
        if set(alternative).issubset(columns):
            return []
    return list(alternatives[0])


def _return_col(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if re.fullmatch(r"future_\d+d_return", str(col)) or re.fullmatch(r"forward_return_\d+d", str(col)):
            return str(col)
    return None


def _with_groups(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "_phenomenon_group" in out.columns:
        return out
    required = {"pb", "operating_cashflow_to_profit"}
    if required.issubset(out.columns) and ("debt_to_asset_delta" in out.columns or "debt_to_asset" in out.columns):
        if "debt_to_asset_delta" not in out.columns:
            if {"ticker", "date"}.issubset(out.columns):
                out = out.sort_values(["ticker", "date"]).copy()
                out["debt_to_asset_delta"] = out.groupby("ticker")["debt_to_asset"].diff()
            else:
                out["debt_to_asset_delta"] = out["debt_to_asset"].diff()
        pb_low = out["pb"] <= out["pb"].median(skipna=True)
        debt_repair = out["debt_to_asset_delta"] <= out["debt_to_asset_delta"].median(skipna=True)
        cashflow_good = out["operating_cashflow_to_profit"] >= out["operating_cashflow_to_profit"].median(skipna=True)
        out["_phenomenon_group"] = "other"
        out.loc[pb_low & debt_repair & cashflow_good, "_phenomenon_group"] = "balance_sheet_repair_low_valuation"
        out.loc[pb_low & ~(debt_repair & cashflow_good), "_phenomenon_group"] = "low_valuation_no_repair"
        out.loc[~pb_low & (debt_repair & cashflow_good), "_phenomenon_group"] = "balance_sheet_repair_not_low_valuation"
        return out
    out["_phenomenon_group"] = "all"
    return out


def _spread_by_bucket(df: pd.DataFrame, bucket_col: str, return_col: str, target_group: str | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    grouped = df.dropna(subset=[bucket_col, return_col]).groupby(bucket_col)
    for bucket, part in grouped:
        if target_group and "_phenomenon_group" in part.columns:
            target = part[part["_phenomenon_group"] == target_group][return_col].dropna()
            control = part[part["_phenomenon_group"] != target_group][return_col].dropna()
            spread = None
            if not target.empty and not control.empty:
                spread = float(target.mean() - control.mean())
            out[str(bucket)] = {
                "row_count": int(part.shape[0]),
                "target_mean": float(target.mean()) if not target.empty else None,
                "control_mean": float(control.mean()) if not control.empty else None,
                "spread_vs_control": spread,
            }
        else:
            out[str(bucket)] = {"row_count": int(part.shape[0]), "mean_return": float(part[return_col].mean())}
    return out


def _bucket_for_check(check_name: str, df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    out = df.copy()
    if check_name == "industry_split_robustness":
        return out, "industry"
    if check_name == "size_split_robustness":
        if "size_bucket" in out.columns:
            return out, "size_bucket"
        size_col = "total_mv" if "total_mv" in out.columns else "market_cap"
        out["_size_bucket"] = pd.qcut(out[size_col].rank(method="first"), q=min(3, max(1, out[size_col].nunique())), duplicates="drop").astype(str)
        return out, "_size_bucket"
    if check_name == "regime_split_robustness":
        if "market_regime" in out.columns:
            return out, "market_regime"
        ticker_ret = out.groupby("ticker")["close"].pct_change() if "ticker" in out.columns else out["close"].pct_change()
        market_ret = ticker_ret.groupby(out["date"]).transform("mean") if "date" in out.columns else ticker_ret
        out["_market_regime"] = pd.Series(["risk_on" if x >= 0 else "risk_off" for x in market_ret.fillna(0)], index=out.index)
        return out, "_market_regime"
    if check_name == "turnover_sensitivity":
        turnover_col = "turnover_rate" if "turnover_rate" in out.columns else "turnover"
        out["_turnover_bucket"] = pd.qcut(out[turnover_col].rank(method="first"), q=min(3, max(1, out[turnover_col].nunique())), duplicates="drop").astype(str)
        return out, "_turnover_bucket"
    return out, "_phenomenon_group"


def _apply_risk_cost_constraints(df: pd.DataFrame, constraints: dict[str, Any]) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = df.copy()
    rows_before = int(out.shape[0])
    applied: list[str] = []
    skipped: dict[str, str] = {}

    if constraints.get("liquidity_turnover_filter"):
        turnover_col = "turnover_rate" if "turnover_rate" in out.columns else "turnover" if "turnover" in out.columns else None
        if turnover_col:
            threshold = out[turnover_col].quantile(0.67)
            out = out[out[turnover_col] <= threshold].copy()
            applied.append("liquidity_turnover_filter")
        else:
            skipped["liquidity_turnover_filter"] = "missing_turnover_column"

    if constraints.get("drawdown_guard"):
        return_col = _return_col(out)
        if return_col:
            # Remove only the extreme negative tail. This is a research
            # diagnostic guard, not a trading stop-loss rule.
            tail_threshold = min(-0.20, float(out[return_col].quantile(0.05)))
            out = out[(out[return_col].isna()) | (out[return_col] > tail_threshold)].copy()
            applied.append("drawdown_guard")
        else:
            skipped["drawdown_guard"] = "missing_forward_return_column"

    return out, {
        "constraints_requested": list(constraints.keys()),
        "constraints_applied": applied,
        "constraints_skipped": skipped,
        "rows_before_constraints": rows_before,
        "rows_after_constraints": int(out.shape[0]),
        "rows_removed": rows_before - int(out.shape[0]),
    }


def _execute_step(step: dict[str, Any], df: pd.DataFrame, target_group: str | None, *, constraint_adjusted: bool = False) -> dict[str, Any]:
    columns = set(map(str, df.columns))
    missing = _missing_columns_for(step.get("required_column_alternatives") or [], columns)
    return_col = _return_col(df)
    if return_col is None:
        missing = [*missing, "future_<horizon>d_return"]
    if missing:
        return {
            "step_id": step.get("step_id"),
            "check_name": step.get("check_name"),
            "status": "blocked_missing_columns",
            "missing_columns": sorted(set(missing)),
            "metrics": {},
        }
    work = _with_groups(df)
    check_name = str(step.get("check_name"))
    if check_name in {"industry_split_robustness", "size_split_robustness", "regime_split_robustness", "turnover_sensitivity"}:
        work, bucket_col = _bucket_for_check(check_name, work)
        metrics = {"bucket_column": bucket_col, "spread_by_bucket": _spread_by_bucket(work, bucket_col, return_col or "", target_group)}
    elif check_name == "drawdown_sensitivity":
        ret = work[return_col or ""].dropna()
        metrics = {
            "row_count": int(ret.shape[0]),
            "downside_frequency": float((ret < 0).mean()) if not ret.empty else None,
            "worst_forward_return": float(ret.min()) if not ret.empty else None,
        }
    elif check_name == "cost_sensitivity_probe":
        turnover_col = "turnover_rate" if "turnover_rate" in work.columns else "turnover"
        gross = work[return_col or ""].dropna()
        aligned = work.dropna(subset=[return_col or "", turnover_col])
        cost_proxy = aligned[turnover_col].abs() * 0.001
        net = aligned[return_col or ""] - cost_proxy
        metrics = {
            "gross_mean_return": float(gross.mean()) if not gross.empty else None,
            "cost_adjusted_mean_return": float(net.mean()) if not net.empty else None,
            "cost_proxy_bps_per_turnover_unit": 10,
            "constraint_adjusted": constraint_adjusted,
        }
    else:
        metrics = {"row_count": int(work.shape[0]), "constraint_adjusted": constraint_adjusted}
    return {
        "step_id": step.get("step_id"),
        "check_name": check_name,
        "status": "executed",
        "missing_columns": [],
        "metrics": metrics,
    }


def run_controlled_research_execution(*, run_id: str, execution_plan: dict[str, Any], feature_frame: pd.DataFrame) -> dict[str, Any]:
    validation = validate_controlled_execution_plan(execution_plan)
    if validation["decision"] != "keep":
        return {
            "schema_version": 1,
            "run_id": run_id,
            "mode": "controlled_research_execution_result",
            "source_execution_plan_run_id": execution_plan.get("run_id"),
            "phenomenon_id": execution_plan.get("phenomenon_id"),
            "result_status": "rejected_invalid_execution_plan",
            "plan_validation": validation,
            "summary": {"executed": 0, "blocked": 0, "failed": 1},
            "check_results": [],
            "production_execution_allowed": False,
            "live_trading_allowed": False,
            "queue_write_allowed": False,
        }
    constraints = execution_plan.get("risk_cost_constraints") or {}
    constrained_frame, constraint_application = _apply_risk_cost_constraints(feature_frame, constraints)
    constraint_adjusted = bool(constraint_application.get("constraints_applied"))
    check_results = [
        _execute_step(step, constrained_frame, execution_plan.get("target_group"), constraint_adjusted=constraint_adjusted)
        for step in execution_plan.get("execution_steps") or []
    ]
    counts = Counter("blocked" if item.get("status", "").startswith("blocked") else item.get("status") for item in check_results)
    result_status = "executed_with_blockers" if counts.get("blocked") else "executed"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "controlled_research_execution_result",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_execution_plan_run_id": execution_plan.get("run_id"),
        "phenomenon_id": execution_plan.get("phenomenon_id"),
        "result_status": result_status,
        "summary": {
            "executed": int(counts.get("executed", 0)),
            "blocked": int(counts.get("blocked", 0)),
            "total": len(check_results),
        },
        "constraint_application": constraint_application,
        "check_results": check_results,
        "next_allowed_action": "diagnose_or_mutate_after_review" if counts.get("executed") else "resolve_data_blockers",
        "production_execution_allowed": False,
        "live_trading_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "daemon_restore_allowed": False,
        "auto_promotion_allowed": False,
    }


def controlled_execution_result_to_markdown(result: dict[str, Any]) -> str:
    lines = [
        "# Controlled Research Execution Result",
        "",
        f"run_id: {result.get('run_id')}",
        f"phenomenon_id: {result.get('phenomenon_id')}",
        f"result_status: {result.get('result_status')}",
        f"production_execution_allowed: {result.get('production_execution_allowed')}",
        f"queue_write_allowed: {result.get('queue_write_allowed')}",
        "",
        "## Summary",
    ]
    for key, value in (result.get("summary") or {}).items():
        lines.append(f"- {key}: {value}")
    application = result.get("constraint_application") or {}
    if application:
        lines.extend(["", "## Constraint application"])
        for key in ["constraints_requested", "constraints_applied", "rows_before_constraints", "rows_after_constraints", "rows_removed"]:
            lines.append(f"- {key}: {application.get(key)}")
    lines.append("")
    lines.append("## Check results")
    for item in result.get("check_results") or []:
        lines.append(f"- {item.get('check_name')}: {item.get('status')}")
        if item.get("missing_columns"):
            lines.append(f"  - missing_columns: {', '.join(item.get('missing_columns') or [])}")
    return "\n".join(lines).rstrip() + "\n"


def write_controlled_execution_artifacts(plan: dict[str, Any], result: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "plan_json": out / "controlled_research_execution_plan.json",
        "result_json": out / "controlled_research_execution_result.json",
        "result_markdown": out / "controlled_research_execution_result.md",
    }
    paths["plan_json"].write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["result_json"].write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["result_markdown"].write_text(controlled_execution_result_to_markdown(result), encoding="utf-8")
    return paths
