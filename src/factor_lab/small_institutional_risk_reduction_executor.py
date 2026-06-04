from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.simulated_portfolio_construction_repair import write_simulated_portfolio_construction_repair
from factor_lab.small_institutional_backtest_matrix import (
    DEFAULT_DATASET_PATH,
    run_long_only_backtest,
    load_dataset,
)
from factor_lab.small_institutional_risk_reduction_plan import DEFAULT_POLICY_PATH

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PLAN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_plan.json"
DEFAULT_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_results.json"
DEFAULT_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_results.md"
DEFAULT_REPAIR_JSON_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_repair.json"
DEFAULT_REPAIR_MARKDOWN_PATH = ROOT / "artifacts" / "small_institutional_simulation" / "risk_reduction_repair.md"
CONTROLLED_EXECUTOR_MAX_CANDIDATES = 20

_FORBIDDEN_ACTIONS = [
    "no_broad_daemon_restore",
    "no_queue_write",
    "no_timer_enable",
    "no_auto_promotion",
    "no_live_trading",
    "no_drawdown_limit_relaxation",
]


class RiskFilterError(ValueError):
    def __init__(self, reason: str, missing_columns: list[str] | None = None):
        super().__init__(reason)
        self.reason = reason
        self.missing_columns = missing_columns or []


def _load_json(path: str | Path) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_path(value: str | Path | None, *, anchor: Path = ROOT) -> Path:
    if value is None or str(value) == "":
        return DEFAULT_DATASET_PATH
    p = Path(value)
    return p if p.is_absolute() else anchor / p


def _ensure_allowed_output_path(path: str | Path) -> Path:
    resolved = Path(path).resolve()
    forbidden_parts = {
        "configs",
        "research_tasks",
        "cron",
        "systemd",
        ".config",
        ".hermes",
        "profiles",
        "plugins",
    }
    if forbidden_parts & set(resolved.parts):
        raise ValueError(f"forbidden_output_path: {resolved}")
    return resolved


def apply_candidate_risk_filters(dataset: pd.DataFrame, filters: list[dict[str, Any]] | None) -> pd.DataFrame:
    """Apply candidate risk filters using per-date quantile thresholds.

    Supported operators are ``lte_quantile`` and ``gte_quantile``. Numeric fields
    are coerced before thresholds are computed. Missing ``date`` or filter fields
    raise ``RiskFilterError`` so the executor can emit controlled
    insufficient-data rows instead of crashing.
    """
    if not filters:
        return dataset.copy()
    if "date" not in dataset.columns:
        raise RiskFilterError("missing_risk_filter_columns", ["date"])

    frame = dataset.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for risk_filter in filters:
        field = str(risk_filter.get("field") or "")
        operator = str(risk_filter.get("operator") or "")
        if not field or field not in frame.columns:
            raise RiskFilterError("missing_risk_filter_columns", [field] if field else [])
        if operator not in {"lte_quantile", "gte_quantile"}:
            raise RiskFilterError("unsupported_risk_filter_operator")
        try:
            quantile = float(risk_filter.get("quantile"))
        except (TypeError, ValueError):
            raise RiskFilterError("invalid_risk_filter_quantile") from None
        if quantile < 0.0 or quantile > 1.0:
            raise RiskFilterError("invalid_risk_filter_quantile")

        frame[field] = pd.to_numeric(frame[field], errors="coerce")
        thresholds = frame.groupby("date", dropna=False)[field].transform(lambda series: series.quantile(quantile))
        if operator == "lte_quantile":
            mask = frame[field].notna() & thresholds.notna() & (frame[field] <= thresholds)
        else:
            mask = frame[field].notna() & thresholds.notna() & (frame[field] >= thresholds)
        frame = frame[mask].copy()
    return frame


def _spec_projection(spec: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "candidate_id",
        "combo_id",
        "source",
        "signal_column",
        "label",
        "start_date",
        "end_date",
        "holding_count",
        "rebalance_frequency",
        "cost_bps",
        "return_column",
        "risk_filters",
    ]
    return {key: spec.get(key) for key in keys if key in spec}


def _row_from_backtest(spec: dict[str, Any], metrics: dict[str, Any], filtered_count: int) -> dict[str, Any]:
    projection = _spec_projection(spec)
    row = {
        **projection,
        "status": metrics.get("status"),
        "metrics": metrics,
        "filtered_row_count": int(filtered_count),
    }
    for key, value in metrics.items():
        row.setdefault(key, value)
    return row


def _insufficient_row(spec: dict[str, Any], *, reason: str, missing_columns: list[str] | None = None) -> dict[str, Any]:
    projection = _spec_projection(spec)
    metrics: dict[str, Any] = {"status": "insufficient_data", "reason": reason, "rebalance_count": 0}
    if missing_columns:
        metrics["missing_columns"] = missing_columns
    return {
        **projection,
        "status": "insufficient_data",
        "reason": reason,
        "missing_columns": missing_columns or [],
        "metrics": metrics,
        "filtered_row_count": 0,
    }


def build_risk_reduction_executor_results(
    *,
    plan_path: str | Path = DEFAULT_PLAN_PATH,
    dataset_path: str | Path | None = None,
    max_candidates: int = 20,
) -> dict[str, Any]:
    plan = _load_json(plan_path)
    effective_dataset_path = _resolve_path(dataset_path or plan.get("source_dataset_path"))
    dataset = load_dataset(effective_dataset_path)
    specs = [spec for spec in plan.get("candidate_specs") or [] if isinstance(spec, dict)]
    cap = min(max(0, int(max_candidates)), CONTROLLED_EXECUTOR_MAX_CANDIDATES)
    runnable = specs[:cap]
    results: list[dict[str, Any]] = []

    for spec in runnable:
        filters = spec.get("risk_filters") or []
        try:
            filtered = apply_candidate_risk_filters(dataset, filters)
        except RiskFilterError as exc:
            results.append(_insufficient_row(spec, reason=exc.reason, missing_columns=exc.missing_columns))
            continue
        metrics = run_long_only_backtest(
            filtered,
            signal_column=str(spec.get("signal_column") or ""),
            start_date=str(spec.get("start_date") or ""),
            end_date=str(spec.get("end_date") or ""),
            holding_count=int(spec.get("holding_count") or 0),
            rebalance_frequency=str(spec.get("rebalance_frequency") or "monthly"),
            cost_bps=float(spec.get("cost_bps") or 0.0),
            return_column=str(spec.get("return_column") or "forward_return_5d"),
        )
        results.append(_row_from_backtest(spec, metrics, len(filtered)))

    ok_results = [row for row in results if row.get("status") == "ok"]
    insufficient = [row for row in results if row.get("status") != "ok"]
    best = max(
        ok_results,
        key=lambda row: (float(row.get("total_return") or 0.0), float(row.get("sharpe") or 0.0), float(row.get("max_drawdown") or -999.0)),
        default=None,
    )
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "executor_type": "small_institutional_risk_reduction_controlled_executor",
        "plan_path": str(plan_path),
        "dataset_path": str(effective_dataset_path),
        "automation_allowed": False,
        "manual_review_required": True,
        "queue_write_allowed": False,
        "live_trading_enabled": False,
        "forbidden_actions": list(plan.get("forbidden_actions") or _FORBIDDEN_ACTIONS),
        "drawdown_limit": plan.get("drawdown_limit"),
        "matrix_status": "ok" if ok_results and not insufficient else "partial" if ok_results else "insufficient_data",
        "return_column": plan.get("return_column") or (runnable[0].get("return_column") if runnable else None),
        "execution": {
            "planned_count": len(specs),
            "cap": cap,
            "capped": len(runnable) < len(specs),
            "executed_count": len(results),
            "result_count": len(results),
        },
        "summary": {
            "ok_count": len(ok_results),
            "insufficient_data_count": len(insufficient),
            "result_count": len(results),
        },
        "best_result": best,
        "results": results,
    }


def risk_reduction_results_to_markdown(payload: dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    execution = payload.get("execution") or {}
    best = payload.get("best_result") or {}
    lines = [
        "# Small Institutional Risk Reduction Controlled Executor",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"matrix_status: {payload.get('matrix_status')}",
        f"automation_allowed: {payload.get('automation_allowed')}",
        f"manual_review_required: {payload.get('manual_review_required')}",
        f"queue_write_allowed: {payload.get('queue_write_allowed')}",
        f"live_trading_enabled: {payload.get('live_trading_enabled')}",
        "",
        "## Execution",
        f"- planned_count: {execution.get('planned_count')}",
        f"- cap: {execution.get('cap')}",
        f"- capped: {execution.get('capped')}",
        f"- executed_count: {execution.get('executed_count')}",
        "",
        "## Summary",
        f"- ok_count: {summary.get('ok_count')}",
        f"- insufficient_data_count: {summary.get('insufficient_data_count')}",
        f"- result_count: {summary.get('result_count')}",
        "",
        "## Best result",
        f"- candidate_id: {best.get('candidate_id')}",
        f"- combo_id: {best.get('combo_id')}",
        f"- signal_column: {best.get('signal_column')}",
        f"- total_return: {best.get('total_return')}",
        f"- sharpe: {best.get('sharpe')}",
        f"- max_drawdown: {best.get('max_drawdown')}",
        "",
        "## Results",
    ]
    for row in (payload.get("results") or [])[:20]:
        lines.append(
            f"- {row.get('candidate_id')} combo={row.get('combo_id')} status={row.get('status')} "
            f"signal={row.get('signal_column')} holdings={row.get('holding_count')} cost={row.get('cost_bps')} "
            f"return={row.get('total_return')} sharpe={row.get('sharpe')} drawdown={row.get('max_drawdown')}"
        )
    if not payload.get("results"):
        lines.append("- none")
    return "\n".join(lines) + "\n"


def write_risk_reduction_executor_results(
    *,
    plan_path: str | Path = DEFAULT_PLAN_PATH,
    dataset_path: str | Path | None = None,
    max_candidates: int = 20,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    repair_json_path: str | Path = DEFAULT_REPAIR_JSON_PATH,
    repair_markdown_path: str | Path = DEFAULT_REPAIR_MARKDOWN_PATH,
    policy_path: str | Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    payload = build_risk_reduction_executor_results(plan_path=plan_path, dataset_path=dataset_path, max_candidates=max_candidates)
    json_out = _ensure_allowed_output_path(json_path)
    markdown_out = _ensure_allowed_output_path(markdown_path)
    repair_json_out = _ensure_allowed_output_path(repair_json_path)
    repair_markdown_out = _ensure_allowed_output_path(repair_markdown_path)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    markdown_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    markdown_out.write_text(risk_reduction_results_to_markdown(payload), encoding="utf-8")
    write_simulated_portfolio_construction_repair(
        matrix_path=json_out,
        policy_path=policy_path,
        json_path=repair_json_out,
        markdown_path=repair_markdown_out,
    )
    return payload
