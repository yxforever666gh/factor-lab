from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRODUCTION_GATE_KEYS = [
    "production_execution_allowed",
    "live_trading_allowed",
    "queue_write_allowed",
    "timer_enable_allowed",
    "daemon_restore_allowed",
    "auto_promotion_allowed",
]


def _closed_gates() -> dict[str, bool]:
    return {key: False for key in PRODUCTION_GATE_KEYS}


def _check_by_name(execution_result: dict[str, Any], name: str) -> dict[str, Any] | None:
    for item in execution_result.get("check_results") or []:
        if item.get("check_name") == name:
            return item
    return None


def _bucket_spreads(check: dict[str, Any] | None) -> list[float]:
    if not check:
        return []
    spread_by_bucket = (check.get("metrics") or {}).get("spread_by_bucket") or {}
    values: list[float] = []
    for item in spread_by_bucket.values():
        value = item.get("spread_vs_control") if isinstance(item, dict) else None
        if value is not None:
            values.append(float(value))
    return values


def _support_summary(execution_result: dict[str, Any]) -> dict[str, Any]:
    names = [
        "industry_split_robustness",
        "size_split_robustness",
        "regime_split_robustness",
        "turnover_sensitivity",
    ]
    checks: dict[str, Any] = {}
    for name in names:
        spreads = _bucket_spreads(_check_by_name(execution_result, name))
        positives = [v for v in spreads if v > 0]
        checks[name] = {
            "non_null_spread_count": len(spreads),
            "positive_spread_count": len(positives),
            "positive_rate": len(positives) / len(spreads) if spreads else None,
            "mean_spread": sum(spreads) / len(spreads) if spreads else None,
            "min_spread": min(spreads) if spreads else None,
            "max_spread": max(spreads) if spreads else None,
        }
    drawdown = _check_by_name(execution_result, "drawdown_sensitivity") or {}
    cost = _check_by_name(execution_result, "cost_sensitivity_probe") or {}
    return {
        "checks": checks,
        "drawdown": drawdown.get("metrics") or {},
        "cost": cost.get("metrics") or {},
    }


def _missing_columns(execution_result: dict[str, Any]) -> list[str]:
    cols: list[str] = []
    for item in execution_result.get("check_results") or []:
        if str(item.get("status", "")).startswith("blocked"):
            cols.extend(item.get("missing_columns") or [])
    return sorted(set(cols))


def _decide(execution_result: dict[str, Any], support: dict[str, Any]) -> tuple[str, list[str], str]:
    summary = execution_result.get("summary") or {}
    if int(summary.get("blocked") or 0) > 0:
        return "request_more_data", ["execution_blocked_missing_data"], "Resolve missing data before mutation or further execution."
    cost = support.get("cost") or {}
    cost_adjusted = cost.get("cost_adjusted_mean_return")
    if cost_adjusted is not None and float(cost_adjusted) <= 0:
        return "mutate_risk_or_cost_model", ["cost_adjusted_return_non_positive"], "Cost-adjusted return is non-positive; mutate turnover/cost constraints before further research."
    drawdown = support.get("drawdown") or {}
    worst = drawdown.get("worst_forward_return")
    downside = drawdown.get("downside_frequency")
    if worst is not None and float(worst) < -0.20:
        return "mutate_risk_or_cost_model", ["drawdown_tail_risk_too_large"], "Worst forward return indicates tail risk may dominate the phenomenon."
    if downside is not None and float(downside) > 0.65:
        return "mutate_risk_or_cost_model", ["downside_frequency_too_high"], "Downside frequency is too high for clean continuation."
    checks = support.get("checks") or {}
    regime = checks.get("regime_split_robustness") or {}
    if regime.get("positive_rate") is not None and float(regime["positive_rate"]) < 1.0:
        return "add_regime_filter", ["regime_fragility_detected"], "Regime split is not uniformly positive; continue only through a stricter regime condition."
    weak_core = []
    for name in ["industry_split_robustness", "size_split_robustness", "turnover_sensitivity"]:
        item = checks.get(name) or {}
        rate = item.get("positive_rate")
        if rate is not None and float(rate) < 0.60:
            weak_core.append(name)
    if weak_core:
        return "mutate_conditions", ["weak_core_split_support", *[f"weak_{name}" for name in weak_core]], "One or more core splits are weak; mutate cohort conditions before further execution."
    return "continue_research", ["controlled_execution_supported", "production_gates_remain_closed"], "Controlled diagnostics support continued research, not production deployment."


def _mutation_request(*, run_id: str, execution_result: dict[str, Any], decision: str, reason_codes: list[str], support: dict[str, Any]) -> dict[str, Any]:
    missing = _missing_columns(execution_result)
    actions = {
        "continue_research": ["run_deeper_oos_split", "extend_holding_horizon_variation", "write_next_agent_iteration_plan"],
        "request_more_data": ["resolve_missing_columns", "rerun_controlled_execution_after_data_fix"],
        "add_regime_filter": ["derive_or_select_supported_regime", "rerun_regime_filtered_controlled_execution"],
        "mutate_risk_or_cost_model": ["tighten_turnover_or_liquidity_filter", "add_drawdown_guard", "rerun_cost_sensitivity"],
        "mutate_conditions": ["tighten_balance_sheet_repair_condition", "revise_target_control_matching", "rerun_split_diagnostics"],
        "reject_phenomenon": ["write_rejection_lesson", "request_new_mechanism"],
    }
    return {
        "schema_version": 1,
        "run_id": run_id + "_mutation_request",
        "mode": "next_mutation_request",
        "phenomenon_id": execution_result.get("phenomenon_id"),
        "source_execution_result_run_id": execution_result.get("run_id"),
        "action": decision,
        "reason_codes": reason_codes,
        "requested_actions": actions.get(decision, []),
        "missing_columns": missing,
        "support_summary": support,
        **_closed_gates(),
    }


def build_controlled_research_verdict(*, run_id: str, execution_result: dict[str, Any]) -> dict[str, Any]:
    support = _support_summary(execution_result)
    decision, reason_codes, explanation = _decide(execution_result, support)
    verdict = {
        "schema_version": 1,
        "phenomenon_id": execution_result.get("phenomenon_id"),
        "source_execution_result_run_id": execution_result.get("run_id"),
        "decision": decision,
        "reason_codes": reason_codes,
        "explanation": explanation,
        "support_summary": support,
        "strategy_generation_allowed": False,
        "human_approval_required_for_strategy_phase": True,
        **_closed_gates(),
    }
    mutation = _mutation_request(run_id=run_id, execution_result=execution_result, decision=decision, reason_codes=reason_codes, support=support)
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "controlled_research_verdict",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_execution_result_run_id": execution_result.get("run_id"),
        "verdict": verdict,
        "next_mutation_request": mutation,
        **_closed_gates(),
    }


def validate_controlled_research_verdict(report: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    verdict = report.get("verdict") or {}
    for key in PRODUCTION_GATE_KEYS:
        if report.get(key) is not False:
            reason_codes.append(f"report_gate_not_closed_{key}")
        if verdict.get(key) is not False:
            reason_codes.append(f"production_gate_not_closed_{key}")
    if verdict.get("strategy_generation_allowed") is not False:
        reason_codes.append("strategy_generation_not_closed")
    if verdict.get("decision") not in {"continue_research", "request_more_data", "add_regime_filter", "mutate_risk_or_cost_model", "mutate_conditions", "reject_phenomenon"}:
        reason_codes.append("unsupported_decision")
    mutation = report.get("next_mutation_request") or {}
    if mutation.get("action") != verdict.get("decision"):
        reason_codes.append("mutation_action_mismatch")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def controlled_research_verdict_to_markdown(report: dict[str, Any]) -> str:
    verdict = report.get("verdict") or {}
    mutation = report.get("next_mutation_request") or {}
    lines = [
        "# Controlled Research Verdict",
        "",
        f"run_id: {report.get('run_id')}",
        f"phenomenon_id: {verdict.get('phenomenon_id')}",
        f"decision: {verdict.get('decision')}",
        f"strategy_generation_allowed: {verdict.get('strategy_generation_allowed')}",
        f"queue_write_allowed: {verdict.get('queue_write_allowed')}",
        "",
        "## Reason codes",
    ]
    lines.extend(f"- {code}" for code in verdict.get("reason_codes") or [])
    lines.extend(["", "## Explanation", str(verdict.get("explanation")), "", "## Next mutation request", f"action: {mutation.get('action')}"])
    for action in mutation.get("requested_actions") or []:
        lines.append(f"- {action}")
    checks = ((verdict.get("support_summary") or {}).get("checks") or {})
    if checks:
        lines.extend(["", "## Split support"])
        for name, item in checks.items():
            lines.append(f"- {name}: positive_rate={item.get('positive_rate')} mean_spread={item.get('mean_spread')}")
    return "\n".join(lines).rstrip() + "\n"


def write_controlled_research_verdict_artifacts(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = {
        "verdict_json": out / "controlled_research_verdict.json",
        "verdict_markdown": out / "controlled_research_verdict.md",
        "mutation_request_json": out / "next_mutation_request.json",
    }
    paths["verdict_json"].write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    paths["verdict_markdown"].write_text(controlled_research_verdict_to_markdown(report), encoding="utf-8")
    paths["mutation_request_json"].write_text(json.dumps(report.get("next_mutation_request") or {}, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return paths
