from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.market_phenomena_schema import SAFETY_FLAGS

FORBIDDEN_PLAN_FIELDS = [
    "buy_rule",
    "sell_rule",
    "position_size",
    "rebalance_rule",
    "portfolio_weight",
    "order_generation",
    "queue_write",
]


def _primary_horizon(item: dict[str, Any]) -> str:
    horizons = item.get("requested_target_horizons") or []
    return horizons[-1] if horizons else "60d"


def _target_variables(horizon: str) -> list[str]:
    return [f"future_{horizon}_return", f"future_{horizon}_downside_risk", f"future_{horizon}_max_drawdown"]


def _plan_template_for(item: dict[str, Any]) -> tuple[list[str], list[str], list[str]]:
    pid = item.get("phenomenon_id")
    variables = list(item.get("observable_variables") or [])
    if pid == "value_trap_escape_after_balance_sheet_repair_v1":
        return (
            [v for v in ["debt_to_asset_delta", "operating_cashflow_to_profit", "roe", "pb", "industry"] if v in variables],
            ["balance_sheet_repair_low_valuation", "low_valuation_no_repair", "balance_sheet_repair_not_low_valuation"],
            [
                "repair group return distribution does not exceed low-valuation no-repair control",
                "repair group downside risk remains worse than control",
                "effect disappears after industry split",
            ],
        )
    return (
        [v for v in ["profit_yoy", "roe", "debt_to_asset", "operating_cashflow_to_profit", "pb", "industry_return_60d"] if v in variables],
        ["quality_repair_low_valuation", "low_quality_low_valuation", "quality_repair_not_low_valuation"],
        [
            "quality repair group return distribution does not exceed controls",
            "quality repair group downside risk is not lower than controls",
            "effect is only an industry-regime artifact",
        ],
    )


def plan_experiment_for_ready_phenomenon(item: dict[str, Any]) -> dict[str, Any]:
    horizon = _primary_horizon(item)
    condition_variables, comparison_groups, falsification_criteria = _plan_template_for(item)
    return {
        "schema_version": 1,
        "phenomenon_id": item.get("phenomenon_id"),
        "title": item.get("title"),
        "experiment_type": "conditional_distribution_test",
        "condition_variables": condition_variables,
        "target_variables": _target_variables(horizon),
        "comparison_groups": comparison_groups,
        "success_criteria": {
            "target_group_return_above_control": True,
            "target_group_downside_risk_not_worse_than_control": True,
            "regime_stability_required": True,
            "minimum_usable_tickers": 50,
            "minimum_usable_rows": 250,
        },
        "falsification_criteria": falsification_criteria,
        "input_feasibility": {
            "row_count": item.get("row_count"),
            "ticker_count": item.get("ticker_count"),
            "field_coverage": item.get("field_coverage") or {},
        },
        "forbidden_outputs": FORBIDDEN_PLAN_FIELDS,
        **SAFETY_FLAGS,
    }


def validate_minimal_verification_plan(plan: dict[str, Any]) -> dict[str, Any]:
    reason_codes: list[str] = []
    for field in FORBIDDEN_PLAN_FIELDS:
        if field in plan:
            reason_codes.append(f"forbidden_strategy_field_{field}")
    if plan.get("experiment_type") not in {"conditional_distribution_test", "cross_sectional_group_comparison", "regime_split_distribution_test", "lead_lag_distribution_test"}:
        reason_codes.append("unsupported_experiment_type")
    if not plan.get("condition_variables"):
        reason_codes.append("missing_condition_variables")
    if not plan.get("target_variables"):
        reason_codes.append("missing_target_variables")
    if not plan.get("comparison_groups"):
        reason_codes.append("missing_comparison_groups")
    return {"decision": "keep" if not reason_codes else "reject", "reason_codes": reason_codes}


def build_minimal_verification_plan(*, run_id: str, data_feasibility_review: dict[str, Any]) -> dict[str, Any]:
    experiments = []
    skipped = []
    for item in data_feasibility_review.get("reviewed_phenomena") or []:
        if item.get("decision") != "ready_for_minimal_verification":
            skipped.append({"phenomenon_id": item.get("phenomenon_id"), "decision": item.get("decision")})
            continue
        plan = plan_experiment_for_ready_phenomenon(item)
        validation = validate_minimal_verification_plan(plan)
        plan["plan_validation"] = validation
        if validation["decision"] == "keep":
            experiments.append(plan)
        else:
            skipped.append({"phenomenon_id": item.get("phenomenon_id"), "decision": "invalid_plan", "reason_codes": validation["reason_codes"]})
    summary = Counter({"planned": len(experiments), "skipped_not_ready": len(skipped)})
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "minimal_verification_plan_artifact_only",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_data_feasibility_run_id": data_feasibility_review.get("run_id"),
        "experiments": experiments,
        "skipped_phenomena": skipped,
        "summary": dict(sorted(summary.items())),
        **SAFETY_FLAGS,
    }


def minimal_verification_plan_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Minimal Verification Plan",
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
    lines.append("## Experiments")
    for plan in report.get("experiments") or []:
        lines.extend([
            "",
            f"### {plan.get('phenomenon_id')}: {plan.get('title')}",
            f"- experiment_type: {plan.get('experiment_type')}",
            f"- condition_variables: {', '.join(plan.get('condition_variables') or [])}",
            f"- target_variables: {', '.join(plan.get('target_variables') or [])}",
            f"- comparison_groups: {', '.join(plan.get('comparison_groups') or [])}",
        ])
    if report.get("skipped_phenomena"):
        lines.append("")
        lines.append("## Skipped phenomena")
        for item in report.get("skipped_phenomena") or []:
            lines.append(f"- {item.get('phenomenon_id')}: {item.get('decision')}")
    return "\n".join(lines).rstrip() + "\n"


def write_minimal_verification_plan(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "minimal_verification_plan.json"
    markdown_path = out / "minimal_verification_plan.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(minimal_verification_plan_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
