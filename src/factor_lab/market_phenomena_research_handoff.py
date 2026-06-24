from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRODUCTION_BLOCKS = {
    "queue_write_allowed": False,
    "timer_enable_allowed": False,
    "daemon_restore_allowed": False,
    "auto_promotion_allowed": False,
    "live_trading_allowed": False,
}

RESEARCH_TASKS = [
    "industry_split_robustness",
    "size_split_robustness",
    "regime_split_robustness",
    "turnover_sensitivity",
    "drawdown_sensitivity",
    "holding_horizon_variation",
    "condition_threshold_variation",
    "factor_definition_mutation",
    "cost_sensitivity_probe",
]


def handoff_for_supported_verdict(verdict: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "phenomenon_id": verdict.get("phenomenon_id"),
        "title": verdict.get("title"),
        "source_verdict": verdict.get("verdict"),
        "handoff_status": "ready_for_controlled_research_backtest",
        "phenomenon_supported_for_further_research": True,
        "controlled_research_backtest_allowed": True,
        "strategy_generation_allowed": True,
        "production_execution_allowed": False,
        "spread_vs_control": verdict.get("spread_vs_control"),
        "target_group": verdict.get("target_group"),
        "usable_row_count": verdict.get("usable_row_count"),
        "usable_ticker_count": verdict.get("usable_ticker_count"),
        "next_research_question": verdict.get("next_research_question"),
        "research_tasks": RESEARCH_TASKS,
        "iteration_policy": {
            "if_drawdown_fails": ["add_regime_filter", "add_liquidity_filter", "mutate_holding_horizon", "tighten_balance_sheet_repair_condition"],
            "if_return_fails": ["mutate_factor_definition", "change_thresholds", "request_new_mechanism_variant"],
            "if_split_unstable": ["keep_only_supported_regime", "write_rejection_for_unstable_splits"],
        },
        **PRODUCTION_BLOCKS,
    }


def build_research_handoff(*, run_id: str, verdict_report: dict[str, Any]) -> dict[str, Any]:
    handoffs = []
    skipped = []
    for verdict in verdict_report.get("verdicts") or []:
        if verdict.get("verdict") == "supported_for_further_research":
            handoffs.append(handoff_for_supported_verdict(verdict))
        else:
            skipped.append({"phenomenon_id": verdict.get("phenomenon_id"), "verdict": verdict.get("verdict")})
    return {
        "schema_version": 1,
        "run_id": run_id,
        "mode": "controlled_research_backtest_handoff",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_verdict_run_id": verdict_report.get("run_id"),
        "summary": {
            "ready_for_controlled_research_backtest": len(handoffs),
            "skipped_not_supported": len(skipped),
        },
        "handoffs": handoffs,
        "skipped_verdicts": skipped,
        "controlled_research_backtest_allowed": bool(handoffs),
        "strategy_generation_allowed": bool(handoffs),
        "production_execution_allowed": False,
        **PRODUCTION_BLOCKS,
    }


def research_handoff_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Controlled Research / Backtest Handoff",
        "",
        f"run_id: {report.get('run_id')}",
        f"mode: {report.get('mode')}",
        "scope: autonomous research/backtest iteration; not live trading, not production queue, not auto-promotion",
        f"controlled_research_backtest_allowed: {report.get('controlled_research_backtest_allowed')}",
        f"strategy_generation_allowed: {report.get('strategy_generation_allowed')}",
        f"live_trading_allowed: {report.get('live_trading_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Summary",
    ]
    for key, value in (report.get("summary") or {}).items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    lines.append("## Handoffs")
    for item in report.get("handoffs") or []:
        lines.extend([
            "",
            f"### {item.get('phenomenon_id')}: {item.get('title')}",
            f"- handoff_status: {item.get('handoff_status')}",
            f"- spread_vs_control: {item.get('spread_vs_control')}",
            f"- target_group: {item.get('target_group')}",
            "- research_tasks:",
        ])
        lines.extend(f"  - {task}" for task in item.get("research_tasks") or [])
    if report.get("skipped_verdicts"):
        lines.append("")
        lines.append("## Skipped verdicts")
        for item in report.get("skipped_verdicts") or []:
            lines.append(f"- {item.get('phenomenon_id')}: {item.get('verdict')}")
    return "\n".join(lines).rstrip() + "\n"


def write_research_handoff(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "research_handoff.json"
    markdown_path = out / "research_handoff.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(research_handoff_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
