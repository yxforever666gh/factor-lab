from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]

CANDIDATE_SCREENS = [
    {
        "screen_id": "cheap_baseline_pb_pe",
        "description": "Baseline cheap-vs-expensive spread using low pb and low pe_ttm buckets.",
        "filters": ["pb bottom quantile", "pe_ttm bottom quantile"],
    },
    {
        "screen_id": "cheap_plus_roe_top50",
        "description": "Cheap stocks with ROE above daily median.",
        "filters": ["cheap_baseline_pb_pe", "roe >= daily median"],
    },
    {
        "screen_id": "cheap_plus_profit_yoy_positive",
        "description": "Cheap stocks with positive PIT-safe profit_yoy.",
        "filters": ["cheap_baseline_pb_pe", "profit_yoy > 0"],
    },
    {
        "screen_id": "cheap_plus_debt_to_asset_bottom50",
        "description": "Cheap stocks with debt_to_asset below daily median.",
        "filters": ["cheap_baseline_pb_pe", "debt_to_asset <= daily median"],
    },
    {
        "screen_id": "cheap_plus_operating_cashflow_to_profit_positive",
        "description": "Cheap stocks with positive operating_cashflow_to_profit.",
        "filters": ["cheap_baseline_pb_pe", "operating_cashflow_to_profit > 0"],
    },
    {
        "screen_id": "combined_quality_profit_proxy",
        "description": "Cheap stocks passing ROE, profit growth, debt pressure, and operating cashflow quality proxy filters.",
        "filters": [
            "cheap_baseline_pb_pe",
            "roe >= daily median",
            "profit_yoy > 0",
            "debt_to_asset <= daily median",
            "operating_cashflow_to_profit > 0",
        ],
    },
]


def build_proxy_cheap_screen_plan(*, run_id: str, phase6_final_verdict: dict[str, Any], proxy_pit_alignment: dict[str, Any]) -> dict[str, Any]:
    phase6_complete = phase6_final_verdict.get("phase_status") == "completed"
    pit_ready = proxy_pit_alignment.get("decision") == "prepare_proxy_cheap_screen_plan"
    ready = phase6_complete and pit_ready
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "proxy_cheap_screen_plan",
        "phase": 7,
        "mechanism_id": "quality_profit_proxy_value_repair_v1",
        "decision": "prepare_proxy_cheap_screen_execution" if ready else "block_proxy_cheap_screen_plan",
        "recommended_next_step": "run_proxy_cheap_screen_execution" if ready else "inspect_phase6_or_pit_alignment_state",
        "phase6_complete": phase6_complete,
        "pit_alignment_ready": pit_ready,
        "required_fields": [
            "date",
            "ticker",
            "industry",
            "pb",
            "pe_ttm",
            "forward_return_5d",
            "roe",
            "profit_yoy",
            "debt_to_asset",
            "operating_cashflow_to_profit",
        ],
        "candidate_screens": CANDIDATE_SCREENS,
        "hard_gates": [
            "phase6_final_verdict.phase_status == completed",
            "proxy_pit_alignment.decision == prepare_proxy_cheap_screen_plan",
            "PIT overlay coverage >= 0.60",
            "PIT alignment usable coverage >= 0.60",
            "controlled_execution_allowed == false",
            "queue_write_allowed == false",
            "timer_enable_allowed == false",
        ],
        "execution_constraints": [
            "bounded cheap-screen diagnostics only",
            "no controlled backtest",
            "no queue write",
            "no timer enable",
            "no daemon restore",
            "no auto promotion",
            "no live trading",
        ],
        "risk_gate": {
            "max_drawdown_minimum": -0.35,
            "mean_daily_spread_required": "positive",
            "usable_rows_required": "non-trivial",
            "usable_tickers_required": "non-trivial",
        },
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": ["run_proxy_cheap_screen_execution"] if ready else ["inspect_phase6_or_pit_alignment_state"],
    }


def proxy_cheap_screen_plan_to_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# Proxy Cheap Screen Plan",
        "",
        f"mechanism_id: {plan.get('mechanism_id')}",
        f"decision: {plan.get('decision')}",
        f"recommended_next_step: {plan.get('recommended_next_step')}",
        f"controlled_execution_allowed: {plan.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {plan.get('queue_write_allowed')}",
        "",
        "## Required fields",
    ]
    lines.extend(f"- {field}" for field in plan.get("required_fields") or [])
    lines += ["", "## Candidate screens"]
    for screen in plan.get("candidate_screens") or []:
        lines.append(f"### {screen.get('screen_id')}")
        lines.append(screen.get("description") or "")
        for item in screen.get("filters") or []:
            lines.append(f"- {item}")
        lines.append("")
    lines += ["## Hard gates"]
    lines.extend(f"- {gate}" for gate in plan.get("hard_gates") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in plan.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_proxy_cheap_screen_plan(plan: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "proxy_cheap_screen_plan.json"
    markdown_path = out / "proxy_cheap_screen_plan.md"
    json_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(proxy_cheap_screen_plan_to_markdown(plan), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
