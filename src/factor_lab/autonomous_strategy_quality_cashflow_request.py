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

BASE_FIELDS = ["date", "ticker", "industry", "pb", "pe_ttm", "forward_return_5d"]
QUALITY_CASHFLOW_FIELDS = ["ocfps", "roe", "gross_margin", "net_profit_yoy", "operating_cashflow_yoy"]
BALANCE_SHEET_FIELDS = ["debt_to_assets", "current_ratio", "quick_ratio", "interest_coverage"]
OPTIONAL_RISK_FIELDS = ["turnover_rate", "volume_ratio", "market_cap"]


def build_quality_cashflow_value_repair_request(*, run_id: str, route_closure: dict[str, Any]) -> dict[str, Any]:
    closure_is_terminal = (
        route_closure.get("route_id") == "industry_cycle_inflection_value_anchor_v1"
        and route_closure.get("route_status") == "stopped"
        and route_closure.get("stop_reason") == "industry_cycle_cheap_screen_risk_failed"
        and route_closure.get("controlled_execution_allowed") is False
        and route_closure.get("queue_write_allowed") is False
    )
    if closure_is_terminal:
        decision = "request_new_mechanism"
        next_allowed_actions = ["field_resolution", "pit_safety_preflight", "coverage_preflight", "cheap_screen_plan"]
        prerequisite_status = "ready_for_field_resolution"
        reason_codes = [
            "prior_route_closed_after_drawdown_failure",
            "cashflow_quality_balance_sheet_filter_required",
            "no_execution_until_preflight_and_cheap_screen_pass",
        ]
    else:
        decision = "blocked"
        next_allowed_actions = ["inspect_route_closure"]
        prerequisite_status = "blocked_until_terminal_route_closure"
        reason_codes = ["industry_cycle_route_closure_missing_or_not_terminal"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "quality_cashflow_value_repair_request",
        "mechanism_id": "quality_cashflow_value_repair_v1",
        "parent_route_id": route_closure.get("route_id"),
        "decision": decision,
        "prerequisite_status": prerequisite_status,
        "reason_codes": reason_codes,
        "economic_hypothesis": (
            "Cheap stocks are more likely to represent valuation dislocation rather than value traps when cashflow resilience, "
            "quality, balance-sheet stress control, and earnings non-deterioration confirm the valuation signal."
        ),
        "failed_route_lesson": route_closure.get("mechanism_lesson"),
        "do_not_repeat": [
            "do not use valuation cheapness alone",
            "do not treat industry-cycle momentum as a sufficient drawdown repair",
            "do not run controlled backtests before PIT-safe field resolution and coverage preflight",
            "do not write research queue or restore daemon from a request artifact",
        ],
        "required_fields": BASE_FIELDS + QUALITY_CASHFLOW_FIELDS + BALANCE_SHEET_FIELDS,
        "field_groups": {
            "base": BASE_FIELDS,
            "quality_cashflow": QUALITY_CASHFLOW_FIELDS,
            "balance_sheet_stress": BALANCE_SHEET_FIELDS,
            "optional_risk_controls": OPTIONAL_RISK_FIELDS,
        },
        "required_screens_before_execution": [
            "field_resolution",
            "PIT_safety_alignment",
            "coverage_preflight",
            "cheap_screen_positive_spread",
            "drawdown_guard_max_drawdown_at_least_minus_0_35",
        ],
        "falsification_criteria": [
            "required quality/cashflow/balance-sheet fields are unavailable or not PIT safe",
            "coverage is insufficient by date/industry after cache scan",
            "quality-cashflow filter does not improve drawdown versus cheap-only baseline",
            "best cheap-screen candidate has max_drawdown below -0.35",
            "spread advantage disappears after industry-neutral grouping",
        ],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
        "recommended_next_step": "run_quality_cashflow_field_resolution" if closure_is_terminal else "inspect_route_closure",
    }


def quality_cashflow_request_to_markdown(request: dict[str, Any]) -> str:
    lines = [
        "# Quality Cashflow Value Repair Request",
        "",
        f"mechanism_id: {request.get('mechanism_id')}",
        f"decision: {request.get('decision')}",
        f"prerequisite_status: {request.get('prerequisite_status')}",
        f"recommended_next_step: {request.get('recommended_next_step')}",
        f"controlled_execution_allowed: {request.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {request.get('queue_write_allowed')}",
        "",
        "## Economic hypothesis",
        str(request.get("economic_hypothesis")),
        "",
        "## Required fields",
    ]
    lines.extend(f"- {field}" for field in request.get("required_fields") or [])
    lines += ["", "## Required screens before execution"]
    lines.extend(f"- {screen}" for screen in request.get("required_screens_before_execution") or [])
    lines += ["", "## Falsification criteria"]
    lines.extend(f"- {criterion}" for criterion in request.get("falsification_criteria") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in request.get("blocked_actions") or [])
    lines += ["", "## Next allowed actions"]
    lines.extend(f"- {action}" for action in request.get("next_allowed_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_quality_cashflow_value_repair_request(request: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_cashflow_value_repair_request.json"
    markdown_path = out / "quality_cashflow_value_repair_request.md"
    json_path.write_text(json.dumps(request, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(quality_cashflow_request_to_markdown(request), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
