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

PROXY_REQUIRED_FIELDS = [
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
]


def build_quality_profit_proxy_revision(*, run_id: str, field_resolution: dict[str, Any]) -> dict[str, Any]:
    blocked_request_data = (
        field_resolution.get("mechanism_id") == "quality_cashflow_value_repair_v1"
        and field_resolution.get("decision") == "request_data"
        and field_resolution.get("ready_for_cheap_screen") is False
    )
    missing = set(field_resolution.get("missing_fields") or [])
    proxy_blocked = set(field_resolution.get("proxy_blocked_fields") or [])
    expected_blockers = {"gross_margin", "current_ratio", "quick_ratio", "interest_coverage"}
    expected_proxy_blockers = {"ocfps", "operating_cashflow_yoy"}
    if blocked_request_data and expected_blockers.issubset(missing) and expected_proxy_blockers.issubset(proxy_blocked):
        decision = "revise_to_proxy_mechanism"
        revision_status = "ready_for_proxy_field_resolution"
        recommended_next_step = "run_quality_profit_proxy_field_resolution"
        next_allowed_actions = ["proxy_field_resolution", "pit_safety_preflight", "coverage_preflight"]
        reason_codes = [
            "original_quality_cashflow_fields_blocked",
            "available_cache_supports_profit_quality_debt_cashflow_proxy",
            "proxy_requires_explicit_caveats_and_pit_validation",
        ]
    else:
        decision = "blocked"
        revision_status = "blocked_until_quality_cashflow_resolution_confirms_request_data"
        recommended_next_step = "inspect_quality_cashflow_field_resolution"
        next_allowed_actions = ["inspect_quality_cashflow_field_resolution"]
        reason_codes = ["quality_cashflow_field_resolution_not_in_expected_blocked_state"]

    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "quality_profit_proxy_revision",
        "source_mechanism_id": "quality_cashflow_value_repair_v1",
        "mechanism_id": "quality_profit_proxy_value_repair_v1",
        "decision": decision,
        "revision_status": revision_status,
        "recommended_next_step": recommended_next_step,
        "reason_codes": reason_codes,
        "proxy_hypothesis": (
            "When full cashflow and balance-sheet fields are blocked, an explicitly labeled proxy route can test whether "
            "cheap stocks with stronger ROE, positive profit growth, lower debt-to-asset pressure, and better operating-cashflow-to-profit quality show improved cheap-screen drawdown."
        ),
        "proxy_required_fields": PROXY_REQUIRED_FIELDS,
        "proxy_caveats": [
            "operating_cashflow_to_profit is not equivalent to ocfps or operating_cashflow_yoy",
            "profit_yoy is an alias/proxy for net_profit_yoy and still requires report-date alignment",
            "debt_to_asset is an alias for debt_to_assets and still requires report-date alignment",
            "proxy route cannot validate the original full quality_cashflow_value_repair_v1 hypothesis",
            "controlled execution remains blocked until proxy cheap screen and drawdown guard pass",
        ],
        "required_screens_before_execution": [
            "proxy_field_resolution",
            "PIT_safety_alignment",
            "coverage_preflight",
            "proxy_cheap_screen_positive_spread",
            "drawdown_guard_max_drawdown_at_least_minus_0_35",
        ],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": next_allowed_actions,
    }


def quality_profit_proxy_revision_to_markdown(revision: dict[str, Any]) -> str:
    lines = [
        "# Quality Profit Proxy Value Repair Revision",
        "",
        f"source_mechanism_id: {revision.get('source_mechanism_id')}",
        f"mechanism_id: {revision.get('mechanism_id')}",
        f"decision: {revision.get('decision')}",
        f"revision_status: {revision.get('revision_status')}",
        f"recommended_next_step: {revision.get('recommended_next_step')}",
        f"controlled_execution_allowed: {revision.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {revision.get('queue_write_allowed')}",
        "",
        "## Proxy hypothesis",
        str(revision.get("proxy_hypothesis")),
        "",
        "## Proxy required fields",
    ]
    lines.extend(f"- {field}" for field in revision.get("proxy_required_fields") or [])
    lines += ["", "## Proxy caveats"]
    lines.extend(f"- {caveat}" for caveat in revision.get("proxy_caveats") or [])
    lines += ["", "## Required screens before execution"]
    lines.extend(f"- {screen}" for screen in revision.get("required_screens_before_execution") or [])
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in revision.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_quality_profit_proxy_revision(revision: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "quality_profit_proxy_value_repair_revision.json"
    markdown_path = out / "quality_profit_proxy_value_repair_revision.md"
    json_path.write_text(json.dumps(revision, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(quality_profit_proxy_revision_to_markdown(revision), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
