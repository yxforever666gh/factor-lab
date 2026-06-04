from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BLOCKED_ACTIONS = ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"]


def build_next_workstream_decision(*, run_id: str, proxy_report: dict[str, Any]) -> dict[str, Any]:
    alpha_failed = proxy_report.get("alpha_status") == "failed"
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "next_workstream_decision",
        "source_report": "artifacts/autonomous_strategy_lab/proxy_workstream_report.json",
        "decision": "request_new_mechanism" if alpha_failed else "manual_review_existing_route",
        "recommended_next_step": "write_new_mechanism_request_v2" if alpha_failed else "manual_review_existing_route",
        "reason_codes": ["proxy_workstream_alpha_failed", "do_not_repeat_quality_proxy_value_repair"] if alpha_failed else ["manual_review_candidate"],
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
    }


def write_next_workstream_decision(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "next_workstream_decision.json"
    markdown_path = out / "next_workstream_decision.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    lines = ["# Next Workstream Decision", "", f"decision: {report.get('decision')}", f"recommended_next_step: {report.get('recommended_next_step')}", f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}", f"queue_write_allowed: {report.get('queue_write_allowed')}", "", "## Reason codes"]
    lines.extend(f"- {code}" for code in report.get("reason_codes") or [])
    markdown_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
