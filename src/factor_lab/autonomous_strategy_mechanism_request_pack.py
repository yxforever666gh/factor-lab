from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_mechanism_researcher_request_pack(*, run_id: str, new_mechanism_request: dict[str, Any]) -> dict[str, Any]:
    prompt_constraints = [
        "Return only mechanisms that are distinct from static valuation cheapness.",
        "Every candidate must include required PIT-safe fields or an explicit request_data path.",
        "Every candidate must define a cheap-screen falsification test before any controlled backtest.",
        "Do not reuse stopped routes unless the new mechanism has a new catalyst or new data.",
    ]
    worker_tasks = [
        {
            "worker_key": "factor_lab_mechanism_researcher",
            "task": "propose_new_mechanism_routes",
            "candidate_families": new_mechanism_request.get("candidate_next_mechanism_families") or [],
            "external_data_requests": new_mechanism_request.get("external_data_requests") or [],
            "do_not_repeat": new_mechanism_request.get("do_not_repeat") or [],
            "prompt_constraints": prompt_constraints,
            "max_candidate_routes": 3,
        }
    ]
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "mechanism_researcher_request_pack",
        "decision": "send_to_mechanism_researcher",
        "source_decision": new_mechanism_request.get("decision"),
        "failed_route_evidence": new_mechanism_request.get("failed_route_evidence") or {},
        "required_new_mechanism_properties": new_mechanism_request.get("required_new_mechanism_properties") or [],
        "external_data_requests": new_mechanism_request.get("external_data_requests") or [],
        "candidate_next_mechanism_families": new_mechanism_request.get("candidate_next_mechanism_families") or [],
        "do_not_repeat": new_mechanism_request.get("do_not_repeat") or [],
        "worker_tasks": worker_tasks,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": ["controlled_backtest", "queue_write", "timer_enable", "broad_daemon_restore", "auto_promotion", "live_trading"],
        "next_allowed_actions": ["run_mechanism_researcher_preview_worker", "manual_review_request_pack"],
    }


def mechanism_researcher_request_pack_to_markdown(pack: dict[str, Any]) -> str:
    lines = [
        "# Mechanism Researcher Request Pack",
        "",
        f"decision: {pack.get('decision')}",
        f"controlled_execution_allowed: {pack.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {pack.get('queue_write_allowed')}",
        "",
        "## Candidate families",
    ]
    lines.extend(f"- {item}" for item in pack.get("candidate_next_mechanism_families") or [])
    lines.append("")
    lines.append("## Do not repeat")
    lines.extend(f"- {item}" for item in pack.get("do_not_repeat") or [])
    lines.append("")
    lines.append("## Worker tasks")
    for task in pack.get("worker_tasks") or []:
        lines.append(f"- {task.get('worker_key')}: {task.get('task')} max={task.get('max_candidate_routes')}")
    return "\n".join(lines).rstrip() + "\n"


def write_mechanism_researcher_request_pack(pack: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir); out.mkdir(parents=True, exist_ok=True)
    jp = out / "mechanism_researcher_request.json"
    mp = out / "mechanism_researcher_request.md"
    jp.write_text(json.dumps(pack, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    mp.write_text(mechanism_researcher_request_pack_to_markdown(pack), encoding="utf-8")
    return {"json": jp, "markdown": mp}
