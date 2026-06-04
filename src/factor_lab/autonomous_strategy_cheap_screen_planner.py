from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def build_cheap_screen_plan(*, run_id: str, route_registry: dict[str, Any]) -> dict[str, Any]:
    tasks = []
    for route in route_registry.get("routes") or []:
        if route.get("route_status") != "cheap_screen_candidate":
            continue
        tasks.append({
            "route_id": route.get("route_id"),
            "mechanism_id": route.get("mechanism_id") or route.get("route_id"),
            "required_fields": list(route.get("required_fields") or []),
            "cheap_screens": list(route.get("cheap_screens") or []),
            "falsification_criteria": list(route.get("falsification_criteria") or []),
            "execution_status": "not_executed",
            "requires_manual_review": True,
            "queue_write_allowed": False,
            "controlled_execution_allowed": False,
        })
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "preview_only",
        "cheap_screen_tasks": tasks,
        "task_count": len(tasks),
        "max_backtests_before_review": 0,
        "queue_write_allowed": False,
        "controlled_execution_allowed": False,
        "automation_allowed": False,
        "blocked_actions": [
            "full_backtest",
            "queue_write",
            "timer_enable",
            "broad_daemon_restore",
            "auto_promotion",
        ],
    }


def cheap_screen_plan_to_markdown(plan: dict[str, Any]) -> str:
    lines = [
        "# Autonomous Strategy Cheap Screen Plan",
        "",
        f"run_id: {plan.get('run_id')}",
        f"mode: {plan.get('mode')}",
        f"task_count: {plan.get('task_count')}",
        f"controlled_execution_allowed: {plan.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {plan.get('queue_write_allowed')}",
        "",
        "## Cheap screen tasks",
    ]
    for task in plan.get("cheap_screen_tasks") or []:
        lines.extend([
            f"### {task.get('route_id')}",
            f"- execution_status: {task.get('execution_status')}",
            f"- requires_manual_review: {task.get('requires_manual_review')}",
            "- cheap_screens:",
        ])
        lines.extend(f"  - {screen}" for screen in task.get("cheap_screens") or [])
        lines.extend(["- falsification_criteria:"])
        lines.extend(f"  - {criterion}" for criterion in task.get("falsification_criteria") or [])
        lines.append("")
    if not plan.get("cheap_screen_tasks"):
        lines.append("- none")
    return "\n".join(lines).rstrip() + "\n"


def write_cheap_screen_plan(plan: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "cheap_screen_plan.json"
    md_path = out / "cheap_screen_plan.md"
    json_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(cheap_screen_plan_to_markdown(plan), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}
