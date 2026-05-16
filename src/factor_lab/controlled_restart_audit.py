from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from factor_lab.workflow_admission_adapter import enforce_workflow_admission


def _load_pending_tasks(db_path: str | Path, limit: int = 200) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT * FROM research_tasks WHERE status='pending' ORDER BY priority DESC, created_at_utc ASC LIMIT ?",
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    tasks = []
    for row in rows:
        d = dict(row)
        payload = d.get("payload") or d.get("payload_json") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        d["payload"] = payload if isinstance(payload, dict) else {}
        tasks.append(d)
    return tasks


def dry_run_controlled_restart(*, db_path: str | Path = "artifacts/factor_lab.db", max_new_workflows: int = 3) -> dict[str, Any]:
    tasks = _load_pending_tasks(db_path)
    would_run: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    allowed_non_workflow: list[dict[str, Any]] = []
    blocked_workflow_total = 0
    allowed_workflow_total = 0
    unsafe_reason_counts: dict[str, int] = {}
    for task in tasks:
        if task.get("task_type") != "workflow":
            allowed_non_workflow.append({"task_id": task.get("task_id"), "task_type": task.get("task_type")})
            continue
        decision = enforce_workflow_admission(task)
        row = {
            "task_id": task.get("task_id"),
            "task_type": task.get("task_type"),
            "worker_note": task.get("worker_note"),
            "decision": decision.get("decision"),
            "reasons": decision.get("reasons") or [],
            "route_id": ((decision.get("admission") or {}).get("route_id")),
            "mechanism_id": ((decision.get("admission") or {}).get("mechanism_id")),
        }
        if decision.get("decision") == "allow":
            allowed_workflow_total += 1
            if len(would_run) < max_new_workflows:
                would_run.append(row)
            else:
                row["reasons"] = ["controlled_restart_cap"]
                blocked.append(row)
                blocked_workflow_total += 1
                unsafe_reason_counts["controlled_restart_cap"] = unsafe_reason_counts.get("controlled_restart_cap", 0) + 1
        else:
            blocked.append(row)
            blocked_workflow_total += 1
            for reason in row.get("reasons") or ["unknown"]:
                unsafe_reason_counts[str(reason)] = unsafe_reason_counts.get(str(reason), 0) + 1
    return {
        "pending_count": len(tasks),
        "would_run_count": len(would_run),
        "blocked_count": len(blocked),
        "allowed_non_workflow_count": len(allowed_non_workflow),
        "claimable_workflow_count": len(would_run),
        "allowed_workflow_count": allowed_workflow_total,
        "blocked_workflow_count": blocked_workflow_total,
        "pending_non_workflow_count": len(allowed_non_workflow),
        "pending_diagnostic_count": sum(1 for row in allowed_non_workflow if row.get("task_type") == "diagnostic"),
        "unsafe_reason_counts": unsafe_reason_counts,
        "would_run": would_run,
        "blocked": blocked,
        "allowed_non_workflow": allowed_non_workflow,
    }


def write_controlled_restart_dry_run(*, db_path: str | Path = "artifacts/factor_lab.db", output_dir: str | Path = "artifacts") -> dict[str, Any]:
    result = dry_run_controlled_restart(db_path=db_path)
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "controlled_restart_dry_run.json"
    md_path = out / "controlled_restart_dry_run.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Controlled Restart Dry Run", "", f"Pending: {result['pending_count']}", f"Would run: {result['would_run_count']}", f"Blocked: {result['blocked_count']}", "", "## Would run"]
    for row in result["would_run"]:
        lines.append(f"- {row['task_id']} route={row.get('route_id')} mechanism={row.get('mechanism_id')}")
    lines.extend(["", "## Blocked"])
    for row in result["blocked"]:
        lines.append(f"- {row['task_id']} reasons={','.join(row.get('reasons') or [])}")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {**result, "json_path": str(json_path), "markdown_path": str(md_path)}
