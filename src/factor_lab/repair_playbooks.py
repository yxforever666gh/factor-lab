from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.heartbeat import append_heartbeat
from factor_lab.repair_runtime import (
    append_repair_incident,
    append_runtime_observation,
    build_repair_runtime_snapshot,
    load_runtime_incident_state,
    repair_running_task_state_file,
    task_outputs_look_complete,
    write_repair_summary_artifacts,
    write_runtime_incident_state,
)
from factor_lab.storage import ExperimentStore


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


AUTO_SAFE_ACTIONS = {
    "clean_stale_task",
    "recover_outputs_and_finalize",
    "reseed_queue",
    "refresh_runtime_snapshots",
    "mark_incident_only",
}


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(minimum, value)


def _incident_signature(response: dict[str, Any]) -> str:
    incident_type = str(response.get("incident_type") or "unknown")
    actions = response.get("recommended_actions") or []
    first_action = actions[0] if actions else {}
    action_type = str(first_action.get("action_type") or "none")
    target = str(first_action.get("target") or "none")
    causes = response.get("suspected_root_causes") or []
    first_cause = str((causes[0] or {}).get("cause") or "none") if causes else "none"
    return f"{incident_type}::{action_type}::{target}::{first_cause[:80]}"


def _is_observation_only(response: dict[str, Any]) -> bool:
    return str(response.get("incident_type") or "unknown") == "unknown" and str(response.get("repair_mode") or "observe") == "observe"


def repair_stale_running_tasks(store: ExperimentStore, task_ids: list[str]) -> list[str]:
    repaired: list[str] = []
    for task_id in task_ids:
        task = store.get_research_task(task_id)
        if not task or task.get("status") != "running":
            continue
        outputs_complete = task_outputs_look_complete(task)
        note_suffix = "｜auto_repaired_unfinalized_workflow_output" if outputs_complete else "｜auto_cleaned_stale_running"
        error_text = "stale_running_task_repaired_after_outputs_written" if outputs_complete else "stale_running_task_cleaned"
        worker_note = ((task.get("worker_note") or "") + note_suffix)
        store.finish_research_task(
            task_id,
            status="failed",
            last_error=error_text,
            worker_note=worker_note,
        )
        repair_running_task_state_file(task, status="failed", error=error_text)
        repaired.append(task_id)
    return repaired


def recover_outputs_and_finalize(store: ExperimentStore, task_ids: list[str]) -> list[str]:
    finalized: list[str] = []
    for task_id in task_ids:
        task = store.get_research_task(task_id)
        if not task:
            continue
        if not task_outputs_look_complete(task):
            continue
        worker_note = ((task.get("worker_note") or "") + "｜auto_finalize_recovered_outputs")
        store.finish_research_task(
            task_id,
            status="finished",
            last_error="recovered_outputs_finalize",
            worker_note=worker_note,
        )
        repair_running_task_state_file(task, status="finished", error=None)
        finalized.append(task_id)
    return finalized


def _execute_action(store: ExperimentStore, action: dict[str, Any]) -> dict[str, Any]:
    action_type = str(action.get("action_type") or "").strip()
    target = str(action.get("target") or "").strip()
    if action_type == "clean_stale_task":
        task_ids = [item.strip() for item in target.split(",") if item.strip()]
        repaired = repair_stale_running_tasks(store, task_ids)
        return {
            "action_type": action_type,
            "target": target,
            "status": "ok" if repaired else "failed_no_effect",
            "repaired_task_ids": repaired,
            "effect_count": len(repaired),
            "effect_summary": f"cleaned {len(repaired)} stale tasks",
        }
    if action_type == "recover_outputs_and_finalize":
        task_ids = [item.strip() for item in target.split(",") if item.strip()]
        finalized = recover_outputs_and_finalize(store, task_ids)
        return {
            "action_type": action_type,
            "target": target,
            "status": "ok" if finalized else "failed_no_effect",
            "finalized_task_ids": finalized,
            "effect_count": len(finalized),
            "effect_summary": f"finalized {len(finalized)} recovered output tasks",
        }
    if action_type == "reseed_queue":
        from factor_lab import research_queue  # local import to avoid cycle

        result = research_queue.enqueue_baseline_tasks_with_diagnostics(store)
        seeded = result["task_ids"]
        return {
            "action_type": action_type,
            "target": target,
            "status": "ok" if seeded else "failed_no_effect",
            "seeded_task_ids": seeded,
            "effect_count": len(seeded),
            "effect_summary": f"seeded {len(seeded)} baseline tasks",
            "reseed_diagnostics": result,
        }
    if action_type == "refresh_runtime_snapshots":
        path = ARTIFACTS / "repair_runtime_snapshot.json"
        build_repair_runtime_snapshot(store, output_path=path, stale_minutes=_env_int("RESEARCH_STALE_RUNNING_MINUTES", 10, minimum=1))
        return {
            "action_type": action_type,
            "target": target,
            "status": "ok",
            "written_path": str(path),
            "effect_count": 1,
            "effect_summary": "refreshed repair runtime snapshot",
        }
    if action_type in {"restart_daemon", "restart_daemon_if_stale"}:
        completed = subprocess.run(
            ["systemctl", "--user", "restart", "factor-lab-research-daemon.service"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        ok = completed.returncode == 0
        return {
            "action_type": action_type,
            "target": target,
            "status": "ok" if ok else "failed",
            "stdout": completed.stdout,
            "stderr": completed.stderr,
            "returncode": completed.returncode,
            "effect_count": 1 if ok else 0,
            "effect_summary": "daemon restarted" if ok else "daemon restart failed",
        }
    if action_type == "mark_incident_only":
        return {
            "action_type": action_type,
            "target": target,
            "status": "noop",
            "effect_count": 0,
            "effect_summary": "observation only",
        }
    return {
        "action_type": action_type,
        "target": target,
        "status": "unsupported",
        "effect_count": 0,
        "effect_summary": "unsupported action",
    }


def execute_repair_actions(
    response: dict[str, Any],
    *,
    store: ExperimentStore,
    auto_only: bool = True,
) -> dict[str, Any]:
    actions = list(response.get("recommended_actions") or [])
    executed: list[dict[str, Any]] = []
    for action in actions:
        action_type = str(action.get("action_type") or "")
        risk_level = str(action.get("risk_level") or "low")
        if auto_only and (action_type not in AUTO_SAFE_ACTIONS or risk_level != "low"):
            executed.append(
                {
                    "action_type": action_type,
                    "target": action.get("target"),
                    "status": "skipped_requires_manual",
                    "effect_count": 0,
                    "effect_summary": "requires manual approval",
                }
            )
            continue
        executed.append(_execute_action(store, action))

    incident_signature = _incident_signature(response)
    any_effect = any(int(row.get("effect_count") or 0) > 0 and row.get("status") == "ok" for row in executed)
    any_manual = any(row.get("status") == "skipped_requires_manual" for row in executed)
    now = datetime.now(timezone.utc)
    payload = {
        "incident_id": f"repair-{now.strftime('%Y%m%d%H%M%S')}",
        "incident_signature": incident_signature,
        "recorded_at_utc": _iso_now(),
        "incident_type": response.get("incident_type"),
        "severity": response.get("severity"),
        "repair_mode": response.get("repair_mode"),
        "target": ((actions[0] or {}).get("target") if actions else None),
        "actions": executed,
        "status": "repair_executed" if any_effect else ("needs_manual" if any_manual else "observed"),
    }

    if _is_observation_only(response):
        append_runtime_observation({**payload, "kind": "repair_observation", "suppressed": False})
        write_repair_summary_artifacts()
        return payload

    state = load_runtime_incident_state()
    incidents = state.setdefault("incidents", {})
    existing = incidents.get(incident_signature) or {}
    cooldown_seconds = _env_int("REPAIR_INCIDENT_COOLDOWN_SECONDS", 300, minimum=0)
    last_seen = existing.get("last_seen_at_utc")
    last_seen_dt = datetime.fromisoformat(last_seen) if last_seen else None
    within_cooldown = bool(last_seen_dt and cooldown_seconds > 0 and (now - last_seen_dt).total_seconds() < cooldown_seconds)

    if existing and within_cooldown:
        existing["last_seen_at_utc"] = _iso_now()
        existing["seen_count"] = int(existing.get("seen_count") or 1) + 1
        existing["latest_execution"] = payload
        if any_effect and existing.get("status") not in {"verified", "resolved"}:
            existing["status"] = "repair_executed"
        incidents[incident_signature] = existing
        write_runtime_incident_state(state)
        append_runtime_observation({**payload, "kind": "repair_observation", "suppressed": True})
        write_repair_summary_artifacts()
        return {**payload, "status": "suppressed"}

    incident_row = {
        "incident_id": payload["incident_id"],
        "incident_signature": incident_signature,
        "incident_type": response.get("incident_type"),
        "severity": response.get("severity"),
        "repair_mode": response.get("repair_mode"),
        "target": payload.get("target"),
        "first_seen_at_utc": existing.get("first_seen_at_utc") or _iso_now(),
        "last_seen_at_utc": _iso_now(),
        "seen_count": int(existing.get("seen_count") or 0) + 1,
        "status": payload["status"] if payload["status"] != "observed" else "active",
        "latest_execution": payload,
        "summary": response.get("summary_markdown"),
    }
    incidents[incident_signature] = incident_row
    write_runtime_incident_state(state)
    append_repair_incident(payload)

    if any_effect:
        append_heartbeat(
            "repair_agent",
            "repair",
            summary=f"repair actions executed incident={response.get('incident_type')} count={len([r for r in executed if r.get('status') == 'ok'])}",
        )

    write_repair_summary_artifacts()
    return payload
