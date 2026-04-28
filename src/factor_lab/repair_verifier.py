from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.repair_runtime import load_runtime_incident_state, write_repair_summary_artifacts, write_runtime_incident_state
from factor_lab.storage import ExperimentStore


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
DAEMON_STATUS_PATH = ARTIFACTS / "research_daemon_status.json"
REPAIR_RUNTIME_SNAPSHOT_PATH = ARTIFACTS / "repair_runtime_snapshot.json"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _queue_has_work(store: ExperimentStore) -> bool:
    tasks = store.list_research_tasks_by_status(("pending", "running"), limit=50)
    return bool(tasks)


def verify_repair_actions(
    response: dict[str, Any],
    execution: dict[str, Any],
    *,
    store: ExperimentStore,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    verified = 0
    failed = 0
    manual = 0

    for row in execution.get("actions") or []:
        action_type = row.get("action_type")
        action_status = row.get("status")
        if action_type == "clean_stale_task":
            targets = list(row.get("repaired_task_ids") or [])
            ok = bool(targets) and all((store.get_research_task(task_id) or {}).get("status") != "running" for task_id in targets)
            result = "verified" if ok else ("no_effect" if action_status == "failed_no_effect" else "failed")
            checks.append({"action_type": action_type, "ok": ok, "result": result, "targets": targets})
        elif action_type == "recover_outputs_and_finalize":
            targets = list(row.get("finalized_task_ids") or [])
            ok = bool(targets) and all((store.get_research_task(task_id) or {}).get("status") == "finished" for task_id in targets)
            result = "verified" if ok else ("no_effect" if action_status == "failed_no_effect" else "failed")
            checks.append({"action_type": action_type, "ok": ok, "result": result, "targets": targets})
        elif action_type == "reseed_queue":
            seeded = list(row.get("seeded_task_ids") or [])
            ok = bool(seeded) and _queue_has_work(store)
            result = "verified" if ok else ("no_effect" if action_status == "failed_no_effect" else "failed")
            checks.append({"action_type": action_type, "ok": ok, "result": result, "seeded_task_ids": seeded})
        elif action_type in {"restart_daemon", "restart_daemon_if_stale"}:
            daemon_status = _read_json(DAEMON_STATUS_PATH, {})
            ok = daemon_status.get("state") == "running"
            result = "verified" if ok else ("failed" if action_status == "failed" else "no_effect")
            checks.append({"action_type": action_type, "ok": ok, "result": result, "daemon_state": daemon_status.get("state")})
        elif action_type == "refresh_runtime_snapshots":
            snapshot = _read_json(REPAIR_RUNTIME_SNAPSHOT_PATH, {})
            ok = bool(snapshot.get("generated_at_utc"))
            result = "verified" if ok else "failed"
            checks.append({"action_type": action_type, "ok": ok, "result": result})
        elif action_type == "mark_incident_only":
            checks.append({"action_type": action_type, "ok": True, "result": "observation_only"})
        else:
            result = "needs_manual" if action_status == "skipped_requires_manual" else "unsupported"
            checks.append({"action_type": action_type, "ok": result in {"needs_manual", "observation_only"}, "result": result})

    for row in checks:
        result = row.get("result")
        if result == "verified":
            verified += 1
        elif result == "needs_manual":
            manual += 1
        elif result not in {"observation_only"}:
            failed += 1

    if execution.get("status") == "suppressed":
        overall_status = "suppressed"
    elif verified > 0 and failed == 0 and manual == 0:
        overall_status = "verified"
    elif verified > 0 and failed > 0:
        overall_status = "partial"
    elif manual > 0 and verified == 0 and failed == 0:
        overall_status = "needs_manual"
    elif failed > 0:
        overall_status = "repair_failed"
    else:
        overall_status = "observation_only"

    verification = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "incident_type": response.get("incident_type"),
        "overall_status": overall_status,
        "verified_checks": verified,
        "failed_checks": failed,
        "manual_checks": manual,
        "total_checks": len(checks),
        "all_passed": overall_status == "verified",
        "checks": checks,
    }

    incident_signature = execution.get("incident_signature")
    if incident_signature:
        state = load_runtime_incident_state()
        incidents = state.setdefault("incidents", {})
        row = incidents.get(incident_signature)
        if row:
            row["status"] = overall_status if overall_status != "observation_only" else row.get("status")
            row["last_verified_at_utc"] = verification["verified_at_utc"]
            row["verification"] = verification
            incidents[incident_signature] = row
            write_runtime_incident_state(state)

    write_repair_summary_artifacts()
    return verification
