from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def derive_research_flow_state(
    *,
    snapshot: dict[str, Any],
    candidate_pool: dict[str, Any],
    recovery_used: bool,
    injected_count: int | None = None,
) -> dict[str, Any]:
    memory = _read_json(ARTIFACTS / "research_memory.json", {})
    recovery_history = list(memory.get("fallback_history") or [])
    branch_lifecycle = memory.get("branch_lifecycle") or {}

    main_candidates = len(candidate_pool.get("tasks") or [])
    recent_recovery_events = recovery_history[-5:]
    recovery_active = [
        branch_id for branch_id, state in branch_lifecycle.items()
        if str(branch_id).startswith("fallback_") and state.get("state") in {"validating", "exploring", "stable_candidate"}
    ]
    recent_recovery_success = any(row.get("has_gain") for row in recent_recovery_events)
    queue_counts = snapshot.get("queue_counts") or {}
    queue_finished = int(queue_counts.get("finished") or 0)
    queue_failed = int(queue_counts.get("failed") or 0)
    queue_pending = int(queue_counts.get("pending") or 0)
    queue_running = int(queue_counts.get("running") or 0)
    recovery_soft_landing = bool(
        recovery_active
        and recent_recovery_success
        and main_candidates > 0
        and queue_failed <= 0
        and (queue_finished > 0 or queue_pending > 0 or queue_running > 0)
    )

    state = "ready"
    reasons: list[str] = []
    if main_candidates <= 0 and not recovery_used:
        state = "exhausted"
        reasons.append("research_candidates_empty")
    elif recovery_used and injected_count and injected_count > 0:
        state = "recovered"
        reasons.append("recovery_injected_new_tasks")
    elif recovery_used:
        state = "recovering"
        reasons.append("recovery_step_triggered")
    elif recovery_soft_landing:
        state = "recovered"
        reasons.append("recovery_branches_stable_under_load")
    elif recovery_active:
        state = "recovering"
        reasons.append("recovery_branch_still_active")
    elif recent_recovery_success:
        state = "recovered"
        reasons.append("recent_recovery_produced_gain")

    if main_candidates > 0 and state in {"ready", "recovered"}:
        reasons.append("research_candidates_available")

    payload = {
        "state": state,
        "reasons": reasons,
        "candidate_count": main_candidates,
        "recovery_used": recovery_used,
        "recovery_active_branch_count": len(recovery_active),
        "recent_recovery_event_count": len(recent_recovery_events),
        "recent_recovery_success": recent_recovery_success,
        "injected_count": injected_count or 0,
        "queue_counts": queue_counts,
    }
    return payload
