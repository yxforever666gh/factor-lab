from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_runtime_state import recently_finished_same_fingerprint
from factor_lab.storage import ExperimentStore
from factor_lab.exploration_budget import exploration_floor_context


RECOVERY_REPEAT_COOLDOWN_MINUTES = 30


def _task_repeat_blocked(store: ExperimentStore, task: dict[str, Any], fingerprint: str | None) -> bool:
    if not fingerprint:
        return False
    payload = task.get("payload") or {}
    reasons = payload.get("reasons") or []
    # Recovery / fallback tasks should be allowed to re-run on a shorter cadence,
    # otherwise the planner can deadlock itself in an empty-queue recovery loop.
    cooldown = RECOVERY_REPEAT_COOLDOWN_MINUTES if "recovery_step" in reasons else None
    if cooldown is not None:
        return recently_finished_same_fingerprint(store, fingerprint, cooldown_minutes=cooldown)
    return recently_finished_same_fingerprint(
        store,
        fingerprint,
        task_type=("generated_batch" if task.get("category") == "exploration" else "diagnostic" if task.get("category") == "validation" else "workflow"),
        payload=payload,
        worker_note=task.get("worker_note"),
    )


CATEGORY_LIMITS_DEFAULT = {"baseline": 2, "validation": 3, "exploration": 1}
DB_PATH = Path("artifacts") / "factor_lab.db"


def _budget_bucket(task: dict[str, Any]) -> str:
    category = task.get("category", "validation")
    goal = str(task.get("goal") or (task.get("payload") or {}).get("goal") or "")
    branch_id = str(task.get("branch_id") or (task.get("payload") or {}).get("branch_id") or "")
    worker_note = str(task.get("worker_note") or "")
    text = " ".join([goal, branch_id, worker_note]).lower()
    if category == "validation" and ("fragile_candidate" in text or "fragile 候选" in text):
        return "validation_fragile"
    if category == "validation" and ("medium_horizon" in text or "中窗" in text):
        return "validation_medium_horizon"
    if category == "validation" and ("stable_candidate" in text or "稳定候选" in text):
        return "validation_stable"
    return category



def _representative_scope_key(task: dict[str, Any]) -> str | None:
    payload = task.get("payload") or {}
    focus_names = sorted(
        {
            name
            for name in (payload.get("focus_factors") or [])
            if name
        }
        | {
            row.get("candidate_name")
            for row in (task.get("focus_candidates") or [])
            if row.get("candidate_name")
        }
    )
    if not focus_names:
        return None
    return "|".join(focus_names)


def validate_research_planner_proposal(proposal_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    proposal = json.loads(Path(proposal_path).read_text(encoding="utf-8"))
    selected_tasks = proposal.get("selected_tasks", []) or []
    store = ExperimentStore(DB_PATH)

    selection_policy = proposal.get("selection_policy") or {}
    category_limits = selection_policy.get("category_limits") or CATEGORY_LIMITS_DEFAULT
    floor = selection_policy.get("exploration_floor") or exploration_floor_context({})
    bucket_limits = {**category_limits, "validation_stable": 2, "validation_medium_horizon": 2, "validation_fragile": 1}
    counts = {"baseline": 0, "validation": 0, "exploration": 0, "exploration_generated": 0, "validation_stable": 0, "validation_medium_horizon": 0, "validation_fragile": 0}
    accepted = []
    rejected = []
    selected_exploration = 0
    accepted_representative_scopes: set[str] = set()

    required_payload_fields = {"goal", "hypothesis", "expected_information_gain", "branch_id", "stop_if", "promote_if", "disconfirm_if"}

    for task in selected_tasks:
        category = task.get("category", "validation")
        bucket = _budget_bucket(task)
        fingerprint = task.get("fingerprint")
        reason = []
        ok = True
        payload = task.get("payload") or {}

        if category in counts and counts[category] >= category_limits.get(category, 99):
            ok = False
            reason.append(f"category_limit_exceeded:{category}")
        if bucket in counts and counts[bucket] >= bucket_limits.get(bucket, category_limits.get(category, 99)):
            ok = False
            reason.append(f"category_limit_exceeded:{bucket}")

        if _task_repeat_blocked(store, task, fingerprint):
            ok = False
            reason.append("recently_finished_same_fingerprint")

        if fingerprint and any(item.get("fingerprint") == fingerprint for item in accepted):
            ok = False
            reason.append("duplicate_within_plan")

        representative_scope = _representative_scope_key(task)
        if representative_scope and representative_scope in accepted_representative_scopes:
            ok = False
            reason.append("duplicate_representative_scope_within_plan")

        missing_fields = sorted(field for field in required_payload_fields if field not in payload)
        if missing_fields:
            ok = False
            reason.append(f"missing_hypothesis_fields:{','.join(missing_fields)}")

        if ok:
            accepted.append(task)
            if representative_scope:
                accepted_representative_scopes.add(representative_scope)
            if category in counts:
                counts[category] += 1
            if bucket in counts:
                counts[bucket] += 1
            if category == "exploration":
                selected_exploration += 1
        else:
            rejected.append({**task, "validation_reasons": reason})

    payload = {
        "generated_from_proposal": str(proposal_path),
        "summary": {
            "accepted_count": len(accepted),
            "rejected_count": len(rejected),
            "category_counts": counts,
            "exploration_floor": floor,
            "selected_exploration": selected_exploration,
        },
        "accepted_tasks": accepted,
        "rejected_tasks": rejected,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
