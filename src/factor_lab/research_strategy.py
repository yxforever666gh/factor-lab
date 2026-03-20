from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.research_runtime_state import recently_finished_same_fingerprint
from factor_lab.storage import ExperimentStore

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"
ARTIFACTS = ROOT / "artifacts"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _task_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload")
    if isinstance(payload, dict):
        return payload
    raw = task.get("payload_json")
    if raw:
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def load_or_initialize_research_memory(memory_path: str | Path) -> dict[str, Any]:
    path = Path(memory_path)
    memory = _read_json(path, None)
    if isinstance(memory, dict):
        return memory
    memory = {
        "updated_at_utc": None,
        "stable_candidates": [],
        "repeated_failure_patterns": [],
        "high_value_open_questions": [],
        "branch_history": [],
        "strategy_runs": [],
        "candidate_lifecycle": {},
        "branch_lifecycle": {},
        "archived_branches": [],
    }
    _write_json(path, memory)
    return memory


def build_research_state_snapshot(
    db_path: str | Path,
    planner_snapshot_path: str | Path,
    candidate_pool_path: str | Path,
    proposal_path: str | Path,
    output_path: str | Path,
    memory_path: str | Path,
) -> dict[str, Any]:
    db_path = Path(db_path)
    planner_snapshot = _read_json(Path(planner_snapshot_path), {})
    candidate_pool = _read_json(Path(candidate_pool_path), {})
    proposal = _read_json(Path(proposal_path), {})
    memory = load_or_initialize_research_memory(memory_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        recent_tasks = [
            dict(row)
            for row in conn.execute(
                """
                SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                       parent_task_id, attempt_count, last_error, created_at_utc,
                       started_at_utc, finished_at_utc, worker_note
                FROM research_tasks
                ORDER BY created_at_utc DESC
                LIMIT 120
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    finished_tasks = [t for t in recent_tasks if t["status"] == "finished"]
    failed_tasks = [t for t in recent_tasks if t["status"] == "failed"]
    pending_tasks = [t for t in recent_tasks if t["status"] in {"pending", "running"}]

    stable_candidates = [row.get("factor_name") for row in (planner_snapshot.get("stable_candidates") or []) if row.get("factor_name")]
    latest_graveyard = planner_snapshot.get("latest_graveyard") or []

    candidate_tasks = candidate_pool.get("tasks") or []
    proposal_selected = proposal.get("selected_tasks") or []

    repeated_failures: dict[str, int] = {}
    for task in failed_tasks[:20]:
        key = task.get("task_type") or "unknown"
        repeated_failures[key] = repeated_failures.get(key, 0) + 1

    branch_signals: list[dict[str, Any]] = []
    for task in candidate_tasks:
        relationship_signal = task.get("relationship_signal") or {}
        branch_signals.append(
            {
                "branch_id": task.get("branch_id") or task.get("dedupe_signature") or task.get("fingerprint") or task.get("worker_note"),
                "category": task.get("category"),
                "task_type": task.get("task_type"),
                "priority_hint": task.get("priority_hint"),
                "goal": task.get("goal") or (task.get("payload") or {}).get("goal"),
                "hypothesis": task.get("hypothesis") or (task.get("payload") or {}).get("hypothesis"),
                "expected_knowledge_gain": task.get("expected_knowledge_gain") or [],
                "duplicate_risk": int(relationship_signal.get("duplicate_count") or 0),
                "fragile_candidate_count": int(relationship_signal.get("fragile_candidate_count") or 0),
                "family_focus": task.get("family_focus"),
                "lifecycle_state": ((memory.get("branch_lifecycle") or {}).get(task.get("branch_id") or "") or {}).get("state", "exploring"),
            }
        )

    payload = {
        "updated_at_utc": _iso_now(),
        "generated_from_planner_snapshot": str(planner_snapshot_path),
        "generated_from_candidate_pool": str(candidate_pool_path),
        "generated_from_proposal": str(proposal_path),
        "generated_from_memory": str(memory_path),
        "queue": {
            "pending": len([t for t in recent_tasks if t["status"] == "pending"]),
            "running": len([t for t in recent_tasks if t["status"] == "running"]),
            "finished": len(finished_tasks),
            "failed": len(failed_tasks),
        },
        "queue_budget": planner_snapshot.get("queue_budget") or {},
        "failure_state": planner_snapshot.get("failure_state") or {},
        "exploration_state": planner_snapshot.get("exploration_state") or {},
        "knowledge_gain_counter": planner_snapshot.get("knowledge_gain_counter") or {},
        "candidates": {
            "stable": stable_candidates,
            "provisional": [
                row.get("factor_name") for row in (planner_snapshot.get("top_scores") or [])[:10] if row.get("factor_name")
            ],
            "graveyard": latest_graveyard,
        },
        "planner": {
            "candidate_task_count": len(candidate_tasks),
            "proposal_selected_count": len(proposal_selected),
            "branch_selected_families": (proposal.get("strategy_summary") or {}).get("selected_families")
            or planner_snapshot.get("family_recommendations")
            or [],
        },
        "recent_finished_tasks": [
            {
                **{k: v for k, v in task.items() if k != "payload_json"},
                "payload": _task_payload(task),
            }
            for task in finished_tasks[:15]
        ],
        "recent_failed_tasks": [
            {
                **{k: v for k, v in task.items() if k != "payload_json"},
                "payload": _task_payload(task),
            }
            for task in failed_tasks[:10]
        ],
        "pending_task_preview": [
            {
                **{k: v for k, v in task.items() if k != "payload_json"},
                "payload": _task_payload(task),
            }
            for task in pending_tasks[:10]
        ],
        "branch_signals": branch_signals[:40],
        "memory": memory,
        "open_questions": list(memory.get("high_value_open_questions") or []),
        "repeated_failure_patterns": [
            {"task_type": key, "count": count} for key, count in sorted(repeated_failures.items(), key=lambda item: (-item[1], item[0]))
        ],
    }
    _write_json(Path(output_path), payload)
    return payload


class StrategyBrain:
    DEFAULT_BUDGETS = {"validation": 2, "baseline": 1, "exploration": 1}

    def build_plan(self, state_snapshot: dict[str, Any], proposal: dict[str, Any], branch_plan: dict[str, Any] | None = None) -> dict[str, Any]:
        tasks = list(proposal.get("selected_tasks") or [])
        if not tasks:
            return {
                "summary": "proposal 无可执行任务，strategy 仅记录当前状态。",
                "budget": dict(self.DEFAULT_BUDGETS),
                "approved_tasks": [],
                "rejected_tasks": [],
                "branch_actions": [],
                "memory_updates": {},
            }

        memory = state_snapshot.get("memory") or {}
        stable_candidates = set((state_snapshot.get("candidates") or {}).get("stable") or [])
        graveyard_candidates = set((state_snapshot.get("candidates") or {}).get("graveyard") or [])
        repeated_failures = {
            row.get("task_type"): int(row.get("count") or 0)
            for row in (state_snapshot.get("repeated_failure_patterns") or [])
            if row.get("task_type")
        }
        exploration_state = state_snapshot.get("exploration_state") or {}
        knowledge_gain_counter = state_snapshot.get("knowledge_gain_counter") or {}
        selected_families = set((branch_plan or {}).get("selected_families") or [])

        budgets = dict(self.DEFAULT_BUDGETS)
        if exploration_state.get("should_throttle"):
            budgets["exploration"] = 0
            budgets["validation"] = 3
        if knowledge_gain_counter.get("stable_candidate_confirmed", 0) > 0:
            budgets["validation"] += 1
        if repeated_failures.get("diagnostic", 0) >= 2:
            budgets["validation"] += 1

        ranked: list[dict[str, Any]] = []
        branch_actions: list[dict[str, Any]] = []
        candidate_updates: dict[str, dict[str, Any]] = {}
        for task in tasks:
            category = task.get("category") or "validation"
            relationship_signal = task.get("relationship_signal") or {}
            expected_gain = set(task.get("expected_knowledge_gain") or [])
            focus_candidates = {row.get("candidate_name") for row in (task.get("focus_candidates") or []) if row.get("candidate_name")}
            score = float(task.get("planner_score") or (100 - int(task.get("priority_hint", 50))))
            reason_bits = [task.get("planner_reason") or task.get("reason") or ""]

            if category == "validation":
                score += 8
                reason_bits.append("验证型任务优先，防止系统只扩不收敛。")
            if focus_candidates & stable_candidates:
                score += 10
                reason_bits.append("命中稳定候选主线。")
            if expected_gain & {"stable_candidate_validation_requested", "stable_candidate_confirmed"}:
                score += 8
            if any(gain.startswith("graveyard_") for gain in expected_gain):
                score += 4
            if relationship_signal.get("fragile_candidate_count"):
                score += min(int(relationship_signal.get("fragile_candidate_count") or 0) * 3, 9)
                reason_bits.append("存在 fragile 候选，优先做稳健性验证。")
            if relationship_signal.get("duplicate_count") and category != "validation":
                score -= min(int(relationship_signal.get("duplicate_count") or 0) * 2, 8)
                reason_bits.append("重复信号偏高，非验证任务降权。")
            if category == "exploration" and exploration_state.get("should_throttle"):
                score -= 50
                reason_bits.append("exploration 当前被 throttle。")
            if selected_families and task.get("family_focus") in selected_families:
                score += 4
            for failed_type, count in repeated_failures.items():
                if failed_type == task.get("task_type") and count >= 2:
                    score -= 6
                    reason_bits.append(f"近期 {failed_type} 失败偏多，轻微降权。")
            if (memory.get("stable_candidates") or []) and category == "validation":
                score += 3
            if task.get("worker_note", "").find("graveyard") >= 0 and category == "validation":
                score += 2

            branch_action = _branch_lifecycle_decision(task, memory, exploration_state)
            branch_actions.append(branch_action)
            for candidate_name in sorted(focus_candidates):
                candidate_updates[candidate_name] = {
                    "candidate_name": candidate_name,
                    "next_state": _candidate_lifecycle_state(candidate_name, stable_candidates, graveyard_candidates, task),
                    "source_branch_id": task.get("branch_id") or task.get("dedupe_signature") or task.get("fingerprint"),
                    "action": branch_action.get("action"),
                }

            strategy_meta = {
                "score": round(score, 3),
                "category": category,
                "reason": " ".join(bit for bit in reason_bits if bit),
                "selected_families": sorted(selected_families),
                "focus_candidates": sorted(focus_candidates),
                "goal": task.get("goal") or (task.get("payload") or {}).get("goal"),
                "hypothesis": task.get("hypothesis") or (task.get("payload") or {}).get("hypothesis"),
                "branch_id": task.get("branch_id") or (task.get("payload") or {}).get("branch_id"),
                "stop_if": task.get("stop_if") or (task.get("payload") or {}).get("stop_if") or [],
                "promote_if": task.get("promote_if") or (task.get("payload") or {}).get("promote_if") or [],
                "disconfirm_if": task.get("disconfirm_if") or (task.get("payload") or {}).get("disconfirm_if") or [],
            }
            ranked.append({**task, "strategy_meta": strategy_meta, "strategy_score": round(score, 3), "branch_action": branch_action})

        ranked.sort(key=lambda item: (-float(item.get("strategy_score") or 0.0), int(item.get("priority_hint", 999))))
        counts = {key: 0 for key in budgets}
        approved: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        approved_branch_actions: list[dict[str, Any]] = []
        for task in ranked:
            category = task.get("category") or "validation"
            limit = budgets.get(category, 1)
            if counts.get(category, 0) >= limit:
                rejected.append({**task, "strategy_rejection_reason": f"budget_exhausted:{category}"})
                continue
            approved.append(task)
            counts[category] = counts.get(category, 0) + 1
            if task.get("branch_action"):
                approved_branch_actions.append(task["branch_action"])

        branch_history_append = []
        branch_lifecycle_updates = {}
        for task in approved[:10]:
            action = task.get("branch_action") or _branch_lifecycle_decision(task, memory, exploration_state)
            branch_history_append.append(
                {
                    "updated_at_utc": _iso_now(),
                    "target": task.get("branch_id") or task.get("dedupe_signature") or task.get("fingerprint"),
                    "category": task.get("category"),
                    "strategy_score": task.get("strategy_score"),
                    "action": action.get("action") or "approved",
                    "next_state": action.get("next_state"),
                }
            )
            branch_lifecycle_updates[action["branch_id"]] = {
                "state": action.get("next_state"),
                "last_action": action.get("action"),
                "reason": action.get("reason"),
                "updated_at_utc": _iso_now(),
                "goal": task.get("goal") or (task.get("payload") or {}).get("goal"),
                "hypothesis": task.get("hypothesis") or (task.get("payload") or {}).get("hypothesis"),
            }

        memory_updates = {
            "stable_candidates": sorted(stable_candidates),
            "high_value_open_questions": _derive_open_questions(approved),
            "branch_history_append": branch_history_append,
            "branch_lifecycle_updates": branch_lifecycle_updates,
            "candidate_lifecycle_updates": candidate_updates,
            "archived_branches": [
                action["branch_id"]
                for action in approved_branch_actions
                if action.get("next_state") in {"archived", "terminated", "saturated"}
            ],
        }
        return {
            "summary": "strategy brain 对 proposal 进行了预算约束、显式打分与分支动作标注。",
            "budget": budgets,
            "budget_usage": counts,
            "approved_tasks": approved,
            "rejected_tasks": rejected,
            "branch_actions": approved_branch_actions,
            "memory_updates": memory_updates,
        }


def _candidate_lifecycle_state(name: str, stable_candidates: set[str], graveyard_candidates: set[str], task: dict[str, Any] | None = None) -> str:
    if name in stable_candidates:
        return "stable_candidate"
    if name in graveyard_candidates:
        return "graveyard"
    if task and task.get("category") == "validation":
        return "validating"
    return "provisional"


def _branch_lifecycle_decision(task: dict[str, Any], memory: dict[str, Any], exploration_state: dict[str, Any]) -> dict[str, Any]:
    relationship_signal = task.get("relationship_signal") or {}
    branch_id = task.get("branch_id") or task.get("dedupe_signature") or task.get("fingerprint")
    branch_memory = (memory.get("branch_lifecycle") or {}).get(branch_id, {})
    previous_state = branch_memory.get("state", "exploring")
    duplicate_count = int(relationship_signal.get("duplicate_count") or 0)
    fragile_count = int(relationship_signal.get("fragile_candidate_count") or 0)
    strategy_score = float(task.get("strategy_score") or task.get("planner_score") or 0.0)
    category = task.get("category") or "validation"

    action = "hold"
    next_state = previous_state
    reason = "maintain_current_branch_state"
    if category == "validation" and strategy_score >= 130:
        action = "promote"
        next_state = "validating"
        reason = "high_confidence_validation_branch"
    elif category == "exploration" and exploration_state.get("should_throttle"):
        action = "archive"
        next_state = "archived"
        reason = "exploration_throttled"
    elif duplicate_count >= 4 and category != "validation":
        action = "terminate"
        next_state = "terminated"
        reason = "duplicate_pressure_high"
    elif fragile_count >= 2 and category == "validation":
        action = "hold"
        next_state = "validating"
        reason = "fragile_candidates_need_more_validation"
    elif strategy_score < 90:
        action = "demote"
        next_state = "saturated"
        reason = "low_strategy_score"
    return {
        "branch_id": branch_id,
        "previous_state": previous_state,
        "next_state": next_state,
        "action": action,
        "reason": reason,
    }


def _derive_open_questions(approved_tasks: list[dict[str, Any]]) -> list[str]:
    questions: list[str] = []
    for task in approved_tasks:
        category = task.get("category") or "validation"
        focus = task.get("focus_candidates") or []
        focus_names = [row.get("candidate_name") for row in focus if row.get("candidate_name")]
        if task.get("hypothesis"):
            questions.append(str(task.get("hypothesis")))
        elif category == "validation" and focus_names:
            questions.append(f"验证候选 {', '.join(focus_names[:3])} 的跨窗口稳定性是否继续成立？")
        elif category == "baseline":
            questions.append("更宽历史窗口下，当前强候选是否仍保持一致排序？")
        elif category == "exploration":
            questions.append("是否存在能补充当前家族结构的新组合，而不是重复旧信号？")
    deduped: list[str] = []
    seen = set()
    for question in questions:
        if question in seen:
            continue
        seen.add(question)
        deduped.append(question)
    return deduped[:5]


def build_strategy_plan(
    state_snapshot_path: str | Path,
    proposal_path: str | Path,
    output_path: str | Path,
    branch_plan_path: str | Path | None = None,
) -> dict[str, Any]:
    state_snapshot = _read_json(Path(state_snapshot_path), {})
    proposal = _read_json(Path(proposal_path), {})
    branch_plan = _read_json(Path(branch_plan_path), {}) if branch_plan_path and Path(branch_plan_path).exists() else None
    brain = StrategyBrain()
    result = brain.build_plan(state_snapshot, proposal, branch_plan)
    payload = {
        "updated_at_utc": _iso_now(),
        "generated_from_state_snapshot": str(state_snapshot_path),
        "generated_from_proposal": str(proposal_path),
        "generated_from_branch_plan": str(branch_plan_path) if branch_plan_path else None,
        **result,
    }
    _write_json(Path(output_path), payload)
    return payload


def apply_strategy_plan(
    validated_path: str | Path,
    strategy_plan_path: str | Path,
    output_path: str | Path,
    memory_path: str | Path,
    db_path: str | Path = DB_PATH,
) -> dict[str, Any]:
    validated = _read_json(Path(validated_path), {})
    strategy_plan = _read_json(Path(strategy_plan_path), {})
    approved_tasks = strategy_plan.get("approved_tasks") or []
    validated_accepted = validated.get("accepted_tasks") or []
    validated_by_fingerprint = {task.get("fingerprint"): task for task in validated_accepted if task.get("fingerprint")}

    store = ExperimentStore(db_path)
    injected = []
    skipped = []
    for task in approved_tasks:
        fingerprint = task.get("fingerprint")
        if fingerprint and recently_finished_same_fingerprint(store, fingerprint):
            skipped.append({"fingerprint": fingerprint, "reason": "recently_finished_same_fingerprint"})
            continue
        validated_task = validated_by_fingerprint.get(fingerprint, task)
        payload = dict(validated_task.get("payload") or task.get("payload") or {})
        payload["strategy"] = task.get("strategy_meta") or {}
        worker_note = (validated_task.get("worker_note") or task.get("worker_note") or "")
        strategy_reason = ((task.get("strategy_meta") or {}).get("reason") or "").strip()
        if strategy_reason:
            worker_note = f"{worker_note}｜strategy:{strategy_reason}"
        task_id = store.enqueue_research_task(
            task_type=validated_task.get("task_type") or task.get("task_type"),
            payload=payload,
            priority=int(task.get("priority_hint") or validated_task.get("priority_hint") or 50),
            fingerprint=fingerprint,
            parent_task_id=(validated_task.get("parent_task_id") or task.get("parent_task_id")),
            worker_note=worker_note + "｜strategy_selected",
        )
        injected.append({
            "task_id": task_id,
            "fingerprint": fingerprint,
            "category": task.get("category"),
            "strategy_score": task.get("strategy_score"),
        })

    memory = load_or_initialize_research_memory(memory_path)
    memory["updated_at_utc"] = _iso_now()
    updates = strategy_plan.get("memory_updates") or {}
    if updates.get("stable_candidates"):
        memory["stable_candidates"] = updates.get("stable_candidates")
    if updates.get("high_value_open_questions"):
        memory["high_value_open_questions"] = updates.get("high_value_open_questions")
    candidate_lifecycle = dict(memory.get("candidate_lifecycle") or {})
    for name, candidate_update in (updates.get("candidate_lifecycle_updates") or {}).items():
        candidate_lifecycle[name] = {
            **candidate_lifecycle.get(name, {}),
            **candidate_update,
            "updated_at_utc": _iso_now(),
        }
    memory["candidate_lifecycle"] = candidate_lifecycle
    branch_lifecycle = dict(memory.get("branch_lifecycle") or {})
    branch_lifecycle.update(updates.get("branch_lifecycle_updates") or {})
    memory["branch_lifecycle"] = branch_lifecycle
    archived_branches = list(memory.get("archived_branches") or [])
    archived_branches.extend(updates.get("archived_branches") or [])
    memory["archived_branches"] = archived_branches[-100:]
    branch_history = list(memory.get("branch_history") or [])
    branch_history.extend(updates.get("branch_history_append") or [])
    memory["branch_history"] = branch_history[-100:]
    strategy_runs = list(memory.get("strategy_runs") or [])
    strategy_runs.append({
        "updated_at_utc": _iso_now(),
        "approved_count": len(approved_tasks),
        "injected_count": len(injected),
        "branch_actions": strategy_plan.get("branch_actions") or [],
    })
    memory["strategy_runs"] = strategy_runs[-50:]
    if strategy_plan.get("branch_actions"):
        patterns = list(memory.get("repeated_failure_patterns") or [])
        for action in strategy_plan.get("branch_actions") or []:
            patterns.append(action)
        memory["repeated_failure_patterns"] = patterns[-50:]
    _write_json(Path(memory_path), memory)

    payload = {
        "generated_from_validated": str(validated_path),
        "generated_from_strategy_plan": str(strategy_plan_path),
        "generated_from_memory": str(memory_path),
        "injected_count": len(injected),
        "injected_tasks": injected,
        "skipped_tasks": skipped,
        "memory_updated": True,
        "branch_lifecycle_updates": updates.get("branch_lifecycle_updates") or {},
        "candidate_lifecycle_updates": updates.get("candidate_lifecycle_updates") or {},
        "archived_branches": updates.get("archived_branches") or [],
    }
    _write_json(Path(output_path), payload)
    return payload
