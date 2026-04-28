from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.research_runtime_state import recently_finished_same_fingerprint
from factor_lab.storage import ExperimentStore
from factor_lab.exploration_budget import exploration_floor_context

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"
ARTIFACTS = ROOT / "artifacts"
AUTONOMY_POLICY_PATH = ROOT / "configs" / "research_autonomy_policy.json"
CODING_POLICY_PATH = ROOT / "configs" / "research_coding_policy.json"


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


def _frontier_preferred_candidates(source: dict[str, Any]) -> set[str]:
    frontier = source.get("frontier_focus") or {}
    robust = frontier.get("robust_candidates") or []
    if robust:
        return {name for name in robust if name}
    soft_robust = frontier.get("soft_robust_candidates") or []
    if soft_robust:
        return {name for name in soft_robust if name}
    preferred = frontier.get("short_window_candidates") or frontier.get("preferred_candidates") or []
    if preferred:
        return {name for name in preferred if name}
    return {name for name in ((source.get("candidates") or {}).get("stable") or []) if name}


def _frontier_suppressed_candidates(source: dict[str, Any]) -> set[str]:
    frontier = source.get("frontier_focus") or {}
    return {name for name in (frontier.get("suppressed_candidates") or []) if name}


def _is_explicit_stable_validation(
    *,
    task: dict[str, Any],
    payload: dict[str, Any],
    strategy: dict[str, Any],
    knowledge_gain: list[str],
    summary_text: str,
) -> bool:
    branch_id = str(payload.get("branch_id") or strategy.get("branch_id") or task.get("fingerprint") or "")
    goal = str(payload.get("goal") or strategy.get("goal") or "")
    worker_note = str(task.get("worker_note") or "")
    promote_if = payload.get("promote_if") or strategy.get("promote_if") or []
    text_haystack = " ".join([
        branch_id,
        goal,
        worker_note,
        summary_text,
        " ".join(promote_if),
        " ".join(knowledge_gain),
    ]).lower()
    return (
        "stable_candidate_validation" in text_haystack
        or "validate_stable_candidates" in text_haystack
        or "stable_candidate_confirmed" in text_haystack
    )


def _reconcile_candidate_lifecycle(
    candidate_lifecycle: dict[str, dict[str, Any]],
    *,
    preferred_candidates: set[str],
    suppressed_candidates: set[str],
) -> dict[str, dict[str, Any]]:
    now = _iso_now()
    for name, current in candidate_lifecycle.items():
        current_state = current.get("next_state") or "provisional"
        target_state = current_state
        target_action = current.get("action") or "hold"
        reason = None
        if name in suppressed_candidates and current_state in {"stable_candidate", "validating"}:
            target_state = "provisional"
            target_action = "demote"
            reason = "frontier_suppressed"
        elif current_state == "stable_candidate" and name not in preferred_candidates:
            target_state = "validating"
            target_action = "demote"
            reason = "frontier_not_preferred"
        if not reason:
            continue
        history = list(current.get("history") or [])
        history.append({
            "updated_at_utc": now,
            "next_state": target_state,
            "action": target_action,
            "source_branch_id": "frontier_reconcile",
            "reason": reason,
        })
        current.update({
            "candidate_name": name,
            "next_state": target_state,
            "action": target_action,
            "source_branch_id": "frontier_reconcile",
            "updated_at_utc": now,
        })
        current["history"] = history[-20:]
    return candidate_lifecycle


def _compact_branch_action_rows(rows: list[dict[str, Any]], *, key_fields: tuple[str, ...]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    last_key: tuple[Any, ...] | None = None
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        if compacted and key == last_key:
            compacted[-1] = row
            continue
        compacted.append(row)
        last_key = key
    return compacted


def _compact_fallback_history(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    last_key: tuple[Any, ...] | None = None
    for row in rows:
        key = (
            row.get("branch_id"),
            row.get("status"),
            row.get("has_gain"),
            row.get("task_type"),
            row.get("goal"),
            tuple(row.get("focus_candidates") or []),
            tuple(row.get("knowledge_gain") or []),
        )
        if compacted and key == last_key:
            compacted[-1] = row
            continue
        compacted.append(row)
        last_key = key
    return compacted


def _compact_branch_lifecycle(branch_lifecycle: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    no_gain_reasons = {
        "no_significant_information_gain",
        "task_failed",
        "frontier_suppressed",
        "frontier_not_preferred",
        "low_strategy_score",
        "repeated_no_gain_or_demote",
    }
    compacted_lifecycle: dict[str, dict[str, Any]] = {}
    for branch_id, current in (branch_lifecycle or {}).items():
        current = dict(current or {})
        history = _compact_branch_action_rows(
            list(current.get("history") or []),
            key_fields=("state", "last_action", "reason"),
        )[-20:]
        current["history"] = history
        if history:
            tail = history[-1]
            current["state"] = tail.get("state", current.get("state"))
            current["last_action"] = tail.get("last_action", current.get("last_action"))
            current["reason"] = tail.get("reason", current.get("reason"))
        current["validation_runs"] = sum(1 for row in history if row.get("state") in {"validating", "stable_candidate"})
        no_gain_runs = 0
        for row in reversed(history):
            if row.get("reason") in no_gain_reasons:
                no_gain_runs += 1
            else:
                break
        current["no_gain_runs"] = no_gain_runs
        compacted_lifecycle[branch_id] = current
    return compacted_lifecycle


def _compact_memory_state(memory: dict[str, Any]) -> dict[str, Any]:
    memory = dict(memory or {})
    memory.setdefault("suppressed_candidates", [])
    memory["candidate_lifecycle"] = _reconcile_candidate_lifecycle(
        dict(memory.get("candidate_lifecycle") or {}),
        preferred_candidates=set(memory.get("stable_candidates") or []),
        suppressed_candidates=set(memory.get("suppressed_candidates") or []),
    )
    memory["branch_lifecycle"] = _compact_branch_lifecycle(dict(memory.get("branch_lifecycle") or {}))
    memory["branch_history"] = _compact_branch_action_rows(
        list(memory.get("branch_history") or [])[-120:],
        key_fields=("target", "action", "next_state"),
    )[-100:]
    memory["repeated_failure_patterns"] = _compact_branch_action_rows(
        list(memory.get("repeated_failure_patterns") or [])[-80:],
        key_fields=("branch_id", "action", "next_state", "reason"),
    )[-50:]
    memory["fallback_history"] = _compact_fallback_history(list(memory.get("fallback_history") or [])[-60:])[-30:]
    return memory


def load_or_initialize_research_memory(memory_path: str | Path) -> dict[str, Any]:
    path = Path(memory_path)
    memory = _read_json(path, None)
    if isinstance(memory, dict):
        return _compact_memory_state(memory)
    memory = {
        "updated_at_utc": None,
        "stable_candidates": [],
        "suppressed_candidates": [],
        "repeated_failure_patterns": [],
        "high_value_open_questions": [],
        "branch_history": [],
        "strategy_runs": [],
        "candidate_lifecycle": {},
        "branch_lifecycle": {},
        "archived_branches": [],
        "execution_feedback": [],
        "fallback_history": [],
        "generated_candidate_outcomes": [],
        "candidate_generation_history": [],
        "representative_candidate_reviews": [],
        "autonomy_profile": {
            "policy_name": None,
            "unit_of_research": None,
            "preferred_objectives": [],
            "budget_policy": {},
            "learning_bias": {
                "reward_high_value_failure": False,
                "discourage_low_information_repetition": False,
                "treat_boundary_discovery_as_progress": False
            },
            "observed_outcome_mix": {},
            "last_research_style_refresh_at_utc": None
        },
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
    autonomy_policy = _read_json(AUTONOMY_POLICY_PATH, {})
    coding_policy = _read_json(CODING_POLICY_PATH, {})

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
    frontier_focus = planner_snapshot.get("frontier_focus") or {}

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

    knowledge_gain_counter = planner_snapshot.get("knowledge_gain_counter") or {}
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
        "knowledge_gain_counter": knowledge_gain_counter,
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
            "analyst_signals": planner_snapshot.get("analyst_signals") or {},
        },
        "frontier_focus": frontier_focus,
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
        "autonomy_policy": autonomy_policy,
        "coding_policy": coding_policy,
        "open_questions": list(memory.get("high_value_open_questions") or []),
        "repeated_failure_patterns": [
            {"task_type": key, "count": count} for key, count in sorted(repeated_failures.items(), key=lambda item: (-item[1], item[0]))
        ],
        "convergence_policy": {
            "archive_after_no_gain_runs": 2,
            "terminate_after_duplicate_pressure": 4,
            "promote_after_validation_score": 145,
            "demote_below_strategy_score": 90,
            "terminate_after_hold_count": 3,
        },
    }
    _write_json(Path(output_path), payload)
    return payload


class StrategyBrain:
    DEFAULT_BUDGETS = {"validation": 2, "baseline": 1, "exploration": 1}

    @staticmethod
    def _budget_bucket(task: dict[str, Any]) -> str:
        category = task.get("category") or "validation"
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

    @staticmethod
    def _apply_autonomy_budget_policy(budgets: dict[str, int], autonomy_policy: dict[str, Any], *, exploration_state: dict[str, Any], stable_gain_count: int, repeated_diagnostic_failures: int) -> dict[str, int]:
        policy = (autonomy_policy or {}).get("budget_policy") or {}
        exploitation = float(policy.get("exploitation") or 0.45)
        adjacent = float(policy.get("adjacent_exploration") or 0.35)
        novelty = float(policy.get("novelty_search") or 0.2)
        total = max(exploitation + adjacent + novelty, 0.01)
        base_slots = max(sum(StrategyBrain.DEFAULT_BUDGETS.values()), 4)
        new_budgets = dict(budgets)
        new_budgets["validation"] = max(1, round(base_slots * exploitation / total))
        new_budgets["baseline"] = max(1, round(base_slots * adjacent / total))
        new_budgets["exploration"] = max(1, round(base_slots * novelty / total))
        floor = exploration_floor_context({
            "failure_state": {"cooldown_active": exploration_state.get("cooldown_active")},
            "research_flow_state": {"state": exploration_state.get("runtime_state")},
        })
        if exploration_state.get("should_throttle") and not floor["true_fault_recovery"]:
            new_budgets["exploration"] = max(new_budgets["exploration"], floor["exploration_floor_slots"])
            new_budgets["validation"] = max(new_budgets["validation"], 3)
        elif floor["true_fault_recovery"]:
            new_budgets["exploration"] = 0
            new_budgets["validation"] = max(new_budgets["validation"], 3)
        if stable_gain_count > 0:
            new_budgets["validation"] += 1
            new_budgets["validation_stable"] = max(int(new_budgets.get("validation_stable") or 1), 2)
            new_budgets["validation_fragile"] = max(int(new_budgets.get("validation_fragile") or 0), 1)
        if repeated_diagnostic_failures >= 2:
            new_budgets["validation"] += 1
        return new_budgets

    def build_plan(self, state_snapshot: dict[str, Any], proposal: dict[str, Any], branch_plan: dict[str, Any] | None = None) -> dict[str, Any]:
        tasks = list(proposal.get("selected_tasks") or [])
        if not tasks:
            convergence_policy = state_snapshot.get("convergence_policy") or {
                "archive_after_no_gain_runs": 2,
                "terminate_after_duplicate_pressure": 4,
                "promote_after_validation_score": 145,
                "demote_below_strategy_score": 90,
                "terminate_after_hold_count": 3,
            }
            return {
                "summary": "proposal 无可执行任务，strategy 仅记录当前状态。",
                "autonomy_policy": state_snapshot.get("autonomy_policy") or {},
                "coding_policy": state_snapshot.get("coding_policy") or {},
                "budget": dict(self.DEFAULT_BUDGETS),
                "approved_tasks": [],
                "rejected_tasks": [],
                "branch_actions": [],
                "memory_updates": {"convergence_policy": convergence_policy},
                "convergence_policy": convergence_policy,
            }

        memory = state_snapshot.get("memory") or {}
        floor = exploration_floor_context(state_snapshot)
        stable_candidates = _frontier_preferred_candidates(state_snapshot)
        suppressed_candidates = _frontier_suppressed_candidates(state_snapshot)
        graveyard_candidates = set((state_snapshot.get("candidates") or {}).get("graveyard") or [])
        repeated_failures = {
            row.get("task_type"): int(row.get("count") or 0)
            for row in (state_snapshot.get("repeated_failure_patterns") or [])
            if row.get("task_type")
        }
        exploration_state = state_snapshot.get("exploration_state") or {}
        knowledge_gain_counter = state_snapshot.get("knowledge_gain_counter") or {}
        convergence_policy = state_snapshot.get("convergence_policy") or {}
        selected_families = set((branch_plan or {}).get("selected_families") or [])
        analyst_signals = ((state_snapshot.get("planner") or {}).get("analyst_signals") or {})
        analyst_focus = set(analyst_signals.get("focus_factors") or [])
        analyst_core = set(analyst_signals.get("keep_as_core_candidates") or [])
        analyst_graveyard = set(analyst_signals.get("review_graveyard") or [])
        agent_signals = analyst_signals or {}
        agent_mode = str(agent_signals.get("planner_mode") or "").strip()
        agent_task_mix = agent_signals.get("planner_task_mix") or {}
        agent_priority_families = set(agent_signals.get("focus_factors") or [])
        failure_probe_targets = set(agent_signals.get("failure_should_probe") or [])
        agent_suppress_families = set((agent_signals.get("risk_flags") or []))
        agent_suppress_families = {
            flag.split(":", 1)[1]
            for flag in agent_suppress_families
            if isinstance(flag, str) and flag.startswith("stop:")
        }

        autonomy_policy = state_snapshot.get("autonomy_policy") or {}
        coding_policy = state_snapshot.get("coding_policy") or {}
        budgets = dict(self.DEFAULT_BUDGETS)
        budgets["validation_stable"] = 2
        budgets["validation_medium_horizon"] = 2
        budgets["validation_fragile"] = 1
        budgets = self._apply_autonomy_budget_policy(
            budgets,
            autonomy_policy,
            exploration_state=exploration_state,
            stable_gain_count=int(knowledge_gain_counter.get("stable_candidate_confirmed", 0) or 0),
            repeated_diagnostic_failures=int(repeated_failures.get("diagnostic", 0) or 0),
        )
        if agent_task_mix:
            for key in ["baseline", "validation", "exploration"]:
                if key in agent_task_mix:
                    try:
                        budgets[key] = max(0, int(agent_task_mix[key]))
                    except Exception:
                        pass
        if agent_mode == "recover":
            budgets["exploration"] = 0 if floor["true_fault_recovery"] else max(int(budgets.get("exploration", 0) or 0), floor["exploration_floor_slots"])
            budgets["validation"] = max(budgets.get("validation", 0), 2)
        elif agent_mode == "converge":
            budgets["exploration"] = max(min(budgets.get("exploration", 0), 1), floor["exploration_floor_slots"])
            budgets["validation"] = max(budgets.get("validation", 0), 3)

        budgets["exploration"] = 0 if floor["true_fault_recovery"] else max(int(budgets.get("exploration", 0) or 0), floor["exploration_floor_slots"])

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
            if focus_candidates & analyst_focus:
                score += 10
                reason_bits.append("命中 analyst focus。")
            if focus_candidates & analyst_core:
                score += 12
                reason_bits.append("命中 analyst core。")
            if category == "validation" and analyst_graveyard and set((task.get("payload") or {}).get("focus_factors") or []) & analyst_graveyard:
                score += 8
                reason_bits.append("命中 analyst 指定复核墓地。")
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
            if analyst_signals.get("must_validate_before_expand") and category in {"exploration", "baseline"}:
                score -= 18 if category == "exploration" else 8
                reason_bits.append("analyst 当前要求先验证再扩张。")
            if selected_families and task.get("family_focus") in selected_families:
                score += 4
            if task.get("family_focus") in agent_priority_families:
                score += 10
                reason_bits.append("命中 Agent priority family。")
            if task.get("family_focus") in failure_probe_targets and category == "validation":
                score += 8
                reason_bits.append("命中 failure analyst probe target。")
            for failed_type, count in repeated_failures.items():
                if failed_type == task.get("task_type") and count >= 2:
                    score -= 6
                    reason_bits.append(f"近期 {failed_type} 失败偏多，轻微降权。")
            if (memory.get("stable_candidates") or []) and category == "validation":
                score += 3
            if task.get("worker_note", "").find("graveyard") >= 0 and category == "validation":
                score += 2

            quality_gates = autonomy_policy.get("quality_gates") or {}
            avoid = set(quality_gates.get("avoid") or [])
            prefer = set(quality_gates.get("prefer") or [])
            if "high_corr_duplicate_variants" in avoid and relationship_signal.get("duplicate_count") and category != "validation":
                score -= min(int(relationship_signal.get("duplicate_count") or 0) * 3, 12)
                reason_bits.append("autonomy_policy: 避免高相关重复变体。")
            if "single_window_luck" in avoid and category == "exploration" and not expected_gain:
                score -= 6
                reason_bits.append("autonomy_policy: 低信息探索降权，避免单窗口运气型任务。")
            if "repeated_no_gain_retests" in avoid and int((memory.get("branch_lifecycle") or {}).get(task.get("branch_id") or '', {}).get("no_gain_runs") or 0) >= 1:
                score -= 8
                reason_bits.append("autonomy_policy: 连续无增益重试降权。")
            if "epistemic_gain" in set(((autonomy_policy.get("principles") or {}).get("objective") or [])):
                if any(tag in expected_gain for tag in {"search_space_reduced", "boundary_confirmed", "new_branch_opened", "stable_candidate_confirmed", "repeated_graveyard_confirmed"}):
                    score += 6
                    reason_bits.append("autonomy_policy: 奖励高信息增益任务。")
            if "cross_window_survival" in prefer and category == "validation" and ("window" in str(task.get("goal") or '').lower() or "window" in str(task.get("branch_id") or '').lower()):
                score += 4
                reason_bits.append("autonomy_policy: 偏好跨窗口存活验证。")
            if "portfolio_contribution" in set(((autonomy_policy.get("principles") or {}).get("objective") or [])) and category == "validation" and any('portfolio' in str(x).lower() for x in (expected_gain or [])):
                score += 3
                reason_bits.append("autonomy_policy: 偏好组合层有贡献的验证。")

            branch_action = _branch_lifecycle_decision(task, memory, exploration_state)
            branch_action["policy"] = convergence_policy
            branch_actions.append(branch_action)
            for candidate_name in sorted(focus_candidates):
                candidate_updates[candidate_name] = {
                    "candidate_name": candidate_name,
                    "next_state": _candidate_lifecycle_state(candidate_name, stable_candidates, graveyard_candidates, task, suppressed_candidates),
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
            bucket = self._budget_bucket(task)
            family_focus = task.get("family_focus")
            if family_focus and family_focus in agent_suppress_families:
                rejected.append({**task, "strategy_rejection_reason": f"agent_suppressed_family:{family_focus}"})
                continue
            category_limit = budgets.get(category, 1)
            bucket_limit = budgets.get(bucket, category_limit)
            if counts.get(category, 0) >= category_limit:
                rejected.append({**task, "strategy_rejection_reason": f"budget_exhausted:{category}"})
                continue
            if counts.get(bucket, 0) >= bucket_limit:
                rejected.append({**task, "strategy_rejection_reason": f"budget_exhausted:{bucket}"})
                continue
            approved.append(task)
            counts[category] = counts.get(category, 0) + 1
            counts[bucket] = counts.get(bucket, 0) + 1
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
            "suppressed_candidates": sorted(suppressed_candidates),
            "high_value_open_questions": _derive_open_questions(approved),
            "branch_history_append": branch_history_append,
            "branch_lifecycle_updates": branch_lifecycle_updates,
            "candidate_lifecycle_updates": candidate_updates,
            "archived_branches": [
                action["branch_id"]
                for action in approved_branch_actions
                if action.get("next_state") in {"archived", "terminated", "saturated"}
            ],
            "convergence_policy": convergence_policy,
        }
        return {
            "summary": "strategy brain 对 proposal 进行了预算约束、显式打分与分支动作标注，并携带自主研究 / coding policy 上下文。",
            "autonomy_policy": autonomy_policy,
            "coding_policy": coding_policy,
            "budget": budgets,
            "budget_usage": counts,
            "approved_tasks": approved,
            "rejected_tasks": rejected,
            "branch_actions": approved_branch_actions,
            "memory_updates": memory_updates,
            "convergence_policy": convergence_policy,
        }


def _candidate_lifecycle_state(
    name: str,
    stable_candidates: set[str],
    graveyard_candidates: set[str],
    task: dict[str, Any] | None = None,
    suppressed_candidates: set[str] | None = None,
) -> str:
    suppressed_candidates = suppressed_candidates or set()
    if name in suppressed_candidates:
        return "provisional"
    if name in stable_candidates:
        return "stable_candidate"
    if name in graveyard_candidates:
        return "graveyard"
    if task and task.get("category") == "validation":
        return "validating"
    return "provisional"


def _count_recent_branch_actions(memory: dict[str, Any], branch_id: str, action_name: str | None = None) -> int:
    total = 0
    for row in reversed(list(memory.get("branch_history") or [])):
        if row.get("target") != branch_id:
            continue
        if action_name and row.get("action") != action_name:
            continue
        total += 1
    return total


def _count_recent_candidate_transitions(memory: dict[str, Any], candidate_name: str, state_name: str | None = None) -> int:
    lifecycle = (memory.get("candidate_lifecycle") or {}).get(candidate_name, {})
    history = lifecycle.get("history") or []
    if not state_name:
        return len(history)
    return len([row for row in history if row.get("next_state") == state_name])


def _branch_lifecycle_decision(task: dict[str, Any], memory: dict[str, Any], exploration_state: dict[str, Any]) -> dict[str, Any]:
    relationship_signal = task.get("relationship_signal") or {}
    payload = task.get("payload") or {}
    branch_id = task.get("branch_id") or task.get("dedupe_signature") or task.get("fingerprint")
    branch_memory = (memory.get("branch_lifecycle") or {}).get(branch_id, {})
    previous_state = branch_memory.get("state", "exploring")
    duplicate_count = int(relationship_signal.get("duplicate_count") or 0)
    fragile_count = int(relationship_signal.get("fragile_candidate_count") or 0)
    strategy_score = float(task.get("strategy_score") or task.get("planner_score") or 0.0)
    category = task.get("category") or "validation"
    no_gain_runs = int(branch_memory.get("no_gain_runs") or 0)
    validation_runs = int(branch_memory.get("validation_runs") or 0)
    recent_holds = _count_recent_branch_actions(memory, branch_id, "hold")
    recent_demotes = _count_recent_branch_actions(memory, branch_id, "demote")
    is_generated_candidate = payload.get("source") == "candidate_generation"

    action = "hold"
    next_state = previous_state
    reason = "maintain_current_branch_state"

    if is_generated_candidate:
        action = "hold"
        next_state = "validating"
        reason = "generated_candidate_needs_execution_feedback"
    elif category == "exploration" and exploration_state.get("should_throttle"):
        action = "archive"
        next_state = "archived"
        reason = "exploration_throttled"
    elif duplicate_count >= 4 and category != "validation":
        action = "terminate"
        next_state = "terminated"
        reason = "duplicate_pressure_high"
    elif no_gain_runs >= 2 or recent_demotes >= 2:
        action = "archive"
        next_state = "archived"
        reason = "repeated_no_gain_or_demote"
    elif strategy_score < 90:
        action = "demote"
        next_state = "saturated"
        reason = "low_strategy_score"
    elif fragile_count >= 2 and category == "validation":
        action = "hold"
        next_state = "validating"
        reason = "fragile_candidates_need_more_validation"
    elif category == "validation" and strategy_score >= 145 and validation_runs >= 1:
        action = "promote"
        next_state = "stable_candidate"
        reason = "validated_above_promotion_threshold"
    elif category == "validation" and strategy_score >= 130:
        action = "promote"
        next_state = "validating"
        reason = "high_confidence_validation_branch"
    elif recent_holds >= 3:
        action = "terminate"
        next_state = "terminated"
        reason = "too_many_consecutive_holds"
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
    agent_responses_path: str | Path | None = None,
) -> dict[str, Any]:
    state_snapshot = _read_json(Path(state_snapshot_path), {})
    proposal = _read_json(Path(proposal_path), {})
    branch_plan = _read_json(Path(branch_plan_path), {}) if branch_plan_path and Path(branch_plan_path).exists() else None
    agent_responses = _read_json(Path(agent_responses_path), {}) if agent_responses_path and Path(agent_responses_path).exists() else {}
    planner_agent = agent_responses.get("planner") or {}
    failure_analyst = agent_responses.get("failure_analyst") or {}
    brain = StrategyBrain()
    result = brain.build_plan(state_snapshot, proposal, branch_plan)
    if planner_agent.get("task_mix"):
        result["agent_task_mix"] = planner_agent.get("task_mix")
    if planner_agent.get("recommended_actions"):
        result["agent_recommended_actions"] = planner_agent.get("recommended_actions")
    if planner_agent.get("priority_families"):
        result.setdefault("memory_updates", {})["agent_priority_families"] = planner_agent.get("priority_families")
    if planner_agent.get("suppress_families"):
        result.setdefault("memory_updates", {})["agent_suppress_families"] = planner_agent.get("suppress_families")
    if planner_agent.get("hypothesis_cards"):
        result.setdefault("memory_updates", {})["agent_hypothesis_cards"] = planner_agent.get("hypothesis_cards")
    if planner_agent.get("challenger_queue"):
        result.setdefault("memory_updates", {})["agent_challenger_queue"] = planner_agent.get("challenger_queue")
    if failure_analyst.get("failure_patterns"):
        result.setdefault("memory_updates", {})["agent_failure_patterns"] = failure_analyst.get("failure_patterns")
    if failure_analyst.get("should_stop"):
        result.setdefault("memory_updates", {})["agent_should_stop"] = failure_analyst.get("should_stop")
    if failure_analyst.get("should_reroute"):
        result.setdefault("memory_updates", {})["agent_should_reroute"] = failure_analyst.get("should_reroute")
    payload = {
        "updated_at_utc": _iso_now(),
        "generated_from_state_snapshot": str(state_snapshot_path),
        "generated_from_proposal": str(proposal_path),
        "generated_from_branch_plan": str(branch_plan_path) if branch_plan_path else None,
        "generated_from_agent_responses": str(agent_responses_path) if agent_responses_path else None,
        **result,
    }
    _write_json(Path(output_path), payload)
    return payload


def _candidate_generation_observed_gain(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    output_dir = Path(str(payload.get("output_dir") or ""))
    observed: list[str] = []
    if not output_dir.exists():
        return False, observed
    candidate_pool_path = output_dir / "candidate_pool.json"
    graveyard_path = output_dir / "factor_graveyard.json"
    try:
        if candidate_pool_path.exists():
            candidate_rows = json.loads(candidate_pool_path.read_text(encoding="utf-8"))
            if candidate_rows:
                observed.append("candidate_survival_check")
        if graveyard_path.exists():
            graveyard_rows = json.loads(graveyard_path.read_text(encoding="utf-8"))
            if graveyard_rows:
                observed.append("search_space_reduced")
    except Exception:
        return False, []
    return ("candidate_survival_check" in observed), observed


def _candidate_generation_increment_check(payload: dict[str, Any]) -> dict[str, Any]:
    output_dir = Path(str(payload.get("output_dir") or ""))
    context = payload.get("candidate_generation_context") or {}
    candidate_id = context.get("candidate_id") or payload.get("branch_id")
    base_factors = list(context.get("base_factors") or [])
    result = {
        "candidate_id": candidate_id,
        "base_factors": base_factors,
        "generated_score": None,
        "best_parent_score": None,
        "incremental_delta": None,
        "improved_vs_parent": False,
    }
    if not output_dir.exists() or not candidate_id:
        return result
    scores_path = output_dir / "factor_scores.json"
    if not scores_path.exists():
        return result
    try:
        rows = json.loads(scores_path.read_text(encoding="utf-8"))
    except Exception:
        return result
    score_map = {row.get("factor_name"): row for row in rows if row.get("factor_name")}
    generated_score = float((score_map.get(candidate_id) or {}).get("score") or 0.0)
    parent_scores = [float((score_map.get(name) or {}).get("score") or 0.0) for name in base_factors if name in score_map]
    best_parent_score = max(parent_scores) if parent_scores else None
    result["generated_score"] = generated_score
    result["best_parent_score"] = best_parent_score
    if best_parent_score is not None:
        delta = generated_score - best_parent_score
        result["incremental_delta"] = round(delta, 6)
        result["improved_vs_parent"] = delta > 0.05
    return result


def _classify_research_outcome(*, status: str, has_gain: bool, knowledge_gain: list[str], summary_text: str, autonomy_policy: dict[str, Any]) -> dict[str, Any]:
    gains = {g for g in knowledge_gain if g}
    text = (summary_text or '').lower()
    failure_policy = ((autonomy_policy or {}).get('principles') or {}).get('failure_policy') or {}
    reward_high_value_failure = bool(failure_policy.get('reward_high_value_failure'))
    discourage_low_information_repetition = bool(failure_policy.get('discourage_low_information_repetition'))
    treat_boundary_discovery_as_progress = bool(failure_policy.get('treat_boundary_discovery_as_progress'))

    high_value_failure_tags = {
        'search_space_reduced', 'negative_result_recorded', 'boundary_confirmed', 'boundary_broken',
        'repeated_graveyard_confirmed', 'neutralization_diagnosis_requested', 'graveyard_diagnosis_requested',
    }
    repeated_low_info_tags = {'no_significant_information_gain', 'repeat_without_new_information', 'low_novelty_realized'}
    exploratory_progress_tags = {'new_branch_opened', 'probe_promising', 'uncertainty_reduced', 'partial_support', 'hypothesis_supported'}

    if status != 'finished':
        return {'outcome_class': 'execution_failure', 'epistemic_value': 'low', 'should_downweight': True}
    if has_gain:
        if gains & exploratory_progress_tags:
            return {'outcome_class': 'high_value_success', 'epistemic_value': 'high', 'should_downweight': False}
        return {'outcome_class': 'useful_success', 'epistemic_value': 'medium', 'should_downweight': False}
    if reward_high_value_failure and (gains & high_value_failure_tags):
        return {'outcome_class': 'high_value_failure', 'epistemic_value': 'high' if treat_boundary_discovery_as_progress else 'medium', 'should_downweight': False}
    if discourage_low_information_repetition and ((gains & repeated_low_info_tags) or ('no_significant_information_gain' in text)):
        return {'outcome_class': 'low_value_repeat', 'epistemic_value': 'low', 'should_downweight': True}
    return {'outcome_class': 'ordinary_failure', 'epistemic_value': 'medium', 'should_downweight': False}



def _representative_review_stage(task: dict[str, Any], payload: dict[str, Any]) -> str:
    validation_stage = str(payload.get('validation_stage') or '').strip()
    if validation_stage:
        return validation_stage
    worker_note = str(task.get('worker_note') or '')
    for token in ('recent_45d', 'recent_60d', 'recent_90d', 'recent_120d', 'expanding'):
        if token in worker_note:
            return token
    return str(payload.get('goal') or payload.get('diagnostic_type') or task.get('task_type') or 'unknown')



def _representative_review_rows(
    *,
    task: dict[str, Any],
    payload: dict[str, Any],
    branch_id: str,
    status: str,
    summary: str | None,
    error_text: str | None,
    has_gain: bool,
    outcome_meta: dict[str, Any],
) -> list[dict[str, Any]]:
    if task.get('category') != 'validation' and payload.get('diagnostic_type') != 'representative_candidate_competition_review':
        return []
    focus_rows = list(task.get('focus_candidates') or [])
    if not focus_rows:
        return []
    rows: list[dict[str, Any]] = []
    source_stage = _representative_review_stage(task, payload)
    focus_scope = sorted({row.get('candidate_name') for row in focus_rows if row.get('candidate_name')})
    for row in focus_rows:
        candidate_name = row.get('candidate_name')
        if not candidate_name:
            continue
        failure = row.get('failure_dossier') or {}
        rows.append({
            'updated_at_utc': _iso_now(),
            'branch_id': branch_id,
            'candidate_name': candidate_name,
            'focus_scope': focus_scope,
            'source_stage': source_stage,
            'worker_note': task.get('worker_note'),
            'task_type': task.get('task_type'),
            'status': status,
            'outcome_class': outcome_meta.get('outcome_class'),
            'epistemic_value': outcome_meta.get('epistemic_value'),
            'has_gain': has_gain,
            'summary': summary,
            'error_text': error_text,
            'candidate_status': row.get('candidate_status'),
            'quality_classification': row.get('quality_classification'),
            'raw_rank_ic_mean': row.get('raw_rank_ic_mean'),
            'neutralized_rank_ic_mean': row.get('neutralized_rank_ic_mean'),
            'retention_industry': row.get('retention_industry'),
            'failure_modes': list(failure.get('failure_modes') or []),
            'recommended_action': failure.get('recommended_action'),
            'regime_dependency': failure.get('regime_dependency'),
            'parent_delta_status': failure.get('parent_delta_status'),
            'parent_candidates': list(failure.get('parent_candidates') or []),
        })
    return rows


def update_research_memory_from_task_result(
    memory_path: str | Path,
    task: dict[str, Any],
    *,
    status: str,
    summary: str | None = None,
    error_text: str | None = None,
) -> dict[str, Any]:
    memory = load_or_initialize_research_memory(memory_path)
    autonomy_policy = _read_json(AUTONOMY_POLICY_PATH, {})
    memory["updated_at_utc"] = _iso_now()
    payload = task.get("payload") or {}
    strategy = payload.get("strategy") or {}
    branch_id = payload.get("branch_id") or strategy.get("branch_id") or task.get("fingerprint") or task.get("task_id")
    focus_candidates = payload.get("focus_factors") or strategy.get("focus_candidates") or []
    candidate_generation_context = payload.get("candidate_generation_context") or {}
    if isinstance(focus_candidates, str):
        focus_candidates = [focus_candidates]
    knowledge_gain = [g for g in (payload.get("knowledge_gain") or payload.get("expected_information_gain") or []) if g]
    summary_text = (summary or "") + " " + (error_text or "")
    has_gain = any(g and g != "no_significant_information_gain" for g in knowledge_gain) or ("knowledge_gain=" in summary_text and "no_significant_information_gain" not in summary_text)
    if payload.get("source") == "candidate_generation":
        has_gain, observed_gain = _candidate_generation_observed_gain(payload)
        if observed_gain:
            knowledge_gain = observed_gain
        elif not has_gain:
            knowledge_gain = []
    outcome_meta = _classify_research_outcome(status=status, has_gain=has_gain, knowledge_gain=knowledge_gain, summary_text=summary_text, autonomy_policy=autonomy_policy)
    branch_lifecycle = dict(memory.get("branch_lifecycle") or {})
    branch_state = dict(branch_lifecycle.get(branch_id, {}))
    branch_state.setdefault("history", [])
    branch_state["updated_at_utc"] = _iso_now()
    branch_state["last_task_type"] = task.get("task_type")
    branch_state["last_status"] = status
    branch_state["goal"] = payload.get("goal") or strategy.get("goal")
    branch_state["hypothesis"] = payload.get("hypothesis") or strategy.get("hypothesis")
    branch_state["validation_runs"] = int(branch_state.get("validation_runs") or 0) + (1 if task.get("task_type") == "diagnostic" or (payload.get("goal") or "").startswith("validate") else 0)
    execution_feedback = list(memory.get("execution_feedback") or [])
    if status != "finished":
        branch_state["no_gain_runs"] = int(branch_state.get("no_gain_runs") or 0) + 1
        branch_state["state"] = branch_state.get("state") or "failed"
        branch_state["last_action"] = "demote"
        branch_state["history"].append({"updated_at_utc": _iso_now(), "state": branch_state.get("state"), "last_action": "demote", "reason": "task_failed"})
        execution_feedback.append({
            "updated_at_utc": _iso_now(),
            "branch_id": branch_id,
            "task_type": task.get("task_type"),
            "status": status,
            "has_gain": False,
            "summary": summary,
            "error_text": error_text,
            "focus_candidates": focus_candidates,
            "outcome_class": outcome_meta.get("outcome_class"),
            "epistemic_value": outcome_meta.get("epistemic_value"),
        })
    else:
        if has_gain:
            branch_state["no_gain_runs"] = 0
            if payload.get("promote_if"):
                branch_state["last_action"] = "promote"
                branch_state["state"] = branch_state.get("state") or "validating"
            else:
                branch_state["last_action"] = "hold"
                branch_state["state"] = branch_state.get("state") or "exploring"
            branch_state["history"].append({"updated_at_utc": _iso_now(), "state": branch_state.get("state"), "last_action": branch_state.get("last_action"), "reason": "knowledge_gain_detected"})
        else:
            branch_state["no_gain_runs"] = int(branch_state.get("no_gain_runs") or 0) + 1
            branch_state["last_action"] = "hold"
            branch_state["state"] = branch_state.get("state") or "saturated"
            branch_state["history"].append({"updated_at_utc": _iso_now(), "state": branch_state.get("state"), "last_action": "hold", "reason": "no_significant_information_gain"})
        execution_feedback.append({
            "updated_at_utc": _iso_now(),
            "branch_id": branch_id,
            "task_type": task.get("task_type"),
            "status": status,
            "has_gain": has_gain,
            "summary": summary,
            "error_text": error_text,
            "focus_candidates": focus_candidates,
            "knowledge_gain": knowledge_gain,
            "goal": payload.get("goal") or strategy.get("goal"),
            "hypothesis": payload.get("hypothesis") or strategy.get("hypothesis"),
            "outcome_class": outcome_meta.get("outcome_class"),
            "epistemic_value": outcome_meta.get("epistemic_value"),
        })
    branch_state["outcome_class"] = outcome_meta.get("outcome_class")
    branch_state["epistemic_value"] = outcome_meta.get("epistemic_value")
    branch_state["history"] = branch_state["history"][-20:]
    branch_lifecycle[branch_id] = branch_state
    memory["branch_lifecycle"] = branch_lifecycle
    memory["execution_feedback"] = execution_feedback[-50:]

    fallback_history = list(memory.get("fallback_history") or [])
    if str(branch_id).startswith("fallback_"):
        fallback_history.append({
            "updated_at_utc": _iso_now(),
            "branch_id": branch_id,
            "status": status,
            "has_gain": has_gain,
            "summary": summary,
            "error_text": error_text,
            "task_type": task.get("task_type"),
            "focus_candidates": focus_candidates,
            "knowledge_gain": knowledge_gain,
            "goal": payload.get("goal") or strategy.get("goal"),
        })
    memory["fallback_history"] = fallback_history[-30:]

    generated_candidate_outcomes = list(memory.get("generated_candidate_outcomes") or [])
    if (payload.get("source") == "candidate_generation") or candidate_generation_context:
        increment_check = _candidate_generation_increment_check(payload)
        generated_candidate_outcomes.append({
            "updated_at_utc": _iso_now(),
            "candidate_id": candidate_generation_context.get("candidate_id") or branch_id,
            "operator": candidate_generation_context.get("operator"),
            "base_factors": list(candidate_generation_context.get("base_factors") or []),
            "source": candidate_generation_context.get("source") or payload.get("source"),
            "target_family": candidate_generation_context.get("target_family"),
            "expected_information_gain": list(candidate_generation_context.get("expected_information_gain") or payload.get("expected_information_gain") or []),
            "exploration_pool": candidate_generation_context.get("exploration_pool") or payload.get("exploration_pool"),
            "mechanism_novelty_class": candidate_generation_context.get("mechanism_novelty_class") or payload.get("mechanism_novelty_class"),
            "question_card_id": candidate_generation_context.get("question_card_id"),
            "question_type": candidate_generation_context.get("question_type"),
            "outcome_class": outcome_meta.get("outcome_class"),
            "epistemic_value": outcome_meta.get("epistemic_value"),
            "has_gain": has_gain,
            "task_type": task.get("task_type"),
            "increment_check": increment_check,
        })
    memory["generated_candidate_outcomes"] = generated_candidate_outcomes[-100:]

    representative_candidate_reviews = list(memory.get("representative_candidate_reviews") or [])
    representative_candidate_reviews.extend(
        _representative_review_rows(
            task=task,
            payload=payload,
            branch_id=branch_id,
            status=status,
            summary=summary,
            error_text=error_text,
            has_gain=has_gain,
            outcome_meta=outcome_meta,
        )
    )
    memory["representative_candidate_reviews"] = representative_candidate_reviews[-120:]

    candidate_lifecycle = dict(memory.get("candidate_lifecycle") or {})
    stable_candidates = set(memory.get("stable_candidates") or [])
    suppressed_candidates = set(memory.get("suppressed_candidates") or [])
    explicit_stable_validation = _is_explicit_stable_validation(
        task=task,
        payload=payload,
        strategy=strategy,
        knowledge_gain=knowledge_gain,
        summary_text=summary_text,
    )
    for name in focus_candidates:
        current = dict(candidate_lifecycle.get(name, {}))
        current.setdefault("history", [])
        if status != "finished":
            next_state = "provisional"
            action = "demote"
        elif name in suppressed_candidates:
            next_state = "provisional"
            action = "demote"
        elif explicit_stable_validation and has_gain and name in stable_candidates:
            next_state = "stable_candidate"
            action = "promote"
        elif has_gain:
            next_state = "validating"
            action = "hold"
        else:
            next_state = "provisional"
            action = "demote"
        current.update({
            "candidate_name": name,
            "next_state": next_state,
            "action": action,
            "source_branch_id": branch_id,
            "updated_at_utc": _iso_now(),
        })
        current["history"].append({"updated_at_utc": _iso_now(), "next_state": next_state, "action": action, "source_branch_id": branch_id})
        current["history"] = current["history"][-20:]
        candidate_lifecycle[name] = current
    memory["candidate_lifecycle"] = _reconcile_candidate_lifecycle(
        candidate_lifecycle,
        preferred_candidates=stable_candidates,
        suppressed_candidates=suppressed_candidates,
    )

    convergence_policy = memory.get("convergence_policy") or {}
    archive_after_no_gain = int(convergence_policy.get("archive_after_no_gain_runs") or 2)
    if int(branch_state.get("no_gain_runs") or 0) >= archive_after_no_gain:
        branch_state["state"] = "archived"
        branch_state["last_action"] = "archive"
        archived = list(memory.get("archived_branches") or [])
        if branch_id not in archived:
            archived.append(branch_id)
        memory["archived_branches"] = archived[-100:]
        branch_lifecycle[branch_id] = branch_state
        memory["branch_lifecycle"] = branch_lifecycle

    memory = _compact_memory_state(memory)
    memory = _compact_memory_state(memory)
    _write_json(Path(memory_path), memory)
    return memory


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
    memory = load_or_initialize_research_memory(memory_path)
    candidate_generation_history = list(memory.get("candidate_generation_history") or [])
    for task in approved_tasks:
        fingerprint = task.get("fingerprint")
        validated_task = validated_by_fingerprint.get(fingerprint, task)
        payload = dict(validated_task.get("payload") or task.get("payload") or {})
        reasons = payload.get("reasons") or []
        worker_note = (validated_task.get("worker_note") or task.get("worker_note") or "")
        repeat_blocked = recently_finished_same_fingerprint(
            store,
            fingerprint,
            cooldown_minutes=30 if "recovery_step" in reasons else None,
            task_type=validated_task.get("task_type") or task.get("task_type"),
            payload=payload,
            worker_note=worker_note,
        ) if fingerprint else False
        if repeat_blocked:
            skipped.append({"fingerprint": fingerprint, "reason": "recently_finished_same_fingerprint"})
            continue
        payload["strategy"] = task.get("strategy_meta") or {}
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
        if payload.get("source") == "candidate_generation":
            candidate_generation_history.append({
                "updated_at_utc": _iso_now(),
                "candidate_id": ((payload.get("candidate_generation_context") or {}).get("candidate_id") or payload.get("branch_id")),
                "operator": ((payload.get("candidate_generation_context") or {}).get("operator")),
                "base_factors": list(((payload.get("candidate_generation_context") or {}).get("base_factors") or [])),
                "source": ((payload.get("candidate_generation_context") or {}).get("source") or payload.get("source")),
                "target_family": ((payload.get("candidate_generation_context") or {}).get("target_family")),
                "cheap_screen": dict(((payload.get("candidate_generation_context") or {}).get("cheap_screen") or {})),
                "exploration_pool": ((payload.get("candidate_generation_context") or {}).get("exploration_pool") or payload.get("exploration_pool")),
                "mechanism_novelty_class": ((payload.get("candidate_generation_context") or {}).get("mechanism_novelty_class") or payload.get("mechanism_novelty_class")),
                "question_card_id": ((payload.get("candidate_generation_context") or {}).get("question_card_id")),
                "question_type": ((payload.get("candidate_generation_context") or {}).get("question_type")),
                "injected": True,
                "task_id": task_id,
            })

    memory["updated_at_utc"] = _iso_now()
    updates = strategy_plan.get("memory_updates") or {}
    autonomy_policy = strategy_plan.get("autonomy_policy") or _read_json(AUTONOMY_POLICY_PATH, {})
    coding_policy = strategy_plan.get("coding_policy") or _read_json(CODING_POLICY_PATH, {})
    if updates.get("stable_candidates") is not None:
        memory["stable_candidates"] = updates.get("stable_candidates")
    if updates.get("suppressed_candidates") is not None:
        memory["suppressed_candidates"] = updates.get("suppressed_candidates")
    if updates.get("high_value_open_questions"):
        memory["high_value_open_questions"] = updates.get("high_value_open_questions")
    memory["agent_control"] = {
        "updated_at_utc": _iso_now(),
        "planner_mode": strategy_plan.get("agent_task_mix") and ((strategy_plan.get("generated_from_agent_responses") and (_read_json(Path(strategy_plan.get("generated_from_agent_responses")), {}).get("planner") or {}).get("mode")) or None),
        "task_mix": strategy_plan.get("agent_task_mix") or {},
        "recommended_actions": strategy_plan.get("agent_recommended_actions") or [],
        "priority_families": updates.get("agent_priority_families") or [],
        "suppress_families": updates.get("agent_suppress_families") or [],
        "hypothesis_cards": updates.get("agent_hypothesis_cards") or [],
        "challenger_queue": updates.get("agent_challenger_queue") or [],
        "failure_patterns": updates.get("agent_failure_patterns") or [],
        "should_stop": updates.get("agent_should_stop") or [],
        "should_reroute": updates.get("agent_should_reroute") or [],
    }
    convergence_policy = (updates.get("convergence_policy") or strategy_plan.get("convergence_policy") or {
        "archive_after_no_gain_runs": 2,
        "terminate_after_duplicate_pressure": 4,
        "promote_after_validation_score": 145,
        "demote_below_strategy_score": 90,
        "terminate_after_hold_count": 3,
    })
    memory["convergence_policy"] = convergence_policy
    candidate_lifecycle = dict(memory.get("candidate_lifecycle") or {})
    for name, candidate_update in (updates.get("candidate_lifecycle_updates") or {}).items():
        current = dict(candidate_lifecycle.get(name, {}))
        history = list(current.get("history") or [])
        history.append({
            "updated_at_utc": _iso_now(),
            "next_state": candidate_update.get("next_state"),
            "action": candidate_update.get("action"),
            "source_branch_id": candidate_update.get("source_branch_id"),
        })
        current.update(candidate_update)
        current["updated_at_utc"] = _iso_now()
        current["history"] = history[-20:]
        candidate_lifecycle[name] = current
    memory["candidate_lifecycle"] = _reconcile_candidate_lifecycle(
        candidate_lifecycle,
        preferred_candidates=set(memory.get("stable_candidates") or []),
        suppressed_candidates=set(memory.get("suppressed_candidates") or []),
    )
    branch_lifecycle = dict(memory.get("branch_lifecycle") or {})
    for branch_id, branch_update in (updates.get("branch_lifecycle_updates") or {}).items():
        current = dict(branch_lifecycle.get(branch_id, {}))
        history = list(current.get("history") or [])
        history.append({
            "updated_at_utc": _iso_now(),
            "state": branch_update.get("state"),
            "last_action": branch_update.get("last_action"),
            "reason": branch_update.get("reason"),
        })
        current.update(branch_update)
        current["history"] = history[-20:]
        current["validation_runs"] = int(current.get("validation_runs") or 0) + (1 if branch_update.get("state") in {"validating", "stable_candidate"} else 0)
        current["no_gain_runs"] = int(current.get("no_gain_runs") or 0) + (1 if branch_update.get("last_action") in {"demote", "hold"} else 0)
        branch_lifecycle[branch_id] = current
    memory["branch_lifecycle"] = branch_lifecycle
    archived_branches = list(memory.get("archived_branches") or [])
    archived_branches.extend(updates.get("archived_branches") or [])
    memory["archived_branches"] = archived_branches[-100:]
    branch_history = list(memory.get("branch_history") or [])
    branch_history.extend(updates.get("branch_history_append") or [])
    memory["branch_history"] = branch_history[-100:]
    memory["candidate_generation_history"] = candidate_generation_history[-120:]
    strategy_runs = list(memory.get("strategy_runs") or [])
    strategy_runs.append({
        "updated_at_utc": _iso_now(),
        "approved_count": len(approved_tasks),
        "injected_count": len(injected),
        "branch_actions": strategy_plan.get("branch_actions") or [],
    })
    memory["strategy_runs"] = strategy_runs[-50:]
    execution_feedback = list(memory.get("execution_feedback") or [])[-60:]
    outcome_mix = {}
    for row in execution_feedback:
        key = row.get("outcome_class") or "unknown"
        outcome_mix[key] = int(outcome_mix.get(key) or 0) + 1
    principles = (autonomy_policy.get("principles") or {})
    failure_policy = principles.get("failure_policy") or {}
    memory["autonomy_profile"] = {
        "policy_name": autonomy_policy.get("name"),
        "unit_of_research": principles.get("unit_of_research"),
        "preferred_objectives": list(principles.get("objective") or []),
        "budget_policy": dict(autonomy_policy.get("budget_policy") or {}),
        "learning_bias": {
            "reward_high_value_failure": bool(failure_policy.get("reward_high_value_failure")),
            "discourage_low_information_repetition": bool(failure_policy.get("discourage_low_information_repetition")),
            "treat_boundary_discovery_as_progress": bool(failure_policy.get("treat_boundary_discovery_as_progress")),
        },
        "observed_outcome_mix": outcome_mix,
        "last_research_style_refresh_at_utc": _iso_now(),
    }
    memory["coding_profile"] = {
        "policy_name": coding_policy.get("name"),
        "shared_intermediates_first": bool((coding_policy.get("principles") or {}).get("shared_intermediates_first")),
        "avoid_recomputing_factor_values": bool((coding_policy.get("principles") or {}).get("avoid_recomputing_factor_values")),
        "prefer_factor_matrix_or_cache": bool((coding_policy.get("principles") or {}).get("prefer_factor_matrix_or_cache")),
        "performance_rules": dict(coding_policy.get("performance_rules") or {}),
        "engineering_rules": dict(coding_policy.get("engineering_rules") or {}),
        "last_coding_style_refresh_at_utc": _iso_now(),
    }

    if strategy_plan.get("branch_actions"):
        patterns = list(memory.get("repeated_failure_patterns") or [])
        for action in strategy_plan.get("branch_actions") or []:
            patterns.append(action)
        memory["repeated_failure_patterns"] = _compact_branch_action_rows(
            patterns[-80:],
            key_fields=("branch_id", "action", "next_state", "reason"),
        )[-50:]
    memory = _compact_memory_state(memory)
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
