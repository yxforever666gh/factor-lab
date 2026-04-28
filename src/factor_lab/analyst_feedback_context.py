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


def build_analyst_feedback_context(base_dir: str | Path | None = None) -> dict[str, Any]:
    base = Path(base_dir) if base_dir else ARTIFACTS
    strategy_plan = _read_json(base / "strategy_plan.json", {})
    injected = _read_json(base / "research_planner_injected.json", {})
    research_memory = _read_json(base / "research_memory.json", {})
    llm_feedback = _read_json(base / "llm_plan_feedback.json", {})
    llm_retrospective = _read_json(base / "llm_retrospective.json", {})
    llm_status = _read_json(base / "llm_status.json", {})
    research_flow_state = _read_json(base / "research_flow_state.json", {})
    research_learning = _read_json(base / "research_learning.json", {})

    strategy_runs = list(research_memory.get("strategy_runs") or [])
    branch_history = list(research_memory.get("branch_history") or [])
    branch_lifecycle = research_memory.get("branch_lifecycle") or {}
    candidate_lifecycle = research_memory.get("candidate_lifecycle") or {}

    recent_strategy_runs = strategy_runs[-5:]
    recent_branch_actions = branch_history[-10:]
    active_branches = [
        {
            "branch_id": branch_id,
            **state,
        }
        for branch_id, state in branch_lifecycle.items()
        if state.get("state") in {"validating", "exploring", "stable_candidate"}
    ]

    recovery_events = [
        row for row in recent_strategy_runs if int(row.get("approved_count") or 0) > 0 and any(
            (action.get("branch_id") or "").startswith("fallback_") for action in (row.get("branch_actions") or [])
        )
    ]
    recovery_history = list(research_memory.get("fallback_history") or [])

    recovery_no_gain_tail = [row for row in recovery_history[-10:] if not row.get("has_gain")]
    analyst_learning_loop = {
        "recent_strategy_runs": recent_strategy_runs,
        "recent_branch_actions": recent_branch_actions,
        "active_branch_count": len(active_branches),
        "recovery_trigger_count_last_5": len(recovery_events),
        "recovery_no_gain_count_last_10": len(recovery_no_gain_tail),
        "last_injected_count": int(injected.get("injected_count") or 0),
        "last_llm_status": llm_status.get("status"),
    }

    return {
        "strategy_plan_summary": {
            "summary": strategy_plan.get("summary"),
            "budget": strategy_plan.get("budget") or {},
            "budget_usage": strategy_plan.get("budget_usage") or {},
            "approved_count": len(strategy_plan.get("approved_tasks") or []),
            "rejected_count": len(strategy_plan.get("rejected_tasks") or []),
        },
        "injection_summary": {
            "injected_count": int(injected.get("injected_count") or 0),
            "skipped_count": len(injected.get("skipped_tasks") or []),
            "injected_tasks": injected.get("injected_tasks") or [],
            "candidate_lifecycle_updates": injected.get("candidate_lifecycle_updates") or {},
            "branch_lifecycle_updates": injected.get("branch_lifecycle_updates") or {},
            "archived_branches": injected.get("archived_branches") or [],
        },
        "research_memory_tail": {
            "high_value_open_questions": list(research_memory.get("high_value_open_questions") or [])[:5],
            "strategy_runs_tail": recent_strategy_runs,
            "branch_history_tail": recent_branch_actions,
            "active_branches": active_branches[:10],
            "candidate_lifecycle_sample": list(candidate_lifecycle.values())[:10],
            "recovery_history_tail": recovery_history[-10:],
        },
        "llm_execution_feedback": {
            "status": llm_status.get("status"),
            "knowledge_gain": llm_status.get("knowledge_gain") or [],
            "feedback_summary": llm_status.get("feedback_summary") or llm_feedback.get("batch_summary") or [],
            "retrospective": llm_retrospective,
        },
        "analyst_learning_loop": analyst_learning_loop,
        "research_flow_state": research_flow_state,
        "research_learning": research_learning,
    }
