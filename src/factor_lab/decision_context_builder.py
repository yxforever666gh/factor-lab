from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from typing import Any


def _stable_hash(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _trim_list(rows: list[Any], limit: int) -> list[Any]:
    return list(rows or [])[:limit]


def build_planner_decision_context(brief: dict[str, Any]) -> dict[str, Any]:
    inputs = deepcopy((brief.get("inputs") or {}))
    context_inputs = {
        "latest_run": inputs.get("latest_run") or {},
        "stable_candidates": _trim_list(inputs.get("stable_candidates") or [], 8),
        "latest_candidates": _trim_list(inputs.get("latest_candidates") or [], 8),
        "latest_graveyard": _trim_list(inputs.get("latest_graveyard") or [], 8),
        "queue_budget": inputs.get("queue_budget") or {},
        "failure_state": inputs.get("failure_state") or {},
        "exploration_state": inputs.get("exploration_state") or {},
        "research_flow_state": inputs.get("research_flow_state") or {},
        "knowledge_gain_counter": inputs.get("knowledge_gain_counter") or {},
        "candidate_pool_tasks": _trim_list(inputs.get("candidate_pool_tasks") or [], 10),
        "candidate_pool_suppressed": _trim_list(inputs.get("candidate_pool_suppressed") or [], 10),
        "branch_selected_families": _trim_list(inputs.get("branch_selected_families") or [], 8),
        "open_questions": _trim_list(inputs.get("open_questions") or [], 8),
        "candidate_hypothesis_cards": _trim_list(inputs.get("candidate_hypothesis_cards") or [], 8),
    }
    summary = {
        "stable_candidate_count": len(inputs.get("stable_candidates") or []),
        "graveyard_count": len(inputs.get("latest_graveyard") or []),
        "candidate_pool_task_count": len(inputs.get("candidate_pool_tasks") or []),
        "candidate_pool_suppressed_count": len(inputs.get("candidate_pool_suppressed") or []),
        "open_question_count": len(inputs.get("open_questions") or []),
        "hypothesis_card_count": len(inputs.get("candidate_hypothesis_cards") or []),
    }
    payload = {
        "schema_version": "factor_lab.planner_decision_context.v1",
        "decision_type": "planner",
        "agent_role": brief.get("agent_role") or "planner_agent",
        "mission": brief.get("mission") or "planner decision context",
        "constraints": list(brief.get("constraints") or []),
        "summary": summary,
        "inputs": context_inputs,
    }
    payload["context_id"] = _stable_hash(payload)
    return payload



def build_failure_decision_context(brief: dict[str, Any]) -> dict[str, Any]:
    inputs = deepcopy((brief.get("inputs") or {}))
    context_inputs = {
        "failure_state": inputs.get("failure_state") or {},
        "research_flow_state": inputs.get("research_flow_state") or {},
        "knowledge_gain_counter": inputs.get("knowledge_gain_counter") or {},
        "recent_failed_or_risky_tasks": _trim_list(inputs.get("recent_failed_or_risky_tasks") or [], 12),
        "open_questions": _trim_list(inputs.get("open_questions") or [], 8),
        "llm_diagnostics": inputs.get("llm_diagnostics") or {},
        "latest_graveyard": _trim_list(inputs.get("latest_graveyard") or [], 8),
        "research_learning": inputs.get("research_learning") or {},
        "analyst_feedback_context": inputs.get("analyst_feedback_context") or {},
    }
    summary = {
        "recent_failed_or_risky_task_count": len(inputs.get("recent_failed_or_risky_tasks") or []),
        "graveyard_count": len(inputs.get("latest_graveyard") or []),
        "open_question_count": len(inputs.get("open_questions") or []),
        "warning_count": len(((inputs.get("llm_diagnostics") or {}).get("warnings") or [])),
    }
    payload = {
        "schema_version": "factor_lab.failure_decision_context.v1",
        "decision_type": "failure_analyst",
        "agent_role": brief.get("agent_role") or "failure_analyst",
        "mission": brief.get("mission") or "failure analyst decision context",
        "constraints": list(brief.get("constraints") or []),
        "summary": summary,
        "inputs": context_inputs,
    }
    payload["context_id"] = _stable_hash(payload)
    return payload
