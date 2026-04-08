from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PLANNER_AGENT_SCHEMA_VERSION = "factor_lab.planner_agent_brief.v1"
FAILURE_ANALYST_SCHEMA_VERSION = "factor_lab.failure_analyst_brief.v1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: str | Path, payload: dict[str, Any]) -> dict[str, Any]:
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload



def _candidate_hypothesis_cards(snapshot: dict[str, Any], limit: int = 6) -> list[dict[str, Any]]:
    rows = ((snapshot.get("promotion_scorecard") or {}).get("rows") or [])
    cards: list[dict[str, Any]] = []
    for row in rows[:limit]:
        quality_scores = row.get("quality_scores") or {}
        hard_flags = row.get("quality_hard_flags") or {}
        falsifiers = [key for key, value in hard_flags.items() if value]
        if not falsifiers:
            if row.get("quality_classification") == "duplicate-suppress":
                falsifiers = ["duplicate_risk"]
            elif row.get("quality_classification") in {"needs-validation", "validate-only"}:
                falsifiers = ["insufficient_window_evidence"]
        cross_window = float(quality_scores.get("cross_window_robustness") or 0.0)
        if cross_window >= 20:
            target_window = "medium_horizon"
        elif cross_window >= 10:
            target_window = "recent_extension"
        else:
            target_window = "short_window_recheck"
        cards.append(
            {
                "candidate_name": row.get("factor_name"),
                "family": row.get("family") or row.get("quality_classification"),
                "mechanism_note": row.get("quality_summary") or row.get("decision_summary") or row.get("decision_label"),
                "incremental_value": quality_scores.get("incremental_value"),
                "target_window": target_window,
                "invalidation_signals": falsifiers[:4],
                "incremental_value_thesis": f"相对现有 frontier 提供 {quality_scores.get('incremental_value', 0)} 分级别的新增信息。",
            }
        )
    return [row for row in cards if row.get("candidate_name")]


def build_planner_agent_brief(
    snapshot: dict[str, Any],
    candidate_pool: dict[str, Any],
    branch_plan: dict[str, Any],
    state_snapshot: dict[str, Any],
    strategy_plan: dict[str, Any],
    output_path: str | Path,
) -> dict[str, Any]:
    payload = {
        "schema_version": PLANNER_AGENT_SCHEMA_VERSION,
        "generated_at_utc": _iso_now(),
        "agent_role": "planner_agent",
        "mission": "决定下一轮研究该优先验证什么、探索什么、停止什么，并输出结构化 task mix / family priority / suppress list。",
        "inputs": {
            "latest_run": snapshot.get("latest_run") or {},
            "stable_candidates": snapshot.get("stable_candidates") or [],
            "latest_candidates": snapshot.get("latest_candidates") or [],
            "latest_graveyard": snapshot.get("latest_graveyard") or [],
            "queue_budget": snapshot.get("queue_budget") or {},
            "failure_state": snapshot.get("failure_state") or {},
            "exploration_state": snapshot.get("exploration_state") or {},
            "research_flow_state": snapshot.get("research_flow_state") or {},
            "research_learning": snapshot.get("research_learning") or {},
            "knowledge_gain_counter": snapshot.get("knowledge_gain_counter") or {},
            "candidate_pool_tasks": candidate_pool.get("tasks") or [],
            "candidate_pool_suppressed": candidate_pool.get("suppressed_tasks") or [],
            "branch_selected_families": branch_plan.get("selected_families") or [],
            "open_questions": state_snapshot.get("open_questions") or [],
            "current_strategy_summary": strategy_plan.get("summary") or {},
            "candidate_hypothesis_cards": _candidate_hypothesis_cards(snapshot),
        },
        "required_output_schema": {
            "mode": "explore|validate|recover|converge|harvest_exposure",
            "task_mix": {"baseline": "int", "validation": "int", "exploration": "int"},
            "priority_families": ["family-name"],
            "suppress_families": ["family-name"],
            "recommended_actions": [
                {
                    "type": "validation|exploration|diagnostic|suppress|promote",
                    "target": "candidate/family/branch identifier",
                    "reason": "why now",
                }
            ],
            "hypothesis_cards": [
                {
                    "candidate_name": "candidate identifier",
                    "mechanism_note": "why this might work",
                    "target_window": "short_window_recheck|recent_extension|medium_horizon",
                    "invalidation_signals": ["flag"],
                    "incremental_value_thesis": "why it adds something new",
                }
            ],
            "challenger_queue": ["candidate identifier"],
            "confidence_score": "0..1",
            "rationale_markdown": "short markdown summary",
        },
        "constraints": [
            "must_ground_on_snapshot",
            "must_not_override_numeric_metrics",
            "prefer fewer higher-value tasks over broad low-signal expansion",
            "if recovery is active, explain how to return to mainline research",
        ],
    }
    return _write_json(output_path, payload)



def build_failure_analyst_brief(
    snapshot: dict[str, Any],
    state_snapshot: dict[str, Any],
    llm_diagnostics: dict[str, Any],
    output_path: str | Path,
) -> dict[str, Any]:
    recent_tasks = snapshot.get("recent_research_tasks") or []
    failed_or_risky = [
        row for row in recent_tasks
        if row.get("status") in {"failed", "quarantined"}
        or "budget_guard" in ((row.get("worker_note") or "") + " " + (row.get("last_error") or ""))
    ]
    payload = {
        "schema_version": FAILURE_ANALYST_SCHEMA_VERSION,
        "generated_at_utc": _iso_now(),
        "agent_role": "failure_analyst",
        "mission": "归纳最近研究失败模式，判断哪些路线应停止、诊断或继续，并产出可回流到 planner 的结构化失败知识。",
        "inputs": {
            "failure_state": snapshot.get("failure_state") or {},
            "research_flow_state": snapshot.get("research_flow_state") or {},
            "knowledge_gain_counter": snapshot.get("knowledge_gain_counter") or {},
            "recent_failed_or_risky_tasks": failed_or_risky[:20],
            "open_questions": state_snapshot.get("open_questions") or [],
            "llm_diagnostics": llm_diagnostics or {},
            "latest_graveyard": snapshot.get("latest_graveyard") or [],
            "research_learning": snapshot.get("research_learning") or {},
            "analyst_feedback_context": snapshot.get("analyst_feedback_context") or {},
        },
        "required_output_schema": {
            "failure_patterns": [
                {
                    "pattern_id": "short-id",
                    "scope": "candidate|family|branch|workflow",
                    "symptom": "what is failing",
                    "likely_cause": "best guess grounded on evidence",
                    "recommended_action": "stop|diagnose|retry|deprioritize|reroute",
                    "confidence_score": "0..1",
                }
            ],
            "should_stop": ["route identifiers"],
            "should_probe": ["diagnostic targets"],
            "should_reroute": ["family/branch identifiers"],
            "summary_markdown": "short markdown summary",
        },
        "constraints": [
            "must_reference concrete evidence from snapshot",
            "must separate deterministic errors from research dead ends",
            "must prefer high-information-gain diagnostics over blind retries",
        ],
    }
    return _write_json(output_path, payload)
