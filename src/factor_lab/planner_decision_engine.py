from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _top_candidate_names(stable_candidates: list[dict[str, Any]], limit: int = 3) -> list[str]:
    names: list[str] = []
    for row in stable_candidates:
        name = row.get("factor_name") if isinstance(row, dict) else row
        if name:
            names.append(name)
    return names[:limit]



def build_planner_response(context: dict[str, Any], *, source_label: str = "heuristic") -> dict[str, Any]:
    inputs = context.get("inputs") or {}
    flow = inputs.get("research_flow_state") or {}
    failure = inputs.get("failure_state") or {}
    queue_budget = inputs.get("queue_budget") or {}
    learning = inputs.get("research_learning") or {}
    stable_candidates = inputs.get("stable_candidates") or []
    latest_graveyard = inputs.get("latest_graveyard") or []
    selected_families = list(inputs.get("branch_selected_families") or [])
    knowledge_gain_counter = inputs.get("knowledge_gain_counter") or {}
    open_questions = inputs.get("open_questions") or []
    candidate_pool_tasks = inputs.get("candidate_pool_tasks") or []
    candidate_pool_suppressed = inputs.get("candidate_pool_suppressed") or []
    candidate_hypothesis_cards = inputs.get("candidate_hypothesis_cards") or []
    repair_feedback = inputs.get("repair_feedback") or {}

    stable_names = _top_candidate_names(stable_candidates, limit=3)
    stable_count = len(stable_candidates)
    recovering = flow.get("state") in {"recovering", "exhausted"}
    queue_validation = int(queue_budget.get("validation", 0) or 0)
    queue_exploration = int(queue_budget.get("exploration", 0) or 0)
    recovery_active_branch_count = int(flow.get("recovery_active_branch_count") or 0)
    no_gain_count = int(knowledge_gain_counter.get("no_significant_information_gain") or 0)
    graveyard_gain_count = int(knowledge_gain_counter.get("exploration_graveyard_identified") or 0)

    mode = "validate"
    task_mix = {"baseline": 1, "validation": 3, "exploration": 1}
    suppress_families: list[str] = []
    priority_families: list[str] = []
    recommended_actions: list[dict[str, Any]] = []
    rationale_bits: list[str] = []

    if recovering or recovery_active_branch_count > 0:
        mode = "recover"
        task_mix = {"baseline": 1, "validation": 2, "exploration": 0}
        rationale_bits.append("研究流仍在 recovery，先压探索，优先恢复主线。")
    elif failure.get("cooldown_active"):
        mode = "converge"
        task_mix = {"baseline": 1, "validation": 4, "exploration": 0}
        rationale_bits.append("失败冷却激活，探索预算清零，收敛优先。")
    elif stable_count >= 5 and queue_validation <= 1:
        mode = "converge"
        task_mix = {"baseline": 1, "validation": 4, "exploration": 1}
        rationale_bits.append("稳定候选充足且验证积压偏低，进入收敛期。")
    elif queue_exploration > queue_validation or no_gain_count >= 2:
        mode = "validate"
        task_mix = {"baseline": 1, "validation": 3, "exploration": 0}
        rationale_bits.append("探索空转/偏多，重新把算力拉回验证。")
    else:
        rationale_bits.append("当前处于验证主导、保留少量扩展的平衡状态。")

    if int(repair_feedback.get("active_incident_count") or 0) > 0:
        mode = "validate" if mode == "explore" else mode
        task_mix["exploration"] = 0 if int(repair_feedback.get("active_incident_count") or 0) >= 2 else min(task_mix.get("exploration", 0), 1)
        rationale_bits.append(f"repair layer 当前有 {repair_feedback.get('active_incident_count')} 个 active incident，压低 exploration。")
    if repair_feedback.get("route_unhealthy"):
        suppress_families.append("network_sensitive_exploration")
        rationale_bits.append("当前 route 不健康，压制 network-sensitive exploration。")
    for family in (repair_feedback.get("blocked_families") or []):
        if family:
            suppress_families.append(str(family))

    family_rows = (learning.get("families") or {})
    for family, row in family_rows.items():
        if row.get("cooldown_active"):
            suppress_families.append(family)
            continue
        if row.get("recommended_action") in {"validate", "promote", "continue", "upweight"}:
            priority_families.append(family)
        elif row.get("recommended_action") in {"downweight", "cooldown"}:
            suppress_families.append(family)

    for family in selected_families:
        if family not in priority_families:
            priority_families.append(family)

    if latest_graveyard or graveyard_gain_count > 0:
        priority_families.append("graveyard_diagnosis")
        recommended_actions.append(
            {
                "type": "diagnostic",
                "target": "graveyard_diagnosis",
                "reason": "最近墓地非空或刚识别出墓地信息增益，优先解释失败共性。",
            }
        )
    if stable_names:
        priority_families.append("stable_candidate_validation")
        recommended_actions.append(
            {
                "type": "validation",
                "target": stable_names[0],
                "reason": "已有稳定候选，优先补跨窗口验证与晋级判断。",
            }
        )
    if open_questions:
        recommended_actions.append(
            {
                "type": "diagnostic",
                "target": open_questions[0],
                "reason": "存在未决研究问题，优先把问题压缩成可验证结论。",
            }
        )
    if queue_exploration > queue_validation or recovering:
        suppress_families.append("broad_exploration")
    if candidate_pool_suppressed:
        rationale_bits.append(f"候选池已有 {len(candidate_pool_suppressed)} 个任务被压制，说明前端筛选已在收紧。")
    if not candidate_pool_tasks:
        rationale_bits.append("当前 candidate pool 较空，保留 baseline 作为再注入锚点。")

    challenger_queue: list[str] = []
    hypothesis_cards: list[dict[str, Any]] = []
    for row in candidate_hypothesis_cards[:4]:
        name = row.get("candidate_name")
        if not name:
            continue
        if row.get("target_window") == "medium_horizon":
            priority_families.append("watchlist_candidate_validation")
        if row.get("incremental_value") is not None and float(row.get("incremental_value") or 0.0) >= 12:
            challenger_queue.append(name)
        hypothesis_cards.append(
            {
                "candidate_name": name,
                "family": row.get("family"),
                "mechanism_note": row.get("mechanism_note") or "候选需要结构化假设说明。",
                "target_window": row.get("target_window") or "recent_extension",
                "invalidation_signals": list(row.get("invalidation_signals") or [])[:4],
                "incremental_value_thesis": row.get("incremental_value_thesis") or f"{name} 需要证明自己不是旧 frontier 的重复变体。",
            }
        )
    if hypothesis_cards:
        rationale_bits.append(f"已为 {len(hypothesis_cards)} 个候选生成 hypothesis cards，优先验证是否具备真正增量价值。")

    return {
        "schema_version": "factor_lab.planner_agent_response.v1",
        "generated_at_utc": _iso_now(),
        "agent_name": "planner-decision-engine",
        "mode": mode,
        "task_mix": task_mix,
        "priority_families": sorted(set(priority_families))[:8],
        "suppress_families": sorted(set(suppress_families))[:8],
        "recommended_actions": recommended_actions[:8],
        "hypothesis_cards": hypothesis_cards[:6],
        "challenger_queue": challenger_queue[:6],
        "confidence_score": 0.72 if recovering or stable_count >= 3 else 0.64,
        "rationale_markdown": "\n".join(f"- {x}" for x in rationale_bits[:6]),
        "decision_source": source_label,
        "decision_context_id": context.get("context_id"),
    }
