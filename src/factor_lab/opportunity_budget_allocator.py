from __future__ import annotations

from typing import Any

MIN_EXPLORATION_FLOOR = {"recombine": 1, "probe": 1}


def allocate_opportunity_budget(snapshot: dict[str, Any], opportunity_learning: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    types = opportunity_learning.get("types") or {}
    recovery_state = flow_state.get("state")

    budget = {
        "confirm": 2,
        "diagnose": 2,
        "expand": 1,
        "recombine": 1,
        "probe": 1,
    }
    reasons: list[str] = []

    if recovery_state == "recovering":
        budget["confirm"] += 1
        budget["diagnose"] += 1
        reasons.append("recovering_bias")
    elif recovery_state == "recovered":
        budget["expand"] += 1
        budget["recombine"] += 1
        reasons.append("recovered_bias_to_expand_recombine")

    for otype, meta in types.items():
        action = meta.get("recommended_action")
        if otype not in budget:
            continue
        if action == "upweight":
            budget[otype] += 1
            reasons.append(f"learning_upweight:{otype}")
        elif action == "downweight":
            budget[otype] = max(0, budget[otype] - 1)
            reasons.append(f"learning_downweight:{otype}")

    # 动态纠偏：如果 confirm/diagnose 都在被 downweight，不要继续无限挤占探索预算
    if all((types.get(k, {}) or {}).get("recommended_action") == "downweight" for k in ["confirm", "diagnose"] if k in types):
        budget["confirm"] = max(1, budget["confirm"] - 1)
        budget["diagnose"] = max(1, budget["diagnose"] - 1)
        budget["recombine"] += 1
        budget["probe"] += 1
        reasons.append("dynamic_shift_from_stalled_confirm_diagnose")

    # 最小探索槽：除非系统彻底失控，否则至少保留一个 recombine/probe 位置
    for key, floor in MIN_EXPLORATION_FLOOR.items():
        if budget.get(key, 0) < floor:
            budget[key] = floor
            reasons.append(f"exploration_floor:{key}")

    return {"budget": budget, "reasons": reasons}
