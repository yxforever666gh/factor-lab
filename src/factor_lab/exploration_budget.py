from __future__ import annotations

from typing import Any

from factor_lab.regime_awareness import QUESTION_TYPES, build_regime_context


BASE_EXPLORATION_FLOOR_SLOTS = 2
TRUE_FAULT_RECOVERY_STATES = {"exhausted"}


def exploration_floor_context(snapshot: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    failure_state = snapshot.get("failure_state") or {}
    relationship_summary = snapshot.get("relationship_summary") or {}
    knowledge_gain_counter = snapshot.get("knowledge_gain_counter") or {}

    reasons: list[str] = []
    true_fault_recovery = False
    if bool(failure_state.get("cooldown_active")):
        true_fault_recovery = True
        reasons.append("failure_cooldown_active")
    if (flow_state.get("state") or "") in TRUE_FAULT_RECOVERY_STATES:
        true_fault_recovery = True
        reasons.append(f"flow_state={(flow_state.get('state') or '')}")

    if true_fault_recovery:
        slots = 0
    else:
        slots = BASE_EXPLORATION_FLOOR_SLOTS
        if (flow_state.get("state") or "") == "recovering":
            slots = 1
            reasons.append("recovering_floor_reduced")
        duplicate_pressure = int(relationship_summary.get("duplicate_of") or 0)
        repeated_no_gain = int(knowledge_gain_counter.get("no_significant_information_gain") or 0)
        if slots > 0 and duplicate_pressure >= 24 and repeated_no_gain >= 2:
            slots += 1
            reasons.append("crowded_search_space_expands_controlled_floor")

    return {
        "true_fault_recovery": true_fault_recovery,
        "exploration_floor_slots": slots,
        "reasons": reasons,
    }


def build_exploration_budget(snapshot: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    learning = snapshot.get("research_learning") or {}
    exploration_state = snapshot.get("exploration_state") or {}
    regime_context = build_regime_context(snapshot)

    budget = {
        "confirm": 2,
        "diagnose": 2,
        "expand": 1,
        "recombine": 1,
        "probe": 1,
    }
    reasons: list[str] = []
    floor = exploration_floor_context(snapshot)

    if flow_state.get("state") == "recovering":
        budget["confirm"] += 1
        budget["diagnose"] += 1
        budget["recombine"] = max(0, budget["recombine"] - 1)
        budget["probe"] = max(0, budget["probe"] - 1)
        reasons.append("recovering_bias_to_confirm_diagnose")

    if flow_state.get("state") == "recovered":
        budget["expand"] += 1
        budget["recombine"] += 1
        reasons.append("recovered_bias_to_expand_recombine")

    if exploration_state.get("should_throttle"):
        budget["probe"] = max(floor["exploration_floor_slots"], 0)
        budget["recombine"] = max(0, budget["recombine"] - 1)
        reasons.append("exploration_throttled_but_floor_preserved" if floor["exploration_floor_slots"] else "exploration_throttled")

    learning_families = learning.get("families") or {}
    upweight_count = len([1 for row in learning_families.values() if row.get("recommended_action") == "upweight"])
    if upweight_count:
        budget["expand"] += min(upweight_count, 2)
        reasons.append("research_learning_upweight_favors_expand")

    regime_weights = regime_context.get("weights") or {}
    for key in QUESTION_TYPES:
        weight = float(regime_weights.get(key) or 1.0)
        if weight >= 1.12:
            budget[key] += 1
            reasons.append(f"regime_upweight:{key}")
        elif weight <= 0.82:
            budget[key] = max(0, budget[key] - 1)
            reasons.append(f"regime_downweight:{key}")

    if floor["exploration_floor_slots"]:
        budget["probe"] = max(int(budget.get("probe") or 0), floor["exploration_floor_slots"])
    return {
        "budget": budget,
        "reasons": reasons,
        "floor": floor,
        "regime_context": regime_context,
    }
