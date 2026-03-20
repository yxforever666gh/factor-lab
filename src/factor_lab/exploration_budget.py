from __future__ import annotations

from typing import Any


def build_exploration_budget(snapshot: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    learning = snapshot.get("research_learning") or {}
    exploration_state = snapshot.get("exploration_state") or {}

    budget = {
        "confirm": 2,
        "diagnose": 2,
        "expand": 1,
        "recombine": 1,
        "probe": 1,
    }
    reasons: list[str] = []

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
        budget["probe"] = 0
        budget["recombine"] = max(0, budget["recombine"] - 1)
        reasons.append("exploration_throttled")

    learning_families = learning.get("families") or {}
    upweight_count = len([1 for row in learning_families.values() if row.get("recommended_action") == "upweight"])
    if upweight_count:
        budget["expand"] += min(upweight_count, 2)
        reasons.append("research_learning_upweight_favors_expand")

    return {
        "budget": budget,
        "reasons": reasons,
    }
