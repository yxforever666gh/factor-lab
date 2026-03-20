from __future__ import annotations

from typing import Any


def allocate_opportunity_budget(snapshot: dict[str, Any], opportunity_learning: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    types = opportunity_learning.get("types") or {}
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
        reasons.append("recovering_bias")

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

    return {"budget": budget, "reasons": reasons}
