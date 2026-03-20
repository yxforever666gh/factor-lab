from __future__ import annotations

from typing import Any


TYPE_BASE = {
    "confirm": 0.76,
    "diagnose": 0.78,
    "expand": 0.68,
    "recombine": 0.64,
    "probe": 0.58,
    "archive": 0.35,
}


def score_opportunity(question: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, float | str]:
    analyst = snapshot.get("analyst_signals") or {}
    learning = snapshot.get("research_learning") or {}
    flow_state = snapshot.get("research_flow_state") or {}

    qtype = question.get("question_type") or "probe"
    family = question.get("target_family")
    family_learning = (learning.get("families") or {}).get(family or "", {})

    priority = float(TYPE_BASE.get(qtype, 0.55))
    novelty = 0.45
    confidence = 0.60
    rationale_bits: list[str] = [f"base_type={qtype}"]

    if family_learning.get("recommended_action") == "upweight":
        priority += 0.10
        confidence += 0.08
        rationale_bits.append("family_learning=upweight")
    elif family_learning.get("recommended_action") == "downweight":
        priority -= 0.08
        confidence -= 0.05
        rationale_bits.append("family_learning=downweight")
    elif family_learning.get("cooldown_active"):
        priority -= 0.18
        confidence -= 0.10
        rationale_bits.append("family_learning=cooldown")

    if flow_state.get("state") == "recovering" and qtype in {"confirm", "diagnose"}:
        priority += 0.06
        rationale_bits.append("recovering_prefers_confirm_diagnose")
    if flow_state.get("state") == "recovered" and qtype in {"expand", "recombine"}:
        priority += 0.07
        novelty += 0.08
        rationale_bits.append("recovered_prefers_expand_recombine")

    targets = set(question.get("target_candidates") or [])
    analyst_focus = set(analyst.get("focus_factors") or [])
    analyst_graveyard = set(analyst.get("review_graveyard") or [])
    if targets & analyst_focus:
        priority += 0.05
        confidence += 0.03
        rationale_bits.append("analyst_focus_overlap")
    if targets & analyst_graveyard:
        priority += 0.05
        rationale_bits.append("analyst_graveyard_overlap")

    if qtype in {"recombine", "probe"}:
        novelty += 0.18
    elif qtype == "expand":
        novelty += 0.10
    elif qtype == "diagnose":
        novelty += 0.06

    priority = min(max(priority, 0.05), 0.99)
    novelty = min(max(novelty, 0.05), 0.99)
    confidence = min(max(confidence, 0.05), 0.99)
    return {
        "priority": round(priority, 3),
        "novelty_score": round(novelty, 3),
        "confidence": round(confidence, 3),
        "score_rationale": "; ".join(rationale_bits),
    }
