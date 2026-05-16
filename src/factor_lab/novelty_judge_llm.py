from __future__ import annotations

from typing import Any

from factor_lab.exploration_pools import NEW_MECHANISM_POOL



def judge_generation_proposal(
    proposal: dict[str, Any],
    *,
    factor_context: dict[str, dict[str, Any]] | None = None,
    source_label: str = "heuristic",
) -> dict[str, Any]:
    factor_context = factor_context or {}
    base_factors = [name for name in (proposal.get("base_factors") or []) if name]
    families = {
        (factor_context.get(name) or {}).get("family")
        for name in base_factors
        if (factor_context.get(name) or {}).get("family")
    }
    relationship_pressure = sum(int((factor_context.get(name) or {}).get("relationship_count") or 0) for name in base_factors)
    explicit_pool = proposal.get("exploration_pool")
    question_driven = proposal.get("source") in {"failure_question", "hypothesis_template", "family_gap"}

    mechanism_novelty_class = "new_mechanism" if explicit_pool == NEW_MECHANISM_POOL or question_driven else "old_space"
    if len(families) <= 1 and relationship_pressure >= 12 and mechanism_novelty_class != "new_mechanism":
        mechanism_novelty_class = "old_space"

    rationale_bits = []
    if question_driven:
        rationale_bits.append("question_driven_route")
    if explicit_pool == NEW_MECHANISM_POOL:
        rationale_bits.append("new_mechanism_pool")
    if len(families) > 1:
        rationale_bits.append("cross_family_pair")
    if relationship_pressure >= 12:
        rationale_bits.append("crowded_old_neighborhood")
    if not rationale_bits:
        rationale_bits.append("adjacent_space_optimization")

    confidence = 0.78 if mechanism_novelty_class == "new_mechanism" else 0.68
    return {
        "mechanism_novelty_class": mechanism_novelty_class,
        "novelty_judgment_source": source_label,
        "novelty_confidence": confidence,
        "mechanism_rationale": ", ".join(rationale_bits),
    }
