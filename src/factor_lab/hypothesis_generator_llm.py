from __future__ import annotations

from typing import Any

from factor_lab.exploration_pools import NEW_MECHANISM_POOL



def generate_hypothesis_routes(
    *,
    failure_question_cards: list[dict[str, Any]] | None,
    generation_anchors: list[str] | None,
    family_gap_seeds: list[str] | None,
    limit: int = 4,
    source_label: str = "heuristic",
) -> list[dict[str, Any]]:
    cards = list(failure_question_cards or [])
    anchors = [name for name in (generation_anchors or []) if name]
    gaps = [name for name in (family_gap_seeds or []) if name]
    suggestions: list[dict[str, Any]] = []

    for card in cards:
        candidate_name = card.get("candidate_name")
        if not candidate_name:
            continue
        partner = next((name for name in gaps + anchors if name and name != candidate_name), None)
        if not partner:
            continue
        suggestions.append(
            {
                "question_card_id": card.get("card_id"),
                "question_type": card.get("question_type"),
                "source": "failure_question",
                "base_factors": [candidate_name, partner],
                "target_family": None,
                "rationale": card.get("prompt") or f"围绕 {candidate_name} 生成更远距离的新机制路线。",
                "expected_information_gain": list(card.get("expected_information_gain") or ["new_branch_opened", "candidate_survival_check"]),
                "exploration_pool": card.get("target_pool") or NEW_MECHANISM_POOL,
                "mechanism_novelty_class": "new_mechanism",
                "decision_source": source_label,
                "mechanism_rationale": card.get("prompt") or "failure-question guided hypothesis route",
            }
        )
        if len(suggestions) >= limit:
            break
    return suggestions
