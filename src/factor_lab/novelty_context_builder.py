from __future__ import annotations

from typing import Any



def build_novelty_context(*, proposal: dict[str, Any], factor_context: dict[str, dict[str, Any]] | None = None) -> dict[str, Any]:
    factor_context = factor_context or {}
    base_factors = [name for name in (proposal.get("base_factors") or []) if name]
    return {
        "schema_version": "factor_lab.novelty_context.v1",
        "candidate_id": proposal.get("candidate_id"),
        "source": proposal.get("source"),
        "exploration_pool": proposal.get("exploration_pool"),
        "base_factors": base_factors,
        "factor_context": {name: factor_context.get(name) or {} for name in base_factors},
        "target_family": proposal.get("target_family"),
        "question_card_id": proposal.get("question_card_id"),
        "hypothesis_template_id": proposal.get("hypothesis_template_id"),
    }
