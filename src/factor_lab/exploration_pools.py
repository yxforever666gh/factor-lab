from __future__ import annotations

from typing import Any

OLD_SPACE_POOL = "old_space_optimization"
NEW_MECHANISM_POOL = "new_mechanism_exploration"

_OLD_SPACE_SOURCES = {
    "stable_plus_graveyard",
    "high_value_failure_seed",
}

_NEW_MECHANISM_SOURCES = {
    "hypothesis_template",
    "family_gap",
    "failure_question",
}


def classify_exploration_pool(source: str | None, payload: dict[str, Any] | None = None) -> str:
    payload = payload or {}
    explicit_pool = payload.get("exploration_pool") or payload.get("target_pool")
    if explicit_pool in {OLD_SPACE_POOL, NEW_MECHANISM_POOL}:
        return explicit_pool

    source_text = str(source or "").strip()
    if source_text in _NEW_MECHANISM_SOURCES:
        return NEW_MECHANISM_POOL
    if source_text in _OLD_SPACE_SOURCES:
        return OLD_SPACE_POOL
    return NEW_MECHANISM_POOL if "template" in source_text or "question" in source_text else OLD_SPACE_POOL


def split_exploration_pool_budget(
    total_budget: int,
    *,
    prioritize_new_mechanism: bool = False,
    quality_priority_mode: bool = False,
    regime: str | None = None,
) -> dict[str, int]:
    total = max(0, int(total_budget or 0))
    if total <= 0:
        return {
            OLD_SPACE_POOL: 0,
            NEW_MECHANISM_POOL: 0,
        }

    if total == 1:
        return {
            OLD_SPACE_POOL: 0 if (prioritize_new_mechanism or quality_priority_mode or regime == "expansion_ready") else 1,
            NEW_MECHANISM_POOL: 1 if (prioritize_new_mechanism or quality_priority_mode or regime == "expansion_ready") else 0,
        }

    new_budget = total // 2
    old_budget = total - new_budget

    if total >= 3:
        new_budget = max(new_budget, (total + 1) // 2)
        old_budget = total - new_budget

    if prioritize_new_mechanism or regime == "expansion_ready":
        new_budget = min(total - 1, new_budget + 1) if total >= 2 else total
        old_budget = total - new_budget

    if quality_priority_mode and total >= 2:
        new_budget = max(new_budget, 1)
        old_budget = max(old_budget, 1)
        overflow = (new_budget + old_budget) - total
        if overflow > 0:
            if new_budget >= old_budget:
                new_budget -= overflow
            else:
                old_budget -= overflow

    return {
        OLD_SPACE_POOL: max(0, int(old_budget)),
        NEW_MECHANISM_POOL: max(0, int(new_budget)),
    }
