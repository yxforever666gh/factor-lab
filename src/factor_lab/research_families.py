from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TaskFamilySpec:
    family: str
    category: str
    min_level: int
    max_level: int
    base_priority: int


TASK_FAMILIES: dict[str, TaskFamilySpec] = {
    "window_expansion": TaskFamilySpec(
        family="window_expansion",
        category="baseline",
        min_level=1,
        max_level=12,
        base_priority=18,
    ),
    "recent_window_validation": TaskFamilySpec(
        family="recent_window_validation",
        category="validation",
        min_level=1,
        max_level=12,
        base_priority=24,
    ),
    "stable_candidate_validation": TaskFamilySpec(
        family="stable_candidate_validation",
        category="validation",
        min_level=1,
        max_level=8,
        base_priority=28,
    ),
    "graveyard_diagnosis": TaskFamilySpec(
        family="graveyard_diagnosis",
        category="validation",
        min_level=1,
        max_level=8,
        base_priority=30,
    ),
    "exploration": TaskFamilySpec(
        family="exploration",
        category="exploration",
        min_level=1,
        max_level=4,
        base_priority=55,
    ),
}


def family_spec(name: str) -> TaskFamilySpec:
    return TASK_FAMILIES[name]


def next_level(current_level: int, family: str) -> int | None:
    spec = family_spec(family)
    nxt = current_level + 1
    if nxt > spec.max_level:
        return None
    return nxt


def level_priority(family: str, level: int) -> int:
    spec = family_spec(family)
    return spec.base_priority + max(level - spec.min_level, 0)


def stable_candidate_task_name(level: int) -> str:
    if level <= 1:
        return "stable_candidate_validation_review"
    return f"stable_candidate_validation_review_v{level}"


def graveyard_task_name(level: int) -> str:
    mapping = {
        1: "batch_consistency_review",
        2: "graveyard_window_sensitivity_review",
        3: "graveyard_raw_vs_neutral_review",
        4: "graveyard_construction_review",
        5: "graveyard_cross_window_review",
        6: "graveyard_regime_shift_review",
    }
    return mapping.get(level, f"graveyard_diagnosis_level_{level}")
