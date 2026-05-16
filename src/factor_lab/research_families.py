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
    "medium_horizon_validation": TaskFamilySpec(
        family="medium_horizon_validation",
        category="validation",
        min_level=1,
        max_level=6,
        base_priority=26,
    ),
    "stable_candidate_validation": TaskFamilySpec(
        family="stable_candidate_validation",
        category="validation",
        min_level=1,
        max_level=8,
        base_priority=28,
    ),
    "fragile_candidate_hardening": TaskFamilySpec(
        family="fragile_candidate_hardening",
        category="validation",
        min_level=1,
        max_level=6,
        base_priority=29,
    ),
    "watchlist_candidate_validation": TaskFamilySpec(
        family="watchlist_candidate_validation",
        category="validation",
        min_level=1,
        max_level=4,
        base_priority=27,
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


def stable_candidate_gain_name(level: int) -> str:
    if level <= 1:
        return "stable_candidate_validation_requested"
    return f"stable_candidate_validation_v{level}_requested"


def stable_candidate_worker_note(level: int) -> str:
    if level <= 1:
        return "validation｜稳定候选深化验证"
    return f"validation｜稳定候选深化验证 v{level}"


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


def graveyard_gain_name(level: int) -> str:
    mapping = {
        1: "graveyard_batch_consistency_requested",
        2: "graveyard_window_sensitivity_requested",
        3: "graveyard_raw_vs_neutral_requested",
        4: "graveyard_construction_requested",
        5: "graveyard_cross_window_requested",
        6: "graveyard_regime_shift_requested",
    }
    return mapping.get(level, f"graveyard_diagnosis_level_{level}_requested")


def graveyard_worker_note(level: int) -> str:
    mapping = {
        1: "validation｜graveyard 一致性诊断",
        2: "validation｜graveyard 窗口敏感性诊断",
        3: "validation｜graveyard raw-vs-neutral 诊断",
        4: "validation｜graveyard 构造诊断",
        5: "validation｜graveyard 跨窗口对照诊断",
        6: "validation｜graveyard 风格/阶段切换诊断",
    }
    return mapping.get(level, f"validation｜graveyard level {level} 诊断")
