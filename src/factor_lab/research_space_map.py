from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_families import TASK_FAMILIES, next_level
from factor_lab.research_space_registry import build_research_space_registry


ROOT = Path(__file__).resolve().parents[2]


def build_research_space_map(db_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    registry_path = ROOT / "artifacts" / "research_space_registry.json"
    registry = build_research_space_registry(db_path, registry_path)

    knowledge_gain_counter = registry.get("knowledge_gain_counter", {}) or {}
    windows_covered = registry.get("windows_covered", {}) or {}
    validation_depth = registry.get("validation_depth", {}) or {}
    graveyard_diagnostics = registry.get("graveyard_diagnostics", {}) or {}
    exploration_lines = registry.get("exploration_lines", {}) or {}

    family_progress: dict[str, Any] = {
        "window_expansion": {
            "covered_windows": sorted(windows_covered.keys()),
            "current_level": len(windows_covered),
            "next_level": next_level(len(windows_covered), "window_expansion"),
        },
        "recent_window_validation": {
            "covered_recent_windows": sorted([k for k in windows_covered.keys() if "recent" in k]),
            "current_level": len([k for k in windows_covered.keys() if "recent" in k]),
            "next_level": next_level(len([k for k in windows_covered.keys() if "recent" in k]), "recent_window_validation"),
        },
        "stable_candidate_validation": {
            "tracked_keys": validation_depth,
            "current_level": max(validation_depth.values()) if validation_depth else 0,
            "next_level": next_level(max(validation_depth.values()) if validation_depth else 0, "stable_candidate_validation"),
        },
        "graveyard_diagnosis": {
            "tracked_keys": graveyard_diagnostics,
            "current_level": max(graveyard_diagnostics.values()) if graveyard_diagnostics else 0,
            "next_level": next_level(max(graveyard_diagnostics.values()) if graveyard_diagnostics else 0, "graveyard_diagnosis"),
        },
        "exploration": {
            "tracked_lines": exploration_lines,
            "current_level": len(exploration_lines),
            "next_level": next_level(len(exploration_lines), "exploration"),
        },
    }

    family_recent_gain = {
        "stable_candidate_validation": knowledge_gain_counter.get("stable_candidate_validation_requested", 0)
            + knowledge_gain_counter.get("stable_candidate_validation_v2_requested", 0)
            + knowledge_gain_counter.get("stable_candidate_validation_v3_requested", 0)
            + knowledge_gain_counter.get("stable_candidate_validation_v4_requested", 0)
            + knowledge_gain_counter.get("stable_candidate_validation_v5_requested", 0),
        "graveyard_diagnosis": knowledge_gain_counter.get("graveyard_window_sensitivity_requested", 0)
            + knowledge_gain_counter.get("graveyard_raw_vs_neutral_requested", 0)
            + knowledge_gain_counter.get("graveyard_construction_requested", 0)
            + knowledge_gain_counter.get("graveyard_cross_window_requested", 0)
            + knowledge_gain_counter.get("graveyard_regime_shift_requested", 0),
        "exploration": knowledge_gain_counter.get("exploration_candidate_survived", 0)
            + knowledge_gain_counter.get("exploration_graveyard_identified", 0),
    }

    family_fatigue = {}
    family_saturation = {}
    coverage_gaps = {}
    for family_name, spec in TASK_FAMILIES.items():
        progress = family_progress.get(family_name, {})
        current_level = progress.get("current_level", 0)
        family_fatigue[family_name] = {
            "fatigue_level": "high" if current_level >= spec.max_level - 1 else "medium" if current_level >= spec.max_level // 2 else "low"
        }
        family_saturation[family_name] = {
            "saturated": current_level >= spec.max_level,
            "remaining_levels": max(spec.max_level - current_level, 0),
        }
        coverage_gaps[family_name] = {
            "next_level": progress.get("next_level"),
            "needs_expansion": progress.get("next_level") is not None,
        }

    payload = {
        "registry_path": str(registry_path.relative_to(ROOT)),
        "family_progress": family_progress,
        "family_recent_gain": family_recent_gain,
        "family_fatigue": family_fatigue,
        "family_saturation": family_saturation,
        "coverage_gaps": coverage_gaps,
        "knowledge_gain_counter": knowledge_gain_counter,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
