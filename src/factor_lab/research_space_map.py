from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.research_families import TASK_FAMILIES, next_level
from factor_lab.research_space_registry import build_research_space_registry

ROOT = Path(__file__).resolve().parents[2]


def _max_window_level(keys: list[str], family: str) -> int:
    level = 0
    for key in keys:
        if family == 'window_expansion':
            mapping = {
                'window_rolling_30d_back': 1,
                'window_rolling_60d_back': 2,
                'window_rolling_120d_back': 3,
                'window_rolling_180d_back': 4,
                'window_rolling_240d_back': 5,
                'window_rolling_300d_back': 6,
                'window_rolling_360d_back': 7,
                'window_rolling_420d_back': 8,
                'window_expanding_2025_10_01': 9,
                'window_expanding_2025_07_01': 10,
                'window_expanding_2025_04_01': 11,
                'window_expanding_2025_01_01': 12,
                'window_expanding_2024_10_01': 13,
                'window_expanding_2024_07_01': 14,
            }
        else:
            mapping = {
                'window_recent_45d': 1,
                'window_recent_90d': 2,
                'window_recent_120d': 3,
                'window_recent_150d': 4,
                'window_recent_180d': 5,
                'window_recent_210d': 6,
                'window_recent_240d': 7,
                'window_recent_270d': 8,
            }
        for prefix, lv in mapping.items():
            if key.startswith(prefix):
                level = max(level, lv)
    return level


def _sum_prefix(counter: dict[str, int], prefixes: list[str]) -> int:
    total = 0
    for key, value in counter.items():
        if any(key.startswith(prefix) for prefix in prefixes):
            total += value
    return total


def build_research_space_map(db_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    registry_path = ROOT / 'artifacts' / 'research_space_registry.json'
    registry = build_research_space_registry(db_path, registry_path)

    knowledge_gain_counter = registry.get('knowledge_gain_counter', {}) or {}
    windows_covered = registry.get('windows_covered', {}) or {}
    validation_depth = registry.get('validation_depth', {}) or {}
    graveyard_diagnostics = registry.get('graveyard_diagnostics', {}) or {}
    exploration_lines = registry.get('exploration_lines', {}) or {}

    covered_window_keys = sorted(windows_covered.keys())
    covered_recent_keys = sorted([k for k in windows_covered.keys() if 'recent' in k])
    window_level = _max_window_level(covered_window_keys, 'window_expansion')
    recent_level = _max_window_level(covered_recent_keys, 'recent_window_validation')
    stable_level = max(validation_depth.values()) if validation_depth else 0
    graveyard_level = max(graveyard_diagnostics.values()) if graveyard_diagnostics else 0
    exploration_level = len(exploration_lines)

    family_progress: dict[str, Any] = {
        'window_expansion': {
            'covered_windows': covered_window_keys,
            'current_level': window_level,
            'next_level': next_level(window_level, 'window_expansion'),
        },
        'recent_window_validation': {
            'covered_recent_windows': covered_recent_keys,
            'current_level': recent_level,
            'next_level': next_level(recent_level, 'recent_window_validation'),
        },
        'stable_candidate_validation': {
            'tracked_keys': validation_depth,
            'current_level': stable_level,
            'next_level': next_level(stable_level, 'stable_candidate_validation'),
        },
        'graveyard_diagnosis': {
            'tracked_keys': graveyard_diagnostics,
            'current_level': graveyard_level,
            'next_level': next_level(graveyard_level, 'graveyard_diagnosis'),
        },
        'exploration': {
            'tracked_lines': exploration_lines,
            'current_level': exploration_level,
            'next_level': next_level(exploration_level, 'exploration'),
        },
    }

    family_recent_gain = {
        'stable_candidate_validation': _sum_prefix(knowledge_gain_counter, ['stable_candidate_validation']),
        'graveyard_diagnosis': _sum_prefix(knowledge_gain_counter, ['graveyard_']),
        'exploration': _sum_prefix(knowledge_gain_counter, ['exploration_']),
    }

    family_fatigue = {}
    family_saturation = {}
    coverage_gaps = {}
    for family_name, spec in TASK_FAMILIES.items():
        progress = family_progress.get(family_name, {})
        current_level = progress.get('current_level', 0)
        family_fatigue[family_name] = {
            'fatigue_level': 'high' if current_level >= spec.max_level - 1 else 'medium' if current_level >= max(spec.max_level // 2, 1) else 'low'
        }
        family_saturation[family_name] = {
            'saturated': current_level >= spec.max_level,
            'remaining_levels': max(spec.max_level - current_level, 0),
        }
        coverage_gaps[family_name] = {
            'next_level': progress.get('next_level'),
            'needs_expansion': progress.get('next_level') is not None,
        }

    payload = {
        'registry_path': str(registry_path.relative_to(ROOT)),
        'family_progress': family_progress,
        'family_recent_gain': family_recent_gain,
        'family_fatigue': family_fatigue,
        'family_saturation': family_saturation,
        'coverage_gaps': coverage_gaps,
        'knowledge_gain_counter': knowledge_gain_counter,
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload
