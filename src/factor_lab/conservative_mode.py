from __future__ import annotations

from typing import Any


def conservative_policy_from_portfolio(stability: dict[str, Any] | None) -> dict[str, Any]:
    stability = stability or {}
    score = stability.get("stability_score")

    if not isinstance(score, (int, float)):
        return {
            "enabled": False,
            "mode": "normal",
            "max_focus_factors": None,
            "prefer_core_candidates": False,
            "graveyard_review_limit": None,
            "new_template_budget": None,
            "reason": "无纸面组合稳定性数据。",
        }

    if score < 0.4:
        return {
            "enabled": True,
            "mode": "strict_conservative",
            "max_focus_factors": 2,
            "prefer_core_candidates": True,
            "graveyard_review_limit": 1,
            "new_template_budget": 0,
            "reason": "纸面组合稳定性很低，进入严格保守模式。",
        }
    if score < 0.6:
        return {
            "enabled": True,
            "mode": "conservative",
            "max_focus_factors": 3,
            "prefer_core_candidates": True,
            "graveyard_review_limit": 2,
            "new_template_budget": 1,
            "reason": "纸面组合稳定性偏低，进入保守模式。",
        }
    return {
        "enabled": False,
        "mode": "normal",
        "max_focus_factors": None,
        "prefer_core_candidates": False,
        "graveyard_review_limit": None,
        "new_template_budget": None,
        "reason": "纸面组合稳定性正常。",
    }
