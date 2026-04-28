from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def infer_template_type(plan: dict[str, Any]) -> str:
    checks = set(plan.get("portfolio_checks", []))
    focus = set(plan.get("focus_factors", []))
    graveyard = set(plan.get("review_graveyard", []))

    if "diagnose_neutralized_underperformance" in checks:
        return "neutralization_diagnosis"
    if graveyard:
        return "graveyard_review"
    if focus & {"mom_20", "mom_plus_value"}:
        return "momentum_core_extension"
    return "general_factor_probe"


def append_recommendation_history(
    plan_path: str | Path,
    retrospective_path: str | Path,
    output_path: str | Path,
) -> list[dict[str, Any]]:
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    retro = json.loads(Path(retrospective_path).read_text(encoding="utf-8"))
    path = Path(output_path)
    history = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []

    entry = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "template_type": infer_template_type(plan),
        "focus_factors": plan.get("focus_factors", []),
        "keep_as_core_candidates": plan.get("keep_as_core_candidates", []),
        "review_graveyard": plan.get("review_graveyard", []),
        "portfolio_checks": plan.get("portfolio_checks", []),
        "rationale": plan.get("rationale", ""),
        "effectiveness": retro.get("effectiveness", "neutral"),
        "reason": retro.get("reason", ""),
        "survived_as_candidates": retro.get("survived_as_candidates", []),
        "fell_to_graveyard": retro.get("fell_to_graveyard", []),
        "next_action_hint": retro.get("next_action_hint", ""),
    }
    history.append(entry)
    path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    return history
