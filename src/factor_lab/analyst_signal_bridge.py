from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def load_analyst_signals(base_dir: str | Path | None = None) -> dict[str, Any]:
    base = Path(base_dir) if base_dir else ARTIFACTS
    plan = _read_json(base / "llm_next_batch_proposal.json", {})
    context = _read_json(base / "llm_recommendation_context.json", {})
    weights = _read_json(base / "llm_recommendation_weights.json", {})
    status = _read_json(base / "llm_status.json", {})

    focus_factors = [x for x in (plan.get("focus_factors") or []) if x]
    core_candidates = [x for x in (plan.get("keep_as_core_candidates") or []) if x]
    review_graveyard = [x for x in (plan.get("review_graveyard") or []) if x]
    portfolio_checks = [x for x in (plan.get("portfolio_checks") or []) if x]
    rationale = plan.get("rationale") or ""

    suggested_families = [x for x in (plan.get("suggested_families") or []) if x]
    if core_candidates:
        suggested_families.append("stable_candidate_validation")
    if review_graveyard:
        suggested_families.append("graveyard_diagnosis")
    if portfolio_checks:
        suggested_families.append("recent_window_validation")
    if "diagnose_neutralized_underperformance" in portfolio_checks:
        suggested_families.append("graveyard_diagnosis")
    if "compare_all_factors_vs_candidates_only" in portfolio_checks or "compare_cluster_representatives_vs_all_factors" in portfolio_checks:
        suggested_families.append("stable_candidate_validation")

    risk_flags = [x for x in (plan.get("risk_flags") or []) if x]
    if "diagnose_neutralized_underperformance" in portfolio_checks:
        risk_flags.append("must_validate_neutralization")
    if review_graveyard:
        risk_flags.append("graveyard_review_required")
    if not focus_factors:
        risk_flags.append("analyst_low_specificity")

    template_hint = None
    priority_summary = context.get("priority_summary") or []
    if priority_summary:
        template_hint = priority_summary[0]
        if template_hint.get("cooldown_active"):
            risk_flags.append("template_cooldown_active")
        if (template_hint.get("recommended_action") or "").endswith("downweight"):
            risk_flags.append("template_downweighted")

    must_validate_before_expand = bool(plan.get("must_validate_before_expand")) or any(
        flag in risk_flags
        for flag in ["must_validate_neutralization", "graveyard_review_required", "template_cooldown_active"]
    )

    return {
        "available": bool(plan),
        "agent_name": status.get("agent_name"),
        "focus_factors": focus_factors,
        "keep_as_core_candidates": core_candidates,
        "review_graveyard": review_graveyard,
        "portfolio_checks": portfolio_checks,
        "rationale": rationale,
        "suggested_families": sorted(set(suggested_families)),
        "risk_flags": sorted(set(risk_flags)),
        "must_validate_before_expand": must_validate_before_expand,
        "priority_summary": priority_summary,
        "template_hint": template_hint,
        "confidence_score": plan.get("confidence_score"),
        "global_hint": weights.get("global_hint") or context.get("planner_hint"),
    }
