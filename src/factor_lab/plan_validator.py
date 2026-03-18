from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.llm_recommendation_memory import infer_template_type
from factor_lab.conservative_mode import conservative_policy_from_portfolio


REQUIRED_KEYS = {"focus_factors", "keep_as_core_candidates", "review_graveyard", "portfolio_checks", "rationale"}
ALLOWED_PORTFOLIO_CHECKS = {
    "compare_all_factors_vs_candidates_only",
    "compare_cluster_representatives_vs_all_factors",
    "diagnose_neutralized_underperformance",
}


def _apply_template_policy(
    template_type: str,
    recommendation_weights: dict[str, Any] | None,
    recommendation_context: dict[str, Any] | None,
    max_focus_factors: int,
    max_review_graveyard: int,
) -> tuple[int, int, dict[str, Any]]:
    templates = (recommendation_weights or {}).get("templates", {})
    template_info = templates.get(template_type, {})
    action = template_info.get("recommended_action", "keep")
    cooldown_info = ((recommendation_context or {}).get("cooldown", {}) or {}).get(template_type, {})

    adjusted_focus = max_focus_factors
    adjusted_graveyard = max_review_graveyard
    policy = {
        "template_type": template_type,
        "recommended_action": action,
        "base_max_focus_factors": max_focus_factors,
        "base_max_review_graveyard": max_review_graveyard,
    }

    if action == "upweight":
        adjusted_focus = max_focus_factors + 1
        adjusted_graveyard = max_review_graveyard + 1
    elif action == "soft_upweight":
        adjusted_focus = max_focus_factors
        adjusted_graveyard = max_review_graveyard
    elif action == "downweight":
        adjusted_focus = max(2, max_focus_factors - 2)
        adjusted_graveyard = max(2, max_review_graveyard - 2)
    elif action == "soft_downweight":
        adjusted_focus = max(3, max_focus_factors - 1)
        adjusted_graveyard = max(3, max_review_graveyard - 1)

    if cooldown_info.get("cooldown_active"):
        adjusted_focus = max(2, min(adjusted_focus, 3))
        adjusted_graveyard = max(2, min(adjusted_graveyard, 3))

    policy["adjusted_max_focus_factors"] = adjusted_focus
    policy["adjusted_max_review_graveyard"] = adjusted_graveyard
    policy["avg_effect_score"] = template_info.get("avg_effect_score")
    policy["sample_count"] = template_info.get("sample_count")
    policy["cooldown_active"] = cooldown_info.get("cooldown_active", False)
    policy["cooldown_reason"] = cooldown_info.get("reason")
    return adjusted_focus, adjusted_graveyard, policy


def validate_plan(
    plan: dict[str, Any],
    allowed_factor_names: set[str],
    max_focus_factors: int = 6,
    max_review_graveyard: int = 6,
    recommendation_weights: dict[str, Any] | None = None,
    recommendation_context: dict[str, Any] | None = None,
    paper_portfolio_stability: dict[str, Any] | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    template_type = infer_template_type(plan)
    adjusted_focus_limit, adjusted_graveyard_limit, template_policy = _apply_template_policy(
        template_type,
        recommendation_weights,
        recommendation_context,
        max_focus_factors,
        max_review_graveyard,
    )

    missing = REQUIRED_KEYS - set(plan.keys())
    if missing:
        errors.append(f"缺少字段: {', '.join(sorted(missing))}")

    conservative_policy = conservative_policy_from_portfolio(paper_portfolio_stability)

    focus_factors = plan.get("focus_factors", [])
    keep_as_core = plan.get("keep_as_core_candidates", [])
    review_graveyard = plan.get("review_graveyard", [])
    portfolio_checks = plan.get("portfolio_checks", [])
    rationale = plan.get("rationale", "")
    novelty_reason = plan.get("novelty_reason", "")

    if not isinstance(focus_factors, list) or not focus_factors:
        errors.append("focus_factors 必须是非空列表")
    effective_focus_limit = adjusted_focus_limit
    if conservative_policy.get("enabled") and conservative_policy.get("max_focus_factors") is not None:
        effective_focus_limit = min(effective_focus_limit, int(conservative_policy["max_focus_factors"]))
    if isinstance(focus_factors, list) and len(focus_factors) > effective_focus_limit:
        errors.append(f"focus_factors 超出上限 {effective_focus_limit}")

    if not isinstance(keep_as_core, list):
        errors.append("keep_as_core_candidates 必须是列表")
    if not isinstance(review_graveyard, list):
        errors.append("review_graveyard 必须是列表")
    effective_graveyard_limit = adjusted_graveyard_limit
    if conservative_policy.get("enabled") and conservative_policy.get("graveyard_review_limit") is not None:
        effective_graveyard_limit = min(effective_graveyard_limit, int(conservative_policy["graveyard_review_limit"]))
    if isinstance(review_graveyard, list) and len(review_graveyard) > effective_graveyard_limit:
        warnings.append(f"review_graveyard 超出建议上限 {effective_graveyard_limit}，后续会截断")

    if not isinstance(portfolio_checks, list) or not portfolio_checks:
        errors.append("portfolio_checks 必须是非空列表")
    else:
        invalid_checks = [item for item in portfolio_checks if item not in ALLOWED_PORTFOLIO_CHECKS]
        if invalid_checks:
            errors.append(f"存在非法 portfolio_checks: {', '.join(invalid_checks)}")

    if not isinstance(rationale, str) or not rationale.strip():
        errors.append("rationale 不能为空")

    high_fatigue = template_policy.get("cooldown_active") or ((recommendation_context or {}).get("fatigue", {}) or {}).get(template_type, {}).get("fatigue_level") == "high"
    if high_fatigue and (not isinstance(novelty_reason, str) or not novelty_reason.strip()):
        errors.append("高疲劳或冷却模板继续提议时，必须提供 novelty_reason")

    referenced = []
    for group in [focus_factors, keep_as_core, review_graveyard]:
        if isinstance(group, list):
            referenced.extend(group)
    invalid_factors = sorted({name for name in referenced if name not in allowed_factor_names})
    if invalid_factors:
        errors.append(f"存在未允许的因子名: {', '.join(invalid_factors)}")

    if isinstance(keep_as_core, list) and isinstance(focus_factors, list):
        outside_focus = [name for name in keep_as_core if name not in focus_factors]
        if outside_focus:
            warnings.append(f"keep_as_core_candidates 中部分因子不在 focus_factors 内: {', '.join(outside_focus)}")

    if conservative_policy.get("enabled") and conservative_policy.get("prefer_core_candidates"):
        non_core = [name for name in focus_factors if name not in keep_as_core]
        if non_core:
            warnings.append(f"保守模式下，建议减少非核心候选: {', '.join(non_core)}")
            if len(non_core) >= len(focus_factors) - len(keep_as_core):
                pass

    portfolio_policy = {
        "stability_score": (paper_portfolio_stability or {}).get("stability_score"),
        "label": (paper_portfolio_stability or {}).get("label"),
        "risk_mode": conservative_policy.get("mode", "normal"),
        "conservative_policy": conservative_policy,
    }
    stability_score = (paper_portfolio_stability or {}).get("stability_score")
    if isinstance(stability_score, (int, float)) and stability_score < 0.6:
        warnings.append("纸面组合稳定性偏低，建议减少本轮 focus_factors 数量")
        if isinstance(focus_factors, list) and len(focus_factors) > 3:
            errors.append("纸面组合稳定性偏低时，focus_factors 不应超过 3")

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "template_policy": template_policy,
        "portfolio_policy": portfolio_policy,
        "normalized_plan": {
            "focus_factors": focus_factors[:effective_focus_limit] if isinstance(focus_factors, list) else [],
            "keep_as_core_candidates": keep_as_core if isinstance(keep_as_core, list) else [],
            "review_graveyard": review_graveyard[:effective_graveyard_limit] if isinstance(review_graveyard, list) else [],
            "portfolio_checks": portfolio_checks if isinstance(portfolio_checks, list) else [],
            "rationale": rationale.strip() if isinstance(rationale, str) else "",
            "novelty_reason": novelty_reason.strip() if isinstance(novelty_reason, str) else "",
            "template_type": template_type,
        },
    }


def validate_plan_file(
    plan_path: str | Path,
    allowed_factor_names: set[str],
    recommendation_weights: dict[str, Any] | None = None,
    recommendation_context: dict[str, Any] | None = None,
    paper_portfolio_stability: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    return validate_plan(
        plan,
        allowed_factor_names,
        recommendation_weights=recommendation_weights,
        recommendation_context=recommendation_context,
        paper_portfolio_stability=paper_portfolio_stability,
    )
