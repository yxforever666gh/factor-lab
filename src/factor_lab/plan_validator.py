from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REQUIRED_KEYS = {"focus_factors", "keep_as_core_candidates", "review_graveyard", "portfolio_checks", "rationale"}
ALLOWED_PORTFOLIO_CHECKS = {
    "compare_all_factors_vs_candidates_only",
    "compare_cluster_representatives_vs_all_factors",
    "diagnose_neutralized_underperformance",
}


def validate_plan(
    plan: dict[str, Any],
    allowed_factor_names: set[str],
    max_focus_factors: int = 6,
    max_review_graveyard: int = 6,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []

    missing = REQUIRED_KEYS - set(plan.keys())
    if missing:
        errors.append(f"缺少字段: {', '.join(sorted(missing))}")

    focus_factors = plan.get("focus_factors", [])
    keep_as_core = plan.get("keep_as_core_candidates", [])
    review_graveyard = plan.get("review_graveyard", [])
    portfolio_checks = plan.get("portfolio_checks", [])
    rationale = plan.get("rationale", "")

    if not isinstance(focus_factors, list) or not focus_factors:
        errors.append("focus_factors 必须是非空列表")
    if isinstance(focus_factors, list) and len(focus_factors) > max_focus_factors:
        errors.append(f"focus_factors 超出上限 {max_focus_factors}")

    if not isinstance(keep_as_core, list):
        errors.append("keep_as_core_candidates 必须是列表")
    if not isinstance(review_graveyard, list):
        errors.append("review_graveyard 必须是列表")
    if isinstance(review_graveyard, list) and len(review_graveyard) > max_review_graveyard:
        warnings.append(f"review_graveyard 超出建议上限 {max_review_graveyard}，后续会截断")

    if not isinstance(portfolio_checks, list) or not portfolio_checks:
        errors.append("portfolio_checks 必须是非空列表")
    else:
        invalid_checks = [item for item in portfolio_checks if item not in ALLOWED_PORTFOLIO_CHECKS]
        if invalid_checks:
            errors.append(f"存在非法 portfolio_checks: {', '.join(invalid_checks)}")

    if not isinstance(rationale, str) or not rationale.strip():
        errors.append("rationale 不能为空")

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

    return {
        "valid": not errors,
        "errors": errors,
        "warnings": warnings,
        "normalized_plan": {
            "focus_factors": focus_factors[:max_focus_factors] if isinstance(focus_factors, list) else [],
            "keep_as_core_candidates": keep_as_core if isinstance(keep_as_core, list) else [],
            "review_graveyard": review_graveyard[:max_review_graveyard] if isinstance(review_graveyard, list) else [],
            "portfolio_checks": portfolio_checks if isinstance(portfolio_checks, list) else [],
            "rationale": rationale.strip() if isinstance(rationale, str) else "",
        },
    }


def validate_plan_file(plan_path: str | Path, allowed_factor_names: set[str]) -> dict[str, Any]:
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    return validate_plan(plan, allowed_factor_names)
