from __future__ import annotations

from typing import Any

from factor_lab.exploration_pools import NEW_MECHANISM_POOL


def _question_card(
    card_id: str,
    *,
    candidate_name: str,
    question_type: str,
    prompt: str,
    route_bias: str,
    expected_information_gain: list[str],
    target_pool: str = NEW_MECHANISM_POOL,
    priority: int = 50,
    evidence: list[str] | None = None,
    preferred_context_mode: str | None = None,
    allowed_operators: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "card_id": card_id,
        "candidate_name": candidate_name,
        "question_type": question_type,
        "prompt": prompt,
        "route_bias": route_bias,
        "expected_information_gain": expected_information_gain,
        "target_pool": target_pool,
        "priority": int(priority),
        "evidence": list(evidence or []),
        "preferred_context_mode": preferred_context_mode,
        "allowed_operators": list(allowed_operators or []),
    }


def build_failure_question_cards(representative_failure_dossiers: dict[str, Any] | None) -> list[dict[str, Any]]:
    dossiers = representative_failure_dossiers or {}
    cards: list[dict[str, Any]] = []

    for candidate_name, dossier in dossiers.items():
        if not candidate_name:
            continue
        failure_modes = set(dossier.get("failure_modes") or [])
        failure_mode_counts = dict(dossier.get("failure_mode_counts") or {})
        evidence = list(dossier.get("evidence") or [])
        regime_dependency = dossier.get("regime_dependency") or "unclear"
        parent_delta_status = dossier.get("parent_delta_status") or "unknown"
        recommended_action = dossier.get("recommended_action") or "keep_validating"
        neutralized_break = ("neutralized_break" in failure_modes) or int(dossier.get("neutralized_break_count") or failure_mode_counts.get("neutralized_break") or 0) >= 1
        parent_non_incremental = parent_delta_status == "non_incremental" or "non_incremental_vs_parent" in failure_modes
        short_window_only = (
            regime_dependency == "short_window_only"
            or "short_to_medium_decay" in failure_modes
            or "medium_to_long_decay" in failure_modes
            or int(dossier.get("decay_45_to_60") or 0) >= 1
            or int(dossier.get("decay_45_to_90") or 0) >= 1
        )

        if neutralized_break or regime_dependency == "exposure_dependent":
            cards.append(
                _question_card(
                    f"question::{candidate_name}::neutralization_collapse",
                    candidate_name=candidate_name,
                    question_type="neutralization_collapse",
                    prompt=f"为 {candidate_name} 生成更偏 idiosyncratic / neutralized 后仍能存活的机制路线，尽量避开原有暴露轴。",
                    route_bias="cross_family_idiosyncratic",
                    expected_information_gain=["candidate_survival_check", "new_branch_opened", "search_space_expanded"],
                    priority=92,
                    evidence=evidence,
                    preferred_context_mode="far_family",
                    allowed_operators=["combine_sub", "combine_ratio", "combine_mul", "combine_primary_bias"],
                )
            )

        if parent_non_incremental:
            cards.append(
                _question_card(
                    f"question::{candidate_name}::parent_non_incremental",
                    candidate_name=candidate_name,
                    question_type="parent_non_incremental",
                    prompt=f"为 {candidate_name} 生成刻意绕开 parent 信息轴的远邻 / 跨 family 组合，重点验证 incremental value。",
                    route_bias="far_family_incremental",
                    expected_information_gain=["new_branch_opened", "candidate_survival_check", "boundary_confirmed"],
                    priority=95,
                    evidence=evidence,
                    preferred_context_mode="far_family",
                    allowed_operators=["combine_sub", "combine_ratio", "combine_mul", "combine_avg"],
                )
            )

        if short_window_only:
            cards.append(
                _question_card(
                    f"question::{candidate_name}::medium_long_persistence",
                    candidate_name=candidate_name,
                    question_type="medium_long_persistence",
                    prompt=f"为 {candidate_name} 生成更强调 medium/long-window persistence 的新机制候选，避免只在短窗有效。",
                    route_bias="persistence_template",
                    expected_information_gain=["candidate_survival_check", "boundary_confirmed"],
                    priority=88,
                    evidence=evidence,
                    preferred_context_mode="cross_family_or_quality",
                    allowed_operators=["combine_primary_bias", "combine_ratio", "combine_avg"],
                )
            )

        if recommended_action in {"diagnose", "suppress"} and not any(card["candidate_name"] == candidate_name for card in cards):
            cards.append(
                _question_card(
                    f"question::{candidate_name}::general_diagnose",
                    candidate_name=candidate_name,
                    question_type="general_diagnose",
                    prompt=f"围绕 {candidate_name} 的失败模式生成更远距离的新机制探索题，不要继续在旧邻域做微调。",
                    route_bias="new_mechanism",
                    expected_information_gain=["search_space_expanded", "new_branch_opened"],
                    priority=80,
                    evidence=evidence,
                    preferred_context_mode="far_family",
                    allowed_operators=["combine_sub", "combine_ratio", "combine_mul", "combine_avg"],
                )
            )

    cards.sort(key=lambda row: (-int(row.get("priority") or 0), row.get("candidate_name") or "", row.get("card_id") or ""))
    return cards
