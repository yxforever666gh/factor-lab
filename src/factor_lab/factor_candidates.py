from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any

from factor_lab.exposure_scorecard import classify_bucket


GOOD_STATUSES = {"promising", "testing", "fragile"}
BAD_STATUSES = {"rejected", "archived"}
OFFICIAL_RUN_SCOPES = {"official", "generated", "batch_official"}


ACCEPTANCE_GATE_20_THRESHOLDS = {
    "promising": {
        "min_official_eval_count": 5,
        "min_official_window_count": 3,
        "min_pass_rate": 0.6,
        "min_avg_score": 1.0,
        "max_risk_score": 44.0,
    },
    "testing": {
        "min_official_eval_count": 2,
        "min_official_window_count": 1,
        "min_pass_rate": 0.2,
        "min_avg_score": 0.5,
        "max_risk_score": 64.0,
    },
    "fragile": {
        "max_risk_score": 84.0,
    },
}


def infer_factor_family(name: str, expression: str | None = None) -> str:
    haystack = f"{name} {expression or ''}".lower()
    quality_hit = any(token in haystack for token in ["quality", "profit", "roe", "margin"])
    value_hit = any(token in haystack for token in ["value", "yield", "ep", "bp", "pb"])
    liquidity_hit = any(token in haystack for token in ["liq", "turnover", "volume"])
    momentum_hit = "mom" in haystack or "momentum" in haystack
    volatility_hit = any(token in haystack for token in ["vol", "variance", "std", "atr"])
    combo_hit = any(token in haystack for token in ["+", "-", "*", "/", "combo", "hybrid"])

    family_hits = sum([quality_hit, value_hit, liquidity_hit, momentum_hit, volatility_hit])
    if combo_hit and family_hits >= 2:
        return "hybrid"
    if momentum_hit:
        return "momentum"
    if quality_hit:
        return "quality"
    if value_hit:
        return "value"
    if volatility_hit:
        return "volatility"
    if liquidity_hit:
        return "liquidity"
    if combo_hit:
        return "hybrid"
    return "other"


def derive_window_label(config_path: str | None, start_date: str | None, end_date: str | None) -> str:
    config_path = config_path or ""
    if "recent" in config_path:
        return "recent"
    if "expanding" in config_path:
        return "expanding"
    if "rolling_" in config_path:
        stem = config_path.rsplit("/", 1)[-1].replace(".json", "")
        return stem
    if start_date and end_date:
        return f"{start_date}->{end_date}"
    return config_path or "unknown"


def _evaluate_fragility(status_pool: list[dict[str, Any]], avg_score: float, best_score: float, pass_rate: float) -> dict[str, Any]:
    split_fail_max = max(int(row.get("split_fail_count") or 0) for row in status_pool) if status_pool else 0
    high_corr_max = max(int(row.get("high_corr_peer_count") or 0) for row in status_pool) if status_pool else 0
    robust_total = max(int(row.get("robust_total_count") or 0) for row in status_pool) if status_pool else 0
    robust_pass = max(int(row.get("robust_pass_count") or 0) for row in status_pool) if status_pool else 0
    neutral_breaks = len([
        row for row in status_pool
        if float(row.get("neutralized_rank_ic_mean") or 0.0) < 0 and float(row.get("raw_rank_ic_mean") or 0.0) > 0
    ])
    latest_status = (status_pool[-1].get("status") if status_pool else None) or "testing"
    robustness_ratio = robust_pass / max(robust_total, 1)

    trigger_bits: list[str] = []
    if split_fail_max >= 1:
        trigger_bits.append(f"split_fail_max={split_fail_max}")
    if robust_total and robustness_ratio < 0.6:
        trigger_bits.append(f"robustness_ratio={robustness_ratio:.2f}")
    if neutral_breaks:
        trigger_bits.append(f"neutralization_breaks={neutral_breaks}")
    if high_corr_max >= 2:
        trigger_bits.append(f"high_corr_peer_max={high_corr_max}")
    if pass_rate < 0.6 and avg_score < 1.0 and best_score >= 1.5:
        trigger_bits.append("peak_without_repeatability")
    if latest_status in {"archived"} and best_score >= 1.2:
        trigger_bits.append("latest_window_regression")

    risk_score = min(
        100.0,
        split_fail_max * 22.0
        + max(0.0, (0.6 - robustness_ratio)) * 55.0
        + neutral_breaks * 14.0
        + high_corr_max * 7.0
        + (18.0 if pass_rate < 0.35 and best_score >= 1.5 else 0.0)
        + (10.0 if avg_score < 0.5 < best_score else 0.0),
    )
    is_fragile = bool(trigger_bits) and risk_score >= 25.0
    return {
        "is_fragile": is_fragile,
        "risk_score": round(risk_score, 6),
        "trigger_bits": trigger_bits,
        "robustness_ratio": round(robustness_ratio, 6) if robust_total else None,
        "split_fail_max": split_fail_max,
        "high_corr_peer_max": high_corr_max,
        "neutralization_break_count": neutral_breaks,
    }


def _build_acceptance_gate_20(status: str, *, official_eval_count: int, official_window_count: int, avg_score: float, pass_rate: float, fragility: dict[str, Any]) -> dict[str, Any]:
    thresholds = ACCEPTANCE_GATE_20_THRESHOLDS
    promising_ok = (
        official_eval_count >= thresholds["promising"]["min_official_eval_count"]
        and official_window_count >= thresholds["promising"]["min_official_window_count"]
        and pass_rate >= thresholds["promising"]["min_pass_rate"]
        and avg_score >= thresholds["promising"]["min_avg_score"]
        and float(fragility.get("risk_score") or 0.0) <= thresholds["promising"]["max_risk_score"]
        and not fragility.get("is_fragile")
    )
    testing_ok = (
        official_eval_count >= thresholds["testing"]["min_official_eval_count"]
        and official_window_count >= thresholds["testing"]["min_official_window_count"]
        and (pass_rate >= thresholds["testing"]["min_pass_rate"] or avg_score >= thresholds["testing"]["min_avg_score"])
        and float(fragility.get("risk_score") or 0.0) <= thresholds["testing"]["max_risk_score"]
    )
    fragile_gate = {
        "allowed_but_blocked_from_refinement": status == "fragile",
        "risk_score": fragility.get("risk_score"),
        "triggers": fragility.get("trigger_bits") or [],
    }

    if promising_ok:
        outcome = "pass"
        promotion = "eligible_for_refinement"
        explanation = "Acceptance Gate 2.0 passed: repeatability is strong and fragility checks are clean."
    elif testing_ok and status == "testing":
        outcome = "monitor"
        promotion = "needs_more_validation"
        explanation = "Acceptance Gate 2.0 monitor: candidate is usable for validation, but evidence is not deep enough for refinement."
    elif status == "fragile":
        outcome = "blocked"
        promotion = "route_to_robustness_validation"
        explanation = "Acceptance Gate 2.0 blocked refinement: the candidate shows some alpha, but fragility/risk checks failed and robustness validation must come first."
    else:
        outcome = "fail"
        promotion = "do_not_refine"
        explanation = "Acceptance Gate 2.0 failed: evidence quality or repeatability is insufficient."

    return {
        "version": "2.0",
        "status": outcome,
        "promotion": promotion,
        "explanation": explanation,
        "thresholds": thresholds,
        "metrics": {
            "official_eval_count": official_eval_count,
            "official_window_count": official_window_count,
            "avg_score": round(avg_score, 6),
            "pass_rate": round(pass_rate, 6),
            "fragility_risk_score": fragility.get("risk_score"),
        },
        "fragility_gate": fragile_gate,
    }


def score_candidate_evaluation(metrics: dict[str, Any]) -> dict[str, Any]:
    sample_size = int(metrics.get("sample_size") or 0)
    return_metric = float(metrics.get("return_metric") or 0.0)
    sharpe_like = float(metrics.get("sharpe_like") or 0.0)
    max_drawdown = float(metrics.get("max_drawdown") or 0.0)
    turnover = float(metrics.get("turnover") or 0.0)
    coverage = float(metrics.get("coverage") or 0.0)
    raw_ic = float(metrics.get("raw_rank_ic_mean") or 0.0)
    neutral_ic = float(metrics.get("neutralized_rank_ic_mean") or 0.0)
    split_fail_count = int(metrics.get("split_fail_count") or 0)
    high_corr_peer_count = int(metrics.get("high_corr_peer_count") or 0)
    observations = int(metrics.get("observations") or 0)
    robust_pass_count = int(metrics.get("robust_pass_count") or 0)
    robust_total_count = int(metrics.get("robust_total_count") or 0)
    run_scope = metrics.get("run_scope") or "official"

    clipped_return = max(min(return_metric, 1.5), -1.5)
    clipped_sharpe = max(min(sharpe_like, 4.0), -2.0)
    drawdown_penalty = abs(min(max_drawdown, 0.0))
    scope_multiplier = 0.35 if run_scope == "demo" else 1.0

    stability_score = (
        raw_ic * 3.2
        + max(neutral_ic, -0.1) * 2.4
        + clipped_sharpe * 0.35
        + min(coverage, 1.0) * 0.9
        + min(sample_size / 240.0, 1.2) * 0.7
        + min(observations / 240.0, 1.2) * 0.55
        + (robust_pass_count / max(robust_total_count, 1)) * 0.9
        - split_fail_count * 0.55
        - high_corr_peer_count * 0.12
    ) * scope_multiplier

    quality_score = (
        clipped_return * 1.4
        + clipped_sharpe * 0.55
        + max(raw_ic, -0.1) * 2.1
        + max(neutral_ic, -0.1) * 1.8
        - drawdown_penalty * 4.0
        - turnover * 0.45
    ) * scope_multiplier

    final_score = stability_score + quality_score
    rejection_reasons: list[str] = []
    if sample_size < 80:
        rejection_reasons.append("sample_too_small")
    if observations < 40:
        rejection_reasons.append("insufficient_observations")
    if coverage < 0.2:
        rejection_reasons.append("coverage_too_low")
    if max_drawdown < -0.30:
        rejection_reasons.append("drawdown_too_deep")
    if split_fail_count >= 2:
        rejection_reasons.append("too_many_split_failures")
    if neutral_ic < 0 and raw_ic > 0:
        rejection_reasons.append("neutralization_breaks_signal")
    if sharpe_like < 0:
        rejection_reasons.append("negative_sharpe")
    if return_metric < 0:
        rejection_reasons.append("negative_return")

    fragility_signals = [
        split_fail_count >= 1,
        robust_total_count > 0 and (robust_pass_count / max(robust_total_count, 1)) < 0.6,
        neutral_ic < 0 < raw_ic,
        high_corr_peer_count >= 2,
    ]
    pass_flag = not rejection_reasons and final_score >= 1.0
    if pass_flag and final_score >= 2.4 and not any(fragility_signals):
        status = "promising"
    elif pass_flag and any(fragility_signals):
        status = "fragile"
    elif pass_flag:
        status = "testing"
    elif final_score <= -0.75 or len(rejection_reasons) >= 2:
        status = "rejected"
    elif any(fragility_signals) and final_score >= 0:
        status = "fragile"
    else:
        status = "archived"

    return {
        "stability_score": round(stability_score, 6),
        "quality_score": round(quality_score, 6),
        "final_score": round(final_score, 6),
        "pass_flag": int(pass_flag),
        "status": status,
        "rejection_reason": "; ".join(rejection_reasons) if rejection_reasons else None,
    }


def _is_recent_evaluation(row: dict[str, Any]) -> bool:
    label = str(row.get("window_label") or "").lower()
    notes = row.get("notes") or {}
    config_path = str(notes.get("config_path") or "").lower()
    haystack = " ".join([label, config_path])
    recent_tokens = (
        "recent_30d",
        "recent_45d",
        "rolling_recent_30d",
        "rolling_recent_45d",
        "rolling_30d_back",
        "rolling_45d_back",
        "probe_recent_30d",
    )
    return any(token in haystack for token in recent_tokens) or label == "recent"



def _derive_research_stage(
    *,
    status: str,
    avg_score: float,
    best_score: float,
    pass_rate: float,
    official_window_count: int,
    fragility: dict[str, Any],
) -> str:
    if status in {"rejected", "archived"} and best_score < 1.0:
        return "graveyard"
    if status == "promising" and official_window_count >= 3 and pass_rate >= 0.6 and not fragility.get("is_fragile"):
        return "candidate"
    if status in {"promising", "testing", "fragile"} and (pass_rate >= 0.2 or avg_score >= 0.5 or best_score >= 1.0):
        return "watchlist"
    if best_score > 0 or avg_score > 0:
        return "explore"
    return "graveyard"


def summarize_candidate_status(evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    if not evaluations:
        gate = _build_acceptance_gate_20(
            "new",
            official_eval_count=0,
            official_window_count=0,
            avg_score=0.0,
            pass_rate=0.0,
            fragility={"risk_score": 0.0, "trigger_bits": [], "is_fragile": False},
        )
        return {
            "status": "new",
            "research_stage": "explore",
            "evaluation_count": 0,
            "window_count": 0,
            "avg_final_score": None,
            "best_final_score": None,
            "latest_final_score": None,
            "latest_recent_final_score": None,
            "pass_rate": None,
            "summary": "No evaluations yet.",
            "next_action": "seed_validation",
            "rejection_reason": None,
            "fragility": {"is_fragile": False, "risk_score": 0.0, "trigger_bits": []},
            "acceptance_gate": gate,
            "acceptance_gate_explanation": gate["explanation"],
        }

    evaluations = sorted(evaluations, key=lambda row: row.get("created_at_utc") or "")
    scores = [float(row.get("final_score") or 0.0) for row in evaluations]
    windows = {row.get("window_label") or "unknown" for row in evaluations}
    latest_score = round(scores[-1], 6)
    best_score = round(max(scores), 6)

    official_evaluations = [
        row for row in evaluations
        if (row.get("notes") or {}).get("run_scope") in OFFICIAL_RUN_SCOPES
    ]
    recent_official_evaluations = [row for row in official_evaluations if _is_recent_evaluation(row)]
    status_pool = official_evaluations or evaluations
    pass_flags = [int(row.get("pass_flag") or 0) for row in status_pool]
    statuses = [row.get("status") or "testing" for row in status_pool]
    official_scores = [float(row.get("final_score") or 0.0) for row in status_pool]
    official_windows = {row.get("window_label") or "unknown" for row in status_pool}
    status_counter = Counter(statuses)
    avg_score = round(sum(official_scores) / len(status_pool), 6)
    pass_rate = round(sum(pass_flags) / len(status_pool), 4)
    official_eval_count = len(status_pool)
    official_window_count = len(official_windows)
    latest_recent_score = None
    if recent_official_evaluations:
        latest_recent_score = round(float(recent_official_evaluations[-1].get("final_score") or 0.0), 6)
    fragility = _evaluate_fragility(status_pool, avg_score, best_score, pass_rate)

    if official_eval_count >= 5 and official_window_count >= 3 and pass_rate >= 0.6 and avg_score >= 1.0 and not fragility["is_fragile"]:
        status = "promising"
        next_action = "refine"
    elif fragility["is_fragile"] and best_score >= 1.0:
        status = "fragile"
        next_action = "run_robustness_validation"
    elif official_eval_count >= 2 and official_window_count >= 1 and (pass_rate >= 0.2 or avg_score >= 0.5 or best_score >= 1.5):
        status = "testing"
        next_action = "validate_more_windows"
    elif official_eval_count <= 1 and len(evaluations) >= 1:
        status = "testing"
        next_action = "validate_more_windows"
    elif status_counter.get("archived", 0) >= 2 and status_counter.get("promising", 0) == 0:
        status = "archived"
        next_action = "low_priority_retest"
    else:
        status = "rejected"
        next_action = "stop"

    rejection_reasons = [row.get("rejection_reason") for row in status_pool if row.get("rejection_reason")]
    rejection_reason = rejection_reasons[-1] if rejection_reasons else None
    research_stage = _derive_research_stage(
        status=status,
        avg_score=avg_score,
        best_score=best_score,
        pass_rate=pass_rate,
        official_window_count=official_window_count,
        fragility=fragility,
    )
    gate = _build_acceptance_gate_20(
        status,
        official_eval_count=official_eval_count,
        official_window_count=official_window_count,
        avg_score=avg_score,
        pass_rate=pass_rate,
        fragility=fragility,
    )
    summary = (
        f"{len(evaluations)} evals ({official_eval_count} official) across {len(windows)} windows "
        f"({official_window_count} official); stage={research_stage}; avg_score={avg_score}, latest={scores[-1]:.3f}"
        + (f", latest_recent={latest_recent_score:.3f}" if latest_recent_score is not None else "")
        + f", pass_rate={pass_rate:.2f}."
    )
    if fragility["is_fragile"]:
        summary += f" Fragile triggers: {', '.join(fragility['trigger_bits'])}."
    return {
        "status": status,
        "research_stage": research_stage,
        "evaluation_count": len(evaluations),
        "window_count": len(windows),
        "avg_final_score": avg_score,
        "best_final_score": best_score,
        "latest_final_score": latest_score,
        "latest_recent_final_score": latest_recent_score,
        "pass_rate": pass_rate,
        "summary": summary,
        "next_action": next_action,
        "rejection_reason": rejection_reason,
        "fragility": fragility,
        "acceptance_gate": gate,
        "acceptance_gate_explanation": gate["explanation"],
    }


def build_hypothesis_summary(candidate: dict[str, Any], evaluations: list[dict[str, Any]]) -> dict[str, Any]:
    family = candidate.get("family") or infer_factor_family(candidate.get("name", ""), None)
    promising_windows = [row.get("window_label") for row in evaluations if row.get("status") == "promising"]
    rejected_windows = [row.get("window_label") for row in evaluations if row.get("status") in BAD_STATUSES]
    fragile_windows = [row.get("window_label") for row in evaluations if row.get("status") == "fragile"]
    positive_scores = [float(row.get("final_score") or 0.0) for row in evaluations if float(row.get("final_score") or 0.0) > 0]
    candidate_name = candidate.get("name") or "candidate"
    evidence_for = []
    evidence_against = []
    if promising_windows:
        evidence_for.append(f"支持窗口: {', '.join(promising_windows[:4])}")
    if positive_scores:
        evidence_for.append(f"正分评估数: {len(positive_scores)} / {len(evaluations)}")
    if promising_windows and rejected_windows:
        evidence_for.append("存在条件性支持，值得做窗口/阶段边界验证")
    if fragile_windows:
        evidence_against.append(f"脆弱窗口: {', '.join(fragile_windows[:4])}")
    if rejected_windows:
        evidence_against.append(f"弱势窗口: {', '.join(rejected_windows[:4])}")
    recent_reason = next((row.get("rejection_reason") for row in reversed(evaluations) if row.get("rejection_reason")), None)
    if recent_reason:
        evidence_against.append(f"最近失效信号: {recent_reason}")

    candidate_status = candidate.get("status") or "testing"
    if candidate_status == "promising":
        next_action = "expand same family with nearby variants"
    elif candidate_status == "fragile":
        next_action = "run robustness and validation before any refinement"
    elif candidate_status == "rejected":
        next_action = "stop expanding this branch"
    elif candidate_status == "archived":
        next_action = "only retest if stronger context appears"
    else:
        next_action = "collect more validation windows"

    if promising_windows and not rejected_windows:
        target_window = "medium_horizon"
    elif promising_windows:
        target_window = "recent_extension"
    else:
        target_window = "short_window_recheck"
    mechanism_note = f"{candidate_name} 代表 {family} family 的一个条件性 alpha 假设，需要确认它是否真的提供独立增量，而不是复制现有风格暴露。"
    invalidation_bits = []
    if fragile_windows:
        invalidation_bits.append("更多窗口出现 split/rolling fragility")
    if rejected_windows:
        invalidation_bits.append("在更长窗口里持续掉出 watchlist/candidate")
    if recent_reason and recent_reason not in invalidation_bits:
        invalidation_bits.append(recent_reason)
    if invalidation_bits:
        evidence_against.append(f"证伪条件: {'；'.join(invalidation_bits[:3])}")
    evidence_for.append(f"目标窗口: {target_window}")
    evidence_for.append("增量价值主张: 若成立，应比现有 frontier 多提供一层 family/窗口上的可解释差异")

    title = f"{family} hypothesis: {candidate_name}"
    hypothesis_text = mechanism_note
    return {
        "title": title,
        "family": family,
        "hypothesis_text": hypothesis_text,
        "status": candidate_status,
        "evidence_for_json": json.dumps(evidence_for, ensure_ascii=False),
        "evidence_against_json": json.dumps(evidence_against, ensure_ascii=False),
        "next_action": next_action,
    }


def _institutional_bucket_from_candidate(name: str, expression: str | None = None) -> tuple[str, str, str, str]:
    bucket_key, bucket_label = classify_bucket(name, expression)
    mapping = {
        "raw_exposure": ("exposure_regime", "Exposure / Regime Track"),
        "controlled_composite": ("controlled_composite", "Controlled Composite"),
        "residual_like": ("residual_like_alpha", "Residual-like / Alpha"),
    }
    institutional_key, institutional_label = mapping[bucket_key]
    return bucket_key, bucket_label, institutional_key, institutional_label


def _thesis_type_from_family(family: str, factor_role: str | None = None) -> str:
    if factor_role == "exposure_probe":
        return "regime_exposure"
    mapping = {
        "momentum": "behavioral_continuation",
        "value": "valuation_mean_reversion",
        "quality": "quality_persistence",
        "liquidity": "liquidity_shock",
        "size": "trading_frictions",
        "volatility": "risk_transfer",
    }
    return mapping.get((family or "other").lower(), "cross_sectional_anomaly")


def build_research_thesis_summary(
    candidate: dict[str, Any],
    evaluations: list[dict[str, Any]],
    representative_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    representative_context = representative_context or {}
    definition = dict(candidate.get("definition") or {})
    candidate_name = candidate.get("name") or definition.get("name") or "candidate"
    expression = candidate.get("expression") or definition.get("expression")
    family = candidate.get("family") or infer_factor_family(candidate_name, expression)
    factor_role = candidate.get("factor_role") or definition.get("role")
    thesis_type = _thesis_type_from_family(family, factor_role)
    source_template_id = definition.get("hypothesis_template_id") or definition.get("template_id")
    source_template_label = definition.get("hypothesis_template_label") or definition.get("template_label")
    bucket_key, bucket_label, institutional_bucket_key, institutional_bucket_label = _institutional_bucket_from_candidate(candidate_name, expression)

    promising_windows = [row.get("window_label") for row in evaluations if row.get("status") == "promising" and row.get("window_label")]
    rejected_windows = [row.get("window_label") for row in evaluations if row.get("status") in BAD_STATUSES and row.get("window_label")]
    fragile_windows = [row.get("window_label") for row in evaluations if row.get("status") == "fragile" and row.get("window_label")]
    recent_reason = next((row.get("rejection_reason") for row in reversed(evaluations) if row.get("rejection_reason")), None)

    mechanism_bits = [
        f"{candidate_name} 围绕 {family} family 的 {thesis_type} 命题展开。",
        f"当前制度桶归类为 {institutional_bucket_label}（表达式桶={bucket_label}）。",
    ]
    if source_template_label or source_template_id:
        mechanism_bits.append(f"来源模板={source_template_label or source_template_id}。")
    if definition.get("generator_operator"):
        mechanism_bits.append(f"生成算子={definition.get('generator_operator')}。")
    if factor_role:
        mechanism_bits.append(f"factor_role={factor_role}。")

    invalidation_bits: list[str] = []
    if fragile_windows:
        invalidation_bits.append(f"更多窗口继续出现 fragility: {', '.join(fragile_windows[:4])}")
    if rejected_windows:
        invalidation_bits.append(f"中长窗继续失守: {', '.join(rejected_windows[:4])}")
    if recent_reason:
        invalidation_bits.append(recent_reason)
    if not invalidation_bits:
        invalidation_bits.append("若后续验证显示没有增量价值或长期窗口无法延展，则该 thesis 应降级。")

    roster = representative_context.get("representative_candidates") or []
    primary_representative = representative_context.get("primary_candidate") or representative_context.get("representative_candidate") or candidate_name
    thesis_id = source_template_id or f"{family}:{thesis_type}:{institutional_bucket_key}"
    thesis_title = source_template_label or f"{family} thesis / {institutional_bucket_label}"
    source_context = {
        "hypothesis_template_id": source_template_id,
        "hypothesis_template_label": source_template_label,
        "question_card_id": definition.get("question_card_id"),
        "generator_operator": definition.get("generator_operator"),
        "parent_factor_name": definition.get("parent_factor_name") or definition.get("left_factor_name"),
        "paired_factor_name": definition.get("right_factor_name"),
        "factor_role": factor_role,
        "promising_windows": promising_windows[:6],
        "rejected_windows": rejected_windows[:6],
    }

    return {
        "thesis_id": thesis_id,
        "title": thesis_title,
        "family": family,
        "thesis_type": thesis_type,
        "institutional_bucket_key": institutional_bucket_key,
        "institutional_bucket_label": institutional_bucket_label,
        "thesis_text": " ".join(mechanism_bits),
        "mechanism_rationale": " ".join(mechanism_bits),
        "status": candidate.get("status") or "testing",
        "invalidation_json": json.dumps(invalidation_bits[:5], ensure_ascii=False),
        "representative_candidate": primary_representative,
        "representative_rank": representative_context.get("representative_rank"),
        "representative_count": representative_context.get("representative_count"),
        "roster_json": json.dumps(roster, ensure_ascii=False),
        "source_context_json": json.dumps(source_context, ensure_ascii=False),
    }


def grouped_evaluation_windows(evaluations: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evaluations:
        grouped[row.get("window_label") or "unknown"].append(row)
    return dict(grouped)
