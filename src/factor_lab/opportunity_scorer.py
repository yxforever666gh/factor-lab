from __future__ import annotations

from typing import Any

from factor_lab.regime_awareness import build_regime_context


TYPE_BASE = {
    "confirm": 0.76,
    "diagnose": 0.78,
    "expand": 0.68,
    "recombine": 0.64,
    "probe": 0.58,
    "archive": 0.35,
}


def score_opportunity(question: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, float | str]:
    analyst = snapshot.get("analyst_signals") or {}
    learning = snapshot.get("research_learning") or {}
    flow_state = snapshot.get("research_flow_state") or {}
    regime_context = build_regime_context(snapshot)

    qtype = question.get("question_type") or "probe"
    family = question.get("target_family")
    family_learning = (learning.get("families") or {}).get(family or "", {})
    parent_kind = "child" if question.get("parent_opportunity_id") else "root"
    template_key = f"{qtype}::{family or 'none'}::{parent_kind}"
    target_candidates = list(question.get("target_candidates") or [])
    if not target_candidates:
        target_shape = "no_targets"
    elif len(target_candidates) == 1:
        target_shape = "single_target"
    elif len(target_candidates) == 2:
        target_shape = "pair_target"
    else:
        target_shape = "multi_target"
    expected_gain = sorted([str(x) for x in (question.get("expected_knowledge_gain") or []) if x])
    intent_signature = "+".join(expected_gain[:3]) if expected_gain else "no_expected_gain"
    pattern_prefix = f"{template_key}::{target_shape}::{intent_signature}::"
    pattern_learning_candidates = learning.get("patterns") or {}
    pattern_learning = {}
    for key, value in pattern_learning_candidates.items():
        if str(key).startswith(pattern_prefix):
            pattern_learning = value
            break
    template_learning = (learning.get("templates") or {}).get(template_key, {})
    type_learning = (learning.get("types") or {}).get(qtype, {})

    priority = float(TYPE_BASE.get(qtype, 0.55))
    novelty = 0.45
    confidence = 0.60
    rationale_bits: list[str] = [f"base_type={qtype}"]

    if family_learning.get("cooldown_active"):
        priority -= 0.18
        confidence -= 0.10
        rationale_bits.append(f"family_learning=cooldown:{family_learning.get('cooldown_reason') or 'generic'}")
    elif family_learning.get("recommended_action") == "upweight":
        priority += 0.10
        confidence += 0.08
        rationale_bits.append("family_learning=upweight")
    elif family_learning.get("recommended_action") == "downweight":
        priority -= 0.08
        confidence -= 0.05
        rationale_bits.append("family_learning=downweight")

    family_epistemic_value = float(family_learning.get("epistemic_value_score") or 0.0)
    template_epistemic_value = float(template_learning.get("epistemic_value_score") or 0.0)
    pattern_epistemic_value = float(pattern_learning.get("epistemic_value_score") or 0.0)
    type_epistemic_value = float(type_learning.get("epistemic_value_score") or 0.0)
    epistemic_value = max(family_epistemic_value, template_epistemic_value, pattern_epistemic_value, type_epistemic_value)
    if epistemic_value >= 0.45:
        priority += 0.06
        novelty += 0.04
        confidence += 0.04
        rationale_bits.append("epistemic_learning=high_value")
    elif epistemic_value <= -0.25:
        priority -= 0.07
        confidence -= 0.05
        rationale_bits.append("epistemic_learning=low_value")

    if template_learning.get("cooldown_active"):
        priority -= 0.12
        confidence -= 0.07
        rationale_bits.append(f"template_learning=cooldown:{template_learning.get('cooldown_reason') or template_key}")
    elif template_learning.get("recommended_action") == "upweight":
        priority += 0.05
        novelty += 0.05
        confidence += 0.03
        rationale_bits.append(f"template_learning=upweight:{template_key}")
    elif template_learning.get("recommended_action") == "downweight":
        priority -= 0.06
        confidence -= 0.04
        rationale_bits.append(f"template_learning=downweight:{template_key}")

    if pattern_learning.get("cooldown_active"):
        priority -= 0.14
        confidence -= 0.08
        novelty -= 0.04
        rationale_bits.append(f"pattern_learning=cooldown:{pattern_learning.get('cooldown_reason') or pattern_prefix}")
    elif pattern_learning.get("recommended_action") == "upweight":
        priority += 0.04
        novelty += 0.06
        confidence += 0.03
        rationale_bits.append(f"pattern_learning=upweight:{pattern_prefix}")
    elif pattern_learning.get("recommended_action") == "downweight":
        priority -= 0.05
        confidence -= 0.04
        rationale_bits.append(f"pattern_learning=downweight:{pattern_prefix}")

    if int(template_learning.get("recent_resource_exhaustion_count") or 0) > 0 or int(pattern_learning.get("recent_resource_exhaustion_count") or 0) > 0:
        priority -= 0.08
        confidence -= 0.05
        rationale_bits.append("recent_resource_exhaustion_penalty")

    if (
        qtype in {"expand", "recombine", "probe"}
        and int(pattern_learning.get("recent_no_gain_count") or 0) >= 2
        and int(pattern_learning.get("recent_gain_count") or 0) == 0
    ):
        priority -= 0.09
        confidence -= 0.06
        rationale_bits.append("recent_no_gain_pattern_penalty")

    if flow_state.get("state") == "recovering" and qtype in {"confirm", "diagnose"}:
        priority += 0.06
        rationale_bits.append("recovering_prefers_confirm_diagnose")
    candidate_count = int(flow_state.get("candidate_count") or 0)
    if flow_state.get("state") == "recovered" and qtype in {"expand", "recombine"}:
        priority += 0.07
        novelty += 0.08
        rationale_bits.append("recovered_prefers_expand_recombine")
    if flow_state.get("state") == "recovered" and candidate_count > 0 and qtype in {"confirm", "diagnose"}:
        priority += 0.05
        confidence += 0.03
        rationale_bits.append("recovered_with_candidates_prefers_validation")

    targets = set(question.get("target_candidates") or [])
    analyst_focus = set(analyst.get("focus_factors") or [])
    analyst_graveyard = set(analyst.get("review_graveyard") or [])
    if targets & analyst_focus:
        priority += 0.05
        confidence += 0.03
        rationale_bits.append("analyst_focus_overlap")
    if targets & analyst_graveyard:
        priority += 0.05
        rationale_bits.append("analyst_graveyard_overlap")

    if qtype in {"recombine", "probe"}:
        novelty += 0.18
    elif qtype == "expand":
        novelty += 0.10
    elif qtype == "diagnose":
        novelty += 0.06

    regime = regime_context.get("regime") or "neutral"
    regime_weight = float(((regime_context.get("weights") or {}).get(qtype) or 1.0))
    regime_confidence = float(regime_context.get("confidence") or 0.0)
    priority *= regime_weight
    if regime_weight > 1.0:
        novelty += min(0.08, (regime_weight - 1.0) * 0.15)
    else:
        novelty -= min(0.06, (1.0 - regime_weight) * 0.12)
    confidence += (regime_confidence - 0.5) * 0.12
    rationale_bits.append(f"regime={regime}")
    rationale_bits.append(f"regime_weight={regime_weight:.2f}")

    priority = min(max(priority, 0.05), 0.99)
    novelty = min(max(novelty, 0.05), 0.99)
    confidence = min(max(confidence, 0.05), 0.99)
    return {
        "priority": round(priority, 3),
        "novelty_score": round(novelty, 3),
        "confidence": round(confidence, 3),
        "score_rationale": "; ".join(rationale_bits),
        "regime": regime,
        "regime_confidence": round(regime_confidence, 3),
    }
