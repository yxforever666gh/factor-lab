from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.exploration_budget import exploration_floor_context
from factor_lab.candidate_triage import load_candidate_triage_model, score_generation_proposal
from factor_lab.exploration_pools import (
    NEW_MECHANISM_POOL,
    OLD_SPACE_POOL,
    classify_exploration_pool,
    split_exploration_pool_budget,
)
from factor_lab.regime_awareness import build_regime_context
from factor_lab.hypothesis_generator_llm import generate_hypothesis_routes
from factor_lab.novelty_judge_llm import judge_generation_proposal

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "configs" / "candidate_generation_policy.json"
EVIDENCE_POLICY_PATH = ROOT / "configs" / "research_evidence_policy.json"
HYPOTHESIS_TEMPLATE_PATH = ROOT / "configs" / "hypothesis_templates_v1.json"
PRIMITIVE_LIBRARY_PATH = ROOT / "configs" / "research_factor_primitives.json"
LEARNING_PATH = ROOT / "artifacts" / "research_learning.json"


def _read_json(path: str | Path, default: Any) -> Any:
    path = Path(path)
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            obj, _ = json.JSONDecoder().raw_decode(text)
            return obj
        except Exception:
            return default


def _factor_context(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get("candidate_context") or []
    context = {row.get("candidate_name"): row for row in rows if row.get("candidate_name")}
    primitive_rows = list((_read_json(PRIMITIVE_LIBRARY_PATH, {}) or {}).get("factors") or [])
    for row in primitive_rows:
        name = row.get("name") if isinstance(row, dict) else None
        if not name or name in context:
            continue
        context[name] = {
            "candidate_name": name,
            "family": row.get("family") or "other",
            "relationship_count": 0,
            "robustness_score": None,
            "is_primary_candidate": False,
        }
    return context


def _high_value_failure_seeds(memory: dict[str, Any]) -> list[str]:
    seeds: list[str] = []
    for row in list(memory.get("execution_feedback") or [])[-40:]:
        if row.get("outcome_class") != "high_value_failure":
            continue
        for name in row.get("focus_candidates") or []:
            if name and name not in seeds:
                seeds.append(name)
    for row in list(memory.get("generated_candidate_outcomes") or [])[-60:]:
        if row.get("outcome_class") != "high_value_failure":
            continue
        for name in row.get("base_factors") or []:
            if name and name not in seeds:
                seeds.append(name)
    return seeds


def _high_value_failure_routes(memory: dict[str, Any]) -> list[dict[str, Any]]:
    routes: list[dict[str, Any]] = []
    for row in list(memory.get("generated_candidate_outcomes") or [])[-60:]:
        if row.get("outcome_class") != "high_value_failure":
            continue
        bases = [name for name in (row.get("base_factors") or []) if name]
        if len(bases) < 2:
            continue
        routes.append({
            "base_factors": bases[:2],
            "target_family": row.get("target_family"),
            "failed_operator": row.get("operator"),
            "source": row.get("source") or "high_value_failure_seed",
        })
    return routes


def _generation_constraints(memory: dict[str, Any]) -> dict[str, Any]:
    blocked_operator_pairs: set[tuple[str, str, str]] = set()
    blocked_pairs: set[tuple[str, str]] = set()
    pair_failure_counts: dict[tuple[str, str], int] = {}
    for row in list(memory.get("generated_candidate_outcomes") or [])[-80:]:
        bases = tuple(sorted([name for name in (row.get("base_factors") or []) if name]))
        if len(bases) != 2:
            continue
        operator = row.get("operator") or "unknown"
        outcome_class = row.get("outcome_class")
        if outcome_class in {"high_value_failure", "execution_failure"}:
            blocked_operator_pairs.add((bases[0], bases[1], operator))
        if outcome_class == "low_value_repeat":
            blocked_pairs.add(bases)
        if outcome_class in {"high_value_failure", "execution_failure", "low_value_repeat"}:
            pair_failure_counts[bases] = int(pair_failure_counts.get(bases) or 0) + 1
    return {
        "blocked_operator_pairs": blocked_operator_pairs,
        "blocked_pairs": blocked_pairs,
        "pair_failure_counts": pair_failure_counts,
    }


def _candidate_evidence_gate(candidate_context: dict[str, Any], evidence_policy: dict[str, Any]) -> dict[str, Any]:
    gate = dict(candidate_context.get("acceptance_gate") or {})
    frontier_gate = evidence_policy.get("frontier_gate") or {}
    status = gate.get("status") or frontier_gate.get("missing_status") or "missing"
    pass_statuses = set(frontier_gate.get("pass_statuses") or ["pass"])
    validation_statuses = set(frontier_gate.get("validation_statuses") or ["monitor", "blocked"])
    if status in pass_statuses:
        action = "frontier_ok"
    elif status in validation_statuses:
        action = "needs_validation"
    else:
        action = "evidence_missing"
    return {
        "status": status,
        "action": action,
        "explanation": gate.get("explanation") or gate.get("promotion") or "acceptance gate missing or incomplete",
    }


def _frontier_focus_names(snapshot: dict[str, Any]) -> list[str]:
    frontier = snapshot.get("frontier_focus") or {}
    ordered = []
    for group in (
        frontier.get("robust_candidates") or [],
        frontier.get("soft_robust_candidates") or [],
        frontier.get("short_window_candidates") or [],
        [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")],
    ):
        for name in group:
            if name and name not in ordered:
                ordered.append(name)
    return ordered



def _promotion_row_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = ((snapshot.get("promotion_scorecard") or {}).get("rows") or [])
    return {row.get("factor_name"): row for row in rows if row.get("factor_name")}



def _representative_failure_dossier_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return dict(snapshot.get("representative_failure_dossiers") or {})



def _front_gate_blocked_names(snapshot: dict[str, Any]) -> set[str]:
    blocked: set[str] = set()
    for name, row in _promotion_row_map(snapshot).items():
        quality_class = row.get("quality_classification")
        retention = row.get("retention_industry")
        net_metric = row.get("net_metric")
        if quality_class in {"duplicate-suppress", "drop"}:
            blocked.add(name)
            continue
        if net_metric is not None and float(net_metric) <= 0:
            blocked.add(name)
            continue
        if retention is not None and float(retention) < 0.15 and float(row.get("latest_recent_final_score") or row.get("latest_final_score") or 0.0) > 0.5:
            blocked.add(name)
    return blocked



def _quality_throttle(snapshot: dict[str, Any], factor_context: dict[str, dict[str, Any]], evidence_policy: dict[str, Any], research_mode: str) -> dict[str, Any]:
    focus_names = _frontier_focus_names(snapshot)[:4]
    evidence_missing = []
    evidence_needs_validation = []
    for name in focus_names:
        context = factor_context.get(name) or {}
        gate = _candidate_evidence_gate(context, evidence_policy)
        if gate["action"] == "evidence_missing":
            evidence_missing.append(name)
        elif gate["action"] == "needs_validation":
            evidence_needs_validation.append(name)

    relationship_summary = snapshot.get("relationship_summary") or {}
    duplicate_pressure = int(relationship_summary.get("duplicate_of") or 0)
    refinement_pressure = int(relationship_summary.get("refinement_of") or 0)
    cluster_pressure = int(relationship_summary.get("high_corr") or 0)

    floor = exploration_floor_context(snapshot)
    regime_context = build_regime_context(snapshot)
    regime = str(regime_context.get("regime") or "neutral")
    representative_failure_dossiers = _representative_failure_dossier_map(snapshot)
    front_gate_blocked_candidates = sorted(_front_gate_blocked_names(snapshot))
    dossier_diagnose_count = len([row for row in representative_failure_dossiers.values() if row.get("recommended_action") == "diagnose"])
    dossier_suppress_count = len([row for row in representative_failure_dossiers.values() if row.get("recommended_action") == "suppress"])
    dossier_short_window_only_count = len([row for row in representative_failure_dossiers.values() if row.get("regime_dependency") == "short_window_only"])
    dossier_parent_delta_failure_count = len([row for row in representative_failure_dossiers.values() if row.get("parent_delta_status") == "non_incremental"])
    unresolved_failure_focus = sorted(
        {
            name
            for name, row in representative_failure_dossiers.items()
            if row.get("recommended_action") in {"diagnose", "suppress"}
            or row.get("regime_dependency") == "short_window_only"
            or row.get("parent_delta_status") == "non_incremental"
        }
    )
    quality_priority_mode = bool(
        evidence_missing
        or duplicate_pressure >= 8
        or research_mode == "diagnosis_heavy"
        or dossier_diagnose_count >= 1
        or dossier_short_window_only_count >= 1
    )
    severe_quality_hold = floor["true_fault_recovery"] and (bool(evidence_missing) or duplicate_pressure >= 12 or (research_mode == "diagnosis_heavy" and evidence_needs_validation))

    candidate_floor = int(floor["exploration_floor_slots"] or 0)
    if not severe_quality_hold:
        if quality_priority_mode:
            candidate_floor = max(candidate_floor, 1)
            if regime in {"crowded_frontier", "regime_sensitive_frontier"}:
                candidate_floor = max(candidate_floor, 2)
        elif regime == "expansion_ready":
            candidate_floor = max(candidate_floor, 3)

    relaxed_admission = bool(
        not severe_quality_hold
        and regime in {"crowded_frontier", "regime_sensitive_frontier", "expansion_ready"}
    )

    return {
        "quality_priority_mode": quality_priority_mode,
        "severe_quality_hold": severe_quality_hold,
        "frontier_focus_names": focus_names,
        "frontier_evidence_missing": evidence_missing,
        "frontier_needs_validation": evidence_needs_validation,
        "duplicate_pressure": duplicate_pressure,
        "refinement_pressure": refinement_pressure,
        "cluster_pressure": cluster_pressure,
        "exploration_floor": floor,
        "regime_context": regime_context,
        "candidate_floor": candidate_floor,
        "relaxed_admission": relaxed_admission,
        "front_gate_blocked_candidates": front_gate_blocked_candidates,
        "representative_failure_dossier_count": len(representative_failure_dossiers),
        "dossier_diagnose_count": dossier_diagnose_count,
        "dossier_suppress_count": dossier_suppress_count,
        "dossier_short_window_only_count": dossier_short_window_only_count,
        "dossier_parent_delta_failure_count": dossier_parent_delta_failure_count,
        "unresolved_failure_focus": unresolved_failure_focus,
        "new_mechanism_bias": bool(dossier_suppress_count or dossier_parent_delta_failure_count or dossier_short_window_only_count),
    }



def _primary_generation_anchors(snapshot: dict[str, Any], stable: list[str]) -> list[str]:
    anchors: list[str] = []
    for name in stable:
        if name and name not in anchors:
            anchors.append(name)
    for name in _frontier_focus_names(snapshot):
        if name and name not in anchors:
            anchors.append(name)
    return anchors[:5]


def _family_gap_seeds(snapshot: dict[str, Any], factor_context: dict[str, dict[str, Any]], stable: list[str]) -> list[str]:
    stable_families = {
        (factor_context.get(name) or {}).get("family")
        for name in stable
        if (factor_context.get(name) or {}).get("family")
    }
    family_rows = list(snapshot.get("family_summary") or [])
    candidate_rows = list(snapshot.get("candidate_context") or [])

    target_families = []
    for row in sorted(
        family_rows,
        key=lambda item: (
            int(item.get("representative_count") or 0),
            int(item.get("duplicate_pressure") or 0),
            -float(item.get("avg_latest_score") or -999.0),
            -float(item.get("family_score") or -999.0),
            item.get("family") or "",
        ),
    ):
        family = row.get("family")
        if not family or family in stable_families:
            continue
        if row.get("recommended_action") == "pause":
            continue
        if float(row.get("family_score") or 0.0) < 10.0:
            continue
        target_families.append(family)
    target_families = target_families[:3]

    seeds: list[str] = []
    for family in target_families:
        family_candidates = [
            row for row in candidate_rows
            if row.get("family") == family and row.get("candidate_name")
        ]
        family_candidates.sort(
            key=lambda row: (
                not bool(row.get("is_primary_candidate")),
                int(row.get("relationship_count") or 0),
                -float(row.get("robustness_score") or -999.0),
                -(float((row.get("cluster") or {}).get("cluster_size") or 1.0)),
                row.get("candidate_name") or "",
            )
        )
        for row in family_candidates:
            name = row.get("candidate_name")
            if not name or name in seeds or name in stable:
                continue
            seeds.append(name)
            break
    return seeds



def _load_hypothesis_templates() -> list[dict[str, Any]]:
    payload = _read_json(HYPOTHESIS_TEMPLATE_PATH, {}) or {}
    return list(payload.get("templates") or [])



def _template_routes(snapshot: dict[str, Any], factor_context: dict[str, dict[str, Any]], stable: list[str]) -> list[dict[str, Any]]:
    stable_set = {name for name in stable if name}
    available = {name for name in factor_context if name}
    routes: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str, str]] = set()
    for template in _load_hypothesis_templates():
        anchors = [name for name in (template.get("anchor_candidates") or []) if name in available]
        contexts = [name for name in (template.get("context_candidates") or []) if name in available]
        if not anchors or not contexts:
            continue
        anchor = next((name for name in anchors if name in stable_set), anchors[0])
        if template.get("require_anchor_stable", False) and anchor not in stable_set:
            continue
        context = next((name for name in contexts if name != anchor and name not in stable_set), None)
        if context is None:
            context = next((name for name in contexts if name != anchor), None)
        if context is None or context == anchor:
            continue
        pair_key = tuple(sorted([anchor, context])) + (template.get("template_id") or "template",)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)
        rationale_template = template.get("rationale_template") or "hypothesis_template/{template_id}: test {anchor} with {context}."
        routes.append(
            {
                "template_id": template.get("template_id") or "template",
                "template_label": template.get("label") or template.get("template_id") or "template",
                "source": template.get("source") or "hypothesis_template",
                "base_factors": [anchor, context],
                "target_family": template.get("target_family") or (factor_context.get(anchor) or {}).get("family") or "generated",
                "expected_information_gain": list(template.get("expected_information_gain") or []),
                "rationale": rationale_template.format(anchor=anchor, context=context, template_id=template.get("template_id") or "template"),
                "exploration_pool": NEW_MECHANISM_POOL,
                "mechanism_novelty_class": "new_mechanism",
            }
        )
    return routes



def _failure_question_cards(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    direct_cards = list(snapshot.get("failure_question_cards") or [])
    if direct_cards:
        return direct_cards
    enhanced_cards = list(((snapshot.get("failure_analyst_enhancement") or {}).get("question_cards_v2") or []))
    if enhanced_cards:
        return enhanced_cards
    return list(((snapshot.get("research_learning") or {}).get("failure_question_cards") or []))



def _candidate_sort_key(name: str, factor_context: dict[str, dict[str, Any]], stable_set: set[str], *, anchor_family: str | None = None) -> tuple[Any, ...]:
    row = factor_context.get(name) or {}
    family = row.get("family")
    return (
        family == anchor_family,
        name in stable_set,
        not bool(row.get("is_primary_candidate")),
        int(row.get("relationship_count") or 0),
        -(float(row.get("robustness_score") or -999.0)),
        name,
    )



def _failure_question_routes(
    snapshot: dict[str, Any],
    factor_context: dict[str, dict[str, Any]],
    stable: list[str],
    family_gap_seeds: list[str],
) -> list[dict[str, Any]]:
    cards = _failure_question_cards(snapshot)
    if not cards:
        return []
    stable_set = {name for name in stable if name}
    available = [name for name in factor_context if name]
    frontier_focus = _frontier_focus_names(snapshot)
    routes: list[dict[str, Any]] = []
    seen_pairs: set[tuple[str, str, str]] = set()

    for card in cards:
        candidate_name = card.get("candidate_name")
        if candidate_name not in factor_context:
            candidate_name = next((name for name in frontier_focus if name in factor_context), None)
        if not candidate_name:
            continue
        anchor_family = (factor_context.get(candidate_name) or {}).get("family")
        context_candidates = list(family_gap_seeds) + frontier_focus + available
        ordered_context = []
        seen_context: set[str] = set()
        preferred_context_mode = card.get("preferred_context_mode") or ""
        for name in context_candidates:
            if not name or name == candidate_name or name in seen_context or name not in factor_context:
                continue
            family = (factor_context.get(name) or {}).get("family")
            if preferred_context_mode == "far_family" and anchor_family and family == anchor_family:
                continue
            if preferred_context_mode == "cross_family_or_quality" and anchor_family and family == anchor_family and family != "quality":
                continue
            seen_context.add(name)
            ordered_context.append(name)
        if not ordered_context and preferred_context_mode in {"far_family", "cross_family_or_quality"}:
            for name in context_candidates:
                if not name or name == candidate_name or name in seen_context or name not in factor_context:
                    continue
                seen_context.add(name)
                ordered_context.append(name)
        ordered_context.sort(key=lambda name: _candidate_sort_key(name, factor_context, stable_set, anchor_family=anchor_family))
        context_name = ordered_context[0] if ordered_context else None
        if not context_name:
            continue
        question_type = card.get("question_type") or "failure_question"
        pair_key = tuple(sorted([candidate_name, context_name])) + (question_type,)
        if pair_key in seen_pairs:
            continue
        seen_pairs.add(pair_key)
        routes.append(
            {
                "template_id": card.get("card_id") or f"question::{candidate_name}",
                "template_label": question_type,
                "source": "failure_question",
                "base_factors": [candidate_name, context_name],
                "target_family": (factor_context.get(context_name) or {}).get("family") or anchor_family or "generated",
                "expected_information_gain": list(card.get("expected_information_gain") or ["new_branch_opened"]),
                "rationale": card.get("prompt") or f"failure question for {candidate_name}",
                "question_card_id": card.get("card_id"),
                "question_type": question_type,
                "route_bias": card.get("route_bias"),
                "preferred_context_mode": card.get("preferred_context_mode"),
                "allowed_operators": list(card.get("allowed_operators") or []),
                "exploration_pool": card.get("target_pool") or NEW_MECHANISM_POOL,
                "mechanism_novelty_class": "new_mechanism",
            }
        )
    return routes



def _proposal(candidate_id: str, base_factors: list[str], operator: str, *, target_family: str | None, source: str, rationale: str, expected_gain: list[str]) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "base_factors": base_factors,
        "operator": operator,
        "target_family": target_family,
        "source": source,
        "rationale": rationale,
        "expected_information_gain": expected_gain,
    }


def _operators_for(expected_gain: list[str], policy: dict[str, Any], operator_stats: dict[str, Any] | None = None, *, source: str | None = None, target_family: str | None = None, family_operator_stats: dict[str, Any] | None = None, allowed_operators: list[str] | None = None) -> list[str]:
    enabled = list(allowed_operators or policy.get("enabled_operators") or [])
    mapping = policy.get("failure_reason_operator_map") or {}
    source_preferences = (policy.get("source_operator_preferences") or {}).get(source or "", [])
    preferred: list[str] = []
    for op in source_preferences:
        if op in enabled and op not in preferred:
            preferred.append(op)
    for gain in expected_gain or []:
        for op in mapping.get(gain) or []:
            if op in enabled and op not in preferred:
                preferred.append(op)

    def _score(op: str) -> tuple[float, int, str]:
        score = 0.0
        preferred_index = preferred.index(op) if op in preferred else 999
        if op in preferred:
            score += 2.0
        stats = (operator_stats or {}).get(op) or {}
        family_stats = (((family_operator_stats or {}).get(target_family or "") or {}).get(op) or {})
        family_action = family_stats.get("recommended_action")
        action = family_action or stats.get("recommended_action")
        if family_action == "upweight":
            score += 2.2
        elif family_action == "keep":
            score += 0.3
        elif family_action == "downweight":
            score -= 2.4
        elif action == "upweight":
            score += 1.5
        elif action == "keep":
            score += 0.2
            if int(stats.get("high_value_failure_count") or 0) >= 1:
                score += 0.4
        elif action == "downweight":
            score -= 2.0
        return (-score, preferred_index, op)

    ordered = sorted(enabled, key=_score)
    return ordered


def _cheap_screen_proposal(proposal: dict[str, Any], snapshot: dict[str, Any], policy: dict[str, Any], factor_context: dict[str, dict[str, Any]]) -> dict[str, Any]:
    cfg = (policy.get("cheap_screen") or {})
    stable = {row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")}
    graveyard = set(snapshot.get("latest_graveyard") or [])
    base_factors = list(proposal.get("base_factors") or [])
    score = 0.5
    reasons = []
    fams = [((factor_context.get(name) or {}).get("family")) for name in base_factors]
    fams = [f for f in fams if f]
    if len(set(fams)) >= 2:
        score += float(cfg.get("cross_family_bonus") or 0.2)
        reasons.append("cross_family_bonus")
    elif fams:
        score -= float(cfg.get("same_family_penalty") or 0.25)
        reasons.append("same_family_penalty")
    if any(name in stable for name in base_factors) and any(name in graveyard for name in base_factors):
        score += float(cfg.get("stable_graveyard_pair_bonus") or 0.2)
        reasons.append("stable_graveyard_pair_bonus")
    if proposal.get("source") == "high_value_failure_seed":
        score += float(cfg.get("high_value_failure_seed_bonus") or 0.15)
        reasons.append("high_value_failure_seed_bonus")
    expected_gain = set(proposal.get("expected_information_gain") or [])
    if expected_gain & {"search_space_reduced", "boundary_confirmed", "candidate_survival_check"}:
        score += float(cfg.get("epistemic_gain_bonus") or 0.15)
        reasons.append("epistemic_gain_bonus")
    duplicate_pressure = sum(int(((factor_context.get(name) or {}).get("relationship_count") or 0) > 8) for name in base_factors)
    if duplicate_pressure >= 2:
        score -= float(cfg.get("duplicate_penalty") or 0.3)
        reasons.append("duplicate_penalty")
    screen_pass = score >= float(cfg.get("min_score") or 0.55)
    proposal["cheap_screen"] = {
        "score": round(score, 3),
        "pass": screen_pass,
        "reasons": reasons,
    }
    return proposal


def build_candidate_generation_plan(
    snapshot_path: str | Path,
    memory_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    snapshot = _read_json(snapshot_path, {})
    memory = _read_json(memory_path, {})
    policy = _read_json(POLICY_PATH, {})
    evidence_policy = _read_json(EVIDENCE_POLICY_PATH, {})
    learning = _read_json(LEARNING_PATH, {})
    operator_stats = learning.get("operator_stats") or {}
    family_operator_stats = learning.get("family_operator_stats") or {}
    research_mode = (learning.get("research_mode") or {}).get("mode") or "balanced"
    factor_context = _factor_context(snapshot)
    quality_throttle = _quality_throttle(snapshot, factor_context, evidence_policy, research_mode)

    stable = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")][:5]
    generation_anchors = _primary_generation_anchors(snapshot, stable)
    graveyard = list(snapshot.get("latest_graveyard") or [])[:5]
    failure_seeds = _high_value_failure_seeds(memory)[:5]
    failure_routes = _high_value_failure_routes(memory)
    family_gap_seeds = _family_gap_seeds(snapshot, factor_context, generation_anchors)
    template_routes = _template_routes(snapshot, factor_context, generation_anchors)
    failure_question_cards = _failure_question_cards(snapshot)
    failure_question_routes = _failure_question_routes(snapshot, factor_context, generation_anchors, family_gap_seeds)
    llm_hypothesis_routes = generate_hypothesis_routes(
        failure_question_cards=failure_question_cards,
        generation_anchors=generation_anchors,
        family_gap_seeds=family_gap_seeds,
        limit=4,
        source_label="heuristic",
    )
    constraints = _generation_constraints(memory)

    proposals: list[dict[str, Any]] = []
    triage_model = load_candidate_triage_model()
    seen_keys: set[tuple[str, str, str]] = set()
    pair_operator_counts: dict[tuple[str, str], int] = {}
    cluster_variant_counts: dict[str, int] = {}
    max_new = int(((policy.get("limits") or {}).get("max_new_candidates_per_cycle") or 4))
    max_operators_per_pair = int(((policy.get("limits") or {}).get("max_operators_per_pair") or 2))
    prefer_cross_family = bool(((policy.get("selection_rules") or {}).get("prefer_cross_family_pairs")))
    generation_constraints = policy.get("constraints") or {}
    pair_failure_threshold = int(generation_constraints.get("base_pair_cooldown_after_failures") or 2)
    max_variants_per_cluster = int(generation_constraints.get("max_variants_per_cluster_per_cycle") or 1)
    allowed_operators = list(policy.get("enabled_operators") or [])
    floor_slots = int(((quality_throttle.get("exploration_floor") or {}).get("exploration_floor_slots") or 0))
    candidate_floor = int(quality_throttle.get("candidate_floor") or 0)
    regime = str(((quality_throttle.get("regime_context") or {}).get("regime") or "neutral"))
    if quality_throttle["quality_priority_mode"]:
        max_new = min(max_new, max(1, candidate_floor or floor_slots or 1))
        max_operators_per_pair = 1
        allowed_operators = [
            op for op in allowed_operators
            if op in {"residualize_against_peer", "orthogonalize_against_peer", "combine_sub"}
        ] or allowed_operators
    elif candidate_floor:
        max_new = max(max_new, min(candidate_floor, max_new + 1))
    if quality_throttle.get("relaxed_admission"):
        max_variants_per_cluster = max(max_variants_per_cluster, max(1, candidate_floor or 1))
    if quality_throttle.get("new_mechanism_bias"):
        max_variants_per_cluster = 1
    if quality_throttle["severe_quality_hold"]:
        max_new = 0

    pool_budgets = split_exploration_pool_budget(
        max_new,
        prioritize_new_mechanism=bool(quality_throttle.get("new_mechanism_bias") or failure_question_cards),
        quality_priority_mode=bool(quality_throttle.get("quality_priority_mode")),
        regime=regime,
    )
    proposal_pool_counts = {
        OLD_SPACE_POOL: 0,
        NEW_MECHANISM_POOL: 0,
    }

    front_gate_blocked_candidates = set(quality_throttle.get("front_gate_blocked_candidates") or [])
    unresolved_failure_focus = set(quality_throttle.get("unresolved_failure_focus") or [])

    def _cluster_budget_key(a: str, b: str) -> str | None:
        cluster_ids = sorted(
            {
                str(((factor_context.get(name) or {}).get("cluster") or {}).get("cluster_id"))
                for name in (a, b)
                if ((factor_context.get(name) or {}).get("cluster") or {}).get("cluster_id") is not None
            }
        )
        if len(cluster_ids) == 1:
            return f"cluster:{cluster_ids[0]}"
        return None

    def add_pair(
        a: str,
        b: str,
        *,
        source: str,
        expected_gain: list[str],
        rationale_override: str | None = None,
        target_family_override: str | None = None,
        extra_fields: dict[str, Any] | None = None,
        allow_relaxed_retry: bool = False,
    ) -> None:
        nonlocal proposals
        if not a or not b or a == b:
            return
        ctx_a = factor_context.get(a) or {}
        ctx_b = factor_context.get(b) or {}
        fam_a = ctx_a.get("family")
        fam_b = ctx_b.get("family")
        if source not in {"hypothesis_template", "family_gap", "failure_question"} and (a in front_gate_blocked_candidates or b in front_gate_blocked_candidates):
            return
        if source in {"stable_plus_graveyard", "high_value_failure_seed"} and (a in unresolved_failure_focus or b in unresolved_failure_focus):
            return
        if prefer_cross_family and fam_a and fam_b and fam_a == fam_b:
            return
        pair_key = tuple(sorted([a, b]))
        pair_failure_count = int((constraints.get("pair_failure_counts") or {}).get(pair_key) or 0)
        if pair_key in constraints["blocked_pairs"] and not allow_relaxed_retry:
            return
        retry_ceiling = pair_failure_threshold + 2 if allow_relaxed_retry else pair_failure_threshold
        if pair_failure_count >= retry_ceiling:
            return
        cluster_budget_key = _cluster_budget_key(a, b)
        if cluster_budget_key and int(cluster_variant_counts.get(cluster_budget_key) or 0) >= max_variants_per_cluster:
            return
        extra_fields = dict(extra_fields or {})
        exploration_pool = classify_exploration_pool(source, extra_fields)
        if int(proposal_pool_counts.get(exploration_pool) or 0) >= int(pool_budgets.get(exploration_pool) or 0):
            return
        route_allowed_operators = list(extra_fields.get("allowed_operators") or allowed_operators)
        if not extra_fields.get("allowed_operators") and exploration_pool == NEW_MECHANISM_POOL and source in {"failure_question", "hypothesis_template"}:
            route_allowed_operators = [
                op for op in route_allowed_operators
                if op not in {"orthogonalize_against_peer", "residualize_against_peer"}
            ] or route_allowed_operators
        for operator in _operators_for(
            expected_gain,
            policy,
            operator_stats,
            source=source,
            target_family=fam_a or fam_b,
            family_operator_stats=family_operator_stats,
            allowed_operators=route_allowed_operators,
        ):
            key = pair_key + (operator,)
            if key in seen_keys or key in constraints["blocked_operator_pairs"]:
                continue
            if int(pair_operator_counts.get(pair_key) or 0) >= max_operators_per_pair:
                continue
            seen_keys.add(key)
            pair_operator_counts[pair_key] = int(pair_operator_counts.get(pair_key) or 0) + 1
            candidate_id = f"gen__{operator}__{a}__{b}".replace("-", "_")
            rationale = rationale_override or f"{source}: 从 {a} 与 {b} 生成新候选，优先探索互补而非重复。"
            proposal = _proposal(
                candidate_id,
                [a, b],
                operator,
                target_family=target_family_override or fam_a or fam_b,
                source=source,
                rationale=rationale,
                expected_gain=expected_gain,
            )
            proposal["exploration_pool"] = exploration_pool
            proposal["mechanism_novelty_class"] = "new_mechanism" if exploration_pool == NEW_MECHANISM_POOL else "old_space"
            proposal["decision_source"] = "heuristic"
            if extra_fields:
                proposal.update(extra_fields)
            novelty = judge_generation_proposal(proposal, factor_context=factor_context, source_label="heuristic")
            proposal.update(novelty)
            if allow_relaxed_retry:
                proposal["relaxed_admission"] = {
                    "pair_failure_count": pair_failure_count,
                    "retry_ceiling": retry_ceiling,
                    "reason": "regime_escape_hatch",
                }
            proposal = _cheap_screen_proposal(proposal, snapshot, policy, factor_context)
            proposal["triage"] = score_generation_proposal(
                proposal,
                snapshot=snapshot,
                factor_context=factor_context,
                model=triage_model,
            )
            proposals.append(proposal)
            proposal_pool_counts[exploration_pool] = int(proposal_pool_counts.get(exploration_pool) or 0) + 1
            if cluster_budget_key:
                cluster_variant_counts[cluster_budget_key] = int(cluster_variant_counts.get(cluster_budget_key) or 0) + 1
            if len(proposals) >= max_new:
                return
            if int(proposal_pool_counts.get(exploration_pool) or 0) >= int(pool_budgets.get(exploration_pool) or 0):
                return

    failure_driven_budget = int((policy.get("budgets") or {}).get("failure_driven", 2))
    if quality_throttle.get("new_mechanism_bias"):
        failure_driven_budget = min(failure_driven_budget, 1)

    if len(proposals) < max_new:
        candidate_question_routes = llm_hypothesis_routes + failure_question_routes
        question_budget = min(len(candidate_question_routes), max(1, pool_budgets.get(NEW_MECHANISM_POOL, 0)))
        for route in candidate_question_routes[:question_budget]:
            bases = list(route.get("base_factors") or [])
            if len(bases) < 2:
                continue
            add_pair(
                bases[0],
                bases[1],
                source=route.get("source") or "failure_question",
                expected_gain=list(route.get("expected_information_gain") or []),
                rationale_override=route.get("rationale"),
                target_family_override=route.get("target_family"),
                extra_fields={
                    "question_card_id": route.get("question_card_id"),
                    "question_type": route.get("question_type"),
                    "route_bias": route.get("route_bias"),
                    "hypothesis_template_id": route.get("template_id"),
                    "hypothesis_template_label": route.get("template_label"),
                    "exploration_pool": route.get("exploration_pool") or NEW_MECHANISM_POOL,
                    "mechanism_novelty_class": route.get("mechanism_novelty_class") or "new_mechanism",
                },
            )
            if len(proposals) >= max_new:
                break

    if research_mode == "diagnosis_heavy":
        if len(proposals) < max_new:
            for route in failure_routes[:failure_driven_budget]:
                bases = list(route.get("base_factors") or [])
                if len(bases) < 2:
                    continue
                add_pair(bases[0], bases[1], source="high_value_failure_seed", expected_gain=["boundary_confirmed", "search_space_reduced", "candidate_survival_check"], extra_fields={"exploration_pool": OLD_SPACE_POOL, "mechanism_novelty_class": "old_space"})
                if len(proposals) >= max_new:
                    break

    if len(proposals) < max_new:
        template_budget = int((policy.get("budgets") or {}).get("template_driven", 2))
        if quality_throttle.get("relaxed_admission"):
            template_budget = max(template_budget, min(len(template_routes), max_new * 3))
        if quality_throttle.get("new_mechanism_bias"):
            template_budget = max(template_budget, min(len(template_routes), max_new * 4))
        for route in template_routes[:template_budget]:
            bases = list(route.get("base_factors") or [])
            if len(bases) < 2:
                continue
            add_pair(
                bases[0],
                bases[1],
                source=route.get("source") or "hypothesis_template",
                expected_gain=list(route.get("expected_information_gain") or []),
                rationale_override=route.get("rationale"),
                target_family_override=route.get("target_family"),
                extra_fields={
                    "hypothesis_template_id": route.get("template_id"),
                    "hypothesis_template_label": route.get("template_label"),
                    "exploration_pool": route.get("exploration_pool") or NEW_MECHANISM_POOL,
                    "mechanism_novelty_class": route.get("mechanism_novelty_class") or "new_mechanism",
                },
            )
            if len(proposals) >= max_new:
                break

    anchor_budget = int((policy.get("budgets") or {}).get("stable_neighbor", 2))
    if quality_throttle.get("new_mechanism_bias"):
        anchor_budget = min(anchor_budget, 1)
    for a in generation_anchors[:anchor_budget]:
        for b in graveyard[:2]:
            add_pair(a, b, source="stable_plus_graveyard", expected_gain=["search_space_reduced", "candidate_survival_check"], extra_fields={"exploration_pool": OLD_SPACE_POOL, "mechanism_novelty_class": "old_space"})
            if len(proposals) >= max_new:
                break
        if len(proposals) >= max_new:
            break

    if len(proposals) < max_new:
        for route in failure_routes[:failure_driven_budget]:
            bases = list(route.get("base_factors") or [])
            if len(bases) < 2:
                continue
            add_pair(bases[0], bases[1], source="high_value_failure_seed", expected_gain=["boundary_confirmed", "search_space_reduced", "candidate_survival_check"], extra_fields={"exploration_pool": OLD_SPACE_POOL, "mechanism_novelty_class": "old_space"})
            if len(proposals) >= max_new:
                break

    if len(proposals) < max_new:
        for a in generation_anchors[:2]:
            for b in failure_seeds[:failure_driven_budget]:
                add_pair(a, b, source="high_value_failure_seed", expected_gain=["boundary_confirmed", "candidate_survival_check"], extra_fields={"exploration_pool": OLD_SPACE_POOL, "mechanism_novelty_class": "old_space"})
                if len(proposals) >= max_new:
                    break
            if len(proposals) >= max_new:
                break

    if len(proposals) < max_new:
        family_gap_budget = int((policy.get("budgets") or {}).get("family_gap", 1))
        for a in generation_anchors[: max(1, family_gap_budget)]:
            for b in family_gap_seeds[:family_gap_budget]:
                add_pair(a, b, source="family_gap", expected_gain=["new_branch_opened", "candidate_survival_check"], extra_fields={"exploration_pool": NEW_MECHANISM_POOL, "mechanism_novelty_class": "new_mechanism"})
                if len(proposals) >= max_new:
                    break
            if len(proposals) >= max_new:
                break

    relaxed_minimum = min(max_new, max(1, candidate_floor or 0))
    if quality_throttle.get("relaxed_admission") and max_new > 0 and len(proposals) < relaxed_minimum:
        fallback_routes = (failure_question_routes + template_routes)[: min(len(failure_question_routes) + len(template_routes), max_new * 6)]
        for route in fallback_routes:
            bases = list(route.get("base_factors") or [])
            if len(bases) < 2:
                continue
            add_pair(
                bases[0],
                bases[1],
                source=route.get("source") or "hypothesis_template",
                expected_gain=list(route.get("expected_information_gain") or []),
                rationale_override=route.get("rationale"),
                target_family_override=route.get("target_family"),
                extra_fields={
                    "hypothesis_template_id": route.get("template_id"),
                    "hypothesis_template_label": route.get("template_label"),
                    "question_card_id": route.get("question_card_id"),
                    "question_type": route.get("question_type"),
                    "route_bias": route.get("route_bias"),
                    "exploration_pool": route.get("exploration_pool") or classify_exploration_pool(route.get("source"), route),
                    "mechanism_novelty_class": route.get("mechanism_novelty_class") or ("new_mechanism" if classify_exploration_pool(route.get("source"), route) == NEW_MECHANISM_POOL else "old_space"),
                },
                allow_relaxed_retry=True,
            )
            if len(proposals) >= relaxed_minimum:
                break

    proposals.sort(
        key=lambda row: (
            -float(((row.get("triage") or {}).get("score") or 0.0)),
            -float(((row.get("cheap_screen") or {}).get("score") or 0.0)),
            row.get("candidate_id") or "",
        )
    )

    payload = {
        "generated_from_snapshot": str(snapshot_path),
        "generated_from_memory": str(memory_path),
        "policy_name": policy.get("name"),
        "quality_throttle": {
            "quality_priority_mode": quality_throttle["quality_priority_mode"],
            "severe_quality_hold": quality_throttle["severe_quality_hold"],
            "frontier_focus_names": quality_throttle["frontier_focus_names"],
            "frontier_evidence_missing": quality_throttle["frontier_evidence_missing"],
            "frontier_needs_validation": quality_throttle["frontier_needs_validation"],
            "duplicate_pressure": quality_throttle["duplicate_pressure"],
            "refinement_pressure": quality_throttle["refinement_pressure"],
            "cluster_pressure": quality_throttle["cluster_pressure"],
            "regime_context": quality_throttle.get("regime_context") or {},
            "candidate_floor": quality_throttle.get("candidate_floor") or 0,
            "relaxed_admission": bool(quality_throttle.get("relaxed_admission")),
            "front_gate_blocked_candidates": quality_throttle.get("front_gate_blocked_candidates") or [],
            "unresolved_failure_focus": quality_throttle.get("unresolved_failure_focus") or [],
            "new_mechanism_bias": bool(quality_throttle.get("new_mechanism_bias")),
            "generation_anchors": generation_anchors,
            "family_gap_seeds": family_gap_seeds,
            "template_routes": template_routes,
            "failure_question_cards": failure_question_cards,
            "failure_question_routes": failure_question_routes,
            "llm_hypothesis_routes": llm_hypothesis_routes,
            "exploration_floor": quality_throttle.get("exploration_floor") or {},
            "allowed_operators": allowed_operators,
            "max_new_candidates_per_cycle": max_new,
            "max_operators_per_pair": max_operators_per_pair,
            "pool_budgets": pool_budgets,
            "pool_counts": proposal_pool_counts,
            "constraints": {
                "base_pair_cooldown_after_failures": pair_failure_threshold,
                "max_variants_per_cluster_per_cycle": max_variants_per_cluster,
                "pair_failure_counts": {
                    "|".join(key): value
                    for key, value in (constraints.get("pair_failure_counts") or {}).items()
                },
            },
        },
        "proposals": proposals[:max_new],
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
