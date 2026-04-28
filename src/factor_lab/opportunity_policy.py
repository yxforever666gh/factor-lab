from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from factor_lab.regime_awareness import QUESTION_TYPES, build_regime_context

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
MEMORY_PATH = ARTIFACTS / "research_memory.json"
AUTONOMY_POLICY_PATH = ROOT / "configs" / "research_autonomy_policy.json"
MIN_EXPLORATION_FLOOR = {"recombine": 1, "probe": 1}
CHILD_BUDGET = {"expand": 1, "recombine": 1, "probe": 1}
ADAPTIVE_BANDIT_SLOTS = 4


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        # Tolerate common tail-corruption (for example an extra trailing brace)
        # so planner/opportunity generation can keep running instead of deadlocking.
        decoder = json.JSONDecoder()
        try:
            obj, end = decoder.raw_decode(text)
            trailing = text[end:].strip()
            if trailing and set(trailing) <= {'}'}:
                return obj
        except Exception:
            pass
        raise exc


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(minimum, value)


def _template_key(row: dict[str, Any]) -> str:
    otype = row.get("opportunity_type") or "unknown"
    family = row.get("target_family") or "none"
    parent = "child" if row.get("parent_opportunity_id") else "root"
    return f"{otype}::{family}::{parent}"


def _target_shape(row: dict[str, Any]) -> str:
    targets = list(row.get("target_candidates") or [])
    if not targets:
        return "no_targets"
    if len(targets) == 1:
        return "single_target"
    if len(targets) == 2:
        return "pair_target"
    return "multi_target"


def _intent_signature(row: dict[str, Any]) -> str:
    expected = sorted([str(x) for x in (row.get("expected_knowledge_gain") or []) if x])
    if not expected:
        return "no_expected_gain"
    return "+".join(expected[:3])


def _pattern_signature(row: dict[str, Any]) -> str:
    template = _template_key(row)
    evaluation = row.get("evaluation") or {}
    epistemic = sorted([str(x) for x in (evaluation.get("epistemic_gain") or []) if x])
    epistemic_part = "+".join(epistemic[:3]) if epistemic else "pre_eval"
    return f"{template}::{_target_shape(row)}::{_intent_signature(row)}::{epistemic_part}"


def _apply_epistemic_updates(meta: dict[str, Any], evaluation: dict[str, Any]) -> None:
    epistemic_gain = list(evaluation.get("epistemic_gain") or [])
    if any(tag in epistemic_gain for tag in {"uncertainty_reduced", "boundary_confirmed", "new_branch_opened", "probe_promising", "hypothesis_supported", "partial_support"}):
        meta["uncertainty_reduction_count"] += 1
        meta["epistemic_value_score"] += 1.0
    if any(tag in epistemic_gain for tag in {"repeat_without_new_information", "low_novelty_realized"}):
        meta["repeat_signal_count"] += 1
        meta["epistemic_value_score"] -= 0.7
    if any(tag in epistemic_gain for tag in {"negative_result_recorded", "search_space_reduced", "hybrid_invalidated", "probe_negative_but_informative", "boundary_broken"}):
        meta["negative_informative_count"] += 1
        meta["epistemic_value_score"] += 0.6
    if any(tag in epistemic_gain for tag in {"new_branch_opened", "search_space_expanded"}):
        meta["new_branch_count"] += 1
        meta["epistemic_value_score"] += 0.8
    if any(tag in epistemic_gain for tag in {"inconclusive", "uncertainty_preserved"}):
        meta["inconclusive_count"] += 1
        meta["epistemic_value_score"] -= 0.2


def _feedback_is_resource_exhausted(feedback: dict[str, Any]) -> bool:
    text = " ".join([
        str(feedback.get("summary") or ""),
        str(feedback.get("error_text") or ""),
    ]).lower()
    return any(token in text for token in ["rss exceeded", "worker_rss_exceeded", "generated_batch_worker_rss_exceeded", "quarantined｜generated_batch_worker_rss_exceeded"])


def _apply_feedback_updates(meta: dict[str, Any], feedback: dict[str, Any], branch_state: dict[str, Any]) -> None:
    meta["recent_execution_count"] += 1
    has_gain = bool(feedback.get("has_gain"))
    if has_gain:
        meta["recent_gain_count"] += 1
    else:
        meta["recent_no_gain_count"] += 1
    if _feedback_is_resource_exhausted(feedback):
        meta["recent_resource_exhaustion_count"] += 1
    meta["max_no_gain_runs"] = max(meta["max_no_gain_runs"], int(branch_state.get("no_gain_runs") or 0))


def _finalize_learning_bucket(meta: dict[str, Any]) -> None:
    terminal = meta["promoted"] + meta["evaluated"] + meta["rejected"] + meta["archived"]
    if terminal > 0:
        meta["success_rate"] = round(meta["promoted"] / terminal, 3)
        meta["epistemic_value_score"] = round(meta["epistemic_value_score"] / terminal, 3)
    recent_execution_count = int(meta.get("recent_execution_count") or 0)
    recent_gain_count = int(meta.get("recent_gain_count") or 0)
    recent_no_gain_count = int(meta.get("recent_no_gain_count") or 0)
    recent_resource_exhaustion_count = int(meta.get("recent_resource_exhaustion_count") or 0)
    meta["recent_yield"] = round(recent_gain_count / recent_execution_count, 3) if recent_execution_count else None

    no_gain_cooldown_threshold = _env_int("RESEARCH_OPPORTUNITY_NO_GAIN_COOLDOWN_THRESHOLD", 2, minimum=1)
    resource_cooldown_threshold = _env_int("RESEARCH_OPPORTUNITY_RESOURCE_COOLDOWN_THRESHOLD", 2, minimum=1)
    low_yield_window = _env_int("RESEARCH_OPPORTUNITY_LOW_YIELD_WINDOW", 3, minimum=1)
    max_no_gain_runs = int(meta.get("max_no_gain_runs") or 0)
    cooldown_active = bool(
        max_no_gain_runs >= no_gain_cooldown_threshold
        or recent_resource_exhaustion_count >= resource_cooldown_threshold
        or (
            recent_execution_count >= low_yield_window
            and recent_gain_count == 0
            and recent_no_gain_count >= no_gain_cooldown_threshold
        )
    )
    meta["cooldown_active"] = cooldown_active

    if cooldown_active:
        if recent_resource_exhaustion_count >= resource_cooldown_threshold:
            meta["cooldown_reason"] = "resource_exhaustion"
        elif max_no_gain_runs >= no_gain_cooldown_threshold:
            meta["cooldown_reason"] = "repeated_no_gain"
        else:
            meta["cooldown_reason"] = "low_recent_yield"
    else:
        meta["cooldown_reason"] = None

    if meta["success_rate"] is None:
        meta["recommended_action"] = "keep"
    elif cooldown_active:
        meta["recommended_action"] = "downweight"
    elif meta["uncertainty_reduction_count"] >= 2 or meta["new_branch_count"] >= 1 or meta["epistemic_value_score"] >= 0.45:
        meta["recommended_action"] = "upweight"
    elif meta["repeat_signal_count"] >= max(2, meta["uncertainty_reduction_count"] + meta["negative_informative_count"]) or meta["epistemic_value_score"] <= -0.25:
        meta["recommended_action"] = "downweight"
    elif meta["negative_informative_count"] >= 1 and meta["repeat_signal_count"] == 0:
        meta["recommended_action"] = "keep"
    else:
        meta["recommended_action"] = "keep"


def _new_meta(identity_key: str, label_key: str) -> dict[str, Any]:
    return {
        label_key: identity_key,
        "count": 0,
        "promoted": 0,
        "evaluated": 0,
        "rejected": 0,
        "archived": 0,
        "success_rate": None,
        "recommended_action": "keep",
        "epistemic_value_score": 0.0,
        "uncertainty_reduction_count": 0,
        "repeat_signal_count": 0,
        "negative_informative_count": 0,
        "new_branch_count": 0,
        "inconclusive_count": 0,
        "recent_execution_count": 0,
        "recent_gain_count": 0,
        "recent_no_gain_count": 0,
        "recent_resource_exhaustion_count": 0,
        "max_no_gain_runs": 0,
        "recent_yield": None,
        "cooldown_active": False,
        "cooldown_reason": None,
    }


def build_opportunity_learning(store_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    opath = Path(output_path) if output_path else (ARTIFACTS / "opportunity_learning.json")
    store = _read_json(spath, {"opportunities": {}})
    items = list((store.get("opportunities") or {}).values())
    items_by_id = {
        row.get("opportunity_id"): row
        for row in items
        if row.get("opportunity_id")
    }
    memory = _read_json(MEMORY_PATH, {})
    branch_lifecycle = dict(memory.get("branch_lifecycle") or {})
    execution_feedback = list(memory.get("execution_feedback") or [])[-160:]

    types: dict[str, dict[str, Any]] = {}
    families: dict[str, dict[str, Any]] = {}
    templates: dict[str, dict[str, Any]] = {}
    patterns: dict[str, dict[str, Any]] = {}

    def buckets_for_row(row: dict[str, Any]) -> list[tuple[dict[str, dict[str, Any]], str, str]]:
        otype = row.get("opportunity_type") or "unknown"
        family = row.get("target_family") or "none"
        return [
            (types, otype, "opportunity_type"),
            (families, family, "family"),
            (templates, _template_key(row), "template"),
            (patterns, _pattern_signature(row), "pattern"),
        ]

    for row in items:
        state = row.get("state")
        evaluation = row.get("evaluation") or {}
        branch_state = dict(branch_lifecycle.get(row.get("opportunity_id") or "") or {})
        for container, identity, label_key in buckets_for_row(row):
            meta = container.setdefault(identity, _new_meta(identity, label_key))
            meta["count"] += 1
            if state == "promoted":
                meta["promoted"] += 1
            elif state == "evaluated":
                meta["evaluated"] += 1
            elif state == "rejected":
                meta["rejected"] += 1
            elif state == "archived":
                meta["archived"] += 1
            meta["max_no_gain_runs"] = max(meta["max_no_gain_runs"], int(branch_state.get("no_gain_runs") or 0))
            _apply_epistemic_updates(meta, evaluation)

    for feedback in execution_feedback:
        branch_id = feedback.get("branch_id")
        row = items_by_id.get(branch_id)
        if not row:
            continue
        branch_state = dict(branch_lifecycle.get(branch_id) or {})
        for container, identity, label_key in buckets_for_row(row):
            meta = container.setdefault(identity, _new_meta(identity, label_key))
            _apply_feedback_updates(meta, feedback, branch_state)

    for bucket in (types, families, templates, patterns):
        for meta in bucket.values():
            _finalize_learning_bucket(meta)

    payload = {
        "types": types,
        "families": families,
        "templates": templates,
        "patterns": patterns,
        "memory_window": {
            "execution_feedback_count": len(execution_feedback),
            "branch_lifecycle_count": len(branch_lifecycle),
        },
    }
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def _bandit_score_for_type(otype: str, meta: dict[str, Any], regime_context: dict[str, Any], objectives: set[str]) -> float:
    count = float(meta.get("count") or 0.0)
    success_rate = float(meta.get("success_rate") or 0.0)
    epistemic_value = float(meta.get("epistemic_value_score") or 0.0)
    normalized_epistemic = max(0.0, min(1.0, (epistemic_value + 1.0) / 2.0))
    uncertainty_bonus = 0.45 / ((count + 1.0) ** 0.5)
    regime_weight = float(((regime_context.get("weights") or {}).get(otype) or 1.0))
    reward = 0.52 * success_rate + 0.33 * normalized_epistemic + 0.15 * min(1.0, regime_weight / 1.25)
    if "epistemic_gain" in objectives and otype in {"diagnose", "probe", "recombine"}:
        reward += 0.05
    if (meta.get("recommended_action") or "") == "upweight":
        reward += 0.08
    elif (meta.get("recommended_action") or "") == "downweight":
        reward -= 0.08
    return reward + uncertainty_bonus


def allocate_opportunity_budget(snapshot: dict[str, Any], opportunity_learning: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    autonomy_policy = _read_json(AUTONOMY_POLICY_PATH, {})
    types = opportunity_learning.get("types") or {}
    recovery_state = flow_state.get("state")
    regime_context = build_regime_context(snapshot)
    representative_failure_dossiers = snapshot.get("representative_failure_dossiers") or {}

    budget = {"confirm": 2, "diagnose": 2, "expand": 1, "recombine": 1, "probe": 1}
    reasons: list[str] = []

    if recovery_state == "recovering":
        budget["confirm"] += 1
        budget["diagnose"] += 1
        reasons.append("recovering_bias")
    elif recovery_state == "recovered":
        budget["expand"] += 1
        budget["recombine"] += 1
        reasons.append("recovered_bias_to_expand_recombine")

    for otype, meta in types.items():
        action = meta.get("recommended_action")
        if otype not in budget:
            continue
        if action == "upweight":
            budget[otype] += 1
            reasons.append(f"learning_upweight:{otype}")
        elif action == "downweight":
            budget[otype] = max(0, budget[otype] - 1)
            reasons.append(f"learning_downweight:{otype}")

    regime_weights = regime_context.get("weights") or {}
    for otype in QUESTION_TYPES:
        weight = float(regime_weights.get(otype) or 1.0)
        if weight >= 1.12:
            budget[otype] += 1
            reasons.append(f"regime_upweight:{otype}")
        elif weight <= 0.82:
            budget[otype] = max(0, budget[otype] - 1)
            reasons.append(f"regime_downweight:{otype}")

    if all((types.get(k, {}) or {}).get("recommended_action") == "downweight" for k in ["confirm", "diagnose"] if k in types):
        budget["confirm"] = max(1, budget["confirm"] - 1)
        budget["diagnose"] = max(1, budget["diagnose"] - 1)
        budget["recombine"] += 1
        budget["probe"] += 1
        reasons.append("dynamic_shift_from_stalled_confirm_diagnose")

    dossier_diagnose_count = len([row for row in representative_failure_dossiers.values() if row.get("recommended_action") == "diagnose"])
    dossier_suppress_count = len([row for row in representative_failure_dossiers.values() if row.get("recommended_action") == "suppress"])
    dossier_short_window_only_count = len([row for row in representative_failure_dossiers.values() if row.get("regime_dependency") == "short_window_only"])
    dossier_parent_delta_failure_count = len([row for row in representative_failure_dossiers.values() if row.get("parent_delta_status") == "non_incremental"])
    if dossier_diagnose_count or dossier_short_window_only_count:
        budget["diagnose"] += min(2, dossier_diagnose_count + dossier_short_window_only_count)
        budget["expand"] = max(0, budget["expand"] - 1)
        reasons.append("representative_failure_bias_to_diagnose")
    if dossier_suppress_count or dossier_parent_delta_failure_count:
        budget["recombine"] = max(0, budget["recombine"] - 1)
        budget["expand"] = max(0, budget["expand"] - 1)
        budget["probe"] = max(budget.get("probe", 0), 2)
        reasons.append("representative_failure_bias_away_from_old_space_recombine")

    exploration_types = [types.get(key) or {} for key in ["expand", "recombine", "probe"]]
    exploration_no_gain = sum(int(row.get("recent_no_gain_count") or 0) for row in exploration_types)
    exploration_gain = sum(int(row.get("recent_gain_count") or 0) for row in exploration_types)
    exploration_resource_pressure = sum(int(row.get("recent_resource_exhaustion_count") or 0) for row in exploration_types)
    exploration_cooldown_count = sum(1 for row in exploration_types if row.get("cooldown_active"))
    if exploration_resource_pressure >= 2 or exploration_cooldown_count >= 2 or (exploration_no_gain >= 4 and exploration_no_gain >= (exploration_gain + 2)):
        budget["confirm"] += 1
        budget["diagnose"] += 1
        budget["expand"] = max(0, budget.get("expand", 0) - 1)
        budget["recombine"] = max(0, budget.get("recombine", 0) - 1)
        reasons.append("recent_low_yield_exploration_shift_to_validation")

    for key, floor in MIN_EXPLORATION_FLOOR.items():
        if budget.get(key, 0) < floor:
            budget[key] = floor
            reasons.append(f"exploration_floor:{key}")

    principles = (autonomy_policy.get("principles") or {})
    objectives = set(principles.get("objective") or [])
    quality_gates = autonomy_policy.get("quality_gates") or {}
    if "epistemic_gain" in objectives:
        budget["diagnose"] = max(budget.get("diagnose", 0), 2)
        budget["probe"] = max(budget.get("probe", 0), 2)
        reasons.append("autonomy_policy_epistemic_gain_floor")
    if "high_corr_duplicate_variants" in set(quality_gates.get("avoid") or []):
        budget["recombine"] = max(1, budget.get("recombine", 0))
        reasons.append("autonomy_policy_keep_recombine_but_not_dominant")

    bandit_scores = {
        otype: _bandit_score_for_type(otype, dict(types.get(otype) or {}), regime_context, objectives)
        for otype in QUESTION_TYPES
    }
    adaptive_allocations = {otype: 0 for otype in QUESTION_TYPES}
    for _ in range(ADAPTIVE_BANDIT_SLOTS):
        winner = max(
            QUESTION_TYPES,
            key=lambda otype: (bandit_scores.get(otype, 0.0) / (1 + adaptive_allocations[otype]), budget.get(otype, 0), otype),
        )
        budget[winner] = int(budget.get(winner, 0)) + 1
        adaptive_allocations[winner] += 1
    reasons.append("bandit_allocator_applied")

    child_budget = dict(CHILD_BUDGET)
    reasons.append("child_budget_reserved")
    return {
        "budget": budget,
        "child_budget": child_budget,
        "reasons": reasons,
        "autonomy_policy": autonomy_policy,
        "regime_context": regime_context,
        "representative_failure_summary": {
            "count": len(representative_failure_dossiers),
            "diagnose_count": dossier_diagnose_count,
            "suppress_count": dossier_suppress_count,
            "short_window_only_count": dossier_short_window_only_count,
            "parent_delta_failure_count": dossier_parent_delta_failure_count,
        },
        "bandit_scores": {key: round(value, 4) for key, value in bandit_scores.items()},
        "bandit_allocations": adaptive_allocations,
        "exploration_pressure": {
            "recent_no_gain_count": exploration_no_gain,
            "recent_gain_count": exploration_gain,
            "recent_resource_exhaustion_count": exploration_resource_pressure,
            "cooldown_count": exploration_cooldown_count,
        },
    }


def build_child_opportunities(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    store = _read_json(STORE_PATH, {"opportunities": {}})
    items = list((store.get("opportunities") or {}).values())
    children: list[dict[str, Any]] = []
    for row in items:
        oid = row.get("opportunity_id")
        state = row.get("state")
        evaluation = row.get("evaluation") or {}
        otype = row.get("opportunity_type")
        target_family = row.get("target_family")
        target_candidates = list(row.get("target_candidates") or [])
        if not oid:
            continue
        branchworthy = state in {"promoted", "evaluated"} or (evaluation.get("evaluation_label") in {"high_gain", "moderate_gain"})
        if not branchworthy:
            continue
        if otype == "confirm":
            children.append({"question_id": f"child-expand-from-{oid}", "question_type": "expand", "question": f"{oid} 已确认，下一步应扩展哪种验证维度？", "hypothesis": "confirm 类型机会被验证后，应自然分裂出 expand 类型子机会。", "target_family": target_family, "target_candidates": target_candidates[:2], "expected_knowledge_gain": ["window_stability_check"], "evidence_gap": "已确认信号存在，但缺少顺势扩展动作。", "sources": ["opportunity_brancher", oid], "parent_opportunity_id": oid})
        if otype == "diagnose":
            children.append({"question_id": f"child-recombine-from-{oid}", "question_type": "recombine", "question": f"{oid} 已提供失败解释，是否可据此构造新的重组型机会？", "hypothesis": "diagnose 类型机会在给出失败解释后，应该派生出更高层的重组或替代方向。", "target_family": target_family, "target_candidates": target_candidates[:2], "expected_knowledge_gain": ["exploration_candidate_survived"], "evidence_gap": "已有失败解释，但尚未把它转成新方向。", "sources": ["opportunity_brancher", oid], "parent_opportunity_id": oid})
        if oid.startswith("opp-q-recovery-to-opportunity"):
            children.append({"question_id": f"child-probe-from-{oid}", "question_type": "probe", "question": f"{oid} 作为 recovery 转机会节点，下一步是否值得小成本试探新分支？", "hypothesis": "recovery 桥接机会应该至少派生出一个低成本 probe 子机会。", "target_family": target_family, "target_candidates": target_candidates[:2], "expected_knowledge_gain": ["exploration_candidate_survived"], "evidence_gap": "recovery 已被转成机会，但还没有真正试探式子机会。", "sources": ["opportunity_brancher", oid], "parent_opportunity_id": oid})
    return children


def should_bypass_recent_fingerprint(opportunity: dict[str, Any]) -> dict[str, Any]:
    priority = float(opportunity.get("priority") or 0.0)
    novelty = float(opportunity.get("novelty_score") or 0.0)
    confidence = float(opportunity.get("confidence") or 0.0)
    otype = opportunity.get("opportunity_type") or "unknown"
    expected_gain = {
        str(item).strip()
        for item in (opportunity.get("expected_knowledge_gain") or [])
        if item
    }
    high_epistemic = bool(expected_gain & {"boundary_confirmed", "new_branch_opened", "repeated_graveyard_confirmed", "uncertainty_reduced", "stable_candidate_confirmed"})
    allow = False
    reason = None
    if otype in {"confirm", "diagnose"} and priority >= 0.88 and confidence >= 0.6:
        allow = True
        reason = "high_priority_validation_override"
    elif high_epistemic and otype in {"diagnose", "probe", "recombine"} and priority >= 0.72 and confidence >= 0.5:
        allow = True
        reason = "high_epistemic_gain_override"
    elif novelty >= 0.7 and priority >= 0.75:
        allow = True
        reason = "high_novelty_override"
    return {"allow_bypass": allow, "reason": reason}


def build_recovery_bridge_questions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    feedback = snapshot.get("analyst_feedback_context") or {}
    learning = snapshot.get("research_learning") or {}
    recovery_history = ((feedback.get("research_memory_tail") or {}).get("recovery_history_tail") or [])[-6:]
    flow_state = snapshot.get("research_flow_state") or {}
    questions: list[dict[str, Any]] = []
    if flow_state.get("state") not in {"recovering", "recovered"}:
        return questions
    stable_success = any((row.get("branch_id") == "fallback_stable_candidate_validation" and row.get("has_gain")) for row in recovery_history)
    graveyard_success = any((row.get("branch_id") == "fallback_graveyard_diagnosis" and row.get("has_gain")) for row in recovery_history)
    stable_normal_ready = ((learning.get("families") or {}).get("stable_candidate_validation") or {}).get("recommended_action") == "upweight"
    graveyard_normal_ready = ((learning.get("families") or {}).get("graveyard_diagnosis") or {}).get("recommended_action") == "upweight"
    if stable_success and not stable_normal_ready:
        questions.append({"question_id": "q-recovery-to-opportunity-stable", "question_type": "expand", "question": "recovery 已确认稳定候选，接下来最值得扩展的验证维度是什么？", "hypothesis": "稳定候选 recovery 的成功应直接转化为新的扩展型研究机会。", "target_family": "stable_candidate_validation", "target_candidates": [], "evidence_gap": "恢复动作已经成功，但当前缺少 recovery 成果到新机会的显式转化。", "sources": ["recovery_history", "research_flow_state"], "origin": "recovery_bridge"})
    if graveyard_success and not graveyard_normal_ready:
        questions.append({"question_id": "q-recovery-to-opportunity-graveyard", "question_type": "diagnose", "question": "recovery 已多次触发墓地诊断，下一步应把哪类失败解释升级为新研究机会？", "hypothesis": "墓地 recovery 的有效结果应该转成更高层的失败模式研究机会。", "target_family": "graveyard_diagnosis", "target_candidates": [], "evidence_gap": "recovery 对墓地的解释动作存在，但还没有系统化地转成新的研究机会。", "sources": ["recovery_history", "research_flow_state"], "origin": "recovery_bridge"})
    return questions
