from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"
MIN_EXPLORATION_FLOOR = {"recombine": 1, "probe": 1}
CHILD_BUDGET = {"expand": 1, "recombine": 1, "probe": 1}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def build_opportunity_learning(store_path: str | Path | None = None, output_path: str | Path | None = None) -> dict[str, Any]:
    spath = Path(store_path) if store_path else STORE_PATH
    opath = Path(output_path) if output_path else (ARTIFACTS / "opportunity_learning.json")
    store = _read_json(spath, {"opportunities": {}})
    items = list((store.get("opportunities") or {}).values())

    types: dict[str, dict[str, Any]] = {}
    for row in items:
        otype = row.get("opportunity_type") or "unknown"
        meta = types.setdefault(otype, {
            "opportunity_type": otype,
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
        })
        meta["count"] += 1
        state = row.get("state")
        if state == "promoted":
            meta["promoted"] += 1
        elif state == "evaluated":
            meta["evaluated"] += 1
        elif state == "rejected":
            meta["rejected"] += 1
        elif state == "archived":
            meta["archived"] += 1

        evaluation = row.get("evaluation") or {}
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

    for meta in types.values():
        terminal = meta["promoted"] + meta["evaluated"] + meta["rejected"] + meta["archived"]
        if terminal > 0:
            meta["success_rate"] = round(meta["promoted"] / terminal, 3)
            meta["epistemic_value_score"] = round(meta["epistemic_value_score"] / terminal, 3)
        if meta["success_rate"] is None:
            meta["recommended_action"] = "keep"
        elif meta["uncertainty_reduction_count"] >= 2 or meta["new_branch_count"] >= 1 or meta["epistemic_value_score"] >= 0.45:
            meta["recommended_action"] = "upweight"
        elif meta["repeat_signal_count"] >= max(2, meta["uncertainty_reduction_count"] + meta["negative_informative_count"]) or meta["epistemic_value_score"] <= -0.25:
            meta["recommended_action"] = "downweight"
        elif meta["negative_informative_count"] >= 1 and meta["repeat_signal_count"] == 0:
            meta["recommended_action"] = "keep"
        else:
            meta["recommended_action"] = "keep"

    payload = {"types": types}
    opath.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def allocate_opportunity_budget(snapshot: dict[str, Any], opportunity_learning: dict[str, Any]) -> dict[str, Any]:
    flow_state = snapshot.get("research_flow_state") or {}
    types = opportunity_learning.get("types") or {}
    recovery_state = flow_state.get("state")

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

    if all((types.get(k, {}) or {}).get("recommended_action") == "downweight" for k in ["confirm", "diagnose"] if k in types):
        budget["confirm"] = max(1, budget["confirm"] - 1)
        budget["diagnose"] = max(1, budget["diagnose"] - 1)
        budget["recombine"] += 1
        budget["probe"] += 1
        reasons.append("dynamic_shift_from_stalled_confirm_diagnose")

    for key, floor in MIN_EXPLORATION_FLOOR.items():
        if budget.get(key, 0) < floor:
            budget[key] = floor
            reasons.append(f"exploration_floor:{key}")

    child_budget = dict(CHILD_BUDGET)
    reasons.append("child_budget_reserved")
    return {"budget": budget, "child_budget": child_budget, "reasons": reasons}


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
    allow = False
    reason = None
    if otype in {"confirm", "diagnose"} and priority >= 0.88 and confidence >= 0.6:
        allow = True
        reason = "high_priority_validation_override"
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
