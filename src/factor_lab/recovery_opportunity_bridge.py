from __future__ import annotations

from typing import Any


def build_recovery_bridge_questions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    feedback = snapshot.get("analyst_feedback_context") or {}
    recovery_history = ((feedback.get("research_memory_tail") or {}).get("recovery_history_tail") or [])[-6:]
    flow_state = snapshot.get("research_flow_state") or {}

    questions: list[dict[str, Any]] = []
    if flow_state.get("state") not in {"recovering", "recovered"}:
        return questions

    stable_success = any((row.get("branch_id") == "fallback_stable_candidate_validation" and row.get("has_gain")) for row in recovery_history)
    graveyard_success = any((row.get("branch_id") == "fallback_graveyard_diagnosis" and row.get("has_gain")) for row in recovery_history)

    if stable_success:
        questions.append({
            "question_id": "q-recovery-to-opportunity-stable",
            "question_type": "expand",
            "question": "recovery 已确认稳定候选，接下来最值得扩展的验证维度是什么？",
            "hypothesis": "稳定候选 recovery 的成功应直接转化为新的扩展型研究机会。",
            "target_family": "stable_candidate_validation",
            "target_candidates": [],
            "evidence_gap": "恢复动作已经成功，但当前缺少 recovery 成果到新机会的显式转化。",
            "sources": ["recovery_history", "research_flow_state"],
            "origin": "recovery_bridge",
        })

    if graveyard_success:
        questions.append({
            "question_id": "q-recovery-to-opportunity-graveyard",
            "question_type": "diagnose",
            "question": "recovery 已多次触发墓地诊断，下一步应把哪类失败解释升级为新研究机会？",
            "hypothesis": "墓地 recovery 的有效结果应该转成更高层的失败模式研究机会。",
            "target_family": "graveyard_diagnosis",
            "target_candidates": [],
            "evidence_gap": "recovery 对墓地的解释动作存在，但还没有系统化地转成新的研究机会。",
            "sources": ["recovery_history", "research_flow_state"],
            "origin": "recovery_bridge",
        })

    return questions
