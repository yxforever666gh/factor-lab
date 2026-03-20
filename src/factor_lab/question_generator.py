from __future__ import annotations

from typing import Any

from factor_lab.exploration_budget import build_exploration_budget
from factor_lab.new_branch_generator import build_new_branch_questions
from factor_lab.recovery_opportunity_bridge import build_recovery_bridge_questions


def build_research_questions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    analyst = snapshot.get("analyst_signals") or {}
    feedback = snapshot.get("analyst_feedback_context") or {}
    learning = snapshot.get("research_learning") or {}
    relationship_summary = snapshot.get("relationship_summary") or {}
    stable_candidates = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")][:5]
    latest_graveyard = list(snapshot.get("latest_graveyard") or [])[:5]
    family_summary = snapshot.get("family_summary") or []

    budget_payload = build_exploration_budget(snapshot)
    question_budget = budget_payload.get("budget") or {}

    questions: list[dict[str, Any]] = []

    if stable_candidates and int(question_budget.get("confirm", 0)) > 0:
        questions.append({
            "question_id": "q-stable-boundary",
            "question_type": "confirm",
            "question": "当前稳定候选的有效性边界在哪里？",
            "hypothesis": "稳定候选并非只在局部窗口有效，而是存在可继续扩展验证的稳健边界。",
            "target_family": "stable_candidate_validation",
            "target_candidates": stable_candidates[:3],
            "expected_knowledge_gain": ["stable_candidate_confirmed"],
            "evidence_gap": "稳定候选已多次被恢复动作确认，但缺少更主动的边界验证。",
            "sources": ["stable_candidates", "analyst_feedback_context", "exploration_budget"],
        })

    if latest_graveyard and int(question_budget.get("diagnose", 0)) > 0:
        questions.append({
            "question_id": "q-graveyard-cause",
            "question_type": "diagnose",
            "question": "当前 graveyard 因子的失败是否共享共同原因？",
            "hypothesis": "至少部分 graveyard 因子并非随机失败，而是共享可解释的结构性原因。",
            "target_family": "graveyard_diagnosis",
            "target_candidates": latest_graveyard[:4],
            "expected_knowledge_gain": ["neutralization_diagnosis_requested", "repeated_graveyard_confirmed"],
            "evidence_gap": "墓地诊断频繁出现，但失败解释尚未系统转化成新研究机会。",
            "sources": ["latest_graveyard", "analyst_signals", "analyst_feedback_context", "exploration_budget"],
        })

    if (relationship_summary.get("hybrid_of") or relationship_summary.get("refinement_of")) and int(question_budget.get("recombine", 0)) > 0:
        questions.append({
            "question_id": "q-recombine-space",
            "question_type": "recombine",
            "question": "现有候选图里的 hybrid / refinement 关系能否扩展出新的研究方向？",
            "hypothesis": "当前候选关系图并未被充分利用，仍能长出新的高信息增益方向。",
            "target_family": "exploration",
            "target_candidates": stable_candidates[:2],
            "expected_knowledge_gain": ["exploration_candidate_survived"],
            "evidence_gap": "关系图已出现结构性信号，但 exploration 仍偏弱。",
            "sources": ["relationship_summary", "candidate_graph", "exploration_budget"],
        })

    if int(question_budget.get("expand", 0)) > 0:
        for row in family_summary[:6]:
            family = row.get("family")
            if not family:
                continue
            family_learning = (learning.get("families") or {}).get(family, {})
            if family_learning.get("recommended_action") == "upweight":
                questions.append({
                    "question_id": f"q-expand-{family}",
                    "question_type": "expand",
                    "question": f"{family} family 最近有效，是否值得扩大验证覆盖？",
                    "hypothesis": f"{family} family 当前的有效性不是偶然噪声，而是值得主动扩展的研究机会。",
                    "target_family": family,
                    "target_candidates": [],
                    "expected_knowledge_gain": ["window_stability_check"],
                    "evidence_gap": f"{family} 最近有效，但尚未形成主动扩展动作。",
                    "sources": ["research_learning", "family_summary", "exploration_budget"],
                })

    llm_feedback = (feedback.get("llm_execution_feedback") or {}).get("retrospective") or {}
    if llm_feedback.get("core_candidates_lost") and int(question_budget.get("diagnose", 0)) > 0:
        questions.append({
            "question_id": "q-llm-plan-mismatch",
            "question_type": "diagnose",
            "question": "为什么 LLM 计划中的核心候选在执行后没有保留下来？",
            "hypothesis": "当前计划建议与执行结果之间存在结构性错配，需要显式诊断。",
            "target_family": None,
            "target_candidates": list(llm_feedback.get("core_candidates_lost") or [])[:4],
            "expected_knowledge_gain": ["neutralization_diagnosis_requested"],
            "evidence_gap": "LLM retrospective 显示核心候选丢失，但下游还未把它当作独立研究问题。",
            "sources": ["llm_retrospective", "analyst_feedback_context", "exploration_budget"],
        })

    bridge_questions = build_recovery_bridge_questions(snapshot)
    branch_questions = build_new_branch_questions(snapshot)
    if int(question_budget.get("expand", 0)) > 0 or int(question_budget.get("diagnose", 0)) > 0:
        questions.extend(bridge_questions)
    if int(question_budget.get("probe", 0)) > 0 or int(question_budget.get("recombine", 0)) > 0:
        questions.extend(branch_questions)

    return questions
