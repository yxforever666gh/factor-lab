from __future__ import annotations

from typing import Any


def build_critic_questions(snapshot: dict[str, Any], critique: dict[str, Any]) -> list[dict[str, Any]]:
    latest_graveyard = list(snapshot.get("latest_graveyard") or [])
    stable = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")]
    questions: list[dict[str, Any]] = []

    for action in critique.get("corrective_actions") or []:
        kind = action.get("action")
        if kind == "force_positive_frontier_probe":
            questions.append({
                "question_id": "critic-frontier-positive-probe",
                "question_type": "probe",
                "question": "当前是否存在一个能对冲负结果循环的正向 frontier probe？",
                "hypothesis": "系统需要主动寻找能长出 survived candidate 的新试探方向，而不只是高价值负结果。",
                "target_family": "exploration",
                "target_candidates": stable[:2],
                "expected_knowledge_gain": ["exploration_candidate_survived"],
                "evidence_gap": "critic 发现系统陷入高价值负结果循环，需要强制注入正向 frontier probe。",
                "sources": ["meta_research_critique", "critic_action:force_positive_frontier_probe"],
                "origin": "meta_research_critic",
            })
        elif kind == "inject_meta_probe_question":
            questions.append({
                "question_id": "critic-meta-diversity-probe",
                "question_type": "probe",
                "question": "当前研究问题空间是否过窄，遗漏了尚未表达的新方向？",
                "hypothesis": "当前开放问题过少，系统需要主动补一个高多样性 meta probe。",
                "target_family": None,
                "target_candidates": (stable + latest_graveyard)[:4],
                "expected_knowledge_gain": ["exploration_candidate_survived"],
                "evidence_gap": "critic 发现 question diversity 不足，需要补一个 meta-level frontier probe。",
                "sources": ["meta_research_critique", "critic_action:inject_meta_probe_question"],
                "origin": "meta_research_critic",
            })
        elif kind == "seed_frontier_patterns":
            questions.append({
                "question_id": "critic-seed-frontier-patterns",
                "question_type": "probe",
                "question": "是否应主动播种新的 frontier patterns，以避免研究前沿枯竭？",
                "hypothesis": "当前 frontier pattern 供应不足，需要主动播种高新颖度问题。",
                "target_family": "exploration",
                "target_candidates": stable[:1],
                "expected_knowledge_gain": ["exploration_candidate_survived"],
                "evidence_gap": "critic 发现 frontier_probe 缺少 pattern 支撑，需要显式播种新 frontier pattern。",
                "sources": ["meta_research_critique", "critic_action:seed_frontier_patterns"],
                "origin": "meta_research_critic",
            })
    return questions
