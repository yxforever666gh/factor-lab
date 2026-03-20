from __future__ import annotations

from typing import Any


def build_new_branch_questions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    relationship_summary = snapshot.get("relationship_summary") or {}
    top_scores = [row.get("factor_name") for row in (snapshot.get("top_scores") or [])[:6] if row.get("factor_name")]
    stable_candidates = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")][:4]
    family_summary = snapshot.get("family_summary") or []

    questions: list[dict[str, Any]] = []

    if relationship_summary.get("hybrid_of"):
        questions.append({
            "question_id": "q-new-branch-hybrid-expansion",
            "question_type": "recombine",
            "question": "现有 hybrid 关系是否值得长出新的跨 family 分支？",
            "hypothesis": "hybrid 关系不是偶然拼接，而是可延展的研究新分支。",
            "target_family": "exploration",
            "target_candidates": stable_candidates[:2],
            "evidence_gap": "候选图已有 hybrid 线索，但系统尚未把它系统化为新分支。",
            "sources": ["relationship_summary", "candidate_graph"],
            "origin": "new_branch_generator",
        })

    if len(top_scores) >= 3:
        questions.append({
            "question_id": "q-new-branch-topscore-probe",
            "question_type": "probe",
            "question": "评分靠前但未被充分验证的因子组合里，是否藏着新方向？",
            "hypothesis": "top score 尾部候选中仍有未展开的研究方向。",
            "target_family": None,
            "target_candidates": top_scores[2:6],
            "evidence_gap": "高分因子被排名看见了，但还没被组织成明确的新分支机会。",
            "sources": ["top_scores"],
            "origin": "new_branch_generator",
        })

    for row in family_summary[:5]:
        family = row.get("family")
        if not family:
            continue
        if row.get("recommended_action") == "explore_new_branch":
            questions.append({
                "question_id": f"q-new-branch-family-{family}",
                "question_type": "probe",
                "question": f"{family} family 是否值得主动开出一条新的探索分支？",
                "hypothesis": f"{family} family 当前具有形成新研究分支的潜力。",
                "target_family": family,
                "target_candidates": [],
                "evidence_gap": f"{family} 被建议探索新分支，但目前还没有机会对象承接。",
                "sources": ["family_summary"],
                "origin": "new_branch_generator",
            })

    return questions
