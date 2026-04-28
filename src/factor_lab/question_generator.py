from __future__ import annotations

from typing import Any

from factor_lab.exploration_budget import build_exploration_budget
from factor_lab.new_branch_generator import build_new_branch_questions
from factor_lab.pattern_question_generator import build_pattern_native_questions
from factor_lab.recovery_opportunity_bridge import build_recovery_bridge_questions
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AUTONOMY_POLICY_PATH = ROOT / "configs" / "research_autonomy_policy.json"


def _pattern_entries(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return (snapshot.get("research_learning") or {}).get("patterns") or {}

def _autonomy_policy() -> dict[str, Any]:
    if not AUTONOMY_POLICY_PATH.exists():
        return {}
    return json.loads(AUTONOMY_POLICY_PATH.read_text(encoding="utf-8"))


def _epistemic_priority_gain(expected_gain: list[str], policy: dict[str, Any]) -> bool:
    objectives = set(((policy.get("principles") or {}).get("objective") or []))
    if "epistemic_gain" not in objectives:
        return False
    return bool(set(expected_gain or []) & {"search_space_reduced", "boundary_confirmed", "new_branch_opened", "repeated_graveyard_confirmed", "stable_candidate_confirmed"})


def _pattern_action_for(question_type: str, family: str | None, parent_kind: str, target_count: int, expected_gain: list[str], snapshot: dict[str, Any]) -> str | None:
    family_key = family or "none"
    if target_count <= 0:
        target_shape = "no_targets"
    elif target_count == 1:
        target_shape = "single_target"
    elif target_count == 2:
        target_shape = "pair_target"
    else:
        target_shape = "multi_target"
    intent = "+".join(sorted([str(x) for x in expected_gain if x])[:3]) if expected_gain else "no_expected_gain"
    prefix = f"{question_type}::{family_key}::{parent_kind}::{target_shape}::{intent}::"
    patterns = _pattern_entries(snapshot)
    best_key = None
    best_score = -999.0
    for key, meta in patterns.items():
        if not str(key).startswith(prefix):
            continue
        score = float(meta.get("epistemic_value_score") or 0.0)
        if score > best_score:
            best_score = score
            best_key = key
    if not best_key:
        return None
    return (patterns.get(best_key) or {}).get("recommended_action")


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
    autonomy_policy = _autonomy_policy()

    # Pattern-native questions come first; rules now act as fallback/coverage.
    questions.extend(build_pattern_native_questions(snapshot))

    if stable_candidates and int(question_budget.get("confirm", 0)) > 0:
        expected_gain = ["stable_candidate_confirmed"]
        pattern_action = _pattern_action_for("confirm", "stable_candidate_validation", "root", len(stable_candidates[:3]), expected_gain, snapshot)
        if pattern_action != "downweight" or _epistemic_priority_gain(expected_gain, autonomy_policy):
            questions.append({
                "question_id": "q-stable-boundary",
                "question_type": "confirm",
                "question": "当前稳定候选的有效性边界在哪里？",
                "hypothesis": "稳定候选并非只在局部窗口有效，而是存在可继续扩展验证的稳健边界。",
                "target_family": "stable_candidate_validation",
                "target_candidates": stable_candidates[:3],
                "expected_knowledge_gain": expected_gain,
                "evidence_gap": "稳定候选已多次被恢复动作确认，但缺少更主动的边界验证。",
                "sources": ["stable_candidates", "analyst_feedback_context", "exploration_budget", "pattern_learning"],
            })

    if latest_graveyard and int(question_budget.get("diagnose", 0)) > 0:
        expected_gain = ["neutralization_diagnosis_requested", "repeated_graveyard_confirmed"]
        pattern_action = _pattern_action_for("diagnose", "graveyard_diagnosis", "root", len(latest_graveyard[:4]), expected_gain, snapshot)
        if pattern_action != "downweight" or _epistemic_priority_gain(expected_gain, autonomy_policy):
            questions.append({
                "question_id": "q-graveyard-cause",
                "question_type": "diagnose",
                "question": "当前 graveyard 因子的失败是否共享共同原因？",
                "hypothesis": "至少部分 graveyard 因子并非随机失败，而是共享可解释的结构性原因。",
                "target_family": "graveyard_diagnosis",
                "target_candidates": latest_graveyard[:4],
                "expected_knowledge_gain": expected_gain,
                "evidence_gap": "墓地诊断频繁出现，但失败解释尚未系统转化成新研究机会。",
                "sources": ["latest_graveyard", "analyst_signals", "analyst_feedback_context", "exploration_budget", "pattern_learning"],
            })

    if (relationship_summary.get("hybrid_of") or relationship_summary.get("refinement_of") or stable_candidates) and int(question_budget.get("recombine", 0)) > 0:
        expected_gain = ["exploration_candidate_survived"]
        pattern_action = _pattern_action_for("recombine", "exploration", "root", len(stable_candidates[:2]), expected_gain, snapshot)
        if pattern_action != "downweight" or _epistemic_priority_gain(expected_gain, autonomy_policy):
            questions.append({
                "question_id": "q-recombine-space",
                "question_type": "recombine",
                "question": "现有候选图里的 hybrid / refinement 关系能否扩展出新的研究方向？",
                "hypothesis": "当前候选关系图并未被充分利用，仍能长出新的高信息增益方向。",
                "target_family": "exploration",
                "target_candidates": stable_candidates[:2],
                "expected_knowledge_gain": expected_gain,
                "evidence_gap": "关系图已出现结构性信号，但 exploration 仍偏弱。",
                "sources": ["relationship_summary", "candidate_graph", "exploration_budget", "pattern_learning"],
            })

    if int(question_budget.get("expand", 0)) > 0:
        for row in family_summary[:6]:
            family = row.get("family")
            if not family:
                continue
            family_learning = (learning.get("families") or {}).get(family, {})
            expected_gain = ["window_stability_check"]
            pattern_action = _pattern_action_for("expand", family, "root", 0, expected_gain, snapshot)
            if family_learning.get("recommended_action") == "upweight" or pattern_action == "upweight":
                questions.append({
                    "question_id": f"q-expand-{family}",
                    "question_type": "expand",
                    "question": f"{family} family 最近有效，是否值得扩大验证覆盖？",
                    "hypothesis": f"{family} family 当前的有效性不是偶然噪声，而是值得主动扩展的研究机会。",
                    "target_family": family,
                    "target_candidates": [],
                    "expected_knowledge_gain": expected_gain,
                    "evidence_gap": f"{family} 最近有效，但尚未形成主动扩展动作。",
                    "sources": ["research_learning", "family_summary", "exploration_budget", "pattern_learning"],
                })

    llm_feedback = (feedback.get("llm_execution_feedback") or {}).get("retrospective") or {}
    if llm_feedback.get("core_candidates_lost") and int(question_budget.get("diagnose", 0)) > 0:
        expected_gain = ["neutralization_diagnosis_requested"]
        pattern_action = _pattern_action_for("diagnose", None, "root", len(list(llm_feedback.get("core_candidates_lost") or [])[:4]), expected_gain, snapshot)
        if pattern_action != "downweight" or _epistemic_priority_gain(expected_gain, autonomy_policy):
            questions.append({
                "question_id": "q-llm-plan-mismatch",
                "question_type": "diagnose",
                "question": "为什么 LLM 计划中的核心候选在执行后没有保留下来？",
                "hypothesis": "当前计划建议与执行结果之间存在结构性错配，需要显式诊断。",
                "target_family": None,
                "target_candidates": list(llm_feedback.get("core_candidates_lost") or [])[:4],
                "expected_knowledge_gain": expected_gain,
                "evidence_gap": "LLM retrospective 显示核心候选丢失，但下游还未把它当作独立研究问题。",
                "sources": ["llm_retrospective", "analyst_feedback_context", "exploration_budget", "pattern_learning"],
            })

    bridge_questions = build_recovery_bridge_questions(snapshot)
    branch_questions = build_new_branch_questions(snapshot)
    if int(question_budget.get("expand", 0)) > 0 or int(question_budget.get("diagnose", 0)) > 0:
        questions.extend(bridge_questions)
    if int(question_budget.get("probe", 0)) > 0 or int(question_budget.get("recombine", 0)) > 0:
        questions.extend(branch_questions)

    pattern_promoted_recombine = _pattern_action_for("recombine", "graveyard_diagnosis", "child", 2, ["exploration_candidate_survived"], snapshot)
    if pattern_promoted_recombine == "upweight" and int(question_budget.get("recombine", 0)) > 0:
        questions.append({
            "question_id": "q-pattern-graveyard-child-recombine",
            "question_type": "recombine",
            "question": "graveyard 诊断派生的 child recombine 模式是否仍值得系统性试探？",
            "hypothesis": "某些来自 graveyard diagnosis 的 child recombine，即使经常给出负结果，也能持续高质量缩小搜索空间。",
            "target_family": "graveyard_diagnosis",
            "target_candidates": latest_graveyard[:2],
            "expected_knowledge_gain": ["exploration_candidate_survived"],
            "evidence_gap": "pattern learning 显示该模式具有认知价值，但当前问题生成还没有显式承接它。",
            "sources": ["pattern_learning", "research_learning", "latest_graveyard"],
            "origin": "pattern_learning",
        })

    return questions
