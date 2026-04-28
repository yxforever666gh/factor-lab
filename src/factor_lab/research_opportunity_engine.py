from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.question_generator import build_research_questions
from factor_lab.critic_question_generator import build_critic_questions
from factor_lab.cheap_screen_promotion import build_full_run_followups
from factor_lab.opportunity_scorer import score_opportunity
from factor_lab.opportunity_policy import build_opportunity_learning, allocate_opportunity_budget, build_child_opportunities
from factor_lab.research_portfolio import build_research_portfolio_plan
from factor_lab.meta_research_critic import build_meta_research_critique
from factor_lab.storage import ExperimentStore
from factor_lab.research_runtime_state import recently_finished_same_fingerprint, task_repeat_cooldown_minutes

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
SCHEMA_VERSION = "factor_lab.research_opportunity.v1"
AUTONOMY_POLICY_PATH = ROOT / "configs" / "research_autonomy_policy.json"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_opportunity(
    *,
    opportunity_id: str,
    opportunity_type: str,
    title: str,
    question: str,
    hypothesis: str,
    target_family: str | None,
    target_candidates: list[str],
    expected_knowledge_gain: list[str],
    evidence_gap: str,
    priority: float,
    novelty_score: float,
    confidence: float,
    rationale: str,
    sources: list[str],
) -> dict[str, Any]:
    return {
        "opportunity_id": opportunity_id,
        "schema_version": SCHEMA_VERSION,
        "opportunity_type": opportunity_type,
        "title": title,
        "question": question,
        "hypothesis": hypothesis,
        "target_family": target_family,
        "target_candidates": target_candidates,
        "expected_knowledge_gain": expected_knowledge_gain,
        "evidence_gap": evidence_gap,
        "priority": round(priority, 3),
        "novelty_score": round(novelty_score, 3),
        "confidence": round(confidence, 3),
        "rationale": rationale,
        "sources": sources,
    }


def _question_fingerprint(question: dict[str, Any]) -> str:
    payload = {
        "type": question.get("question_type"),
        "family": question.get("target_family"),
        "targets": list(question.get("target_candidates") or []),
        "expected": list(question.get("expected_knowledge_gain") or []),
        "parent": question.get("parent_opportunity_id"),
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _should_pre_suppress_question(question: dict[str, Any], store: ExperimentStore, autonomy_policy: dict[str, Any] | None = None) -> bool:
    sources = set(question.get("sources") or [])
    if "meta_research_critique" in "".join(sources) or "pattern_learning" in "".join(sources):
        return False
    fingerprint = f"pre_question::{_question_fingerprint(question)}"
    repeated = recently_finished_same_fingerprint(
        store,
        fingerprint,
        cooldown_minutes=task_repeat_cooldown_minutes(task_type="diagnostic", payload={"expected_information_gain": list(question.get("expected_knowledge_gain") or [])}),
    )
    if not repeated:
        return False
    expected_gain = set(question.get("expected_knowledge_gain") or [])
    objectives = set((((autonomy_policy or {}).get("principles") or {}).get("objective") or []))
    if "epistemic_gain" in objectives and expected_gain & {"search_space_reduced", "boundary_confirmed", "new_branch_opened", "repeated_graveyard_confirmed"}:
        return False
    return True


def build_research_opportunities(snapshot_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    snapshot = _read_json(Path(snapshot_path), {})
    flow_state = snapshot.get("research_flow_state") or _read_json(ARTIFACTS / "research_flow_state.json", {})
    recovery_state = flow_state.get("state")
    store = ExperimentStore(ROOT / "artifacts" / "factor_lab.db")

    base_questions = build_research_questions(snapshot)
    child_questions = build_child_opportunities(snapshot)

    opportunity_learning = build_opportunity_learning()
    research_portfolio = build_research_portfolio_plan(snapshot, opportunity_learning, ARTIFACTS / "research_portfolio_plan.json")
    meta_research_critique = build_meta_research_critique(snapshot, opportunity_learning, research_portfolio, ARTIFACTS / "meta_research_critique.json")
    critic_questions = build_critic_questions(snapshot, meta_research_critique)
    promoted_full_run_questions = build_full_run_followups()
    questions = list(base_questions) + list(critic_questions) + list(promoted_full_run_questions) + list(child_questions)
    autonomy_policy = _read_json(AUTONOMY_POLICY_PATH, {})
    opportunity_budget = allocate_opportunity_budget(snapshot, opportunity_learning)
    type_budget = dict(opportunity_budget.get("budget") or {})
    child_budget = dict(opportunity_budget.get("child_budget") or {})

    opportunities: list[dict[str, Any]] = []
    suppressed_questions: list[dict[str, Any]] = []
    for question in questions:
        qtype = question.get("question_type") or "probe"
        if qtype in type_budget and int(type_budget.get(qtype, 0)) <= 0:
            continue
        if _should_pre_suppress_question(question, store, autonomy_policy):
            suppressed_questions.append({
                "question_id": question.get("question_id"),
                "reason": "pre_suppressed_recent_repeat",
                "sources": question.get("sources") or [],
            })
            continue
        scores = score_opportunity(question, snapshot)
        expected_gain = set(question.get("expected_knowledge_gain") or [])
        epistemic_bonus = 0.0
        if expected_gain & {"search_space_reduced", "boundary_confirmed", "new_branch_opened", "repeated_graveyard_confirmed"}:
            epistemic_bonus = 0.08
        opportunity = _make_opportunity(
            opportunity_id=f"opp-{question['question_id']}",
            opportunity_type=qtype,
            title=question.get("question") or question.get("question_id") or "untitled",
            question=question.get("question") or "",
            hypothesis=question.get("hypothesis") or "",
            target_family=question.get("target_family"),
            target_candidates=list(question.get("target_candidates") or []),
            expected_knowledge_gain=list(question.get("expected_knowledge_gain") or []),
            evidence_gap=question.get("evidence_gap") or "",
            priority=min(0.99, float(scores.get("priority") or 0.5) + epistemic_bonus),
            novelty_score=float(scores.get("novelty_score") or 0.5),
            confidence=float(scores.get("confidence") or 0.5),
            rationale=(str(scores.get("score_rationale") or "") + (" | autonomy_policy: epistemic_gain_priority" if epistemic_bonus > 0 else "")),
            sources=list(question.get("sources") or []),
        )
        opportunity["regime"] = scores.get("regime")
        opportunity["regime_confidence"] = scores.get("regime_confidence")
        if question.get("parent_opportunity_id"):
            opportunity["parent_opportunity_id"] = question.get("parent_opportunity_id")
        opportunities.append(opportunity)
        if qtype in type_budget:
            type_budget[qtype] = max(0, int(type_budget.get(qtype, 0)) - 1)

    opportunities.sort(
        key=lambda row: (
            -float(row.get("priority") or 0.0),
            -float(row.get("novelty_score") or 0.0),
            row.get("opportunity_id") or "",
        )
    )

    payload = {
        "generated_at_utc": _iso_now(),
        "schema_version": SCHEMA_VERSION,
        "flow_state": flow_state,
        "summary": {
            "count": len(opportunities),
            "question_count": len(questions),
            "suppressed_question_count": len(suppressed_questions),
            "child_question_count": len(child_questions),
            "top_types": sorted({row.get("opportunity_type") for row in opportunities if row.get("opportunity_type")}),
            "opportunity_budget": opportunity_budget,
            "opportunity_learning": opportunity_learning,
            "research_portfolio": research_portfolio,
            "meta_research_critique": meta_research_critique,
            "recovery_state": recovery_state,
            "child_budget_remaining": child_budget,
        },
        "opportunities": opportunities[:12],
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
