from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.question_generator import build_research_questions
from factor_lab.opportunity_scorer import score_opportunity
from factor_lab.opportunity_learning import build_opportunity_learning
from factor_lab.opportunity_budget_allocator import allocate_opportunity_budget
from factor_lab.opportunity_brancher import build_child_opportunities

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
SCHEMA_VERSION = "factor_lab.research_opportunity.v1"


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


def build_research_opportunities(snapshot_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    snapshot = _read_json(Path(snapshot_path), {})
    flow_state = snapshot.get("research_flow_state") or _read_json(ARTIFACTS / "research_flow_state.json", {})

    base_questions = build_research_questions(snapshot)
    child_questions = build_child_opportunities(snapshot)
    questions = list(base_questions) + list(child_questions)

    opportunity_learning = build_opportunity_learning()
    opportunity_budget = allocate_opportunity_budget(snapshot, opportunity_learning)
    type_budget = dict(opportunity_budget.get("budget") or {})

    opportunities: list[dict[str, Any]] = []
    for question in questions:
        qtype = question.get("question_type") or "probe"
        if qtype in type_budget and int(type_budget.get(qtype, 0)) <= 0:
            continue
        scores = score_opportunity(question, snapshot)
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
            priority=float(scores.get("priority") or 0.5),
            novelty_score=float(scores.get("novelty_score") or 0.5),
            confidence=float(scores.get("confidence") or 0.5),
            rationale=str(scores.get("score_rationale") or ""),
            sources=list(question.get("sources") or []),
        )
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
            "child_question_count": len(child_questions),
            "top_types": sorted({row.get("opportunity_type") for row in opportunities if row.get("opportunity_type")}),
            "opportunity_budget": opportunity_budget,
            "opportunity_learning": opportunity_learning,
        },
        "opportunities": opportunities[:12],
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
