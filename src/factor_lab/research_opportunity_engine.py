from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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
    analyst = snapshot.get("analyst_signals") or {}
    flow_state = snapshot.get("research_flow_state") or _read_json(ARTIFACTS / "research_flow_state.json", {})
    learning = snapshot.get("research_learning") or _read_json(ARTIFACTS / "research_learning.json", {})
    family_summary = snapshot.get("family_summary") or []
    relationship_summary = snapshot.get("relationship_summary") or {}
    stable_candidates = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")][:5]
    latest_graveyard = list(snapshot.get("latest_graveyard") or [])[:5]
    top_scores = [row.get("factor_name") for row in (snapshot.get("top_scores") or [])[:6] if row.get("factor_name")]

    opportunities: list[dict[str, Any]] = []
    sources = ["snapshot", "analyst_signals", "research_learning", "research_flow_state"]

    if stable_candidates:
        opportunities.append(_make_opportunity(
            opportunity_id="opp-confirm-stable-candidates",
            opportunity_type="confirm",
            title="确认稳定候选是否仍具跨窗口稳健性",
            question="当前稳定候选是否只是局部窗口有效，还是具备更稳的跨窗口结构？",
            hypothesis="稳定候选在更深一层验证后依然成立，并能继续提供研究主线。",
            target_family="stable_candidate_validation",
            target_candidates=stable_candidates[:3],
            expected_knowledge_gain=["stable_candidate_confirmed"],
            evidence_gap="当前 recovery 已多次确认稳定候选，但尚未把结果转化为更广义的研究机会。",
            priority=0.84 if flow_state.get("state") in {"recovering", "recovered"} else 0.72,
            novelty_score=0.42,
            confidence=0.78,
            rationale="稳定候选是当前最强结构信号，继续确认其边界有助于后续扩展与重组。",
            sources=sources,
        ))

    if latest_graveyard:
        opportunities.append(_make_opportunity(
            opportunity_id="opp-diagnose-graveyard-pattern",
            opportunity_type="diagnose",
            title="解释 graveyard 因子是否共享失败模式",
            question="当前墓地因子是随机失败，还是存在可解释的共同结构性原因？",
            hypothesis="至少一部分 graveyard 因子共享可诊断的 neutralization / regime / 同构风险。",
            target_family="graveyard_diagnosis",
            target_candidates=latest_graveyard[:4],
            expected_knowledge_gain=["neutralization_diagnosis_requested", "repeated_graveyard_confirmed"],
            evidence_gap="当前系统已多次进入 recovery 墓地诊断，但仍需把失败解释转成新研究动作。",
            priority=0.81,
            novelty_score=0.47,
            confidence=0.74,
            rationale="失败解释是打开新研究空间的重要入口，不应只作为恢复动作存在。",
            sources=sources,
        ))

    if relationship_summary.get("hybrid_of") or relationship_summary.get("refinement_of"):
        opportunities.append(_make_opportunity(
            opportunity_id="opp-recombine-family-signals",
            opportunity_type="recombine",
            title="尝试跨 family 重组与关系扩展",
            question="当前 family 之间是否存在值得系统化验证的重组机会，而不只是重复旧候选？",
            hypothesis="候选图中的 hybrid / refinement 关系可以扩展出新的高知识增益研究方向。",
            target_family="exploration",
            target_candidates=top_scores[:4],
            expected_knowledge_gain=["exploration_candidate_survived"],
            evidence_gap="当前 exploration 仍偏弱，需要把已有关系图信号转成新的探索机会。",
            priority=0.68,
            novelty_score=0.73,
            confidence=0.55,
            rationale="关系图已经出现 hybrid/refinement 信号，说明研究空间可以继续外推。",
            sources=sources + ["relationship_summary"],
        ))

    for row in family_summary[:6]:
        family = row.get("family")
        if not family:
            continue
        family_learning = (learning.get("families") or {}).get(family, {})
        if family_learning.get("recommended_action") == "upweight":
            opportunities.append(_make_opportunity(
                opportunity_id=f"opp-expand-{family}",
                opportunity_type="expand",
                title=f"扩展 {family} 方向的研究覆盖",
                question=f"{family} family 最近有效，是否值得扩大覆盖范围与验证维度？",
                hypothesis=f"{family} family 当前不是偶然有效，而是存在进一步扩展空间。",
                target_family=family,
                target_candidates=[],
                expected_knowledge_gain=["window_stability_check"],
                evidence_gap=f"{family} 最近有效，但尚未形成更系统的扩展验证。",
                priority=0.77,
                novelty_score=0.58,
                confidence=0.69,
                rationale="研究学习显示该 family 最近有价值，适合从被动保留升级为主动扩展。",
                sources=sources + ["research_learning"],
            ))

    opportunities.sort(key=lambda row: (-float(row.get("priority") or 0.0), -float(row.get("novelty_score") or 0.0), row.get("opportunity_id") or ""))
    payload = {
        "generated_at_utc": _iso_now(),
        "schema_version": SCHEMA_VERSION,
        "flow_state": flow_state,
        "summary": {
            "count": len(opportunities),
            "top_types": sorted({row.get("opportunity_type") for row in opportunities if row.get("opportunity_type")}),
        },
        "opportunities": opportunities[:12],
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
