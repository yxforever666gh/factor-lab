from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def build_meta_research_critique(
    snapshot: dict[str, Any],
    opportunity_learning: dict[str, Any],
    research_portfolio: dict[str, Any],
    output_path: str | Path | None = None,
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    corrective_actions: list[dict[str, Any]] = []

    families = list((opportunity_learning.get("families") or {}).values())
    patterns = list((opportunity_learning.get("patterns") or {}).values())
    horizon = list((research_portfolio.get("horizon") or []))
    open_questions = list(snapshot.get("open_questions") or [])
    repeated_failures = list(snapshot.get("repeated_failure_patterns") or [])
    knowledge_gain_counter = snapshot.get("knowledge_gain_counter") or {}

    top_family = max(families, key=lambda row: float(row.get("epistemic_value_score") or 0.0), default=None)
    if top_family and float(top_family.get("epistemic_value_score") or 0.0) >= 0.5:
        findings.append({
            "kind": "portfolio_concentration_risk",
            "severity": "medium",
            "message": f"当前研究资本明显集中在 family={top_family.get('family')}。",
            "evidence": top_family,
        })
        corrective_actions.append({
            "action": "diversify_family_allocation",
            "reason": "avoid_single_family_lock_in",
            "suggested_family": top_family.get("family"),
        })

    repetitive_patterns = [
        row for row in patterns
        if int(row.get("negative_informative_count") or 0) >= 1 and float(row.get("epistemic_value_score") or 0.0) >= 0.5
    ]
    if len(repetitive_patterns) >= 1 and int(knowledge_gain_counter.get("exploration_candidate_survived") or 0) == 0:
        findings.append({
            "kind": "negative_result_loop_warning",
            "severity": "high",
            "message": "系统当前较擅长记录高价值负结果，但仍缺少把负结果转化为新候选存活的能力。",
            "evidence": {
                "pattern_count": len(repetitive_patterns),
                "exploration_candidate_survived": knowledge_gain_counter.get("exploration_candidate_survived"),
            },
        })
        corrective_actions.append({
            "action": "force_positive_frontier_probe",
            "reason": "counterbalance_negative_loop",
            "minimum_probe_budget": 1,
        })

    if repeated_failures:
        findings.append({
            "kind": "failure_repetition_notice",
            "severity": "low",
            "message": "近期存在重复失败类型，需确认 evaluator 是否把失败模式误当成高价值主线。",
            "evidence": repeated_failures[:5],
        })
        corrective_actions.append({
            "action": "audit_evaluator_bias",
            "reason": "check_failure_rewarding_bias",
        })

    if len(open_questions) <= 2:
        findings.append({
            "kind": "question_diversity_warning",
            "severity": "medium",
            "message": "当前高价值开放问题数量偏少，研究空间可能过快塌缩。",
            "evidence": {"open_question_count": len(open_questions)},
        })
        corrective_actions.append({
            "action": "inject_meta_probe_question",
            "reason": "restore_question_diversity",
        })

    if horizon:
        pattern_slots = [row for row in horizon if row.get("focus") == "pattern_exploitation"]
        frontier_slots = [row for row in horizon if row.get("focus") == "frontier_probe"]
        if pattern_slots and frontier_slots and len((frontier_slots[0].get("patterns") or [])) == 0:
            findings.append({
                "kind": "frontier_starvation_warning",
                "severity": "medium",
                "message": "portfolio 已规划 frontier_probe，但当前没有足够 frontier pattern 支撑。",
                "evidence": frontier_slots[0],
            })
            corrective_actions.append({
                "action": "seed_frontier_patterns",
                "reason": "maintain_frontier_diversity",
            })

    payload = {
        "generated_at_utc": _iso_now(),
        "summary": {
            "finding_count": len(findings),
            "corrective_action_count": len(corrective_actions),
        },
        "findings": findings,
        "corrective_actions": corrective_actions,
    }
    out = Path(output_path) if output_path else (ARTIFACTS / "meta_research_critique.json")
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
