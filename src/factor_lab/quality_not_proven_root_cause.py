from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _top_borderline_candidates(promotion_rows: list[dict[str, Any]], novelty_rows: dict[str, dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    candidates = []
    for row in promotion_rows:
        decision = row.get("quality_promotion_decision")
        if decision not in {"keep_validating", "do_not_promote", "suppress"}:
            continue
        name = row.get("factor_name")
        if not name:
            continue
        hard_flags = row.get("quality_hard_flags") or {}
        active_flags = sorted([key for key, value in hard_flags.items() if value])
        novelty = novelty_rows.get(name) or {}
        candidates.append(
            {
                "candidate_name": name,
                "quality_total_score": row.get("quality_total_score"),
                "quality_classification": row.get("quality_classification"),
                "quality_promotion_decision": decision,
                "active_hard_flags": active_flags,
                "novelty_class": novelty.get("novelty_class"),
                "novelty_action": novelty.get("recommended_action"),
                "summary": row.get("quality_summary"),
            }
        )
    candidates.sort(
        key=lambda row: (
            -(int(row.get("quality_total_score") or 0)),
            len(row.get("active_hard_flags") or []),
            row.get("candidate_name") or "",
        )
    )
    return candidates[:limit]


def build_quality_not_proven_root_cause_report(artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    effect = _read_json(artifacts_dir / "factor_quality_effect_report.json", {})
    promotion = _read_json(artifacts_dir / "promotion_scorecard.json", {})
    novelty = _read_json(artifacts_dir / "novelty_judgments.json", {})
    novelty_summary = _read_json(artifacts_dir / "novelty_judge_summary.json", {})
    novelty_calibration = _read_json(artifacts_dir / "novelty_calibration_report.json", {})
    approved = _read_json(artifacts_dir / "approved_candidate_universe.json", {})
    au_diagnosis = _read_json(artifacts_dir / "au_zero_diagnosis.json", {})
    decision_ab = _read_json(artifacts_dir / "decision_ab_report.json", {})
    consistency = _read_json(artifacts_dir / "artifact_consistency_report.json", {})

    promotion_rows = list(promotion.get("rows") or [])
    novelty_rows = {row.get("candidate_name"): row for row in (novelty.get("rows") or []) if row.get("candidate_name")}
    borderline = _top_borderline_candidates(promotion_rows, novelty_rows)

    hard_flag_counts: Counter[str] = Counter()
    for row in promotion_rows:
        for key, value in (row.get("quality_hard_flags") or {}).items():
            if value:
                hard_flag_counts[key] += 1

    root_causes: list[dict[str, Any]] = []
    approved_count = (approved.get("summary") or {}).get("approved_count") or 0
    stable_alpha_count = (promotion.get("summary") or {}).get("stable_alpha_candidate_count") or 0
    near_neighbor_soft = int(novelty_calibration.get("soft_neighbor_count") or 0)
    hard_duplicate = int(novelty_calibration.get("hard_duplicate_count") or 0)
    meaningful_extension = int(novelty_calibration.get("meaningful_extension_count") or 0)
    ab_recommendation = decision_ab.get("recommendation")

    if meaningful_extension == 0 and near_neighbor_soft + hard_duplicate > 0:
        root_causes.append(
            {
                "cause_key": "search_space_too_narrow",
                "confidence": 0.84,
                "evidence": {
                    "meaningful_extension_count": meaningful_extension,
                    "near_neighbor_soft": near_neighbor_soft,
                    "hard_duplicate": hard_duplicate,
                },
                "why": "新机制/有新增信息的候选没有长出来，绝大多数仍落在近邻或重复区域。",
            }
        )

    if hard_flag_counts.get("evidence_missing", 0) > 0 or hard_flag_counts.get("evidence_blocked", 0) > 0:
        root_causes.append(
            {
                "cause_key": "evidence_gate_or_validation_insufficient",
                "confidence": 0.7,
                "evidence": {
                    "evidence_missing": hard_flag_counts.get("evidence_missing", 0),
                    "evidence_blocked": hard_flag_counts.get("evidence_blocked", 0),
                },
                "why": "部分候选没有被证明，不是因为完全无效，而是因为证据链不完整或仍被 validation gate 卡住。",
            }
        )

    if approved_count > 0 and stable_alpha_count == 0:
        root_causes.append(
            {
                "cause_key": "admission_exists_but_quality_not_upgraded",
                "confidence": 0.78,
                "evidence": {
                    "approved_count": approved_count,
                    "stable_alpha_candidate_count": stable_alpha_count,
                },
                "why": "系统已经能留下 AU 成员，但还没有把它们提升成可证明的 stable alpha。",
            }
        )

    if ab_recommendation == "reject":
        root_causes.append(
            {
                "cause_key": "functional_agents_not_yet_converting_to_quality",
                "confidence": 0.76,
                "evidence": {
                    "ab_recommendation": ab_recommendation,
                    "quality_delta": decision_ab.get("quality_delta"),
                    "duplicate_delta": decision_ab.get("duplicate_delta"),
                },
                "why": "功能型 agent 目前更多带来收缩/去重，而不是直接转化成质量提升。",
            }
        )

    if consistency.get("warning_count"):
        root_causes.append(
            {
                "cause_key": "artifact_staleness_or_mismatch",
                "confidence": 0.55,
                "evidence": {"warning_count": consistency.get("warning_count"), "warnings": consistency.get("warnings")},
                "why": "产物链仍有不同步，会放大判断噪声。",
            }
        )

    root_causes.sort(key=lambda row: (-float(row.get("confidence") or 0.0), row.get("cause_key") or ""))

    next_actions: list[dict[str, Any]] = []
    if any(row.get("cause_key") == "search_space_too_narrow" for row in root_causes):
        next_actions.append(
            {
                "action_key": "expand_search_space_away_from_neighbors",
                "priority": 1,
                "why": "先让 reroute / failure question card 把 budget 推向 far-family / new-mechanism，而不是旧近邻。",
            }
        )
    if any(row.get("cause_key") == "evidence_gate_or_validation_insufficient" for row in root_causes):
        next_actions.append(
            {
                "action_key": "targeted_validation_for_borderline_candidates",
                "priority": 2,
                "why": "对最接近晋级的候选补中长窗/neutralized/persistence 证据，而不是扩大泛探索。",
            }
        )
    if any(row.get("cause_key") == "admission_exists_but_quality_not_upgraded" for row in root_causes):
        next_actions.append(
            {
                "action_key": "separate_au_membership_from_quality_proof",
                "priority": 3,
                "why": "明确定义 AU 成员只是可保留研究对象，不等于已经证明质量提升。",
            }
        )
    if not next_actions:
        next_actions.append(
            {
                "action_key": "observe_more_samples",
                "priority": 9,
                "why": "当前没有足够根因信号，先继续观察更多样本。",
            }
        )

    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "schema_version": "factor_lab.quality_not_proven_root_cause.v1",
        "current_state": {
            "factor_quality_judgment": ((effect.get("final_judgment") or {}).get("factor_quality") or "unknown"),
            "approved_count": approved_count,
            "stable_alpha_candidate_count": stable_alpha_count,
            "ab_recommendation": ab_recommendation,
            "au_zero_direct_cause": (au_diagnosis.get("summary") or {}).get("direct_cause"),
        },
        "hard_flag_counts": dict(hard_flag_counts),
        "novelty_summary": novelty_summary,
        "novelty_calibration": novelty_calibration,
        "top_borderline_candidates": borderline,
        "root_causes": root_causes,
        "next_actions": sorted(next_actions, key=lambda row: (int(row.get("priority") or 99), row.get("action_key") or "")),
    }
    return result


def write_quality_not_proven_root_cause_report(artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    payload = build_quality_not_proven_root_cause_report(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "quality_not_proven_root_cause_report.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# Quality Not Proven Root Cause Report",
        "",
        f"- factor_quality_judgment: {(payload.get('current_state') or {}).get('factor_quality_judgment')}",
        f"- approved_count: {(payload.get('current_state') or {}).get('approved_count')}",
        f"- stable_alpha_candidate_count: {(payload.get('current_state') or {}).get('stable_alpha_candidate_count')}",
        f"- A/B recommendation: {(payload.get('current_state') or {}).get('ab_recommendation')}",
        "",
        "## Root causes",
    ]
    for row in payload.get("root_causes") or []:
        lines.append(f"- {row.get('cause_key')}: {row.get('why')} | confidence={row.get('confidence')}")
    lines.append("")
    lines.append("## Next actions")
    for row in payload.get("next_actions") or []:
        lines.append(f"- P{row.get('priority')}: {row.get('action_key')} — {row.get('why')}")
    (artifacts_dir / "quality_not_proven_root_cause_report.md").write_text("\n".join(lines), encoding="utf-8")
    return payload
