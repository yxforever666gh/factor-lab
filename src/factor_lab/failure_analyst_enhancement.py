from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.agent_schemas import FAILURE_ANALYST_ENHANCEMENT_SCHEMA_VERSION


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cluster_rows(candidate_failure_dossiers: list[dict[str, Any]], representative_failure_dossiers: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows = list(candidate_failure_dossiers or []) + [
        {"candidate_name": name, **(row or {})}
        for name, row in (representative_failure_dossiers or {}).items()
        if name
    ]
    failure_mode_counts: Counter[str] = Counter()
    action_counts: Counter[str] = Counter()
    regime_counts: Counter[str] = Counter()
    parent_delta_counts: Counter[str] = Counter()
    for row in rows:
        for mode in (row.get("failure_modes") or []):
            if mode:
                failure_mode_counts[mode] += 1
        action_counts[row.get("recommended_action") or "unknown"] += 1
        regime_counts[row.get("regime_dependency") or "unknown"] += 1
        parent_delta_counts[row.get("parent_delta_status") or "unknown"] += 1
    summary = {
        "candidate_count": len(rows),
        "failure_mode_counts": dict(failure_mode_counts),
        "recommended_action_counts": dict(action_counts),
        "regime_dependency_counts": dict(regime_counts),
        "parent_delta_counts": dict(parent_delta_counts),
    }
    top_patterns = [
        {"pattern": key, "count": count}
        for key, count in failure_mode_counts.most_common(8)
    ]
    return rows, {**summary, "top_patterns": top_patterns}


def _build_reroute_proposals(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    proposals: list[dict[str, Any]] = []
    for row in rows:
        name = row.get("candidate_name")
        if not name:
            continue
        recommended_action = row.get("recommended_action") or "keep_validating"
        regime_dependency = row.get("regime_dependency") or "unknown"
        parent_delta_status = row.get("parent_delta_status") or "unknown"
        failure_modes = set(row.get("failure_modes") or [])

        to_route = "stable_candidate_validation"
        rationale = "优先继续验证当前候选。"
        priority = 50
        if parent_delta_status == "non_incremental":
            to_route = "new_mechanism_exploration"
            rationale = "相对父因子没有新增信息，应转向更远的新机制搜索。"
            priority = 92
        elif regime_dependency in {"short_window_only", "medium_to_long_decay"} or {"short_to_medium_decay", "medium_to_long_decay"} & failure_modes:
            to_route = "medium_horizon_validation"
            rationale = "短窗有效但中长窗衰减，应先做 persistence / medium-horizon 验证。"
            priority = 88
        elif regime_dependency in {"exposure_dependent", "neutralization_break"} or "neutralized_break" in failure_modes:
            to_route = "neutralization_diagnosis"
            rationale = "疑似依赖暴露或 neutralized 后崩解，应先转向中性化诊断。"
            priority = 90
        elif recommended_action == "suppress":
            to_route = "graveyard_diagnosis"
            rationale = "当前建议 suppress，应转向失败解释而不是继续扩散。"
            priority = 84
        elif recommended_action == "diagnose":
            to_route = "graveyard_diagnosis"
            rationale = "当前更适合先解释失败模式，而不是继续生成近邻候选。"
            priority = 80
        proposals.append(
            {
                "candidate_name": name,
                "from_route": "current_frontier",
                "to_route": to_route,
                "priority": priority,
                "recommended_action": recommended_action,
                "rationale": rationale,
            }
        )
    proposals.sort(key=lambda row: (-int(row.get("priority") or 0), row.get("candidate_name") or ""))
    return proposals[:20]


def _build_question_cards_v2(question_cards: list[dict[str, Any]], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    row_map = {row.get("candidate_name"): row for row in rows if row.get("candidate_name")}
    out: list[dict[str, Any]] = []
    source_cards = list(question_cards or [])
    if not source_cards:
        for row in rows:
            candidate_name = row.get("candidate_name")
            if not candidate_name:
                continue
            recommended_action = row.get("recommended_action") or "keep_validating"
            if recommended_action not in {"diagnose", "suppress", "keep_validating"}:
                continue
            source_cards.append(
                {
                    "card_id": f"question_v2::{candidate_name}::{recommended_action}",
                    "candidate_name": candidate_name,
                    "question_type": "failure_analyst_bootstrap",
                    "prompt": f"围绕 {candidate_name} 的失败模式生成更远离近邻的新机制探索题。",
                    "route_bias": "far_family_incremental",
                    "expected_information_gain": ["new_branch_opened", "search_space_expanded", "candidate_survival_check"],
                    "target_pool": "new_mechanism_exploration",
                    "priority": 82 if recommended_action in {"diagnose", "suppress"} else 72,
                    "preferred_context_mode": "far_family",
                    "allowed_operators": ["combine_sub", "combine_ratio", "combine_mul", "combine_avg", "combine_primary_bias"],
                }
            )
    for card in source_cards:
        candidate_name = card.get("candidate_name")
        row = row_map.get(candidate_name) or {}
        recommended_action = row.get("recommended_action") or "keep_validating"
        parent_delta_status = row.get("parent_delta_status") or "unknown"
        regime_dependency = row.get("regime_dependency") or "unknown"
        bonus = 0
        if parent_delta_status == "non_incremental":
            bonus += 8
        if regime_dependency in {"short_window_only", "exposure_dependent"}:
            bonus += 4
        if recommended_action in {"diagnose", "suppress"}:
            bonus += 4
        out.append(
            {
                **card,
                "schema_version": FAILURE_ANALYST_ENHANCEMENT_SCHEMA_VERSION,
                "priority": int(card.get("priority") or 50) + bonus,
                "recommended_action": recommended_action,
                "parent_delta_status": parent_delta_status,
                "regime_dependency": regime_dependency,
                "failure_modes": row.get("failure_modes") or [],
                "analyst_note": row.get("summary") or row.get("rationale") or row.get("likely_cause") or "failure analyst enhanced question",
            }
        )
    out.sort(key=lambda row: (-int(row.get("priority") or 0), row.get("candidate_name") or "", row.get("card_id") or ""))
    return out


def _build_stop_continue(rows: list[dict[str, Any]], reroute_proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reroute_map = {row.get("candidate_name"): row for row in reroute_proposals if row.get("candidate_name")}
    out: list[dict[str, Any]] = []
    for row in rows:
        name = row.get("candidate_name")
        if not name:
            continue
        recommended_action = row.get("recommended_action") or "keep_validating"
        if recommended_action == "suppress":
            recommendation = "stop"
            confidence = 0.87
        elif recommended_action == "diagnose":
            recommendation = "reroute"
            confidence = 0.76
        else:
            recommendation = "continue"
            confidence = 0.62 if reroute_map.get(name) else 0.58
        out.append(
            {
                "candidate_name": name,
                "recommendation": recommendation,
                "confidence": round(confidence, 4),
                "reroute_target": (reroute_map.get(name) or {}).get("to_route"),
                "reasoning_summary": (reroute_map.get(name) or {}).get("rationale") or "沿当前验证主线继续推进。",
            }
        )
    out.sort(key=lambda row: (-float(row.get("confidence") or 0.0), row.get("candidate_name") or ""))
    return out[:20]


def build_failure_analyst_enhancement(snapshot: dict[str, Any]) -> dict[str, Any]:
    candidate_failure_dossiers = list(snapshot.get("candidate_failure_dossiers") or [])
    representative_failure_dossiers = dict(snapshot.get("representative_failure_dossiers") or {})
    failure_question_cards = list(snapshot.get("failure_question_cards") or [])
    rows, cluster_summary = _cluster_rows(candidate_failure_dossiers, representative_failure_dossiers)
    reroute_proposals = _build_reroute_proposals(rows)
    question_cards_v2 = _build_question_cards_v2(failure_question_cards, rows)
    stop_continue = _build_stop_continue(rows, reroute_proposals)
    summary = {
        "schema_version": FAILURE_ANALYST_ENHANCEMENT_SCHEMA_VERSION,
        "cluster_candidate_count": cluster_summary.get("candidate_count") or 0,
        "reroute_proposal_count": len(reroute_proposals),
        "question_card_count": len(question_cards_v2),
        "stop_count": len([row for row in stop_continue if row.get("recommendation") == "stop"]),
        "continue_count": len([row for row in stop_continue if row.get("recommendation") == "continue"]),
        "reroute_count": len([row for row in stop_continue if row.get("recommendation") == "reroute"]),
        "top_patterns": cluster_summary.get("top_patterns") or [],
    }
    return {
        "generated_at_utc": _iso_now(),
        "schema_version": FAILURE_ANALYST_ENHANCEMENT_SCHEMA_VERSION,
        "failure_cluster_summary": cluster_summary,
        "reroute_proposals": reroute_proposals,
        "question_cards_v2": question_cards_v2,
        "stop_or_continue_recommendation": stop_continue,
        "summary": summary,
    }


def write_failure_analyst_enhancement(snapshot: dict[str, Any], artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    payload = build_failure_analyst_enhancement(snapshot)
    (artifacts_dir / "failure_analyst_enhancement.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (artifacts_dir / "failure_question_cards_v2.json").write_text(json.dumps(payload.get("question_cards_v2") or [], ensure_ascii=False, indent=2), encoding="utf-8")
    (artifacts_dir / "failure_reroute_proposals.json").write_text(json.dumps(payload.get("reroute_proposals") or [], ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def load_failure_analyst_enhancement(artifacts_dir: str | Path) -> dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    return _load_json(
        artifacts_dir / "failure_analyst_enhancement.json",
        {
            "failure_cluster_summary": {},
            "reroute_proposals": [],
            "question_cards_v2": [],
            "stop_or_continue_recommendation": [],
            "summary": {},
        },
    )
