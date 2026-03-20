from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
STORE_PATH = ARTIFACTS / "research_opportunity_store.json"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def build_child_opportunities(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    store = _read_json(STORE_PATH, {"opportunities": {}})
    items = list((store.get("opportunities") or {}).values())
    children: list[dict[str, Any]] = []

    for row in items:
        oid = row.get("opportunity_id")
        state = row.get("state")
        otype = row.get("opportunity_type")
        target_family = row.get("target_family")
        target_candidates = list(row.get("target_candidates") or [])
        if not oid:
            continue
        evaluation = row.get("evaluation") or {}
        branchworthy = state in {"promoted", "evaluated"} or (evaluation.get("evaluation_label") in {"high_gain", "moderate_gain"})
        if not branchworthy:
            continue

        if otype == "confirm":
            children.append({
                "question_id": f"child-expand-from-{oid}",
                "question_type": "expand",
                "question": f"{oid} 已确认，下一步应扩展哪种验证维度？",
                "hypothesis": "confirm 类型机会被验证后，应自然分裂出 expand 类型子机会。",
                "target_family": target_family,
                "target_candidates": target_candidates[:2],
                "expected_knowledge_gain": ["window_stability_check"],
                "evidence_gap": "已确认信号存在，但缺少顺势扩展动作。",
                "sources": ["opportunity_brancher", oid],
                "parent_opportunity_id": oid,
            })

        if otype == "diagnose":
            children.append({
                "question_id": f"child-recombine-from-{oid}",
                "question_type": "recombine",
                "question": f"{oid} 已提供失败解释，是否可据此构造新的重组型机会？",
                "hypothesis": "diagnose 类型机会在给出失败解释后，应该派生出更高层的重组或替代方向。",
                "target_family": target_family,
                "target_candidates": target_candidates[:2],
                "expected_knowledge_gain": ["exploration_candidate_survived"],
                "evidence_gap": "已有失败解释，但尚未把它转成新方向。",
                "sources": ["opportunity_brancher", oid],
                "parent_opportunity_id": oid,
            })

        if oid.startswith("opp-q-recovery-to-opportunity"):
            children.append({
                "question_id": f"child-probe-from-{oid}",
                "question_type": "probe",
                "question": f"{oid} 作为 recovery 转机会节点，下一步是否值得小成本试探新分支？",
                "hypothesis": "recovery 桥接机会应该至少派生出一个低成本 probe 子机会。",
                "target_family": target_family,
                "target_candidates": target_candidates[:2],
                "expected_knowledge_gain": ["exploration_candidate_survived"],
                "evidence_gap": "recovery 已被转成机会，但还没有真正试探式子机会。",
                "sources": ["opportunity_brancher", oid],
                "parent_opportunity_id": oid,
            })

    return children
