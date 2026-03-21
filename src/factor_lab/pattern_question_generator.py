from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"


def _pattern_entries(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    inline = (snapshot.get("research_learning") or {}).get("patterns") or {}
    if inline:
        return inline
    learning_path = ARTIFACTS / "opportunity_learning.json"
    if learning_path.exists():
        payload = json.loads(learning_path.read_text(encoding="utf-8"))
        return (payload.get("patterns") or {})
    return {}


def _parse_pattern_signature(signature: str) -> dict[str, str]:
    parts = str(signature).split("::")
    while len(parts) < 6:
        parts.append("unknown")
    return {
        "question_type": parts[0],
        "family": parts[1],
        "parent_kind": parts[2],
        "target_shape": parts[3],
        "intent_signature": parts[4],
        "epistemic_outcome": parts[5],
    }


def _targets_for_shape(snapshot: dict[str, Any], family: str, target_shape: str) -> list[str]:
    stable = [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")]
    graveyard = list(snapshot.get("latest_graveyard") or [])
    top_scores = [row.get("factor_name") for row in (snapshot.get("top_scores") or [])[:6] if row.get("factor_name")]

    if family == "graveyard_diagnosis":
        pool = graveyard or top_scores or stable
    elif family == "stable_candidate_validation":
        pool = stable or top_scores or graveyard
    else:
        pool = stable or top_scores or graveyard

    if target_shape == "single_target":
        return pool[:1]
    if target_shape == "pair_target":
        return pool[:2]
    if target_shape == "multi_target":
        return pool[:4]
    return []


def _expected_gain_from_intent(intent_signature: str) -> list[str]:
    if not intent_signature or intent_signature == "no_expected_gain":
        return []
    return [part for part in intent_signature.split("+") if part]


def build_pattern_native_questions(snapshot: dict[str, Any], limit: int = 4) -> list[dict[str, Any]]:
    patterns = _pattern_entries(snapshot)
    ranked = sorted(
        [
            (key, meta)
            for key, meta in patterns.items()
            if (meta.get("recommended_action") == "upweight") and float(meta.get("epistemic_value_score") or 0.0) > 0
        ],
        key=lambda item: (-float((item[1] or {}).get("epistemic_value_score") or 0.0), item[0]),
    )

    questions: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for signature, meta in ranked[:limit]:
        parsed = _parse_pattern_signature(signature)
        qtype = parsed["question_type"]
        family = None if parsed["family"] in {"none", "unknown"} else parsed["family"]
        targets = _targets_for_shape(snapshot, parsed["family"], parsed["target_shape"])
        expected_gain = _expected_gain_from_intent(parsed["intent_signature"])
        question_id = f"pattern-native-{signature.replace('::', '-').replace('+', '-') }"
        if question_id in seen_ids:
            continue
        seen_ids.add(question_id)

        question = f"模式 {parsed['question_type']} / {parsed['family']} / {parsed['epistemic_outcome']} 是否值得继续作为研究主线？"
        hypothesis = (
            f"历史上呈现 {parsed['epistemic_outcome']} 的 {parsed['question_type']} 模式，"
            f"对 {parsed['family']} family 具有持续的认知价值。"
        )
        evidence_gap = (
            f"pattern learning 已将该模式标为高价值（score={meta.get('epistemic_value_score')}），"
            "但当前问题空间尚未显式把它作为一等研究入口。"
        )
        questions.append({
            "question_id": question_id,
            "question_type": qtype,
            "question": question,
            "hypothesis": hypothesis,
            "target_family": family,
            "target_candidates": targets,
            "expected_knowledge_gain": expected_gain,
            "evidence_gap": evidence_gap,
            "sources": ["pattern_learning", signature],
            "origin": "pattern_question_generator",
            "pattern_signature": signature,
        })

    return questions
