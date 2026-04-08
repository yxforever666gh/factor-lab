from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = ROOT / "artifacts"
WINDOW = 12
NO_GAIN_COOLDOWN = 2


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    text = path.read_text(encoding="utf-8")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        try:
            obj, _ = json.JSONDecoder().raw_decode(text)
            return obj
        except Exception:
            return default


def _family_key_from_branch_id(branch_id: str | None) -> str | None:
    text = branch_id or ""
    if text.startswith("fallback_"):
        return None
    for key in [
        "stable_candidate_validation",
        "graveyard_diagnosis",
        "recent_window_validation",
        "window_expansion",
        "exploration",
    ]:
        if key in text:
            return key
    return None


def build_research_learning(memory_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(memory_path) if memory_path else (ARTIFACTS / "research_memory.json")
    memory = _read_json(path, {})
    execution_feedback = list(memory.get("execution_feedback") or [])[-WINDOW:]
    generated_candidate_outcomes = list(memory.get("generated_candidate_outcomes") or [])[-60:]
    candidate_generation_history = list(memory.get("candidate_generation_history") or [])[-120:]
    representative_candidate_reviews = list(memory.get("representative_candidate_reviews") or [])[-40:]

    families: dict[str, dict[str, Any]] = {}
    for row in execution_feedback:
        family = _family_key_from_branch_id(row.get("branch_id"))
        if not family:
            continue
        meta = families.setdefault(family, {
            "family": family,
            "recent_runs": 0,
            "recent_gain": 0,
            "recent_no_gain": 0,
            "recent_high_value_failure": 0,
            "recent_low_value_repeat": 0,
            "consecutive_no_gain": 0,
            "cooldown_active": False,
            "recommended_action": "keep",
        })
        meta["recent_runs"] += 1
        outcome_class = row.get("outcome_class")
        if row.get("has_gain"):
            meta["recent_gain"] += 1
        else:
            meta["recent_no_gain"] += 1
            if outcome_class == "high_value_failure":
                meta["recent_high_value_failure"] += 1
            if outcome_class == "low_value_repeat":
                meta["recent_low_value_repeat"] += 1

    for family, meta in families.items():
        consecutive = 0
        for row in reversed(execution_feedback):
            row_family = _family_key_from_branch_id(row.get("branch_id"))
            if row_family != family:
                continue
            if row.get("has_gain"):
                break
            consecutive += 1
        meta["consecutive_no_gain"] = consecutive
        meta["cooldown_active"] = consecutive >= NO_GAIN_COOLDOWN
        if meta["cooldown_active"]:
            meta["recommended_action"] = "cooldown"
        elif meta["recent_gain"] >= 2 and meta["recent_gain"] >= meta["recent_no_gain"]:
            meta["recommended_action"] = "upweight"
        elif meta["recent_high_value_failure"] >= 1 and meta["recent_low_value_repeat"] == 0:
            meta["recommended_action"] = "keep"
        elif meta["recent_low_value_repeat"] >= max(1, meta["recent_no_gain"] - meta["recent_high_value_failure"]):
            meta["recommended_action"] = "downweight"
        elif meta["recent_no_gain"] > meta["recent_gain"]:
            meta["recommended_action"] = "downweight"
        else:
            meta["recommended_action"] = "keep"

    operator_stats: dict[str, dict[str, Any]] = {}
    for row in candidate_generation_history:
        operator = row.get("operator") or "unknown"
        meta = operator_stats.setdefault(operator, {
            "operator": operator,
            "proposal_count": 0,
            "cheap_screen_pass_count": 0,
            "cheap_screen_fail_count": 0,
            "count": 0,
            "gain_count": 0,
            "high_value_failure_count": 0,
            "low_value_repeat_count": 0,
            "recommended_action": "keep",
        })
        meta["proposal_count"] += 1
        if (row.get("cheap_screen") or {}).get("pass"):
            meta["cheap_screen_pass_count"] += 1
        else:
            meta["cheap_screen_fail_count"] += 1

    for row in generated_candidate_outcomes:
        operator = row.get("operator") or "unknown"
        meta = operator_stats.setdefault(operator, {
            "operator": operator,
            "proposal_count": 0,
            "cheap_screen_pass_count": 0,
            "cheap_screen_fail_count": 0,
            "count": 0,
            "gain_count": 0,
            "high_value_failure_count": 0,
            "low_value_repeat_count": 0,
            "recommended_action": "keep",
        })
        meta["count"] += 1
        if row.get("has_gain"):
            meta["gain_count"] += 1
        if row.get("outcome_class") == "high_value_failure":
            meta["high_value_failure_count"] += 1
        if row.get("outcome_class") == "low_value_repeat":
            meta["low_value_repeat_count"] += 1

    for meta in operator_stats.values():
        total_attempts = int(meta.get("count") or 0)
        total_proposals = int(meta.get("proposal_count") or 0)
        if meta["gain_count"] >= 1:
            meta["recommended_action"] = "upweight"
        elif meta["low_value_repeat_count"] >= 1:
            meta["recommended_action"] = "downweight"
        elif meta["high_value_failure_count"] >= 3 and total_attempts >= 3:
            meta["recommended_action"] = "downweight"
        elif meta["high_value_failure_count"] >= 1 and meta["low_value_repeat_count"] == 0:
            meta["recommended_action"] = "keep"
        elif total_proposals >= 4 and total_attempts == 0:
            meta["recommended_action"] = "downweight"

    representative_candidate_stats = {
        "count": len(representative_candidate_reviews),
        "gain_count": len([row for row in representative_candidate_reviews if row.get("has_gain")]),
        "high_value_failure_count": len([row for row in representative_candidate_reviews if row.get("outcome_class") == "high_value_failure"]),
        "low_value_repeat_count": len([row for row in representative_candidate_reviews if row.get("outcome_class") == "low_value_repeat"]),
        "recommended_action": "keep",
    }
    if representative_candidate_stats["gain_count"] >= 1:
        representative_candidate_stats["recommended_action"] = "upweight"
    elif representative_candidate_stats["low_value_repeat_count"] >= 1:
        representative_candidate_stats["recommended_action"] = "downweight"

    family_operator_stats: dict[str, dict[str, Any]] = {}
    for row in generated_candidate_outcomes:
        family = row.get("target_family") or "generated"
        operator = row.get("operator") or "unknown"
        fam_meta = family_operator_stats.setdefault(family, {})
        meta = fam_meta.setdefault(operator, {
            "family": family,
            "operator": operator,
            "count": 0,
            "gain_count": 0,
            "high_value_failure_count": 0,
            "low_value_repeat_count": 0,
            "recommended_action": "keep",
        })
        meta["count"] += 1
        if row.get("has_gain"):
            meta["gain_count"] += 1
        if row.get("outcome_class") == "high_value_failure":
            meta["high_value_failure_count"] += 1
        if row.get("outcome_class") == "low_value_repeat":
            meta["low_value_repeat_count"] += 1

    for ops in family_operator_stats.values():
        for meta in ops.values():
            total_attempts = int(meta.get("count") or 0)
            if meta["gain_count"] >= 1:
                meta["recommended_action"] = "upweight"
            elif meta["low_value_repeat_count"] >= 1:
                meta["recommended_action"] = "downweight"
            elif meta["high_value_failure_count"] >= 3 and total_attempts >= 3:
                meta["recommended_action"] = "downweight"
            elif meta["high_value_failure_count"] >= 1 and meta["low_value_repeat_count"] == 0:
                meta["recommended_action"] = "keep"

    total_operator_gains = sum(meta.get("gain_count") or 0 for meta in operator_stats.values())
    total_operator_high_value_failures = sum(meta.get("high_value_failure_count") or 0 for meta in operator_stats.values())
    total_operator_low_value_repeats = sum(meta.get("low_value_repeat_count") or 0 for meta in operator_stats.values())
    research_mode = {
        "mode": "balanced",
        "reason": "insufficient_signal",
    }
    if total_operator_low_value_repeats >= 2:
        research_mode = {"mode": "novelty_heavy", "reason": "too_many_low_value_repeats"}
    elif total_operator_high_value_failures >= 2 and total_operator_gains == 0:
        research_mode = {"mode": "diagnosis_heavy", "reason": "high_value_failures_need_structural_followup"}
    elif total_operator_gains >= 2:
        research_mode = {"mode": "generation_heavy", "reason": "multiple_operator_gains_detected"}

    payload = {
        "updated_at_utc": memory.get("updated_at_utc"),
        "families": families,
        "autonomy_profile": memory.get("autonomy_profile") or {},
        "coding_profile": memory.get("coding_profile") or {},
        "candidate_generation_history": candidate_generation_history,
        "operator_stats": operator_stats,
        "family_operator_stats": family_operator_stats,
        "representative_candidate_stats": representative_candidate_stats,
        "research_mode": research_mode,
    }
    (ARTIFACTS / "research_learning.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
