from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.failure_question_generator import build_failure_question_cards

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



def _avg(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)



def _recommend_representative_next_question(meta: dict[str, Any]) -> str:
    if int(meta.get("parent_delta_failures") or 0) >= 1:
        return "verify_incremental_value_vs_parent"
    if int(meta.get("decay_45_to_60") or 0) >= 1:
        return "verify_medium_horizon_decay"
    if int(meta.get("decay_45_to_90") or 0) >= 1:
        return "verify_longer_horizon_decay"
    if int(meta.get("neutralized_break_count") or 0) >= 1:
        return "verify_post_neutralization_signal"
    if int(meta.get("gain_count") or 0) >= 1:
        return "promote_representative_validation"
    return "keep_monitoring"



def _build_representative_failure_dossiers(reviews: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    dossiers: dict[str, dict[str, Any]] = {}
    for row in reviews:
        candidate_name = row.get("candidate_name")
        if not candidate_name:
            continue
        meta = dossiers.setdefault(
            candidate_name,
            {
                "candidate_name": candidate_name,
                "review_count": 0,
                "gain_count": 0,
                "high_value_failure_count": 0,
                "low_value_repeat_count": 0,
                "decay_45_to_60": 0,
                "decay_45_to_90": 0,
                "neutralized_break_count": 0,
                "parent_delta_failures": 0,
                "current_frontier_status": None,
                "latest_stage": None,
                "latest_summary": None,
                "recommended_action": "keep_validating",
                "recommended_next_question": "keep_monitoring",
                "regime_dependency": "unclear",
                "parent_delta_status": "unknown",
                "recent_failures": [],
                "regime_dependency_counts": {},
                "failure_mode_counts": {},
                "source_stage_counts": {},
                "retention_values": [],
                "raw_values": [],
                "neutralized_values": [],
            },
        )
        meta["review_count"] += 1
        if row.get("has_gain"):
            meta["gain_count"] += 1
        if row.get("outcome_class") == "high_value_failure":
            meta["high_value_failure_count"] += 1
        if row.get("outcome_class") == "low_value_repeat":
            meta["low_value_repeat_count"] += 1
        source_stage = row.get("source_stage") or "unknown"
        meta["source_stage_counts"][source_stage] = int(meta["source_stage_counts"].get(source_stage) or 0) + 1
        current_frontier_status = row.get("quality_classification") or row.get("candidate_status")
        if current_frontier_status:
            meta["current_frontier_status"] = current_frontier_status
        if row.get("summary"):
            meta["latest_summary"] = row.get("summary")
        meta["latest_stage"] = source_stage
        regime_dependency = row.get("regime_dependency") or "unclear"
        meta["regime_dependency_counts"][regime_dependency] = int(meta["regime_dependency_counts"].get(regime_dependency) or 0) + 1
        meta["regime_dependency"] = max(meta["regime_dependency_counts"].items(), key=lambda item: (item[1], item[0]))[0]
        for failure_mode in (row.get("failure_modes") or []):
            meta["failure_mode_counts"][failure_mode] = int(meta["failure_mode_counts"].get(failure_mode) or 0) + 1
            if failure_mode == "short_to_medium_decay":
                if "90" in source_stage:
                    meta["decay_45_to_90"] += 1
                else:
                    meta["decay_45_to_60"] += 1
            elif failure_mode == "neutralized_break":
                meta["neutralized_break_count"] += 1
        if row.get("parent_delta_status") == "non_incremental":
            meta["parent_delta_failures"] += 1
            meta["parent_delta_status"] = "non_incremental"
        elif meta.get("parent_delta_status") == "unknown" and row.get("parent_delta_status"):
            meta["parent_delta_status"] = row.get("parent_delta_status")
        if row.get("retention_industry") is not None:
            meta["retention_values"].append(float(row.get("retention_industry") or 0.0))
        if row.get("raw_rank_ic_mean") is not None:
            meta["raw_values"].append(float(row.get("raw_rank_ic_mean") or 0.0))
        if row.get("neutralized_rank_ic_mean") is not None:
            meta["neutralized_values"].append(float(row.get("neutralized_rank_ic_mean") or 0.0))
        if row.get("error_text") or row.get("summary"):
            meta["recent_failures"].append(
                {
                    "updated_at_utc": row.get("updated_at_utc"),
                    "source_stage": source_stage,
                    "summary": row.get("summary"),
                    "error_text": row.get("error_text"),
                    "failure_modes": list(row.get("failure_modes") or []),
                }
            )

    for meta in dossiers.values():
        retention_values = [float(value) for value in meta.pop("retention_values", [])]
        raw_values = [float(value) for value in meta.pop("raw_values", [])]
        neutralized_values = [float(value) for value in meta.pop("neutralized_values", [])]
        meta["raw_to_neutralized_retention"] = {
            "avg": _avg(retention_values),
            "last": retention_values[-1] if retention_values else None,
        }
        meta["latest_raw_rank_ic_mean"] = raw_values[-1] if raw_values else None
        meta["latest_neutralized_rank_ic_mean"] = neutralized_values[-1] if neutralized_values else None
        meta["avg_raw_rank_ic_mean"] = _avg(raw_values)
        meta["avg_neutralized_rank_ic_mean"] = _avg(neutralized_values)
        meta["recent_failures"] = meta.get("recent_failures", [])[-5:]
        if meta["parent_delta_failures"] >= 1 and meta["gain_count"] == 0:
            meta["recommended_action"] = "suppress"
        elif meta["decay_45_to_60"] >= 1 or meta["decay_45_to_90"] >= 1 or meta["neutralized_break_count"] >= 1:
            meta["recommended_action"] = "diagnose"
        elif meta["gain_count"] >= 1:
            meta["recommended_action"] = "promote_validation"
        else:
            meta["recommended_action"] = "keep_validating"
        meta["recommended_next_question"] = _recommend_representative_next_question(meta)

    return dossiers


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

    representative_failure_dossiers = _build_representative_failure_dossiers(representative_candidate_reviews)
    failure_question_cards = build_failure_question_cards(representative_failure_dossiers)

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
        "representative_failure_dossiers": representative_failure_dossiers,
        "failure_question_cards": failure_question_cards,
        "research_mode": research_mode,
    }
    (ARTIFACTS / "research_learning.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
