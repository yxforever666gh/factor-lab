from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = ROOT / "artifacts" / "candidate_triage_model.json"
MEMORY_PATH = ROOT / "artifacts" / "research_memory.json"

DEFAULT_MODEL = {
    "version": 1,
    "name": "candidate-triage-v1",
    "prior_success_rate": 0.42,
    "source_success": {},
    "operator_success": {},
    "family_success": {},
    "minimum_samples": 3,
}

_POSITIVE_OUTCOMES = {"high_value_success", "useful_success"}
_NEUTRAL_OUTCOMES = {"high_value_failure"}


def _read_json(path: str | Path, default: Any) -> Any:
    p = Path(path)
    if not p.exists():
        return default
    return json.loads(p.read_text(encoding="utf-8"))


def _rate(counts: dict[str, int], *, prior: float) -> float:
    total = int(counts.get("total") or 0)
    if total <= 0:
        return prior
    positive = float(counts.get("positive") or 0)
    neutral = float(counts.get("neutral") or 0)
    # Neutral outcomes still carry some research value but should not dominate.
    return max(0.0, min(1.0, (positive + neutral * 0.35) / total))


def build_candidate_triage_model(memory_payload: dict[str, Any]) -> dict[str, Any]:
    outcomes = list(memory_payload.get("generated_candidate_outcomes") or [])[-400:]
    prior_positive = 0
    prior_neutral = 0
    prior_total = 0
    source_counts: dict[str, dict[str, int]] = {}
    operator_counts: dict[str, dict[str, int]] = {}
    family_counts: dict[str, dict[str, int]] = {}

    for row in outcomes:
        outcome = row.get("outcome_class") or ""
        prior_total += 1
        if outcome in _POSITIVE_OUTCOMES:
            prior_positive += 1
        elif outcome in _NEUTRAL_OUTCOMES:
            prior_neutral += 1

        def bump(bucket: dict[str, dict[str, int]], key: str | None) -> None:
            if not key:
                return
            slot = bucket.setdefault(str(key), {"positive": 0, "neutral": 0, "total": 0})
            slot["total"] += 1
            if outcome in _POSITIVE_OUTCOMES:
                slot["positive"] += 1
            elif outcome in _NEUTRAL_OUTCOMES:
                slot["neutral"] += 1

        bump(source_counts, row.get("source"))
        bump(operator_counts, row.get("operator"))
        bump(family_counts, row.get("target_family"))

    if prior_total:
        prior = max(0.0, min(1.0, (prior_positive + prior_neutral * 0.35) / prior_total))
    else:
        prior = float(DEFAULT_MODEL["prior_success_rate"])

    def to_payload(counts: dict[str, dict[str, int]]) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, row in counts.items():
            out[key] = {
                **row,
                "success_rate": round(_rate(row, prior=prior), 6),
            }
        return out

    return {
        "version": 1,
        "name": "candidate-triage-v1",
        "generated_from_outcomes": prior_total,
        "prior_success_rate": round(prior, 6),
        "source_success": to_payload(source_counts),
        "operator_success": to_payload(operator_counts),
        "family_success": to_payload(family_counts),
        "minimum_samples": 3,
    }


def load_candidate_triage_model(model_path: str | Path = MODEL_PATH, memory_path: str | Path = MEMORY_PATH) -> dict[str, Any]:
    model = _read_json(model_path, None)
    if model:
        return model
    memory_payload = _read_json(memory_path, {}) or {}
    if memory_payload:
        return build_candidate_triage_model(memory_payload)
    return dict(DEFAULT_MODEL)


def _lookup_success(model: dict[str, Any], bucket: str, key: str | None) -> tuple[float | None, int]:
    if not key:
        return None, 0
    row = ((model.get(bucket) or {}).get(str(key)) or {})
    if not row:
        return None, 0
    return row.get("success_rate"), int(row.get("total") or 0)


def score_generation_proposal(
    proposal: dict[str, Any],
    *,
    snapshot: dict[str, Any],
    factor_context: dict[str, dict[str, Any]],
    model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    model = model or load_candidate_triage_model()
    prior = float(model.get("prior_success_rate") or DEFAULT_MODEL["prior_success_rate"])
    cheap_screen = float(((proposal.get("cheap_screen") or {}).get("score") or 0.5))
    base_factors = list(proposal.get("base_factors") or [])
    families = [((factor_context.get(name) or {}).get("family")) for name in base_factors]
    families = [name for name in families if name]
    cross_family = len(set(families)) >= 2
    relationship_pressure = sum(int((factor_context.get(name) or {}).get("relationship_count") or 0) for name in base_factors)
    frontier_focus = set((snapshot.get("frontier_focus") or {}).get("preferred_candidates") or [])
    frontier_overlap = len([name for name in base_factors if name in frontier_focus])

    score = prior
    breakdown: dict[str, float] = {"prior": round(prior, 6)}

    cheap_delta = (cheap_screen - 0.5) * 0.55
    score += cheap_delta
    breakdown["cheap_screen"] = round(cheap_delta, 6)

    if cross_family:
        score += 0.08
        breakdown["cross_family_bonus"] = 0.08
    if proposal.get("source") == "hypothesis_template":
        score += 0.07
        breakdown["hypothesis_template_bonus"] = 0.07
    elif proposal.get("source") == "family_gap":
        score += 0.05
        breakdown["family_gap_bonus"] = 0.05
    elif proposal.get("source") == "stable_plus_graveyard":
        score += 0.03
        breakdown["stable_graveyard_bonus"] = 0.03

    expected_gain = set(proposal.get("expected_information_gain") or [])
    if expected_gain & {"new_branch_opened", "boundary_confirmed", "candidate_survival_check"}:
        score += 0.04
        breakdown["expected_gain_bonus"] = 0.04

    if relationship_pressure >= 16:
        score -= 0.08
        breakdown["relationship_pressure_penalty"] = -0.08
    elif relationship_pressure >= 10:
        score -= 0.04
        breakdown["relationship_pressure_penalty"] = -0.04

    if frontier_overlap >= 2:
        score -= 0.04
        breakdown["frontier_overlap_penalty"] = -0.04

    min_samples = int(model.get("minimum_samples") or 3)
    for bucket, key_name, proposal_key in [
        ("source_success", "source", proposal.get("source")),
        ("operator_success", "operator", proposal.get("operator")),
        ("family_success", "target_family", proposal.get("target_family")),
    ]:
        success_rate, sample_count = _lookup_success(model, bucket, proposal_key)
        if success_rate is None or sample_count < min_samples:
            continue
        centered = (float(success_rate) - prior) * 0.45
        score += centered
        breakdown[f"{key_name}_empirical"] = round(centered, 6)
        breakdown[f"{key_name}_samples"] = float(sample_count)

    score = max(0.01, min(0.99, score))
    confidence = min(0.95, 0.45 + 0.08 * len([k for k in breakdown if k.endswith("_empirical")]))
    if score >= 0.67:
        label = "high"
    elif score >= 0.48:
        label = "medium"
    else:
        label = "low"

    return {
        "score": round(score, 6),
        "label": label,
        "confidence": round(confidence, 6),
        "breakdown": breakdown,
    }
