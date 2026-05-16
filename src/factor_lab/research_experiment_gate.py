from __future__ import annotations

from typing import Any, Iterable

from factor_lab.research_experiment_proposal import ResearchExperimentProposal
from factor_lab.research_runtime_state import recently_finished_same_fingerprint
from factor_lab.storage import ExperimentStore


HIGH_VALUE_GAIN_MARKERS = {
    "mechanism_validation",
    "window_stability_check",
    "candidate_survival_check",
    "new_branch_opened",
    "boundary_confirmed",
    "neutralization_diagnosis_requested",
}


def _score_information_gain(markers: Iterable[str], *, mechanism_id: str | None, falsification_criteria: list[str]) -> float:
    marker_set = {str(marker) for marker in markers if marker}
    score = 0.15
    score += min(0.45, 0.15 * len(marker_set & HIGH_VALUE_GAIN_MARKERS))
    if mechanism_id:
        score += 0.2
    if falsification_criteria:
        score += 0.2
    return round(min(1.0, score), 4)


def _mechanical_expression_only(proposal: ResearchExperimentProposal) -> bool:
    if proposal.mechanism_id or proposal.novelty_claim:
        return False
    joined = " ".join(proposal.expressions).lower()
    arithmetic_markers = ["+", "-", "*", "/"]
    return any(marker in joined for marker in arithmetic_markers)


def evaluate_research_experiment_gate(
    proposal: ResearchExperimentProposal,
    *,
    available_fields: set[str] | frozenset[str],
    store: ExperimentStore | None = None,
    fingerprint: str | None = None,
    budget_available: bool = True,
) -> dict[str, Any]:
    reasons: list[str] = []
    decision = "allow"

    validation_errors = proposal.validation_errors()
    if validation_errors:
        reasons.extend(validation_errors)
        if proposal.experiment_type != "generated_candidate":
            decision = "manual_review"

    missing_fields = sorted(set(proposal.required_data_fields) - set(available_fields))
    if missing_fields:
        reasons.append(f"missing required data fields: {', '.join(missing_fields)}")
        decision = "block"

    if store is not None and fingerprint and recently_finished_same_fingerprint(
        store,
        fingerprint,
        task_type=proposal.experiment_type,
        payload=proposal.to_dict(),
        worker_note=proposal.budget_bucket,
    ):
        reasons.append("equivalent experiment already finished within governance window")
        decision = "block"

    if not proposal.falsification_criteria:
        reasons.append("missing falsification criteria")
        if decision == "allow":
            decision = "manual_review"

    if proposal.experiment_type == "generated_candidate" and _mechanical_expression_only(proposal):
        reasons.append("generated candidate lacks mechanism_id or novelty_claim")
        if decision == "allow":
            decision = "cheap_screen_only"

    if not budget_available:
        reasons.append(f"budget bucket exhausted: {proposal.budget_bucket}")
        if decision == "allow":
            decision = "manual_review"

    return {
        "decision": decision,
        "reasons": reasons,
        "budget_bucket": proposal.budget_bucket,
        "expected_information_gain_score": _score_information_gain(
            proposal.expected_information_gain,
            mechanism_id=proposal.mechanism_id,
            falsification_criteria=proposal.falsification_criteria,
        ),
        "proposal_id": proposal.proposal_id,
    }
