"""CLI-facing helpers for the authoritative monthly research coordinator.

The helpers intentionally accept parsed proposal payloads, never research
frames, negative-control metrics, p-values, or caller-computed statistics.
Argument parsing remains in :mod:`factor_lab.research_os.cli`.
"""

from __future__ import annotations

from typing import Any, Mapping

from .monthly_research import MonthlyResearchCoordinator, ProposalAdmission
from .proposals import HypothesisProposalPort


def _submission_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        "submission_id": value.submission_id,
        "proposal_decision_id": value.proposal_decision_id,
        "family_id": value.family_id,
        "recovery_case_id": value.recovery_case_id,
        "status": value.status,
        "research_equivalence_hash": value.research_equivalence_hash,
        "experiment_fingerprint": value.experiment_fingerprint,
        "trial_id": value.trial_id,
        "lease_owner": value.lease_owner,
        "lease_expires_at": (
            None if value.lease_expires_at is None else value.lease_expires_at.isoformat()
        ),
        "attempts": value.attempts,
        "experiment_id": value.experiment_id,
        "error": value.error,
        "created_at": value.created_at.isoformat(),
        "updated_at": value.updated_at.isoformat(),
    }


def _admission_payload(value: ProposalAdmission) -> dict[str, Any]:
    return {
        "accepted": value.accepted,
        "proposal_decision_id": value.decision.decision_id,
        "proposal_review_accepted": value.decision.accepted,
        "violations": list(value.violations),
        "submission": _submission_payload(value.submission),
    }


def coordinator_propose(
    coordinator: MonthlyResearchCoordinator,
    port: HypothesisProposalPort,
    *,
    family_id: str,
    recovery_case_id: str | None = None,
) -> dict[str, Any]:
    return _admission_payload(
        coordinator.propose(
            port,
            family_id=family_id,
            recovery_case_id=recovery_case_id,
        )
    )


def coordinator_submit(
    coordinator: MonthlyResearchCoordinator,
    proposal: Mapping[str, Any],
    *,
    family_id: str,
    recovery_case_id: str | None = None,
) -> dict[str, Any]:
    return _admission_payload(
        coordinator.submit(
            proposal,
            family_id=family_id,
            recovery_case_id=recovery_case_id,
        )
    )


def coordinator_status(
    coordinator: MonthlyResearchCoordinator, submission_id: str
) -> dict[str, Any]:
    result = _submission_payload(coordinator.status(submission_id))
    assert result is not None
    return result


def coordinator_resume(
    coordinator: MonthlyResearchCoordinator,
    *,
    worker_id: str,
    limit: int = 100,
    lease_seconds: int = 1_800,
) -> dict[str, Any]:
    rows = coordinator.resume(
        worker_id=worker_id,
        limit=limit,
        lease_seconds=lease_seconds,
    )
    return {
        "worker_id": worker_id,
        "count": len(rows),
        "submissions": [
            {
                **(_submission_payload(row.submission) or {}),
                "claimed": row.claimed,
                "shadow_account_id": row.shadow_account_id,
            }
            for row in rows
        ],
    }


__all__ = [
    "coordinator_propose",
    "coordinator_resume",
    "coordinator_status",
    "coordinator_submit",
]
