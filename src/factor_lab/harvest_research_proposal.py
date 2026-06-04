from __future__ import annotations

from typing import Any

LEGACY_BROAD_MARKERS = ("generated/", "broad", "force-new", "legacy_agent")


def _contains_forbidden_text(value: Any) -> bool:
    text = str(value).lower()
    return any(marker in text for marker in LEGACY_BROAD_MARKERS)


def validate_harvest_research_proposal(proposal: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    required = [
        ("proposal_id", "missing_proposal_id"),
        ("mechanism_id", "missing_mechanism_id"),
        ("hypothesis", "missing_hypothesis"),
        ("required_fields", "missing_required_fields"),
        ("expected_information_gain", "missing_expected_information_gain"),
        ("falsification_criteria", "missing_falsification_criteria"),
        ("duplicate_rationale", "missing_duplicate_rationale"),
    ]
    for field, reason in required:
        if not proposal.get(field):
            reasons.append(reason)
    if str(proposal.get("experiment_type", "")).lower() in {"live_trading", "paper_trading", "broker_order"}:
        reasons.append("live_trading_requested")
    if proposal.get("live_trading_enabled") or proposal.get("paper_portfolio_promotion"):
        reasons.append("paper_or_live_promotion_requested")
    if _contains_forbidden_text(proposal):
        reasons.append("legacy_broad_path_requested")
    return {"valid": not reasons, "reasons": sorted(set(reasons))}


def proposal_to_experiment_id(proposal: dict[str, Any]) -> str:
    return str(proposal.get("proposal_id") or proposal.get("experiment_id") or "")
