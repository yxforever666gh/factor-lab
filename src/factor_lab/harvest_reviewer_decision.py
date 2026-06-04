from __future__ import annotations

from typing import Any

VALID_REVIEWER_DECISIONS = {"allow", "cheap_screen_only", "manual_review", "block"}
MANUAL_REVIEW_CHANGES = {"external_data_source", "paper_portfolio_promotion", "enable_timer", "increase_budget", "live_trading"}


def normalize_reviewer_decision(decision: dict[str, Any] | None) -> dict[str, Any]:
    raw = dict(decision or {})
    raw.setdefault("decision", "manual_review")
    raw.setdefault("reasons", [])
    raw.setdefault("required_changes", [])
    raw.setdefault("overfit_risk", "unknown")
    if raw["decision"] == "allow" and raw.get("overfit_risk") == "high":
        raw["decision"] = "cheap_screen_only"
    if raw["decision"] == "allow" and set(raw.get("required_changes") or []) & MANUAL_REVIEW_CHANGES:
        raw["decision"] = "manual_review"
    raw["manual_review_required"] = raw["decision"] == "manual_review"
    return raw


def validate_reviewer_decision(decision: dict[str, Any] | None) -> dict[str, Any]:
    normalized = normalize_reviewer_decision(decision)
    reasons: list[str] = []
    if normalized.get("decision") not in VALID_REVIEWER_DECISIONS:
        reasons.append("invalid_reviewer_decision")
    return {"valid": not reasons, "reasons": reasons, "decision": normalized}
