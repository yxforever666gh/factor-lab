from __future__ import annotations

from typing import Any


def should_bypass_recent_fingerprint(opportunity: dict[str, Any]) -> dict[str, Any]:
    priority = float(opportunity.get("priority") or 0.0)
    novelty = float(opportunity.get("novelty_score") or 0.0)
    confidence = float(opportunity.get("confidence") or 0.0)
    otype = opportunity.get("opportunity_type") or "unknown"

    allow = False
    reason = None
    if otype in {"confirm", "diagnose"} and priority >= 0.88 and confidence >= 0.6:
        allow = True
        reason = "high_priority_validation_override"
    elif novelty >= 0.7 and priority >= 0.75:
        allow = True
        reason = "high_novelty_override"

    return {
        "allow_bypass": allow,
        "reason": reason,
    }
