from __future__ import annotations

from typing import Any


def evaluate_opportunity_from_task(task: dict[str, Any], *, status: str, summary: str | None = None, error_text: str | None = None) -> dict[str, Any] | None:
    payload = task.get("payload") or {}
    opportunity_id = payload.get("opportunity_id")
    if not opportunity_id:
        return None

    knowledge_gain = [g for g in (payload.get("knowledge_gain") or payload.get("expected_information_gain") or []) if g]
    summary_text = (summary or "") + " " + (error_text or "")
    gain_count = len([g for g in knowledge_gain if g and g != "no_significant_information_gain"])
    has_gain = gain_count > 0 or ("knowledge_gain=" in summary_text and "no_significant_information_gain" not in summary_text)

    if status != "finished":
        label = "failed"
        next_state = "rejected"
    elif gain_count >= 2:
        label = "high_gain"
        next_state = "promoted"
    elif gain_count == 1:
        label = "moderate_gain"
        next_state = "evaluated"
    elif "inconclusive" in summary_text.lower():
        label = "inconclusive"
        next_state = "evaluated"
    else:
        label = "low_gain"
        next_state = "evaluated"

    evaluation = {
        "opportunity_id": opportunity_id,
        "status": status,
        "evaluation_label": label,
        "has_gain": has_gain,
        "gain_count": gain_count,
        "summary": summary,
        "error_text": error_text,
        "task_id": task.get("task_id"),
        "task_type": task.get("task_type"),
        "knowledge_gain": knowledge_gain,
        "next_state": next_state,
    }
    return evaluation
