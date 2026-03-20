from __future__ import annotations

from typing import Any


def evaluate_opportunity_from_task(task: dict[str, Any], *, status: str, summary: str | None = None, error_text: str | None = None) -> dict[str, Any] | None:
    payload = task.get("payload") or {}
    opportunity_id = payload.get("opportunity_id")
    if not opportunity_id:
        return None

    knowledge_gain = [g for g in (payload.get("knowledge_gain") or payload.get("expected_information_gain") or []) if g]
    summary_text = (summary or "") + " " + (error_text or "")
    has_gain = any(g and g != "no_significant_information_gain" for g in knowledge_gain) or (
        "knowledge_gain=" in summary_text and "no_significant_information_gain" not in summary_text
    )

    evaluation = {
        "opportunity_id": opportunity_id,
        "status": status,
        "evaluation_label": "high_gain" if has_gain else ("failed" if status != "finished" else "low_gain"),
        "has_gain": has_gain,
        "summary": summary,
        "error_text": error_text,
        "task_id": task.get("task_id"),
        "task_type": task.get("task_type"),
        "knowledge_gain": knowledge_gain,
        "next_state": "promoted" if has_gain else ("rejected" if status != "finished" else "evaluated"),
    }
    return evaluation
