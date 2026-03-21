from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _evaluation_from_generated_batch(task: dict[str, Any], payload: dict[str, Any]) -> tuple[str, str, list[str], dict[str, Any]]:
    output_dir = Path(payload.get("output_dir") or "")
    batch_summary = _read_json(output_dir / "batch_summary.json")
    batch_comparison = _read_json(output_dir / "batch_comparison.json")

    jobs = batch_summary if isinstance(batch_summary, list) else []
    total_candidates = sum(int(row.get("candidate_count") or 0) for row in jobs)
    total_graveyard = sum(int(row.get("graveyard_count") or 0) for row in jobs)
    candidate_presence = batch_comparison.get("candidate_presence") or {}
    graveyard_presence = batch_comparison.get("graveyard_presence") or {}

    knowledge_gain: list[str] = []
    if total_candidates > 0:
        knowledge_gain.append("exploration_candidate_survived")
    if total_graveyard > 0:
        knowledge_gain.append("exploration_graveyard_identified")

    if total_candidates > 0 and total_graveyard == 0:
        return "high_gain", "promoted", knowledge_gain, {
            "total_candidates": total_candidates,
            "total_graveyard": total_graveyard,
            "candidate_presence": candidate_presence,
            "graveyard_presence": graveyard_presence,
        }
    if total_candidates > 0:
        return "moderate_gain", "evaluated", knowledge_gain, {
            "total_candidates": total_candidates,
            "total_graveyard": total_graveyard,
            "candidate_presence": candidate_presence,
            "graveyard_presence": graveyard_presence,
        }
    if total_graveyard > 0:
        return "graveyard_only", "evaluated", knowledge_gain, {
            "total_candidates": total_candidates,
            "total_graveyard": total_graveyard,
            "candidate_presence": candidate_presence,
            "graveyard_presence": graveyard_presence,
        }
    return "low_gain", "evaluated", [], {
        "total_candidates": total_candidates,
        "total_graveyard": total_graveyard,
        "candidate_presence": candidate_presence,
        "graveyard_presence": graveyard_presence,
    }


def evaluate_opportunity_from_task(task: dict[str, Any], *, status: str, summary: str | None = None, error_text: str | None = None) -> dict[str, Any] | None:
    payload = task.get("payload") or {}
    opportunity_id = payload.get("opportunity_id")
    if not opportunity_id:
        return None

    evidence: dict[str, Any] = {}
    summary_text = (summary or "") + " " + (error_text or "")

    if status != "finished":
        label = "failed"
        next_state = "rejected"
        knowledge_gain = []
        gain_count = 0
        has_gain = False
    elif task.get("task_type") == "generated_batch":
        label, next_state, knowledge_gain, evidence = _evaluation_from_generated_batch(task, payload)
        gain_count = len([g for g in knowledge_gain if g and g != "no_significant_information_gain"])
        has_gain = gain_count > 0
    else:
        knowledge_gain = [g for g in (payload.get("knowledge_gain") or payload.get("expected_information_gain") or []) if g]
        gain_count = len([g for g in knowledge_gain if g and g != "no_significant_information_gain"])
        has_gain = gain_count > 0 or ("knowledge_gain=" in summary_text and "no_significant_information_gain" not in summary_text)
        if gain_count >= 2:
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
        "evidence": evidence,
    }
    return evaluation
