from __future__ import annotations

from typing import Any


def compute_parent_child_delta(opportunity_store: dict[str, Any], opportunity_id: str) -> dict[str, Any] | None:
    items = (opportunity_store.get("opportunities") or {}) if isinstance(opportunity_store, dict) else {}
    current = items.get(opportunity_id) or {}
    parent_id = current.get("parent_opportunity_id")
    if not parent_id:
        return None
    parent = items.get(parent_id) or {}
    parent_eval = parent.get("evaluation") or {}
    child_eval = current.get("evaluation") or {}

    parent_gain = len([g for g in (parent_eval.get("epistemic_gain") or []) if g])
    child_gain = len([g for g in (child_eval.get("epistemic_gain") or []) if g])
    parent_value = 1 if parent_eval.get("has_gain") else 0
    child_value = 1 if child_eval.get("has_gain") else 0

    delta = {
        "parent_opportunity_id": parent_id,
        "child_opportunity_id": opportunity_id,
        "delta_epistemic_gain_count": child_gain - parent_gain,
        "delta_has_gain": child_value - parent_value,
        "parent_label": parent_eval.get("evaluation_label"),
        "child_label": child_eval.get("evaluation_label"),
        "incremental_value": "higher" if child_gain > parent_gain else ("equal" if child_gain == parent_gain else "lower"),
    }
    return delta
