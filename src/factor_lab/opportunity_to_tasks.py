from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _fingerprint_for_opportunity(opportunity: dict[str, Any]) -> str:
    return f"opportunity::{opportunity.get('opportunity_id')}::{opportunity.get('opportunity_type')}::{json.dumps(opportunity.get('target_candidates') or [], ensure_ascii=False)}"


def map_opportunity_to_task(opportunity: dict[str, Any]) -> dict[str, Any] | None:
    otype = opportunity.get("opportunity_type")
    target_candidates = list(opportunity.get("target_candidates") or [])
    target_family = opportunity.get("target_family")
    expected_gain = list(opportunity.get("expected_knowledge_gain") or [])
    priority_hint = max(1, int(round((1.0 - float(opportunity.get("priority") or 0.5)) * 100)))
    payload_base = {
        "opportunity_id": opportunity.get("opportunity_id"),
        "opportunity_type": otype,
        "question": opportunity.get("question"),
        "hypothesis": opportunity.get("hypothesis"),
        "target_family": target_family,
        "target_candidates": target_candidates,
        "expected_information_gain": expected_gain,
        "source": "research_opportunity",
    }

    if otype in {"confirm", "diagnose"}:
        diagnostic_type = f"opportunity_{otype}"
        return {
            "task_type": "diagnostic",
            "priority": priority_hint,
            "fingerprint": _fingerprint_for_opportunity(opportunity),
            "worker_note": f"validation｜opportunity:{opportunity.get('opportunity_id')}",
            "payload": {
                **payload_base,
                "diagnostic_type": diagnostic_type,
                "focus_factors": target_candidates,
                "reasons": ["opportunity_selected"],
                "goal": opportunity.get("question") or diagnostic_type,
                "branch_id": opportunity.get("opportunity_id"),
                "stop_if": [],
                "promote_if": [],
                "disconfirm_if": [],
            },
        }

    if otype in {"expand", "recombine", "probe"}:
        generated_batch_path = ROOT / "artifacts" / "generated_batch_from_llm.json"
        if generated_batch_path.exists():
            return {
                "task_type": "generated_batch",
                "priority": max(priority_hint, 40),
                "fingerprint": _fingerprint_for_opportunity(opportunity),
                "worker_note": f"exploration｜opportunity:{opportunity.get('opportunity_id')}",
                "payload": {
                    **payload_base,
                    "batch_path": str(generated_batch_path),
                    "output_dir": "artifacts/opportunity_generated_batch_run",
                    "goal": opportunity.get("question") or otype,
                    "branch_id": opportunity.get("opportunity_id"),
                    "stop_if": [],
                    "promote_if": [],
                    "disconfirm_if": [],
                },
            }

    return None
