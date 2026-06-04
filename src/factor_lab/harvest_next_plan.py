from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
STOP_DECISIONS = {"stop_no_information_gain", "block_route", "demote_route"}
MANUAL_REVIEW_DECISIONS = {"promote_to_manual_review", "manual_review_required"}


def _next_cycle_id(cycle_id: str | None) -> str:
    cycle_id = cycle_id or "cycle_0001"
    try:
        n = int(str(cycle_id).split("_")[-1]) + 1
    except Exception:
        n = 2
    return f"cycle_{n:04d}"


def build_next_plan(verdict: dict[str, Any]) -> dict[str, Any]:
    decision = str(verdict.get("decision") or "hold_route")
    cid = _next_cycle_id(verdict.get("cycle_id"))
    if decision in STOP_DECISIONS:
        return {
            "schema_version": 1,
            "cycle_id": cid,
            "plan_status": "stop",
            "reason": decision,
            "manual_approval_required": decision == "block_route",
            "experiments": [],
            "proposals": [],
        }
    if decision in MANUAL_REVIEW_DECISIONS or verdict.get("manual_approval_required"):
        return {
            "schema_version": 1,
            "cycle_id": cid,
            "plan_status": "manual_review",
            "reason": decision,
            "manual_approval_required": True,
            "experiments": [],
            "proposals": [],
        }
    if decision == "modify_experiment_design":
        proposal_id = "harvest_modified_design_followup_v1"
        question = "Can the route survive a minimal design modification that addresses the diagnosed failure class?"
    elif decision == "hold_route":
        proposal_id = "harvest_hold_route_review_v1"
        question = "Hold route and perform only a cheap non-executing review unless manual approval changes scope."
    else:
        proposal_id = "harvest_same_mainline_followup_v1"
        question = str(verdict.get("next_action") or "Run the smallest independent follow-up on the same mainline.")
    return {
        "schema_version": 1,
        "cycle_id": cid,
        "plan_status": "planned",
        "created_from_verdict": verdict.get("cycle_id"),
        "mainline": "defensive_quality_risk_layer",
        "research_budget": {"max_experiments": 1, "max_runtime_minutes": 20, "budget_bucket": "followup_validation"},
        "research_question": question,
        "manual_approval_required": False,
        "proposals": [
            {
                "proposal_id": proposal_id,
                "mechanism_id": "defensive_quality_risk_layer",
                "hypothesis": "A bounded follow-up can add independent evidence without expanding scope.",
                "required_fields": ["roe", "pb", "return_1d", "turnover"],
                "experiment_type": "defensive_quality_risk_layer",
                "expected_information_gain": "Tests the smallest follow-up implied by the previous Harvest verdict.",
                "falsification_criteria": ["no positive cost-adjusted result", "drawdown remains beyond configured limit", "duplicate-equivalent evidence only"],
                "duplicate_rationale": "Next-cycle follow-up must use independent validation or remain a cheap screen.",
            }
        ],
    }


def write_next_plan(verdict: dict[str, Any] | None = None, *, root: str | Path = ROOT, previous_cycle_id: str = "cycle_0001") -> dict[str, Any]:
    root = Path(root)
    prev_dir = root / "artifacts/harvest_agent" / previous_cycle_id
    if verdict is None:
        verdict_path = prev_dir / "verdict.json"
        verdict = json.loads(verdict_path.read_text(encoding="utf-8")) if verdict_path.exists() else {"cycle_id": previous_cycle_id, "decision": "hold_route"}
    plan = build_next_plan(verdict)
    out_dir = root / "artifacts/harvest_agent" / plan["cycle_id"]
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "next_cycle_plan.json").write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (out_dir / "next_cycle_plan.md").write_text("# Harvest Next Cycle Plan\n\n```json\n" + json.dumps(plan, indent=2) + "\n```\n", encoding="utf-8")
    return plan
