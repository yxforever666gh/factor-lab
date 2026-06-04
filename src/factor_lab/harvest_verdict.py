from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.harvest_scorecard import score_evidence_ledger

ROOT = Path(__file__).resolve().parents[2]
VERDICT_DECISIONS = {
    "promote_to_manual_review", "continue_same_mainline", "modify_experiment_design", "hold_route", "demote_route",
    "block_route", "stop_no_information_gain", "manual_review_required",
}


def _candidate(row: dict[str, Any]) -> str:
    return str(row.get("mechanism_id") or row.get("experiment_id") or "unknown")


def build_verdict(ledger: dict[str, Any], *, previous_no_information_gain_count: int = 0) -> dict[str, Any]:
    scored = score_evidence_ledger(ledger)
    rows = scored.get("evidence") or []
    reasoning: list[str] = []
    promoted: list[str] = []
    held: list[str] = []
    blocked: list[str] = []

    if any(row.get("manual_review_required") or row.get("failure_class") == "manual_review_required" for row in rows):
        decision = "manual_review_required"
        reasoning.append("At least one experiment requires manual review before further action.")
    elif not rows:
        decision = "hold_route"
        reasoning.append("No evidence rows were available; hold route and do not execute further automatically.")
    elif any(row.get("failure_class") in {"unsupported_feature_requested", "future_data_or_timing_risk"} for row in rows):
        decision = "block_route"
        reasoning.append("Unsafe or unsupported request detected.")
    elif previous_no_information_gain_count >= 1 and all(row.get("information_gain") in {"duplicate_or_low_information", "execution_failure"} for row in rows):
        decision = "stop_no_information_gain"
        reasoning.append("Two consecutive cycles added no useful information.")
    elif any(row.get("failure_class") == "duplicate_equivalent_experiment" for row in rows):
        decision = "stop_no_information_gain" if previous_no_information_gain_count >= 1 else "hold_route"
        reasoning.append("Duplicate-equivalent evidence detected; do not count as new progress.")
    elif any(row.get("failure_class") == "missing_required_fields" for row in rows):
        decision = "block_route"
        reasoning.append("Required data is missing and no automatic data expansion is allowed.")
    elif any(row.get("promotion_eligible") and float(row.get("soft_score_average", 0.0)) >= 4.0 for row in rows) or any(
        row.get("promotion_eligible") and float(row.get("soft_score_average", 0.0)) >= 4.0 for row in ledger.get("evidence", [])
    ):
        decision = "promote_to_manual_review"
        promoted = [_candidate(row) for row in rows if row.get("promotion_eligible")] or [
            _candidate(row) for row in ledger.get("evidence", []) if row.get("promotion_eligible")
        ]
        reasoning.append("All hard gates passed with strong soft score; promotion still requires manual review.")
    elif any(row.get("information_gain") == "positive_progress" for row in rows):
        decision = "continue_same_mainline"
        held = [_candidate(row) for row in rows if row.get("information_gain") == "positive_progress"]
        reasoning.append("Evidence shows positive progress but is incomplete for promotion.")
    elif any(row.get("failure_class") in {"portfolio_construction_mismatch", "cost_sensitivity_failure", "drawdown_too_deep", "negative_return_after_cost", "direction_error", "horizon_mismatch"} for row in rows):
        decision = "modify_experiment_design"
        reasoning.append("Signal or route may exist, but construction, cost, risk, direction, or horizon needs modification.")
    else:
        decision = "demote_route"
        reasoning.append("Evidence is weak or unstable with no clear positive progress.")

    for row in rows:
        if row.get("failure_class") in {"missing_required_fields", "unsupported_feature_requested", "future_data_or_timing_risk", "duplicate_equivalent_experiment"}:
            blocked.append(_candidate(row))
    manual = decision in {"promote_to_manual_review", "manual_review_required"}
    next_action = {
        "promote_to_manual_review": "manual review before any paper portfolio promotion",
        "continue_same_mainline": "run the smallest independent follow-up on the same mainline",
        "modify_experiment_design": "modify experiment design before another run",
        "hold_route": "hold route; wait for non-duplicate evidence or manual review",
        "demote_route": "demote route priority",
        "block_route": "block route until issue is resolved manually",
        "stop_no_information_gain": "stop this mainline due to low information gain",
        "manual_review_required": "manual review required before proceeding",
    }[decision]
    return {
        "schema_version": 1,
        "cycle_id": ledger.get("cycle_id"),
        "decision": decision,
        "promoted_candidates": sorted(set(promoted)),
        "held_candidates": sorted(set(held)),
        "blocked_candidates": sorted(set(blocked)),
        "reasoning": reasoning,
        "next_action": next_action,
        "manual_approval_required": manual,
    }


def write_verdict(*, root: str | Path = ROOT, cycle_id: str = "cycle_0001") -> dict[str, Any]:
    cycle_dir = Path(root) / "artifacts/harvest_agent" / cycle_id
    ledger_path = cycle_dir / "evidence_ledger.json"
    ledger = json.loads(ledger_path.read_text(encoding="utf-8")) if ledger_path.exists() else {"cycle_id": cycle_id, "evidence": []}
    verdict = build_verdict(ledger)
    (cycle_dir / "verdict.json").write_text(json.dumps(verdict, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (cycle_dir / "verdict.md").write_text("# Harvest Verdict\n\n```json\n" + json.dumps(verdict, indent=2) + "\n```\n", encoding="utf-8")
    return verdict
