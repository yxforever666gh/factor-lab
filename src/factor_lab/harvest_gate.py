from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from factor_lab.harvest_agent_policy import DEFAULT_HARVEST_AGENT_POLICY, validate_mainline
from factor_lab.harvest_budget import allocate_harvest_budget, budget_after_admission
from factor_lab.harvest_research_proposal import proposal_to_experiment_id, validate_harvest_research_proposal
from factor_lab.harvest_reviewer_decision import normalize_reviewer_decision

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AVAILABLE_FIELDS = {
    "date",
    "ticker",
    "industry",
    "close",
    "return_1d",
    "forward_return_5d",
    "turnover",
    "momentum_20",
    "earnings_yield",
    "book_yield",
    "roe",
    "size_inv",
    "pe_ttm",
    "pb",
    "total_mv",
    "volatility_20",
    "volatility_60",
    "industry_relative_book_yield",
    "industry_relative_earnings_yield",
}


def check_harvest_gate(
    plan: dict[str, Any],
    *,
    reviewer_decision: dict[str, Any] | None = None,
    policy: dict[str, Any] | None = None,
    available_fields: set[str] | None = None,
    recent_experiment_ids: list[str] | None = None,
    completed_today: int = 0,
    allow_controlled_execution: bool = False,
) -> dict[str, Any]:
    policy = policy or DEFAULT_HARVEST_AGENT_POLICY
    proposals = plan.get("proposals") or []
    charter = plan.get("cycle_charter") or plan
    reasons: list[str] = []
    blocked: list[str] = []
    allowed: list[str] = []
    try:
        validate_mainline(str(charter.get("mainline")), policy)
    except ValueError:
        reasons.append("unsupported_mainline")
    if charter.get("live_trading_enabled"):
        reasons.append("live_trading_requested")
    if charter.get("broad_daemon_restore_allowed"):
        reasons.append("broad_daemon_restore_requested")
    fields = available_fields or DEFAULT_AVAILABLE_FIELDS
    recent = set(recent_experiment_ids or [])
    for proposal in proposals:
        pid = proposal_to_experiment_id(proposal)
        proposal_reasons = validate_harvest_research_proposal(proposal)["reasons"]
        missing = sorted(f for f in proposal.get("required_fields", []) if f not in fields)
        if missing:
            proposal_reasons.append("missing_required_fields")
        if pid in recent:
            proposal_reasons.append("duplicate_equivalent_experiment")
        if proposal.get("paper_portfolio_promotion"):
            proposal_reasons.append("paper_portfolio_promotion_requested")
        if proposal.get("live_trading_enabled"):
            proposal_reasons.append("live_trading_requested")
        if proposal_reasons:
            blocked.append(pid)
            reasons.extend(proposal_reasons)
        else:
            allowed.append(pid)
    reviewer = normalize_reviewer_decision(reviewer_decision)
    if reviewer["decision"] == "block":
        reasons.append("reviewer_block")
    budget = allocate_harvest_budget(policy, requested_experiments=len(allowed), completed_today=completed_today)
    if budget["budget_exhausted"] and allowed:
        reasons.append("budget_exhausted")
    admitted = allowed[: budget["admitted_experiments"]]
    blocked.extend(x for x in allowed[budget["admitted_experiments"] :] if x not in blocked)
    if reasons or not admitted:
        decision = "manual_review" if reviewer["decision"] == "manual_review" and not reasons else "block"
    elif reviewer["decision"] == "manual_review":
        decision = "manual_review"
    elif reviewer["decision"] == "cheap_screen_only":
        decision = "cheap_screen_only"
    else:
        decision = "allow_controlled_execution" if allow_controlled_execution else "allow_dry_run"
    return {
        "schema_version": 1,
        "cycle_id": plan.get("cycle_id") or charter.get("cycle_id"),
        "decision": decision,
        "reasons": sorted(set(reasons)),
        "allowed_experiments": admitted if decision != "block" else [],
        "blocked_experiments": sorted(set(blocked)),
        "manual_review_required": decision == "manual_review",
        "budget_after_decision": budget_after_admission(
            max_cycle=budget["max_cycle_experiments"],
            max_daily=budget["max_daily_experiments"],
            admitted=len(admitted),
            completed_today=completed_today,
        ),
    }


def write_harvest_gate_decision(plan_path: str | Path, *, allow_controlled_execution: bool = False) -> dict[str, Any]:
    p = Path(plan_path)
    plan = json.loads(p.read_text(encoding="utf-8"))
    decision = check_harvest_gate(plan, reviewer_decision={"decision": "allow"}, allow_controlled_execution=allow_controlled_execution)
    out = p.parent / "gate_decision.json"
    out.write_text(json.dumps(decision, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (p.parent / "gate_decision.md").write_text(f"# Harvest Gate Decision\n\nDecision: {decision['decision']}\n", encoding="utf-8")
    return decision
