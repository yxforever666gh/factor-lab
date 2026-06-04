from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.harvest_agent_policy import DEFAULT_HARVEST_AGENT_POLICY

ROOT = Path(__file__).resolve().parents[2]


def _proposal(pid: str, mechanism: str, experiment_type: str = "controlled_backtest", fields: list[str] | None = None) -> dict[str, Any]:
    return {
        "proposal_id": pid,
        "mechanism_id": mechanism,
        "hypothesis": f"Mechanism {mechanism} can produce informative simulation/backtest evidence under controlled admission.",
        "required_fields": fields or ["earnings_yield", "roe", "pb", "turnover", "return_1d"],
        "derived_fields": ["bucket_pair_spread", "cost_adjusted_return"],
        "experiment_type": experiment_type,
        "portfolio_construction": {"mode": "bucket_pair", "long_quantile": 3, "short_quantile": 0},
        "validation_protocol": "bucket_aware_oos_cost_sensitivity",
        "expected_information_gain": "Small bounded test of a mechanism-driven Harvest mainline.",
        "falsification_criteria": ["net spread turns negative", "OOS pass fails", "coverage below threshold"],
        "duplicate_rationale": "Uses current Harvest cycle scope and controlled admission checks.",
    }


def _choose_mainline(state: dict[str, Any], policy: dict[str, Any]) -> str:
    allowed = policy.get("allowed_mainlines", [])
    if state.get("promoted_bucket_aware_routes") and "bucket_aware_oos_followup" in allowed:
        return "bucket_aware_oos_followup"
    if state.get("data_blockers", {}).get("blocked_fields") and "mechanism_data_gap_analysis" in allowed:
        return "mechanism_data_gap_analysis"
    blockers = " ".join(state.get("current_blockers") or []).lower()
    if "drawdown" in blockers and "defensive_quality_risk_layer" in allowed:
        return "defensive_quality_risk_layer"
    return "direction_sanity_diagnostics" if "direction_sanity_diagnostics" in allowed else allowed[0]


def build_harvest_cycle_plan(
    state: dict[str, Any], *, cycle_id: str = "cycle_0001", policy: dict[str, Any] | None = None
) -> dict[str, Any]:
    policy = policy or DEFAULT_HARVEST_AGENT_POLICY
    max_experiments = min(2, int(policy.get("max_experiments_per_cycle", 2)))
    mainline = _choose_mainline(state, policy)
    if mainline == "bucket_aware_oos_followup":
        routes = state.get("promoted_bucket_aware_routes") or ["value_quality_no_distress"]
        proposals = [_proposal(f"{r}_cost_sensitivity_v1", r) for r in routes[:max_experiments]]
        question = "Can promoted bucket-aware routes survive stricter cost and tail-risk tests?"
    elif mainline == "defensive_quality_risk_layer":
        proposals = [_proposal("defensive_quality_drawdown_repair_v1", "defensive_quality_risk_layer", fields=["roe", "pb", "turnover", "return_1d", "volatility_20"])]
        question = "Can defensive quality/risk controls reduce drawdown without destroying return?"
    elif mainline == "mechanism_data_gap_analysis":
        proposals = [_proposal("mechanism_data_gap_triage_v1", "mechanism_data_gap_analysis", "data_gap_analysis", fields=[])]
        question = "Which blocked fields are worth manual data-enrichment review?"
    else:
        proposals = [_proposal("direction_sanity_diagnostics_v1", "direction_sanity_diagnostics")]
        question = "Do IC/spread disagreements reflect sign or bucket-shape errors?"
    proposals = proposals[:max_experiments]
    charter = {
        "schema_version": 1,
        "cycle_id": cycle_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mainline": mainline,
        "research_budget": {"max_experiments": max_experiments, "max_runtime_minutes": 60, "budget_bucket": mainline},
        "current_blockers": state.get("current_blockers", []),
        "research_question": question,
        "success_definition": [
            "positive cost-adjusted return where applicable",
            "bucket-aware OOS stable where applicable",
            "no duplicate-equivalent evidence",
            "mechanism rationale remains valid",
        ],
        "manual_approval_required": False,
    }
    return {"schema_version": 1, "cycle_id": cycle_id, "cycle_charter": charter, "proposals": proposals}


def write_harvest_cycle_plan(plan: dict[str, Any], *, root: str | Path = ROOT) -> dict[str, Path]:
    d = Path(root) / "artifacts/harvest_agent" / plan["cycle_id"]
    d.mkdir(parents=True, exist_ok=True)
    charter_path = d / "cycle_charter.json"
    proposals_path = d / "proposals.json"
    charter_path.write_text(json.dumps(plan["cycle_charter"], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    proposals_path.write_text(json.dumps(plan["proposals"], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (d / "cycle_charter.md").write_text(f"# Harvest Cycle Charter\n\nMainline: {plan['cycle_charter']['mainline']}\n", encoding="utf-8")
    (d / "proposals.md").write_text(f"# Harvest Proposals\n\nCount: {len(plan['proposals'])}\n", encoding="utf-8")
    return {"charter_path": charter_path, "proposals_path": proposals_path}
