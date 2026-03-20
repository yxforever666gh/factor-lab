from __future__ import annotations

from pathlib import Path
from typing import Any
import json

from factor_lab.research_planner_snapshot import build_research_planner_snapshot
from factor_lab.research_candidate_pool import build_research_candidate_pool
from factor_lab.research_branch_planner import build_branch_planner_output
from factor_lab.research_planner import build_research_plan
from factor_lab.research_planner_validate import validate_research_planner_proposal
from factor_lab.planner_recovery import build_recovery_tasks
from factor_lab.research_space_registry import build_research_space_registry
from factor_lab.research_space_map import build_research_space_map
from factor_lab.research_flow_state import derive_research_flow_state
from factor_lab.research_opportunity_engine import build_research_opportunities
from factor_lab.llm_diagnostics import build_llm_diagnostics
from factor_lab.opportunity_executor import enqueue_opportunities
from factor_lab.research_strategy import (
    build_research_state_snapshot,
    build_strategy_plan,
    apply_strategy_plan,
)


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def run_research_planner_pipeline() -> dict[str, Any]:
    registry_path = ROOT / "artifacts" / "research_space_registry.json"
    space_map_path = ROOT / "artifacts" / "research_space_map.json"
    snapshot_path = ROOT / "artifacts" / "research_planner_snapshot.json"
    candidate_pool_path = ROOT / "artifacts" / "research_candidate_pool.json"
    branch_plan_path = ROOT / "artifacts" / "research_branch_plan.json"
    proposal_path = ROOT / "artifacts" / "research_planner_proposal.json"
    state_snapshot_path = ROOT / "artifacts" / "research_state_snapshot.json"
    strategy_plan_path = ROOT / "artifacts" / "strategy_plan.json"
    memory_path = ROOT / "artifacts" / "research_memory.json"
    validated_path = ROOT / "artifacts" / "research_planner_validated.json"
    injected_path = ROOT / "artifacts" / "research_planner_injected.json"

    registry = build_research_space_registry(DB_PATH, registry_path)
    space_map = build_research_space_map(DB_PATH, space_map_path)
    snapshot = build_research_planner_snapshot(DB_PATH, snapshot_path)
    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path)
    branch_plan = build_branch_planner_output(space_map_path, snapshot_path, candidate_pool_path, branch_plan_path)
    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path, branch_plan_path)
    recovery_used = False
    if not (candidate_pool.get("tasks") or []):
        candidate_pool = build_recovery_tasks(snapshot_path, candidate_pool_path, branch_plan_path)
        recovery_used = True
    proposal = build_research_plan(snapshot_path, candidate_pool_path, proposal_path, branch_plan_path)
    state_snapshot = build_research_state_snapshot(
        DB_PATH,
        snapshot_path,
        candidate_pool_path,
        proposal_path,
        state_snapshot_path,
        memory_path,
    )
    strategy_plan = build_strategy_plan(state_snapshot_path, proposal_path, strategy_plan_path, branch_plan_path)
    validated = validate_research_planner_proposal(proposal_path, validated_path)
    injected = apply_strategy_plan(validated_path, strategy_plan_path, injected_path, memory_path, DB_PATH)
    research_flow_state = derive_research_flow_state(
        snapshot=snapshot,
        candidate_pool=candidate_pool,
        recovery_used=recovery_used,
        injected_count=injected.get("injected_count", 0),
    )
    (ROOT / "artifacts" / "research_flow_state.json").write_text(
        json.dumps(research_flow_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    research_opportunities = build_research_opportunities(snapshot_path, ROOT / "artifacts" / "research_opportunities.json")
    opportunity_execution = enqueue_opportunities(ROOT / "artifacts" / "research_opportunities.json", ROOT / "artifacts" / "opportunity_execution_plan.json", DB_PATH, limit=2)
    llm_diagnostics = build_llm_diagnostics(snapshot_path, ROOT / "artifacts" / "research_opportunities.json", ROOT / "artifacts" / "llm_diagnostics.json")

    return {
        "registry_windows_count": len((registry.get("windows_covered") or {})),
        "registry_validation_depth_count": len((registry.get("validation_depth") or {})),
        "registry_graveyard_depth_count": len((registry.get("graveyard_diagnostics") or {})),
        "snapshot_latest_run": (snapshot.get("latest_run") or {}).get("config_path"),
        "space_map_families": list((space_map.get("family_progress") or {}).keys()),
        "candidate_count": len(candidate_pool.get("tasks", [])),
        "recovery_used": recovery_used,
        "branch_selected_families": branch_plan.get("selected_families", []),
        "proposal_selected_count": len(proposal.get("selected_tasks", [])),
        "strategy_approved_count": len(strategy_plan.get("approved_tasks", [])),
        "validated_accepted_count": len(validated.get("accepted_tasks", [])),
        "injected_count": injected.get("injected_count", 0),
        "injected_tasks": injected.get("injected_tasks", []),
        "research_flow_state": research_flow_state,
        "research_opportunity_count": len(research_opportunities.get("opportunities", [])),
        "opportunity_execution": opportunity_execution,
        "llm_diagnostics": llm_diagnostics,
        "state_snapshot_open_questions": len(state_snapshot.get("open_questions", [])),
    }
