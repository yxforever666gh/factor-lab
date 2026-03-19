from __future__ import annotations

from pathlib import Path
from typing import Any

from factor_lab.research_planner_snapshot import build_research_planner_snapshot
from factor_lab.research_candidate_pool import build_research_candidate_pool
from factor_lab.research_branch_planner import build_branch_planner_output
from factor_lab.research_planner import build_research_plan
from factor_lab.research_planner_validate import validate_research_planner_proposal
from factor_lab.research_planner_inject import inject_research_planner_tasks
from factor_lab.research_space_registry import build_research_space_registry
from factor_lab.research_space_map import build_research_space_map


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def run_research_planner_pipeline() -> dict[str, Any]:
    registry_path = ROOT / "artifacts" / "research_space_registry.json"
    space_map_path = ROOT / "artifacts" / "research_space_map.json"
    snapshot_path = ROOT / "artifacts" / "research_planner_snapshot.json"
    candidate_pool_path = ROOT / "artifacts" / "research_candidate_pool.json"
    branch_plan_path = ROOT / "artifacts" / "research_branch_plan.json"
    proposal_path = ROOT / "artifacts" / "research_planner_proposal.json"
    validated_path = ROOT / "artifacts" / "research_planner_validated.json"
    injected_path = ROOT / "artifacts" / "research_planner_injected.json"

    registry = build_research_space_registry(DB_PATH, registry_path)
    space_map = build_research_space_map(DB_PATH, space_map_path)
    snapshot = build_research_planner_snapshot(DB_PATH, snapshot_path)
    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path)
    branch_plan = build_branch_planner_output(space_map_path, snapshot_path, candidate_pool_path, branch_plan_path)
    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path, branch_plan_path)
    proposal = build_research_plan(snapshot_path, candidate_pool_path, proposal_path, branch_plan_path)
    validated = validate_research_planner_proposal(proposal_path, validated_path)
    injected = inject_research_planner_tasks(validated_path, injected_path)

    return {
        "registry_windows_count": len((registry.get("windows_covered") or {})),
        "registry_validation_depth_count": len((registry.get("validation_depth") or {})),
        "registry_graveyard_depth_count": len((registry.get("graveyard_diagnostics") or {})),
        "snapshot_latest_run": (snapshot.get("latest_run") or {}).get("config_path"),
        "space_map_families": list((space_map.get("family_progress") or {}).keys()),
        "candidate_count": len(candidate_pool.get("tasks", [])),
        "branch_selected_families": branch_plan.get("selected_families", []),
        "proposal_selected_count": len(proposal.get("selected_tasks", [])),
        "validated_accepted_count": len(validated.get("accepted_tasks", [])),
        "injected_count": injected.get("injected_count", 0),
        "injected_tasks": injected.get("injected_tasks", []),
    }
