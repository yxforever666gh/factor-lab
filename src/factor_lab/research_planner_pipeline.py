from __future__ import annotations

from pathlib import Path
from typing import Any

from factor_lab.research_planner_snapshot import build_research_planner_snapshot
from factor_lab.research_candidate_pool import build_research_candidate_pool
from factor_lab.research_planner import build_research_plan
from factor_lab.research_planner_validate import validate_research_planner_proposal
from factor_lab.research_planner_inject import inject_research_planner_tasks


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"


def run_research_planner_pipeline() -> dict[str, Any]:
    snapshot_path = ROOT / "artifacts" / "research_planner_snapshot.json"
    candidate_pool_path = ROOT / "artifacts" / "research_candidate_pool.json"
    proposal_path = ROOT / "artifacts" / "research_planner_proposal.json"
    validated_path = ROOT / "artifacts" / "research_planner_validated.json"
    injected_path = ROOT / "artifacts" / "research_planner_injected.json"

    snapshot = build_research_planner_snapshot(DB_PATH, snapshot_path)
    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path)
    proposal = build_research_plan(snapshot_path, candidate_pool_path, proposal_path)
    validated = validate_research_planner_proposal(proposal_path, validated_path)
    injected = inject_research_planner_tasks(validated_path, injected_path)

    return {
        "snapshot_latest_run": (snapshot.get("latest_run") or {}).get("config_path"),
        "candidate_count": len(candidate_pool.get("tasks", [])),
        "proposal_selected_count": len(proposal.get("selected_tasks", [])),
        "validated_accepted_count": len(validated.get("accepted_tasks", [])),
        "injected_count": injected.get("injected_count", 0),
        "injected_tasks": injected.get("injected_tasks", []),
    }
