from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

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
from factor_lab.research_metrics import build_research_metrics
from factor_lab.agent_briefs import build_planner_agent_brief, build_failure_analyst_brief
import subprocess
import sys
from factor_lab.research_strategy import (
    build_research_state_snapshot,
    build_strategy_plan,
    apply_strategy_plan,
)


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "artifacts" / "factor_lab.db"
PLANNER_FINGERPRINT_PATH = ROOT / "artifacts" / "planner_state_fingerprint.json"
PLANNER_COOLDOWN_MINUTES = 5


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _planner_fingerprint(snapshot: dict[str, Any], candidate_pool: dict[str, Any], branch_plan: dict[str, Any]) -> str:
    payload = {
        "latest_run": (snapshot.get("latest_run") or {}).get("run_id"),
        "latest_graveyard": snapshot.get("latest_graveyard") or [],
        "stable_candidates": [row.get("factor_name") for row in (snapshot.get("stable_candidates") or []) if row.get("factor_name")],
        "candidate_task_ids": [row.get("branch_id") or row.get("fingerprint") for row in (candidate_pool.get("tasks") or [])],
        "selected_families": branch_plan.get("selected_families") or [],
    }
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _maybe_skip_pipeline(fingerprint: str) -> dict[str, Any] | None:
    if not PLANNER_FINGERPRINT_PATH.exists():
        return None
    state = json.loads(PLANNER_FINGERPRINT_PATH.read_text(encoding="utf-8"))
    last_fp = state.get("fingerprint")
    last_run_at = _parse_iso(state.get("updated_at_utc"))
    last_injected = int(state.get("last_injected_count") or 0)
    if last_fp != fingerprint:
        return None
    if last_injected > 0:
        return None
    if last_run_at is None:
        return None
    if datetime.now(timezone.utc) - last_run_at < timedelta(minutes=PLANNER_COOLDOWN_MINUTES):
        return {
            "skipped": True,
            "reason": "planner_cooldown_no_state_change",
            "fingerprint": fingerprint,
            "last_injected_count": last_injected,
        }
    return None


def _write_fingerprint_state(fingerprint: str, injected_count: int) -> None:
    PLANNER_FINGERPRINT_PATH.write_text(
        json.dumps(
            {
                "updated_at_utc": _iso_now(),
                "fingerprint": fingerprint,
                "last_injected_count": injected_count,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )



def _active_sticky_medium_horizon_candidates(memory_path: Path) -> list[str]:
    if not memory_path.exists():
        return []
    try:
        memory = json.loads(memory_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    active = []
    for row in (memory.get("sticky_medium_horizon_candidates") or []):
        name = row.get("candidate_name")
        rounds_remaining = int(row.get("rounds_remaining") or 0)
        if name and rounds_remaining > 0:
            active.append(name)
    return active


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
    planner_agent_brief_path = ROOT / "artifacts" / "planner_agent_brief.json"
    failure_analyst_brief_path = ROOT / "artifacts" / "failure_analyst_brief.json"
    agent_responses_path = ROOT / "artifacts" / "agent_responses.json"

    with ThreadPoolExecutor(max_workers=3) as executor:
        registry_future = executor.submit(build_research_space_registry, DB_PATH, registry_path)
        space_map_future = executor.submit(build_research_space_map, DB_PATH, space_map_path)
        snapshot_future = executor.submit(build_research_planner_snapshot, DB_PATH, snapshot_path)
        registry = registry_future.result()
        space_map = space_map_future.result()
        snapshot = snapshot_future.result()

    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path)
    branch_plan = build_branch_planner_output(space_map_path, snapshot_path, candidate_pool_path, branch_plan_path)
    candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path, branch_plan_path)

    fingerprint = _planner_fingerprint(snapshot, candidate_pool, branch_plan)
    skip = _maybe_skip_pipeline(fingerprint)
    if skip is not None:
        return skip

    recovery_used = False
    active_sticky_medium_horizon = _active_sticky_medium_horizon_candidates(memory_path)
    if not (candidate_pool.get("tasks") or []) and active_sticky_medium_horizon:
        # If sticky medium-horizon candidates are still alive, do one unconstrained rebuild
        # before allowing recovery to take over the whole planner turn.
        candidate_pool = build_research_candidate_pool(snapshot_path, candidate_pool_path)
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
    # Optional structured agent responses can be dropped into artifacts/planner_agent_response.json
    # and artifacts/failure_analyst_response.json; we merge them here when present.
    if os.getenv("FACTOR_LAB_ENABLE_BRIEF_RUNNER", "1").strip().lower() in {"1", "true", "yes", "on"}:
        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "run_agent_briefs.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
    agent_responses_payload = {
        "planner": json.loads((ROOT / "artifacts" / "planner_agent_response.json").read_text(encoding="utf-8")) if (ROOT / "artifacts" / "planner_agent_response.json").exists() else {},
        "failure_analyst": json.loads((ROOT / "artifacts" / "failure_analyst_response.json").read_text(encoding="utf-8")) if (ROOT / "artifacts" / "failure_analyst_response.json").exists() else {},
    }
    agent_responses_path.write_text(json.dumps(agent_responses_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    strategy_plan = build_strategy_plan(state_snapshot_path, proposal_path, strategy_plan_path, branch_plan_path, agent_responses_path)
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
    # Autonomy knob: allow more scheduled opportunities per planner loop.
    # Keep conservative by default but not artificially capped.
    opportunity_limit = int(os.getenv("RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT", "4"))
    opportunity_limit = max(1, min(8, opportunity_limit))
    opportunity_execution = enqueue_opportunities(
        ROOT / "artifacts" / "research_opportunities.json",
        ROOT / "artifacts" / "opportunity_execution_plan.json",
        DB_PATH,
        limit=opportunity_limit,
        queue_aware=True,
    )
    llm_diagnostics = build_llm_diagnostics(snapshot_path, ROOT / "artifacts" / "research_opportunities.json", ROOT / "artifacts" / "llm_diagnostics.json")
    planner_agent_brief = build_planner_agent_brief(
        snapshot=snapshot,
        candidate_pool=candidate_pool,
        branch_plan=branch_plan,
        state_snapshot=state_snapshot,
        strategy_plan=strategy_plan,
        output_path=planner_agent_brief_path,
    )
    failure_analyst_brief = build_failure_analyst_brief(
        snapshot=snapshot,
        state_snapshot=state_snapshot,
        llm_diagnostics=llm_diagnostics,
        output_path=failure_analyst_brief_path,
    )
    research_metrics = build_research_metrics(
        db_path=DB_PATH,
        memory_path=memory_path,
        learning_path=ROOT / "artifacts" / "research_learning.json",
        candidate_pool_path=candidate_pool_path,
        output_path=ROOT / "artifacts" / "research_metrics.json",
    )

    result = {
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
        "planner_agent_brief": {
            "path": str(planner_agent_brief_path),
            "schema_version": planner_agent_brief.get("schema_version"),
            "input_open_question_count": len((planner_agent_brief.get("inputs") or {}).get("open_questions") or []),
            "candidate_task_count": len((planner_agent_brief.get("inputs") or {}).get("candidate_pool_tasks") or []),
        },
        "agent_responses": {
            "path": str(agent_responses_path),
            "planner_present": bool(agent_responses_payload.get("planner")),
            "failure_analyst_present": bool(agent_responses_payload.get("failure_analyst")),
        },
        "failure_analyst_brief": {
            "path": str(failure_analyst_brief_path),
            "schema_version": failure_analyst_brief.get("schema_version"),
            "recent_failed_or_risky_task_count": len((failure_analyst_brief.get("inputs") or {}).get("recent_failed_or_risky_tasks") or []),
        },
        "research_metrics": research_metrics,
        "state_snapshot_open_questions": len(state_snapshot.get("open_questions", [])),
    }
    _write_fingerprint_state(fingerprint, injected.get("injected_count", 0))
    return result
