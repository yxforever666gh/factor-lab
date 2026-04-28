from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

from factor_lab.agent_runtime_hooks import safe_run_reviewer_review
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
from factor_lab.research_attribution import build_research_attribution
from factor_lab.research_metrics import build_research_metrics
from factor_lab.agent_briefs import build_planner_agent_brief, build_failure_analyst_brief
from factor_lab.agent_responses import load_validated_agent_responses
from factor_lab.decision_impact_report import build_decision_impact_report
from factor_lab.llm_provider_router import DecisionProviderRouter
from factor_lab.paths import artifacts_dir, db_path, project_root
import subprocess
import sys
from factor_lab.research_strategy import (
    build_research_state_snapshot,
    build_strategy_plan,
    apply_strategy_plan,
)


PLANNER_COOLDOWN_MINUTES = 5


def _root_path() -> Path:
    return project_root()


def _artifacts_path() -> Path:
    return artifacts_dir()


def _db_path() -> Path:
    return db_path()


def _planner_fingerprint_path() -> Path:
    return _artifacts_path() / "planner_state_fingerprint.json"


def _live_provider_health_path() -> Path:
    return _artifacts_path() / "llm_provider_health_live.json"


def _observation_provider_health_path() -> Path:
    return _artifacts_path() / "llm_provider_health.json"


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
    planner_fingerprint_path = _planner_fingerprint_path()
    if not planner_fingerprint_path.exists():
        return None
    state = json.loads(planner_fingerprint_path.read_text(encoding="utf-8"))
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
    planner_fingerprint_path = _planner_fingerprint_path()
    planner_fingerprint_path.parent.mkdir(parents=True, exist_ok=True)
    planner_fingerprint_path.write_text(
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


def _configured_decision_provider() -> str:
    return (
        os.getenv("FACTOR_LAB_DECISION_PROVIDER")
        or os.getenv("FACTOR_LAB_LLM_PROVIDER")
        or "heuristic"
    ).strip().lower()


def _live_decision_provider() -> str:
    return (
        os.getenv("FACTOR_LAB_LIVE_DECISION_PROVIDER")
        or _configured_decision_provider()
    ).strip().lower()


def _observation_decision_provider() -> str:
    return (
        os.getenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER")
        or _configured_decision_provider()
    ).strip().lower()


def _decision_effective_source(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("decision_metadata") or {}
    return metadata.get("effective_source") or metadata.get("source") or payload.get("decision_source")


def run_research_planner_pipeline() -> dict[str, Any]:
    root = _root_path()
    artifacts = _artifacts_path()
    db = _db_path()
    artifacts.mkdir(parents=True, exist_ok=True)

    registry_path = artifacts / "research_space_registry.json"
    space_map_path = artifacts / "research_space_map.json"
    snapshot_path = artifacts / "research_planner_snapshot.json"
    candidate_pool_path = artifacts / "research_candidate_pool.json"
    branch_plan_path = artifacts / "research_branch_plan.json"
    proposal_path = artifacts / "research_planner_proposal.json"
    state_snapshot_path = artifacts / "research_state_snapshot.json"
    strategy_plan_path = artifacts / "strategy_plan.json"
    memory_path = artifacts / "research_memory.json"
    validated_path = artifacts / "research_planner_validated.json"
    injected_path = artifacts / "research_planner_injected.json"
    planner_agent_brief_path = artifacts / "planner_agent_brief.json"
    failure_analyst_brief_path = artifacts / "failure_analyst_brief.json"
    agent_responses_path = artifacts / "agent_responses.json"
    decision_impact_path = artifacts / "decision_impact_report.json"
    research_flow_state_path = artifacts / "research_flow_state.json"
    research_opportunities_path = artifacts / "research_opportunities.json"
    opportunity_execution_plan_path = artifacts / "opportunity_execution_plan.json"
    llm_diagnostics_path = artifacts / "llm_diagnostics.json"
    research_learning_path = artifacts / "research_learning.json"
    candidate_generation_plan_path = artifacts / "candidate_generation_plan.json"
    promotion_scorecard_path = artifacts / "promotion_scorecard.json"
    research_attribution_path = artifacts / "research_attribution.json"
    factor_quality_observation_report_path = artifacts / "factor_quality_observation_report.md"
    reviewer_review_path = artifacts / "reviewer_review.json"
    live_provider_health_path = _live_provider_health_path()
    observation_provider_health_path = _observation_provider_health_path()
    live_decision_provider = _live_decision_provider()
    observation_decision_provider = _observation_decision_provider()

    with ThreadPoolExecutor(max_workers=3) as executor:
        registry_future = executor.submit(build_research_space_registry, db, registry_path)
        space_map_future = executor.submit(build_research_space_map, db, space_map_path)
        snapshot_future = executor.submit(build_research_planner_snapshot, db, snapshot_path)
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
        db,
        snapshot_path,
        candidate_pool_path,
        proposal_path,
        state_snapshot_path,
        memory_path,
    )
    research_flow_state = derive_research_flow_state(
        snapshot=snapshot,
        candidate_pool=candidate_pool,
        recovery_used=recovery_used,
        injected_count=0,
    )
    research_flow_state_path.write_text(
        json.dumps(research_flow_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    research_opportunities = build_research_opportunities(snapshot_path, research_opportunities_path)
    # Autonomy knob: allow more scheduled opportunities per planner loop.
    # Keep conservative by default but not artificially capped.
    opportunity_limit = int(os.getenv("RESEARCH_OPPORTUNITY_ENQUEUE_LIMIT", "4"))
    opportunity_limit = max(1, min(8, opportunity_limit))
    opportunity_execution = enqueue_opportunities(
        research_opportunities_path,
        opportunity_execution_plan_path,
        db,
        limit=opportunity_limit,
        queue_aware=True,
    )
    llm_diagnostics = build_llm_diagnostics(snapshot_path, research_opportunities_path, llm_diagnostics_path)
    planner_agent_brief = build_planner_agent_brief(
        snapshot=snapshot,
        candidate_pool=candidate_pool,
        branch_plan=branch_plan,
        state_snapshot=state_snapshot,
        strategy_plan={},
        output_path=planner_agent_brief_path,
    )
    failure_analyst_brief = build_failure_analyst_brief(
        snapshot=snapshot,
        state_snapshot=state_snapshot,
        llm_diagnostics=llm_diagnostics,
        output_path=failure_analyst_brief_path,
    )
    live_provider_health = DecisionProviderRouter(provider=live_decision_provider).healthcheck(
        output_path=live_provider_health_path,
        probe=False,
    )
    observation_provider_health = DecisionProviderRouter(provider=observation_decision_provider).healthcheck(
        output_path=observation_provider_health_path
    )
    # Enhance provider_health with explicit diagnostics for gray-mode switching
    provider_health = {
        "live": {
            **live_provider_health,
            "normalized_provider": live_provider_health.get("normalized_provider"),
            "provider_class": live_provider_health.get("provider_class"),
        },
        "observation": {
            **observation_provider_health,
            "normalized_provider": observation_provider_health.get("normalized_provider"),
            "provider_class": observation_provider_health.get("provider_class"),
        },
    }
    brief_runner_result: dict[str, Any] = {"enabled": False}
    if os.getenv("FACTOR_LAB_ENABLE_BRIEF_RUNNER", "1").strip().lower() in {"1", "true", "yes", "on"}:
        completed = subprocess.run(
            [sys.executable, str(root / "scripts" / "run_agent_briefs.py"), "--provider", live_decision_provider],
            cwd=root,
            capture_output=True,
            text=True,
        )
        brief_runner_result = {
            "enabled": True,
            "provider": live_decision_provider,
            "returncode": completed.returncode,
            "stdout": (completed.stdout or "").strip(),
            "stderr": (completed.stderr or "").strip(),
        }
    agent_responses_payload = load_validated_agent_responses(artifacts)
    agent_responses_path.write_text(json.dumps(agent_responses_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    decision_impact = build_decision_impact_report(output_path=decision_impact_path)
    
    # Determine gray_mode marker when providers differ
    gray_mode = None
    live_normalized = provider_health["live"].get("normalized_provider")
    observation_normalized = provider_health["observation"].get("normalized_provider")
    if live_normalized and observation_normalized and live_normalized != observation_normalized:
        gray_mode = "observation_only"
    strategy_plan = build_strategy_plan(state_snapshot_path, proposal_path, strategy_plan_path, branch_plan_path, agent_responses_path)
    validated = validate_research_planner_proposal(proposal_path, validated_path)
    injected = apply_strategy_plan(validated_path, strategy_plan_path, injected_path, memory_path, db)
    research_flow_state = derive_research_flow_state(
        snapshot=snapshot,
        candidate_pool=candidate_pool,
        recovery_used=recovery_used,
        injected_count=injected.get("injected_count", 0),
    )
    research_flow_state_path.write_text(
        json.dumps(research_flow_state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    research_metrics = build_research_metrics(
        db_path=db,
        memory_path=memory_path,
        learning_path=research_learning_path,
        candidate_pool_path=candidate_pool_path,
        output_path=artifacts / "research_metrics.json",
    )
    research_attribution = build_research_attribution(
        memory_path=memory_path,
        learning_path=research_learning_path,
        candidate_pool_path=candidate_pool_path,
        candidate_generation_plan_path=candidate_generation_plan_path,
        promotion_scorecard_path=promotion_scorecard_path,
        output_path=research_attribution_path,
        report_path=factor_quality_observation_report_path,
    )
    reviewer_review = safe_run_reviewer_review(
        context={
            "context_id": f"reviewer:{(snapshot.get('latest_run') or {}).get('run_id') or 'planner-cycle'}",
            "inputs": {
                "latest_run": snapshot.get("latest_run") or {},
                "promotion_scorecard": snapshot.get("promotion_scorecard") or {},
                "candidate_pool": candidate_pool,
                "research_attribution": research_attribution,
                "stable_candidates": snapshot.get("stable_candidates") or [],
                "latest_graveyard": snapshot.get("latest_graveyard") or [],
            },
        },
        output_path=reviewer_review_path,
        provider=observation_decision_provider,
    ) or {}

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
            "planner_errors": agent_responses_payload.get("planner_errors") or [],
            "failure_analyst_errors": agent_responses_payload.get("failure_analyst_errors") or [],
            "configured_live_provider": live_decision_provider,
            "configured_observation_provider": observation_decision_provider,
            "planner_source": _decision_effective_source(agent_responses_payload.get("planner") or {}),
            "failure_analyst_source": _decision_effective_source(agent_responses_payload.get("failure_analyst") or {}),
            "brief_runner": brief_runner_result,
            "provider_health": provider_health,
            "gray_mode": gray_mode,
            "decision_impact_path": str(decision_impact_path),
            "decision_impact_changed": bool((decision_impact.get("planner") or {}).get("changed") or (decision_impact.get("failure_analyst") or {}).get("changed")),
        },
        "failure_analyst_brief": {
            "path": str(failure_analyst_brief_path),
            "schema_version": failure_analyst_brief.get("schema_version"),
            "recent_failed_or_risky_task_count": len((failure_analyst_brief.get("inputs") or {}).get("recent_failed_or_risky_tasks") or []),
        },
        "research_metrics": research_metrics,
        "research_attribution": research_attribution,
        "reviewer_review": {
            "path": str(reviewer_review_path),
            "schema_version": reviewer_review.get("schema_version"),
            "source": _decision_effective_source(reviewer_review),
            "recommendation_count": len(reviewer_review.get("candidate_reviews") or []),
        },
        "state_snapshot_open_questions": len(state_snapshot.get("open_questions", [])),
    }
    _write_fingerprint_state(fingerprint, injected.get("injected_count", 0))
    return result
