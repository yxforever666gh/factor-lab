import json
from pathlib import Path

from factor_lab.research_family_generators import build_watchlist_candidate_task, build_fragile_candidate_task
from factor_lab.research_planner import ResearchPlannerAgent


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG = json.loads((REPO_ROOT / "configs" / "tushare_workflow.json").read_text(encoding="utf-8"))


def test_build_watchlist_candidate_task_creates_progressive_validation_workflow(tmp_path, monkeypatch):
    latest_run = {"config_path": "artifacts/generated_configs/rolling_30d_back.json"}
    tasks = build_watchlist_candidate_task(
        1,
        ["book_yield_plus_earnings_yield", "earnings_yield_over_pb"],
        latest_run,
        "2026-03-18",
        BASE_CONFIG,
        set(),
        set(),
    )

    assert len(tasks) == 1
    assert tasks[0]["category"] == "validation"
    assert tasks[0]["payload"]["focus_factors"] == ["book_yield_plus_earnings_yield", "earnings_yield_over_pb"]
    assert "watchlist" in tasks[0]["worker_note"]


def test_build_fragile_candidate_task_creates_hardening_validation():
    tasks = build_fragile_candidate_task(
        1,
        ["hybrid_mom_20_value_ep", "mom_plus_value"],
        set(),
    )

    assert len(tasks) == 1
    assert tasks[0]["category"] == "validation"
    assert tasks[0]["payload"]["focus_factors"] == ["hybrid_mom_20_value_ep", "mom_plus_value"]
    assert "fragile" in tasks[0]["worker_note"]


def test_research_planner_keeps_high_triage_generated_candidate_in_exploration_mix():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
        "promotion_scorecard": {"rows": []},
        "research_flow_state": {"state": "ready"},
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "workflow",
                "category": "exploration",
                "priority_hint": 49,
                "worker_note": "exploration｜generated_candidate:test｜triage=high",
                "payload": {"source": "candidate_generation", "triage": {"score": 0.81, "label": "high"}},
                "focus_candidates": [],
                "expected_knowledge_gain": ["candidate_survival_check"],
                "reason": "high triage generated candidate",
            }
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)

    assert result["selected_tasks"][0]["planner_score"] >= 80
