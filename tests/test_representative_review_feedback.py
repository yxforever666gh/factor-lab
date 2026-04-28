import json
from pathlib import Path

from factor_lab.research_branch_planner import ResearchBranchPlanner
from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import update_research_memory_from_task_result


def test_representative_review_written_to_memory(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    task = {
        "task_id": "task-representative-1",
        "task_type": "diagnostic",
        "category": "validation",
        "fingerprint": "fp-representative-1",
        "payload": {
            "branch_id": "representative_candidate_competition",
            "diagnostic_type": "representative_candidate_competition_review",
            "focus_factors": ["mom_20"],
            "expected_information_gain": ["representative_candidate_confirmed"],
        },
        "focus_candidates": [
            {
                "candidate_name": "mom_20",
                "raw_rank_ic_mean": 0.12,
                "neutralized_rank_ic_mean": 0.05,
                "retention_industry": 0.42,
                "quality_classification": "needs-validation",
                "candidate_status": "testing",
                "failure_dossier": {
                    "failure_modes": ["short_to_medium_decay"],
                    "recommended_action": "diagnose",
                    "regime_dependency": "short_window_only",
                    "parent_delta_status": "non_incremental",
                    "parent_candidates": ["mom_base"],
                },
            }
        ],
    }

    memory = update_research_memory_from_task_result(memory_path, task, status="finished", summary="knowledge_gain=representative_candidate_confirmed")

    row = memory["representative_candidate_reviews"][-1]
    assert row["branch_id"] == "representative_candidate_competition"
    assert row["candidate_name"] == "mom_20"
    assert row["regime_dependency"] == "short_window_only"
    assert row["parent_delta_status"] == "non_incremental"
    assert row["retention_industry"] == 0.42


def test_research_learning_builds_representative_stats_and_dossiers(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "representative_candidate_reviews": [
                    {
                        "candidate_name": "mom_20",
                        "has_gain": False,
                        "outcome_class": "high_value_failure",
                        "source_stage": "recent_60d",
                        "retention_industry": 0.18,
                        "raw_rank_ic_mean": 0.10,
                        "neutralized_rank_ic_mean": -0.01,
                        "failure_modes": ["short_to_medium_decay", "neutralized_break"],
                        "regime_dependency": "short_window_only",
                        "parent_delta_status": "non_incremental",
                        "summary": "60d dropped hard",
                    },
                    {
                        "candidate_name": "mom_20",
                        "has_gain": True,
                        "outcome_class": "high_value_success",
                        "source_stage": "recent_45d",
                        "retention_industry": 0.44,
                        "raw_rank_ic_mean": 0.14,
                        "neutralized_rank_ic_mean": 0.06,
                        "failure_modes": [],
                        "regime_dependency": "short_window_only",
                        "parent_delta_status": "incremental",
                        "summary": "45d okay",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)
    assert learning["representative_candidate_stats"]["recommended_action"] == "upweight"
    dossier = learning["representative_failure_dossiers"]["mom_20"]
    assert dossier["decay_45_to_60"] == 1
    assert dossier["neutralized_break_count"] == 1
    assert dossier["parent_delta_failures"] == 1
    assert dossier["recommended_action"] == "diagnose"
    assert dossier["recommended_next_question"] == "verify_incremental_value_vs_parent"


def test_research_branch_planner_uses_representative_failure_dossiers():
    planner = ResearchBranchPlanner()
    space_map = {
        "family_progress": {
            "stable_candidate_validation": {"current_level": 1, "next_level": 2},
            "medium_horizon_validation": {"current_level": 1, "next_level": 2},
            "exploration": {"current_level": 1, "next_level": 2},
        },
        "family_fatigue": {},
        "family_saturation": {},
        "family_recent_gain": {},
    }
    snapshot = {
        "family_summary": [{"family_score": 72}],
        "relationship_summary": {"hybrid_of": 1, "refinement_of": 1, "duplicate_of": 0},
        "family_recommendations": [],
        "research_trial_summary": {},
        "analyst_signals": {},
        "representative_failure_dossiers": {
            "rep_a": {
                "recommended_action": "diagnose",
                "regime_dependency": "short_window_only",
                "parent_delta_status": "non_incremental",
            }
        },
    }
    candidate_pool = {"tasks": []}

    result = planner.plan(space_map, snapshot, candidate_pool)
    medium = next(row for row in result["branch_decisions"] if row["family"] == "medium_horizon_validation")
    explore = next(row for row in result["branch_decisions"] if row["family"] == "exploration")

    assert medium["priority_score"] > explore["priority_score"]
    assert "representative_diagnose" in medium["reason"]
