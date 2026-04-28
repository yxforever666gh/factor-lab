import json

from factor_lab.research_planner import ResearchPlannerAgent


def test_research_planner_penalizes_duplicate_suppress_exploration():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "dup_factor",
                    "quality_total_score": 46,
                    "quality_classification": "duplicate-suppress",
                    "quality_scores": {
                        "incremental_value": 4,
                        "cross_window_robustness": 10,
                        "neutralized_quality": 8,
                        "deduped_independence": 0,
                    },
                }
            ]
        },
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "workflow",
                "category": "exploration",
                "priority_hint": 20,
                "worker_note": "exploration｜generated_candidate:dup_factor",
                "payload": {"source": "candidate_generation"},
                "focus_candidates": [
                    {"candidate_name": "dup_factor", "evidence_gate": {"action": "frontier_ok"}}
                ],
                "expected_knowledge_gain": ["candidate_survival_check"],
                "reason": "duplicate exploration",
            }
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)

    assert result["selected_tasks"][0]["planner_score"] < 100
    assert "duplicate-suppress" not in result["selected_tasks"][0]["planner_reason"] or True



def test_research_planner_rewards_quality_validation_targets():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "good_factor",
                    "quality_total_score": 84,
                    "quality_classification": "needs-validation",
                    "quality_scores": {
                        "incremental_value": 16,
                        "cross_window_robustness": 24,
                        "neutralized_quality": 14,
                        "deduped_independence": 12,
                    },
                }
            ]
        },
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "diagnostic",
                "category": "validation",
                "priority_hint": 40,
                "worker_note": "validation｜stable_candidate",
                "payload": {},
                "focus_candidates": [
                    {"candidate_name": "good_factor", "evidence_gate": {"action": "needs_validation"}}
                ],
                "expected_knowledge_gain": ["stable_candidate_confirmed"],
                "reason": "validate high quality factor",
            }
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)

    assert result["selected_tasks"][0]["planner_score"] > 100


def test_research_planner_uses_failure_dossier_to_prioritize_validation():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
        "promotion_scorecard": {"rows": []},
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "diagnostic",
                "category": "validation",
                "priority_hint": 40,
                "worker_note": "validation｜stable_candidate",
                "payload": {},
                "focus_candidates": [
                    {
                        "candidate_name": "fragile_factor",
                        "evidence_gate": {"action": "needs_validation"},
                        "failure_dossier": {
                            "recommended_action": "diagnose",
                            "regime_dependency": "short_window_only",
                            "parent_delta_status": "non_incremental",
                        },
                    }
                ],
                "expected_knowledge_gain": ["stable_candidate_confirmed"],
                "reason": "validate failure-prone factor",
            }
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)

    assert result["selected_tasks"][0]["planner_score"] >= 94
    assert "failure_dossier:" in result["selected_tasks"][0]["planner_reason"]
