import json

from factor_lab.research_strategy import StrategyBrain


def test_strategy_brain_uses_autonomy_policy_budget_bias():
    brain = StrategyBrain()
    state_snapshot = {
        "autonomy_policy": {
            "budget_policy": {
                "exploitation": 0.6,
                "adjacent_exploration": 0.3,
                "novelty_search": 0.1,
            },
            "principles": {"objective": ["epistemic_gain"]},
            "quality_gates": {"prefer": [], "avoid": []},
        },
        "memory": {},
        "candidates": {"graveyard": [], "stable": []},
        "exploration_state": {},
        "knowledge_gain_counter": {"stable_candidate_confirmed": 1},
        "repeated_failure_patterns": [],
        "convergence_policy": {"archive_after_no_gain_runs": 2},
        "planner": {"analyst_signals": {}},
        "frontier_focus": {},
    }
    proposal = {
        "selected_tasks": [
            {
                "task_type": "diagnostic",
                "category": "validation",
                "priority_hint": 20,
                "goal": "validate stable candidate",
                "branch_id": "stable_candidate_validation_level_1",
                "expected_knowledge_gain": ["stable_candidate_confirmed"],
                "planner_score": 100,
                "payload": {"goal": "validate stable candidate", "branch_id": "stable_candidate_validation_level_1"},
                "focus_candidates": [],
                "relationship_signal": {},
            }
        ]
    }

    payload = brain.build_plan(state_snapshot, proposal)

    assert payload["autonomy_policy"]["budget_policy"]["exploitation"] == 0.6
    assert payload["budget"]["validation"] >= 3


def test_strategy_brain_preserves_exploration_floor_outside_true_fault_recovery():
    brain = StrategyBrain()
    state_snapshot = {
        "autonomy_policy": {
            "budget_policy": {"exploitation": 0.6, "adjacent_exploration": 0.3, "novelty_search": 0.1},
            "principles": {"objective": ["epistemic_gain"]},
            "quality_gates": {"prefer": [], "avoid": []},
        },
        "memory": {},
        "candidates": {"graveyard": [], "stable": []},
        "exploration_state": {"should_throttle": True},
        "failure_state": {"cooldown_active": False},
        "research_flow_state": {"state": "ready"},
        "knowledge_gain_counter": {},
        "repeated_failure_patterns": [],
        "convergence_policy": {"archive_after_no_gain_runs": 2},
        "planner": {"analyst_signals": {}},
        "frontier_focus": {},
    }
    proposal = {
        "selected_tasks": [
            {
                "task_type": "generated_batch",
                "category": "exploration",
                "priority_hint": 20,
                "goal": "probe new branch",
                "branch_id": "exploration_probe_1",
                "expected_knowledge_gain": ["new_branch_opened"],
                "planner_score": 100,
                "payload": {"goal": "probe new branch", "branch_id": "exploration_probe_1"},
                "focus_candidates": [],
                "relationship_signal": {},
            }
        ]
    }

    payload = brain.build_plan(state_snapshot, proposal)

    assert payload["budget"]["exploration"] >= 1



def test_strategy_brain_rewards_epistemic_gain_tasks():
    brain = StrategyBrain()
    state_snapshot = {
        "autonomy_policy": {
            "budget_policy": {"exploitation": 0.45, "adjacent_exploration": 0.35, "novelty_search": 0.2},
            "principles": {"objective": ["epistemic_gain"]},
            "quality_gates": {"prefer": ["cross_window_survival"], "avoid": ["high_corr_duplicate_variants"]},
        },
        "memory": {},
        "candidates": {"graveyard": [], "stable": []},
        "exploration_state": {},
        "knowledge_gain_counter": {},
        "repeated_failure_patterns": [],
        "convergence_policy": {"archive_after_no_gain_runs": 2},
        "planner": {"analyst_signals": {}},
        "frontier_focus": {},
    }
    proposal = {
        "selected_tasks": [
            {
                "task_type": "diagnostic",
                "category": "validation",
                "priority_hint": 20,
                "goal": "validate window stability",
                "branch_id": "window_validation_level_1",
                "expected_knowledge_gain": ["boundary_confirmed"],
                "planner_score": 100,
                "payload": {"goal": "validate window stability", "branch_id": "window_validation_level_1"},
                "focus_candidates": [],
                "relationship_signal": {},
            }
        ]
    }

    payload = brain.build_plan(state_snapshot, proposal)
    approved = payload["approved_tasks"][0]

    assert approved["strategy_score"] > 100
    assert "高信息增益" in approved["strategy_meta"]["reason"]
