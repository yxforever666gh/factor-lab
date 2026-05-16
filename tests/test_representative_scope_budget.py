import json
from pathlib import Path

from factor_lab.research_planner import ResearchPlannerAgent
from factor_lab import research_planner_validate
from factor_lab.storage import ExperimentStore


def test_planner_selects_only_one_task_per_representative_scope():
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
                "priority_hint": 20,
                "worker_note": "validation｜stable_candidate",
                "payload": {"focus_factors": ["rep_a"]},
                "focus_candidates": [{"candidate_name": "rep_a", "evidence_gate": {"action": "needs_validation"}}],
                "expected_knowledge_gain": ["stable_candidate_confirmed"],
                "reason": "stable validation",
            },
            {
                "task_type": "diagnostic",
                "category": "validation",
                "priority_hint": 22,
                "worker_note": "validation｜中窗 60 天晋级赛",
                "payload": {"focus_factors": ["rep_a"]},
                "focus_candidates": [{"candidate_name": "rep_a", "evidence_gate": {"action": "needs_validation"}}],
                "expected_knowledge_gain": ["window_stability_check"],
                "reason": "medium horizon validation",
            },
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)

    assert len(result["selected_tasks"]) == 1
    assert result["selection_policy"]["representative_only_budget"] is True


def test_validate_research_planner_proposal_accepts_distinct_stable_medium_and_fragile_buckets(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    ExperimentStore(db_path)
    monkeypatch.setattr(research_planner_validate, "DB_PATH", db_path)

    proposal_path = tmp_path / "proposal_buckets.json"
    output_path = tmp_path / "validated_buckets.json"
    proposal_path.write_text(
        json.dumps(
            {
                "selection_policy": {
                    "category_limits": {"baseline": 2, "validation": 3, "exploration": 1},
                    "exploration_floor": {"true_fault_recovery": False, "exploration_floor_slots": 1},
                },
                "selected_tasks": [
                    {
                        "task_type": "diagnostic",
                        "category": "validation",
                        "worker_note": "validation｜稳定候选深化验证",
                        "fingerprint": "fp-stable",
                        "payload": {
                            "goal": "validate_stable_candidates",
                            "hypothesis": "stable",
                            "expected_information_gain": ["stable_candidate_confirmed"],
                            "branch_id": "stable-1",
                            "stop_if": [],
                            "promote_if": [],
                            "disconfirm_if": [],
                            "focus_factors": ["rep_a", "rep_b"],
                        },
                        "focus_candidates": [{"candidate_name": "rep_a"}, {"candidate_name": "rep_b"}],
                    },
                    {
                        "task_type": "workflow",
                        "category": "validation",
                        "worker_note": "validation｜中窗 60 天晋级赛",
                        "fingerprint": "fp-medium",
                        "payload": {
                            "goal": "validate_medium_horizon_stability",
                            "hypothesis": "medium",
                            "expected_information_gain": ["medium_horizon_promotion_check"],
                            "branch_id": "medium-1",
                            "stop_if": [],
                            "promote_if": [],
                            "disconfirm_if": [],
                            "focus_factors": ["rep_c"],
                        },
                        "focus_candidates": [{"candidate_name": "rep_c"}],
                    },
                    {
                        "task_type": "diagnostic",
                        "category": "validation",
                        "worker_note": "validation｜fragile 候选加固",
                        "fingerprint": "fp-fragile",
                        "payload": {
                            "goal": "harden_fragile_candidates",
                            "hypothesis": "fragile",
                            "expected_information_gain": ["candidate_survival_check"],
                            "branch_id": "fragile-1",
                            "stop_if": [],
                            "promote_if": [],
                            "disconfirm_if": [],
                            "focus_factors": ["rep_d"],
                        },
                        "focus_candidates": [{"candidate_name": "rep_d"}],
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = research_planner_validate.validate_research_planner_proposal(proposal_path, output_path)

    assert len(payload["accepted_tasks"]) == 3
    assert payload["summary"]["category_counts"]["validation_stable"] == 1
    assert payload["summary"]["category_counts"]["validation_medium_horizon"] == 1
    assert payload["summary"]["category_counts"]["validation_fragile"] == 1


def test_validate_research_planner_proposal_rejects_duplicate_representative_scope(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    ExperimentStore(db_path)
    monkeypatch.setattr(research_planner_validate, "DB_PATH", db_path)

    proposal_path = tmp_path / "proposal.json"
    output_path = tmp_path / "validated.json"
    proposal_path.write_text(
        json.dumps(
            {
                "selection_policy": {
                    "category_limits": {"baseline": 2, "validation": 3, "exploration": 1},
                    "exploration_floor": {"true_fault_recovery": False, "exploration_floor_slots": 1},
                },
                "selected_tasks": [
                    {
                        "task_type": "diagnostic",
                        "category": "validation",
                        "fingerprint": "fp-1",
                        "payload": {
                            "goal": "validate rep",
                            "hypothesis": "rep still valid",
                            "expected_information_gain": ["stable_candidate_confirmed"],
                            "branch_id": "branch-1",
                            "stop_if": [],
                            "promote_if": [],
                            "disconfirm_if": [],
                            "focus_factors": ["rep_a"],
                        },
                        "focus_candidates": [{"candidate_name": "rep_a"}],
                    },
                    {
                        "task_type": "diagnostic",
                        "category": "validation",
                        "fingerprint": "fp-2",
                        "payload": {
                            "goal": "validate rep again",
                            "hypothesis": "rep still valid",
                            "expected_information_gain": ["window_stability_check"],
                            "branch_id": "branch-2",
                            "stop_if": [],
                            "promote_if": [],
                            "disconfirm_if": [],
                            "focus_factors": ["rep_a"],
                        },
                        "focus_candidates": [{"candidate_name": "rep_a"}],
                    },
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = research_planner_validate.validate_research_planner_proposal(proposal_path, output_path)

    assert len(payload["accepted_tasks"]) == 1
    assert payload["rejected_tasks"][0]["validation_reasons"] == ["duplicate_representative_scope_within_plan"]
