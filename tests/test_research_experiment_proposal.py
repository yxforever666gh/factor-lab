from factor_lab.research_experiment_proposal import ResearchExperimentProposal, proposal_from_workflow_task


def test_research_experiment_proposal_round_trips_json():
    proposal = ResearchExperimentProposal(
        proposal_id="p1",
        experiment_type="workflow",
        factor_names=["value"],
        expressions=["book_yield"],
        mechanism_id="value_quality_no_distress",
        hypothesis="cheap but quality companies should outperform",
        expected_information_gain=["mechanism_validation"],
        required_data_fields=["book_yield", "roe"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        universe_limit=100,
        horizon="60d",
        parent_candidates=[],
        novelty_claim="industry-relative value plus quality",
        falsification_criteria=["neutralized IC <= 0"],
        promote_if=["multi-window positive IC"],
        stop_if=["coverage below threshold"],
        source_agent="planner",
        budget_bucket="mechanism_validation",
    )

    loaded = ResearchExperimentProposal.from_dict(proposal.to_dict())

    assert loaded == proposal
    assert loaded.to_json().startswith("{")


def test_research_experiment_proposal_requires_hypothesis_for_generated_candidates():
    proposal = ResearchExperimentProposal(
        proposal_id="p1",
        experiment_type="generated_candidate",
        factor_names=["gen_a"],
        expressions=["book_yield + roe"],
        mechanism_id=None,
        hypothesis="",
        expected_information_gain=[],
        required_data_fields=[],
        start_date="2024-01-01",
        end_date="2024-12-31",
        universe_limit=100,
        horizon=None,
        parent_candidates=[],
        novelty_claim=None,
        falsification_criteria=[],
        promote_if=[],
        stop_if=[],
        source_agent="candidate_generator",
        budget_bucket="pure_exploration",
    )

    assert "generated_candidate proposals require hypothesis" in proposal.validation_errors()


def test_proposal_from_workflow_task_wraps_legacy_task():
    task = {
        "task_type": "workflow",
        "payload": {
            "config_path": "configs/test.json",
            "output_dir": "artifacts/test",
            "goal": "validate window",
            "hypothesis": "candidate survives longer window",
            "expected_information_gain": ["window_stability_check"],
            "branch_id": "window_expansion",
            "promote_if": ["stable"],
            "stop_if": ["fails"],
        },
        "worker_note": "validation｜test",
    }

    proposal = proposal_from_workflow_task(task, config={"start_date": "2024-01-01", "end_date": "2024-12-31", "universe_limit": 100, "factors": [{"name": "value", "expression": "book_yield"}]})

    assert proposal.experiment_type == "workflow"
    assert proposal.factor_names == ["value"]
    assert proposal.expressions == ["book_yield"]
    assert proposal.hypothesis == "candidate survives longer window"
    assert proposal.budget_bucket == "robustness_validation"
