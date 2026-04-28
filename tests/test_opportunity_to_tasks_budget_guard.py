from factor_lab.opportunity_to_tasks import map_opportunity_to_task


def test_budget_risky_probe_without_targets_becomes_diagnostic():
    task = map_opportunity_to_task(
        {
            "opportunity_id": "opp-risky-probe",
            "opportunity_type": "probe",
            "question": "Is this risky probe worth it?",
            "hypothesis": "maybe",
            "target_family": "graveyard_diagnosis",
            "target_candidates": [],
            "execution_mode": "cheap_screen",
            "expected_knowledge_gain": ["exploration_candidate_survived"],
        }
    )

    assert task is not None
    assert task["task_type"] == "diagnostic"
    assert task["payload"]["diagnostic_type"] == "opportunity_probe_budget_guard"


def test_probe_with_targets_still_maps_to_generated_batch():
    task = map_opportunity_to_task(
        {
            "opportunity_id": "opp-safe-probe",
            "opportunity_type": "probe",
            "question": "Is this probe worth it?",
            "hypothesis": "maybe",
            "target_family": "exploration",
            "target_candidates": ["mom_20", "mom_plus_value"],
            "execution_mode": "cheap_screen",
            "expected_knowledge_gain": ["exploration_candidate_survived"],
        }
    )

    assert task is not None
    assert task["task_type"] == "generated_batch"
