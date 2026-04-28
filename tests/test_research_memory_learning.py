import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import update_research_memory_from_task_result


def test_update_research_memory_classifies_high_value_failure(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    task = {
        "task_id": "task-1",
        "task_type": "diagnostic",
        "fingerprint": "fp-1",
        "payload": {
            "branch_id": "graveyard_diagnosis_level_1",
            "goal": "diagnose graveyard cause",
            "hypothesis": "shared structural failure exists",
            "expected_information_gain": ["repeated_graveyard_confirmed"],
            "focus_factors": ["mom_20"],
        },
    }

    memory = update_research_memory_from_task_result(
        memory_path,
        task,
        status="finished",
        summary="knowledge_gain=repeated_graveyard_confirmed",
    )

    feedback = memory["execution_feedback"][-1]
    assert feedback["outcome_class"] in {"high_value_success", "useful_success", "high_value_failure"}
    assert feedback["epistemic_value"] in {"high", "medium"}


def test_research_learning_does_not_downweight_high_value_failure_only(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    payload = {
        "updated_at_utc": "2026-04-01T00:00:00+00:00",
        "execution_feedback": [
            {
                "branch_id": "graveyard_diagnosis_level_1",
                "has_gain": False,
                "outcome_class": "high_value_failure",
                "epistemic_value": "high",
            }
        ],
    }
    memory_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    learning = build_research_learning(memory_path)
    family = learning["families"]["graveyard_diagnosis"]

    assert family["recent_high_value_failure"] == 1
    assert family["recommended_action"] == "keep"
