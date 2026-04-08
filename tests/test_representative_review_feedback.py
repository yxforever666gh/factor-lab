import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import update_research_memory_from_task_result


def test_representative_review_written_to_memory(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    task = {
        "task_id": "task-representative-1",
        "task_type": "diagnostic",
        "fingerprint": "fp-representative-1",
        "payload": {
            "branch_id": "representative_candidate_competition",
            "diagnostic_type": "representative_candidate_competition_review",
            "focus_factors": ["mom_20"],
            "expected_information_gain": ["representative_candidate_confirmed"],
        },
    }

    memory = update_research_memory_from_task_result(memory_path, task, status="finished", summary="knowledge_gain=representative_candidate_confirmed")

    row = memory["representative_candidate_reviews"][-1]
    assert row["branch_id"] == "representative_candidate_competition"


def test_research_learning_builds_representative_stats(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "representative_candidate_reviews": [
                    {"has_gain": True, "outcome_class": "high_value_success"}
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)
    assert learning["representative_candidate_stats"]["recommended_action"] == "upweight"
