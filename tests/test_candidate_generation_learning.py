import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import update_research_memory_from_task_result


def test_generated_candidate_outcome_written_to_memory(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    task = {
        "task_id": "task-generated-1",
        "task_type": "workflow",
        "fingerprint": "fp-generated-1",
        "payload": {
            "branch_id": "gen__combine_add__mom_20__book_yield",
            "source": "candidate_generation",
            "goal": "validate_generated_candidate:gen__combine_add__mom_20__book_yield",
            "expected_information_gain": ["candidate_survival_check"],
            "candidate_generation_context": {
                "candidate_id": "gen__combine_add__mom_20__book_yield",
                "operator": "combine_add",
                "base_factors": ["mom_20", "book_yield"],
                "source": "stable_plus_graveyard",
                "expected_information_gain": ["candidate_survival_check"],
            },
        },
    }

    memory = update_research_memory_from_task_result(memory_path, task, status="finished", summary="knowledge_gain=candidate_survival_check")

    row = memory["generated_candidate_outcomes"][-1]
    assert row["candidate_id"] == "gen__combine_add__mom_20__book_yield"
    assert row["operator"] == "combine_add"


def test_research_learning_builds_operator_stats(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "generated_candidate_outcomes": [
                    {"operator": "combine_add", "has_gain": True, "outcome_class": "high_value_success"},
                    {"operator": "combine_sub", "has_gain": False, "outcome_class": "high_value_failure"},
                    {"operator": "combine_ratio", "has_gain": False, "outcome_class": "low_value_repeat"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)

    assert learning["operator_stats"]["combine_add"]["recommended_action"] == "upweight"
    assert learning["operator_stats"]["combine_sub"]["recommended_action"] == "keep"
    assert learning["operator_stats"]["combine_ratio"]["recommended_action"] == "downweight"
