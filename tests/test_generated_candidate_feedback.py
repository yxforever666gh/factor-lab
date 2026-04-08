import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning
from factor_lab.research_strategy import update_research_memory_from_task_result


def test_candidate_generation_uses_observed_output_for_gain(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    output_dir = tmp_path / "generated_candidate_run"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "candidate_pool.json").write_text("[]", encoding="utf-8")
    (output_dir / "factor_graveyard.json").write_text('[{"factor_name":"x"}]', encoding="utf-8")

    task = {
        "task_id": "task-generated-2",
        "task_type": "workflow",
        "fingerprint": "fp-generated-2",
        "payload": {
            "branch_id": "gen__combine_add__mom_20__book_yield",
            "source": "candidate_generation",
            "output_dir": str(output_dir),
            "expected_information_gain": ["candidate_survival_check", "search_space_reduced"],
            "candidate_generation_context": {
                "candidate_id": "gen__combine_add__mom_20__book_yield",
                "operator": "combine_add",
                "base_factors": ["mom_20", "book_yield"],
                "source": "stable_plus_graveyard",
                "expected_information_gain": ["candidate_survival_check", "search_space_reduced"],
            },
        },
    }

    memory = update_research_memory_from_task_result(memory_path, task, status="finished", summary="workflow finished")
    row = memory["generated_candidate_outcomes"][-1]

    assert row["has_gain"] is False
    assert row["outcome_class"] == "high_value_failure"


def test_operator_stats_reflect_generated_candidate_feedback(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "generated_candidate_outcomes": [
                    {"operator": "combine_add", "has_gain": False, "outcome_class": "high_value_failure"}
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)
    assert learning["operator_stats"]["combine_add"]["recommended_action"] == "keep"



def test_operator_stats_downweight_repeated_high_value_failures_without_gains(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "generated_candidate_outcomes": [
                    {"operator": "combine_add", "has_gain": False, "outcome_class": "high_value_failure"},
                    {"operator": "combine_add", "has_gain": False, "outcome_class": "high_value_failure"},
                    {"operator": "combine_add", "has_gain": False, "outcome_class": "high_value_failure"}
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)
    assert learning["operator_stats"]["combine_add"]["recommended_action"] == "downweight"
