import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning


def test_research_learning_builds_family_operator_stats(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "generated_candidate_outcomes": [
                    {"operator": "combine_sub", "target_family": "momentum", "has_gain": True, "outcome_class": "high_value_success"},
                    {"operator": "combine_add", "target_family": "value", "has_gain": False, "outcome_class": "low_value_repeat"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)

    assert learning["family_operator_stats"]["momentum"]["combine_sub"]["recommended_action"] == "upweight"
    assert learning["family_operator_stats"]["value"]["combine_add"]["recommended_action"] == "downweight"
