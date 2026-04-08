import json
from pathlib import Path

from factor_lab.research_learning import build_research_learning


def test_research_learning_enters_diagnosis_heavy_mode_when_high_value_failures_accumulate(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    memory_path.write_text(
        json.dumps(
            {
                "updated_at_utc": "2026-04-01T00:00:00+00:00",
                "execution_feedback": [],
                "generated_candidate_outcomes": [
                    {"operator": "combine_add", "target_family": "momentum", "has_gain": False, "outcome_class": "high_value_failure"},
                    {"operator": "combine_sub", "target_family": "momentum", "has_gain": False, "outcome_class": "high_value_failure"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    learning = build_research_learning(memory_path)

    assert learning["research_mode"]["mode"] == "diagnosis_heavy"
