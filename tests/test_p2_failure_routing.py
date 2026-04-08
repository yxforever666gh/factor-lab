import json
from pathlib import Path

from factor_lab.candidate_generator import build_candidate_generation_plan


def test_high_value_failure_generated_outcome_seeds_next_generation(tmp_path):
    snapshot = {
        "stable_candidates": [],
        "latest_graveyard": [],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 1},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 1},
        ],
    }
    memory = {
        "execution_feedback": [],
        "generated_candidate_outcomes": [
            {
                "base_factors": ["mom_20", "book_yield"],
                "target_family": "momentum",
                "operator": "combine_add",
                "outcome_class": "high_value_failure",
                "source": "stable_plus_graveyard",
            }
        ],
    }
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)
    pairs = [tuple(row["base_factors"]) for row in payload["proposals"]]
    operators = [row["operator"] for row in payload["proposals"]]

    assert ("mom_20", "book_yield") in pairs
    assert "combine_add" not in operators
