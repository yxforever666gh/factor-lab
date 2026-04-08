import json
from pathlib import Path

from factor_lab.candidate_generator import build_candidate_generation_plan
from factor_lab.research_strategy import update_research_memory_from_task_result


def test_candidate_generation_limits_operator_count_per_pair(tmp_path):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 1},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 1},
        ],
    }
    memory = {"execution_feedback": []}
    sp = tmp_path / "snapshot.json"
    mp = tmp_path / "memory.json"
    op = tmp_path / "plan.json"
    sp.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    mp.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    payload = build_candidate_generation_plan(sp, mp, op)

    pair_proposals = [row for row in payload["proposals"] if row["base_factors"] == ["mom_20", "book_yield"]]
    assert len(pair_proposals) <= 2


def test_generated_candidate_outcome_contains_increment_check(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    output_dir = tmp_path / "generated_candidate_run"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "candidate_pool.json").write_text("[]", encoding="utf-8")
    (output_dir / "factor_graveyard.json").write_text('[{"factor_name":"x"}]', encoding="utf-8")
    (output_dir / "factor_scores.json").write_text(
        json.dumps(
            [
                {"factor_name": "mom_20", "score": 1.0},
                {"factor_name": "book_yield", "score": 0.5},
                {"factor_name": "gen__combine_add__mom_20__book_yield", "score": 0.4},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    task = {
        "task_id": "task-generated-3",
        "task_type": "workflow",
        "fingerprint": "fp-generated-3",
        "payload": {
            "branch_id": "gen__combine_add__mom_20__book_yield",
            "source": "candidate_generation",
            "output_dir": str(output_dir),
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

    assert row["increment_check"]["best_parent_score"] == 1.0
    assert row["increment_check"]["improved_vs_parent"] is False
