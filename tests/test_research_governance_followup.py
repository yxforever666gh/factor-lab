import json
from pathlib import Path

from factor_lab.candidate_generator import build_candidate_generation_plan
from factor_lab.research_candidate_pool import build_research_candidate_pool


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_candidate_generation_avoids_repeating_failed_operator_pair(tmp_path):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
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
                "operator": "combine_add",
                "outcome_class": "high_value_failure",
            }
        ],
    }
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)
    operators = [row["operator"] for row in payload["proposals"] if row["base_factors"] == ["mom_20", "book_yield"]]

    assert "combine_add" not in operators


def test_candidate_pool_exposes_quality_priority_summary():
    payload = build_research_candidate_pool(
        REPO_ROOT / "artifacts" / "research_planner_snapshot.json",
        REPO_ROOT / "artifacts" / "research_candidate_pool.json",
        REPO_ROOT / "artifacts" / "research_branch_plan.json",
    )
    quality_priority = (payload.get("summary") or {}).get("quality_priority") or {}

    assert "quality_priority_mode" in quality_priority
    assert "generated_candidate_budget" in quality_priority
    assert "reasons" in quality_priority
