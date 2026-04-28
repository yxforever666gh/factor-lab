import json

from factor_lab.candidate_generator import build_candidate_generation_plan



def test_candidate_generation_proposals_include_decision_and_novelty_metadata(tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "candidate_generation_plan.json"

    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}, {"factor_name": "book_yield"}],
        "latest_graveyard": ["earnings_yield"],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 1, "is_primary_candidate": True, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 1, "is_primary_candidate": True, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "earnings_yield", "family": "value", "relationship_count": 1, "is_primary_candidate": True, "acceptance_gate": {"status": "pass"}},
        ],
        "frontier_focus": {"robust_candidates": ["mom_20", "book_yield"], "summary": {}},
        "promotion_scorecard": {"rows": []},
        "relationship_summary": {},
        "family_summary": [],
        "research_learning": {"failure_question_cards": []},
    }
    memory = {"execution_feedback": [], "generated_candidate_outcomes": [], "candidate_generation_history": []}

    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["proposals"]
    assert all(row.get("decision_source") == "heuristic" for row in payload["proposals"])
    assert all(row.get("novelty_judgment_source") == "heuristic" for row in payload["proposals"])
    assert all(isinstance(row.get("mechanism_rationale"), str) and row.get("mechanism_rationale") for row in payload["proposals"])
