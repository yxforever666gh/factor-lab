import json

from factor_lab.harvest_research_lessons import build_harvest_research_lessons, write_harvest_research_lessons


def test_lessons_summarize_repeated_blockers(tmp_path):
    c = tmp_path / "artifacts" / "harvest_agent" / "cycle_0001"
    c.mkdir(parents=True)
    (c / "mechanism_route.json").write_text(json.dumps({"mechanism_id":"industry_relative_value"}))
    (c / "failure_attribution.json").write_text(json.dumps({"primary_blockers":["zero_cost_only_best"]}))
    (c / "data_request.json").write_text(json.dumps({"missing_required_fields":[],"recommended_data":["debt_to_asset"]}))
    out = build_harvest_research_lessons(tmp_path)
    assert "zero_cost_only_best" in out["blockers"]
    assert "debt_to_asset" in out["data_requests"]


def test_write_lessons_outputs_knowledge_files(tmp_path):
    out = write_harvest_research_lessons(tmp_path)
    assert (tmp_path / "knowledge" / "harvest_research_lessons.md").exists()
    assert (tmp_path / "knowledge" / "harvest_route_state.json").exists()
    assert out["written"] is True
