import json

from scripts.inspect_harvest_v3_status import inspect_harvest_v3_status


def test_no_cycles_returns_empty_summary(tmp_path):
    out = inspect_harvest_v3_status(root=tmp_path, latest=5)
    assert out["latest_cycle_id"] is None
    assert out["cycles"] == []


def test_cycles_with_v3_artifacts_summary(tmp_path):
    c = tmp_path / "artifacts" / "harvest_agent" / "cycle_0001"
    c.mkdir(parents=True)
    (c / "mechanism_route.json").write_text(json.dumps({"mechanism_id":"industry_relative_value"}))
    (c / "oos_validation.json").write_text(json.dumps({"oos_class":"fail"}))
    (c / "research_decision.json").write_text(json.dumps({"decision":"risk_reduction_branch"}))
    (c / "route_state.json").write_text(json.dumps({"current_route_status":"watch"}))
    out = inspect_harvest_v3_status(root=tmp_path, latest=5)
    assert out["latest_cycle_id"] == "cycle_0001"
    assert out["cycles"][0]["research_decision"] == "risk_reduction_branch"
    assert (tmp_path / "artifacts" / "harvest_agent" / "v3_status.json").exists()
