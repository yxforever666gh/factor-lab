import json
from pathlib import Path

from factor_lab.workflow_admission_adapter import admission_input_from_task, enforce_workflow_admission


def test_admission_input_from_task_loads_config_metadata(tmp_path):
    cfg = {
        "route_id": "value_quality_no_distress",
        "mechanism_id": "value_quality_no_distress",
        "required_data_fields": ["book_yield", "roe"],
        "factors": [{"name": "x", "expression": "book_yield + roe"}],
    }
    path = tmp_path / "cfg.json"
    path.write_text(json.dumps(cfg))

    task = {"task_type": "workflow", "payload": {"config_path": str(path), "output_dir": "out"}}
    admission_input = admission_input_from_task(task)

    assert admission_input["payload"]["mechanism_id"] == "value_quality_no_distress"
    assert admission_input["payload"]["route_id"] == "value_quality_no_distress"
    assert admission_input["payload"]["factors"][0]["name"] == "x"


def test_enforce_workflow_admission_blocks_unmechanized_workflow():
    result = enforce_workflow_admission({"task_type": "workflow", "payload": {"factors": [{"name": "x", "expression": "book_yield"}]}})

    assert result["decision"] == "block"
    assert "missing_mechanism_id" in result["reasons"]


def test_enforce_workflow_admission_allows_non_workflow_task():
    result = enforce_workflow_admission({"task_type": "diagnostic", "payload": {"diagnostic_type": "x"}})

    assert result["decision"] == "allow"
    assert result["reasons"] == ["non_workflow_task"]


def test_enforce_workflow_admission_malformed_config_blocks(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("not json")
    result = enforce_workflow_admission({"task_type": "workflow", "payload": {"config_path": str(path)}})

    assert result["decision"] == "block"
    assert "config_load_failed" in result["reasons"]
