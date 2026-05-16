import json

from factor_lab.workflow_admission_adapter import enforce_workflow_admission


def _task_with_config(tmp_path, portfolio_construction):
    cfg = {
        "route_id": "value_quality_no_distress",
        "mechanism_id": "value_quality_no_distress",
        "required_data_fields": ["book_yield", "roe"],
        "factors": [{"name": "x", "expression": "book_yield + roe"}],
        "portfolio_construction": portfolio_construction,
    }
    path = tmp_path / "bucket.json"
    path.write_text(json.dumps(cfg))
    return {"task_type": "workflow", "payload": {"config_path": str(path), "output_dir": str(tmp_path / "out")}}


def test_workflow_admission_allows_bucket_aware_value_route(tmp_path):
    result = enforce_workflow_admission(_task_with_config(tmp_path, {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0}))

    assert result["decision"] == "allow"


def test_workflow_admission_blocks_invalid_bucket_aware_config(tmp_path):
    result = enforce_workflow_admission(_task_with_config(tmp_path, {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 5, "short_quantile": 0}))

    assert result["decision"] == "block"
    assert "invalid_portfolio_construction" in result["reasons"]


def test_workflow_admission_blocks_bucket_aware_missing_required_fields(tmp_path):
    task = _task_with_config(tmp_path, {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0})
    cfg_path = task["payload"]["config_path"]
    cfg = json.loads(open(cfg_path).read())
    cfg["required_data_fields"] = ["book_yield", "unsupported_field_for_preflight"]
    open(cfg_path, "w").write(json.dumps(cfg))

    result = enforce_workflow_admission(task)

    assert result["decision"] == "block"
    assert "bucket_aware_coverage_preflight_failed" in result["reasons"]
