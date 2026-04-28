import json

from factor_lab.agent_runtime_hooks import run_data_quality_review, run_reviewer_review


def test_run_data_quality_review_writes_artifact(tmp_path):
    output = tmp_path / "data_quality_review.json"
    payload = run_data_quality_review(
        context={
            "context_id": "ctx-data",
            "inputs": {
                "latest_run": {"dataset_rows": 0, "status": "failed"},
                "last_error": "Missing required environment variable: TUSHARE_TOKEN",
            },
        },
        output_path=output,
        provider="heuristic",
    )

    assert output.exists()
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["decision_metadata"]["agent_role"] == "data_quality"
    assert payload["dataset_health"]["coverage_status"] == "empty"


def test_run_reviewer_review_writes_artifact(tmp_path):
    output = tmp_path / "reviewer_review.json"
    payload = run_reviewer_review(
        context={
            "context_id": "ctx-review",
            "inputs": {"promotion_scorecard": {"rows": []}},
        },
        output_path=output,
        provider="heuristic",
    )

    assert output.exists()
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["decision_metadata"]["agent_role"] == "reviewer"
    assert payload["schema_version"] == "factor_lab.reviewer_agent_response.v1"
