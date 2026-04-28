from factor_lab.data_quality_decision_engine import build_data_quality_response
from factor_lab.llm_schema_validation import validate_decision_payload
from factor_lab.reviewer_decision_engine import build_reviewer_response


def test_reviewer_response_flags_duplicate_like_candidate():
    context = {
        "context_id": "ctx-review",
        "inputs": {
            "promotion_scorecard": {
                "rows": [
                    {
                        "factor_name": "dup_factor",
                        "duplicate_peer_count": 2,
                        "quality_classification": "duplicate-suppress",
                        "quality_scores": {"incremental_value": 2},
                    }
                ]
            }
        },
    }

    payload = build_reviewer_response(context, source_label="heuristic")

    assert payload["schema_version"] == "factor_lab.reviewer_agent_response.v1"
    assert payload["candidate_reviews"][0]["candidate_name"] == "dup_factor"
    assert payload["candidate_reviews"][0]["quality_verdict"] in {"suppress", "deprioritize"}


def test_data_quality_response_flags_missing_tushare_token():
    context = {
        "context_id": "ctx-data",
        "inputs": {
            "latest_run": {"dataset_rows": 0, "status": "failed"},
            "last_error": "RuntimeError: Missing required environment variable: TUSHARE_TOKEN",
        },
    }

    payload = build_data_quality_response(context, source_label="heuristic")

    assert payload["schema_version"] == "factor_lab.data_quality_agent_response.v1"
    assert payload["dataset_health"]["coverage_status"] == "empty"
    assert payload["dataset_health"]["token_status"] == "missing"
    assert payload["should_pause_research"] is True


def test_validate_reviewer_and_data_quality_payloads():
    reviewer_payload = build_reviewer_response({"context_id": "x", "inputs": {}})
    data_payload = build_data_quality_response({"context_id": "y", "inputs": {"latest_run": {"dataset_rows": 1}}})

    assert validate_decision_payload("reviewer", reviewer_payload) == []
    assert validate_decision_payload("data_quality", data_payload) == []
