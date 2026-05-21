from factor_lab.hermes_contracts import validate_hermes_response, common_output_contract


def test_common_contract_requires_shared_fields():
    contract = common_output_contract("researcher", "req-1")
    for key in ["request_id", "profile_key", "summary", "recommendation", "confidence", "risks", "next_actions"]:
        assert key in contract


def test_validate_hermes_response_accepts_common_fields():
    payload = {
        "request_id": "req-1",
        "profile_key": "reviewer",
        "summary": "ok",
        "recommendation": "promote cautiously",
        "confidence": 0.6,
        "risks": [],
        "next_actions": [],
    }
    assert validate_hermes_response(payload, request_id="req-1", profile_key="reviewer") == []


def test_validate_hermes_response_reports_mismatch_and_bounds():
    errors = validate_hermes_response({"request_id":"x","profile_key":"reviewer","confidence":2}, request_id="req-1", profile_key="reviewer")
    assert "request_id_mismatch" in errors
    assert "missing_summary" in errors
    assert "confidence_out_of_range" in errors
