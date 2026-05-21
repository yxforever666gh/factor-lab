import json

from factor_lab.hermes_artifacts import read_hermes_response_artifact, write_hermes_response_artifact


def test_write_and_read_response_artifact(tmp_path):
    path = tmp_path / "responses" / "reviewer" / "req.json"
    payload = {"request_id":"req","profile_key":"reviewer","summary":"s","recommendation":"r","confidence":0.5,"risks":[],"next_actions":[]}
    result = write_hermes_response_artifact(path, payload, request_id="req", profile_key="reviewer")
    assert result == payload
    assert json.loads(path.read_text())["profile_key"] == "reviewer"
    assert read_hermes_response_artifact(path) == payload


def test_write_response_artifact_rejects_invalid_contract(tmp_path):
    path = tmp_path / "bad.json"
    try:
        write_hermes_response_artifact(path, {"request_id":"x"}, request_id="req", profile_key="reviewer")
    except ValueError as exc:
        assert "request_id_mismatch" in str(exc)
    else:
        raise AssertionError("invalid artifact should fail")
