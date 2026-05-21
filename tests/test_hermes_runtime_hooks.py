import json

from factor_lab import hermes_runtime_hooks
from factor_lab.hermes_runtime_hooks import run_data_steward_profile, run_reviewer_profile


def _fake_run(self, request, prompt):
    payload = {
        "request_id": request.request_id,
        "profile_key": request.profile_key,
        "summary": "ok",
        "recommendation": "continue",
        "confidence": 0.8,
        "risks": [],
        "next_actions": [],
    }
    request.response_path.parent.mkdir(parents=True, exist_ok=True)
    request.response_path.write_text(json.dumps(payload), encoding="utf-8")
    from factor_lab.hermes_client import HermesResult
    return HermesResult(True, request.request_id, request.profile_key, request.profile_name, request.response_path, payload, json.dumps(payload), 0, None)


def test_run_data_steward_profile_writes_artifact(tmp_path, monkeypatch):
    monkeypatch.setattr(hermes_runtime_hooks.HermesClient, "run", _fake_run)
    output = tmp_path / "data_steward_review.json"
    payload = run_data_steward_profile({"context_id":"ctx","inputs":{"latest_run":{"dataset_rows":0}}}, output)
    assert output.exists()
    written = json.loads(output.read_text())
    assert written["profile_key"] == "data_steward"
    assert payload["profile_key"] == "data_steward"


def test_run_reviewer_profile_writes_artifact(tmp_path, monkeypatch):
    monkeypatch.setattr(hermes_runtime_hooks.HermesClient, "run", _fake_run)
    output = tmp_path / "reviewer.json"
    payload = run_reviewer_profile({"context_id":"ctx","inputs":{"promotion_scorecard":{"rows":[]}}}, output)
    assert output.exists()
    written = json.loads(output.read_text())
    assert written["profile_key"] == "reviewer"
    assert payload["profile_key"] == "reviewer"
