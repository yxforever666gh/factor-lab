import json
import subprocess

import pytest

from factor_lab.hermes_client import HermesClient, HermesRequest, validate_profile_config_inherits_main


def _request(tmp_path):
    briefing = tmp_path / "brief.json"
    response = tmp_path / "response.json"
    briefing.write_text("{}")
    return HermesRequest(
        request_id="req-1",
        profile_key="researcher",
        profile_name="factor-lab-researcher",
        session_name="factor-lab-researcher-main",
        toolsets=("file","terminal"),
        skills=("factor-lab",),
        briefing_path=briefing,
        response_path=response,
        timeout_seconds=3,
    )


def test_client_builds_profile_backed_command_from_main_model(tmp_path):
    req = _request(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: main-provider\n  default: main-model\n", encoding="utf-8")
    command = HermesClient(config_path=config).build_command(req, "prompt")
    assert command[:3] == ["hermes", "--profile", "factor-lab-researcher"]
    assert "--resume" in command
    assert "factor-lab-researcher-main" in command
    assert command[command.index("--provider") + 1] == "main-provider"
    assert command[command.index("--model") + 1] == "main-model"
    assert "--toolsets" in command
    assert "file,terminal" in command
    assert "--quiet" in command


def test_client_parses_json_stdout_and_writes_response(tmp_path, monkeypatch):
    req = _request(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: main-provider\n  default: main-model\n", encoding="utf-8")
    payload = {"request_id":"req-1","profile_key":"researcher","summary":"s","recommendation":"r","confidence":0.4,"risks":[],"next_actions":[]}
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout=json.dumps(payload), stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    result = HermesClient(config_path=config).run(req, "prompt")
    assert result.ok is True
    assert result.payload == payload
    assert json.loads(req.response_path.read_text())["request_id"] == "req-1"


def test_client_reports_invalid_json(tmp_path, monkeypatch):
    req = _request(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: main-provider\n  default: main-model\n", encoding="utf-8")
    def fake_run(*args, **kwargs):
        return subprocess.CompletedProcess(args=args[0], returncode=0, stdout="not json", stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    result = HermesClient(config_path=config).run(req, "prompt")
    assert result.ok is False
    assert "unable_to_parse_json" in (result.error or "")


def test_changing_main_model_changes_profile_command(tmp_path):
    req = _request(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: provider-a\n  default: model-a\n", encoding="utf-8")
    client = HermesClient(config_path=config)
    first = client.build_command(req, "prompt")
    config.write_text("model:\n  provider: provider-b\n  default: model-b\n", encoding="utf-8")
    second = client.build_command(req, "prompt")
    assert first[first.index("--provider") + 1] == "provider-a"
    assert first[first.index("--model") + 1] == "model-a"
    assert second[second.index("--provider") + 1] == "provider-b"
    assert second[second.index("--model") + 1] == "model-b"


def test_missing_main_model_fails_loudly(tmp_path):
    req = _request(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: ''\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="main model"):
        HermesClient(config_path=config).build_command(req, "prompt")


def test_rejects_profile_local_model_settings():
    errors = validate_profile_config_inherits_main({
        "profiles": {
            "factor-lab-researcher": {"workdir": "/home/admin/factor-lab", "model": "pinned", "provider": "pinned"},
            "factor-lab-reviewer": {"toolsets": ["file"]},
        }
    })
    assert "factor-lab-researcher:model_must_not_be_profile_local" in errors
    assert "factor-lab-researcher:provider_must_not_be_profile_local" in errors


def test_client_retries_fresh_session_when_named_resume_missing(tmp_path, monkeypatch):
    req = _request(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("model:\n  provider: main-provider\n  default: main-model\n", encoding="utf-8")
    payload = {"request_id":"req-1","profile_key":"researcher","summary":"s","recommendation":"r","confidence":0.4,"risks":[],"next_actions":[]}
    calls = []
    def fake_run(command, **kwargs):
        calls.append(command)
        if len(calls) == 1:
            return subprocess.CompletedProcess(args=command, returncode=1, stdout="Session not found: factor-lab-researcher-main", stderr="")
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="session_id: abc\n" + json.dumps(payload), stderr="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    result = HermesClient(config_path=config).run(req, "prompt")
    assert result.ok is True
    assert "--resume" in calls[0]
    assert "--resume" not in calls[1]
