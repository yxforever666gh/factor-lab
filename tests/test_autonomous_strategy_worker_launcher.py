from __future__ import annotations

from pathlib import Path

from factor_lab.autonomous_strategy_worker_launcher import build_hermes_worker_command
from factor_lab.autonomous_strategy_worker_requests import build_worker_requests, load_worker_config

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "autonomous_strategy_workers.json"


def test_build_hermes_worker_command_uses_one_shot_cli_without_forbidden_flags(tmp_path):
    config = load_worker_config(CONFIG)
    request = build_worker_requests(config, run_id="strategy_lab_test", output_dir=tmp_path)[0]
    prompt_path = tmp_path / f"{request['worker_key']}_prompt.txt"

    command = build_hermes_worker_command(request, prompt_path=prompt_path, config=config)

    assert command[:3] == ["hermes", "chat", "-Q"]
    assert "--source" in command
    assert "factor-lab-worker" in command
    assert "--skills" in command
    assert "factor-lab" in command
    assert "--toolsets" in command
    assert "--query" in command
    assert str(prompt_path) in command[-1]
    assert "--model" not in command
    assert "--provider" not in command
    assert "--resume" not in command
    assert "--continue" not in command
    assert "--yolo" not in command


def test_build_hermes_worker_command_uses_worker_toolsets(tmp_path):
    config = load_worker_config(CONFIG)
    requests = build_worker_requests(config, run_id="strategy_lab_test", output_dir=tmp_path)
    data_steward = next(req for req in requests if req["worker_key"] == "factor_lab_data_steward")

    command = build_hermes_worker_command(data_steward, prompt_path=tmp_path / "prompt.txt", config=config)

    toolsets = command[command.index("--toolsets") + 1]
    assert toolsets == "file,terminal,skills"
