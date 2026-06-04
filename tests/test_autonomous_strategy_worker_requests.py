from __future__ import annotations

import json
from pathlib import Path

from factor_lab.autonomous_strategy_worker_requests import (
    build_worker_prompt,
    build_worker_requests,
    load_worker_config,
    write_worker_requests,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs" / "autonomous_strategy_workers.json"


def test_build_worker_requests_from_config_without_agent_provider_fields(tmp_path):
    config = load_worker_config(CONFIG)
    requests = build_worker_requests(
        config,
        run_id="strategy_lab_test",
        output_dir=tmp_path,
    )

    keys = {req["worker_key"] for req in requests}
    assert "factor_lab_diagnostician" in keys
    assert "factor_lab_mechanism_researcher" in keys
    assert "factor_lab_data_steward" in keys
    assert "factor_lab_reviewer" in keys

    for req in requests:
        assert req["schema_version"] == 1
        assert req["runtime_binding"] == "hermes_cli_one_shot"
        assert req["output_artifact_path"].startswith(str(tmp_path))
        assert "queue_write" in req["forbidden_actions"]
        assert "provider_model_change" in req["forbidden_actions"]
        assert req["verification_after"]
        serialized = json.dumps(req, ensure_ascii=False)
        assert "legacy_agent_id" not in serialized
        assert "llm_fallback_order" not in serialized
        assert '"model"' not in serialized
        assert '"provider"' not in serialized
        assert '"profile"' not in serialized


def test_worker_prompt_is_self_contained_and_names_output_artifact(tmp_path):
    config = load_worker_config(CONFIG)
    request = build_worker_requests(config, run_id="strategy_lab_test", output_dir=tmp_path)[0]

    prompt = build_worker_prompt(request)

    assert request["worker_key"] in prompt
    assert request["output_artifact_path"] in prompt
    assert "Input artifacts" in prompt
    assert "Forbidden actions" in prompt
    assert "Verification after completion" in prompt
    assert "Do not pass or change model/provider/profile settings" in prompt
    assert "Return only after writing the response artifact" in prompt


def test_write_worker_requests_creates_request_and_prompt_files(tmp_path):
    config = load_worker_config(CONFIG)
    requests = build_worker_requests(config, run_id="strategy_lab_test", output_dir=tmp_path)

    written = write_worker_requests(requests)

    assert written
    for item in written:
        assert item["request_path"].exists()
        assert item["prompt_path"].exists()
        payload = json.loads(item["request_path"].read_text(encoding="utf-8"))
        assert payload["worker_key"] == item["worker_key"]
        prompt = item["prompt_path"].read_text(encoding="utf-8")
        assert payload["output_artifact_path"] in prompt
