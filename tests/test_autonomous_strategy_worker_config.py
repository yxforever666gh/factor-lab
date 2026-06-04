from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER_CONFIG = ROOT / "configs" / "autonomous_strategy_workers.json"
PLAN = ROOT / "docs" / "plans" / "2026-06-01-autonomous-strategy-lab-implementation-proposal.md"


FORBIDDEN_WORKER_KEYS = {
    "agent",
    "agent_id",
    "agent_role",
    "legacy_agent_id",
    "model",
    "provider",
    "base_url",
    "api_key",
    "profile",
    "llm_fallback_order",
}


def test_worker_config_replaces_agent_settings_with_hermes_worker_specs():
    payload = json.loads(WORKER_CONFIG.read_text(encoding="utf-8"))

    assert payload["schema_version"] == 1
    assert payload["runtime_binding"] == "hermes_cli_one_shot"
    assert payload["factor_lab_authority"] == "deterministic_gate"
    assert payload["model_provider_policy"] == "inherit_current_hermes_session"
    assert payload["preferred_invocation"]["command"] == "hermes chat"
    assert payload["preferred_invocation"]["resume"] is False
    assert "--provider" in payload["preferred_invocation"]["forbidden_flags"]
    assert "--model" in payload["preferred_invocation"]["forbidden_flags"]
    assert payload["workers"]

    for worker in payload["workers"]:
        assert set(worker).isdisjoint(FORBIDDEN_WORKER_KEYS)
        assert worker["worker_key"].startswith("factor_lab_")
        assert worker["output_artifact_namespace"]
        assert worker["forbidden_actions"]
        assert "queue_write" in worker["forbidden_actions"]
        assert "provider_model_change" in worker["forbidden_actions"]
        assert worker["verification_after"]


def test_plan_states_agent_settings_are_replaced_and_verification_follows_key_steps():
    text = PLAN.read_text(encoding="utf-8")

    assert "Agent/profile/provider 设置迁移原则" in text
    assert "不再新增 Factor Lab 内部 agent/provider/model/profile 设置" in text
    assert "Hermes temporary worker specs" in text
    assert "Hermes CLI one-shot worker" in text
    assert "hermes chat -Q --source factor-lab-worker" in text
    assert "关键步骤后的验证设计" in text
    assert "每一个实现阶段后必须立刻运行对应验证" in text
