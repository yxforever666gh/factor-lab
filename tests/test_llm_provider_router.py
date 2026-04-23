import importlib
import json
from pathlib import Path

import factor_lab.llm_provider_router as llm_provider_router
from factor_lab.llm_provider_router import DecisionProviderRouter


PLANNER_CONTEXT = {
    "context_id": "ctx-planner-1",
    "inputs": {
        "research_flow_state": {"state": "ready"},
        "failure_state": {},
        "queue_budget": {"validation": 2, "exploration": 1},
        "research_learning": {},
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "branch_selected_families": ["stable_candidate_validation"],
        "knowledge_gain_counter": {},
        "open_questions": ["verify medium horizon"],
        "candidate_pool_tasks": [],
        "candidate_pool_suppressed": [],
        "candidate_hypothesis_cards": [],
    },
}

FAILURE_CONTEXT = {
    "context_id": "ctx-failure-1",
    "inputs": {
        "recent_failed_or_risky_tasks": [{"task_id": "t1", "status": "failed", "task_type": "workflow", "worker_note": "rss guard hit"}],
        "llm_diagnostics": {"warnings": ["novelty_low"]},
        "research_flow_state": {"state": "recovering"},
        "latest_graveyard": ["value_ep"],
        "knowledge_gain_counter": {"no_significant_information_gain": 1},
    },
}


def test_router_uses_heuristic_provider_with_metadata():
    router = DecisionProviderRouter(provider="heuristic")

    payload = router.generate("planner", PLANNER_CONTEXT)

    assert payload["mode"] in {"validate", "recover", "converge"}
    assert payload["decision_metadata"]["source"] == "heuristic"
    assert payload["decision_metadata"]["effective_source"] == "heuristic"
    assert payload["decision_metadata"]["configured_provider"] == "heuristic"
    assert payload["decision_metadata"]["degraded_to_heuristic"] is False
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["decision_metadata"]["decision_context_id"] == "ctx-planner-1"



def test_router_falls_back_from_real_provider_to_heuristic(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_API_KEY", raising=False)
    router = DecisionProviderRouter(provider="real_llm")

    payload = router.generate("failure_analyst", FAILURE_CONTEXT)

    assert payload["decision_metadata"]["source"] == "heuristic"
    assert payload["decision_metadata"]["effective_source"] == "heuristic"
    assert payload["decision_metadata"]["configured_provider"] == "real_llm"
    assert payload["decision_metadata"]["degraded_to_heuristic"] is True
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["decision_metadata"]["fallback_reason"] == "provider_error:real_llm"



def test_router_healthcheck_reports_missing_real_provider(monkeypatch, tmp_path):
    monkeypatch.delenv("FACTOR_LAB_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_API_KEY", raising=False)
    monkeypatch.delenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", raising=False)
    monkeypatch.delenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", raising=False)
    router = DecisionProviderRouter(provider="auto")

    payload = router.healthcheck(output_path=tmp_path / "health.json")

    assert payload["real_provider_configured"] is False
    assert payload["recommended_effective_source"] == "heuristic"
    assert payload["effective_source"] == "heuristic"
    assert payload["probe"]["attempted"] is False



def test_router_uses_openclaw_agent_provider(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_SESSION_PREFIX", "factor-lab-decision")

    def fake_run(command, cwd, capture_output, text, timeout, env):
        assert "factor-lab-planner" in command
        payload = {
            "payloads": [{"text": json.dumps({
                "schema_version": "factor_lab.planner_agent_response.v1",
                "mode": "validate",
                "task_mix": {"baseline": 1, "validation": 2, "exploration": 0},
                "priority_families": ["stable_candidate_validation"],
                "suppress_families": [],
                "recommended_actions": [],
            }, ensure_ascii=False)}],
            "meta": {"agentMeta": {"provider": "codex-for-me", "model": "gpt-5.4"}},
        }
        return type("Completed", (), {"returncode": 0, "stdout": json.dumps(payload, ensure_ascii=False), "stderr": ""})()

    monkeypatch.setattr("factor_lab.llm_provider_router.subprocess.run", fake_run)
    router = DecisionProviderRouter(provider="openclaw_agent")

    payload = router.generate("planner", PLANNER_CONTEXT)

    assert payload["decision_metadata"]["source"] == "openclaw_agent"
    assert payload["decision_metadata"]["effective_source"] == "openclaw_agent"
    assert payload["decision_metadata"]["session_mode"] == "persistent"
    assert payload["decision_metadata"]["session_id"] == "factor-lab-decision-planner"
    assert payload["decision_metadata"]["request_scope_id"] == "ctx-planner-1"
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["openclaw_agent_meta"]["agent_id"] == "factor-lab-planner"



def test_router_uses_openclaw_gateway_provider(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_SESSION_PREFIX", "factor-lab-decision")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

    class FakeResponse:
        def __init__(self, payload):
            self.status = 200
            self._payload = payload

        def read(self):
            return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(req, timeout):
        headers = {key.lower(): value for key, value in req.header_items()}
        assert req.full_url == "http://127.0.0.1:18789/v1/chat/completions"
        assert headers["x-openclaw-session-key"] == "factor-lab-decision-planner"
        request_body = json.loads(req.data.decode("utf-8"))
        assert request_body["model"] == "openclaw/factor-lab-planner"
        payload = {
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "openclaw/factor-lab-planner",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": json.dumps(
                            {
                                "schema_version": "factor_lab.planner_agent_response.v1",
                                "mode": "validate",
                                "task_mix": {"baseline": 1, "validation": 2, "exploration": 0},
                                "priority_families": ["stable_candidate_validation"],
                                "suppress_families": [],
                                "recommended_actions": [],
                            },
                            ensure_ascii=False,
                        ),
                    },
                    "finish_reason": "stop",
                }
            ],
        }
        return FakeResponse(payload)

    monkeypatch.setattr("factor_lab.llm_provider_router.urllib.request.urlopen", fake_urlopen)
    router = DecisionProviderRouter(provider="openclaw_gateway")

    payload = router.generate("planner", PLANNER_CONTEXT)

    assert payload["decision_metadata"]["source"] == "openclaw_gateway"
    assert payload["decision_metadata"]["effective_source"] == "openclaw_gateway"
    assert payload["decision_metadata"]["session_mode"] == "persistent"
    assert payload["decision_metadata"]["session_id"] == "factor-lab-decision-planner"
    assert payload["decision_metadata"]["request_scope_id"] == "ctx-planner-1"
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["openclaw_gateway_meta"]["agent_id"] == "factor-lab-planner"
    assert payload["openclaw_gateway_meta"]["session_id"] == "factor-lab-decision-planner"
    assert payload["openclaw_gateway_meta"]["request_scope_id"] == "ctx-planner-1"



def test_router_healthcheck_probes_openclaw_gateway(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

    class FakeResponse:
        def __init__(self, payload=b"ok"):
            self.status = 200
            self._payload = payload

        def read(self):
            if isinstance(self._payload, bytes):
                return self._payload
            return json.dumps(self._payload, ensure_ascii=False).encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(req, timeout):
        headers = {key.lower(): value for key, value in req.header_items()}
        assert req.full_url == "http://127.0.0.1:18789/readyz"
        assert req.get_method() == "GET"
        assert "x-openclaw-session-key" not in headers
        return FakeResponse()

    monkeypatch.setattr("factor_lab.llm_provider_router.urllib.request.urlopen", fake_urlopen)
    router = DecisionProviderRouter(provider="openclaw_gateway")

    payload = router.healthcheck(output_path=tmp_path / "health.json")

    assert payload["openclaw_gateway_configured"] is True
    assert payload["normalized_provider"] == "legacy_openclaw_gateway"
    assert payload["provider_class"] == "legacy"
    assert payload["recommended_effective_source"] == "legacy_openclaw_gateway"
    assert payload["effective_source"] == "legacy_openclaw_gateway"
    assert payload["probe"]["attempted"] is True
    assert payload["probe"]["ok"] is True


def test_router_healthcheck_can_skip_gateway_probe(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")
    router = DecisionProviderRouter(provider="openclaw_gateway")

    payload = router.healthcheck(output_path=tmp_path / "health.json", probe=False)

    assert payload["normalized_provider"] == "legacy_openclaw_gateway"
    assert payload["provider_class"] == "legacy"
    assert payload["recommended_effective_source"] == "legacy_openclaw_gateway"
    assert payload["probe"]["attempted"] is False
    assert payload["probe"]["skipped"] is True
    assert payload["probe"]["error"] == "probe_skipped"


def test_openclaw_session_mode_defaults_to_ephemeral(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_OPENCLAW_SESSION_MODE", raising=False)
    router = DecisionProviderRouter(provider="openclaw_gateway")

    assert router._openclaw_session_id("planner", context=PLANNER_CONTEXT) == "factor-lab-decision-planner-ctx-planner-1"
    assert router._openclaw_session_id("failure_analyst", context=FAILURE_CONTEXT) == "factor-lab-decision-failure-analyst-ctx-failure-1"


def test_openclaw_persistent_session_mode_uses_fixed_session(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_SESSION_MODE", "persistent")
    router = DecisionProviderRouter(provider="openclaw_gateway")

    assert router._openclaw_session_id("planner", context=PLANNER_CONTEXT) == "factor-lab-decision-planner"
    assert router._openclaw_session_id("failure_analyst", context=FAILURE_CONTEXT) == "factor-lab-decision-failure-analyst"


def test_openclaw_ephemeral_session_mode_still_supported(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_SESSION_MODE", "ephemeral")
    router = DecisionProviderRouter(provider="openclaw_gateway")

    assert router._openclaw_session_id("planner", context=PLANNER_CONTEXT) == "factor-lab-decision-planner-ctx-planner-1"
    assert router._openclaw_session_id("failure_analyst", context=FAILURE_CONTEXT) == "factor-lab-decision-failure-analyst-ctx-failure-1"


def test_router_loads_env_from_configurable_factor_lab_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "factor-lab.env"
    env_file.write_text(
        "FACTOR_LAB_DECISION_PROVIDER=real_llm\n"
        "FACTOR_LAB_LLM_MODEL=router-test-model\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FACTOR_LAB_ENV_FILE", str(env_file))
    monkeypatch.delenv("FACTOR_LAB_DECISION_PROVIDER", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_MODEL", raising=False)

    reloaded = importlib.reload(llm_provider_router)
    try:
        router = reloaded.DecisionProviderRouter()
        assert router.provider == "real_llm"
        assert router.model == "router-test-model"
    finally:
        importlib.reload(reloaded)


def test_router_healthcheck_writes_to_configurable_artifacts_dir(monkeypatch, tmp_path):
    artifacts_dir = tmp_path / "custom-artifacts"
    monkeypatch.setenv("FACTOR_LAB_ARTIFACTS_DIR", str(artifacts_dir))
    router = DecisionProviderRouter(provider="heuristic")

    payload = router.healthcheck()

    expected = artifacts_dir / "llm_provider_health.json"
    assert expected.exists()
    written = json.loads(expected.read_text(encoding="utf-8"))
    assert written["effective_source"] == payload["effective_source"]


def test_router_healthcheck_reports_normalized_legacy_provider(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

    router = DecisionProviderRouter(provider="openclaw_gateway")
    payload = router.healthcheck(output_path=tmp_path / "health.json", probe=False)

    assert payload["configured_provider"] == "openclaw_gateway"
    assert payload["normalized_provider"] == "legacy_openclaw_gateway"
    assert payload["provider_class"] == "legacy"
    assert payload["recommended_effective_source"] == "legacy_openclaw_gateway"
    assert payload["effective_source"] == "legacy_openclaw_gateway"


def test_router_auto_prefers_real_llm_before_legacy_openclaw(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_LLM_BASE_URL", "https://example.test/v1")
    monkeypatch.setenv("FACTOR_LAB_LLM_API_KEY", "secret")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")
    monkeypatch.setattr(Path, "home", lambda: Path("/nonexistent-home-for-router-test"))

    router = DecisionProviderRouter(provider="auto")

    assert router._provider_chain() == ["real_llm", "heuristic", "mock"]


def test_router_legacy_aliases_map_to_normalized_providers(monkeypatch):
    router = DecisionProviderRouter(provider="openclaw_gateway")
    assert router._normalized_provider_name() == "legacy_openclaw_gateway"
    assert router._provider_class() == "legacy"

    router = DecisionProviderRouter(provider="openclaw_agent")
    assert router._normalized_provider_name() == "legacy_openclaw_agent"
    assert router._provider_class() == "legacy"

    router = DecisionProviderRouter(provider="openclaw_cli")
    assert router._normalized_provider_name() == "legacy_openclaw_agent"
    assert router._provider_class() == "legacy"

    router = DecisionProviderRouter(provider="openclaw_internal")
    assert router._normalized_provider_name() == "legacy_openclaw_agent"
    assert router._provider_class() == "legacy"

    router = DecisionProviderRouter(provider="real_llm")
    assert router._normalized_provider_name() == "real_llm"
    assert router._provider_class() == "primary"

    router = DecisionProviderRouter(provider="heuristic")
    assert router._normalized_provider_name() == "heuristic"
    assert router._provider_class() == "local"


def test_router_healthcheck_reports_normalized_fields_for_all_providers(monkeypatch, tmp_path):
    # Test openclaw_gateway
    planner_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".openclaw" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_PLANNER_AGENT", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

    router = DecisionProviderRouter(provider="openclaw_gateway")
    payload = router.healthcheck(output_path=tmp_path / "health.json", probe=False)

    assert payload["configured_provider"] == "openclaw_gateway"
    assert payload["normalized_provider"] == "legacy_openclaw_gateway"
    assert payload["provider_class"] == "legacy"

    # Test openclaw_agent
    router = DecisionProviderRouter(provider="openclaw_agent")
    payload = router.healthcheck(output_path=tmp_path / "health2.json", probe=False)

    assert payload["configured_provider"] == "openclaw_agent"
    assert payload["normalized_provider"] == "legacy_openclaw_agent"
    assert payload["provider_class"] == "legacy"

    # Test real_llm
    monkeypatch.setenv("FACTOR_LAB_LLM_BASE_URL", "https://example.test/v1")
    monkeypatch.setenv("FACTOR_LAB_LLM_API_KEY", "secret")
    router = DecisionProviderRouter(provider="real_llm")
    payload = router.healthcheck(output_path=tmp_path / "health3.json", probe=False)

    assert payload["configured_provider"] == "real_llm"
    assert payload["normalized_provider"] == "real_llm"
    assert payload["provider_class"] == "primary"

    # Test heuristic
    router = DecisionProviderRouter(provider="heuristic")
    payload = router.healthcheck(output_path=tmp_path / "health4.json", probe=False)

    assert payload["configured_provider"] == "heuristic"
    assert payload["normalized_provider"] == "heuristic"
    assert payload["provider_class"] == "local"
