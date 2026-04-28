import importlib
import json
from pathlib import Path

import factor_lab.llm_provider_router as llm_provider_router
from factor_lab.llm_provider_router import DECISION_SCHEMA_HINTS, DecisionProviderRouter


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


def test_router_supports_reviewer_and_data_quality_schema_hints():
    router = DecisionProviderRouter(provider="heuristic")

    assert router._decision_schema_version("reviewer") == "factor_lab.reviewer_agent_response.v1"
    assert router._decision_schema_version("data_quality") == "factor_lab.data_quality_agent_response.v1"
    assert "candidate_reviews" in router._decision_schema_hint("reviewer")
    assert "data_quality_findings" in router._decision_schema_hint("data_quality")


def test_disabled_agent_role_blocks_generation(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        '[{"name":"reviewer","display_name":"Reviewer","enabled":false,"decision_types":["reviewer"],"purpose":"x","system_prompt":"x","llm_fallback_order":[],"timeout_seconds":1,"max_retries":0,"strict_schema":true}]',
    )
    router = DecisionProviderRouter(provider="heuristic")

    try:
        router.generate("reviewer", {"context_id": "ctx"})
    except RuntimeError as exc:
        assert "disabled" in str(exc)
    else:
        raise AssertionError("disabled reviewer role should block generation")


def test_openclaw_uses_role_specific_legacy_ids(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        '[{"name":"reviewer","display_name":"Reviewer","enabled":true,"decision_types":["reviewer"],"purpose":"x","system_prompt":"x","llm_fallback_order":[],"timeout_seconds":1,"max_retries":0,"strict_schema":true,"legacy_agent_id":"factor-lab-reviewer"}]',
    )
    router = DecisionProviderRouter(provider="heuristic")

    assert router._openclaw_agent_id("reviewer") == "factor-lab-reviewer"
    assert router._openclaw_session_base("data_quality").endswith("data-quality")


def test_openclaw_prompt_uses_role_specific_context_for_reviewer_and_data_quality(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)
    router = DecisionProviderRouter(provider="heuristic")

    reviewer_prompt = router._openclaw_prompt(
        "reviewer",
        {
            "context_id": "ctx-review",
            "inputs": {
                "latest_run": {"run_id": "r1"},
                "promotion_scorecard": {"rows": [{"factor_name": "alpha_x", "quality_classification": "candidate"}]},
                "candidate_pool": {"tasks": [{"branch_id": "branch-1"}]},
                "research_attribution": {"families": ["momentum"]},
            },
        },
    )
    data_quality_prompt = router._openclaw_prompt(
        "data_quality",
        {
            "context_id": "ctx-data",
            "inputs": {
                "task_type": "batch",
                "task_payload_summary": {"config_path": "cfg.json"},
                "latest_run": {"dataset_rows": 0},
                "last_error": "Missing required environment variable: TUSHARE_TOKEN",
            },
        },
    )

    assert "promotion_scorecard" in reviewer_prompt
    assert "alpha_x" in reviewer_prompt
    assert "candidate_pool" in reviewer_prompt
    assert "task_payload_summary" in data_quality_prompt
    assert "dataset_rows" in data_quality_prompt
    assert "Missing required environment variable" in data_quality_prompt


def test_router_generates_reviewer_and_data_quality_with_metadata(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_AGENT_ROLES_JSON", raising=False)
    router = DecisionProviderRouter(provider="heuristic")

    review = router.generate("reviewer", {"context_id": "ctx-review", "inputs": {"promotion_scorecard": {"rows": []}}})
    data = router.generate("data_quality", {"context_id": "ctx-data", "inputs": {"latest_run": {"dataset_rows": 0}}})

    assert review["decision_metadata"]["agent_role"] == "reviewer"
    assert data["decision_metadata"]["agent_role"] == "data_quality"
    assert review["schema_version"] == "factor_lab.reviewer_agent_response.v1"
    assert data["schema_version"] == "factor_lab.data_quality_agent_response.v1"


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


def test_router_attaches_agent_role_metadata_for_planner(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps([
            {
                "name": "planner",
                "enabled": True,
                "decision_types": ["planner"],
                "llm_fallback_order": ["nowcoding"],
                "legacy_agent_id": "factor-lab-planner",
            }
        ]),
    )
    router = DecisionProviderRouter(provider="heuristic")

    payload = router.generate("planner", {"context_id": "ctx-1", "inputs": {}})

    meta = payload["decision_metadata"]
    assert meta["agent_role"] == "planner"
    assert meta["agent_role_source"] == "configured"
    assert meta["agent_role_enabled"] is True
    assert meta["legacy_agent_id"] == "factor-lab-planner"



def test_router_falls_back_from_real_provider_to_heuristic(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_API_KEY", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_PROFILES_JSON", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
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
    monkeypatch.delenv("FACTOR_LAB_LLM_PROFILES_JSON", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_FALLBACK_ORDER", raising=False)
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
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_SESSION_MODE", "persistent")

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
    monkeypatch.setenv("FACTOR_LAB_OPENCLAW_SESSION_MODE", "persistent")
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


def test_real_llm_uses_configured_profile_fallback_order(monkeypatch):
    profiles = [
        {"name": "primary", "base_url": "https://primary.test/v1", "model": "primary-model", "api_key": "***", "enabled": True},
        {"name": "backup", "base_url": "https://backup.test/v1", "model": "backup-model", "api_key": "***", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "primary,backup")
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps([
            {
                "name": "planner",
                "enabled": True,
                "decision_types": ["planner"],
                "llm_fallback_order": ["primary", "backup"],
                "max_retries": 0,
            }
        ]),
    )
    attempts = []

    def fake_call(self, decision_type, context, profile, agent_role=None):
        attempts.append(profile["name"])
        if profile["name"] == "primary":
            raise RuntimeError("primary down")
        return {"decision_source": "real_llm", "schema_version": "x", "agent_name": "test"}

    monkeypatch.setattr(DecisionProviderRouter, "_call_real_llm_profile", fake_call)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm("planner", PLANNER_CONTEXT)

    assert attempts == ["primary", "backup"]
    assert payload["real_llm_profile"]["name"] == "backup"
    assert payload["real_llm_profile"]["fallback_attempts"] == ["primary", "backup"]
    assert payload["real_llm_profile"]["fallback_errors"] == {"primary": "primary down"}


def test_real_llm_uses_agent_role_specific_fallback_order(monkeypatch):
    profiles = [
        {"name": "global-first", "base_url": "https://global.test/v1", "model": "global", "api_key": "***", "enabled": True},
        {"name": "role-first", "base_url": "https://role.test/v1", "model": "role", "api_key": "***", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "global-first,role-first")
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps([
            {
                "name": "planner",
                "enabled": True,
                "decision_types": ["planner"],
                "system_prompt": "planner role prompt",
                "llm_fallback_order": ["role-first", "global-first"],
                "max_retries": 0,
            }
        ]),
    )
    attempts = []

    def fake_call(self, decision_type, context, profile, agent_role=None):
        attempts.append(profile["name"])
        assert agent_role is not None
        assert agent_role.name == "planner"
        assert "planner role prompt" in self._agent_system_prompt(decision_type, agent_role)
        return {"decision_source": "real_llm", "schema_version": "x", "agent_name": "test"}

    monkeypatch.setattr(DecisionProviderRouter, "_call_real_llm_profile", fake_call)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm("planner", PLANNER_CONTEXT)

    assert attempts == ["role-first"]
    assert payload["real_llm_profile"]["name"] == "role-first"


def test_agent_role_max_retries_retry_profile_before_fallback(monkeypatch):
    profiles = [
        {"name": "primary", "base_url": "https://primary.test/v1", "model": "primary", "api_key": "***", "enabled": True},
        {"name": "backup", "base_url": "https://backup.test/v1", "model": "backup", "api_key": "***", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps([
            {
                "name": "planner",
                "enabled": True,
                "decision_types": ["planner"],
                "llm_fallback_order": ["primary", "backup"],
                "max_retries": 1,
            }
        ]),
    )
    attempts = []

    def fake_call(self, decision_type, context, profile, agent_role=None):
        attempts.append(profile["name"])
        if len(attempts) < 2:
            raise RuntimeError("temporary down")
        return {"decision_source": "real_llm", "schema_version": "x", "agent_name": "test"}

    monkeypatch.setattr(DecisionProviderRouter, "_call_real_llm_profile", fake_call)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm("planner", PLANNER_CONTEXT)

    assert attempts == ["primary", "primary"]
    assert payload["real_llm_profile"]["fallback_attempts"] == ["primary", "primary#retry1"]
    assert payload["real_llm_profile"]["fallback_errors"] == {"primary": "temporary down"}


def test_non_strict_agent_role_can_return_schema_invalid_payload(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_AGENT_ROLES_JSON",
        json.dumps([
            {
                "name": "planner",
                "enabled": True,
                "decision_types": ["planner"],
                "strict_schema": False,
            }
        ]),
    )

    def fake_real(self, decision_type, context, agent_role=None):
        return {"schema_version": "x", "agent_name": "loose", "decision_source": "real_llm"}

    monkeypatch.setattr(DecisionProviderRouter, "_call_real_llm", fake_real)

    payload = DecisionProviderRouter(provider="real_llm").generate("planner", PLANNER_CONTEXT)

    assert payload["agent_name"] == "loose"
    assert payload["decision_metadata"]["schema_valid"] is False
    assert payload["decision_metadata"]["agent_role"] == "planner"


def test_real_llm_profiles_skip_disabled_and_order_unlisted_after_explicit(monkeypatch):
    profiles = [
        {"name": "disabled", "base_url": "https://disabled.test/v1", "model": "disabled", "api_key": "disabled", "enabled": False},
        {"name": "last", "base_url": "https://last.test/v1", "model": "last", "api_key": "last", "enabled": True},
        {"name": "first", "base_url": "https://first.test/v1", "model": "first", "api_key": "first", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "first")

    ordered = DecisionProviderRouter(provider="real_llm")._real_llm_profiles()

    assert [profile["name"] for profile in ordered] == ["first", "last"]


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


def test_real_llm_openai_profile_without_v1_uses_v1_chat_completions(monkeypatch):
    profile = {
        "name": "root-openai",
        "base_url": "https://api.example.test",
        "model": "gpt-5.5",
        "api_key": "secret",
        "enabled": True,
    }
    captured = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "real_llm"}
                                )
                            }
                        }
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", fake_urlopen)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm_profile("planner", PLANNER_CONTEXT, profile)

    assert captured["url"] == "https://api.example.test/v1/chat/completions"
    assert captured["headers"]["Accept"] == "application/json"
    assert "User-agent" in captured["headers"]
    assert captured["body"]["response_format"] == {"type": "json_object"}
    assert payload["decision_source"] == "real_llm"


def test_real_llm_openai_responses_profile_uses_responses_api_and_parses_output_text(monkeypatch):
    profile = {
        "name": "responses-openai",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-5.5",
        "api_key": "secret",
        "api_format": "openai_responses",
        "enabled": True,
    }
    captured = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "output": [
                        {
                            "type": "message",
                            "content": [
                                {
                                    "type": "output_text",
                                    "text": json.dumps(
                                        {"schema_version": "x", "agent_name": "test", "decision_source": "real_llm"}
                                    ),
                                }
                            ],
                        }
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", fake_urlopen)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm_profile("planner", PLANNER_CONTEXT, profile)

    assert captured["url"] == "https://api.example.test/v1/responses"
    assert captured["headers"]["Authorization"] == "Bearer secret"
    assert captured["body"]["input"][0]["role"] == "system"
    assert captured["body"]["input"][1]["role"] == "user"
    assert "response_format" not in captured["body"]
    assert payload["decision_source"] == "real_llm"


def test_real_llm_uses_compact_context_by_default(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_REAL_LLM_CONTEXT_MODE", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_CONTEXT_MODE", raising=False)
    profile = {
        "name": "openai-compatible",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-4o-mini",
        "api_key": "secret",
        "api_format": "openai",
        "enabled": True,
    }
    large_marker = "RAW_ONLY_MARKER_" + ("x" * 2000)
    large_context = {
        "context_id": "ctx-large-compact",
        "summary": {"large_marker": large_marker},
        "inputs": {
            "research_flow_state": {"state": "ready"},
            "failure_state": {},
            "queue_budget": {"validation": 2, "exploration": 1},
            "stable_candidates": [
                {"factor_name": f"factor_{i}", "family": "momentum", "extra": "z" * 500}
                for i in range(40)
            ],
            "candidate_pool_tasks": [
                {"goal": "g" * 1000, "payload": {"candidate_name": f"candidate_{i}"}}
                for i in range(60)
            ],
            "open_questions": ["q" * 1000 for _ in range(20)],
        },
    }
    raw_prompt = json.dumps(
        {
            "decision_type": "planner",
            "context": large_context,
            "required_output_schema": DecisionProviderRouter(provider="real_llm")._decision_schema_hint("planner"),
        },
        ensure_ascii=False,
    )
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "real_llm"}
                                )
                            },
                            "finish_reason": "stop",
                        }
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(req, timeout=0):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", fake_urlopen)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm_profile("planner", large_context, profile)

    user_prompt = captured["body"]["messages"][1]["content"]
    assert '"context_mode": "compact"' in user_prompt
    assert "context_compaction" in user_prompt
    assert large_marker not in user_prompt
    assert len(user_prompt) < len(raw_prompt) * 0.35
    assert payload["real_llm_prompt_meta"]["context_mode"] == "compact"
    assert payload["real_llm_prompt_meta"]["prompt_context_chars"] < payload["real_llm_prompt_meta"]["raw_context_chars"]


def test_real_llm_raw_context_mode_is_opt_in(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_REAL_LLM_CONTEXT_MODE", "raw")
    profile = {
        "name": "openai-compatible",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-4o-mini",
        "api_key": "secret",
        "api_format": "openai",
        "enabled": True,
    }
    raw_marker = "RAW_CONTEXT_MARKER_" + ("y" * 500)
    context = {
        "context_id": "ctx-raw-opt-in",
        "summary": {"raw_marker": raw_marker},
        "inputs": {"candidate_pool_tasks": []},
    }
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "real_llm"}
                                )
                            },
                            "finish_reason": "stop",
                        }
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(req, timeout=0):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", fake_urlopen)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm_profile("planner", context, profile)

    user_prompt = captured["body"]["messages"][1]["content"]
    assert '"context_mode": "raw"' in user_prompt
    assert raw_marker in user_prompt
    assert payload["real_llm_prompt_meta"]["context_mode"] == "raw"


def test_real_llm_compact_payload_covers_all_supported_decision_types(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_REAL_LLM_CONTEXT_MODE", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_CONTEXT_MODE", raising=False)
    router = DecisionProviderRouter(provider="real_llm")
    noisy_context = {
        "context_id": "ctx-all-agent-types",
        "summary": {"marker": "M" * 1000},
        "inputs": {
            "latest_run": {"run_id": "r1", "details": "D" * 1000},
            "stable_candidates": [{"factor_name": f"f{i}", "extra": "S" * 500} for i in range(30)],
            "candidate_pool_tasks": [{"goal": "G" * 1000, "payload": {"candidate_name": f"c{i}"}} for i in range(30)],
            "recent_failed_or_risky_tasks": [{"task_id": f"t{i}", "last_error": "E" * 500} for i in range(30)],
            "promotion_scorecard": {"rows": [{"candidate_name": f"p{i}", "evidence": "P" * 500} for i in range(30)]},
            "task_payload_summary": {"large": "Q" * 1000},
            "last_error": "L" * 1000,
        },
    }

    for decision_type in DECISION_SCHEMA_HINTS:
        payload, meta = router._real_llm_prompt_payload(decision_type, noisy_context)

        assert payload["context_mode"] == "compact"
        assert payload["context_compaction"]["context_mode"] == "compact"
        assert payload["required_output_schema"] == DECISION_SCHEMA_HINTS[decision_type]
        assert meta["prompt_context_chars"] < meta["raw_context_chars"]


def test_extract_llm_usage_normalizes_openai_chat_usage():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage(
        {"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
        "openai",
    )

    assert usage["prompt_tokens"] == 10
    assert usage["completion_tokens"] == 5
    assert usage["total_tokens"] == 15
    assert usage["usage_source"] == "provider"


def test_extract_llm_usage_normalizes_responses_usage():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage(
        {"usage": {"input_tokens": 20, "output_tokens": 7, "total_tokens": 27}},
        "openai_responses",
    )

    assert usage["prompt_tokens"] == 20
    assert usage["completion_tokens"] == 7
    assert usage["total_tokens"] == 27
    assert usage["usage_source"] == "provider"


def test_extract_llm_usage_normalizes_anthropic_usage_without_total():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage(
        {"usage": {"input_tokens": 30, "output_tokens": 8}},
        "anthropic",
    )

    assert usage["prompt_tokens"] == 30
    assert usage["completion_tokens"] == 8
    assert usage["total_tokens"] == 38
    assert usage["usage_source"] == "provider"


def test_extract_llm_usage_handles_missing_usage():
    router = DecisionProviderRouter(provider="real_llm")
    usage = router._extract_llm_usage({}, "openai")

    assert usage["prompt_tokens"] is None
    assert usage["completion_tokens"] is None
    assert usage["total_tokens"] is None
    assert usage["usage_source"] == "missing"


def test_append_llm_usage_ledger_writes_jsonl(monkeypatch, tmp_path):
    router = DecisionProviderRouter(provider="real_llm")
    ledger_path = tmp_path / "llm_usage_ledger.jsonl"
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)

    router._append_llm_usage_ledger({"decision_type": "planner", "usage": {"total_tokens": 3}})
    router._append_llm_usage_ledger({"decision_type": "failure_analyst", "usage": {"total_tokens": 4}})

    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 2
    assert rows[0]["decision_type"] == "planner"
    assert rows[1]["usage"]["total_tokens"] == 4


def test_real_llm_success_writes_usage_and_ledger(monkeypatch, tmp_path):
    profile = {
        "name": "openai-compatible",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-4o-mini",
        "api_key": "secret",
        "api_format": "openai",
        "enabled": True,
    }
    ledger_path = tmp_path / "llm_usage_ledger.jsonl"

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "choices": [
                        {
                            "message": {
                                "content": json.dumps(
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "real_llm"}
                                )
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 11, "completion_tokens": 4, "total_tokens": 15},
                }
            ).encode("utf-8")

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", lambda req, timeout=0: FakeResponse())
    router = DecisionProviderRouter(provider="real_llm")
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)

    payload = router._call_real_llm_profile("planner", PLANNER_CONTEXT, profile)

    assert payload["real_llm_usage"]["total_tokens"] == 15
    assert payload["real_llm_prompt_meta"]["user_prompt_chars"] > 0
    assert payload["real_llm_prompt_meta"]["estimated_user_prompt_tokens_4c"] > 0
    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["success"] is True
    assert rows[0]["decision_type"] == "planner"
    assert rows[0]["usage"]["total_tokens"] == 15


def test_real_llm_http_error_writes_failure_ledger(monkeypatch, tmp_path):
    profile = {
        "name": "quota-provider",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-4o-mini",
        "api_key": "secret",
        "api_format": "openai",
        "enabled": True,
    }
    ledger_path = tmp_path / "llm_usage_ledger.jsonl"

    class FakeHTTPError(llm_provider_router.urllib.error.HTTPError):
        def __init__(self):
            super().__init__(
                "https://api.example.test/v1/chat/completions",
                403,
                "Forbidden",
                hdrs=None,
                fp=None,
            )

        def read(self):
            return json.dumps({"error": {"message": "insufficient quota"}}).encode("utf-8")

    def fake_urlopen(req, timeout=0):
        raise FakeHTTPError()

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", fake_urlopen)
    router = DecisionProviderRouter(provider="real_llm")
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)

    try:
        router._call_real_llm_profile("planner", PLANNER_CONTEXT, profile)
    except RuntimeError as exc:
        assert "http_error:403" in str(exc)
    else:
        raise AssertionError("HTTP errors should still raise after writing usage ledger")

    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["success"] is False
    assert rows[0]["error_type"] == "http_error:403"
    assert rows[0]["usage"]["usage_source"] == "missing"


def test_real_llm_anthropic_profile_uses_messages_api_and_parses_content(monkeypatch):
    profile = {
        "name": "claude-direct",
        "base_url": "https://anthropic.example.test",
        "model": "claude-opus-4-7",
        "api_key": "secret",
        "api_format": "anthropic",
        "enabled": True,
    }
    captured = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return json.dumps(
                {
                    "content": [
                        {
                            "type": "text",
                            "text": json.dumps(
                                {"schema_version": "x", "agent_name": "test", "decision_source": "real_llm"}
                            ),
                        }
                    ]
                }
            ).encode("utf-8")

    def fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(llm_provider_router.urllib.request, "urlopen", fake_urlopen)

    payload = DecisionProviderRouter(provider="real_llm")._call_real_llm_profile("planner", PLANNER_CONTEXT, profile)

    assert captured["url"] == "https://anthropic.example.test/v1/messages"
    assert captured["headers"]["X-api-key"] == "secret"
    assert captured["headers"]["Anthropic-version"] == "2023-06-01"
    assert "response_format" not in captured["body"]
    assert captured["body"]["max_tokens"] > 0
    assert payload["decision_source"] == "real_llm"
