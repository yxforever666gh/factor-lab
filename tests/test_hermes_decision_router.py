import importlib
import json
from pathlib import Path

import pytest

import factor_lab.hermes_decision_router as hermes_decision_router
from factor_lab.hermes_decision_router import DECISION_SCHEMA_HINTS, HermesDecisionRouter


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


@pytest.fixture(autouse=True)
def _disable_usage_ledger_side_effects(monkeypatch):
    monkeypatch.setattr(HermesDecisionRouter, "_try_append_llm_usage_ledger", lambda self, row: None, raising=False)


def test_router_supports_reviewer_and_data_steward_schema_hints():
    router = HermesDecisionRouter(provider="heuristic")

    assert router._decision_schema_version("reviewer") == "factor_lab.reviewer_agent_response.v1"
    assert router._decision_schema_version("data_steward") == "factor_lab.data_steward_agent_response.v1"
    assert "candidate_reviews" in router._decision_schema_hint("reviewer")
    assert "data_steward_findings" in router._decision_schema_hint("data_steward")


def test_disabled_hermes_profile_blocks_generation(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
        '[{"name":"reviewer","display_name":"Reviewer","enabled":false,"decision_types":["reviewer"],"purpose":"x","system_prompt":"x","llm_fallback_order":[],"timeout_seconds":1,"max_retries":0,"strict_schema":true}]',
    )
    router = HermesDecisionRouter(provider="heuristic")

    try:
        router.generate("reviewer", {"context_id": "ctx"})
    except RuntimeError as exc:
        assert "disabled" in str(exc)
    else:
        raise AssertionError("disabled reviewer role should block generation")


def test_hermes_native_uses_role_specific_legacy_ids(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
        '[{"name":"reviewer","display_name":"Reviewer","enabled":true,"decision_types":["reviewer"],"purpose":"x","system_prompt":"x","llm_fallback_order":[],"timeout_seconds":1,"max_retries":0,"strict_schema":true,"legacy_agent_id":"factor-lab-reviewer"}]',
    )
    router = HermesDecisionRouter(provider="heuristic")

    assert router._hermes_native_agent_id("reviewer") == "factor-lab-reviewer"
    assert router._hermes_native_session_base("data_steward").endswith("data-steward")


def test_hermes_native_prompt_uses_role_specific_context_for_reviewer_and_data_steward(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON", raising=False)
    router = HermesDecisionRouter(provider="heuristic")

    reviewer_prompt = router._hermes_native_prompt(
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
    data_steward_prompt = router._hermes_native_prompt(
        "data_steward",
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
    assert "task_payload_summary" in data_steward_prompt
    assert "dataset_rows" in data_steward_prompt
    assert "Missing required environment variable" in data_steward_prompt


def test_router_generates_reviewer_and_data_steward_with_metadata(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON", raising=False)
    router = HermesDecisionRouter(provider="heuristic")

    review = router.generate("reviewer", {"context_id": "ctx-review", "inputs": {"promotion_scorecard": {"rows": []}}})
    data = router.generate("data_steward", {"context_id": "ctx-data", "inputs": {"latest_run": {"dataset_rows": 0}}})

    assert review["decision_metadata"]["hermes_profile"] == "reviewer"
    assert data["decision_metadata"]["hermes_profile"] == "data_steward"
    assert review["schema_version"] == "factor_lab.reviewer_agent_response.v1"
    assert data["schema_version"] == "factor_lab.data_steward_agent_response.v1"


def test_router_uses_heuristic_provider_with_metadata():
    router = HermesDecisionRouter(provider="heuristic")

    payload = router.generate("planner", PLANNER_CONTEXT)

    assert payload["mode"] in {"validate", "recover", "converge"}
    assert payload["decision_metadata"]["source"] == "heuristic"
    assert payload["decision_metadata"]["effective_source"] == "heuristic"
    assert payload["decision_metadata"]["configured_provider"] == "heuristic"
    assert payload["decision_metadata"]["degraded_to_heuristic"] is False
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["decision_metadata"]["decision_context_id"] == "ctx-planner-1"


def test_router_attaches_hermes_profile_metadata_for_planner(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
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
    router = HermesDecisionRouter(provider="heuristic")

    payload = router.generate("planner", {"context_id": "ctx-1", "inputs": {}})

    meta = payload["decision_metadata"]
    assert meta["hermes_profile"] == "planner"
    assert meta["hermes_profile_source"] == "configured"
    assert meta["hermes_profile_enabled"] is True
    assert meta["legacy_agent_id"] == "factor-lab-planner"



def test_router_falls_back_from_real_provider_to_heuristic(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_API_KEY", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_PROFILES_JSON", raising=False)
    monkeypatch.delenv("OPENAI_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    router = HermesDecisionRouter(provider="direct_model")

    payload = router.generate("diagnostician", FAILURE_CONTEXT)

    assert payload["decision_metadata"]["source"] == "heuristic"
    assert payload["decision_metadata"]["effective_source"] == "heuristic"
    assert payload["decision_metadata"]["configured_provider"] == "direct_model"
    assert payload["decision_metadata"]["degraded_to_heuristic"] is True
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["decision_metadata"]["fallback_reason"] == "provider_error:direct_model"



def test_router_healthcheck_reports_missing_real_provider(monkeypatch, tmp_path):
    monkeypatch.delenv("FACTOR_LAB_LLM_BASE_URL", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_API_KEY", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_PROFILES_JSON", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_FALLBACK_ORDER", raising=False)
    monkeypatch.delenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", raising=False)
    monkeypatch.delenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", raising=False)
    router = HermesDecisionRouter(provider="auto")

    payload = router.healthcheck(output_path=tmp_path / "health.json")

    assert payload["real_provider_configured"] is False
    assert payload["recommended_effective_source"] == "heuristic"
    assert payload["effective_source"] == "heuristic"
    assert payload["probe"]["attempted"] is False



def test_router_uses_hermes_native_agent_provider(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_SESSION_PREFIX", "factor-lab-decision")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_SESSION_MODE", "persistent")

    def fake_run(command, cwd, capture_output, text, timeout, env):
        assert "factor-lab-planner" in command
        payload = {
            "payloads": [{"text": json.dumps({
                "schema_version": "factor_lab.researcher_profile_response.v1",
                "mode": "validate",
                "task_mix": {"baseline": 1, "validation": 2, "exploration": 0},
                "priority_families": ["stable_candidate_validation"],
                "suppress_families": [],
                "recommended_actions": [],
            }, ensure_ascii=False)}],
            "meta": {"agentMeta": {"provider": "codex-for-me", "model": "gpt-5.4"}},
        }
        return type("Completed", (), {"returncode": 0, "stdout": json.dumps(payload, ensure_ascii=False), "stderr": ""})()

    monkeypatch.setattr("factor_lab.hermes_decision_router.subprocess.run", fake_run)
    router = HermesDecisionRouter(provider="hermes_native_agent")

    payload = router.generate("planner", PLANNER_CONTEXT)

    assert payload["decision_metadata"]["source"] == "hermes_native_agent"
    assert payload["decision_metadata"]["effective_source"] == "hermes_native_agent"
    assert payload["decision_metadata"]["session_mode"] == "persistent"
    assert payload["decision_metadata"]["session_id"] == "factor-lab-decision-planner"
    assert payload["decision_metadata"]["request_scope_id"] == "ctx-planner-1"
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["hermes_native_agent_meta"]["agent_id"] == "factor-lab-planner"



def test_router_uses_hermes_native_gateway_provider(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_SESSION_PREFIX", "factor-lab-decision")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_SESSION_MODE", "persistent")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

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
        assert headers["x-hermes_native-session-key"] == "factor-lab-decision-planner"
        request_body = json.loads(req.data.decode("utf-8"))
        assert request_body["model"] == "hermes_native/factor-lab-planner"
        payload = {
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "model": "hermes_native/factor-lab-planner",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": json.dumps(
                            {
                                "schema_version": "factor_lab.researcher_profile_response.v1",
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

    monkeypatch.setattr("factor_lab.hermes_decision_router.urllib.request.urlopen", fake_urlopen)
    router = HermesDecisionRouter(provider="hermes_native_gateway")

    payload = router.generate("planner", PLANNER_CONTEXT)

    assert payload["decision_metadata"]["source"] == "hermes_native_gateway"
    assert payload["decision_metadata"]["effective_source"] == "hermes_native_gateway"
    assert payload["decision_metadata"]["session_mode"] == "persistent"
    assert payload["decision_metadata"]["session_id"] == "factor-lab-decision-planner"
    assert payload["decision_metadata"]["request_scope_id"] == "ctx-planner-1"
    assert payload["decision_metadata"]["schema_valid"] is True
    assert payload["hermes_native_gateway_meta"]["agent_id"] == "factor-lab-planner"
    assert payload["hermes_native_gateway_meta"]["session_id"] == "factor-lab-decision-planner"
    assert payload["hermes_native_gateway_meta"]["request_scope_id"] == "ctx-planner-1"



def test_router_healthcheck_probes_hermes_native_gateway(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

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
        assert "x-hermes_native-session-key" not in headers
        return FakeResponse()

    monkeypatch.setattr("factor_lab.hermes_decision_router.urllib.request.urlopen", fake_urlopen)
    router = HermesDecisionRouter(provider="hermes_native_gateway")

    payload = router.healthcheck(output_path=tmp_path / "health.json")

    assert payload["hermes_native_gateway_configured"] is True
    assert payload["normalized_provider"] == "legacy_hermes_native_gateway"
    assert payload["provider_class"] == "legacy"
    assert payload["recommended_effective_source"] == "legacy_hermes_native_gateway"
    assert payload["effective_source"] == "legacy_hermes_native_gateway"
    assert payload["probe"]["attempted"] is True
    assert payload["probe"]["ok"] is True


def test_router_healthcheck_can_skip_gateway_probe(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")
    router = HermesDecisionRouter(provider="hermes_native_gateway")

    payload = router.healthcheck(output_path=tmp_path / "health.json", probe=False)

    assert payload["normalized_provider"] == "legacy_hermes_native_gateway"
    assert payload["provider_class"] == "legacy"
    assert payload["recommended_effective_source"] == "legacy_hermes_native_gateway"
    assert payload["probe"]["attempted"] is False
    assert payload["probe"]["skipped"] is True
    assert payload["probe"]["error"] == "probe_skipped"


def test_hermes_native_session_mode_defaults_to_ephemeral(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_HERMES_NATIVE_SESSION_MODE", raising=False)
    router = HermesDecisionRouter(provider="hermes_native_gateway")

    assert router._hermes_native_session_id("planner", context=PLANNER_CONTEXT) == "factor-lab-decision-planner-ctx-planner-1"
    assert router._hermes_native_session_id("diagnostician", context=FAILURE_CONTEXT) == "factor-lab-decision-diagnostician-ctx-failure-1"


def test_hermes_native_persistent_session_mode_uses_fixed_session(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_SESSION_MODE", "persistent")
    router = HermesDecisionRouter(provider="hermes_native_gateway")

    assert router._hermes_native_session_id("planner", context=PLANNER_CONTEXT) == "factor-lab-decision-planner"
    assert router._hermes_native_session_id("diagnostician", context=FAILURE_CONTEXT) == "factor-lab-decision-diagnostician"


def test_hermes_native_ephemeral_session_mode_still_supported(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_SESSION_MODE", "ephemeral")
    router = HermesDecisionRouter(provider="hermes_native_gateway")

    assert router._hermes_native_session_id("planner", context=PLANNER_CONTEXT) == "factor-lab-decision-planner-ctx-planner-1"
    assert router._hermes_native_session_id("diagnostician", context=FAILURE_CONTEXT) == "factor-lab-decision-diagnostician-ctx-failure-1"


def test_router_loads_env_from_configurable_factor_lab_env_file(monkeypatch, tmp_path):
    env_file = tmp_path / "factor-lab.env"
    env_file.write_text(
        "FACTOR_LAB_DECISION_PROVIDER=direct_model\n"
        "FACTOR_LAB_LLM_MODEL=router-test-model\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("FACTOR_LAB_ENV_FILE", str(env_file))
    monkeypatch.delenv("FACTOR_LAB_DECISION_PROVIDER", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_MODEL", raising=False)

    reloaded = importlib.reload(hermes_decision_router)
    try:
        router = reloaded.HermesDecisionRouter()
        assert router.provider == "direct_model"
        assert router.model == "router-test-model"
    finally:
        importlib.reload(reloaded)


def test_router_healthcheck_writes_to_configurable_artifacts_dir(monkeypatch, tmp_path):
    artifacts_dir = tmp_path / "custom-artifacts"
    monkeypatch.setenv("FACTOR_LAB_ARTIFACTS_DIR", str(artifacts_dir))
    router = HermesDecisionRouter(provider="heuristic")

    payload = router.healthcheck()

    expected = artifacts_dir / "llm_provider_health.json"
    assert expected.exists()
    written = json.loads(expected.read_text(encoding="utf-8"))
    assert written["effective_source"] == payload["effective_source"]


def test_router_healthcheck_reports_normalized_legacy_provider(monkeypatch, tmp_path):
    planner_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

    router = HermesDecisionRouter(provider="hermes_native_gateway")
    payload = router.healthcheck(output_path=tmp_path / "health.json", probe=False)

    assert payload["configured_provider"] == "hermes_native_gateway"
    assert payload["normalized_provider"] == "legacy_hermes_native_gateway"
    assert payload["provider_class"] == "legacy"
    assert payload["recommended_effective_source"] == "legacy_hermes_native_gateway"
    assert payload["effective_source"] == "legacy_hermes_native_gateway"


def test_direct_model_uses_configured_profile_fallback_order(monkeypatch):
    profiles = [
        {"name": "primary", "base_url": "https://primary.test/v1", "model": "primary-model", "api_key": "***", "enabled": True},
        {"name": "backup", "base_url": "https://backup.test/v1", "model": "backup-model", "api_key": "***", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "primary,backup")
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
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

    def fake_call(self, decision_type, context, profile, hermes_profile=None):
        attempts.append(profile["name"])
        if profile["name"] == "primary":
            raise RuntimeError("primary down")
        return {"decision_source": "direct_model", "schema_version": "x", "agent_name": "test"}

    monkeypatch.setattr(HermesDecisionRouter, "_call_direct_model_profile", fake_call)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model("planner", PLANNER_CONTEXT)

    assert attempts == ["primary", "backup"]
    assert payload["direct_model_profile"]["name"] == "backup"
    assert payload["direct_model_profile"]["fallback_attempts"] == ["primary", "backup"]
    assert payload["direct_model_profile"]["fallback_errors"] == {"primary": "primary down"}


def test_direct_model_uses_hermes_profile_specific_fallback_order(monkeypatch):
    profiles = [
        {"name": "global-first", "base_url": "https://global.test/v1", "model": "global", "api_key": "***", "enabled": True},
        {"name": "role-first", "base_url": "https://role.test/v1", "model": "role", "api_key": "***", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "global-first,role-first")
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
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

    def fake_call(self, decision_type, context, profile, hermes_profile=None):
        attempts.append(profile["name"])
        assert hermes_profile is not None
        assert hermes_profile.name == "planner"
        assert "planner role prompt" in self._agent_system_prompt(decision_type, hermes_profile)
        return {"decision_source": "direct_model", "schema_version": "x", "agent_name": "test"}

    monkeypatch.setattr(HermesDecisionRouter, "_call_direct_model_profile", fake_call)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model("planner", PLANNER_CONTEXT)

    assert attempts == ["role-first"]
    assert payload["direct_model_profile"]["name"] == "role-first"


def test_hermes_profile_max_retries_retry_profile_before_fallback(monkeypatch):
    profiles = [
        {"name": "primary", "base_url": "https://primary.test/v1", "model": "primary", "api_key": "***", "enabled": True},
        {"name": "backup", "base_url": "https://backup.test/v1", "model": "backup", "api_key": "***", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
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

    def fake_call(self, decision_type, context, profile, hermes_profile=None):
        attempts.append(profile["name"])
        if len(attempts) < 2:
            raise RuntimeError("temporary down")
        return {"decision_source": "direct_model", "schema_version": "x", "agent_name": "test"}

    monkeypatch.setattr(HermesDecisionRouter, "_call_direct_model_profile", fake_call)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model("planner", PLANNER_CONTEXT)

    assert attempts == ["primary", "primary"]
    assert payload["direct_model_profile"]["fallback_attempts"] == ["primary", "primary#retry1"]
    assert payload["direct_model_profile"]["fallback_errors"] == {"primary": "temporary down"}


def test_non_strict_hermes_profile_can_return_schema_invalid_payload(monkeypatch):
    monkeypatch.setenv(
        "FACTOR_LAB_HERMES_PROFILE_SETTINGS_JSON",
        json.dumps([
            {
                "name": "planner",
                "enabled": True,
                "decision_types": ["planner"],
                "strict_schema": False,
            }
        ]),
    )

    def fake_real(self, decision_type, context, hermes_profile=None):
        return {"schema_version": "x", "agent_name": "loose", "decision_source": "direct_model"}

    monkeypatch.setattr(HermesDecisionRouter, "_call_direct_model", fake_real)

    payload = HermesDecisionRouter(provider="direct_model").generate("planner", PLANNER_CONTEXT)

    assert payload["agent_name"] == "loose"
    assert payload["decision_metadata"]["schema_valid"] is False
    assert payload["decision_metadata"]["hermes_profile"] == "planner"


def test_direct_model_profiles_skip_disabled_and_order_unlisted_after_explicit(monkeypatch):
    profiles = [
        {"name": "disabled", "base_url": "https://disabled.test/v1", "model": "disabled", "api_key": "disabled", "enabled": False},
        {"name": "last", "base_url": "https://last.test/v1", "model": "last", "api_key": "last", "enabled": True},
        {"name": "first", "base_url": "https://first.test/v1", "model": "first", "api_key": "first", "enabled": True},
    ]
    monkeypatch.setenv("FACTOR_LAB_LLM_PROFILES_JSON", json.dumps(profiles))
    monkeypatch.setenv("FACTOR_LAB_LLM_FALLBACK_ORDER", "first")

    ordered = HermesDecisionRouter(provider="direct_model")._direct_model_profiles()

    assert [profile["name"] for profile in ordered] == ["first", "last"]


def test_router_auto_prefers_direct_model_before_legacy_hermes_native(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_LLM_BASE_URL", "https://example.test/v1")
    monkeypatch.setenv("FACTOR_LAB_LLM_API_KEY", "secret")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")
    monkeypatch.setattr(Path, "home", lambda: Path("/nonexistent-home-for-router-test"))

    router = HermesDecisionRouter(provider="auto")

    assert router._provider_chain() == ["direct_model", "heuristic", "mock"]


def test_router_legacy_aliases_map_to_normalized_providers(monkeypatch):
    router = HermesDecisionRouter(provider="hermes_native_gateway")
    assert router._normalized_provider_name() == "legacy_hermes_native_gateway"
    assert router._provider_class() == "legacy"

    router = HermesDecisionRouter(provider="hermes_native_agent")
    assert router._normalized_provider_name() == "legacy_hermes_native_agent"
    assert router._provider_class() == "legacy"

    router = HermesDecisionRouter(provider="hermes_native_cli")
    assert router._normalized_provider_name() == "legacy_hermes_native_agent"
    assert router._provider_class() == "legacy"

    router = HermesDecisionRouter(provider="hermes_native_internal")
    assert router._normalized_provider_name() == "legacy_hermes_native_agent"
    assert router._provider_class() == "legacy"

    router = HermesDecisionRouter(provider="direct_model")
    assert router._normalized_provider_name() == "direct_model"
    assert router._provider_class() == "primary"

    router = HermesDecisionRouter(provider="heuristic")
    assert router._normalized_provider_name() == "heuristic"
    assert router._provider_class() == "local"


def test_router_healthcheck_reports_normalized_fields_for_all_providers(monkeypatch, tmp_path):
    # Test hermes_native_gateway
    planner_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-planner" / "agent"
    failure_dir = tmp_path / ".hermes_native" / "agents" / "factor-lab-failure" / "agent"
    planner_dir.mkdir(parents=True)
    failure_dir.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_RESEARCHER_PROFILE", "factor-lab-planner")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_FAILURE_AGENT", "factor-lab-failure")
    monkeypatch.setenv("FACTOR_LAB_HERMES_NATIVE_GATEWAY_URL", "http://127.0.0.1:18789/v1/chat/completions")

    router = HermesDecisionRouter(provider="hermes_native_gateway")
    payload = router.healthcheck(output_path=tmp_path / "health.json", probe=False)

    assert payload["configured_provider"] == "hermes_native_gateway"
    assert payload["normalized_provider"] == "legacy_hermes_native_gateway"
    assert payload["provider_class"] == "legacy"

    # Test hermes_native_agent
    router = HermesDecisionRouter(provider="hermes_native_agent")
    payload = router.healthcheck(output_path=tmp_path / "health2.json", probe=False)

    assert payload["configured_provider"] == "hermes_native_agent"
    assert payload["normalized_provider"] == "legacy_hermes_native_agent"
    assert payload["provider_class"] == "legacy"

    # Test direct_model
    monkeypatch.setenv("FACTOR_LAB_LLM_BASE_URL", "https://example.test/v1")
    monkeypatch.setenv("FACTOR_LAB_LLM_API_KEY", "secret")
    router = HermesDecisionRouter(provider="direct_model")
    payload = router.healthcheck(output_path=tmp_path / "health3.json", probe=False)

    assert payload["configured_provider"] == "direct_model"
    assert payload["normalized_provider"] == "direct_model"
    assert payload["provider_class"] == "primary"

    # Test heuristic
    router = HermesDecisionRouter(provider="heuristic")
    payload = router.healthcheck(output_path=tmp_path / "health4.json", probe=False)

    assert payload["configured_provider"] == "heuristic"
    assert payload["normalized_provider"] == "heuristic"
    assert payload["provider_class"] == "local"


def test_direct_model_openai_profile_without_v1_uses_v1_chat_completions(monkeypatch):
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
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "direct_model"}
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

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", fake_urlopen)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model_profile("planner", PLANNER_CONTEXT, profile)

    assert captured["url"] == "https://api.example.test/v1/chat/completions"
    assert captured["headers"]["Accept"] == "application/json"
    assert "User-agent" in captured["headers"]
    assert captured["body"]["response_format"] == {"type": "json_object"}
    assert payload["decision_source"] == "direct_model"


def test_direct_model_openai_responses_profile_uses_responses_api_and_parses_output_text(monkeypatch):
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
                                        {"schema_version": "x", "agent_name": "test", "decision_source": "direct_model"}
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

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", fake_urlopen)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model_profile("planner", PLANNER_CONTEXT, profile)

    assert captured["url"] == "https://api.example.test/v1/responses"
    assert captured["headers"]["Authorization"] == "Bearer secret"
    assert captured["body"]["input"][0]["role"] == "system"
    assert captured["body"]["input"][1]["role"] == "user"
    assert "response_format" not in captured["body"]
    assert payload["decision_source"] == "direct_model"


def test_direct_model_uses_compact_context_by_default(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_CONTEXT_MODE", raising=False)
    profile = {
        "name": "openai-compatible",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-5.5",
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
            "required_output_schema": HermesDecisionRouter(provider="direct_model")._decision_schema_hint("planner"),
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
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "direct_model"}
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

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", fake_urlopen)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model_profile("planner", large_context, profile)

    user_prompt = captured["body"]["messages"][1]["content"]
    assert '"context_mode": "compact"' in user_prompt
    assert "context_compaction" in user_prompt
    assert large_marker not in user_prompt
    assert len(user_prompt) < len(raw_prompt) * 0.35
    assert payload["direct_model_prompt_meta"]["context_mode"] == "compact"
    assert payload["direct_model_prompt_meta"]["prompt_context_chars"] < payload["direct_model_prompt_meta"]["raw_context_chars"]


def test_direct_model_raw_context_mode_is_opt_in(monkeypatch):
    monkeypatch.setenv("FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE", "raw")
    profile = {
        "name": "openai-compatible",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-5.5",
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
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "direct_model"}
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

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", fake_urlopen)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model_profile("planner", context, profile)

    user_prompt = captured["body"]["messages"][1]["content"]
    assert '"context_mode": "raw"' in user_prompt
    assert raw_marker in user_prompt
    assert payload["direct_model_prompt_meta"]["context_mode"] == "raw"


def test_direct_model_compact_payload_covers_all_supported_decision_types(monkeypatch):
    monkeypatch.delenv("FACTOR_LAB_DIRECT_MODEL_CONTEXT_MODE", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LLM_CONTEXT_MODE", raising=False)
    router = HermesDecisionRouter(provider="direct_model")
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
        payload, meta = router._direct_model_prompt_payload(decision_type, noisy_context)

        assert payload["context_mode"] == "compact"
        assert payload["context_compaction"]["context_mode"] == "compact"
        assert payload["required_output_schema"] == DECISION_SCHEMA_HINTS[decision_type]
        assert meta["prompt_context_chars"] < meta["raw_context_chars"]


def test_extract_llm_usage_normalizes_openai_chat_usage():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage(
        {"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
        "openai",
    )

    assert usage["prompt_tokens"] == 10
    assert usage["completion_tokens"] == 5
    assert usage["total_tokens"] == 15
    assert usage["usage_source"] == "provider"


def test_extract_llm_usage_normalizes_cached_tokens_from_provider_details():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage(
        {
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 20,
                "total_tokens": 120,
                "prompt_tokens_details": {"cached_tokens": 75},
            }
        },
        "openai",
    )

    assert usage["prompt_tokens"] == 100
    assert usage["cached_tokens"] == 75
    assert usage["uncached_prompt_tokens"] == 25
    assert usage["total_tokens"] == 120


def test_extract_llm_usage_normalizes_anthropic_cache_tokens():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage(
        {
            "usage": {
                "input_tokens": 40,
                "output_tokens": 10,
                "cache_creation_input_tokens": 12,
                "cache_read_input_tokens": 28,
            }
        },
        "anthropic",
    )

    assert usage["prompt_tokens"] == 40
    assert usage["completion_tokens"] == 10
    assert usage["cached_tokens"] == 28
    assert usage["cache_creation_tokens"] == 12
    assert usage["uncached_prompt_tokens"] == 12
    assert usage["total_tokens"] == 50


def test_extract_llm_usage_normalizes_provider_cache_aliases():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage(
        {
            "usage": {
                "input_tokens": 100,
                "output_tokens": 10,
                "input_tokens_details": {"cached_input_tokens": 64, "cache_write_input_tokens": 8},
            }
        },
        "openai_responses",
    )

    assert usage["prompt_tokens"] == 100
    assert usage["completion_tokens"] == 10
    assert usage["cached_tokens"] == 64
    assert usage["cache_creation_tokens"] == 8
    assert usage["uncached_prompt_tokens"] == 36
    assert usage["total_tokens"] == 110


def test_extract_llm_usage_normalizes_responses_usage():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage(
        {"usage": {"input_tokens": 20, "output_tokens": 7, "total_tokens": 27}},
        "openai_responses",
    )

    assert usage["prompt_tokens"] == 20
    assert usage["completion_tokens"] == 7
    assert usage["total_tokens"] == 27
    assert usage["usage_source"] == "provider"


def test_extract_llm_usage_normalizes_anthropic_usage_without_total():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage(
        {"usage": {"input_tokens": 30, "output_tokens": 8}},
        "anthropic",
    )

    assert usage["prompt_tokens"] == 30
    assert usage["completion_tokens"] == 8
    assert usage["total_tokens"] == 38
    assert usage["usage_source"] == "provider"


def test_extract_llm_usage_handles_missing_usage():
    router = HermesDecisionRouter(provider="direct_model")
    usage = router._extract_llm_usage({}, "openai")

    assert usage["prompt_tokens"] is None
    assert usage["completion_tokens"] is None
    assert usage["total_tokens"] is None
    assert usage["usage_source"] == "missing"


def test_append_llm_usage_ledger_writes_jsonl(monkeypatch, tmp_path):
    router = HermesDecisionRouter(provider="direct_model")
    ledger_path = tmp_path / "llm_usage_ledger.jsonl"
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)

    router._append_llm_usage_ledger({"decision_type": "planner", "usage": {"total_tokens": 3}})
    router._append_llm_usage_ledger({"decision_type": "diagnostician", "usage": {"total_tokens": 4}})

    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 2
    assert rows[0]["decision_type"] == "planner"
    assert rows[1]["usage"]["total_tokens"] == 4


def test_direct_model_success_writes_usage_and_ledger(monkeypatch, tmp_path):
    profile = {
        "name": "openai-compatible",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-5.5",
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
                                    {"schema_version": "x", "agent_name": "test", "decision_source": "direct_model"}
                                )
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 11,
                        "completion_tokens": 4,
                        "total_tokens": 15,
                        "prompt_tokens_details": {"cached_tokens": 6},
                    },
                }
            ).encode("utf-8")

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", lambda req, timeout=0: FakeResponse())
    router = HermesDecisionRouter(provider="direct_model")
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)
    monkeypatch.setattr(router, "_try_append_llm_usage_ledger", lambda row: router._append_llm_usage_ledger(row))

    payload = router._call_direct_model_profile("planner", PLANNER_CONTEXT, profile)

    assert payload["direct_model_usage"]["total_tokens"] == 15
    assert payload["direct_model_usage"]["cached_tokens"] == 6
    assert payload["direct_model_cost"]["estimated_cost_usd"] > 0
    assert payload["direct_model_prompt_meta"]["user_prompt_chars"] > 0
    assert payload["direct_model_prompt_meta"]["estimated_user_prompt_tokens_4c"] > 0
    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["success"] is True
    assert rows[0]["decision_type"] == "planner"
    assert rows[0]["usage"]["total_tokens"] == 15
    assert rows[0]["usage"]["cached_tokens"] == 6
    assert rows[0]["usage"]["uncached_prompt_tokens"] == 5
    assert rows[0]["estimated_cost_usd"] == rows[0]["cost"]["estimated_cost_usd"]


def test_direct_model_http_error_writes_failure_ledger(monkeypatch, tmp_path):
    profile = {
        "name": "quota-provider",
        "base_url": "https://api.example.test/v1",
        "model": "gpt-5.5",
        "api_key": "secret",
        "api_format": "openai",
        "enabled": True,
    }
    ledger_path = tmp_path / "llm_usage_ledger.jsonl"

    class FakeHTTPError(hermes_decision_router.urllib.error.HTTPError):
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

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", fake_urlopen)
    router = HermesDecisionRouter(provider="direct_model")
    monkeypatch.setattr(router, "_llm_usage_ledger_path", lambda: ledger_path)
    monkeypatch.setattr(router, "_try_append_llm_usage_ledger", lambda row: router._append_llm_usage_ledger(row))

    try:
        router._call_direct_model_profile("planner", PLANNER_CONTEXT, profile)
    except RuntimeError as exc:
        assert "http_error:403" in str(exc)
    else:
        raise AssertionError("HTTP errors should still raise after writing usage ledger")

    rows = [json.loads(line) for line in ledger_path.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["success"] is False
    assert rows[0]["error_type"] == "http_error:403"
    assert rows[0]["usage"]["usage_source"] == "missing"


def test_direct_model_anthropic_profile_uses_messages_api_and_parses_content(monkeypatch):
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
                                {"schema_version": "x", "agent_name": "test", "decision_source": "direct_model"}
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

    monkeypatch.setattr(hermes_decision_router.urllib.request, "urlopen", fake_urlopen)

    payload = HermesDecisionRouter(provider="direct_model")._call_direct_model_profile("planner", PLANNER_CONTEXT, profile)

    assert captured["url"] == "https://anthropic.example.test/v1/messages"
    assert captured["headers"]["X-api-key"] == "secret"
    assert captured["headers"]["Anthropic-version"] == "2023-06-01"
    assert "response_format" not in captured["body"]
    assert captured["body"]["max_tokens"] > 0
    assert payload["decision_source"] == "direct_model"
