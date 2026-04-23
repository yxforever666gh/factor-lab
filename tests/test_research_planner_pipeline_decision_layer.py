import json
from pathlib import Path

from factor_lab import research_planner_pipeline as pipeline


class FakeRouter:
    calls: list[dict[str, object]] = []

    def __init__(self, provider=None, model=None):
        self.provider = provider
        self.model = model

    def healthcheck(self, output_path=None, *, probe=True):
        FakeRouter.calls.append(
            {
                "provider": self.provider,
                "probe": probe,
                "output_path": str(output_path) if output_path is not None else None,
            }
        )
        return {
            "configured_provider": self.provider,
            "normalized_provider": self.provider,
            "provider_class": "generic" if self.provider in {"real_llm", "heuristic", "mock"} else "legacy_openclaw",
            "recommended_effective_source": self.provider,
            "effective_source": self.provider,
            "degraded_to_heuristic": False,
            "probe": {"attempted": probe, "skipped": not probe, "ok": True, "latency_ms": 1, "error": None},
        }



def test_pipeline_builds_briefs_before_running_brief_runner(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "_root_path", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_artifacts_path", lambda: tmp_path / "artifacts")
    monkeypatch.setattr(pipeline, "_db_path", lambda: tmp_path / "artifacts" / "factor_lab.db")
    monkeypatch.setattr(pipeline, "_live_provider_health_path", lambda: tmp_path / "artifacts" / "llm_provider_health_live.json")
    monkeypatch.setattr(pipeline, "_observation_provider_health_path", lambda: tmp_path / "artifacts" / "llm_provider_health.json")
    monkeypatch.setenv("FACTOR_LAB_DECISION_PROVIDER", "heuristic")
    monkeypatch.delenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LIVE_DECISION_PROVIDER", raising=False)

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    FakeRouter.calls = []

    monkeypatch.setattr(pipeline, "build_research_space_registry", lambda *args, **kwargs: {"windows_covered": {}, "validation_depth": {}, "graveyard_diagnostics": {}})
    monkeypatch.setattr(pipeline, "build_research_space_map", lambda *args, **kwargs: {"family_progress": {}})
    monkeypatch.setattr(
        pipeline,
        "build_research_planner_snapshot",
        lambda *args, **kwargs: {
            "latest_run": {"run_id": "r1", "config_path": "cfg.json"},
            "latest_graveyard": [],
            "stable_candidates": [],
            "queue_budget": {},
            "failure_state": {},
            "exploration_state": {},
            "research_flow_state": {"state": "ready"},
            "knowledge_gain_counter": {},
            "promotion_scorecard": {"rows": []},
            "research_learning": {},
        },
    )
    monkeypatch.setattr(pipeline, "build_research_candidate_pool", lambda *args, **kwargs: {"tasks": [], "suppressed_tasks": []})
    monkeypatch.setattr(pipeline, "build_branch_planner_output", lambda *args, **kwargs: {"selected_families": []})
    monkeypatch.setattr(pipeline, "_maybe_skip_pipeline", lambda fingerprint: None)
    monkeypatch.setattr(pipeline, "build_recovery_tasks", lambda *args, **kwargs: {"tasks": []})
    monkeypatch.setattr(pipeline, "build_research_plan", lambda *args, **kwargs: {"selected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_state_snapshot", lambda *args, **kwargs: {"open_questions": []})
    monkeypatch.setattr(pipeline, "derive_research_flow_state", lambda **kwargs: {"state": "ready"})
    monkeypatch.setattr(pipeline, "build_research_opportunities", lambda *args, **kwargs: {"opportunities": []})
    monkeypatch.setattr(pipeline, "enqueue_opportunities", lambda *args, **kwargs: {"injected_count": 0})
    monkeypatch.setattr(pipeline, "build_llm_diagnostics", lambda *args, **kwargs: {"warnings": []})
    monkeypatch.setattr(pipeline, "build_strategy_plan", lambda *args, **kwargs: {"approved_tasks": [], "memory_updates": {}})
    monkeypatch.setattr(pipeline, "validate_research_planner_proposal", lambda *args, **kwargs: {"accepted_tasks": []})
    monkeypatch.setattr(pipeline, "apply_strategy_plan", lambda *args, **kwargs: {"injected_count": 0, "injected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_metrics", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "build_research_attribution", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "DecisionProviderRouter", FakeRouter)

    def fake_planner_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        payload = {"schema_version": "factor_lab.planner_agent_brief.v1", "inputs": {"open_questions": [], "candidate_pool_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    def fake_failure_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        payload = {"schema_version": "factor_lab.failure_analyst_brief.v1", "inputs": {"recent_failed_or_risky_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    monkeypatch.setattr(pipeline, "build_planner_agent_brief", fake_planner_brief)
    monkeypatch.setattr(pipeline, "build_failure_analyst_brief", fake_failure_brief)

    def fake_subprocess_run(cmd, cwd, capture_output, text):
        assert (artifacts / "planner_agent_brief.json").exists()
        assert (artifacts / "failure_analyst_brief.json").exists()
        assert cmd[-2:] == ["--provider", "heuristic"]
        return type("Completed", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    monkeypatch.setattr(pipeline.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(
        pipeline,
        "load_validated_agent_responses",
        lambda *_args, **_kwargs: {
            "loaded_at_utc": "now",
            "planner": {"schema_version": "factor_lab.planner_agent_response.v1", "mode": "validate", "task_mix": {"baseline": 1, "validation": 1, "exploration": 0}, "priority_families": [], "suppress_families": [], "recommended_actions": []},
            "planner_errors": [],
            "failure_analyst": {"schema_version": "factor_lab.failure_analyst_response.v1", "failure_patterns": [], "should_stop": [], "should_probe": [], "should_reroute": []},
            "failure_analyst_errors": [],
        },
    )

    result = pipeline.run_research_planner_pipeline()

    assert result["agent_responses"]["planner_present"] is True
    assert result["agent_responses"]["failure_analyst_present"] is True
    assert result["agent_responses"]["configured_live_provider"] == "heuristic"
    assert result["agent_responses"]["configured_observation_provider"] == "heuristic"
    assert result["agent_responses"]["brief_runner"]["returncode"] == 0
    assert result["agent_responses"]["provider_health"]["live"]["configured_provider"] == "heuristic"
    assert result["agent_responses"]["provider_health"]["observation"]["configured_provider"] == "heuristic"
    assert FakeRouter.calls[0]["probe"] is False
    assert FakeRouter.calls[1]["probe"] is True


def test_observation_vs_live_separation_with_explicit_diagnostics(tmp_path, monkeypatch):
    """Test that observation provider can differ from live provider with explicit diagnostics."""
    monkeypatch.setattr(pipeline, "_root_path", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_artifacts_path", lambda: tmp_path / "artifacts")
    monkeypatch.setattr(pipeline, "_db_path", lambda: tmp_path / "artifacts" / "factor_lab.db")
    monkeypatch.setattr(pipeline, "_live_provider_health_path", lambda: tmp_path / "artifacts" / "llm_provider_health_live.json")
    monkeypatch.setattr(pipeline, "_observation_provider_health_path", lambda: tmp_path / "artifacts" / "llm_provider_health.json")
    
    # Set live to legacy OpenClaw, observation to generic/real_llm
    monkeypatch.setenv("FACTOR_LAB_LIVE_DECISION_PROVIDER", "legacy_openclaw_gateway")
    monkeypatch.setenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER", "real_llm")
    monkeypatch.delenv("FACTOR_LAB_DECISION_PROVIDER", raising=False)

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    FakeRouter.calls = []

    monkeypatch.setattr(pipeline, "build_research_space_registry", lambda *args, **kwargs: {"windows_covered": {}, "validation_depth": {}, "graveyard_diagnostics": {}})
    monkeypatch.setattr(pipeline, "build_research_space_map", lambda *args, **kwargs: {"family_progress": {}})
    monkeypatch.setattr(
        pipeline,
        "build_research_planner_snapshot",
        lambda *args, **kwargs: {
            "latest_run": {"run_id": "r1", "config_path": "cfg.json"},
            "latest_graveyard": [],
            "stable_candidates": [],
            "queue_budget": {},
            "failure_state": {},
            "exploration_state": {},
            "research_flow_state": {"state": "ready"},
            "knowledge_gain_counter": {},
            "promotion_scorecard": {"rows": []},
            "research_learning": {},
        },
    )
    monkeypatch.setattr(pipeline, "build_research_candidate_pool", lambda *args, **kwargs: {"tasks": [], "suppressed_tasks": []})
    monkeypatch.setattr(pipeline, "build_branch_planner_output", lambda *args, **kwargs: {"selected_families": []})
    monkeypatch.setattr(pipeline, "_maybe_skip_pipeline", lambda fingerprint: None)
    monkeypatch.setattr(pipeline, "build_recovery_tasks", lambda *args, **kwargs: {"tasks": []})
    monkeypatch.setattr(pipeline, "build_research_plan", lambda *args, **kwargs: {"selected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_state_snapshot", lambda *args, **kwargs: {"open_questions": []})
    monkeypatch.setattr(pipeline, "derive_research_flow_state", lambda **kwargs: {"state": "ready"})
    monkeypatch.setattr(pipeline, "build_research_opportunities", lambda *args, **kwargs: {"opportunities": []})
    monkeypatch.setattr(pipeline, "enqueue_opportunities", lambda *args, **kwargs: {"injected_count": 0})
    monkeypatch.setattr(pipeline, "build_llm_diagnostics", lambda *args, **kwargs: {"warnings": []})
    monkeypatch.setattr(pipeline, "build_strategy_plan", lambda *args, **kwargs: {"approved_tasks": [], "memory_updates": {}})
    monkeypatch.setattr(pipeline, "validate_research_planner_proposal", lambda *args, **kwargs: {"accepted_tasks": []})
    monkeypatch.setattr(pipeline, "apply_strategy_plan", lambda *args, **kwargs: {"injected_count": 0, "injected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_metrics", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "build_research_attribution", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "DecisionProviderRouter", FakeRouter)

    def fake_planner_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        payload = {"schema_version": "factor_lab.planner_agent_brief.v1", "inputs": {"open_questions": [], "candidate_pool_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    def fake_failure_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        payload = {"schema_version": "factor_lab.failure_analyst_brief.v1", "inputs": {"recent_failed_or_risky_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    monkeypatch.setattr(pipeline, "build_planner_agent_brief", fake_planner_brief)
    monkeypatch.setattr(pipeline, "build_failure_analyst_brief", fake_failure_brief)

    def fake_subprocess_run(cmd, cwd, capture_output, text):
        assert cmd[-2:] == ["--provider", "legacy_openclaw_gateway"]
        return type("Completed", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    monkeypatch.setattr(pipeline.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(
        pipeline,
        "load_validated_agent_responses",
        lambda *_args, **_kwargs: {
            "loaded_at_utc": "now",
            "planner": {
                "schema_version": "factor_lab.planner_agent_response.v1",
                "decision_metadata": {"effective_source": "legacy_openclaw_gateway"},
                "mode": "validate",
                "task_mix": {"baseline": 1, "validation": 1, "exploration": 0},
                "priority_families": [],
                "suppress_families": [],
                "recommended_actions": [],
            },
            "planner_errors": [],
            "failure_analyst": {
                "schema_version": "factor_lab.failure_analyst_response.v1",
                "decision_metadata": {"effective_source": "legacy_openclaw_gateway"},
                "failure_patterns": [],
                "should_stop": [],
                "should_probe": [],
                "should_reroute": [],
            },
            "failure_analyst_errors": [],
        },
    )

    result = pipeline.run_research_planner_pipeline()

    # Assert live provider can remain legacy OpenClaw
    assert result["agent_responses"]["configured_live_provider"] == "legacy_openclaw_gateway"
    # Assert observation provider can be generic/real_llm independently
    assert result["agent_responses"]["configured_observation_provider"] == "real_llm"
    # Assert planner effective source
    assert result["agent_responses"]["planner_source"] == "legacy_openclaw_gateway"
    # Assert failure analyst effective source
    assert result["agent_responses"]["failure_analyst_source"] == "legacy_openclaw_gateway"
    
    # Assert normalized provider health for both live and observation
    assert result["agent_responses"]["provider_health"]["live"]["normalized_provider"] == "legacy_openclaw_gateway"
    assert result["agent_responses"]["provider_health"]["observation"]["normalized_provider"] == "real_llm"
    assert result["agent_responses"]["provider_health"]["live"]["provider_class"] is not None
    assert result["agent_responses"]["provider_health"]["observation"]["provider_class"] is not None
    
    # Assert gray_mode marker when providers differ
    assert result["agent_responses"].get("gray_mode") == "observation_only"


def test_gray_mode_not_set_when_providers_match(tmp_path, monkeypatch):
    """Test that gray_mode is None when live and observation providers are the same."""
    monkeypatch.setattr(pipeline, "_root_path", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_artifacts_path", lambda: tmp_path / "artifacts")
    monkeypatch.setattr(pipeline, "_db_path", lambda: tmp_path / "artifacts" / "factor_lab.db")
    monkeypatch.setattr(pipeline, "_live_provider_health_path", lambda: tmp_path / "artifacts" / "llm_provider_health_live.json")
    monkeypatch.setattr(pipeline, "_observation_provider_health_path", lambda: tmp_path / "artifacts" / "llm_provider_health.json")
    
    # Set both to the same provider
    monkeypatch.setenv("FACTOR_LAB_LIVE_DECISION_PROVIDER", "heuristic")
    monkeypatch.setenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER", "heuristic")
    monkeypatch.delenv("FACTOR_LAB_DECISION_PROVIDER", raising=False)

    artifacts = tmp_path / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    FakeRouter.calls = []

    monkeypatch.setattr(pipeline, "build_research_space_registry", lambda *args, **kwargs: {"windows_covered": {}, "validation_depth": {}, "graveyard_diagnostics": {}})
    monkeypatch.setattr(pipeline, "build_research_space_map", lambda *args, **kwargs: {"family_progress": {}})
    monkeypatch.setattr(
        pipeline,
        "build_research_planner_snapshot",
        lambda *args, **kwargs: {
            "latest_run": {"run_id": "r1", "config_path": "cfg.json"},
            "latest_graveyard": [],
            "stable_candidates": [],
            "queue_budget": {},
            "failure_state": {},
            "exploration_state": {},
            "research_flow_state": {"state": "ready"},
            "knowledge_gain_counter": {},
            "promotion_scorecard": {"rows": []},
            "research_learning": {},
        },
    )
    monkeypatch.setattr(pipeline, "build_research_candidate_pool", lambda *args, **kwargs: {"tasks": [], "suppressed_tasks": []})
    monkeypatch.setattr(pipeline, "build_branch_planner_output", lambda *args, **kwargs: {"selected_families": []})
    monkeypatch.setattr(pipeline, "_maybe_skip_pipeline", lambda fingerprint: None)
    monkeypatch.setattr(pipeline, "build_recovery_tasks", lambda *args, **kwargs: {"tasks": []})
    monkeypatch.setattr(pipeline, "build_research_plan", lambda *args, **kwargs: {"selected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_state_snapshot", lambda *args, **kwargs: {"open_questions": []})
    monkeypatch.setattr(pipeline, "derive_research_flow_state", lambda **kwargs: {"state": "ready"})
    monkeypatch.setattr(pipeline, "build_research_opportunities", lambda *args, **kwargs: {"opportunities": []})
    monkeypatch.setattr(pipeline, "enqueue_opportunities", lambda *args, **kwargs: {"injected_count": 0})
    monkeypatch.setattr(pipeline, "build_llm_diagnostics", lambda *args, **kwargs: {"warnings": []})
    monkeypatch.setattr(pipeline, "build_strategy_plan", lambda *args, **kwargs: {"approved_tasks": [], "memory_updates": {}})
    monkeypatch.setattr(pipeline, "validate_research_planner_proposal", lambda *args, **kwargs: {"accepted_tasks": []})
    monkeypatch.setattr(pipeline, "apply_strategy_plan", lambda *args, **kwargs: {"injected_count": 0, "injected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_metrics", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "build_research_attribution", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "DecisionProviderRouter", FakeRouter)

    def fake_planner_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        payload = {"schema_version": "factor_lab.planner_agent_brief.v1", "inputs": {"open_questions": [], "candidate_pool_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    def fake_failure_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        payload = {"schema_version": "factor_lab.failure_analyst_brief.v1", "inputs": {"recent_failed_or_risky_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    monkeypatch.setattr(pipeline, "build_planner_agent_brief", fake_planner_brief)
    monkeypatch.setattr(pipeline, "build_failure_analyst_brief", fake_failure_brief)

    def fake_subprocess_run(cmd, cwd, capture_output, text):
        return type("Completed", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    monkeypatch.setattr(pipeline.subprocess, "run", fake_subprocess_run)
    monkeypatch.setattr(
        pipeline,
        "load_validated_agent_responses",
        lambda *_args, **_kwargs: {
            "loaded_at_utc": "now",
            "planner": {
                "schema_version": "factor_lab.planner_agent_response.v1",
                "decision_metadata": {"effective_source": "heuristic"},
                "mode": "validate",
                "task_mix": {"baseline": 1, "validation": 1, "exploration": 0},
                "priority_families": [],
                "suppress_families": [],
                "recommended_actions": [],
            },
            "planner_errors": [],
            "failure_analyst": {
                "schema_version": "factor_lab.failure_analyst_response.v1",
                "decision_metadata": {"effective_source": "heuristic"},
                "failure_patterns": [],
                "should_stop": [],
                "should_probe": [],
                "should_reroute": [],
            },
            "failure_analyst_errors": [],
        },
    )

    result = pipeline.run_research_planner_pipeline()

    # Assert both providers are the same
    assert result["agent_responses"]["configured_live_provider"] == "heuristic"
    assert result["agent_responses"]["configured_observation_provider"] == "heuristic"
    
    # Assert gray_mode is None when providers match
    assert result["agent_responses"].get("gray_mode") is None


def test_pipeline_uses_configurable_artifacts_dir_for_runtime_files(tmp_path, monkeypatch):
    artifacts = tmp_path / "custom-artifacts"
    monkeypatch.setenv("FACTOR_LAB_ARTIFACTS_DIR", str(artifacts))
    monkeypatch.setenv("FACTOR_LAB_DECISION_PROVIDER", "heuristic")
    monkeypatch.delenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LIVE_DECISION_PROVIDER", raising=False)
    FakeRouter.calls = []

    monkeypatch.setattr(pipeline, "build_research_space_registry", lambda *args, **kwargs: {"windows_covered": {}, "validation_depth": {}, "graveyard_diagnostics": {}})
    monkeypatch.setattr(pipeline, "build_research_space_map", lambda *args, **kwargs: {"family_progress": {}})
    monkeypatch.setattr(
        pipeline,
        "build_research_planner_snapshot",
        lambda *args, **kwargs: {
            "latest_run": {"run_id": "r1", "config_path": "cfg.json"},
            "latest_graveyard": [],
            "stable_candidates": [],
            "queue_budget": {},
            "failure_state": {},
            "exploration_state": {},
            "research_flow_state": {"state": "ready"},
            "knowledge_gain_counter": {},
            "promotion_scorecard": {"rows": []},
            "research_learning": {},
        },
    )
    monkeypatch.setattr(pipeline, "build_research_candidate_pool", lambda *args, **kwargs: {"tasks": [], "suppressed_tasks": []})
    monkeypatch.setattr(pipeline, "build_branch_planner_output", lambda *args, **kwargs: {"selected_families": []})
    monkeypatch.setattr(pipeline, "_maybe_skip_pipeline", lambda fingerprint: None)
    monkeypatch.setattr(pipeline, "build_recovery_tasks", lambda *args, **kwargs: {"tasks": []})
    monkeypatch.setattr(pipeline, "build_research_plan", lambda *args, **kwargs: {"selected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_state_snapshot", lambda *args, **kwargs: {"open_questions": []})
    monkeypatch.setattr(pipeline, "derive_research_flow_state", lambda **kwargs: {"state": "ready"})
    monkeypatch.setattr(pipeline, "build_research_opportunities", lambda *args, **kwargs: {"opportunities": []})
    monkeypatch.setattr(pipeline, "enqueue_opportunities", lambda *args, **kwargs: {"injected_count": 0})
    monkeypatch.setattr(pipeline, "build_llm_diagnostics", lambda *args, **kwargs: {"warnings": []})
    monkeypatch.setattr(pipeline, "build_strategy_plan", lambda *args, **kwargs: {"approved_tasks": [], "memory_updates": {}})
    monkeypatch.setattr(pipeline, "validate_research_planner_proposal", lambda *args, **kwargs: {"accepted_tasks": []})
    monkeypatch.setattr(pipeline, "apply_strategy_plan", lambda *args, **kwargs: {"injected_count": 0, "injected_tasks": []})
    monkeypatch.setattr(pipeline, "build_research_metrics", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "build_research_attribution", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(pipeline, "DecisionProviderRouter", FakeRouter)

    def fake_planner_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"schema_version": "factor_lab.planner_agent_brief.v1", "inputs": {"open_questions": [], "candidate_pool_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    def fake_failure_brief(**kwargs):
        output_path = Path(kwargs["output_path"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"schema_version": "factor_lab.failure_analyst_brief.v1", "inputs": {"recent_failed_or_risky_tasks": []}}
        output_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return payload

    monkeypatch.setattr(pipeline, "build_planner_agent_brief", fake_planner_brief)
    monkeypatch.setattr(pipeline, "build_failure_analyst_brief", fake_failure_brief)
    monkeypatch.setattr(
        pipeline,
        "load_validated_agent_responses",
        lambda *_args, **_kwargs: {
            "loaded_at_utc": "now",
            "planner": {"schema_version": "factor_lab.planner_agent_response.v1", "mode": "validate", "task_mix": {"baseline": 1, "validation": 1, "exploration": 0}, "priority_families": [], "suppress_families": [], "recommended_actions": []},
            "planner_errors": [],
            "failure_analyst": {"schema_version": "factor_lab.failure_analyst_response.v1", "failure_patterns": [], "should_stop": [], "should_probe": [], "should_reroute": []},
            "failure_analyst_errors": [],
        },
    )

    def fake_subprocess_run(cmd, cwd, capture_output, text):
        return type("Completed", (), {"returncode": 0, "stdout": "ok", "stderr": ""})()

    monkeypatch.setattr(pipeline.subprocess, "run", fake_subprocess_run)

    result = pipeline.run_research_planner_pipeline()

    assert (artifacts / "research_flow_state.json").exists()
    assert (artifacts / "agent_responses.json").exists()
    assert FakeRouter.calls[0]["output_path"] == str(artifacts / "llm_provider_health_live.json")
    assert FakeRouter.calls[1]["output_path"] == str(artifacts / "llm_provider_health.json")
    assert result["agent_responses"]["path"] == str(artifacts / "agent_responses.json")
