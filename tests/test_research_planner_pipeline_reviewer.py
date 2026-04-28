import json
from pathlib import Path

from factor_lab import research_planner_pipeline as pipeline


def test_pipeline_runs_reviewer_review_and_reports_artifact(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    monkeypatch.setattr(pipeline, "_root_path", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "_artifacts_path", lambda: artifacts)
    monkeypatch.setattr(pipeline, "_db_path", lambda: artifacts / "factor_lab.db")
    monkeypatch.setattr(pipeline, "_live_provider_health_path", lambda: artifacts / "llm_provider_health_live.json")
    monkeypatch.setattr(pipeline, "_observation_provider_health_path", lambda: artifacts / "llm_provider_health.json")
    monkeypatch.setenv("FACTOR_LAB_DECISION_PROVIDER", "heuristic")
    monkeypatch.delenv("FACTOR_LAB_OBSERVATION_DECISION_PROVIDER", raising=False)
    monkeypatch.delenv("FACTOR_LAB_LIVE_DECISION_PROVIDER", raising=False)
    artifacts.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(pipeline, "build_research_space_registry", lambda *args, **kwargs: {"windows_covered": {}, "validation_depth": {}, "graveyard_diagnostics": {}})
    monkeypatch.setattr(pipeline, "build_research_space_map", lambda *args, **kwargs: {"family_progress": {}})
    monkeypatch.setattr(
        pipeline,
        "build_research_planner_snapshot",
        lambda *args, **kwargs: {
            "latest_run": {"run_id": "r1", "config_path": "cfg.json", "dataset_rows": 100},
            "latest_graveyard": [],
            "stable_candidates": [],
            "queue_budget": {},
            "failure_state": {},
            "exploration_state": {},
            "research_flow_state": {"state": "ready"},
            "knowledge_gain_counter": {},
            "promotion_scorecard": {"rows": [{"factor_name": "alpha_1", "score": 0.8}]},
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
    monkeypatch.setattr(pipeline.DecisionProviderRouter, "healthcheck", lambda self, output_path=None, probe=True: {"normalized_provider": "heuristic", "provider_class": "generic"})
    monkeypatch.setattr(pipeline.subprocess, "run", lambda *args, **kwargs: type("Completed", (), {"returncode": 0, "stdout": "ok", "stderr": ""})())
    monkeypatch.setattr(pipeline, "build_planner_agent_brief", lambda **kwargs: {"schema_version": "factor_lab.planner_agent_brief.v1", "inputs": {"open_questions": [], "candidate_pool_tasks": []}})
    monkeypatch.setattr(pipeline, "build_failure_analyst_brief", lambda **kwargs: {"schema_version": "factor_lab.failure_analyst_brief.v1", "inputs": {"recent_failed_or_risky_tasks": []}})
    monkeypatch.setattr(
        pipeline,
        "load_validated_agent_responses",
        lambda *_args, **_kwargs: {"planner": {}, "planner_errors": [], "failure_analyst": {}, "failure_analyst_errors": []},
    )

    calls = []
    def fake_review(context, output_path=None, provider=None):
        calls.append({"context": context, "output_path": Path(output_path), "provider": provider})
        payload = {"schema_version": "factor_lab.reviewer_agent_response.v1", "decision_metadata": {"agent_role": "reviewer"}}
        Path(output_path).write_text(json.dumps(payload), encoding="utf-8")
        return payload

    monkeypatch.setattr(pipeline, "safe_run_reviewer_review", fake_review)

    result = pipeline.run_research_planner_pipeline()

    assert calls
    assert calls[0]["output_path"] == artifacts / "reviewer_review.json"
    assert calls[0]["provider"] == "heuristic"
    assert calls[0]["context"]["inputs"]["promotion_scorecard"]["rows"][0]["factor_name"] == "alpha_1"
    assert result["reviewer_review"]["path"] == str(artifacts / "reviewer_review.json")
    assert result["reviewer_review"]["schema_version"] == "factor_lab.reviewer_agent_response.v1"
