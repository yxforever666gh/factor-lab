from __future__ import annotations

from factor_lab.hermes_correction_state import (
    build_hermes_correction_state,
    hermes_correction_state_to_markdown,
)


def test_builder_defaults_to_conservative_manual_review_flags():
    state = build_hermes_correction_state(
        correction_id="test-correction",
        failure_target="portfolio_simulation_drawdown_blocker",
    )

    assert state["schema_version"] == 1
    assert state["correction_id"] == "test-correction"
    assert state["failure_target"] == "portfolio_simulation_drawdown_blocker"
    assert state["source_artifacts"] == []
    assert state["diagnosis"] == {}
    assert state["allowed_actions"] == []
    assert "no_queue_write" in state["forbidden_actions"]
    assert state["next_agent_role"] == "diagnostician"
    assert state["manual_review_required"] is True
    assert state["queue_write_allowed"] is False
    assert state["automation_allowed"] is False
    assert state["live_trading_enabled"] is False
    assert state["created_at_utc"].endswith("+00:00")


def test_builder_accepts_artifacts_diagnosis_and_role_without_relaxing_safety():
    state = build_hermes_correction_state(
        correction_id="x",
        failure_target="portfolio_simulation_drawdown_blocker",
        source_artifacts=[{"path": "artifacts/a.json", "present": True, "payload": {"status": "blocked"}}],
        diagnosis={"primary_blocker": "blocked_no_drawdown_safe_candidate"},
        allowed_actions=["run_pytest", "write_correction_artifacts"],
        next_agent_role="implementer",
        manual_review_required=False,
        queue_write_allowed=True,
        automation_allowed=True,
    )

    assert state["next_agent_role"] == "implementer"
    assert state["diagnosis"]["primary_blocker"] == "blocked_no_drawdown_safe_candidate"
    assert state["allowed_actions"] == ["run_pytest", "write_correction_artifacts"]
    assert state["manual_review_required"] is True
    assert state["queue_write_allowed"] is False
    assert state["automation_allowed"] is False
    assert state["live_trading_enabled"] is False


def test_state_markdown_surfaces_guardrails_and_next_role():
    state = build_hermes_correction_state(
        correction_id="x",
        failure_target="portfolio_simulation_drawdown_blocker",
        diagnosis={"next_action": "run_risk_reduction_controlled_executor"},
        next_agent_role="implementer",
    )

    markdown = hermes_correction_state_to_markdown(state)

    assert "Hermes Correction State" in markdown
    assert "next_agent_role: implementer" in markdown
    assert "manual_review_required: True" in markdown
    assert "queue_write_allowed: False" in markdown
    assert "run_risk_reduction_controlled_executor" in markdown
