from __future__ import annotations

import json

from factor_lab.market_phenomena_agent_policy import (
    build_agent_policy,
    agent_policy_to_markdown,
    validate_agent_policy,
    write_agent_policy,
)


def test_agent_policy_allows_agent_to_choose_research_decisions():
    policy = build_agent_policy(run_id="p")
    allowed = set(policy["agent_may_choose"])
    assert "mechanism_hypotheses" in allowed
    assert "observable_variables" in allowed
    assert "controlled_research_backtest_designs" in allowed
    assert "failure_drawdown_diagnoses" in allowed
    assert "next_generation_hypotheses" in allowed


def test_agent_policy_forbids_production_and_low_level_ta_core_logic():
    policy = build_agent_policy(run_id="p")
    forbidden = set(policy["agent_must_not"])
    assert "live_trading" in forbidden
    assert "production_queue_writes" in forbidden
    assert "timer_daemon_restore" in forbidden
    assert "auto_promotion" in forbidden
    assert "low_level_ta_core_logic" in forbidden
    assert {"Bollinger", "MA cross", "RSI", "MACD"}.issubset(set(policy["forbidden_core_logic_terms"]))


def test_agent_policy_defines_assistant_role_as_boundary_not_research_author():
    policy = build_agent_policy(run_id="p")
    assert policy["assistant_role"] == "boundary_tool_artifact_verifier"
    assert "manual_assistant_research_task_lists_masquerading_as_autonomy" in policy["agent_must_not"]
    assert policy["production_gates"]["live_trading_allowed"] is False
    assert policy["research_gates"]["controlled_research_backtest_allowed_after_evidence_gate"] is True


def test_validate_agent_policy_rejects_missing_forbidden_ta_terms():
    policy = build_agent_policy(run_id="p")
    policy["forbidden_core_logic_terms"] = ["RSI"]
    result = validate_agent_policy(policy)
    assert result["decision"] == "reject"
    assert "missing_required_forbidden_core_logic_terms" in result["reason_codes"]


def test_agent_policy_markdown_and_write(tmp_path):
    policy = build_agent_policy(run_id="p")
    result = validate_agent_policy(policy)
    assert result["decision"] == "keep"
    markdown = agent_policy_to_markdown(policy)
    assert "Market Phenomena Agent Policy" in markdown
    assert "Agent may choose" in markdown
    assert "not live trading" in markdown
    paths = write_agent_policy(policy, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["run_id"] == "p"
