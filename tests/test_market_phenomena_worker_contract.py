from __future__ import annotations

import json

from factor_lab.market_phenomena_worker_contract import (
    build_worker_contract,
    validate_worker_contract,
    worker_contract_to_markdown,
    write_worker_contract,
)


def agent_policy():
    return {
        "run_id": "policy",
        "agent_role": "autonomous_market_phenomena_researcher",
        "assistant_role": "boundary_tool_artifact_verifier",
        "agent_may_choose": ["mechanism_hypotheses", "observable_variables", "controlled_research_backtest_designs", "failure_drawdown_diagnoses", "next_generation_hypotheses"],
        "agent_must_not": ["live_trading", "production_queue_writes", "timer_daemon_restore", "auto_promotion", "low_level_ta_core_logic", "manual_assistant_research_task_lists_masquerading_as_autonomy"],
        "forbidden_core_logic_terms": ["Bollinger", "MA cross", "RSI", "MACD", "KDJ", "grid", "martingale"],
        "production_gates": {"live_trading_allowed": False, "queue_write_allowed": False, "timer_enable_allowed": False, "daemon_restore_allowed": False, "auto_promotion_allowed": False},
    }


def research_handoff():
    return {
        "run_id": "handoff",
        "handoffs": [
            {
                "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
                "title": "资产负债表修复后的价值陷阱脱离",
                "handoff_status": "ready_for_controlled_research_backtest",
                "spread_vs_control": 0.0275,
                "target_group": "balance_sheet_repair_low_valuation",
                "next_research_question": "Does it survive splits?",
                "controlled_research_backtest_allowed": True,
                "strategy_generation_allowed": True,
                "live_trading_allowed": False,
                "queue_write_allowed": False,
            }
        ],
    }


def test_worker_contract_includes_policy_handoff_and_required_outputs():
    contract = build_worker_contract(
        run_id="w",
        agent_policy=agent_policy(),
        research_handoff=research_handoff(),
        phenomenon_verdict={"run_id": "verdict"},
        minimal_result={"run_id": "result"},
        lessons_markdown="lessons",
        data_catalog_summary={"available_field_count": 10},
    )
    assert contract["worker_role"] == "hermes_market_phenomena_research_worker"
    assert contract["source_artifacts"]["agent_policy_run_id"] == "policy"
    assert contract["target_phenomena"] == ["value_trap_escape_after_balance_sheet_repair_v1"]
    assert "agent_iteration_plan.json" in contract["required_output_artifacts"]
    assert "controlled_research_backtest_design" in contract["required_plan_sections"]
    assert contract["model_provider_pinning_allowed"] is False


def test_worker_contract_prompt_forbids_low_level_ta_and_production_actions():
    contract = build_worker_contract(
        run_id="w",
        agent_policy=agent_policy(),
        research_handoff=research_handoff(),
        phenomenon_verdict={},
        minimal_result={},
        lessons_markdown="lessons",
        data_catalog_summary={},
    )
    prompt = contract["worker_prompt"]
    assert "Do not use Bollinger" in prompt
    assert "Do not use RSI" in prompt
    assert "Do not write production queue" in prompt
    assert "You choose the research plan" in prompt


def test_validate_worker_contract_rejects_missing_target_phenomena():
    contract = build_worker_contract(
        run_id="w",
        agent_policy=agent_policy(),
        research_handoff={"handoffs": []},
        phenomenon_verdict={},
        minimal_result={},
        lessons_markdown="lessons",
        data_catalog_summary={},
    )
    result = validate_worker_contract(contract)
    assert result["decision"] == "reject"
    assert "missing_target_phenomena" in result["reason_codes"]


def test_worker_contract_markdown_and_write(tmp_path):
    contract = build_worker_contract(
        run_id="w",
        agent_policy=agent_policy(),
        research_handoff=research_handoff(),
        phenomenon_verdict={},
        minimal_result={},
        lessons_markdown="lessons",
        data_catalog_summary={},
    )
    assert validate_worker_contract(contract)["decision"] == "keep"
    markdown = worker_contract_to_markdown(contract)
    assert "Hermes Research Worker Contract" in markdown
    assert "hermes chat -q" in markdown
    paths = write_worker_contract(contract, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["run_id"] == "w"
