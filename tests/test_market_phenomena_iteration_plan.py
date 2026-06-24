from __future__ import annotations

import json

from factor_lab.market_phenomena_iteration_plan import (
    build_agent_iteration_plan,
    validate_agent_iteration_plan,
    iteration_plan_to_markdown,
    write_agent_iteration_plan,
)


def worker_contract():
    return {
        "run_id": "contract",
        "target_phenomena": ["value_trap_escape_after_balance_sheet_repair_v1"],
        "target_handoffs": [
            {
                "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
                "title": "资产负债表修复后的价值陷阱脱离",
                "target_group": "balance_sheet_repair_low_valuation",
                "next_research_question": "Does it survive splits?",
                "research_tasks": ["industry_split_robustness", "drawdown_sensitivity", "factor_definition_mutation"],
                "iteration_policy": {
                    "if_drawdown_fails": ["add_regime_filter"],
                    "if_return_fails": ["mutate_factor_definition"],
                    "if_split_unstable": ["keep_only_supported_regime"],
                },
                "spread_vs_control": 0.0275,
                "usable_row_count": 109613,
                "usable_ticker_count": 93,
            }
        ],
        "required_plan_sections": [
            "mechanism_hypothesis",
            "participant_logic",
            "observable_variables",
            "data_feasibility_assumptions",
            "controlled_research_backtest_design",
            "split_regime_tests",
            "drawdown_failure_diagnostics",
            "mutation_logic",
            "stop_conditions",
            "artifact_write_plan",
        ],
        "required_output_artifacts": [
            "agent_iteration_plan.json",
            "agent_iteration_plan.md",
            "research_execution_request.json",
            "research_verification_checklist.md",
        ],
        "closed_gates": {"live_trading_allowed": False, "queue_write_allowed": False, "timer_enable_allowed": False, "daemon_restore_allowed": False, "auto_promotion_allowed": False},
    }


def test_iteration_plan_contains_every_contract_required_section():
    plan = build_agent_iteration_plan(run_id="plan", worker_contract=worker_contract())
    assert plan["mode"] == "agent_generated_iteration_plan"
    assert plan["source_worker_contract_run_id"] == "contract"
    assert plan["phenomenon_id"] == "value_trap_escape_after_balance_sheet_repair_v1"
    for section in worker_contract()["required_plan_sections"]:
        assert section in plan
        assert plan[section]
    assert plan["production_boundaries"]["live_trading_allowed"] is False
    assert plan["controlled_execution_allowed"] is True


def test_iteration_plan_is_phenomenon_first_not_strategy_count_or_low_level_ta():
    plan = build_agent_iteration_plan(run_id="plan", worker_contract=worker_contract())
    text = json.dumps(plan, ensure_ascii=False)
    assert "strategy_count" not in text
    assert "RSI" not in text
    assert "MACD" not in text
    assert "Bollinger" not in text
    assert "forced_seller" in text or "capital_constraint" in text


def test_validate_iteration_plan_rejects_missing_sections_and_closed_gate_violations():
    plan = build_agent_iteration_plan(run_id="plan", worker_contract=worker_contract())
    del plan["participant_logic"]
    plan["production_boundaries"]["queue_write_allowed"] = True
    result = validate_agent_iteration_plan(plan, worker_contract())
    assert result["decision"] == "reject"
    assert "missing_required_section_participant_logic" in result["reason_codes"]
    assert "production_gate_not_closed_queue_write_allowed" in result["reason_codes"]


def test_validate_iteration_plan_rejects_low_level_ta_core_logic():
    plan = build_agent_iteration_plan(run_id="plan", worker_contract=worker_contract())
    plan["mechanism_hypothesis"] = "Use RSI reversal after balance sheet repair."
    result = validate_agent_iteration_plan(plan, worker_contract())
    assert result["decision"] == "reject"
    assert "forbidden_low_level_ta_term_RSI" in result["reason_codes"]


def test_iteration_plan_markdown_and_artifact_writes(tmp_path):
    plan = build_agent_iteration_plan(run_id="plan", worker_contract=worker_contract())
    assert validate_agent_iteration_plan(plan, worker_contract())["decision"] == "keep"
    markdown = iteration_plan_to_markdown(plan)
    assert "Agent-generated Iteration Plan" in markdown
    assert "资产负债表修复后的价值陷阱脱离" in markdown
    paths = write_agent_iteration_plan(plan, tmp_path)
    assert paths["plan_json"].exists()
    assert paths["plan_markdown"].exists()
    assert paths["execution_request_json"].exists()
    assert paths["verification_checklist_markdown"].exists()
    payload = json.loads(paths["execution_request_json"].read_text(encoding="utf-8"))
    assert payload["controlled_research_backtest_allowed"] is True
    checklist = paths["verification_checklist_markdown"].read_text(encoding="utf-8")
    assert "No production queue writes" in checklist
