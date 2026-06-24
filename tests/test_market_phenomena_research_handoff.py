from __future__ import annotations

import json

from factor_lab.market_phenomena_research_handoff import (
    build_research_handoff,
    handoff_for_supported_verdict,
    research_handoff_to_markdown,
    write_research_handoff,
)


def supported_verdict():
    return {
        "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
        "title": "资产负债表修复后的价值陷阱脱离",
        "verdict": "supported_for_further_research",
        "spread_vs_control": 0.0275,
        "target_group": "balance_sheet_repair_low_valuation",
        "next_research_question": "Does it survive splits?",
        "usable_row_count": 109613,
        "usable_ticker_count": 93,
    }


def rejected_verdict():
    item = supported_verdict()
    item["phenomenon_id"] = "quality_repair_delayed_repricing_v1"
    item["verdict"] = "rejected_failed_verification"
    item["spread_vs_control"] = -0.0028
    return item


def test_handoff_for_supported_verdict_opens_controlled_research_not_production():
    handoff = handoff_for_supported_verdict(supported_verdict())
    assert handoff["handoff_status"] == "ready_for_controlled_research_backtest"
    assert handoff["controlled_research_backtest_allowed"] is True
    assert handoff["strategy_generation_allowed"] is True
    assert handoff["queue_write_allowed"] is False
    assert handoff["timer_enable_allowed"] is False
    assert handoff["daemon_restore_allowed"] is False
    assert handoff["auto_promotion_allowed"] is False
    assert handoff["live_trading_allowed"] is False
    assert "industry_split_robustness" in handoff["research_tasks"]
    assert "drawdown_sensitivity" in handoff["research_tasks"]
    assert "factor_definition_mutation" in handoff["research_tasks"]


def test_build_research_handoff_skips_rejected_verdicts():
    report = build_research_handoff(run_id="h", verdict_report={"verdicts": [supported_verdict(), rejected_verdict()]})
    assert report["summary"]["ready_for_controlled_research_backtest"] == 1
    assert report["summary"]["skipped_not_supported"] == 1
    assert len(report["handoffs"]) == 1
    assert len(report["skipped_verdicts"]) == 1
    assert report["live_trading_allowed"] is False
    assert report["auto_promotion_allowed"] is False


def test_research_handoff_markdown_and_write(tmp_path):
    report = build_research_handoff(run_id="h", verdict_report={"verdicts": [supported_verdict()]})
    markdown = research_handoff_to_markdown(report)
    assert "Controlled Research / Backtest Handoff" in markdown
    assert "not live trading" in markdown
    paths = write_research_handoff(report, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["ready_for_controlled_research_backtest"] == 1
