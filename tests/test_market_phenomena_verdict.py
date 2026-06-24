from __future__ import annotations

import json

from factor_lab.market_phenomena_verdict import (
    build_phenomenon_verdict_report,
    phenomenon_verdict_to_markdown,
    verdict_for_experiment_result,
    write_phenomenon_verdict_report,
)


def failed_result():
    return {
        "phenomenon_id": "quality_repair_delayed_repricing_v1",
        "title": "盈利质量修复后的延迟重估",
        "result_status": "fail",
        "spread_vs_control": -0.0028,
        "target_group": "quality_repair_low_valuation",
        "usable_row_count": 110851,
        "usable_ticker_count": 93,
        "groups": {"quality_repair_low_valuation": {"downside_risk": 0.58}},
    }


def passed_result():
    return {
        "phenomenon_id": "value_trap_escape_after_balance_sheet_repair_v1",
        "title": "资产负债表修复后的价值陷阱脱离",
        "result_status": "pass",
        "spread_vs_control": 0.0275,
        "target_group": "balance_sheet_repair_low_valuation",
        "usable_row_count": 109613,
        "usable_ticker_count": 93,
        "groups": {"balance_sheet_repair_low_valuation": {"downside_risk": 0.54}},
    }


def test_failed_result_becomes_rejected_verdict():
    verdict = verdict_for_experiment_result(failed_result())
    assert verdict["verdict"] == "rejected_failed_verification"
    assert verdict["strategy_design_allowed"] is False
    assert "negative_or_zero_spread_vs_control" in verdict["reason_codes"]
    assert any("不要" in item or "do not" in item for item in verdict["do_not_repeat"])


def test_passed_result_becomes_supported_for_further_research_not_strategy():
    verdict = verdict_for_experiment_result(passed_result())
    assert verdict["verdict"] == "supported_for_further_research"
    assert verdict["strategy_design_allowed"] is False
    assert verdict["human_approval_required_for_strategy_phase"] is True
    assert "positive_spread_vs_control" in verdict["reason_codes"]
    assert verdict["next_research_question"]


def test_blocked_result_becomes_manual_or_blocked_verdict():
    result = failed_result()
    result["result_status"] = "blocked_missing_columns"
    result["missing_columns"] = ["x"]
    verdict = verdict_for_experiment_result(result)
    assert verdict["verdict"] == "blocked_missing_data"
    assert verdict["strategy_design_allowed"] is False


def test_build_verdict_report_summarizes_all_results_and_preserves_safety_flags():
    report = build_phenomenon_verdict_report(run_id="v", minimal_result_report={"results": [failed_result(), passed_result()]})
    assert report["summary"]["rejected_failed_verification"] == 1
    assert report["summary"]["supported_for_further_research"] == 1
    assert report["strategy_generation_allowed"] is False
    assert report["backtest_allowed"] is False
    assert report["queue_write_allowed"] is False


def test_verdict_markdown_and_write(tmp_path):
    report = build_phenomenon_verdict_report(run_id="v", minimal_result_report={"results": [failed_result(), passed_result()]})
    markdown = phenomenon_verdict_to_markdown(report)
    assert "Market Phenomenon Verdict" in markdown
    assert "value_trap_escape_after_balance_sheet_repair_v1" in markdown
    paths = write_phenomenon_verdict_report(report, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["supported_for_further_research"] == 1
