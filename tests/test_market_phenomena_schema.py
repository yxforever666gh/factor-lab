from __future__ import annotations

from factor_lab.market_phenomena_schema import (
    build_candidates_report,
    candidates_to_markdown,
    score_phenomenon,
    validate_phenomenon,
    write_candidates_report,
)


def valid_phenomenon() -> dict:
    return {
        "phenomenon_id": "quality_repair_delayed_repricing_v1",
        "title": "盈利质量修复后的延迟重估",
        "mechanism_source": "information_delay",
        "participants": ["低频基本面资金", "覆盖不足股票投资者"],
        "participant_constraints": ["信息处理慢", "财报可信度折价"],
        "behavioral_story": "盈利质量改善已出现在 PIT 财务数据中，但部分投资者更新较慢。",
        "temporary_mispricing_reason": "覆盖不足和风险偏好约束导致价格未同步反映质量修复。",
        "why_not_immediately_arbitraged": "容量有限、信息处理成本高，且财报质量需要等待确认。",
        "observable_variables": ["profit_yoy", "roe", "debt_to_asset", "pb"],
        "prediction_target": "future_60d_relative_return_distribution",
        "expected_horizon": "60d/120d",
        "market_states_where_stronger": ["行业风险偏好修复"],
        "failure_conditions": ["估值已提前修复", "行业趋势转负"],
        "minimal_verification_question": "质量修复且估值压制的股票未来收益分布是否优于对照组？",
        "indicator_translation": {"pb": "估值压制代理，不是策略本身"},
        "scores": {
            "mechanism_strength": 8,
            "observability": 7,
            "testability": 7,
            "tradability_potential": 6,
            "novelty": 6,
            "crowding_risk": 5,
            "overfit_risk": 4,
            "cost_sensitivity": 5,
        },
    }


def test_complete_phenomenon_passes_validation():
    result = validate_phenomenon(valid_phenomenon())
    assert result["decision"] == "candidate"
    assert result["reason_codes"] == []


def test_missing_participants_rejects():
    item = valid_phenomenon()
    item["participants"] = []
    result = validate_phenomenon(item)
    assert result["decision"] == "reject"
    assert "missing_participants" in result["reason_codes"]


def test_missing_temporary_mispricing_reason_rejects():
    item = valid_phenomenon()
    item["temporary_mispricing_reason"] = ""
    result = validate_phenomenon(item)
    assert result["decision"] == "reject"
    assert "missing_temporary_mispricing_reason" in result["reason_codes"]


def test_missing_why_not_arbitraged_rejects():
    item = valid_phenomenon()
    item["why_not_immediately_arbitraged"] = ""
    result = validate_phenomenon(item)
    assert result["decision"] == "reject"
    assert "missing_why_not_immediately_arbitraged" in result["reason_codes"]


def test_forbidden_indicator_core_logic_rejects():
    item = valid_phenomenon()
    item["behavioral_story"] = "RSI 超卖后买入。"
    result = validate_phenomenon(item)
    assert result["decision"] == "reject"
    assert "forbidden_indicator_core_logic" in result["reason_codes"]
    assert "RSI" in result["forbidden_core_logic_terms"]


def test_direct_buy_sell_rule_rejects():
    item = valid_phenomenon()
    item["buy_rule"] = "buy when signal is high"
    result = validate_phenomenon(item)
    assert result["decision"] == "reject"
    assert "direct_strategy_rule_detected" in result["reason_codes"]


def test_score_calculation_is_deterministic():
    scores = score_phenomenon(valid_phenomenon())
    expected = 2 * 8 + 1.5 * 7 + 1.5 * 7 + 1.5 * 6 + 6 - 1.5 * 5 - 2 * 4 - 5
    assert scores["total_score"] == expected


def test_candidates_report_markdown_and_write(tmp_path):
    report = build_candidates_report(run_id="r", market="cn_equity_daily", phenomena=[valid_phenomenon()])
    assert report["strategy_generation_allowed"] is False
    assert report["backtest_allowed"] is False
    assert report["queue_write_allowed"] is False
    assert report["phenomena"][0]["hard_gate_decision"] == "candidate"
    markdown = candidates_to_markdown(report)
    assert "Market Phenomenon Candidates" in markdown
    assert "quality_repair_delayed_repricing_v1" in markdown
    paths = write_candidates_report(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
