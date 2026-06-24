from __future__ import annotations

from factor_lab.market_phenomena_generator import build_seed_candidates_report, seed_market_phenomena
from factor_lab.market_phenomena_schema import validate_phenomenon


def test_seed_market_phenomena_are_not_empty_and_are_not_strategies():
    phenomena = seed_market_phenomena()
    assert len(phenomena) == 5
    assert {item["phenomenon_id"] for item in phenomena} >= {
        "quality_repair_delayed_repricing_v1",
        "value_trap_escape_after_balance_sheet_repair_v1",
        "industry_cycle_confirmation_lag_v1",
        "coverage_neglect_post_report_drift_v1",
        "liquidity_discount_reversal_after_volume_recovery_v1",
    }
    for item in phenomena:
        result = validate_phenomenon(item)
        assert result["decision"] == "candidate", result
        assert "buy_rule" not in item
        assert "sell_rule" not in item


def test_build_seed_candidates_report_preserves_safety_flags():
    report = build_seed_candidates_report(run_id="seed")
    assert report["run_id"] == "seed"
    assert report["market"] == "cn_equity_daily"
    assert report["mode"] == "research_artifact_only"
    assert report["strategy_generation_allowed"] is False
    assert report["backtest_allowed"] is False
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False
    assert report["timer_enable_allowed"] is False
    assert all(item["hard_gate_decision"] == "candidate" for item in report["phenomena"])
