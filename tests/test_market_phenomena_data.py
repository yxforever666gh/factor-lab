from __future__ import annotations

import json

from factor_lab.market_phenomena_data import (
    build_data_feasibility_review,
    data_feasibility_to_markdown,
    review_candidate_data_feasibility,
    update_data_requests,
    write_data_feasibility_review,
)


def candidate(**overrides):
    item = {
        "phenomenon_id": "quality_repair_delayed_repricing_v1",
        "title": "盈利质量修复后的延迟重估",
        "observable_variables": ["profit_yoy", "roe", "pb"],
        "prediction_target": "future_60d_relative_return_distribution",
        "expected_horizon": "60d/120d",
    }
    item.update(overrides)
    return item


def data_catalog(**overrides):
    catalog = {
        "available_fields": ["profit_yoy", "roe", "pb", "date", "ticker", "future_60d_return"],
        "pit_fields": ["profit_yoy", "roe"],
        "coverage_by_field": {"profit_yoy": 0.92, "roe": 0.91, "pb": 0.98, "future_60d_return": 0.96},
        "row_count": 50000,
        "ticker_count": 300,
        "target_horizons": ["20d", "60d", "120d"],
    }
    catalog.update(overrides)
    return catalog


def test_data_feasibility_ready_when_fields_coverage_and_horizon_exist():
    result = review_candidate_data_feasibility(candidate(), data_catalog())
    assert result["decision"] == "ready_for_minimal_verification"
    assert result["missing_fields"] == []
    assert result["low_coverage_fields"] == []
    assert result["leakage_risk_fields"] == []


def test_data_feasibility_blocks_missing_fields():
    result = review_candidate_data_feasibility(candidate(observable_variables=["profit_yoy", "analyst_coverage_proxy"]), data_catalog())
    assert result["decision"] == "blocked_missing_data"
    assert result["missing_fields"] == ["analyst_coverage_proxy"]


def test_data_feasibility_blocks_low_coverage():
    catalog = data_catalog(coverage_by_field={"profit_yoy": 0.30, "roe": 0.91, "pb": 0.98, "future_60d_return": 0.96})
    result = review_candidate_data_feasibility(candidate(), catalog, min_coverage=0.60)
    assert result["decision"] == "blocked_low_coverage"
    assert result["low_coverage_fields"] == ["profit_yoy"]


def test_data_feasibility_blocks_future_leakage_fields():
    result = review_candidate_data_feasibility(candidate(observable_variables=["profit_yoy", "future_60d_return"]), data_catalog())
    assert result["decision"] == "blocked_leakage_risk"
    assert result["leakage_risk_fields"] == ["future_60d_return"]


def test_data_feasibility_blocks_missing_horizon():
    result = review_candidate_data_feasibility(candidate(prediction_target="future_240d_return_distribution"), data_catalog())
    assert result["decision"] == "blocked_missing_target_horizon"
    assert result["missing_target_horizons"] == ["240d"]


def test_build_review_only_uses_kept_novel_phenomena_and_preserves_safety_flags():
    review = build_data_feasibility_review(
        run_id="d",
        candidates_report={"phenomena": [candidate(), candidate(phenomenon_id="blocked", observable_variables=["missing_x"])]},
        novelty_review={"reviewed_phenomena": [{"phenomenon_id": "quality_repair_delayed_repricing_v1", "decision": "keep"}]},
        data_catalog=data_catalog(),
    )
    assert len(review["reviewed_phenomena"]) == 1
    assert review["summary"]["ready_for_minimal_verification"] == 1
    assert review["strategy_generation_allowed"] is False
    assert review["backtest_allowed"] is False
    assert review["queue_write_allowed"] is False


def test_update_data_requests_adds_missing_and_low_coverage_blockers():
    review = build_data_feasibility_review(
        run_id="d",
        candidates_report={"phenomena": [candidate(observable_variables=["profit_yoy", "missing_x"])]},
        novelty_review={"reviewed_phenomena": [{"phenomenon_id": "quality_repair_delayed_repricing_v1", "decision": "keep"}]},
        data_catalog=data_catalog(),
    )
    requests = update_data_requests({"schema_version": 1, "requests": []}, review)
    assert requests["requests"][0]["phenomenon_id"] == "quality_repair_delayed_repricing_v1"
    assert requests["requests"][0]["missing_fields"] == ["missing_x"]


def test_data_feasibility_markdown_and_write(tmp_path):
    review = build_data_feasibility_review(
        run_id="d",
        candidates_report={"phenomena": [candidate()]},
        novelty_review={"reviewed_phenomena": [{"phenomenon_id": "quality_repair_delayed_repricing_v1", "decision": "keep"}]},
        data_catalog=data_catalog(),
    )
    markdown = data_feasibility_to_markdown(review)
    assert "Market Phenomenon Data Feasibility" in markdown
    paths = write_data_feasibility_review(review, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["ready_for_minimal_verification"] == 1
