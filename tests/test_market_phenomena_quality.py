from __future__ import annotations

import json

from factor_lab.market_phenomena_generator import seed_market_phenomena
from factor_lab.market_phenomena_quality import (
    build_quality_review,
    quality_review_to_markdown,
    review_phenomenon_quality,
    write_quality_review,
)


def valid_candidate() -> dict:
    return seed_market_phenomena()[0]


def test_quality_review_keeps_valid_candidate():
    result = review_phenomenon_quality(valid_candidate())
    assert result["decision"] == "keep"
    assert result["phenomenon_id"] == "quality_repair_delayed_repricing_v1"
    assert result["reason_codes"] == []


def test_quality_review_rejects_indicator_disguised_as_mechanism():
    candidate = valid_candidate()
    candidate["behavioral_story"] = "RSI 超卖后会反弹，这是核心逻辑。"
    result = review_phenomenon_quality(candidate)
    assert result["decision"] == "reject_indicator_disguised_as_mechanism"
    assert "forbidden_indicator_core_logic" in result["reason_codes"]


def test_quality_review_rejects_direct_strategy_rule():
    candidate = valid_candidate()
    candidate["buy_rule"] = "buy when score is high"
    result = review_phenomenon_quality(candidate)
    assert result["decision"] == "reject_strategy_disguised_as_phenomenon"
    assert "direct_strategy_rule_detected" in result["reason_codes"]


def test_quality_review_rejects_missing_mechanism_fields():
    candidate = valid_candidate()
    candidate["participants"] = []
    result = review_phenomenon_quality(candidate)
    assert result["decision"] == "reject_missing_required_mechanism"
    assert "missing_participants" in result["reason_codes"]


def test_build_quality_review_preserves_safety_flags():
    review = build_quality_review(run_id="q", candidates_report={"phenomena": seed_market_phenomena()})
    assert review["run_id"] == "q"
    assert len(review["reviewed_phenomena"]) == 5
    assert review["summary"]["keep"] == 5
    assert review["strategy_generation_allowed"] is False
    assert review["backtest_allowed"] is False
    assert review["queue_write_allowed"] is False


def test_quality_review_markdown_and_write(tmp_path):
    review = build_quality_review(run_id="q", candidates_report={"phenomena": seed_market_phenomena()})
    markdown = quality_review_to_markdown(review)
    assert "Market Phenomenon Quality Review" in markdown
    assert "quality_repair_delayed_repricing_v1" in markdown
    paths = write_quality_review(review, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["keep"] == 5
