from __future__ import annotations

import json

from factor_lab.market_phenomena_generator import seed_market_phenomena
from factor_lab.market_phenomena_memory import empty_phenomena_memory, upsert_phenomenon_verdict
from factor_lab.market_phenomena_novelty import (
    build_novelty_review,
    compute_similarity,
    novelty_review_to_markdown,
    review_candidate_novelty,
    write_novelty_review,
)


def candidate() -> dict:
    return seed_market_phenomena()[0]


def memory_with_same_candidate() -> dict:
    return upsert_phenomenon_verdict(
        empty_phenomena_memory(),
        {
            "phenomenon_id": "old_quality_repair",
            "title": "old",
            "verdict": "rejected_failed_verification",
            "mechanism_source": "information_delay",
            "participants": candidate()["participants"],
            "observable_variables": candidate()["observable_variables"],
            "prediction_target": candidate()["prediction_target"],
            "do_not_repeat": ["same mechanism failed"],
        },
    )


def test_similarity_is_high_for_same_mechanism_and_variables():
    score = compute_similarity(candidate(), memory_with_same_candidate()["phenomena"][0])
    assert score >= 0.70


def test_similarity_is_low_for_different_mechanism():
    other = {
        "phenomenon_id": "x",
        "mechanism_source": "liquidity_gap",
        "participants": ["流动性受限持有人"],
        "observable_variables": ["volume", "turnover_rate"],
        "prediction_target": "future_20d_return_distribution",
    }
    score = compute_similarity(candidate(), other)
    assert score < 0.55


def test_review_candidate_rejects_duplicate():
    result = review_candidate_novelty(candidate(), memory_with_same_candidate())
    assert result["decision"] == "reject_duplicate"
    assert result["mechanism_similarity_max"] >= 0.70
    assert result["most_similar_phenomenon_id"] == "old_quality_repair"


def test_review_candidate_keeps_novel_candidate():
    result = review_candidate_novelty(candidate(), empty_phenomena_memory())
    assert result["decision"] == "keep"
    assert result["mechanism_similarity_max"] == 0.0


def test_build_novelty_review_preserves_safety_flags():
    review = build_novelty_review(
        run_id="n",
        quality_review={"reviewed_phenomena": [{"phenomenon_id": candidate()["phenomenon_id"], "decision": "keep"}]},
        candidates_report={"phenomena": [candidate()]},
        memory=empty_phenomena_memory(),
    )
    assert review["summary"]["keep"] == 1
    assert review["strategy_generation_allowed"] is False
    assert review["backtest_allowed"] is False
    assert review["queue_write_allowed"] is False


def test_novelty_review_markdown_and_write(tmp_path):
    review = build_novelty_review(
        run_id="n",
        quality_review={"reviewed_phenomena": [{"phenomenon_id": candidate()["phenomenon_id"], "decision": "keep"}]},
        candidates_report={"phenomena": [candidate()]},
        memory=empty_phenomena_memory(),
    )
    markdown = novelty_review_to_markdown(review)
    assert "Market Phenomenon Novelty Review" in markdown
    paths = write_novelty_review(review, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["summary"]["keep"] == 1
