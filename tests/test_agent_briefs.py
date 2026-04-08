from factor_lab.agent_briefs import build_planner_agent_brief


def test_planner_agent_brief_includes_candidate_hypothesis_cards(tmp_path):
    snapshot = {
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "book_yield",
                    "family": "value",
                    "quality_summary": "需要继续验证中长窗稳定性",
                    "quality_classification": "needs-validation",
                    "quality_scores": {"incremental_value": 14, "cross_window_robustness": 18},
                    "quality_hard_flags": {"insufficient_window_evidence": True},
                }
            ]
        }
    }
    output = tmp_path / "planner_brief.json"
    payload = build_planner_agent_brief(snapshot, {"tasks": []}, {}, {}, {}, output)

    cards = payload["inputs"]["candidate_hypothesis_cards"]
    assert len(cards) == 1
    assert cards[0]["candidate_name"] == "book_yield"
    assert cards[0]["target_window"] in {"recent_extension", "medium_horizon"}
    assert "incremental_value_thesis" in cards[0]
