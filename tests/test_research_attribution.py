import json
from datetime import datetime, timezone

from factor_lab.research_attribution import build_research_attribution


def test_build_research_attribution_outputs_dual_pool_windows(tmp_path):
    memory_path = tmp_path / "research_memory.json"
    learning_path = tmp_path / "research_learning.json"
    candidate_pool_path = tmp_path / "research_candidate_pool.json"
    candidate_generation_plan_path = tmp_path / "candidate_generation_plan.json"
    promotion_scorecard_path = tmp_path / "promotion_scorecard.json"
    portfolio_stability_path = tmp_path / "portfolio_stability_score.json"
    output_path = tmp_path / "research_attribution.json"
    report_path = tmp_path / "factor_quality_observation_report.md"

    now = datetime.now(timezone.utc).isoformat()
    memory_path.write_text(
        json.dumps(
            {
                "candidate_generation_history": [
                    {
                        "updated_at_utc": now,
                        "candidate_id": "gen_old",
                        "source": "stable_plus_graveyard",
                        "target_family": "value",
                        "exploration_pool": "old_space_optimization",
                    },
                    {
                        "updated_at_utc": now,
                        "candidate_id": "gen_new",
                        "source": "failure_question",
                        "target_family": "quality",
                        "exploration_pool": "new_mechanism_exploration",
                    },
                ],
                "candidate_lifecycle": {
                    "gen_old": {
                        "history": [
                            {"updated_at_utc": now, "next_state": "validating", "action": "hold"}
                        ]
                    },
                    "gen_new": {
                        "history": [
                            {"updated_at_utc": now, "next_state": "stable_candidate", "action": "promote"}
                        ]
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    learning_path.write_text(json.dumps({"failure_question_cards": [{"card_id": "q1"}]}, ensure_ascii=False), encoding="utf-8")
    candidate_pool_path.write_text(
        json.dumps(
            {
                "tasks": [
                    {"payload": {"source": "candidate_generation", "exploration_pool": "old_space_optimization"}},
                    {"payload": {"source": "candidate_generation", "exploration_pool": "new_mechanism_exploration"}},
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    candidate_generation_plan_path.write_text(
        json.dumps(
            {
                "proposals": [
                    {"candidate_id": "gen_old", "source": "stable_plus_graveyard", "exploration_pool": "old_space_optimization"},
                    {"candidate_id": "gen_new", "source": "failure_question", "exploration_pool": "new_mechanism_exploration"},
                ],
                "quality_throttle": {
                    "pool_budgets": {
                        "old_space_optimization": 1,
                        "new_mechanism_exploration": 1,
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    promotion_scorecard_path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "factor_name": "gen_old",
                        "family": "value",
                        "quality_classification": "needs-validation",
                        "quality_promotion_decision": "keep_validating",
                        "window_count": 2,
                        "retention_industry": 0.2,
                        "neutralized_rank_ic_mean": 0.01,
                        "net_metric": 0.1,
                        "turnover_daily": 0.2,
                    },
                    {
                        "factor_name": "gen_new",
                        "family": "quality",
                        "quality_classification": "stable-alpha-candidate",
                        "quality_promotion_decision": "promote",
                        "window_count": 4,
                        "retention_industry": 0.25,
                        "neutralized_rank_ic_mean": 0.02,
                        "net_metric": 0.3,
                        "turnover_daily": 0.15,
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    portfolio_stability_path.write_text(
        json.dumps({"stability_score": 0.12, "label": "低稳定"}, ensure_ascii=False),
        encoding="utf-8",
    )

    payload = build_research_attribution(
        memory_path=memory_path,
        learning_path=learning_path,
        candidate_pool_path=candidate_pool_path,
        candidate_generation_plan_path=candidate_generation_plan_path,
        promotion_scorecard_path=promotion_scorecard_path,
        portfolio_stability_path=portfolio_stability_path,
        output_path=output_path,
        report_path=report_path,
    )

    assert payload["current_snapshot"]["generation"]["by_pool"]["new_mechanism_exploration"]["proposal_count"] == 1
    assert payload["current_snapshot"]["final_conversion"]["stable_alpha_candidate_count"] == 1
    assert payload["observation_windows"]["48h"]["proposal_count"] == 2
    assert payload["observation_windows"]["48h"]["by_pool"]["new_mechanism_exploration"]["stable_alpha_candidate"] == 1
    assert report_path.exists()
