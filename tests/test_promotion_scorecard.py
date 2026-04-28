import json
from pathlib import Path

from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.storage import ExperimentStore


def test_promotion_scorecard_holds_candidate_when_acceptance_gate_is_missing(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    run_id = "run-1"
    store.insert_run(
        {
            "run_id": run_id,
            "created_at_utc": "2026-04-01T00:00:00+00:00",
            "config_path": "configs/test.json",
            "output_dir": "artifacts/test_run",
            "data_source": "tushare",
            "start_date": "2025-01-01",
            "end_date": "2026-03-01",
            "universe_limit": 20,
            "factor_count": 1,
            "dataset_rows": 100,
            "status": "finished",
        }
    )
    candidate_id = store.upsert_factor_candidate(
        name="book_yield",
        family="value",
        definition={"name": "book_yield", "expression": "book_yield"},
        expression="book_yield",
    )
    store.refresh_factor_candidate(
        candidate_id,
        {
            "status": "testing",
            "research_stage": "watchlist",
            "evaluation_count": 80,
            "window_count": 4,
            "avg_final_score": 2.2,
            "best_final_score": 3.0,
            "latest_final_score": 2.8,
            "latest_recent_final_score": 2.7,
            "pass_rate": 0.6,
            "summary": "test candidate",
            "next_action": "validate_more_windows",
            "rejection_reason": None,
        },
    )
    store.insert_factor_rows(
        [
            {
                "run_id": run_id,
                "factor_name": "book_yield",
                "variant": "raw_scored",
                "expression": "book_yield",
                "rank_ic_mean": 0.04,
                "rank_ic_ir": 0.2,
                "top_bottom_spread_mean": 0.002,
                "pass_gate": True,
                "fail_reason": None,
                "score": 1.5,
                "split_fail_count": 0,
                "high_corr_peers": [],
            },
            {
                "run_id": run_id,
                "factor_name": "book_yield",
                "variant": "neutralized",
                "expression": "book_yield",
                "rank_ic_mean": 0.03,
                "rank_ic_ir": 0.18,
                "top_bottom_spread_mean": 0.001,
                "pass_gate": True,
                "fail_reason": None,
                "score": 1.2,
                "split_fail_count": 0,
                "high_corr_peers": [],
            },
        ]
    )
    store.upsert_exposure_rows(
        [
            {
                "run_id": run_id,
                "factor_name": "book_yield",
                "exposure_type": "style",
                "strength_score": 60,
                "raw_rank_ic_mean": 0.04,
                "raw_rank_ic_ir": 0.2,
                "neutralized_rank_ic_mean": 0.03,
                "neutralized_pass_gate": True,
                "retention_industry": 0.35,
                "split_fail_count": 0,
                "crowding_peers": 0,
                "recommended_max_weight": 0.15,
                "status": "watch",
                "hard_flags": [],
                "notes": {},
            }
        ]
    )
    store.replace_candidate_risk_profile(
        candidate_id,
        {
            "candidate_id": candidate_id,
            "run_id": run_id,
            "risk_level": "medium",
            "risk_score": 45.0,
            "robustness_score": 0.82,
            "family_context_score": 0.7,
            "graph_context_score": 0.7,
            "evaluation_count": 80,
            "passing_check_count": 5,
            "failing_check_count": 0,
            "summary": "risk profile",
            "key_risks": [],
            "mitigations": [],
            "checks": [],
            "acceptance_gate": {},
            "acceptance_gate_explanation": "acceptance gate missing or incomplete",
        },
    )

    payload = build_promotion_scorecard(db_path=db_path, limit=5)
    row = next(item for item in payload["rows"] if item["factor_name"] == "book_yield")

    assert row["evidence_gate"]["action"] == "evidence_missing"
    assert row["quality_hard_flags"]["evidence_missing"] is True
    assert row["quality_classification"] == "validate-only"
    assert row["quality_promotion_decision"] == "hold"

