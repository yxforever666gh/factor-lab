import json
from pathlib import Path

from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.storage import ExperimentStore


def _seed_candidate(
    store: ExperimentStore,
    *,
    run_id: str,
    name: str,
    evaluation_count: int,
    window_count: int,
    avg_final_score: float,
    latest_recent_final_score: float,
    retention_industry: float,
    neutralized_rank_ic_mean: float,
    net_metric: float,
    turnover_daily: float,
    acceptance_gate: dict,
):
    candidate_id = store.upsert_factor_candidate(
        name=name,
        family="value",
        definition={"name": name, "expression": name},
        expression=name,
    )
    store.refresh_factor_candidate(
        candidate_id,
        {
            "status": "testing",
            "research_stage": "watchlist",
            "evaluation_count": evaluation_count,
            "window_count": window_count,
            "avg_final_score": avg_final_score,
            "best_final_score": avg_final_score + 0.5,
            "latest_final_score": latest_recent_final_score,
            "latest_recent_final_score": latest_recent_final_score,
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
                "factor_name": name,
                "variant": "raw_scored",
                "expression": name,
                "rank_ic_mean": 0.05,
                "rank_ic_ir": 0.2,
                "top_bottom_spread_mean": 0.002,
                "pass_gate": True,
                "fail_reason": None,
                "score": 1.8,
                "split_fail_count": 0,
                "high_corr_peers": [],
            },
            {
                "run_id": run_id,
                "factor_name": name,
                "variant": "neutralized",
                "expression": name,
                "rank_ic_mean": neutralized_rank_ic_mean,
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
                "factor_name": name,
                "exposure_type": "style",
                "strength_score": 60,
                "raw_rank_ic_mean": 0.05,
                "raw_rank_ic_ir": 0.2,
                "neutralized_rank_ic_mean": neutralized_rank_ic_mean,
                "neutralized_pass_gate": True,
                "retention_industry": retention_industry,
                "split_fail_count": 0,
                "crowding_peers": 0,
                "recommended_max_weight": 0.15,
                "turnover_daily": turnover_daily,
                "net_metric": net_metric,
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
            "risk_level": "low",
            "risk_score": 20.0,
            "robustness_score": 0.88,
            "family_context_score": 0.7,
            "graph_context_score": 0.7,
            "evaluation_count": evaluation_count,
            "passing_check_count": 5,
            "failing_check_count": 0,
            "summary": "risk profile",
            "key_risks": [],
            "mitigations": [],
            "checks": [],
            "acceptance_gate": acceptance_gate,
            "acceptance_gate_explanation": "gate present",
        },
    )
    return candidate_id


def test_promotion_scorecard_requires_long_horizon_and_implementability_for_stable(tmp_path):
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
            "factor_count": 2,
            "dataset_rows": 100,
            "status": "finished",
        }
    )

    _seed_candidate(
        store,
        run_id=run_id,
        name="short_only_good",
        evaluation_count=80,
        window_count=3,
        avg_final_score=3.2,
        latest_recent_final_score=3.4,
        retention_industry=0.42,
        neutralized_rank_ic_mean=0.03,
        net_metric=0.2,
        turnover_daily=0.2,
        acceptance_gate={"status": "pass", "promotion": "pass"},
    )
    _seed_candidate(
        store,
        run_id=run_id,
        name="long_but_untradable",
        evaluation_count=90,
        window_count=5,
        avg_final_score=3.5,
        latest_recent_final_score=3.6,
        retention_industry=0.44,
        neutralized_rank_ic_mean=0.04,
        net_metric=-0.1,
        turnover_daily=0.7,
        acceptance_gate={"status": "pass", "promotion": "pass"},
    )

    payload = build_promotion_scorecard(db_path=db_path, limit=5)
    rows = {row["factor_name"]: row for row in payload["rows"]}

    assert rows["short_only_good"]["quality_hard_flags"]["insufficient_long_horizon_evidence"] is True
    assert rows["short_only_good"]["quality_promotion_decision"] == "keep_validating"
    assert rows["long_but_untradable"]["quality_hard_flags"]["implementability_weak"] is True
    assert rows["long_but_untradable"]["quality_promotion_decision"] == "do_not_promote"


def test_promotion_scorecard_v2_carries_thesis_representative_and_negative_portfolio_flags(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    run_id = "run-1"
    run_output = tmp_path / "run_output"
    run_output.mkdir(parents=True, exist_ok=True)
    (run_output / "cluster_representatives.json").write_text(
        json.dumps(
            [
                {
                    "factor_name": "rep_factor",
                    "cluster_members": ["rep_factor", "child_factor"],
                    "representative_rank": 1,
                    "representative_count": 1,
                    "is_primary_representative": True,
                }
            ],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    store.insert_run(
        {
            "run_id": run_id,
            "created_at_utc": "2026-04-01T00:00:00+00:00",
            "config_path": "configs/test.json",
            "output_dir": str(run_output),
            "data_source": "tushare",
            "start_date": "2025-01-01",
            "end_date": "2026-03-01",
            "universe_limit": 20,
            "factor_count": 2,
            "dataset_rows": 100,
            "status": "finished",
        }
    )

    parent_id = _seed_candidate(
        store,
        run_id=run_id,
        name="rep_factor",
        evaluation_count=90,
        window_count=5,
        avg_final_score=4.2,
        latest_recent_final_score=4.1,
        retention_industry=0.48,
        neutralized_rank_ic_mean=0.04,
        net_metric=0.2,
        turnover_daily=0.2,
        acceptance_gate={"status": "pass", "promotion": "pass"},
    )
    child_id = _seed_candidate(
        store,
        run_id=run_id,
        name="child_factor",
        evaluation_count=90,
        window_count=5,
        avg_final_score=4.15,
        latest_recent_final_score=4.0,
        retention_industry=0.47,
        neutralized_rank_ic_mean=0.04,
        net_metric=0.2,
        turnover_daily=0.2,
        acceptance_gate={"status": "pass", "promotion": "pass"},
    )

    store.upsert_candidate_relationship(
        left_candidate_id=child_id,
        right_candidate_id=parent_id,
        relationship_type="refinement_of",
        run_id=run_id,
        strength=0.95,
        details={"parent_candidate": "rep_factor", "child_candidate": "child_factor"},
    )
    store.upsert_research_thesis(
        child_id,
        {
            "thesis_id": "momentum:behavioral_continuation:b2_controlled_composite",
            "title": "momentum thesis / Controlled Composite",
            "family": "value",
            "thesis_type": "behavioral_continuation",
            "institutional_bucket_key": "b2_controlled_composite",
            "institutional_bucket_label": "Controlled Composite",
            "thesis_text": "child_factor 围绕可控组合命题展开。",
            "mechanism_rationale": "child_factor 围绕可控组合命题展开。",
            "status": "testing",
            "invalidation_json": json.dumps(["若相对父因子没有新增信息则降级"], ensure_ascii=False),
            "representative_candidate": "rep_factor",
            "representative_rank": 1,
            "representative_count": 1,
            "roster_json": json.dumps(["rep_factor"], ensure_ascii=False),
            "source_context_json": json.dumps({"parent_factor_name": "rep_factor"}, ensure_ascii=False),
        },
    )

    portfolio_dir = tmp_path / "paper_portfolio"
    portfolio_dir.mkdir(parents=True, exist_ok=True)
    (portfolio_dir / "portfolio_contribution_report.json").write_text(
        json.dumps(
            {
                "status": "ok",
                "rows": [
                    {
                        "factor_name": "child_factor",
                        "delta_sharpe": -0.11,
                        "delta_cost_adjusted_annual_return": -0.03,
                        "contribution_class": "negative",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    payload = build_promotion_scorecard(db_path=db_path, limit=5)
    rows = {row["factor_name"]: row for row in payload["rows"]}
    row = rows["child_factor"]

    assert row["scorecard_schema_version"] == "factor-quality-v2"
    assert row["thesis_id"] == "momentum:behavioral_continuation:b2_controlled_composite"
    assert row["institutional_bucket_label"] == "Controlled Composite"
    assert row["quality_hard_flags"]["non_incremental_vs_parent"] is True
    assert row["quality_hard_flags"]["negative_portfolio_contribution"] is True
    assert row["quality_hard_flags"]["representative_suppressed"] is True
    assert row["quality_promotion_decision"] == "suppress"
