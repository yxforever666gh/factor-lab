import json
from pathlib import Path

from factor_lab.approved_universe import (
    build_approved_candidate_universe,
    resolve_paper_portfolio_inputs,
    write_approved_candidate_universe,
)
from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.storage import ExperimentStore


def _write_run_artifacts(base: Path, *, candidate_pool: list[dict], status_snapshot: list[dict], cluster_representatives: list[dict]):
    base.mkdir(parents=True, exist_ok=True)
    (base / "candidate_pool.json").write_text(json.dumps(candidate_pool, ensure_ascii=False, indent=2), encoding="utf-8")
    (base / "candidate_status_snapshot.json").write_text(json.dumps(status_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    (base / "cluster_representatives.json").write_text(json.dumps(cluster_representatives, ensure_ascii=False, indent=2), encoding="utf-8")
    (base / "dataset.csv").write_text("date,ticker,forward_return_5d,book_yield,momentum_20\n2026-03-20,AAA,0.1,1.0,0.2\n", encoding="utf-8")


def test_approved_candidate_universe_prefers_cross_window_representative_and_rejects_short_only(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    recent_dir = tmp_path / "generated_recent_45d"
    rolling_dir = tmp_path / "generated_rolling_60d_back"

    _write_run_artifacts(
        recent_dir,
        candidate_pool=[{"factor_name": "mom_20", "expression": "momentum_20"}],
        status_snapshot=[
            {
                "factor_name": "mom_20",
                "factor_role": "alpha_seed",
                "research_stage": "candidate",
                "raw_pass": True,
                "neutralized_pass": False,
                "rolling_pass": True,
                "blocking_reasons": [],
                "promotion_reason": "recent breakout",
            },
            {
                "factor_name": "book_yield",
                "factor_role": "alpha_seed",
                "research_stage": "watchlist",
                "raw_pass": True,
                "neutralized_pass": True,
                "rolling_pass": True,
                "blocking_reasons": ["split_fail_count:1"],
                "promotion_reason": "raw and neutralized hold",
            },
        ],
        cluster_representatives=[
            {
                "factor_name": "book_yield",
                "expression": "book_yield",
                "cluster_members": ["book_yield"],
                "is_primary_representative": True,
            },
            {
                "factor_name": "mom_20",
                "expression": "momentum_20",
                "cluster_members": ["mom_20"],
                "is_primary_representative": True,
            },
        ],
    )
    _write_run_artifacts(
        rolling_dir,
        candidate_pool=[],
        status_snapshot=[
            {
                "factor_name": "mom_20",
                "factor_role": "alpha_seed",
                "research_stage": "graveyard",
                "raw_pass": False,
                "neutralized_pass": False,
                "rolling_pass": False,
                "blocking_reasons": ["raw_fail"],
                "promotion_reason": "raw failed",
            },
            {
                "factor_name": "book_yield",
                "factor_role": "alpha_seed",
                "research_stage": "watchlist",
                "raw_pass": True,
                "neutralized_pass": False,
                "rolling_pass": True,
                "blocking_reasons": ["neutral_fail"],
                "promotion_reason": "watchlist cross-window",
            },
        ],
        cluster_representatives=[
            {
                "factor_name": "book_yield",
                "expression": "book_yield",
                "cluster_members": ["book_yield"],
                "is_primary_representative": True,
            },
            {
                "factor_name": "mom_20",
                "expression": "momentum_20",
                "cluster_members": ["mom_20"],
                "is_primary_representative": True,
            },
        ],
    )

    store.insert_run(
        {
            "run_id": "run-recent",
            "created_at_utc": "2026-04-20T10:00:00+00:00",
            "config_path": "artifacts/generated_configs/rolling_recent_45d.json",
            "output_dir": str(recent_dir),
            "data_source": "tushare",
            "start_date": "2026-01-01",
            "end_date": "2026-03-20",
            "universe_limit": 20,
            "factor_count": 2,
            "dataset_rows": 1,
            "status": "finished",
        }
    )
    store.insert_run(
        {
            "run_id": "run-roll60",
            "created_at_utc": "2026-04-20T10:05:00+00:00",
            "config_path": "artifacts/generated_configs/rolling_60d_back.json",
            "output_dir": str(rolling_dir),
            "data_source": "tushare",
            "start_date": "2026-01-01",
            "end_date": "2026-03-20",
            "universe_limit": 20,
            "factor_count": 2,
            "dataset_rows": 1,
            "status": "finished",
        }
    )
    book_id = store.upsert_factor_candidate(name="book_yield", family="value", definition={"name": "book_yield", "expression": "book_yield"}, expression="book_yield")
    mom_id = store.upsert_factor_candidate(name="mom_20", family="momentum", definition={"name": "mom_20", "expression": "momentum_20"}, expression="momentum_20")
    for candidate_id, latest_score in ((book_id, 2.2), (mom_id, 4.5)):
        store.refresh_factor_candidate(candidate_id, {
            "status": "testing",
            "research_stage": "watchlist",
            "evaluation_count": 10,
            "window_count": 2,
            "avg_final_score": latest_score,
            "best_final_score": latest_score,
            "latest_final_score": latest_score,
            "latest_recent_final_score": latest_score,
            "pass_rate": 0.5,
            "summary": "test",
            "next_action": "validate",
            "rejection_reason": None,
        })

    payload = build_approved_candidate_universe(db_path)
    approved_names = [row["factor_name"] for row in payload["rows"]]
    assert approved_names == ["book_yield", "mom_20"]
    row_map = {row["factor_name"]: row for row in payload["rows"]}
    assert row_map["book_yield"]["portfolio_bucket"] == "controlled_exposure"
    assert row_map["mom_20"]["approval_tier"] == "bridge"
    assert row_map["mom_20"]["portfolio_bucket"] == "recent_probe"
    debug_map = {row["factor_name"]: row for row in payload["debug_rows"]}
    assert debug_map["mom_20"]["approved"] is True

    out_path = tmp_path / "approved_candidate_universe.json"
    debug_path = tmp_path / "approved_candidate_universe_debug.json"
    write_approved_candidate_universe(db_path, out_path, debug_path)
    resolved = resolve_paper_portfolio_inputs(
        db_path=db_path,
        approved_universe_path=out_path,
        fallback_candidate_pool_path=recent_dir / "candidate_pool.json",
        fallback_dataset_path=recent_dir / "dataset.csv",
    )
    assert resolved["source"] == "approved_candidate_universe"
    resolved_names = [row["name"] for row in resolved["factor_definitions"]]
    assert resolved_names == ["book_yield", "mom_20"]
    assert resolved["factor_definitions"][0]["portfolio_bucket"] == "controlled_exposure"
    assert resolved["factor_definitions"][0]["approval_tier"] == "core"
    assert resolved["factor_definitions"][1]["portfolio_bucket"] == "recent_probe"
    assert resolved["factor_definitions"][1]["approval_tier"] == "bridge"

    scorecard = build_promotion_scorecard(db_path=db_path, limit=10)
    row_map = {row["factor_name"]: row for row in scorecard["rows"]}
    assert row_map["book_yield"]["approved_universe_member"] is True
    assert row_map["book_yield"]["approved_universe_reason"] == "primary_representative_watchlist_with_cross_window_support"
