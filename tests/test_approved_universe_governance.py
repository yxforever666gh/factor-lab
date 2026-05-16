import json
from pathlib import Path

from factor_lab.approved_universe import write_approved_candidate_universe
from factor_lab.storage import ExperimentStore


def _write_run_artifacts(base: Path, *, candidate_pool: list[dict], status_snapshot: list[dict], cluster_representatives: list[dict]):
    base.mkdir(parents=True, exist_ok=True)
    (base / "candidate_pool.json").write_text(json.dumps(candidate_pool, ensure_ascii=False, indent=2), encoding="utf-8")
    (base / "candidate_status_snapshot.json").write_text(json.dumps(status_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    (base / "cluster_representatives.json").write_text(json.dumps(cluster_representatives, ensure_ascii=False, indent=2), encoding="utf-8")
    (base / "dataset.csv").write_text("date,ticker,forward_return_5d,momentum_20\n2026-03-20,AAA,0.1,0.2\n", encoding="utf-8")


def test_governance_demotes_bridge_after_repeated_negative_contribution(tmp_path):
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
            }
        ],
        cluster_representatives=[
            {
                "factor_name": "mom_20",
                "expression": "momentum_20",
                "cluster_members": ["mom_20"],
                "is_primary_representative": True,
            }
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
            }
        ],
        cluster_representatives=[
            {
                "factor_name": "mom_20",
                "expression": "momentum_20",
                "cluster_members": ["mom_20"],
                "is_primary_representative": True,
            }
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
            "factor_count": 1,
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
            "factor_count": 1,
            "dataset_rows": 1,
            "status": "finished",
        }
    )
    candidate_id = store.upsert_factor_candidate(name="mom_20", family="momentum", definition={"name": "mom_20", "expression": "momentum_20"}, expression="momentum_20")
    store.refresh_factor_candidate(candidate_id, {
        "status": "testing",
        "research_stage": "watchlist",
        "evaluation_count": 10,
        "window_count": 2,
        "avg_final_score": 4.5,
        "best_final_score": 4.5,
        "latest_final_score": 4.5,
        "latest_recent_final_score": 4.5,
        "pass_rate": 0.5,
        "summary": "test",
        "next_action": "validate",
        "rejection_reason": None,
    })

    out_path = tmp_path / "approved_candidate_universe.json"
    gov_path = tmp_path / "approved_candidate_universe_governance.json"
    (tmp_path / "paper_portfolio").mkdir(parents=True, exist_ok=True)
    (tmp_path / "paper_portfolio" / "portfolio_contribution_report.json").write_text(
        json.dumps({"rows": [{"factor_name": "mom_20", "contribution_class": "negative"}]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    payload1 = write_approved_candidate_universe(db_path, out_path, governance_output_path=gov_path)
    assert [row["factor_name"] for row in payload1["rows"]] == ["mom_20"]

    # Simulate a previous negative contribution streak.
    gov_payload = json.loads(gov_path.read_text(encoding="utf-8"))
    gov_payload["rows"] = [{
        "factor_name": "mom_20",
        "negative_contribution_streak": 1,
        "approved_streak": 1,
        "approval_tier": "bridge",
        "portfolio_bucket": "recent_probe",
        "portfolio_contribution_class": "negative",
    }]
    gov_path.write_text(json.dumps(gov_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    payload2 = write_approved_candidate_universe(db_path, out_path, governance_output_path=gov_path)
    assert payload2["rows"] == []
    debug = {row["factor_name"]: row for row in payload2["debug_rows"]}
    assert "governance_demotion" in debug["mom_20"]["rejection_reasons"]
    assert payload2["governance"]["summary"]["action_counts"]["demote_bridge_candidate"] == 1
    assert payload2["governance"]["summary"]["state_counts"]["rejected"] == 1
