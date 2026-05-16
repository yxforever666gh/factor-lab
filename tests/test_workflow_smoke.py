from pathlib import Path
import json

from factor_lab.workflow import run_workflow


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_first_workflow_smoke(tmp_path, monkeypatch):
    output_dir = tmp_path / "first_workflow"
    config_path = REPO_ROOT / "configs" / "first_workflow.json"

    monkeypatch.chdir(tmp_path)
    run_workflow(str(config_path), str(output_dir))

    assert (output_dir / "results.json").exists()
    assert (output_dir / "summary.md").exists()
    assert (output_dir / "explore_pool.json").exists()
    assert (output_dir / "watchlist_pool.json").exists()
    assert (output_dir / "candidate_pool.json").exists()
    assert (output_dir / "candidate_status_snapshot.json").exists()
    assert (output_dir / "rolling_results.json").exists()
    assert (output_dir / "rolling_summary.json").exists()
    assert (output_dir / "rolling_failures.json").exists()
    assert (output_dir / "portfolio_results.json").exists()
    assert (tmp_path / "artifacts" / "factor_lab.db").exists()

    rolling_summary = json.loads((output_dir / "rolling_summary.json").read_text(encoding="utf-8"))
    assert rolling_summary
    assert "stability_score" in rolling_summary[0]
    assert "rank_ic_std" in rolling_summary[0]

    status_snapshot = json.loads((output_dir / "candidate_status_snapshot.json").read_text(encoding="utf-8"))
    assert status_snapshot
    assert "promotion_reason" in status_snapshot[0]
    assert "blocking_reasons" in status_snapshot[0]


def test_workflow_handles_simple_and_generated_factors_out_of_order(tmp_path, monkeypatch):
    output_dir = tmp_path / "generated_workflow"
    config_path = tmp_path / "generated_workflow.json"
    config = {
        "seed": 7,
        "num_stocks": 30,
        "num_days": 80,
        "factors": [
            {
                "name": "hybrid_mom_value",
                "expression": "(momentum_20) + (earnings_yield)",
                "generator_operator": "combine_add",
                "left_factor_name": "mom_20",
                "right_factor_name": "value_ep",
            },
            {"name": "mom_20", "expression": "momentum_20"},
            {"name": "value_ep", "expression": "earnings_yield"},
        ],
        "thresholds": {
            "min_rank_ic": -1.0,
            "min_top_bottom_spread": -1.0,
        },
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    run_workflow(str(config_path), str(output_dir))

    results = json.loads((output_dir / "results.json").read_text(encoding="utf-8"))
    assert {row["factor_name"] for row in results} == {"hybrid_mom_value", "mom_20", "value_ep"}


def test_workflow_can_skip_global_refreshes(tmp_path, monkeypatch):
    from factor_lab import workflow as workflow_module

    output_dir = tmp_path / "light_workflow"
    config_path = tmp_path / "light_workflow.json"
    config = {
        "seed": 7,
        "num_stocks": 20,
        "num_days": 60,
        "write_dataset_csv": False,
        "refresh_global_risk": False,
        "refresh_exposure_track": False,
        "factors": [{"name": "mom_20", "expression": "momentum_20"}],
        "thresholds": {
            "min_rank_ic": -1.0,
            "min_top_bottom_spread": -1.0,
        },
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    refresh_calls = {"risk": 0}

    monkeypatch.setattr(
        workflow_module,
        "refresh_candidate_risk_profiles",
        lambda *args, **kwargs: refresh_calls.__setitem__("risk", refresh_calls["risk"] + 1),
    )

    monkeypatch.chdir(tmp_path)
    run_workflow(str(config_path), str(output_dir))

    assert refresh_calls["risk"] == 0
    assert not (output_dir / "dataset.csv").exists()
    assert (output_dir / "results.json").exists()
