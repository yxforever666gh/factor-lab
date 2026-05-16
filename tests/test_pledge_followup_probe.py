import json
from pathlib import Path

from factor_lab.dedup import workflow_experiment_fingerprint
from factor_lab.pledge_followup_probe import (
    FOLLOWUP_TYPE,
    build_pledge_followup_config,
    build_pledge_followup_probe_result,
    write_pledge_followup_probe,
)


def _parent_config() -> dict:
    return {
        "seed": 42,
        "data_source": "tushare",
        "cache_dir": "artifacts/tushare_cache",
        "universe_limit": 80,
        "start_date": "2020-06-01",
        "end_date": "2023-12-31",
        "route_id": "value_quality_high_pledge_record_count_confirmation",
        "mechanism_id": "pledge_control_pressure",
        "source": "controlled_pledge_probe",
        "feature_overlay_csv": "artifacts/pledge_source_mvp/pledge_daily_asof_features.csv",
        "feature_overlay_columns": ["high_pledge_record_count", "forward_return_5d"],
        "required_data_fields": ["high_pledge_record_count", "forward_return_5d"],
        "factors": [{"name": "high_pledge_record_count", "expression": "high_pledge_record_count"}],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
        "benchmark": {"route_id": "value_quality_no_distress", "bucket_spread": 0.0062253011},
    }


def test_build_pledge_followup_config_changes_semantic_fingerprint():
    parent = _parent_config()
    followup = build_pledge_followup_config(parent)

    assert followup["route_id"] == "value_quality_high_pledge_record_count_confirmation"
    assert followup["mechanism_id"] == "pledge_control_pressure"
    assert followup["source"] == "controlled_pledge_probe_followup"
    assert followup["followup_type"] == FOLLOWUP_TYPE
    assert followup["portfolio_cost_bps_per_turnover"] == 20.0
    assert workflow_experiment_fingerprint(parent) != workflow_experiment_fingerprint(followup)


def test_pledge_followup_probe_dry_run_allows_exactly_one(tmp_path):
    parent_path = tmp_path / "parent.json"
    validation_path = tmp_path / "validation.json"
    output_dir = tmp_path / "out"
    parent_path.write_text(json.dumps(_parent_config()), encoding="utf-8")
    validation_path.write_text(
        json.dumps(
            {
                "decision": "pledge_validation_pass_prepare_single_followup_plan",
                "bucket_aware": {"spread_mean": 0.015417, "observations": 104},
                "coverage": {"factor_non_null_rate": 0.211695, "factor_non_null_tickers": 21},
            }
        ),
        encoding="utf-8",
    )

    result = build_pledge_followup_probe_result(
        parent_config_path=parent_path,
        validation_path=validation_path,
        output_dir=output_dir,
    )

    assert result["ok"] is True
    assert result["decision"] == "dry_run_allow_exactly_one"
    assert result["would_enqueue_count"] == 1
    assert result["enqueued_count"] == 0
    assert result["no_queue_write"] is True
    assert result["admission"]["decision"] == "allow"
    assert result["fingerprint_differs_from_parent"] is True
    assert result["validation_evidence"]["spread_above_benchmark"] is True
    assert result["task"]["payload"]["source"] == "controlled_pledge_probe_followup"


def test_write_pledge_followup_probe_writes_config_and_dry_run_artifacts(tmp_path):
    parent_path = tmp_path / "parent.json"
    validation_path = tmp_path / "validation.json"
    output_dir = tmp_path / "out"
    parent_path.write_text(json.dumps(_parent_config()), encoding="utf-8")
    validation_path.write_text(json.dumps({"bucket_aware": {"spread_mean": 0.015417, "observations": 104}}), encoding="utf-8")

    result = write_pledge_followup_probe(
        parent_config_path=parent_path,
        validation_path=validation_path,
        output_dir=output_dir,
    )

    assert result["ok"] is True
    assert Path(result["followup_config_path"]).exists()
    assert (output_dir / "pledge_followup_probe_dry_run.json").exists()
    assert (output_dir / "pledge_followup_probe_dry_run.md").exists()
