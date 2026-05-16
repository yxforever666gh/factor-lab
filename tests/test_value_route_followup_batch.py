import json
from pathlib import Path

from factor_lab.dedup import workflow_experiment_fingerprint
from factor_lab.value_route_followup_batch import (
    build_value_route_followup_batch,
    write_value_route_followup_batch,
)


def _write_parent_config(path: Path, route_id: str, *, long_quantile: int = 3, short_quantile: int = 0) -> dict:
    cfg = {
        "route_id": route_id,
        "mechanism_id": route_id,
        "required_data_fields": ["book_yield", "roe"],
        "factors": [{"name": "x", "expression": "book_yield + roe"}],
        "portfolio_construction": {
            "mode": "bucket_pair",
            "quantiles": 5,
            "long_quantile": long_quantile,
            "short_quantile": short_quantile,
        },
        "thresholds": {"min_rank_ic": 0.02, "min_bucket_spread": 0.001},
        "output_dir": f"artifacts/value_route_bucket_aware/runs/{route_id}_bucket_aware",
    }
    path.write_text(json.dumps(cfg), encoding="utf-8")
    return cfg


def test_followup_batch_uses_only_promoted_routes(tmp_path):
    _write_parent_config(tmp_path / "value_quality_no_distress_bucket_aware.json", "value_quality_no_distress")
    _write_parent_config(tmp_path / "industry_relative_value_bucket_aware.json", "industry_relative_value")
    route_policy = {
        "routes": {
            "value_quality_no_distress": {"decision": "promote", "reason": "bucket_aware_oos_stable"},
            "industry_relative_value": {"decision": "neutral", "reason": "insufficient_evidence"},
        }
    }

    batch = build_value_route_followup_batch(route_policy=route_policy, source_dir=tmp_path)

    assert {cfg["route_id"] for cfg in batch["configs"]} == {"value_quality_no_distress"}
    assert all(cfg["followup_of"]["route_policy_reason"] == "bucket_aware_oos_stable" for cfg in batch["configs"])


def test_followup_batch_generates_nonduplicate_variant_configs(tmp_path):
    _write_parent_config(tmp_path / "value_quality_no_distress_bucket_aware.json", "value_quality_no_distress")
    route_policy = {"routes": {"value_quality_no_distress": {"decision": "promote", "reason": "bucket_aware_oos_stable"}}}

    batch = build_value_route_followup_batch(route_policy=route_policy, source_dir=tmp_path)

    followup_types = {cfg["followup_type"] for cfg in batch["configs"]}
    assert followup_types == {"cost_sensitivity_20bps", "bucket_pair_stricter_tail"}
    assert "longer_validation_window" not in followup_types
    assert all("value_route_followups/runs" in cfg["output_dir"] for cfg in batch["configs"])
    cost_cfg = next(cfg for cfg in batch["configs"] if cfg["followup_type"] == "cost_sensitivity_20bps")
    assert cost_cfg["portfolio_cost_bps_per_turnover"] == 20.0
    tail_cfg = next(cfg for cfg in batch["configs"] if cfg["followup_type"] == "bucket_pair_stricter_tail")
    assert tail_cfg["portfolio_construction"]["long_quantile"] == 4


def test_followup_configs_have_different_fingerprints_from_parent(tmp_path):
    parent = _write_parent_config(tmp_path / "value_quality_no_distress_bucket_aware.json", "value_quality_no_distress")
    route_policy = {"routes": {"value_quality_no_distress": {"decision": "promote", "reason": "bucket_aware_oos_stable"}}}

    batch = build_value_route_followup_batch(route_policy=route_policy, source_dir=tmp_path)

    for followup in batch["configs"]:
        assert workflow_experiment_fingerprint(parent) != workflow_experiment_fingerprint(followup)


def test_write_value_route_followup_batch_dry_run_writes_nothing(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    _write_parent_config(source / "value_quality_no_distress_bucket_aware.json", "value_quality_no_distress")
    output = tmp_path / "out"
    route_policy = {"routes": {"value_quality_no_distress": {"decision": "promote", "reason": "bucket_aware_oos_stable"}}}

    result = write_value_route_followup_batch(route_policy=route_policy, source_dir=source, output_dir=output, dry_run=True)

    assert result["written"] is False
    assert result["config_count"] == 2
    assert not output.exists()


def test_write_value_route_followup_batch_writes_manifest_and_configs(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    _write_parent_config(source / "value_quality_no_distress_bucket_aware.json", "value_quality_no_distress")
    output = tmp_path / "out"
    route_policy = {"routes": {"value_quality_no_distress": {"decision": "promote", "reason": "bucket_aware_oos_stable"}}}

    result = write_value_route_followup_batch(route_policy=route_policy, source_dir=source, output_dir=output, dry_run=False)

    assert result["written"] is True
    assert (output / "manifest.json").exists()
    assert (output / "value_quality_no_distress__cost_sensitivity_20bps.json").exists()
    assert (output / "value_quality_no_distress__bucket_pair_stricter_tail.json").exists()
