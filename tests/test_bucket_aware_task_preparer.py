from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks


def _write_cfg(path, route_id):
    path.write_text('{"route_id":"' + route_id + '","mechanism_id":"' + route_id + '","required_data_fields":["book_yield","roe"],"factors":[{"name":"x","expression":"book_yield + roe"}],"portfolio_construction":{"mode":"bucket_pair","quantiles":5,"long_quantile":3,"short_quantile":0}}')


def test_prepare_bucket_aware_tasks_dry_run_marks_all_allow(tmp_path):
    cfg = tmp_path / "value_quality_no_distress_bucket_aware.json"
    _write_cfg(cfg, "value_quality_no_distress")

    result = prepare_bucket_aware_tasks(config_paths=[cfg], dry_run=True)

    assert result["would_enqueue_count"] == 1
    assert result["tasks"][0]["admission"]["decision"] == "allow"
    assert result["tasks"][0]["payload"]["portfolio_construction"]["long_quantile"] == 3


def test_prepare_bucket_aware_tasks_respects_limit_after_admission(tmp_path):
    cfg1 = tmp_path / "value_quality_no_distress_bucket_aware.json"
    cfg2 = tmp_path / "industry_relative_value_bucket_aware.json"
    _write_cfg(cfg1, "value_quality_no_distress")
    _write_cfg(cfg2, "industry_relative_value")

    result = prepare_bucket_aware_tasks(config_paths=[cfg1, cfg2], dry_run=True, limit=1)

    assert result["would_enqueue_count"] == 1
    assert len(result["tasks"]) == 1


def test_prepare_bucket_aware_tasks_prefers_least_recently_injected_route(tmp_path):
    cfg1 = tmp_path / "industry_relative_value_bucket_aware.json"
    cfg2 = tmp_path / "value_quality_no_distress_bucket_aware.json"
    cfg3 = tmp_path / "value_momentum_confirmation_bucket_aware.json"
    _write_cfg(cfg1, "industry_relative_value")
    _write_cfg(cfg2, "value_quality_no_distress")
    _write_cfg(cfg3, "value_momentum_confirmation")

    result = prepare_bucket_aware_tasks(
        config_paths=[cfg1, cfg2, cfg3],
        dry_run=True,
        limit=1,
        route_history_counts={"industry_relative_value": 5, "value_quality_no_distress": 1, "value_momentum_confirmation": 0},
        value_sleeve_policy={"decision": "no_sleeve_policy", "routes": {}},
    )

    assert result["tasks"][0]["payload"]["route_id"] == "value_momentum_confirmation"
    assert result["tasks"][0]["selection_reason"] == "least_recent_route"


def test_prepare_bucket_aware_tasks_prefers_promoted_route_and_skips_demoted(tmp_path):
    cfg1 = tmp_path / "industry_relative_value_bucket_aware.json"
    cfg2 = tmp_path / "value_quality_no_distress_bucket_aware.json"
    _write_cfg(cfg1, "industry_relative_value")
    _write_cfg(cfg2, "value_quality_no_distress")

    result = prepare_bucket_aware_tasks(
        config_paths=[cfg1, cfg2],
        dry_run=True,
        limit=1,
        route_policy={
            "routes": {
                "industry_relative_value": {"decision": "demote", "reason": "repeated_coverage_too_low"},
                "value_quality_no_distress": {"decision": "promote", "reason": "passes_gate_without_repeated_blockers"},
            }
        },
    )

    assert result["tasks"][0]["payload"]["route_id"] == "value_quality_no_distress"
    assert result["tasks"][0]["route_policy_decision"] == "promote"


def test_prepare_bucket_aware_tasks_filters_route_and_followup_type(tmp_path):
    cfg1 = tmp_path / "value_quality_no_distress__cost_sensitivity_20bps.json"
    cfg2 = tmp_path / "value_quality_no_distress__bucket_pair_stricter_tail.json"
    _write_cfg(cfg1, "value_quality_no_distress")
    _write_cfg(cfg2, "value_quality_no_distress")
    import json
    for path, followup_type in [(cfg1, "cost_sensitivity_20bps"), (cfg2, "bucket_pair_stricter_tail")]:
        cfg = json.loads(path.read_text())
        cfg["followup_type"] = followup_type
        path.write_text(json.dumps(cfg))

    result = prepare_bucket_aware_tasks(
        config_paths=[cfg2, cfg1],
        dry_run=True,
        limit=1,
        route_id="value_quality_no_distress",
        followup_type="cost_sensitivity_20bps",
    )

    assert result["would_enqueue_count"] == 1
    assert result["tasks"][0]["config_path"].endswith("value_quality_no_distress__cost_sensitivity_20bps.json")
    assert result["tasks"][0]["payload"]["followup_type"] == "cost_sensitivity_20bps"
