from factor_lab.bucket_aware_task_preparer import prepare_bucket_aware_tasks


def _write_cfg(path, route_id):
    path.write_text(
        '{"route_id":"' + route_id + '","mechanism_id":"' + route_id + '","required_data_fields":["book_yield","roe"],"factors":[{"name":"x","expression":"book_yield + roe"}],"portfolio_construction":{"mode":"bucket_pair","quantiles":5,"long_quantile":3,"short_quantile":0}}',
        encoding="utf-8",
    )


def _policy():
    return {
        "decision": "collapse_to_value_sleeve_with_primary_route",
        "routes": {
            "value_quality_no_distress": {"role": "primary", "action": "prioritize_primary", "admission_rank": 0},
            "value_momentum_confirmation": {"role": "confirmation", "action": "confirmation_only", "admission_rank": 1},
            "industry_relative_value": {"role": "low_weight_core_value", "action": "cap_or_skip_duplicate", "admission_rank": 2},
        },
    }


def test_prepare_bucket_aware_tasks_prefers_value_sleeve_primary_route(tmp_path):
    industry = tmp_path / "industry_relative_value_bucket_aware.json"
    quality = tmp_path / "value_quality_no_distress_bucket_aware.json"
    momentum = tmp_path / "value_momentum_confirmation_bucket_aware.json"
    _write_cfg(industry, "industry_relative_value")
    _write_cfg(quality, "value_quality_no_distress")
    _write_cfg(momentum, "value_momentum_confirmation")

    result = prepare_bucket_aware_tasks(
        config_paths=[industry, momentum, quality],
        dry_run=True,
        limit=1,
        value_sleeve_policy=_policy(),
    )

    row = result["tasks"][0]
    assert row["payload"]["route_id"] == "value_quality_no_distress"
    assert row["value_sleeve_role"] == "primary"
    assert row["value_sleeve_action"] == "prioritize_primary"


def test_prepare_bucket_aware_tasks_can_select_confirmation_when_explicitly_requested(tmp_path):
    quality = tmp_path / "value_quality_no_distress_bucket_aware.json"
    momentum = tmp_path / "value_momentum_confirmation_bucket_aware.json"
    _write_cfg(quality, "value_quality_no_distress")
    _write_cfg(momentum, "value_momentum_confirmation")

    result = prepare_bucket_aware_tasks(
        config_paths=[quality, momentum],
        dry_run=True,
        limit=1,
        route_id="value_momentum_confirmation",
        value_sleeve_policy=_policy(),
    )

    row = result["tasks"][0]
    assert row["payload"]["route_id"] == "value_momentum_confirmation"
    assert row["value_sleeve_role"] == "confirmation"
    assert row["value_sleeve_action"] == "confirmation_only"


def test_prepare_bucket_aware_tasks_caps_low_weight_duplicate_by_default(tmp_path):
    industry = tmp_path / "industry_relative_value_bucket_aware.json"
    quality = tmp_path / "value_quality_no_distress_bucket_aware.json"
    _write_cfg(industry, "industry_relative_value")
    _write_cfg(quality, "value_quality_no_distress")

    result = prepare_bucket_aware_tasks(
        config_paths=[industry, quality],
        dry_run=True,
        limit=2,
        value_sleeve_policy=_policy(),
    )

    rows = result["tasks"]
    assert rows[0]["payload"]["route_id"] == "value_quality_no_distress"
    industry_row = [r for r in rows if r["payload"]["route_id"] == "industry_relative_value"][0]
    assert industry_row["value_sleeve_role"] == "low_weight_core_value"
    assert industry_row["value_sleeve_action"] == "cap_or_skip_duplicate"
    assert industry_row["value_sleeve_admission_rank"] > rows[0]["value_sleeve_admission_rank"]
