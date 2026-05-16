from factor_lab.value_route_batch import build_value_route_batch


def test_build_value_route_batch_generates_only_allowed_ready_routes():
    batch = build_value_route_batch(
        available_fields={"industry", "book_yield", "earnings_yield", "roe", "momentum_60_skip_5", "pb", "pe_ttm"},
        allowed_routes=["industry_relative_value", "value_quality_no_distress", "value_momentum_confirmation"],
    )

    assert batch["blocked"] == []
    assert batch["configs"]
    route_ids = {cfg["route_id"] for cfg in batch["configs"]}
    assert route_ids == {"industry_relative_value", "value_quality_no_distress", "value_momentum_confirmation"}
    assert {cfg["validation_protocol_name"] for cfg in batch["configs"]} == {"value_factor_default"}
    assert {cfg["horizon"] for cfg in batch["configs"]} >= {"60d", "120d"}
    assert all(cfg["mechanism_id"] for cfg in batch["configs"])
    assert all("recent_45d" not in cfg["output_dir"] for cfg in batch["configs"])


def test_build_value_route_batch_blocks_missing_data_routes():
    batch = build_value_route_batch(
        available_fields={"industry", "book_yield"},
        allowed_routes=["value_quality_no_distress"],
    )

    assert batch["configs"] == []
    assert batch["blocked"][0]["route_id"] == "value_quality_no_distress"
    assert "roe" in batch["blocked"][0]["missing_fields"]
