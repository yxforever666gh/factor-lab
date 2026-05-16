from factor_lab.value_route_bucket_aware_batch import build_value_route_bucket_aware_batch, write_value_route_bucket_aware_batch


def test_bucket_aware_batch_generates_value_quality_only_by_default():
    batch = build_value_route_bucket_aware_batch()

    assert len(batch["configs"]) == 1
    cfg = batch["configs"][0]
    assert cfg["route_id"] == "value_quality_no_distress"
    assert cfg["mechanism_id"] == "value_quality_no_distress"
    assert cfg["data_source"] == "tushare"
    assert cfg["cache_dir"] == "artifacts/tushare_cache"
    assert cfg["portfolio_construction"] == {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0}
    assert cfg["validation_protocol_name"] == "value_factor_default"


def test_bucket_aware_batch_include_all_routes_generates_three_configs():
    batch = build_value_route_bucket_aware_batch(include_all_routes=True)

    route_pairs = {cfg["route_id"]: cfg["portfolio_construction"] for cfg in batch["configs"]}
    assert set(route_pairs) == {"industry_relative_value", "value_quality_no_distress", "value_momentum_confirmation"}
    assert route_pairs["value_momentum_confirmation"]["short_quantile"] == 1


def test_write_bucket_aware_batch_writes_manifest_and_configs(tmp_path):
    batch = write_value_route_bucket_aware_batch(output_dir=tmp_path, dry_run=False)

    assert (tmp_path / "manifest.json").exists()
    assert len(list(tmp_path.glob("*_bucket_aware.json"))) == 1
