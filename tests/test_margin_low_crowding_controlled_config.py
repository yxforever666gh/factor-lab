from scripts.write_margin_low_crowding_controlled_config import build_admission_dry_run, build_config


def test_build_config_has_margin_overlay_and_bucket_pair():
    cfg = build_config()
    assert cfg["feature_overlay_csv"].endswith("margin_feature_monthly_panel.csv")
    assert cfg["portfolio_construction"]["mode"] == "bucket_pair"
    assert cfg["factors"][0]["expression"] == "margin_low_crowding_confirmation"


def test_admission_dry_run_allows_with_overlay_fields():
    dry = build_admission_dry_run(build_config())
    assert dry["admission"]["decision"] == "allow"
    assert dry["would_enqueue_count"] == 1
    assert dry["no_queue_write"] is True
