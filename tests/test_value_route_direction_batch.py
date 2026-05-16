import json
from pathlib import Path

from factor_lab.value_route_direction_batch import build_value_route_direction_batch, write_value_route_direction_batch


def test_direction_batch_creates_original_and_inverted_for_each_route():
    batch = build_value_route_direction_batch()
    configs = batch["configs"]

    route_dirs = {(cfg["route_id"], cfg["direction"]) for cfg in configs}

    assert ("industry_relative_value", "original") in route_dirs
    assert ("industry_relative_value", "inverted") in route_dirs
    assert ("value_quality_no_distress", "original") in route_dirs
    assert ("value_quality_no_distress", "inverted") in route_dirs
    assert ("value_momentum_confirmation", "original") in route_dirs
    assert ("value_momentum_confirmation", "inverted") in route_dirs
    assert len(configs) == 6


def test_direction_batch_inverted_keeps_same_window_and_output_separate():
    batch = build_value_route_direction_batch()
    by_route = {}
    for cfg in batch["configs"]:
        by_route.setdefault(cfg["route_id"], {})[cfg["direction"]] = cfg

    for route, pair in by_route.items():
        original = pair["original"]
        inverted = pair["inverted"]
        assert original["start_date"] == inverted["start_date"]
        assert original["end_date"] == inverted["end_date"]
        assert original["universe_limit"] == inverted["universe_limit"]
        assert original["cache_dir"] == inverted["cache_dir"]
        assert inverted["factors"][0]["expression"] == f"-({original['factors'][0]['expression']})"
        assert original["output_dir"] != inverted["output_dir"]


def test_write_value_route_direction_batch_writes_manifest_and_configs(tmp_path):
    batch = write_value_route_direction_batch(output_dir=tmp_path, dry_run=False)

    manifest = tmp_path / "manifest.json"
    assert manifest.exists()
    saved = json.loads(manifest.read_text())
    assert len(saved["configs"]) == 6
    assert len(list(tmp_path.glob("*.json"))) == 7
