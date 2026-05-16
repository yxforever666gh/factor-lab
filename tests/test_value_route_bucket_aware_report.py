import json
from pathlib import Path

from factor_lab.value_route_bucket_aware_report import build_bucket_aware_comparison_report, write_bucket_aware_comparison_report


def test_bucket_aware_report_compares_original_and_bucket_result(tmp_path):
    original_dir = tmp_path / "original"
    bucket_dir = tmp_path / "bucket"
    original_dir.mkdir(); bucket_dir.mkdir()
    (original_dir / "results.json").write_text(json.dumps([{"factor_name":"x","rank_ic_mean":0.03,"top_bottom_spread_mean":-0.001103,"pass_gate":False}]))
    (bucket_dir / "results.json").write_text(json.dumps([{"factor_name":"x","rank_ic_mean":0.03,"top_bottom_spread_mean":-0.001103,"pass_gate":False}]))
    (bucket_dir / "bucket_aware_portfolio_results.json").write_text(json.dumps([{"factor_name":"x","spread_mean":0.006225,"pass_gate":True}]))

    report = build_bucket_aware_comparison_report(original_run_dir=original_dir, bucket_run_dir=bucket_dir)

    assert report["bucket_aware"]["spread_mean"] == 0.006225
    assert report["original"]["top_bottom_spread_mean"] == -0.001103
    assert report["decision"] == "expand_bucket_aware_routes"


def test_write_bucket_aware_report_outputs_json_and_markdown(tmp_path):
    original_dir = tmp_path / "original"
    bucket_dir = tmp_path / "bucket"
    output_dir = tmp_path / "out"
    original_dir.mkdir(); bucket_dir.mkdir()
    (original_dir / "results.json").write_text(json.dumps([{"factor_name":"x","rank_ic_mean":0.03,"top_bottom_spread_mean":-0.001103,"pass_gate":False}]))
    (bucket_dir / "results.json").write_text(json.dumps([{"factor_name":"x","rank_ic_mean":0.03,"top_bottom_spread_mean":-0.001103,"pass_gate":False}]))
    (bucket_dir / "bucket_aware_portfolio_results.json").write_text(json.dumps([{"factor_name":"x","spread_mean":0.006225,"pass_gate":True}]))

    result = write_bucket_aware_comparison_report(original_run_dir=original_dir, bucket_run_dir=bucket_dir, output_dir=output_dir)

    assert Path(result["json_path"]).exists()
    assert Path(result["markdown_path"]).exists()
    assert "expand_bucket_aware_routes" in Path(result["markdown_path"]).read_text()
