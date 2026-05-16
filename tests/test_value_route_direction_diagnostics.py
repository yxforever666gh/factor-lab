from pathlib import Path
import json

from factor_lab.value_route_direction_diagnostics import (
    diagnose_metric_direction,
    load_run_metrics,
    write_direction_diagnostics,
)


def test_direction_diagnostic_prefers_inverted_when_spread_flips_positive():
    decision = diagnose_metric_direction(
        {
            "route_id": "value_quality_no_distress",
            "factor_name": "value_quality",
            "original": {"rank_ic_mean": 0.03, "top_bottom_spread_mean": -0.001},
            "inverted": {"rank_ic_mean": -0.03, "top_bottom_spread_mean": 0.001},
        }
    )

    assert decision["recommendation"] == "invert_signal_or_portfolio_direction_check"
    assert "original_positive_ic_negative_spread" in decision["reasons"]
    assert "inverted_spread_positive" in decision["reasons"]


def test_direction_diagnostic_flags_weak_mechanism_when_both_spreads_negative():
    decision = diagnose_metric_direction(
        {
            "route_id": "industry_relative_value",
            "factor_name": "industry_relative_book_yield",
            "original": {"rank_ic_mean": 0.01, "top_bottom_spread_mean": -0.001},
            "inverted": {"rank_ic_mean": -0.01, "top_bottom_spread_mean": -0.0002},
        }
    )

    assert decision["recommendation"] == "mechanism_or_portfolio_construction_review"


def test_load_run_metrics_reads_results_and_factor_scores(tmp_path):
    run_dir = tmp_path / "route_original"
    run_dir.mkdir()
    (run_dir / "results.json").write_text(json.dumps([{
        "factor_name": "x",
        "rank_ic_mean": 0.02,
        "rank_ic_ir": 0.1,
        "top_bottom_spread_mean": -0.001,
        "pass_gate": False,
    }]))
    (run_dir / "factor_scores.json").write_text(json.dumps([{"factor_name": "x", "score": 0.4}]))

    metrics = load_run_metrics(run_dir, route_id="route", direction="original")

    assert metrics["factor_name"] == "x"
    assert metrics["rank_ic_mean"] == 0.02
    assert metrics["score"] == 0.4


def test_write_direction_diagnostics_outputs_json_and_markdown(tmp_path):
    output = write_direction_diagnostics(
        [
            {
                "route_id": "route",
                "factor_name": "x",
                "original": {"rank_ic_mean": 0.02, "top_bottom_spread_mean": -0.001},
                "inverted": {"rank_ic_mean": -0.02, "top_bottom_spread_mean": 0.001},
            }
        ],
        output_dir=tmp_path,
    )

    assert Path(output["json_path"]).exists()
    assert Path(output["markdown_path"]).exists()
    assert "invert_signal_or_portfolio_direction_check" in Path(output["markdown_path"]).read_text()
