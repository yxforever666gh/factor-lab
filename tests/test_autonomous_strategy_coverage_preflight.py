from __future__ import annotations

import json

import pandas as pd

from factor_lab.autonomous_strategy_coverage_preflight import (
    build_historical_valuation_coverage_preflight,
    coverage_preflight_to_markdown,
    write_coverage_preflight,
)


def sample_derivation_specs() -> dict:
    return {
        "derived_fields": [
            {
                "field": "pb_history_756d",
                "source_field": "pb",
                "derivation": "rolling_history_window:756d",
                "routes": ["historical_relative_valuation_repair"],
            },
            {
                "field": "pe_ttm_history_756d",
                "source_field": "pe_ttm",
                "derivation": "rolling_history_window:756d",
                "routes": ["historical_relative_valuation_repair"],
            },
        ]
    }


def sample_route_registry() -> dict:
    return {
        "routes": [
            {
                "route_id": "historical_relative_valuation_repair",
                "route_status": "cheap_screen_candidate",
                "required_fields": ["pb_history_756d", "pe_ttm_history_756d", "forward_return_5d"],
            }
        ]
    }


def make_frame(days_by_ticker: dict[str, int]) -> pd.DataFrame:
    rows = []
    for ticker, days in days_by_ticker.items():
        for day in range(days):
            rows.append(
                {
                    "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=day),
                    "ticker": ticker,
                    "pb": 1.0 + day / 1000,
                    "pe_ttm": 10.0 + day / 1000,
                    "forward_return_5d": 0.01,
                }
            )
    return pd.DataFrame(rows)


def test_coverage_preflight_passes_when_enough_tickers_have_full_history():
    frame = make_frame({"000001.SZ": 800, "000002.SZ": 790, "000003.SZ": 100})

    report = build_historical_valuation_coverage_preflight(
        run_id="x",
        route_registry=sample_route_registry(),
        derivation_specs=sample_derivation_specs(),
        frame=frame,
        source_path="memory://sample.csv",
        min_observations=756,
        min_eligible_ticker_ratio=0.60,
    )

    assert report["mode"] == "preflight_only"
    assert report["queue_write_allowed"] is False
    assert report["controlled_execution_allowed"] is False
    assert report["route_id"] == "historical_relative_valuation_repair"
    assert report["overall_status"] == "pass"
    assert report["required_manual_review"] is True
    assert {item["derived_field"] for item in report["field_coverage"]} == {
        "pb_history_756d",
        "pe_ttm_history_756d",
    }
    assert all(item["eligible_ticker_count"] == 2 for item in report["field_coverage"])


def test_coverage_preflight_blocks_when_source_field_missing_or_sparse():
    frame = make_frame({"000001.SZ": 100, "000002.SZ": 100, "000003.SZ": 100}).drop(columns=["pe_ttm"])

    report = build_historical_valuation_coverage_preflight(
        run_id="x",
        route_registry=sample_route_registry(),
        derivation_specs=sample_derivation_specs(),
        frame=frame,
        source_path="memory://sample.csv",
        min_observations=756,
        min_eligible_ticker_ratio=0.60,
    )

    assert report["overall_status"] == "blocked"
    statuses = {item["derived_field"]: item["status"] for item in report["field_coverage"]}
    assert statuses["pb_history_756d"] == "insufficient_history"
    assert statuses["pe_ttm_history_756d"] == "missing_source_field"
    assert "request_data_or_extend_cache" in report["next_allowed_actions"]


def test_coverage_preflight_markdown_and_write(tmp_path):
    frame = make_frame({"000001.SZ": 800, "000002.SZ": 800})
    report = build_historical_valuation_coverage_preflight(
        run_id="x",
        route_registry=sample_route_registry(),
        derivation_specs=sample_derivation_specs(),
        frame=frame,
        source_path="memory://sample.csv",
    )

    markdown = coverage_preflight_to_markdown(report)
    assert "Autonomous Strategy Coverage Preflight" in markdown
    assert "historical_relative_valuation_repair" in markdown
    assert "controlled_execution_allowed: False" in markdown

    paths = write_coverage_preflight(report, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["overall_status"] == "pass"
