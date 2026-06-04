from __future__ import annotations

import json

import pandas as pd

from factor_lab.autonomous_strategy_cheap_screen_runner import (
    add_historical_valuation_screen_features,
    build_historical_valuation_cheap_screen_result,
    cheap_screen_result_to_markdown,
    write_cheap_screen_result,
)


def make_screen_frame(*, favorable: bool = True) -> pd.DataFrame:
    rows = []
    for ticker, pb_base, pe_base, fwd in [
        ("cheap_a", 0.8, 8.0, 0.05 if favorable else -0.05),
        ("cheap_b", 0.9, 9.0, 0.04 if favorable else -0.04),
        ("exp_a", 2.0, 20.0, -0.03 if favorable else 0.03),
        ("exp_b", 2.1, 21.0, -0.02 if favorable else 0.02),
    ]:
        for day in range(12):
            # First half high for cheap tickers then valuation falls; expensive tickers do the inverse.
            if ticker.startswith("cheap"):
                pb = pb_base + (0.8 if day < 6 else 0.0)
                pe = pe_base + (8.0 if day < 6 else 0.0)
            else:
                pb = pb_base - (0.8 if day < 6 else 0.0)
                pe = pe_base - (8.0 if day < 6 else 0.0)
            rows.append({
                "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=day),
                "ticker": ticker,
                "industry": "i1",
                "pb": pb,
                "pe_ttm": pe,
                "forward_return_5d": fwd,
            })
    return pd.DataFrame(rows)


def test_add_historical_valuation_screen_features_builds_percentiles_and_buckets():
    frame = make_screen_frame()
    featured = add_historical_valuation_screen_features(frame, window=6, min_periods=6)

    assert "pb_history_percentile" in featured.columns
    assert "pe_ttm_history_percentile" in featured.columns
    assert "historical_valuation_cheapness" in featured.columns
    assert set(featured["valuation_bucket"].dropna().unique()) <= {"cheap", "middle", "expensive"}
    assert featured["valuation_bucket"].notna().sum() > 0


def test_metric_bearing_cheap_screen_passes_when_cheap_bucket_outperforms():
    result = build_historical_valuation_cheap_screen_result(
        run_id="x",
        frame=make_screen_frame(favorable=True),
        source_path="memory://screen.csv",
        window=6,
        min_periods=6,
        min_rows=8,
    )

    assert result["mode"] == "metric_bearing_cheap_screen"
    assert result["information_screen_status"] == "pass"
    assert result["risk_screen_status"] == "pass"
    assert result["overall_status"] == "pass"
    assert result["recommended_next_step"] == "allow_one_controlled_backtest"
    assert result["controlled_execution_allowed"] is True
    assert result["queue_write_allowed"] is False


def test_metric_bearing_cheap_screen_fails_when_cheap_bucket_underperforms():
    result = build_historical_valuation_cheap_screen_result(
        run_id="x",
        frame=make_screen_frame(favorable=False),
        source_path="memory://screen.csv",
        window=6,
        min_periods=6,
        min_rows=8,
    )

    assert result["information_screen_status"] == "fail"
    assert result["overall_status"] == "fail"
    assert result["recommended_next_step"] == "stop_route_or_switch_mechanism"
    assert result["controlled_execution_allowed"] is False


def test_cheap_screen_result_markdown_and_write(tmp_path):
    result = build_historical_valuation_cheap_screen_result(
        run_id="x",
        frame=make_screen_frame(favorable=True),
        source_path="memory://screen.csv",
        window=6,
        min_periods=6,
        min_rows=8,
    )
    markdown = cheap_screen_result_to_markdown(result)
    assert "Historical Valuation Cheap Screen Result" in markdown
    assert "recommended_next_step" in markdown

    paths = write_cheap_screen_result(result, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["overall_status"] == "pass"
