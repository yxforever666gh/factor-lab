from __future__ import annotations

import json

import pandas as pd

from factor_lab.autonomous_strategy_risk_filter_probe import (
    build_value_trap_risk_filter_probe,
    risk_filter_probe_to_markdown,
    write_risk_filter_probe,
)


def make_probe_frame() -> pd.DataFrame:
    rows = []
    for ticker, pb_base, pe_base, fwd, vol, turnover, roe, debt in [
        ("cheap_good", 0.8, 8.0, 0.05, 0.1, 10, 0.2, 0.3),
        ("cheap_bad", 0.7, 7.0, -0.10, 0.9, 1, 0.01, 0.9),
        ("exp_good", 2.0, 20.0, -0.03, 0.1, 10, 0.2, 0.3),
        ("exp_bad", 2.2, 22.0, 0.02, 0.9, 1, 0.01, 0.9),
    ]:
        for day in range(12):
            if ticker.startswith("cheap"):
                pb = pb_base + (0.8 if day < 6 else 0.0)
                pe = pe_base + (8.0 if day < 6 else 0.0)
            else:
                pb = pb_base - (0.8 if day < 6 else 0.0)
                pe = pe_base - (8.0 if day < 6 else 0.0)
            rows.append({
                "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=day),
                "ticker": ticker,
                "pb": pb,
                "pe_ttm": pe,
                "forward_return_5d": fwd,
                "volatility_20": vol,
                "turnover": turnover,
                "roe": roe,
                "debt_to_asset": debt,
            })
    return pd.DataFrame(rows)


def test_risk_filter_probe_blocks_unless_route_verdict_allows_probe():
    probe = build_value_trap_risk_filter_probe(
        run_id="x",
        frame=make_probe_frame(),
        source_path="memory://probe.csv",
        route_verdict={"verdict": "stop_route"},
        window=6,
        min_periods=6,
    )

    assert probe["overall_status"] == "blocked"
    assert probe["recommended_next_step"] == "respect_route_verdict"


def test_risk_filter_probe_evaluates_candidate_filters():
    probe = build_value_trap_risk_filter_probe(
        run_id="x",
        frame=make_probe_frame(),
        source_path="memory://probe.csv",
        route_verdict={"verdict": "design_risk_filter_one_probe"},
        window=6,
        min_periods=6,
        max_drawdown_limit=-1.0,
        min_usable_rows=1,
    )

    candidates = {item["candidate"] for item in probe["candidate_results"]}
    assert "baseline_cheap_vs_expensive" in candidates
    assert "exclude_top_30pct_daily_volatility_20" in candidates
    assert "exclude_bottom_30pct_daily_turnover" in candidates
    assert "quality_overlay_roe_top70_debt_bottom70" in candidates
    assert probe["best_candidate"] is not None
    assert probe["controlled_execution_allowed"] is False
    assert probe["queue_write_allowed"] is False


def test_risk_filter_probe_markdown_and_write(tmp_path):
    probe = build_value_trap_risk_filter_probe(
        run_id="x",
        frame=make_probe_frame(),
        source_path="memory://probe.csv",
        route_verdict={"verdict": "design_risk_filter_one_probe"},
        window=6,
        min_periods=6,
        min_usable_rows=1,
    )
    markdown = risk_filter_probe_to_markdown(probe)
    assert "Value Trap Risk Filter Probe" in markdown
    assert "Candidates" in markdown

    paths = write_risk_filter_probe(probe, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["mode"] == "value_trap_risk_filter_probe"
