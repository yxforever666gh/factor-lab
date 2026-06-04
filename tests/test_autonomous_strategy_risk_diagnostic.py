from __future__ import annotations

import json

import pandas as pd

from factor_lab.autonomous_strategy_risk_diagnostic import (
    build_historical_valuation_risk_diagnostic,
    risk_diagnostic_to_markdown,
    write_risk_diagnostic,
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
            if ticker.startswith("cheap"):
                pb = pb_base + (0.8 if day < 6 else 0.0)
                pe = pe_base + (8.0 if day < 6 else 0.0)
            else:
                pb = pb_base - (0.8 if day < 6 else 0.0)
                pe = pe_base - (8.0 if day < 6 else 0.0)
            rows.append({
                "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=day),
                "ticker": ticker,
                "industry": "i1" if ticker.endswith("a") else "i2",
                "pb": pb,
                "pe_ttm": pe,
                "forward_return_5d": fwd,
            })
    return pd.DataFrame(rows)


def test_risk_diagnostic_identifies_drawdown_and_repair_candidates():
    diagnostic = build_historical_valuation_risk_diagnostic(
        run_id="x",
        frame=make_screen_frame(favorable=True),
        source_path="memory://screen.csv",
        cheap_screen_result={"overall_status": "manual_review", "recommended_next_step": "manual_review_risk"},
        window=6,
        min_periods=6,
        max_drawdown_limit=-0.35,
    )

    assert diagnostic["mode"] == "cheap_screen_risk_diagnostic"
    assert "max_drawdown" in diagnostic["original_drawdown"]
    assert diagnostic["repair_candidates"]
    assert diagnostic["controlled_execution_allowed"] is False
    assert diagnostic["queue_write_allowed"] is False


def test_risk_diagnostic_recommends_stop_when_no_repair_passes():
    diagnostic = build_historical_valuation_risk_diagnostic(
        run_id="x",
        frame=make_screen_frame(favorable=False),
        source_path="memory://screen.csv",
        cheap_screen_result={"overall_status": "manual_review", "recommended_next_step": "manual_review_risk"},
        window=6,
        min_periods=6,
        max_drawdown_limit=0.0,
    )

    assert diagnostic["overall_status"] == "fail"
    assert diagnostic["recommended_next_step"] == "stop_route_or_design_risk_filter"


def test_risk_diagnostic_markdown_and_write(tmp_path):
    diagnostic = build_historical_valuation_risk_diagnostic(
        run_id="x",
        frame=make_screen_frame(favorable=True),
        source_path="memory://screen.csv",
        cheap_screen_result={"overall_status": "manual_review", "recommended_next_step": "manual_review_risk"},
        window=6,
        min_periods=6,
    )
    markdown = risk_diagnostic_to_markdown(diagnostic)
    assert "Historical Valuation Cheap Screen Risk Diagnostic" in markdown
    assert "Repair candidates" in markdown

    paths = write_risk_diagnostic(diagnostic, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["mode"] == "cheap_screen_risk_diagnostic"
