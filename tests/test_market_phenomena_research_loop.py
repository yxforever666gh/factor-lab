from __future__ import annotations

import json

import pandas as pd

from factor_lab.market_phenomena_research_loop import (
    build_research_loop_report,
    research_loop_report_to_markdown,
    run_research_loop,
    write_research_loop_report,
)


def feature_frame() -> pd.DataFrame:
    rows = []
    for ticker, base in [("A", 10.0), ("B", 20.0), ("C", 30.0), ("D", 40.0)]:
        for i in range(130):
            rows.append(
                {
                    "date": f"2020-01-{(i % 28) + 1:02d}",
                    "ticker": ticker,
                    "industry": "x",
                    "close": base + i * 0.1,
                    "profit_yoy": 10 + i if ticker in {"A", "B"} else -10 - i,
                    "roe": 0.2 if ticker in {"A", "B"} else 0.05,
                    "debt_to_asset": 20 if ticker in {"A", "B"} else 80,
                    "operating_cashflow_to_profit": 1.2 if ticker in {"A", "B"} else 0.3,
                    "pb": 0.8 if ticker in {"A", "C"} else 2.0,
                    "debt_to_asset_delta": -5 if ticker in {"A", "B"} else 5,
                    "volume": 1000 + i,
                    "turnover_rate": 1.0,
                    "amount": 10000,
                    "market_cap": 100000,
                }
            )
    return pd.DataFrame(rows)


def test_run_research_loop_produces_all_core_artifacts_without_strategy_permissions():
    report = run_research_loop(run_id="loop", feature_frame=feature_frame())
    assert report["loop_status"] == "completed"
    assert report["steps_completed"] == [
        "candidates",
        "quality_review",
        "novelty_review",
        "data_feasibility",
        "minimal_verification_plan",
        "minimal_verification_result",
        "phenomenon_verdict",
        "memory_update",
    ]
    assert report["summary"]["phenomenon_count"] == 5
    assert report["strategy_generation_allowed"] is False
    assert report["backtest_allowed"] is False
    assert report["queue_write_allowed"] is False
    assert "phenomenon_verdict" in report["artifacts"]


def test_build_research_loop_report_surfaces_supported_and_rejected_counts():
    report = build_research_loop_report(
        run_id="loop",
        candidates={"phenomena": [{"phenomenon_id": "a"}]},
        quality_review={"summary": {"keep": 1}},
        novelty_review={"summary": {"keep": 1}},
        data_feasibility={"summary": {"ready_for_minimal_verification": 1}},
        minimal_plan={"summary": {"planned": 1}},
        minimal_result={"summary": {"experiment_count": 1, "pass": 1}},
        verdict={"summary": {"supported_for_further_research": 1}},
        artifacts={"x": "y"},
    )
    assert report["summary"]["supported_for_further_research"] == 1
    assert report["summary"]["experiment_count"] == 1
    assert report["loop_status"] == "completed"


def test_research_loop_markdown_and_write(tmp_path):
    report = run_research_loop(run_id="loop", feature_frame=feature_frame())
    markdown = research_loop_report_to_markdown(report)
    assert "Market Phenomena Research Loop" in markdown
    assert "not automatic strategy" in markdown
    paths = write_research_loop_report(report, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert payload["loop_status"] == "completed"
