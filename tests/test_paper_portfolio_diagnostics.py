import json

from factor_lab.paper_portfolio_diagnostics import (
    build_cost_diagnostics,
    build_paper_portfolio_diagnostics,
    build_turnover_diagnostics,
    diagnostics_to_markdown,
    write_paper_portfolio_diagnostics,
)


def _portfolio(tickers):
    weight = round(1.0 / len(tickers), 6) if tickers else 0.0
    return {
        "strategy_name": "test_strategy",
        "as_of_date": "2026-03-20",
        "position_count": len(tickers),
        "positions": [{"ticker": ticker, "weight": weight} for ticker in tickers],
    }


def test_build_turnover_diagnostics_counts_added_removed_overlap_and_one_way_turnover():
    current = _portfolio(["AAA", "BBB", "CCC"])
    previous = _portfolio(["BBB", "CCC", "DDD"])

    turnover = build_turnover_diagnostics(current, previous)

    assert turnover["history_status"] == "ok"
    assert turnover["current_count"] == 3
    assert turnover["previous_count"] == 3
    assert turnover["added_count"] == 1
    assert turnover["removed_count"] == 1
    assert turnover["overlap_count"] == 2
    assert turnover["added_tickers"] == ["AAA"]
    assert turnover["removed_tickers"] == ["DDD"]
    assert turnover["turnover_one_way_estimate"] == round(1 / 3, 6)


def test_build_turnover_diagnostics_reports_insufficient_history_without_previous_portfolio():
    turnover = build_turnover_diagnostics(_portfolio(["AAA", "BBB"]), None)

    assert turnover["history_status"] == "insufficient_history"
    assert turnover["added_count"] is None
    assert turnover["removed_count"] is None
    assert turnover["overlap_count"] is None
    assert turnover["turnover_one_way_estimate"] is None


def test_build_cost_diagnostics_uses_cost_bps_for_one_way_and_round_trip_estimates():
    cost = build_cost_diagnostics({"turnover_one_way_estimate": 0.25}, cost_bps=30)

    assert cost["cost_bps"] == 30.0
    assert cost["estimated_one_way_cost"] == 0.00075
    assert cost["estimated_round_trip_cost"] == 0.0015


def test_build_paper_portfolio_diagnostics_reads_current_and_latest_previous_history(tmp_path):
    current_path = tmp_path / "current_portfolio.json"
    history_path = tmp_path / "portfolio_history.json"
    current_path.write_text(json.dumps(_portfolio(["AAA", "BBB", "CCC"])), encoding="utf-8")
    history_path.write_text(
        json.dumps([_portfolio(["OLD"]), _portfolio(["BBB", "CCC", "DDD"])]),
        encoding="utf-8",
    )

    payload = build_paper_portfolio_diagnostics(
        current_path=current_path,
        history_path=history_path,
        benchmark_id="CSI1000",
        benchmark_name="中证1000",
        cost_bps=30,
    )

    assert payload["strategy_name"] == "test_strategy"
    assert payload["benchmark"]["benchmark_id"] == "CSI1000"
    assert payload["benchmark"]["benchmark_name"] == "中证1000"
    assert payload["benchmark"]["tracking_mode"] == "metadata_only"
    assert payload["turnover"]["added_count"] == 1
    assert payload["cost"]["estimated_round_trip_cost"] == 0.002


def test_write_paper_portfolio_diagnostics_writes_json_and_markdown(tmp_path):
    current_path = tmp_path / "current_portfolio.json"
    history_path = tmp_path / "portfolio_history.json"
    json_path = tmp_path / "portfolio_diagnostics.json"
    markdown_path = tmp_path / "portfolio_diagnostics.md"
    current_path.write_text(json.dumps(_portfolio(["AAA", "BBB"])), encoding="utf-8")
    history_path.write_text(json.dumps([]), encoding="utf-8")

    payload = write_paper_portfolio_diagnostics(
        current_path=current_path,
        history_path=history_path,
        json_path=json_path,
        markdown_path=markdown_path,
        benchmark_id="CSI1000",
        benchmark_name="中证1000",
        cost_bps=30,
    )

    assert payload["turnover"]["history_status"] == "insufficient_history"
    assert json.loads(json_path.read_text(encoding="utf-8"))["benchmark"]["benchmark_id"] == "CSI1000"
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "Paper Portfolio Diagnostics" in markdown
    assert "insufficient_history" in markdown


def test_diagnostics_markdown_includes_benchmark_turnover_and_cost():
    markdown = diagnostics_to_markdown(
        {
            "generated_at_utc": "2026-03-20T00:00:00+00:00",
            "strategy_name": "test_strategy",
            "as_of_date": "2026-03-20",
            "position_count": 3,
            "benchmark": {"benchmark_id": "CSI1000", "benchmark_name": "中证1000", "tracking_mode": "metadata_only"},
            "turnover": {"history_status": "ok", "added_count": 1, "removed_count": 1, "overlap_count": 2, "turnover_one_way_estimate": 0.333333},
            "cost": {"cost_bps": 30.0, "estimated_one_way_cost": 0.001, "estimated_round_trip_cost": 0.002},
        }
    )

    assert "CSI1000" in markdown
    assert "turnover_one_way_estimate" in markdown
    assert "estimated_round_trip_cost" in markdown
