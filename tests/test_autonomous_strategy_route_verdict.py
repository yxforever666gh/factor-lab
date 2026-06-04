from __future__ import annotations

import json

from factor_lab.autonomous_strategy_route_verdict import (
    build_autonomous_strategy_route_verdict,
    route_verdict_to_markdown,
    write_route_verdict,
)


def coverage(status="pass") -> dict:
    return {"overall_status": status}


def cheap(info="pass", risk="fail", spread=0.01, rank_ic=0.02) -> dict:
    return {
        "route_id": "historical_relative_valuation_repair",
        "information_screen_status": info,
        "risk_screen_status": risk,
        "cheap_expensive_spread": spread,
        "rank_ic": rank_ic,
        "drawdown_proxy": -1.0,
    }


def diagnostic(status="fail") -> dict:
    return {"overall_status": status, "recommended_next_step": "stop_route_or_design_risk_filter"}


def test_route_verdict_requests_data_when_coverage_not_passed():
    verdict = build_autonomous_strategy_route_verdict(
        run_id="x",
        coverage_preflight=coverage("blocked"),
        cheap_screen_result=cheap(),
        risk_diagnostic=diagnostic(),
    )

    assert verdict["verdict"] == "request_data"
    assert verdict["max_next_risk_filter_probes"] == 0
    assert verdict["controlled_execution_allowed"] is False


def test_route_verdict_allows_one_risk_filter_probe_for_weak_positive_signal():
    verdict = build_autonomous_strategy_route_verdict(
        run_id="x",
        coverage_preflight=coverage(),
        cheap_screen_result=cheap(),
        risk_diagnostic=diagnostic(),
    )

    assert verdict["verdict"] == "design_risk_filter_one_probe"
    assert verdict["max_next_risk_filter_probes"] == 1
    assert "weak_positive_signal_allows_one_more_risk_filter_probe" in verdict["reason_codes"]
    assert verdict["queue_write_allowed"] is False


def test_route_verdict_stops_route_when_information_screen_fails():
    verdict = build_autonomous_strategy_route_verdict(
        run_id="x",
        coverage_preflight=coverage(),
        cheap_screen_result=cheap(info="fail", spread=-0.01, rank_ic=-0.02),
        risk_diagnostic=diagnostic(),
    )

    assert verdict["verdict"] == "stop_route"
    assert verdict["max_next_risk_filter_probes"] == 0


def test_route_verdict_allows_controlled_backtest_only_when_info_and_risk_pass():
    verdict = build_autonomous_strategy_route_verdict(
        run_id="x",
        coverage_preflight=coverage(),
        cheap_screen_result=cheap(risk="pass"),
        risk_diagnostic={"overall_status": "pass"},
    )

    assert verdict["verdict"] == "allow_one_controlled_backtest"
    assert verdict["controlled_execution_allowed"] is True
    assert verdict["queue_write_allowed"] is False


def test_route_verdict_markdown_and_write(tmp_path):
    verdict = build_autonomous_strategy_route_verdict(
        run_id="x",
        coverage_preflight=coverage(),
        cheap_screen_result=cheap(),
        risk_diagnostic=diagnostic(),
    )
    markdown = route_verdict_to_markdown(verdict)
    assert "Autonomous Strategy Route Verdict" in markdown
    assert "design_risk_filter_one_probe" in markdown

    paths = write_route_verdict(verdict, tmp_path)
    assert paths["json"].exists()
    assert paths["markdown"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["verdict"] == "design_risk_filter_one_probe"
