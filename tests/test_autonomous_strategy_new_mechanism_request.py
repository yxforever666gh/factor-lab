from __future__ import annotations

import json

from factor_lab.autonomous_strategy_new_mechanism_request import (
    build_new_mechanism_request,
    new_mechanism_request_to_markdown,
    write_new_mechanism_request,
)


def test_new_mechanism_request_summarizes_stopped_routes_and_constraints():
    report = build_new_mechanism_request(
        run_id="x",
        route_registry={"routes": [{"route_id": "r1", "route_status": "stopped", "stop_reason": "risk_failed", "recommended_next_step": "new"}]},
        stop_route_state={"final_decision": "stop_route", "stop_reason": "risk_failed", "drawdown_proxy": -1.0},
        distress_route_verdict={"verdict": "stop_route", "reason_codes": ["failed"]},
    )
    assert report["decision"] == "request_new_mechanism_or_external_distress_data"
    assert report["stopped_routes"][0]["route_id"] == "r1"
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False
    assert "do not run controlled backtests until cheap/risk screens pass" in report["do_not_repeat"]


def test_new_mechanism_request_markdown_and_write(tmp_path):
    report = build_new_mechanism_request(
        run_id="x",
        route_registry={"routes": []},
        stop_route_state={},
        distress_route_verdict={},
    )
    markdown = new_mechanism_request_to_markdown(report)
    assert "New Mechanism" in markdown
    assert "External data requests" in markdown
    paths = write_new_mechanism_request(report, tmp_path)
    assert paths["json"].exists()
    payload = json.loads(paths["json"].read_text())
    assert payload["decision"] == "request_new_mechanism_or_external_distress_data"
