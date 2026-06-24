from __future__ import annotations

import json

from factor_lab.market_phenomena_controlled_verdict import (
    build_controlled_research_verdict,
    validate_controlled_research_verdict,
    controlled_research_verdict_to_markdown,
    write_controlled_research_verdict_artifacts,
)


def execution_result(*, blocked: bool = False, weak_regime: bool = False, cost_bad: bool = False):
    if blocked:
        return {
            "run_id": "exec",
            "mode": "controlled_research_execution_result",
            "phenomenon_id": "p1",
            "summary": {"executed": 3, "blocked": 2, "total": 5},
            "check_results": [
                {"check_name": "industry_split_robustness", "status": "blocked_missing_columns", "missing_columns": ["industry"], "metrics": {}},
            ],
            "production_execution_allowed": False,
            "queue_write_allowed": False,
        }
    regime_spreads = {"risk_on": {"spread_vs_control": 0.01}, "risk_off": {"spread_vs_control": -0.01 if weak_regime else 0.006}}
    cost_metrics = {"gross_mean_return": 0.006, "cost_adjusted_mean_return": -0.002 if cost_bad else 0.004}
    return {
        "run_id": "exec",
        "mode": "controlled_research_execution_result",
        "phenomenon_id": "p1",
        "summary": {"executed": 6, "blocked": 0, "total": 6},
        "check_results": [
            {"check_name": "industry_split_robustness", "status": "executed", "metrics": {"spread_by_bucket": {"a": {"spread_vs_control": 0.01}, "b": {"spread_vs_control": 0.02}, "c": {"spread_vs_control": -0.001}}}},
            {"check_name": "size_split_robustness", "status": "executed", "metrics": {"spread_by_bucket": {"small": {"spread_vs_control": 0.01}, "mid": {"spread_vs_control": 0.004}, "large": {"spread_vs_control": 0.003}}}},
            {"check_name": "regime_split_robustness", "status": "executed", "metrics": {"spread_by_bucket": regime_spreads}},
            {"check_name": "turnover_sensitivity", "status": "executed", "metrics": {"spread_by_bucket": {"low": {"spread_vs_control": 0.002}, "high": {"spread_vs_control": 0.003}}}},
            {"check_name": "drawdown_sensitivity", "status": "executed", "metrics": {"downside_frequency": 0.48, "worst_forward_return": -0.08}},
            {"check_name": "cost_sensitivity_probe", "status": "executed", "metrics": cost_metrics},
        ],
        "production_execution_allowed": False,
        "queue_write_allowed": False,
    }


def test_controlled_verdict_continues_research_when_core_splits_are_supported():
    report = build_controlled_research_verdict(run_id="verdict", execution_result=execution_result())
    assert report["mode"] == "controlled_research_verdict"
    verdict = report["verdict"]
    assert verdict["decision"] == "continue_research"
    assert verdict["phenomenon_id"] == "p1"
    assert verdict["production_execution_allowed"] is False
    assert verdict["queue_write_allowed"] is False
    assert report["next_mutation_request"]["action"] == "continue_research"


def test_controlled_verdict_requests_data_when_execution_has_blockers():
    report = build_controlled_research_verdict(run_id="verdict", execution_result=execution_result(blocked=True))
    assert report["verdict"]["decision"] == "request_more_data"
    assert "execution_blocked_missing_data" in report["verdict"]["reason_codes"]
    assert report["next_mutation_request"]["action"] == "request_more_data"
    assert "industry" in json.dumps(report["next_mutation_request"], ensure_ascii=False)


def test_controlled_verdict_routes_regime_fragility_to_regime_filter_mutation():
    report = build_controlled_research_verdict(run_id="verdict", execution_result=execution_result(weak_regime=True))
    assert report["verdict"]["decision"] == "add_regime_filter"
    assert "regime_fragility_detected" in report["verdict"]["reason_codes"]
    assert report["next_mutation_request"]["action"] == "add_regime_filter"


def test_controlled_verdict_routes_cost_failure_to_risk_model_mutation():
    report = build_controlled_research_verdict(run_id="verdict", execution_result=execution_result(cost_bad=True))
    assert report["verdict"]["decision"] == "mutate_risk_or_cost_model"
    assert "cost_adjusted_return_non_positive" in report["verdict"]["reason_codes"]
    assert report["next_mutation_request"]["action"] == "mutate_risk_or_cost_model"


def test_validate_rejects_open_production_gates():
    report = build_controlled_research_verdict(run_id="verdict", execution_result=execution_result())
    report["verdict"]["queue_write_allowed"] = True
    validation = validate_controlled_research_verdict(report)
    assert validation["decision"] == "reject"
    assert "production_gate_not_closed_queue_write_allowed" in validation["reason_codes"]


def test_controlled_verdict_markdown_and_writes(tmp_path):
    report = build_controlled_research_verdict(run_id="verdict", execution_result=execution_result())
    assert validate_controlled_research_verdict(report)["decision"] == "keep"
    markdown = controlled_research_verdict_to_markdown(report)
    assert "Controlled Research Verdict" in markdown
    assert "continue_research" in markdown
    paths = write_controlled_research_verdict_artifacts(report, tmp_path)
    assert paths["verdict_json"].exists()
    assert paths["verdict_markdown"].exists()
    assert paths["mutation_request_json"].exists()
    payload = json.loads(paths["mutation_request_json"].read_text(encoding="utf-8"))
    assert payload["action"] == "continue_research"
