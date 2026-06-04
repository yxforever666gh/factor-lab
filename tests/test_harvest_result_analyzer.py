import json
from pathlib import Path

from factor_lab.harvest_result_analyzer import analyze_result_payload, analyze_cycle_result


def _payload():
    return {
        "matrix_status": "ok",
        "summary": {"ok_count": 6, "result_count": 6},
        "best_result": {
            "label": "2021-2022",
            "signal_column": "industry_relative_book_yield",
            "cost_bps": 0.0,
            "total_return": 0.93,
            "sharpe": 0.45,
            "max_drawdown": -0.49,
        },
        "results": [
            {"label": "2021-2022", "signal_column": "industry_relative_book_yield", "cost_bps": 0.0, "total_return": 0.93, "sharpe": 0.45, "max_drawdown": -0.49, "status": "ok"},
            {"label": "2021-2022", "signal_column": "industry_relative_book_yield", "cost_bps": 30.0, "total_return": -0.10, "sharpe": -0.2, "max_drawdown": -0.52, "status": "ok"},
            {"label": "2021-2022", "signal_column": "industry_relative_book_yield", "cost_bps": 60.0, "total_return": -0.30, "sharpe": -0.4, "max_drawdown": -0.55, "status": "ok"},
            {"label": "2020-2021", "signal_column": "industry_relative_book_yield", "cost_bps": 30.0, "total_return": -0.05, "sharpe": -0.1, "max_drawdown": -0.35, "status": "ok"},
            {"label": "2022-2023", "signal_column": "industry_relative_book_yield", "cost_bps": 30.0, "total_return": -0.02, "sharpe": -0.1, "max_drawdown": -0.30, "status": "ok"},
            {"label": "2021-2022", "signal_column": "earnings_yield", "cost_bps": 30.0, "total_return": 0.01, "sharpe": 0.05, "max_drawdown": -0.20, "status": "ok"},
        ],
    }


def test_analyze_result_payload_flags_risk_and_cost_sensitivity():
    analysis = analyze_result_payload(_payload(), cycle_id="cycle_x")
    assert analysis["cycle_id"] == "cycle_x"
    assert analysis["best_total_return"] == 0.93
    assert analysis["drawdown_too_high"] is True
    assert analysis["sharpe_too_low"] is True
    assert analysis["cost_sensitive"] is True
    assert analysis["window_concentration_risk"] is True
    assert analysis["promotion_ready"] is False


def test_analyze_cycle_result_reads_harvest_artifact(tmp_path):
    run_dir = tmp_path / "artifacts/harvest_agent/cycle_0001/runs/value_quality_cost_sensitivity_v1"
    run_dir.mkdir(parents=True)
    (run_dir / "result.json").write_text(json.dumps(_payload()), encoding="utf-8")
    analysis = analyze_cycle_result(tmp_path, "cycle_0001")
    assert analysis["cycle_id"] == "cycle_0001"
    assert analysis["source_result_path"].endswith("result.json")
    assert analysis["ok_count"] == 6
