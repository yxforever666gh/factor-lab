import json

from factor_lab.small_institutional_self_diagnosis import (
    build_small_institutional_self_diagnosis,
    small_institutional_self_diagnosis_to_markdown,
    write_small_institutional_self_diagnosis,
)


def _preflight(status="ready"):
    return {
        "preflight_status": status,
        "estimated_combinations": {"total": 10, "ready": 10 if status == "ready" else 4},
        "dataset": {"min_date": "2020-01-01", "max_date": "2021-12-31"},
    }


def _matrix(status="ok", ok_count=10, insufficient=0, sharpe=1.0, drawdown=-0.2):
    return {
        "matrix_status": status,
        "summary": {"ok_count": ok_count, "insufficient_data_count": insufficient, "result_count": ok_count + insufficient},
        "best_result": {"sharpe": sharpe, "max_drawdown": drawdown, "total_return": 0.2, "cost_bps": 0},
        "results": [
            {"status": "ok", "label": "2020", "cost_bps": 0, "total_return": 0.2, "sharpe": sharpe, "max_drawdown": drawdown},
            {"status": "ok", "label": "2020", "cost_bps": 60, "total_return": 0.18, "sharpe": sharpe, "max_drawdown": drawdown},
        ],
    }


def test_self_diagnosis_blocks_data_coverage_gap():
    payload = build_small_institutional_self_diagnosis(
        preflight_payload=_preflight("partial"),
        matrix_payload=_matrix(status="partial", ok_count=4, insufficient=6),
    )

    assert payload["diagnosis_status"] == "blocked"
    assert payload["primary_issue"] == "data_coverage_gap"
    assert payload["next_action"] == "extend_backtest_dataset"
    assert payload["automation_allowed"] is False


def test_self_diagnosis_detects_drawdown_risk_before_scaling():
    payload = build_small_institutional_self_diagnosis(
        preflight_payload=_preflight("ready"),
        matrix_payload=_matrix(sharpe=1.2, drawdown=-0.55),
    )

    assert payload["diagnosis_status"] == "blocked"
    assert payload["primary_issue"] == "drawdown_risk_too_high"
    assert payload["next_action"] == "repair_simulated_portfolio_construction"


def test_self_diagnosis_detects_weak_risk_adjusted_return():
    payload = build_small_institutional_self_diagnosis(
        preflight_payload=_preflight("ready"),
        matrix_payload=_matrix(sharpe=0.2, drawdown=-0.2),
    )

    assert payload["diagnosis_status"] == "watch"
    assert payload["primary_issue"] == "weak_risk_adjusted_return"
    assert payload["recommended_run_mode"] == "bounded_matrix"


def test_self_diagnosis_allows_broader_simulation_when_metrics_pass():
    payload = build_small_institutional_self_diagnosis(
        preflight_payload=_preflight("ready"),
        matrix_payload=_matrix(sharpe=1.1, drawdown=-0.2),
    )

    assert payload["diagnosis_status"] == "ready"
    assert payload["primary_issue"] == "ready_for_broader_simulation"
    assert payload["next_action"] == "run_bounded_large_scale_simulation"
    assert payload["automation_allowed"] is True


def test_self_diagnosis_detects_cost_sensitivity():
    matrix = _matrix(sharpe=1.2, drawdown=-0.2)
    matrix["results"] = [
        {"status": "ok", "label": "2020", "cost_bps": 0, "total_return": 0.4, "sharpe": 1.2, "max_drawdown": -0.2},
        {"status": "ok", "label": "2020", "cost_bps": 60, "total_return": 0.1, "sharpe": 1.2, "max_drawdown": -0.2},
    ]

    payload = build_small_institutional_self_diagnosis(preflight_payload=_preflight("ready"), matrix_payload=matrix)

    assert payload["diagnosis_status"] == "watch"
    assert payload["primary_issue"] == "cost_sensitive_unstable"
    assert payload["next_action"] == "repair_cost_turnover_robustness"


def test_write_self_diagnosis_writes_json_and_markdown(tmp_path):
    preflight_path = tmp_path / "preflight.json"
    matrix_path = tmp_path / "matrix.json"
    json_path = tmp_path / "diagnosis.json"
    markdown_path = tmp_path / "diagnosis.md"
    preflight_path.write_text(json.dumps(_preflight("ready")), encoding="utf-8")
    matrix_path.write_text(json.dumps(_matrix(sharpe=1.1, drawdown=-0.2)), encoding="utf-8")

    payload = write_small_institutional_self_diagnosis(
        preflight_path=preflight_path,
        matrix_path=matrix_path,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["diagnosis_status"] == "ready"
    assert json.loads(json_path.read_text(encoding="utf-8"))["diagnosis_status"] == "ready"
    assert "Small Institutional Self Diagnosis" in markdown_path.read_text(encoding="utf-8")


def test_self_diagnosis_markdown_includes_machine_next_action():
    markdown = small_institutional_self_diagnosis_to_markdown(
        {
            "generated_at_utc": "2026-05-12T00:00:00+00:00",
            "diagnosis_status": "blocked",
            "primary_issue": "data_coverage_gap",
            "severity": "high",
            "next_action": "extend_backtest_dataset",
            "automation_allowed": False,
            "recommended_run_mode": "preflight_only",
            "evidence": ["insufficient data"],
        }
    )

    assert "extend_backtest_dataset" in markdown
    assert "data_coverage_gap" in markdown
