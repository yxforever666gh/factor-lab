import json

import pandas as pd

from factor_lab.small_institutional_risk_reduction_executor import (
    apply_candidate_risk_filters,
    build_risk_reduction_executor_results,
    write_risk_reduction_executor_results,
)


def _dataset():
    return pd.DataFrame(
        [
            {"date": "2023-01-01", "ticker": "A", "signal": 3, "forward_return_5d": 0.03, "volatility_20": "0.10", "turnover": "0.10", "roe": "0.30"},
            {"date": "2023-01-01", "ticker": "B", "signal": 2, "forward_return_5d": 0.01, "volatility_20": "0.20", "turnover": "0.20", "roe": "0.20"},
            {"date": "2023-01-01", "ticker": "C", "signal": 1, "forward_return_5d": -0.01, "volatility_20": "0.90", "turnover": "0.90", "roe": "0.10"},
            {"date": "2023-01-08", "ticker": "A", "signal": 1, "forward_return_5d": 0.01, "volatility_20": "0.90", "turnover": "0.10", "roe": "0.30"},
            {"date": "2023-01-08", "ticker": "B", "signal": 3, "forward_return_5d": 0.02, "volatility_20": "0.20", "turnover": "0.20", "roe": "0.20"},
            {"date": "2023-01-08", "ticker": "C", "signal": 2, "forward_return_5d": -0.02, "volatility_20": "0.10", "turnover": "0.90", "roe": "0.10"},
        ]
    )


def _plan():
    return {
        "schema_version": 1,
        "source_dataset_path": "dataset.csv",
        "automation_allowed": False,
        "manual_review_required": True,
        "queue_write_allowed": False,
        "live_trading_enabled": False,
        "candidate_count": 2,
        "drawdown_limit": -0.35,
        "candidate_specs": [
            {
                "candidate_id": "cand-1",
                "combo_id": "combo-1",
                "source": "risk_reduction_manual_review",
                "signal_column": "signal",
                "label": "2023",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "holding_count": 1,
                "rebalance_frequency": "monthly",
                "cost_bps": 30.0,
                "return_column": "forward_return_5d",
                "risk_filters": [{"field": "volatility_20", "operator": "lte_quantile", "quantile": 0.5}],
            },
            {
                "candidate_id": "cand-2",
                "combo_id": "combo-2",
                "source": "risk_reduction_manual_review",
                "signal_column": "signal",
                "label": "2023",
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "holding_count": 1,
                "rebalance_frequency": "monthly",
                "cost_bps": 60.0,
                "return_column": "forward_return_5d",
                "risk_filters": [{"field": "roe", "operator": "gte_quantile", "quantile": 0.5}],
            },
        ],
    }


def test_apply_candidate_risk_filters_uses_per_date_quantiles_and_coerces_numeric():
    filtered = apply_candidate_risk_filters(
        _dataset(), [{"field": "volatility_20", "operator": "lte_quantile", "quantile": 0.5}]
    )

    assert filtered["ticker"].tolist() == ["A", "B", "B", "C"]
    assert filtered["volatility_20"].dtype.kind in "fc"


def test_apply_candidate_risk_filters_supports_gte_quantile_per_date():
    filtered = apply_candidate_risk_filters(_dataset(), [{"field": "roe", "operator": "gte_quantile", "quantile": 0.5}])

    assert filtered["ticker"].tolist() == ["A", "B", "A", "B"]


def test_executor_caps_candidates_and_preserves_guardrails(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    plan_path = tmp_path / "plan.json"
    _dataset().to_csv(dataset_path, index=False)
    plan = _plan()
    plan["source_dataset_path"] = str(dataset_path)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    payload = build_risk_reduction_executor_results(plan_path=plan_path, max_candidates=1)

    assert payload["automation_allowed"] is False
    assert payload["manual_review_required"] is True
    assert payload["queue_write_allowed"] is False
    assert payload["live_trading_enabled"] is False
    assert payload["execution"]["executed_count"] == 1
    assert payload["execution"]["capped"] is True
    row = payload["results"][0]
    assert row["candidate_id"] == "cand-1"
    assert row["combo_id"] == "combo-1"
    assert row["risk_filters"] == plan["candidate_specs"][0]["risk_filters"]
    assert row["status"] == "ok"
    assert "metrics" in row
    assert row["metrics"]["rebalance_count"] > 0


def test_executor_enforces_hard_controlled_cap_even_if_caller_requests_more(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    plan_path = tmp_path / "plan.json"
    _dataset().to_csv(dataset_path, index=False)
    plan = _plan()
    plan["source_dataset_path"] = str(dataset_path)
    expanded_specs = []
    for index in range(30):
        spec = dict(plan["candidate_specs"][index % 2])
        spec["candidate_id"] = f"cand-{index}"
        spec["combo_id"] = f"combo-{index}"
        expanded_specs.append(spec)
    plan["candidate_specs"] = expanded_specs
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    payload = build_risk_reduction_executor_results(plan_path=plan_path, max_candidates=999)

    assert payload["execution"]["cap"] == 20
    assert payload["execution"]["executed_count"] == 20
    assert payload["execution"]["planned_count"] == 30


def test_writer_rejects_forbidden_output_paths(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    plan_path = tmp_path / "plan.json"
    _dataset().to_csv(dataset_path, index=False)
    plan = _plan()
    plan["source_dataset_path"] = str(dataset_path)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    import pytest

    with pytest.raises(ValueError, match="forbidden_output_path"):
        write_risk_reduction_executor_results(
            plan_path=plan_path,
            max_candidates=1,
            json_path=tmp_path / "configs" / "bad.json",
            markdown_path=tmp_path / "risk_reduction_results.md",
            repair_json_path=tmp_path / "risk_reduction_repair.json",
            repair_markdown_path=tmp_path / "risk_reduction_repair.md",
            policy_path=policy_path,
        )


def test_executor_handles_missing_filter_field_as_controlled_insufficient_data(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    plan_path = tmp_path / "plan.json"
    frame = _dataset().drop(columns=["volatility_20"])
    frame.to_csv(dataset_path, index=False)
    plan = _plan()
    plan["source_dataset_path"] = str(dataset_path)
    plan["candidate_specs"] = plan["candidate_specs"][:1]
    plan_path.write_text(json.dumps(plan), encoding="utf-8")

    payload = build_risk_reduction_executor_results(plan_path=plan_path, max_candidates=5)

    assert payload["summary"]["insufficient_data_count"] == 1
    assert payload["results"][0]["status"] == "insufficient_data"
    assert payload["results"][0]["reason"] == "missing_risk_filter_columns"
    assert payload["results"][0]["missing_columns"] == ["volatility_20"]


def test_write_executor_results_outputs_results_and_repair_artifacts(tmp_path):
    dataset_path = tmp_path / "dataset.csv"
    plan_path = tmp_path / "plan.json"
    results_path = tmp_path / "risk_reduction_results.json"
    results_md = tmp_path / "risk_reduction_results.md"
    repair_path = tmp_path / "risk_reduction_repair.json"
    repair_md = tmp_path / "risk_reduction_repair.md"
    policy_path = tmp_path / "policy.json"
    _dataset().to_csv(dataset_path, index=False)
    plan = _plan()
    plan["source_dataset_path"] = str(dataset_path)
    plan_path.write_text(json.dumps(plan), encoding="utf-8")
    policy_path.write_text(json.dumps({"diagnosis_thresholds": {"max_drawdown_limit": -0.35}}), encoding="utf-8")

    payload = write_risk_reduction_executor_results(
        plan_path=plan_path,
        max_candidates=2,
        json_path=results_path,
        markdown_path=results_md,
        repair_json_path=repair_path,
        repair_markdown_path=repair_md,
        policy_path=policy_path,
    )

    assert payload["summary"]["result_count"] == 2
    assert json.loads(results_path.read_text(encoding="utf-8"))["execution"]["executed_count"] == 2
    assert "Risk Reduction Controlled Executor" in results_md.read_text(encoding="utf-8")
    repair = json.loads(repair_path.read_text(encoding="utf-8"))
    assert repair["automation_allowed"] is False
    assert repair["matrix_path"] == str(results_path)
    assert "Simulated Portfolio Construction Repair" in repair_md.read_text(encoding="utf-8")
