import json

from factor_lab.small_institutional_risk_reduction_plan import (
    build_risk_reduction_plan,
    risk_reduction_plan_to_markdown,
    write_risk_reduction_plan,
)


def _policy():
    return {
        "dataset_path": "dataset.csv",
        "signal_columns": ["industry_relative_book_yield", "industry_relative_earnings_yield", "roe"],
        "year_windows": [
            {"label": "2020-2021", "start_date": "2020-01-01", "end_date": "2021-12-31"},
            {"label": "2021-2022", "start_date": "2021-01-01", "end_date": "2022-12-31"},
        ],
        "holding_counts": [50, 75, 100],
        "rebalance_frequencies": ["monthly", "biweekly"],
        "cost_bps_values": [30, 60],
        "return_column": "forward_return_5d",
        "diagnosis_thresholds": {"max_drawdown_limit": -0.35},
    }


def _matrix():
    return {
        "schema_version": 2,
        "execution": {"planned_count": 72, "executed_count": 2, "cap": 2, "capped": True},
        "results": [
            {
                "combo_id": "already-ran-book",
                "status": "ok",
                "signal_column": "industry_relative_book_yield",
                "label": "2020-2021",
                "start_date": "2020-01-01",
                "end_date": "2021-12-31",
                "holding_count": 50,
                "rebalance_frequency": "monthly",
                "cost_bps": 30.0,
                "max_drawdown": -0.48,
            }
        ],
    }


def test_build_risk_reduction_plan_is_manual_review_only_and_bounded():
    plan = build_risk_reduction_plan(
        policy=_policy(),
        matrix_payload=_matrix(),
        available_columns=["volatility_20", "volatility_60", "turnover", "roe"],
        max_next_backtests=5,
    )

    assert plan["plan_status"] == "candidate_plan_ready"
    assert plan["automation_allowed"] is False
    assert plan["manual_review_required"] is True
    assert plan["queue_write_allowed"] is False
    assert plan["live_trading_enabled"] is False
    assert plan["max_next_backtests"] == 5
    assert len(plan["candidate_specs"]) == 5
    assert all(spec["risk_filters"] for spec in plan["candidate_specs"])
    assert all(spec["source"] == "risk_reduction_manual_review" for spec in plan["candidate_specs"])


def test_plan_prioritizes_unexecuted_signals_and_realistic_costs():
    plan = build_risk_reduction_plan(
        policy=_policy(),
        matrix_payload=_matrix(),
        available_columns=["volatility_20", "volatility_60", "turnover", "roe"],
        max_next_backtests=12,
    )

    signals = [spec["signal_column"] for spec in plan["candidate_specs"]]
    assert "industry_relative_earnings_yield" in signals[:6]
    assert "roe" in signals[:6]
    assert all(spec["cost_bps"] in [30.0, 60.0] for spec in plan["candidate_specs"])
    assert all(spec["combo_id"] != "already-ran-book" for spec in plan["candidate_specs"])


def test_missing_risk_filter_fields_blocks_execution_but_still_reports_gap():
    plan = build_risk_reduction_plan(
        policy=_policy(),
        matrix_payload=_matrix(),
        available_columns=["roe"],
        max_next_backtests=5,
    )

    assert plan["plan_status"] == "blocked_missing_risk_filter_fields"
    assert plan["candidate_specs"] == []
    assert plan["automation_allowed"] is False
    assert "volatility_20" in plan["missing_risk_filter_fields"]
    assert "turnover" in plan["missing_risk_filter_fields"]


def test_write_risk_reduction_plan_outputs_json_and_markdown(tmp_path):
    policy_path = tmp_path / "policy.json"
    matrix_path = tmp_path / "matrix.json"
    dataset_path = tmp_path / "dataset.csv"
    json_path = tmp_path / "risk_plan.json"
    markdown_path = tmp_path / "risk_plan.md"
    policy_path.write_text(json.dumps(_policy()), encoding="utf-8")
    matrix_path.write_text(json.dumps(_matrix()), encoding="utf-8")
    dataset_path.write_text("date,ticker,volatility_20,volatility_60,turnover,roe,forward_return_5d\n", encoding="utf-8")

    payload = write_risk_reduction_plan(
        policy_path=policy_path,
        matrix_path=matrix_path,
        dataset_path=dataset_path,
        json_path=json_path,
        markdown_path=markdown_path,
        max_next_backtests=3,
    )

    written = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    assert payload["candidate_count"] == 3
    assert written["candidate_count"] == 3
    assert "Small Institutional Risk Reduction Plan" in markdown
    assert "manual_review_required: True" in markdown


def test_write_plan_uses_policy_dataset_path_when_not_overridden(tmp_path):
    policy_path = tmp_path / "policy.json"
    matrix_path = tmp_path / "matrix.json"
    policy_dataset_path = tmp_path / "policy_dataset.csv"
    explicit_json_path = tmp_path / "risk_plan.json"
    explicit_markdown_path = tmp_path / "risk_plan.md"
    policy = _policy()
    policy["dataset_path"] = str(policy_dataset_path)
    policy_path.write_text(json.dumps(policy), encoding="utf-8")
    matrix_path.write_text(json.dumps(_matrix()), encoding="utf-8")
    policy_dataset_path.write_text("date,ticker,volatility_20,volatility_60,turnover,roe,forward_return_5d\n", encoding="utf-8")

    payload = write_risk_reduction_plan(
        policy_path=policy_path,
        matrix_path=matrix_path,
        json_path=explicit_json_path,
        markdown_path=explicit_markdown_path,
        max_next_backtests=2,
    )

    assert payload["source_dataset_path"] == str(policy_dataset_path)
    assert payload["plan_status"] == "candidate_plan_ready"
    assert payload["candidate_count"] == 2
    assert payload["missing_risk_filter_fields"] == []


def test_risk_reduction_markdown_surfaces_forbidden_actions():
    markdown = risk_reduction_plan_to_markdown(
        {
            "generated_at_utc": "2026-05-31T00:00:00+00:00",
            "plan_status": "candidate_plan_ready",
            "candidate_count": 1,
            "automation_allowed": False,
            "manual_review_required": True,
            "queue_write_allowed": False,
            "live_trading_enabled": False,
            "candidate_specs": [{"candidate_id": "x", "signal_column": "roe", "risk_filters": []}],
            "forbidden_actions": ["no_queue_write", "no_timer_enable"],
        }
    )

    assert "no_queue_write" in markdown
    assert "no_timer_enable" in markdown
