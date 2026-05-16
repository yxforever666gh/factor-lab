import json

from factor_lab.portfolio_constraint_hardening import (
    build_portfolio_constraint_hardening,
    evaluate_portfolio_constraints,
    portfolio_constraint_hardening_to_markdown,
    write_portfolio_constraint_hardening,
)


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _policy(**overrides):
    payload = {
        "target_holdings_min": 50,
        "target_holdings_max": 100,
        "benchmark_candidates": ["CSI500", "CSI1000"],
        "portfolio_constraints_next_phase": {
            "single_name_weight_cap": 0.02,
            "turnover_budget_per_rebalance": 0.35,
            "cost_bps_round_trip_placeholder": 30,
        },
    }
    payload.update(overrides)
    return payload


def _portfolio(position_count=72, max_weight=0.013889):
    positions = [{"ticker": f"000{i:03d}.SZ", "weight": max_weight if i == 0 else 0.01} for i in range(position_count)]
    return {
        "strategy_name": "small_institutional_value_sleeve_mvp",
        "as_of_date": "2021-12-28",
        "position_count": position_count,
        "positions": positions,
        "constraints": {"max_position_weight": max_weight},
    }


def _diagnostics(turnover=0.0, cost=0.0, benchmark_id="CSI1000"):
    return {
        "benchmark": {"benchmark_id": benchmark_id, "tracking_mode": "metadata_only"},
        "turnover": {"turnover_one_way_estimate": turnover},
        "cost": {"estimated_round_trip_cost": cost},
    }


def _tracking(status="ok", portfolio_return=0.01842):
    return {
        "tracking_status": status,
        "portfolio_return": {
            "portfolio_forward_return": portfolio_return,
            "matched_position_count": 72,
            "missing_position_count": 0,
            "coverage": 1.0,
        },
    }


def test_evaluate_portfolio_constraints_passes_when_all_hard_constraints_are_met():
    result = evaluate_portfolio_constraints(_policy(), _portfolio(), _diagnostics(), _tracking())

    assert result["constraint_status"] == "pass"
    assert result["violations"] == []
    assert result["checks"]["position_count"]["passed"] is True
    assert result["checks"]["single_name_weight_cap"]["passed"] is True
    assert result["checks"]["benchmark"]["passed"] is True
    assert result["checks"]["retrospective_tracking"]["passed"] is True


def test_evaluate_portfolio_constraints_fails_when_position_count_out_of_range():
    result = evaluate_portfolio_constraints(_policy(), _portfolio(position_count=20), _diagnostics(), _tracking())

    assert result["constraint_status"] == "fail"
    assert "position_count_out_of_range" in result["violations"]
    assert result["checks"]["position_count"]["actual"] == 20


def test_evaluate_portfolio_constraints_fails_when_single_name_cap_is_breached():
    result = evaluate_portfolio_constraints(_policy(), _portfolio(max_weight=0.05), _diagnostics(), _tracking())

    assert result["constraint_status"] == "fail"
    assert "single_name_weight_cap_breached" in result["violations"]
    assert result["checks"]["single_name_weight_cap"]["actual"] == 0.05


def test_evaluate_portfolio_constraints_waits_when_retrospective_tracking_not_ready():
    result = evaluate_portfolio_constraints(_policy(), _portfolio(), _diagnostics(), _tracking(status="insufficient_forward_window"))

    assert result["constraint_status"] == "wait"
    assert "retrospective_tracking_not_ready" in result["warnings"]
    assert result["checks"]["retrospective_tracking"]["passed"] is False


def test_build_portfolio_constraint_hardening_loads_inputs_and_writes_markdown(tmp_path):
    policy = _write_json(tmp_path / "policy.json", _policy())
    portfolio = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    tracking = _write_json(tmp_path / "retrospective_return_tracking.json", _tracking())

    payload = build_portfolio_constraint_hardening(
        policy_path=policy,
        portfolio_path=portfolio,
        diagnostics_path=diagnostics,
        retrospective_tracking_path=tracking,
    )

    assert payload["constraint_status"] == "pass"
    markdown = portfolio_constraint_hardening_to_markdown(payload)
    assert "Portfolio Constraint Hardening" in markdown
    assert "single_name_weight_cap" in markdown


def test_write_portfolio_constraint_hardening_writes_json_and_markdown(tmp_path):
    policy = _write_json(tmp_path / "policy.json", _policy())
    portfolio = _write_json(tmp_path / "current_portfolio.json", _portfolio())
    diagnostics = _write_json(tmp_path / "portfolio_diagnostics.json", _diagnostics())
    tracking = _write_json(tmp_path / "retrospective_return_tracking.json", _tracking())
    json_path = tmp_path / "portfolio_constraint_hardening.json"
    markdown_path = tmp_path / "portfolio_constraint_hardening.md"

    payload = write_portfolio_constraint_hardening(
        policy_path=policy,
        portfolio_path=portfolio,
        diagnostics_path=diagnostics,
        retrospective_tracking_path=tracking,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    assert payload["constraint_status"] == "pass"
    assert json.loads(json_path.read_text(encoding="utf-8"))["constraint_status"] == "pass"
    assert "Portfolio Constraint Hardening" in markdown_path.read_text(encoding="utf-8")
