import numpy as np
import pandas as pd
import pytest

from factor_lab.research_os.evaluator import (
    CANONICAL_EVALUATOR_VERSION,
    CanonicalLongOnlyEvaluator,
)


def _frame(stocks=50, days=14):
    dates = pd.bdate_range("2024-01-02", periods=days)
    rows = []
    for day_index, day in enumerate(dates):
        for stock_index in range(stocks):
            rows.append(
                {
                    "date": day,
                    "ticker": f"S{stock_index:03d}",
                    "open_adj": 10 + day_index * 0.02 + stock_index * 0.001,
                    "adv_20": 1e9,
                    "volatility_20": 0.02,
                    "eligible": True,
                    "signal": float(stock_index),
                }
            )
    return pd.DataFrame(rows)


def _policy():
    return {
        "portfolio": {
            "mode": "long_only",
            "capital": 50_000_000,
            "holding_days": 5,
            "rebalance_every_days": 5,
            "position_count": 50,
            "target_weight": 0.02,
            "max_adv_participation": 0.05,
            "open_column": "open_adj",
        }
    }


def test_canonical_evaluator_is_deterministically_identified():
    evaluator = CanonicalLongOnlyEvaluator()
    frame = _frame()
    first = evaluator.evaluate(
        experiment_id="e1",
        snapshot_id="s1",
        factor_or_sleeve_id="value_quality",
        frame=frame,
        signal="signal",
        portfolio_policy=_policy(),
    )
    second = evaluator.evaluate(
        experiment_id="e1",
        snapshot_id="s1",
        factor_or_sleeve_id="value_quality",
        frame=frame,
        signal="signal",
        portfolio_policy=_policy(),
    )
    assert first.run_id == second.run_id
    assert first.evaluator_version == CANONICAL_EVALUATOR_VERSION
    assert first.status == "ok"
    assert first.result["max_position_weight"] <= 0.02 + 1e-12
    assert first.result["promotion_eligible"] is False
    assert {
        "missing_pit_exposure_data",
        "missing_historical_returns_for_risk_model",
    } <= set(first.result["promotion_blockers"])


def _risk_inputs(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    tickers = sorted(frame["ticker"].unique())
    exposure = pd.DataFrame(
        {
            "date": pd.Timestamp("2023-12-29"),
            "available_at": pd.Timestamp("2023-12-29T07:00:00Z"),
            "ticker": tickers,
            "industry": [f"industry-{index % 3}" for index in range(len(tickers))],
            "size_bucket": ["large" if index % 2 else "small" for index in range(len(tickers))],
            "beta": np.linspace(0.95, 1.05, len(tickers)),
            "adv_20": 1e9,
            "industry_is_pit": True,
        }
    )
    rng = np.random.default_rng(17)
    history = pd.DataFrame(
        rng.normal(0.0, 0.01, size=(100, len(tickers))),
        index=pd.bdate_range("2023-08-15", periods=100),
        columns=tickers,
    )
    return exposure, history


def test_canonical_evaluator_executes_pit_risk_optimized_targets() -> None:
    frame = _frame(stocks=60)
    exposure, history = _risk_inputs(frame)
    policy = _policy()
    policy["portfolio"]["position_count"] = 60
    policy["portfolio"]["target_weight"] = 0.02

    result = CanonicalLongOnlyEvaluator().evaluate(
        experiment_id="risk-e1",
        snapshot_id="risk-s1",
        factor_or_sleeve_id="value_quality",
        frame=frame,
        signal="signal",
        portfolio_policy=policy,
        exposure_frame=exposure,
        returns_history=history,
        optimization_policy={"risk_aversion": 100_000.0, "turnover_penalty": 0.0},
    )

    assert result.status == "ok"
    assert result.result["promotion_eligible"] is True
    assert result.result["promotion_blockers"] == []
    assert result.result["target_weight_mode"] == "optimized"
    assert result.result["optimization_audit"]
    assert any(
        len({round(weight, 8) for weight in period["target_weights"].values()}) > 1
        for period in result.result["periods"]
    )
    for audit in result.result["optimization_audit"]:
        optimizer = audit["optimizer"]
        assert optimizer["covariance_method"] == "ledoit_wolf"
        assert optimizer["constraints"] == {
            "industry_deviation": 0.05,
            "size_deviation": 0.05,
            "beta_min": 0.9,
            "beta_max": 1.1,
            "max_position_weight": 0.02,
            "max_adv_participation": 0.05,
            "capital": 50_000_000.0,
            "minimum_return_observations": 60,
        }


def test_missing_trusted_industry_runs_diagnostic_but_blocks_promotion() -> None:
    frame = _frame(stocks=60)
    exposure, history = _risk_inputs(frame)
    exposure = exposure.drop(columns="industry")

    result = CanonicalLongOnlyEvaluator().evaluate(
        experiment_id="risk-e2",
        snapshot_id="risk-s2",
        factor_or_sleeve_id="value_quality",
        frame=frame,
        signal="signal",
        portfolio_policy=_policy(),
        exposure_frame=exposure,
        returns_history=history,
    )

    assert result.status == "ok"
    assert result.result["observations"] > 0
    assert result.result["target_weight_mode"] == "equal_weight_fallback"
    assert result.result["promotion_eligible"] is False
    assert any(
        "missing_pit_exposure_columns:industry" in blocker
        for blocker in result.result["promotion_blockers"]
    )


def test_future_exposure_is_not_used_for_signal_date() -> None:
    frame = _frame(stocks=60)
    exposure, history = _risk_inputs(frame)
    exposure["available_at"] = pd.Timestamp("2025-01-01T00:00:00Z")

    result = CanonicalLongOnlyEvaluator().evaluate(
        experiment_id="risk-e3",
        snapshot_id="risk-s3",
        factor_or_sleeve_id="value_quality",
        frame=frame,
        signal="signal",
        portfolio_policy=_policy(),
        exposure_frame=exposure,
        returns_history=history,
    )

    assert result.status == "ok"
    assert result.result["promotion_eligible"] is False
    assert result.result["target_weight_mode"] == "equal_weight_fallback"
    assert all(
        audit["exposure_rows"] == 0 for audit in result.result["optimization_audit"]
    )


def test_production_contract_rejects_old_or_non_long_only_policy():
    evaluator = CanonicalLongOnlyEvaluator()
    with pytest.raises(ValueError, match="long_only"):
        evaluator.validate_policy({"mode": "long_short"}, production_contract=True)
    bad = _policy()
    bad["portfolio"]["capital"] = 1_000_000
    with pytest.raises(ValueError, match="capital"):
        evaluator.validate_policy(bad, production_contract=True)


def test_public_portfolio_contract_names_are_executed_not_ignored():
    config = CanonicalLongOnlyEvaluator.validate_policy(
        {
            "mode": "long_only",
            "capital": 50_000_000,
            "rebalance_sessions": 5,
            "target_position_count": 75,
            "maximum_stock_weight": 1.0 / 75.0,
            "maximum_adv_participation": 0.04,
        },
        production_contract=True,
    )
    assert config.holding_days == 5
    assert config.rebalance_every_days == 5
    assert config.position_count == 75
    assert config.target_weight == pytest.approx(1.0 / 75.0)
    assert config.max_adv_participation == 0.04


def test_public_risk_contract_cannot_be_loosened_by_optimizer_override():
    from factor_lab.research_os.evaluator import _optimizer_policy

    public = _policy()
    public["portfolio"].update(
        {
            "industry_active_weight_limit": 0.03,
            "size_active_weight_limit": 0.04,
            "minimum_beta": 0.95,
            "maximum_beta": 1.05,
        }
    )
    config = CanonicalLongOnlyEvaluator.validate_policy(
        public, production_contract=True
    )
    effective = _optimizer_policy(
        config,
        {
            "industry_deviation": 0.10,
            "size_deviation": 0.10,
            "beta_min": 0.8,
            "beta_max": 1.2,
        },
        public,
    )

    assert effective.industry_deviation == 0.03
    assert effective.size_deviation == 0.04
    assert effective.beta_min == 0.95
    assert effective.beta_max == 1.05
