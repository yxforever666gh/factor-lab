import numpy as np
import pandas as pd

from factor_lab.research_os.risk_optimizer import optimize_stock_weights


def _inputs(count=60):
    tickers = pd.Index([f"S{i:03d}" for i in range(count)])
    rng = np.random.default_rng(7)
    scores = pd.Series(np.linspace(1, 0, count), index=tickers)
    returns = pd.DataFrame(rng.normal(0, 0.01, size=(260, count)), columns=tickers)
    metadata = pd.DataFrame(
        {
            "industry": ["a" if i % 2 == 0 else "b" for i in range(count)],
            "size_bucket": ["large" if i % 3 == 0 else "small" for i in range(count)],
            "beta": np.ones(count),
            "adv_20": np.full(count, 1e9),
            "industry_is_pit": np.ones(count, dtype=bool),
        },
        index=tickers,
    )
    benchmark = pd.Series(1 / count, index=tickers)
    return scores, returns, metadata, benchmark


def test_optimizer_is_long_only_and_audits_constraints():
    result = optimize_stock_weights(*_inputs())
    assert result.status == "ok"
    assert result.promotion_eligible is True
    assert all(weight >= 0 for weight in result.weights.values())
    assert max(result.weights.values()) <= 0.02 + 1e-8
    assert abs(sum(result.weights.values()) + result.cash_weight - 1) < 1e-8
    assert result.audit["capacity_violation_count"] == 0
    assert result.audit["covariance_method"] == "ledoit_wolf"
    assert result.audit["constraints"] == {
        "industry_deviation": 0.05,
        "size_deviation": 0.05,
        "beta_min": 0.9,
        "beta_max": 1.1,
        "max_position_weight": 0.02,
        "max_adv_participation": 0.05,
        "capital": 50_000_000.0,
        "minimum_return_observations": 60,
    }


def test_non_pit_industry_blocks_promotion():
    scores, returns, metadata, benchmark = _inputs()
    metadata["industry_is_pit"] = False
    result = optimize_stock_weights(scores, returns, metadata, benchmark)
    assert result.status == "blocked_non_pit_industry"
    assert result.promotion_eligible is False
    assert result.cash_weight == 1.0
