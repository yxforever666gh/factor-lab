import numpy as np
import pandas as pd
import pytest

from factor_lab.research_os.lifecycle import SleeveState
from factor_lab.research_os.sleeves import (
    SleeveDescriptor,
    apply_health_fallback,
    blend_adaptive_challenger,
    build_cluster_balanced_champion,
    build_market_state_snapshot,
    fit_state_conditioned_overlay,
)


def test_cluster_champion_and_adaptive_overlay_respect_caps():
    champion = build_cluster_balanced_champion(
        [
            SleeveDescriptor("value", "fundamental"),
            SleeveDescriptor("quality", "fundamental"),
            SleeveDescriptor("low_risk", "defensive"),
            SleeveDescriptor("trend", "trend"),
        ]
    )
    assert champion.total_weight == pytest.approx(1.0)
    assert max(champion.sleeve_weights.values()) <= 0.35
    challenger = blend_adaptive_challenger(
        champion.sleeve_weights,
        {"trend": 1.0, "low_risk": 0.1},
        previous_weights=champion.sleeve_weights,
    )
    assert challenger.total_weight == pytest.approx(1.0)
    assert max(challenger.sleeve_weights.values()) <= 0.35
    for sleeve, value in challenger.sleeve_weights.items():
        assert abs(value - champion.sleeve_weights.get(sleeve, 0.0)) <= 0.05 + 1e-12


def test_health_fallback_moves_risk_to_benchmark_or_cash():
    allocation = apply_health_fallback(
        {"value": 0.5, "trend": 0.5},
        {"value": SleeveState.REDUCED, "trend": SleeveState.DORMANT},
    )
    assert allocation.sleeve_weights == {"value": 0.25}
    assert allocation.benchmark_weight == pytest.approx(0.75)
    no_health = apply_health_fallback(
        {"value": 0.5}, {"value": SleeveState.DORMANT}
    )
    assert no_health.benchmark_weight == 0.5
    assert no_health.cash_weight == 0.5
    frozen = apply_health_fallback({}, {}, data_quality_ok=False)
    assert frozen.cash_weight == 1.0


def _market_frame(days=140, tickers=20):
    dates = pd.bdate_range("2025-01-01", periods=days)
    rows = []
    for ticker_idx in range(tickers):
        for day_idx, day in enumerate(dates):
            rows.append(
                {
                    "date": day,
                    "ticker": f"S{ticker_idx:03d}",
                    "close_adj": 10 + ticker_idx * 0.1 + day_idx * (0.01 + ticker_idx * 0.0001),
                    "amount": 1e8 + ticker_idx * 1e6 + day_idx * 1e5,
                }
            )
    return pd.DataFrame(rows)


def test_market_state_never_reads_rows_after_as_of():
    frame = _market_frame()
    as_of = frame["date"].sort_values().unique()[-2]
    baseline = build_market_state_snapshot(frame, as_of=as_of)
    future = frame.copy()
    mask = future["date"] > pd.Timestamp(as_of)
    future.loc[mask, "close_adj"] = 1_000_000
    future.loc[mask, "amount"] = 1
    changed = build_market_state_snapshot(future, as_of=as_of)
    assert baseline == changed


def test_state_conditioned_overlay_uses_latest_state_only_for_prediction():
    index = pd.bdate_range("2022-01-01", periods=100)
    state = pd.DataFrame({"trend": np.linspace(-1, 1, len(index)), "vol": 0.2}, index=index)
    returns = pd.DataFrame(
        {
            "trend_sleeve": np.r_[np.linspace(-0.02, 0.02, len(index) - 1), np.nan],
            "defensive": np.r_[np.linspace(0.01, -0.01, len(index) - 1), np.nan],
        },
        index=index,
    )
    result = fit_state_conditioned_overlay(state, returns, min_observations=60)
    assert result["status"] == "ok"
    assert result["prediction_as_of"].startswith(str(index[-1].date()))
    assert sum(result["weights"].values()) == pytest.approx(1.0)
