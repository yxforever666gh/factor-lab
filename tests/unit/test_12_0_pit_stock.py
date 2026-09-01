from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research.pit_stock import (
    PITStockContractError,
    PITStockStrategyConfig,
    annualized_metrics,
    group_contributions,
    official_quarter_end_sessions,
    select_quarterly_targets,
)


def _snapshot(*, market_long: float = 0.10, market_short: float = 0.05) -> pd.DataFrame:
    rows = []
    for index in range(1000):
        rows.append(
            {
                "ticker": f"{index:06d}.SZ",
                "signal_date": "2022-12-30",
                "universe_member": True,
                "mom12": market_long + (index - 500) / 10_000,
                "mom6": market_short + (index - 500) / 20_000,
                "vol63": 0.10 + index / 100_000,
                "adv20": 1_000_000_000 - index,
                "industry": f"I{index % 10}",
                "size_bucket": ("small", "mid", "large")[index % 3],
            }
        )
    return pd.DataFrame(rows)


def test_official_quarter_end_requires_next_session_in_new_quarter() -> None:
    values = ["2022-09-29", "2022-09-30", "2022-10-10", "2022-12-30", "2023-01-03"]
    assert official_quarter_end_sessions(values) == (
        pd.Timestamp("2022-09-30"),
        pd.Timestamp("2022-12-30"),
    )


def test_final_quarter_end_can_be_proven_by_calendar_without_next_market_day() -> None:
    values = ["2026-09-28", "2026-09-29", "2026-09-30"]
    assert official_quarter_end_sessions(
        values, calendar_through_date="2026-09-30"
    ) == (pd.Timestamp("2026-09-30"),)
    assert official_quarter_end_sessions(values) == ()


def test_dual_market_gate_selects_exact_top80_and_residual_is_zero() -> None:
    targets, decision = select_quarterly_targets(_snapshot())
    assert decision.market_gate_open is True
    assert decision.universe_count == 1000
    assert decision.selected_count == 80
    assert decision.target_weight_sum == pytest.approx(1.0)
    assert decision.cash_weight == pytest.approx(0.0)
    assert len(targets) == 80
    assert targets["target_weight"].eq(1 / 80).all()
    assert targets["selection_rank"].between(1, 80).all()


def test_market_gate_falls_back_to_cash_without_substitute() -> None:
    frame = _snapshot(market_long=-0.20, market_short=-0.10)
    targets, decision = select_quarterly_targets(frame)
    assert targets.empty
    assert decision.market_gate_open is False
    assert decision.selected_count == 0
    assert decision.cash_weight == pytest.approx(1.0)


def test_future_column_is_rejected_even_if_unused() -> None:
    frame = _snapshot()
    frame["future_return"] = 0.0
    with pytest.raises(PITStockContractError, match="forbidden future"):
        select_quarterly_targets(frame)


def test_universe_must_be_exact_and_finite() -> None:
    with pytest.raises(PITStockContractError, match="exactly 1000"):
        select_quarterly_targets(_snapshot().iloc[:-1])
    frame = _snapshot()
    frame.loc[0, "mom12"] = float("nan")
    with pytest.raises(PITStockContractError, match="finite"):
        select_quarterly_targets(frame)


def test_ticker_breaks_exact_score_ties() -> None:
    frame = _snapshot()
    frame["mom12"] = 0.1
    frame["mom6"] = 0.1
    frame["vol63"] = 0.2
    targets, _ = select_quarterly_targets(frame)
    assert targets["ticker"].tolist() == [f"{index:06d}.SZ" for index in range(80)]


def test_group_contributions_reconcile_total() -> None:
    realized = pd.DataFrame(
        {
            "signal_date": ["2022-12-30"] * 2,
            "ticker": ["000001.SZ", "600000.SH"],
            "target_weight": [0.4, 0.6],
            "net_stock_return": [0.1, -0.05],
            "industry": ["bank", "bank"],
            "size_bucket": ["large", "large"],
            "market_state": ["up", "up"],
        }
    )
    grouped = group_contributions(realized)
    expected = 0.4 * 0.1 + 0.6 * -0.05
    for _, rows in grouped.groupby("group_type"):
        assert rows["contribution"].sum() == pytest.approx(expected)


def test_non_default_config_is_validated() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        PITStockStrategyConfig(universe_size=50, position_count=80)


def test_drawdown_includes_initial_nav_boundary() -> None:
    result = annualized_metrics([-0.20, 0.10])
    assert result["max_drawdown"] == pytest.approx(-0.20)
