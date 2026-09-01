from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research.pit_stock import PITStockContractError
from factor_lab.research.pit_stock_account import (
    PITStockScreeningAccountConfig,
    SuspensionProjection,
)


class _Store:
    @staticmethod
    def canonical_ticker(ticker: str, date: pd.Timestamp) -> str:
        return ticker


def test_suspension_projection_carries_until_resume_or_observed_bar() -> None:
    sessions = tuple(pd.to_datetime(["2022-01-04", "2022-01-05", "2022-01-06"]))
    events = pd.DataFrame(
        {
            "ticker": ["000001.SZ", "000001.SZ"],
            "date": [sessions[0], sessions[2]],
            "suspend_type": ["S", "R"],
            "suspend_timing": [None, None],
        }
    )
    projection = SuspensionProjection(events, store=_Store(), official_sessions=sessions)
    assert projection.advance(sessions[0], observed_tickers=set()) == {"000001.SZ"}
    assert projection.advance(sessions[1], observed_tickers=set()) == {"000001.SZ"}
    assert projection.advance(sessions[2], observed_tickers={"000001.SZ"}) == set()


def test_observed_bar_clears_stale_carried_suspension() -> None:
    sessions = tuple(pd.to_datetime(["2022-01-04", "2022-01-05", "2022-01-06"]))
    events = pd.DataFrame(
        {
            "ticker": ["000001.SZ"],
            "date": [sessions[0]],
            "suspend_type": ["S"],
            "suspend_timing": [None],
        }
    )
    projection = SuspensionProjection(events, store=_Store(), official_sessions=sessions)
    projection.advance(sessions[0], observed_tickers=set())
    assert projection.advance(sessions[1], observed_tickers={"000001.SZ"}) == set()
    assert projection.advance(sessions[2], observed_tickers=set()) == set()


def test_suspension_projection_rejects_calendar_gap() -> None:
    sessions = tuple(pd.to_datetime(["2022-01-04", "2022-01-05", "2022-01-06"]))
    events = pd.DataFrame(columns=["ticker", "date", "suspend_type", "suspend_timing"])
    projection = SuspensionProjection(events, store=_Store(), official_sessions=sessions)
    projection.advance(sessions[0], observed_tickers=set())
    with pytest.raises(PITStockContractError, match="calendar gap"):
        projection.advance(sessions[2], observed_tickers=set())


def test_screening_account_explicitly_uses_continuous_units() -> None:
    config = PITStockScreeningAccountConfig()
    assert config.initial_capital_rmb == 10_000_000
    assert config.max_adv_participation == 0.05
    with pytest.raises(ValueError):
        PITStockScreeningAccountConfig(initial_capital_rmb=0)
