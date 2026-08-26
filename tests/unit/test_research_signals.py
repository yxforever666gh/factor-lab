from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factor_lab.research.contracts import FactorSpec
from factor_lab.research.signals import (
    evaluate_expression,
    evaluate_factor_signal,
    pit_cashflow_quality,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-02"] * 3 + ["2024-01-03"] * 3,
            "book_yield": [3.0, 1.0, 2.0, 4.0, 6.0, 5.0],
            "roe": [1.0, 3.0, 2.0, 3.0, 1.0, 2.0],
            "volatility_20": [1.0, 2.0, 3.0, 3.0, 2.0, 1.0],
        }
    )


def test_arithmetic_negation_and_cross_sectional_rank() -> None:
    frame = _frame()
    signal = evaluate_expression(
        frame,
        "rank(book_yield) + rank(roe) + rank(-volatility_20) / 2",
    )

    assert signal.tolist() == pytest.approx(
        [11 / 6, 5 / 3, 3 / 2, 3 / 2, 5 / 3, 11 / 6]
    )


def test_expression_rejects_unsafe_calls_and_unknown_fields() -> None:
    with pytest.raises(ValueError, match="only rank"):
        evaluate_expression(_frame(), "abs(book_yield)")
    with pytest.raises(ValueError, match="unknown field"):
        evaluate_expression(_frame(), "rank(missing)")


def test_factor_signal_supports_registered_builtin() -> None:
    frame = _frame()
    factor = FactorSpec(
        name="defensive",
        family="low_risk",
        kind="builtin",
        builtin="negative_volatility",
        required_fields=("volatility_20",),
    )

    signal = evaluate_factor_signal(
        frame,
        factor,
        builtins={"negative_volatility": lambda data, params: -data["volatility_20"]},
    )

    assert signal.name == "defensive"
    assert signal.tolist() == [-1.0, -2.0, -3.0, -3.0, -2.0, -1.0]


def test_expression_cannot_access_frame_attributes() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        evaluate_expression(_frame(), "book_yield.__class__")


def _fundamental_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-08-30"] * 8),
            "financial_available_date": pd.to_datetime(["2024-05-01"] * 8),
            "fundamental_roic": [2, 3, 4, 5, 12, 13, 14, 15],
            "fundamental_q_ocf_to_sales": [1, 3, 2, 4, 11, 13, 12, 14],
            "fundamental_debt_to_assets": [80, 70, 60, 50, 40, 30, 20, 10],
            "fundamental_age_days": [120, 120, 120, 600, 120, 120, 120, 120],
            "industry_pit": ["A"] * 4 + ["B"] * 4,
            "total_mv": [1e8, 2e8, 4e8, 8e8, 1e9, 2e9, 4e9, 8e9],
        }
    )


def test_pit_cashflow_quality_uses_real_components_and_stale_filter() -> None:
    frame = _fundamental_frame()
    frame.loc[0, "fundamental_q_ocf_to_sales"] = np.nan
    frame.loc[1, ["fundamental_q_ocf_to_sales", "fundamental_roic"]] = np.nan
    factor = FactorSpec(
        name="quality",
        family="cashflow_quality",
        kind="builtin",
        builtin="pit_cashflow_quality",
        required_fields=(
            "fundamental_roic",
            "fundamental_q_ocf_to_sales",
            "fundamental_debt_to_assets",
            "fundamental_age_days",
            "industry_pit",
            "total_mv",
        ),
        params={
            "minimum_components": 2,
            "maximum_age_days": 550,
            "industry_neutral": False,
            "size_neutral": False,
        },
    )

    signal = evaluate_factor_signal(frame, factor)

    assert signal.iloc[7] > signal.iloc[0]
    assert pd.notna(signal.iloc[0])
    assert pd.isna(signal.iloc[1])
    assert pd.isna(signal.iloc[3])


def test_pit_cashflow_quality_neutralizes_industry_and_log_size() -> None:
    frame = _fundamental_frame().assign(fundamental_age_days=120)
    signal = pit_cashflow_quality(
        frame,
        {
            "minimum_components": 2,
            "maximum_age_days": 550,
            "industry_neutral": True,
            "size_neutral": True,
        },
    )

    assert signal.groupby(frame["industry_pit"]).mean().abs().max() < 0.12
    assert abs(signal.corr(np.log(frame["total_mv"]))) < 1e-10


def test_pit_cashflow_quality_rejects_future_availability() -> None:
    frame = _fundamental_frame()
    frame.loc[0, "financial_available_date"] = pd.Timestamp("2024-09-01")

    with pytest.raises(ValueError, match="availability violation"):
        pit_cashflow_quality(
            frame,
            {"industry_neutral": False, "size_neutral": False},
        )
