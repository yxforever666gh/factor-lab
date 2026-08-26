from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research.contracts import FactorSpec
from factor_lab.research.signals import evaluate_expression, evaluate_factor_signal


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
