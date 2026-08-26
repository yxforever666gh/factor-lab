from __future__ import annotations

import pytest

from factor_lab.research.contracts import FactorSpec, ValidationSpec


def test_expression_factor_infers_required_fields() -> None:
    factor = FactorSpec(
        name="value_quality",
        family="value",
        expression="rank(book_yield) + rank(roe)",
    )

    assert factor.required_fields == ("book_yield", "roe")
    assert factor.direction_policy == "train_ic"

    factor_with_declared_subset = FactorSpec(
        name="value_quality_declared",
        family="value",
        expression="rank(book_yield) + rank(roe)",
        required_fields=("book_yield",),
    )
    assert factor_with_declared_subset.required_fields == ("book_yield", "roe")


@pytest.mark.parametrize(
    "expression",
    [
        "book_yield + forward_return_5d_open",
        "rank(label_alpha)",
        "future_price / close",
        "target",
    ],
)
def test_factor_rejects_future_and_label_fields(expression: str) -> None:
    with pytest.raises(ValueError, match="forbidden future/label"):
        FactorSpec(name="leak", family="bad", expression=expression)


def test_builtin_requires_declared_fields() -> None:
    with pytest.raises(ValueError, match="required_fields"):
        FactorSpec(name="momentum", family="trend", kind="builtin", builtin="momentum_12_1")


def test_validation_windows_are_frozen_and_ordered() -> None:
    policy = ValidationSpec()

    assert policy.train_end == "2022-12-31"
    assert policy.validation_start == "2023-01-01"
    assert policy.audit_start == "2025-01-01"
    assert policy.holding_days == 5

    with pytest.raises(ValueError, match="ordered"):
        ValidationSpec(validation_start="2022-01-01")
