from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factor_lab.expanded_factor_research import (
    MAX_PREREGISTERED_VARIANTS,
    FactorDefinition,
    FactorComputationResult,
    benjamini_hochberg,
    build_family_trial_ledger,
    compute_preregistered_factors,
    fixed_sample_labels,
    freeze_directions_from_train,
    preregistered_long_only_factors,
    run_expanded_factor_research,
    split_fixed_samples,
)


def _market_fixture() -> pd.DataFrame:
    dates = pd.bdate_range("2021-01-04", periods=300).append(
        pd.bdate_range("2023-01-02", periods=40)
    ).append(pd.bdate_range("2025-01-02", periods=40))
    tickers = ["AAA", "BBB", "CCC", "DDD"]
    rows = []
    for day_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            cross_section = float(ticker_index + 1)
            validation_sign = -1.0 if pd.Timestamp("2023-01-01") <= date <= pd.Timestamp("2024-12-31") else 1.0
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "industry": "I1" if ticker_index < 2 else "I2",
                    "book_yield": cross_section + (0.01 * day_index),
                    "earnings_yield": 0.8 * cross_section + (0.005 * day_index),
                    "roe": 0.02 * cross_section,
                    "total_mv": 100.0 * (ticker_index + 1) + day_index,
                    "close": 10.0 + cross_section + day_index * (0.01 + ticker_index * 0.0005),
                    "return_1d": 0.001 * cross_section + 0.0001 * ((day_index % 7) - 3),
                    "forward_return_5d": validation_sign * cross_section,
                }
            )
    return pd.DataFrame(rows)


def test_registry_is_exactly_six_explicit_long_only_variants() -> None:
    definitions = preregistered_long_only_factors()

    assert len(definitions) == MAX_PREREGISTERED_VARIANTS == 6
    assert [row.name for row in definitions] == [
        "industry_size_adjusted_value",
        "quality_value_composite",
        "momentum_12_1",
        "momentum_6_1",
        "short_term_reversal",
        "low_volatility_defensive",
    ]
    assert all(row.allow_in_long_only is True for row in definitions)
    assert all(row.direction == "train_selected" for row in definitions)
    assert all(row.expression for row in definitions)
    assert all(set(row.to_dict()) == {"name", "family", "direction", "allow_in_long_only", "expression", "required_fields"} for row in definitions)


def test_computation_rejects_more_than_six_or_unregistered_variants() -> None:
    definitions = preregistered_long_only_factors()
    extra = FactorDefinition(
        name="seventh_variant",
        family="other",
        direction="train_selected",
        allow_in_long_only=True,
        expression="book_yield",
        required_fields=("book_yield",),
        calculator=lambda frame: frame["book_yield"],
    )

    with pytest.raises(ValueError, match="at most 6"):
        compute_preregistered_factors(pd.DataFrame(), (*definitions, extra))
    with pytest.raises(ValueError, match="only preregistered"):
        compute_preregistered_factors(pd.DataFrame({"book_yield": [1.0]}), (extra,))


def test_fixed_sample_windows_have_locked_boundaries() -> None:
    dates = pd.Series(
        pd.to_datetime(
            [
                "2016-12-31",
                "2017-01-01",
                "2022-12-31",
                "2023-01-01",
                "2024-12-31",
                "2025-01-01",
                "2027-06-01",
            ]
        )
    )

    assert fixed_sample_labels(dates).tolist() == [
        "out_of_scope",
        "train",
        "train",
        "validation",
        "validation",
        "observed_audit",
        "observed_audit",
    ]

    split = split_fixed_samples(pd.DataFrame({"date": dates, "value": range(len(dates))}))
    assert {name: len(part) for name, part in split.items()} == {"train": 2, "validation": 2, "observed_audit": 2}


def test_validation_period_cannot_change_train_selected_direction() -> None:
    frame = _market_fixture()
    computation = compute_preregistered_factors(frame)

    first = freeze_directions_from_train(frame, computation)
    changed_validation = frame.copy()
    validation = fixed_sample_labels(changed_validation["date"]) == "validation"
    changed_validation.loc[validation, "forward_return_5d"] *= -100.0
    second = freeze_directions_from_train(changed_validation, computation)

    assert first.keys() == second.keys()
    assert {name: row.direction for name, row in first.items()} == {name: row.direction for name, row in second.items()}
    assert {name: row.train_rank_ic for name, row in first.items()} == {name: row.train_rank_ic for name, row in second.items()}
    assert all(row.selected_on == "train" for row in first.values())
    assert all(row.non_overlapping_step == 5 for row in first.values())


def test_missing_optional_fields_only_disable_the_dependent_factor() -> None:
    frame = _market_fixture().drop(columns=["industry"])

    result = compute_preregistered_factors(frame)

    assert result.unavailable == {"industry_size_adjusted_value": ("industry",)}
    assert set(result.series_by_name) == {
        "quality_value_composite",
        "momentum_12_1",
        "momentum_6_1",
        "short_term_reversal",
        "low_volatility_defensive",
    }
    assert all(len(series) == len(frame) for series in result.series_by_name.values())


def test_benjamini_hochberg_is_monotone_and_preserves_missing_trials() -> None:
    q_values = benjamini_hochberg([0.04, 0.01, None, 0.20])

    assert q_values[0] == pytest.approx(0.08)
    assert q_values[1] == pytest.approx(0.04)
    assert q_values[2] is None
    assert q_values[3] == pytest.approx(0.2666666667)


def test_family_trial_ledger_applies_bh_per_family_and_q_point_one_passes() -> None:
    ledger = build_family_trial_ledger(
        [
            {"name": "value_a", "family": "value", "p_value": 0.01},
            {"name": "value_b", "family": "value", "p_value": 0.04},
            {"name": "mom_a", "family": "momentum", "p_value": 0.05},
            {"name": "mom_b", "family": "momentum", "p_value": 0.10},
            {"name": "mom_unavailable", "family": "momentum", "p_value": None},
            {"name": "reversal", "family": "reversal", "p_value": 0.10},
        ]
    )
    by_name = {row["name"]: row for row in ledger}

    assert by_name["value_a"]["q_value"] == pytest.approx(0.02)
    assert by_name["value_b"]["q_value"] == pytest.approx(0.04)
    assert by_name["value_a"]["passes_fdr"] is True
    assert by_name["mom_a"]["q_value"] == pytest.approx(0.15)
    assert by_name["mom_b"]["q_value"] == pytest.approx(0.15)
    assert by_name["mom_a"]["passes_fdr"] is False
    assert by_name["mom_unavailable"]["q_value"] is None
    assert by_name["mom_unavailable"]["family_trial_count"] == 3
    assert by_name["reversal"]["q_value"] == pytest.approx(0.10)
    assert by_name["reversal"]["passes_fdr"] is True


def test_factor_series_are_computable_for_full_fixture() -> None:
    frame = _market_fixture()
    result = compute_preregistered_factors(frame)

    assert result.unavailable == {}
    assert set(result.series_by_name) == {row.name for row in preregistered_long_only_factors()}
    assert np.isfinite(result.series_by_name["momentum_12_1"]).any()
    assert np.isfinite(result.series_by_name["low_volatility_defensive"]).any()


def test_high_level_run_uses_validation_for_fdr_and_audit_is_observational() -> None:
    result = run_expanded_factor_research(_market_fixture())

    assert len(result.definitions) == 6
    assert {row["window"] for row in result.window_metrics} == {"train", "validation", "observed_audit"}
    assert all(row["window"] == "validation" for row in result.family_trial_ledger)
    assert all(row["direction"] == result.frozen_directions[row["name"]].direction for row in result.window_metrics)


def test_direction_and_validation_statistics_default_to_non_overlapping_five_day_dates() -> None:
    dates = pd.bdate_range("2017-01-02", periods=10)
    tickers = ["AAA", "BBB", "CCC", "DDD"]
    rows = []
    signal = []
    for date_index, date in enumerate(dates):
        sign = 1.0 if date_index in {0, 5} else -1.0
        for ticker_index, ticker in enumerate(tickers):
            value = float(ticker_index)
            rows.append({"date": date, "ticker": ticker, "forward_return_5d": sign * value})
            signal.append(value)
    frame = pd.DataFrame(rows)
    definition = preregistered_long_only_factors()[1]
    computation = FactorComputationResult(
        series_by_name={definition.name: pd.Series(signal, index=frame.index)},
        unavailable={},
    )

    default = freeze_directions_from_train(frame, computation, (definition,))
    diagnostic = freeze_directions_from_train(frame, computation, (definition,), non_overlapping_step=1)

    assert default[definition.name].direction == "higher_is_better"
    assert default[definition.name].train_ic_date_count == 2
    assert diagnostic[definition.name].direction == "lower_is_better"
    assert diagnostic[definition.name].train_ic_date_count == 10


def test_non_overlapping_stride_is_based_on_trading_dates_not_only_valid_ic_dates() -> None:
    dates = pd.bdate_range("2017-02-01", periods=6)
    tickers = ["AAA", "BBB", "CCC", "DDD"]
    rows = []
    signal = []
    for date_index, date in enumerate(dates):
        for ticker_index, ticker in enumerate(tickers):
            value = float(ticker_index)
            if date_index == 0:
                forward_return = 0.0  # Selected by the stride, but has no valid cross-sectional IC.
            elif date_index == 5:
                forward_return = value
            else:
                forward_return = -value
            rows.append({"date": date, "ticker": ticker, "forward_return_5d": forward_return})
            signal.append(value)
    frame = pd.DataFrame(rows)
    definition = preregistered_long_only_factors()[1]
    computation = FactorComputationResult(
        series_by_name={definition.name: pd.Series(signal, index=frame.index)},
        unavailable={},
    )

    frozen = freeze_directions_from_train(frame, computation, (definition,))

    assert frozen[definition.name].direction == "higher_is_better"
    assert frozen[definition.name].train_ic_date_count == 1


def test_high_level_non_overlapping_step_can_be_explicitly_set_to_one() -> None:
    frame = _market_fixture()

    default = run_expanded_factor_research(frame)
    diagnostic = run_expanded_factor_research(frame, non_overlapping_step=1)
    default_quality = next(row for row in default.family_trial_ledger if row["name"] == "quality_value_composite")
    diagnostic_quality = next(row for row in diagnostic.family_trial_ledger if row["name"] == "quality_value_composite")

    assert default_quality["non_overlapping_step"] == 5
    assert default_quality["date_count"] == 8
    assert diagnostic_quality["non_overlapping_step"] == 1
    assert diagnostic_quality["date_count"] == 40


@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_non_overlapping_step_must_be_a_positive_integer(value) -> None:
    frame = _market_fixture()

    with pytest.raises(ValueError, match="positive integer"):
        run_expanded_factor_research(frame, non_overlapping_step=value)
