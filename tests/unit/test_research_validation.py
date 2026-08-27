from __future__ import annotations

import numpy as np
import pandas as pd

from factor_lab.research.contracts import FactorSpec, ValidationSpec
from factor_lab.research.validation import (
    build_stage_a_selection,
    deterministic_block_bootstrap_mean,
    diagnose_train_similarity,
    evaluate_stage_a,
    select_stage_b,
)


def _research_frame(*, reverse_validation: bool = False) -> pd.DataFrame:
    dates = pd.bdate_range("2017-01-02", "2025-02-28")
    rows: list[dict[str, object]] = []
    for date_index, day in enumerate(dates):
        for ticker_index in range(10):
            signal = float(ticker_index + (date_index % 3) * 0.01)
            label = signal
            if reverse_validation and pd.Timestamp("2023-01-01") <= day <= pd.Timestamp("2024-12-31"):
                label = -signal
            rows.append(
                {
                    "date": day,
                    "ticker": f"{ticker_index:06d}.SZ",
                    "signal": signal,
                    "forward_return_5d_open": label,
                }
            )
    return pd.DataFrame(rows)


def test_stage_a_freezes_training_direction_and_uses_non_overlapping_dates() -> None:
    frame = _research_frame()
    factor = FactorSpec(name="alpha", family="test", expression="signal")
    policy = ValidationSpec(min_cross_section=5, bootstrap_samples=16)

    result = evaluate_stage_a(frame, factor, policy)

    all_dates = pd.DatetimeIndex(sorted(frame["date"].unique()))
    expected_train_dates = [
        day
        for day in all_dates[::5]
        if pd.Timestamp("2017-01-01") <= day
        and day <= pd.Timestamp("2022-12-31")
        and all_dates.get_loc(day) + 6 < len(all_dates)
        and all_dates[all_dates.get_loc(day) + 6] <= pd.Timestamp("2022-12-31")
    ]
    assert result.frozen_direction == 1
    assert result.train.signed_rank_ic_mean == 1.0
    assert result.train.expected_date_count == len(expected_train_dates)
    assert result.validation.rank_ic_mean == 1.0
    assert result.validation.signed_rank_ic_mean == 1.0
    assert result.validation.evaluable_date_ratio == 1.0
    assert result.validation.median_cross_section_coverage == 1.0
    assert result.stage_b_eligible is True


def test_stage_a_uses_one_global_anchor_and_purges_cross_boundary_labels() -> None:
    dates = pd.bdate_range("2020-01-01", periods=50)
    rows: list[dict[str, object]] = []
    for date_index, day in enumerate(dates):
        exit_date = dates[date_index + 6] if date_index + 6 < len(dates) else pd.NaT
        for ticker_index in range(6):
            signal = float(ticker_index)
            label = signal if date_index % 5 == 0 else -signal
            rows.append(
                {
                    "date": day,
                    "ticker": f"{ticker_index:06d}.SZ",
                    "signal": signal,
                    "forward_return_5d_open": label,
                    "label_exit_date": exit_date,
                }
            )
    frame = pd.DataFrame(rows)
    policy = ValidationSpec(
        train_start=dates[0].date().isoformat(),
        train_end=dates[12].date().isoformat(),
        validation_start=dates[13].date().isoformat(),
        validation_end=dates[38].date().isoformat(),
        audit_start=dates[39].date().isoformat(),
        min_cross_section=3,
        bootstrap_samples=16,
    )

    result = evaluate_stage_a(
        frame,
        FactorSpec(name="anchored", family="test", expression="signal"),
        policy,
    )

    assert result.frozen_direction == 1
    assert result.validation.rank_ic_mean == 1.0
    assert result.validation.expected_date_count == 4
    assert result.stage_b_eligible is True


def test_validation_cannot_flip_the_training_direction() -> None:
    frame = _research_frame(reverse_validation=True)
    factor = FactorSpec(name="alpha", family="test", expression="signal")

    result = evaluate_stage_a(
        frame, factor, ValidationSpec(min_cross_section=5, bootstrap_samples=16)
    )

    assert result.frozen_direction == 1
    assert result.validation.rank_ic_mean == -1.0
    assert result.direction_consistent is False
    assert result.stage_b_eligible is True
    assert result.blockers == ()


def test_all_history_direction_policy_uses_every_observed_window() -> None:
    dates = pd.DatetimeIndex(
        [
            *pd.bdate_range("2017-01-03", periods=20),
            *pd.bdate_range("2023-01-03", periods=50),
            *pd.bdate_range("2025-01-03", periods=50),
        ]
    )
    rows = []
    for day in dates:
        direction = 1.0 if day.year == 2017 else -1.0
        for ticker_index in range(10):
            signal = float(ticker_index)
            rows.append(
                {
                    "date": day,
                    "ticker": f"{ticker_index:06d}.SZ",
                    "signal": signal,
                    "forward_return_5d_open": signal * direction,
                }
            )
    frame = pd.DataFrame(rows)
    result = evaluate_stage_a(
        frame,
        FactorSpec(
            name="all_history",
            family="test",
            expression="signal",
            direction_policy="all_history_ic",
        ),
        ValidationSpec(min_cross_section=5, bootstrap_samples=16),
    )

    assert result.frozen_direction == -1
    assert result.selection_basis == "all_observed_history_direction"
    assert result.train.rank_ic_mean == 1.0
    assert result.train.signed_rank_ic_mean == -1.0


def test_stage_b_selection_is_capped_and_deterministic() -> None:
    frame = _research_frame()
    policy = ValidationSpec(
        min_cross_section=5, max_challengers=3, bootstrap_samples=16
    )
    results = [
        evaluate_stage_a(
            frame.assign(signal=frame["signal"] * scale),
            FactorSpec(name=name, family="value", expression="signal"),
            policy,
        )
        for name, scale in [("zeta", 1.0), ("beta", 2.0), ("alpha", 3.0), ("control", 4.0)]
    ]

    selected = select_stage_b(results, policy, excluded_names={"control"})

    assert [row.factor_name for row in selected] == ["alpha", "beta", "zeta"]


def test_validation_coverage_cannot_change_train_shortlist() -> None:
    frame = _research_frame()
    validation_mask = frame["date"].between("2023-01-01", "2024-12-31")
    frame.loc[validation_mask & (frame["ticker"] != "000000.SZ"), "signal"] = np.nan
    result = evaluate_stage_a(
        frame,
        FactorSpec(name="sparse", family="test", expression="signal"),
        ValidationSpec(min_cross_section=5, bootstrap_samples=16),
    )

    assert result.stage_b_eligible is True
    assert result.validation.median_cross_section_coverage < 0.8


def test_low_training_coverage_blocks_stage_b() -> None:
    frame = _research_frame()
    train_mask = frame["date"].between("2017-01-01", "2022-12-31")
    frame.loc[train_mask & (frame["ticker"] != "000000.SZ"), "signal"] = np.nan

    result = evaluate_stage_a(
        frame,
        FactorSpec(name="sparse_train", family="test", expression="signal"),
        ValidationSpec(min_cross_section=5, bootstrap_samples=16),
    )

    assert result.stage_b_eligible is False
    assert "train_cross_section_coverage_below_threshold" in result.blockers


def test_stage_a_excludes_ineligible_and_nonmember_rows() -> None:
    frame = _research_frame()
    frame["eligible"] = True
    frame["universe_member"] = True
    excluded = frame["ticker"] == "000009.SZ"
    frame.loc[excluded, "eligible"] = False
    frame.loc[excluded, "forward_return_5d_open"] = -1_000_000.0
    policy = ValidationSpec(
        min_cross_section=5,
        min_train_positive_year_ratio=0.0,
        bootstrap_samples=16,
    )

    included_result = evaluate_stage_a(
        frame,
        FactorSpec(name="filtered", family="test", expression="signal"),
        policy,
    )
    dropped_result = evaluate_stage_a(
        frame.loc[~excluded].copy(),
        FactorSpec(name="filtered", family="test", expression="signal"),
        policy,
    )

    assert included_result.train.rank_ic_mean == dropped_result.train.rank_ic_mean
    assert included_result.train.top_tail_excess_mean == dropped_result.train.top_tail_excess_mean


def test_top_tail_and_annual_stability_are_train_only_blockers() -> None:
    frame = _research_frame()
    train = frame["date"].between("2017-01-01", "2022-12-31")
    top = frame["ticker"].isin(["000008.SZ", "000009.SZ"])
    # Preserve a broadly positive middle rank relationship while making the
    # executable high-score tail lose money.
    frame.loc[train & top, "forward_return_5d_open"] = -100.0
    policy = ValidationSpec(
        min_cross_section=5,
        top_tail_fraction=0.2,
        top_tail_min_count=2,
        min_train_decile_monotonicity=-1.0,
        min_train_positive_year_ratio=0.0,
        bootstrap_samples=16,
    )

    result = evaluate_stage_a(
        frame,
        FactorSpec(name="bad_tail", family="test", expression="signal"),
        policy,
    )

    assert result.train.top_tail_excess_mean < 0.0
    assert "train_top_tail_excess_not_positive" in result.blockers


def test_deterministic_block_bootstrap_is_key_stable() -> None:
    values = [0.1, 0.2, -0.1, 0.3, 0.4, -0.2] * 4
    arguments = {
        "samples": 128,
        "block_size": 4,
        "confidence": 0.95,
        "seed": 20260827,
        "key": "alpha:train:ic",
    }

    first = deterministic_block_bootstrap_mean(values, **arguments)
    second = deterministic_block_bootstrap_mean(values, **arguments)

    assert first == second
    assert first.lower < first.estimate < first.upper


def test_training_year_stability_requires_predeclared_ratio() -> None:
    frame = _research_frame()
    weak_years = frame["date"].dt.year.isin([2019, 2020, 2021])
    frame.loc[weak_years, "forward_return_5d_open"] *= -1.0

    result = evaluate_stage_a(
        frame,
        FactorSpec(name="unstable", family="test", expression="signal"),
        ValidationSpec(
            min_cross_section=5,
            min_train_positive_year_ratio=0.6,
            bootstrap_samples=16,
        ),
    )

    assert result.train.positive_year_ratio == 0.5
    assert "train_positive_year_ratio_below_threshold" in result.blockers


def test_train_similarity_deduplicates_against_control() -> None:
    frame = _research_frame()
    policy = ValidationSpec(min_cross_section=5, bootstrap_samples=16)
    control = evaluate_stage_a(
        frame,
        FactorSpec(name="control", family="value", expression="signal"),
        policy,
    )
    clone_signal = frame["signal"] * 7.0
    clone = evaluate_stage_a(
        frame,
        FactorSpec(name="clone", family="value", expression="signal"),
        policy,
        signal=clone_signal,
    )
    similarities = diagnose_train_similarity(
        frame,
        {"control": frame["signal"], "clone": clone_signal},
        [control, clone],
        policy,
    )

    selection = build_stage_a_selection(
        [control, clone], similarities, policy, excluded_names={"control"}
    )

    assert similarities[0].homogeneous is True
    assert selection.selected == ()
    assert selection.decisions[0].reason == "train_signal_homogeneous"
    assert selection.decisions[0].correlated_with == "control"
