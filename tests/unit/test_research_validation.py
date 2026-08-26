from __future__ import annotations

import numpy as np
import pandas as pd

from factor_lab.research.contracts import FactorSpec, ValidationSpec
from factor_lab.research.validation import evaluate_stage_a, select_stage_b


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
    policy = ValidationSpec(min_cross_section=5)

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

    result = evaluate_stage_a(frame, factor, ValidationSpec(min_cross_section=5))

    assert result.frozen_direction == 1
    assert result.validation.rank_ic_mean == -1.0
    assert result.direction_consistent is False
    assert result.stage_b_eligible is False
    assert "validation_direction_inconsistent" in result.blockers


def test_stage_b_selection_is_capped_and_deterministic() -> None:
    frame = _research_frame()
    policy = ValidationSpec(min_cross_section=5, max_challengers=3)
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


def test_low_validation_coverage_blocks_stage_b() -> None:
    frame = _research_frame()
    validation_mask = frame["date"].between("2023-01-01", "2024-12-31")
    frame.loc[validation_mask & (frame["ticker"] != "000000.SZ"), "signal"] = np.nan
    result = evaluate_stage_a(
        frame,
        FactorSpec(name="sparse", family="test", expression="signal"),
        ValidationSpec(min_cross_section=5),
    )

    assert result.stage_b_eligible is False
    assert "validation_cross_section_coverage_below_threshold" in result.blockers
