from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.research_os.contracts import ValidationProtocol
from factor_lab.research_os.walk_forward import (
    build_nested_walk_forward_plan,
    select_fold_rows,
    stitch_outer_oos_returns,
)


def _sessions() -> pd.DatetimeIndex:
    return pd.bdate_range("2017-01-03", "2026-08-20")


def test_builds_five_promotion_outer_folds_and_diagnostic_window() -> None:
    plan = build_nested_walk_forward_plan(_sessions(), ValidationProtocol())
    assert [fold.outer.fold_id for fold in plan.outer_folds] == [
        "outer-2021",
        "outer-2022",
        "outer-2023",
        "outer-2024",
        "outer-2025",
    ]
    assert [fold.fold_id for fold in plan.diagnostic_folds] == ["diagnostic-2026"]
    assert all(fold.outer.promotion_eligible for fold in plan.outer_folds)
    assert not plan.diagnostic_folds[0].promotion_eligible
    assert [item.fold_id for item in plan.outer_folds[0].inner] == [
        "outer-2021/inner-2019",
        "outer-2021/inner-2020",
    ]


def test_purge_and_embargo_boundaries_are_explicit_and_non_overlapping() -> None:
    plan = build_nested_walk_forward_plan(_sessions(), ValidationProtocol())
    for nested in plan.outer_folds:
        fold = nested.outer
        assert len(fold.purged_positions) == 6
        assert len(fold.embargo_positions) == 5
        assert min(fold.test_positions) - max(fold.train_positions) - 1 >= 6
        assert min(fold.embargo_positions) == max(fold.test_positions) + 1
        fold.assert_no_label_overlap(label_horizon_sessions=6)
        separation = min(fold.test_positions) - max(fold.train_positions) - 1
        with pytest.raises(ValueError, match="label horizon"):
            fold.assert_no_label_overlap(label_horizon_sessions=separation + 1)

    for prior, current in zip(plan.outer_folds, plan.outer_folds[1:]):
        reserved = set(prior.outer.embargo_positions)
        assert reserved.isdisjoint(current.outer.train_positions)
        assert reserved.isdisjoint(current.outer.test_positions)
    final_embargo = set(plan.outer_folds[-1].outer.embargo_positions)
    assert final_embargo.isdisjoint(plan.diagnostic_folds[0].train_positions)
    assert final_embargo.isdisjoint(plan.diagnostic_folds[0].test_positions)


def test_outer_training_expands_without_using_current_test_year() -> None:
    plan = build_nested_walk_forward_plan(_sessions(), ValidationProtocol())
    sizes = [len(item.outer.train_positions) for item in plan.outer_folds]
    assert sizes == sorted(sizes)
    for item in plan.outer_folds:
        assert item.outer.train_end < item.outer.test_start


def test_splitter_rejects_unordered_or_incomplete_sessions() -> None:
    protocol = ValidationProtocol()
    with pytest.raises(ValueError, match="monotonically"):
        build_nested_walk_forward_plan(reversed(_sessions()), protocol)
    truncated = pd.bdate_range("2017-01-03", "2024-12-31")
    with pytest.raises(ValueError, match="2025"):
        build_nested_walk_forward_plan(truncated, protocol)


def test_select_fold_rows_keeps_all_cross_section_rows() -> None:
    sessions = _sessions()
    plan = build_nested_walk_forward_plan(sessions, ValidationProtocol())
    fold = plan.outer_folds[0].outer
    frame = pd.DataFrame(
        [
            {"date": date, "ticker": ticker, "value": 1.0}
            for date in sessions
            for ticker in ("A", "B")
        ]
    )
    train, test = select_fold_rows(frame, fold)
    assert train["date"].nunique() == len(fold.train_positions)
    assert test["date"].nunique() == len(fold.test_positions)
    assert len(test) == 2 * len(fold.test_positions)


def test_stitch_outer_oos_requires_every_fold_and_no_diagnostic_or_overlap() -> None:
    plan = build_nested_walk_forward_plan(_sessions(), ValidationProtocol())
    fold_returns = {
        item.outer.fold_id: pd.Series(
            [0.01, 0.02],
            index=[item.outer.test_start, item.outer.test_start + pd.offsets.BDay(5)],
        )
        for item in plan.outer_folds
    }
    stitched = stitch_outer_oos_returns(plan, fold_returns)
    assert stitched.fold_ids == tuple(f"outer-{year}" for year in range(2021, 2026))
    assert len(stitched.returns) == 10
    assert all(count == 2 for count in stitched.observation_counts.values())
    with pytest.raises(ValueError, match="fold mismatch"):
        stitch_outer_oos_returns(plan, {**fold_returns, "diagnostic-2026": pd.Series([0.1], index=[pd.Timestamp("2026-02-01")])})
