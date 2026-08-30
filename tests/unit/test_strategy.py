from __future__ import annotations

import pandas as pd
import pytest

from factor_lab.strategy import (
    LowChurnStrategyConfig,
    fixed_core_score,
    generate_sleeve_target_schedule,
    ranked_tickers_for_date,
    select_low_churn_targets,
)


def _cross_section(date: str = "2026-08-28") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date] * 5,
            "ticker": ["D", "B", "A", "C", "X"],
            "eligible": [True, True, True, True, False],
            "universe_member": [True] * 5,
            "earnings_yield": [1.0, 3.0, 4.0, 2.0, 100.0],
            "pb": [1.0] * 5,
            "book_yield": [1.0, 3.0, 4.0, 2.0, 100.0],
            "volatility_20": [4.0, 2.0, 1.0, 3.0, 0.01],
        }
    )


def test_fixed_core_score_uses_only_common_investable_cross_section() -> None:
    frame = _cross_section()

    score = fixed_core_score(frame)

    assert score.tolist()[:4] == pytest.approx([0.25, 0.75, 1.0, 0.5])
    assert pd.isna(score.iloc[4])
    assert ranked_tickers_for_date(frame, score, "2026-08-28") == (
        "A",
        "B",
        "C",
        "D",
    )


def test_fixed_core_missing_defensive_component_falls_back_to_control() -> None:
    frame = _cross_section()
    frame.loc[frame["ticker"].eq("B"), "book_yield"] = None

    score = fixed_core_score(frame)

    assert pd.notna(score.loc[frame["ticker"].eq("B")]).all()
    assert pd.isna(score.loc[frame["ticker"].eq("X")]).all()


def test_rank_ties_break_by_ticker_ascending() -> None:
    frame = _cross_section().iloc[:4].copy()
    for column in ("earnings_yield", "pb", "book_yield", "volatility_20"):
        frame[column] = 1.0

    score = fixed_core_score(frame)

    assert score.nunique() == 1
    assert ranked_tickers_for_date(frame, score, "2026-08-28") == (
        "A",
        "B",
        "C",
        "D",
    )


def test_exit25_retains_only_prior_names_inside_rank25_then_fills_top10() -> None:
    ranking = [f"T{value:02d}" for value in range(1, 31)]
    previous = {
        ticker: 0.1
        for ticker in [
            "T08",
            "T09",
            "T10",
            "T11",
            "T12",
            "T13",
            "T14",
            "T15",
            "T25",
            "T26",
        ]
    }

    targets = select_low_churn_targets(ranking, previous)

    assert list(targets) == [
        "T01",
        "T08",
        "T09",
        "T10",
        "T11",
        "T12",
        "T13",
        "T14",
        "T15",
        "T25",
    ]
    assert "T25" in targets
    assert "T26" not in targets
    assert sum(targets.values()) == pytest.approx(1.0)


def test_fixed_core_matches_frozen_5_2_binary64_top10_boundary_vector() -> None:
    # Frozen 5.2 ``small_top10_boundary`` regression vector.  The construction
    # makes control_raw equal ``control`` and rank(-volatility) equal
    # ``inverse_volatility``.  Keeping ``(1.0 - weight)`` in the scorer selects
    # T10 at rank ten; replacing it with a literal 0.3 selects T09.
    tickers = [f"T{value:02d}" for value in range(11)]
    control = [3, 4, 7, 9, 6, 5, 2, 11, 10, 1, 8]
    book = [6, 11, 10, 9, 3, 2, 8, 7, 4, 5, 1]
    inverse_volatility = [10, 4, 8, 5, 3, 6, 7, 1, 11, 9, 2]
    frame = pd.DataFrame(
        {
            "date": ["2026-08-28"] * len(tickers),
            "ticker": tickers,
            "eligible": [True] * len(tickers),
            "universe_member": [True] * len(tickers),
            "earnings_yield": control,
            "pb": [1.0] * len(tickers),
            "book_yield": book,
            "volatility_20": [-value for value in inverse_volatility],
        }
    )

    score = fixed_core_score(frame)
    ranked = ranked_tickers_for_date(frame, score, "2026-08-28")

    assert ranked[9] == "T10"
    assert ranked[10] == "T09"


def _schedule_frame(calendar: pd.DatetimeIndex) -> pd.DataFrame:
    tickers = [f"T{value:02d}" for value in range(1, 31)]
    rows: list[dict[str, object]] = []
    for index, date in enumerate(calendar):
        if index == 3:
            continue
        if index == 10:
            ranking = tickers[10:20] + tickers[20:25] + tickers[:10] + tickers[25:]
        else:
            shift = index % len(tickers)
            ranking = tickers[shift:] + tickers[:shift]
        strength = {ticker: len(tickers) - rank for rank, ticker in enumerate(ranking)}
        for ticker in tickers:
            value = float(strength[ticker])
            rows.append(
                {
                    "date": date,
                    "ticker": ticker,
                    "eligible": True,
                    "universe_member": True,
                    "earnings_yield": value,
                    "pb": 1.0,
                    "book_yield": value,
                    "volatility_20": 31.0 - value,
                }
            )
    return pd.DataFrame(rows)


def test_absolute_calendar_sleeves_keep_separate_state_and_do_not_reindex_skip() -> None:
    calendar = pd.bdate_range("2017-01-03", periods=12)
    frame = _schedule_frame(calendar)

    schedule = generate_sleeve_target_schedule(frame, calendar)

    assert [(row["calendar_index"], row["sleeve"]) for row in schedule] == [
        (index, index % 10) for index in range(12)
    ]
    assert schedule[3]["status"] == "skipped_empty_signal"
    assert schedule[4]["calendar_index"] == 4
    assert schedule[4]["sleeve"] == 4
    assert schedule[10]["previous_tickers"] == schedule[0]["selected_tickers"]
    assert schedule[11]["previous_tickers"] == schedule[1]["selected_tickers"]
    assert schedule[10]["selected_tickers"] == schedule[0]["selected_tickers"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"defensive_weight": 1.1},
        {"position_count": 0},
        {"retention_exit_rank": 9},
        {"sleeve_count": 0},
        {"position_weight": 0.11},
    ],
)
def test_config_rejects_invalid_contract(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        LowChurnStrategyConfig(**kwargs)
