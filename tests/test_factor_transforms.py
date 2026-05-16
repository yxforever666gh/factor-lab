import pandas as pd

from factor_lab.factor_transforms import (
    rank_by_date,
    rank_by_date_industry,
    zscore_by_date,
    zscore_by_date_industry,
)


def _frame():
    return pd.DataFrame(
        {
            "date": ["2023-01-01", "2023-01-01", "2023-01-01", "2023-01-02", "2023-01-02"],
            "ticker": ["a", "b", "c", "a", "b"],
            "industry": ["bank", "bank", "tech", "bank", "bank"],
            "book_yield": [1.0, 3.0, 10.0, 2.0, 4.0],
        }
    )


def test_zscore_by_date_uses_only_same_date_cross_section():
    result = zscore_by_date(_frame(), "book_yield")

    day1 = result[result["date"] == "2023-01-01"]
    day2 = result[result["date"] == "2023-01-02"]
    assert round(float(day1["book_yield_zscore_by_date"].mean()), 8) == 0.0
    assert round(float(day2["book_yield_zscore_by_date"].mean()), 8) == 0.0


def test_industry_transforms_fallback_to_missing_when_group_too_small():
    result = zscore_by_date_industry(_frame(), "book_yield", min_group_size=2)

    tech_row = result[(result["date"] == "2023-01-01") & (result["industry"] == "tech")].iloc[0]
    bank_rows = result[(result["date"] == "2023-01-01") & (result["industry"] == "bank")]

    assert pd.isna(tech_row["book_yield_zscore_by_date_industry"])
    assert set(round(x, 6) for x in bank_rows["book_yield_zscore_by_date_industry"].tolist()) == {-1.0, 1.0}


def test_rank_transforms_are_date_and_industry_scoped():
    by_date = rank_by_date(_frame(), "book_yield")
    by_industry = rank_by_date_industry(_frame(), "book_yield", min_group_size=2)

    assert by_date.loc[by_date["ticker"] == "c", "book_yield_rank_by_date"].iloc[0] == 1.0
    assert by_industry.loc[(by_industry["ticker"] == "b") & (by_industry["date"] == "2023-01-01"), "book_yield_rank_by_date_industry"].iloc[0] == 1.0
