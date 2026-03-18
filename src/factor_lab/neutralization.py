from __future__ import annotations

import numpy as np
import pandas as pd


def neutralize_by_date(
    frame: pd.DataFrame,
    factor_col: str,
    size_col: str = "total_mv",
    industry_col: str = "industry",
) -> pd.Series:
    parts = []
    for _, group in frame.groupby("date", sort=True):
        subset = group[[factor_col, size_col, industry_col]].copy()
        subset = subset.replace([np.inf, -np.inf], np.nan).dropna()
        if subset.empty:
            parts.append(pd.Series(index=group.index, dtype=float))
            continue

        y = subset[factor_col].astype(float)
        log_size = np.log(subset[size_col].astype(float))
        industry_dummies = pd.get_dummies(subset[industry_col].astype(str), prefix="ind", drop_first=True)

        X = pd.concat(
            [pd.Series(1.0, index=subset.index, name="intercept"), log_size.rename("log_size"), industry_dummies],
            axis=1,
        ).astype(float)

        beta, *_ = np.linalg.lstsq(X.values, y.values, rcond=None)
        fitted = X.values @ beta
        resid = y.values - fitted
        parts.append(pd.Series(resid, index=subset.index))

    result = pd.concat(parts).sort_index()
    return result.reindex(frame.index)
