from __future__ import annotations

from dataclasses import asdict, dataclass

import pandas as pd


@dataclass
class BucketPairPortfolioResult:
    mode: str
    quantiles: int
    long_quantile: int
    short_quantile: int
    spread_mean: float
    spread_std: float
    observations: int
    pass_gate: bool
    fail_reason: str

    def to_dict(self) -> dict:
        return asdict(self)


def _validate_bucket_config(*, quantiles: int, long_quantile: int, short_quantile: int) -> None:
    if quantiles < 2:
        raise ValueError("quantiles must be >= 2")
    if not (0 <= long_quantile < quantiles and 0 <= short_quantile < quantiles):
        raise ValueError("bucket quantiles out of range")
    if long_quantile == short_quantile:
        raise ValueError("long_quantile and short_quantile must differ")


def evaluate_bucket_pair_portfolio(
    frame: pd.DataFrame,
    *,
    factor_col: str = "factor_value",
    return_col: str = "forward_return_5d",
    date_col: str = "date",
    quantiles: int = 5,
    long_quantile: int = 3,
    short_quantile: int = 0,
    min_spread: float = 0.0,
) -> BucketPairPortfolioResult:
    _validate_bucket_config(quantiles=quantiles, long_quantile=long_quantile, short_quantile=short_quantile)
    required = {date_col, factor_col, return_col}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise ValueError(f"missing columns: {missing}")

    work = frame[[date_col, factor_col, return_col]].copy()
    work[factor_col] = pd.to_numeric(work[factor_col], errors="coerce")
    work[return_col] = pd.to_numeric(work[return_col], errors="coerce")
    work = work.dropna(subset=[date_col, factor_col, return_col])
    spreads: list[float] = []
    for _, group in work.groupby(date_col, sort=True):
        if len(group) < quantiles or group[factor_col].nunique() < quantiles:
            continue
        ranked = group.assign(
            _bucket=pd.qcut(group[factor_col].rank(method="first"), quantiles, labels=False, duplicates="drop")
        )
        if long_quantile not in set(ranked["_bucket"]) or short_quantile not in set(ranked["_bucket"]):
            continue
        long_ret = ranked.loc[ranked["_bucket"] == long_quantile, return_col].mean()
        short_ret = ranked.loc[ranked["_bucket"] == short_quantile, return_col].mean()
        spreads.append(float(long_ret - short_ret))

    if not spreads:
        spread_mean = 0.0
        spread_std = 0.0
    else:
        s = pd.Series(spreads)
        spread_mean = float(s.mean())
        spread_std = float(s.std(ddof=0)) if len(s) > 1 else 0.0
    fail_reasons = []
    if spread_mean < min_spread:
        fail_reasons.append(f"bucket_spread<{min_spread}")
    return BucketPairPortfolioResult(
        mode="bucket_pair",
        quantiles=int(quantiles),
        long_quantile=int(long_quantile),
        short_quantile=int(short_quantile),
        spread_mean=round(spread_mean, 6),
        spread_std=round(spread_std, 6),
        observations=int(len(spreads)),
        pass_gate=not fail_reasons,
        fail_reason="; ".join(fail_reasons),
    )
