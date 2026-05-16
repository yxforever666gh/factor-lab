from __future__ import annotations

from typing import Any

import pandas as pd


def best_bucket_pair_spread(profile: list[dict[str, Any]]) -> dict[str, Any]:
    if not profile:
        return {"recommendation": "no_profile", "spread_mean": 0.0}
    df = pd.DataFrame(profile)
    means = df.groupby("quantile", sort=True)["mean_forward_return_5d"].mean().astype(float)
    if means.empty or len(means) < 2:
        return {"recommendation": "insufficient_buckets", "spread_mean": 0.0}
    long_q = int(means.idxmax())
    short_q = int(means.idxmin())
    spread = float(means.loc[long_q] - means.loc[short_q])
    return {
        "long_quantile": long_q,
        "short_quantile": short_q,
        "spread_mean": round(spread, 6),
        "long_return_mean": round(float(means.loc[long_q]), 6),
        "short_return_mean": round(float(means.loc[short_q]), 6),
        "recommendation": "long_best_bucket_short_worst_bucket" if spread > 0 else "no_positive_bucket_pair",
    }
