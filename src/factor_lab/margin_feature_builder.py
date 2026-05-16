from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from factor_lab.margin_source_mvp import add_margin_features, load_feature_cache, merge_margin_with_features, normalize_margin_frame

DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD = 0.0062253011


@dataclass(frozen=True)
class MarginFeatureBuildConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_overlap_rows: int = 100
    min_overlap_dates: int = 3
    max_baseline_corr: float = 0.85


def date_zscore(values: pd.Series, dates: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    out = pd.Series(index=numeric.index, dtype="float64")
    for _, idx in pd.Series(dates, index=numeric.index).groupby(dates, sort=False).groups.items():
        s = numeric.loc[idx]
        std = s.std(ddof=0)
        if pd.isna(std) or std == 0:
            out.loc[idx] = pd.NA
        else:
            out.loc[idx] = (s - s.mean()) / std
    return out


def build_margin_low_crowding_features(merged: pd.DataFrame) -> pd.DataFrame:
    """Build read-only margin low-crowding features from margin_detail + feature-cache overlap.

    `total_mv` from Tushare daily_basic is in 10k CNY, while margin balances are CNY;
    add_margin_features therefore uses total_mv * 10000 as denominator.
    """
    if merged.empty:
        return pd.DataFrame()
    out = add_margin_features(merged).copy()
    required = {"date", "ts_code", "forward_return_5d", "industry_relative_book_yield", "roe", "margin_balance_to_mv"}
    missing = sorted(required - set(out.columns))
    if missing:
        out["_missing_required_columns"] = ",".join(missing)
        return out.iloc[0:0].copy()
    out["ticker"] = out.get("ticker", out["ts_code"]).astype(str)
    for col in ["forward_return_5d", "industry_relative_book_yield", "roe", "margin_balance_to_mv", "turnover", "turnover_shock_5_20", "volatility_20", "volatility_60"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["value_quality_baseline"] = out["industry_relative_book_yield"] + out["roe"]
    out["baseline_z"] = date_zscore(out["value_quality_baseline"], out["date"])
    out["margin_balance_to_mv_z"] = date_zscore(out["margin_balance_to_mv"], out["date"])
    out["low_margin_crowding"] = -out["margin_balance_to_mv_z"]
    out["margin_low_crowding_confirmation"] = out["baseline_z"] + out["low_margin_crowding"]
    return out.dropna(subset=["date", "ticker", "forward_return_5d", "value_quality_baseline", "low_margin_crowding", "margin_low_crowding_confirmation"])


def assign_quantile_buckets(frame: pd.DataFrame, score_col: str, *, quantiles: int = 5) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for _, g in frame.groupby("date", sort=True):
        valid = g.dropna(subset=[score_col, "forward_return_5d"]).copy()
        if len(valid) < quantiles or valid[score_col].nunique(dropna=True) < quantiles:
            continue
        valid["bucket"] = pd.qcut(valid[score_col].rank(method="first"), quantiles, labels=False, duplicates="drop")
        parts.append(valid)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=list(frame.columns) + ["bucket"])


def bucket_spread(frame: pd.DataFrame, score_col: str, *, quantiles: int = 5, long_quantile: int = 3, short_quantile: int = 0) -> dict[str, Any]:
    b = assign_quantile_buckets(frame, score_col, quantiles=quantiles)
    if b.empty:
        return {"available": False, "reason": "no_quantile_dates", "score_col": score_col}
    spreads: list[float] = []
    bucket_means: dict[int, list[float]] = {i: [] for i in range(quantiles)}
    for _, g in b.groupby("date"):
        buckets = set(pd.to_numeric(g["bucket"], errors="coerce").dropna().astype(int))
        for q in range(quantiles):
            if q in buckets:
                bucket_means[q].append(float(g.loc[g["bucket"] == q, "forward_return_5d"].mean()))
        if long_quantile in buckets and short_quantile in buckets:
            spreads.append(float(g.loc[g["bucket"] == long_quantile, "forward_return_5d"].mean() - g.loc[g["bucket"] == short_quantile, "forward_return_5d"].mean()))
    return {
        "available": bool(spreads),
        "score_col": score_col,
        "quantiles": quantiles,
        "long_quantile": long_quantile,
        "short_quantile": short_quantile,
        "observations": int(len(spreads)),
        "spread_mean": None if not spreads else round(float(pd.Series(spreads).mean()), 10),
        "spread_positive_rate": None if not spreads else round(float((pd.Series(spreads) > 0).mean()), 4),
        "bucket_forward_return_means": {str(k): (None if not v else round(float(pd.Series(v).mean()), 10)) for k, v in bucket_means.items()},
    }


def spearman_corr(a: pd.Series, b: pd.Series) -> float | None:
    aa = pd.to_numeric(a, errors="coerce")
    bb = pd.to_numeric(b, errors="coerce")
    valid = aa.notna() & bb.notna()
    if int(valid.sum()) < 10:
        return None
    v = aa[valid].rank(method="average").corr(bb[valid].rank(method="average"))
    return None if pd.isna(v) else round(float(v), 6)


def build_readonly_margin_probe_report(features: pd.DataFrame, *, config: MarginFeatureBuildConfig | None = None) -> dict[str, Any]:
    cfg = config or MarginFeatureBuildConfig()
    if features.empty:
        return {"decision": {"decision": "margin_low_crowding_readonly_failed", "reasons": ["empty_feature_overlap"]}, "coverage": {"rows": 0}}
    coverage = {
        "rows": int(len(features)),
        "dates": int(features["date"].nunique()) if "date" in features.columns else 0,
        "tickers": int(features["ticker"].nunique()) if "ticker" in features.columns else 0,
        "margin_balance_to_mv_nonnull_rate": round(float(features["margin_balance_to_mv"].notna().mean()), 4) if "margin_balance_to_mv" in features.columns and len(features) else None,
        "low_margin_crowding_nonnull_rate": round(float(features["low_margin_crowding"].notna().mean()), 4) if "low_margin_crowding" in features.columns and len(features) else None,
    }
    diagnostics = {
        "baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "low_margin_crowding": bucket_spread(features, "low_margin_crowding", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "confirmation": bucket_spread(features, "margin_low_crowding_confirmation", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
    }
    corr = {
        "low_margin_vs_baseline": spearman_corr(features["low_margin_crowding"], features["value_quality_baseline"]),
        "confirmation_vs_baseline": spearman_corr(features["margin_low_crowding_confirmation"], features["value_quality_baseline"]),
        "low_margin_vs_turnover": spearman_corr(features["low_margin_crowding"], features["turnover"]) if "turnover" in features.columns else None,
        "low_margin_vs_turnover_shock_5_20": spearman_corr(features["low_margin_crowding"], features["turnover_shock_5_20"]) if "turnover_shock_5_20" in features.columns else None,
    }
    reasons: list[str] = []
    if coverage["rows"] < cfg.min_overlap_rows:
        reasons.append("overlap_rows_too_low")
    if coverage["dates"] < cfg.min_overlap_dates:
        reasons.append("overlap_dates_too_low")
    low_baseline_corr = corr["low_margin_vs_baseline"]
    if low_baseline_corr is not None and abs(low_baseline_corr) >= cfg.max_baseline_corr:
        reasons.append("low_margin_too_correlated_with_baseline")
    low_spread = diagnostics["low_margin_crowding"].get("spread_mean")
    confirmation_spread = diagnostics["confirmation"].get("spread_mean")
    baseline_spread = diagnostics["baseline"].get("spread_mean")
    benchmark = cfg.benchmark_spread
    if low_spread is None or low_spread <= 0:
        reasons.append("low_margin_no_positive_bucket_signal")
    if confirmation_spread is None or (baseline_spread is not None and confirmation_spread <= baseline_spread):
        reasons.append("confirmation_not_incremental_vs_local_baseline")
    if confirmation_spread is None or confirmation_spread <= benchmark:
        reasons.append("confirmation_not_above_value_quality_benchmark")

    if "overlap_rows_too_low" in reasons or "overlap_dates_too_low" in reasons:
        decision = "need_margin_feature_store_extension"
    elif "low_margin_too_correlated_with_baseline" in reasons:
        decision = "stop_margin_low_crowding_non_incremental"
    elif "low_margin_no_positive_bucket_signal" in reasons or "confirmation_not_incremental_vs_local_baseline" in reasons:
        decision = "margin_low_crowding_readonly_failed"
    elif "confirmation_not_above_value_quality_benchmark" in reasons:
        decision = "need_margin_feature_store_extension"
    else:
        decision = "proceed_controlled_margin_low_crowding_probe"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "correlations": corr,
        "benchmark": {"value_quality_no_distress_bucket_spread": benchmark},
        "decision": {"decision": decision, "reasons": reasons or ["readonly_signal_passed_preliminary_checks"]},
    }


def build_margin_feature_sample(margin: pd.DataFrame, feature_cache_path: str | Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    features = load_feature_cache(feature_cache_path)
    normalized_margin = normalize_margin_frame(margin)
    merged = merge_margin_with_features(normalized_margin, features)
    sample = build_margin_low_crowding_features(merged)
    report = build_readonly_margin_probe_report(sample)
    report["feature_cache"] = str(feature_cache_path)
    report["raw_margin_rows"] = int(len(margin))
    report["merged_overlap_rows_before_feature_dropna"] = int(len(merged))
    return sample, report
