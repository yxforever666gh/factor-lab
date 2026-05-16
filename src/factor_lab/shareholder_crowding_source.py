from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import bucket_spread, date_zscore, spearman_corr

DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD = 0.0062253011


@dataclass(frozen=True)
class ShareholderCrowdingConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_rows: int = 1000
    min_dates: int = 100
    min_tickers: int = 30
    max_baseline_corr: float = 0.85


def normalize_holdernumber_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename = {
        "holder_num": "holder_num",
        "holders": "holder_num",
        "holder_count": "holder_num",
        "ts_code": "ts_code",
        "ann_date": "ann_date",
        "end_date": "end_date",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
    for col in ("ts_code", "ann_date", "end_date"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace("-", "", regex=False)
    if "holder_num" in out.columns:
        out["holder_num"] = pd.to_numeric(out["holder_num"], errors="coerce")
    return out


def holdernumber_preflight(df: pd.DataFrame) -> dict[str, Any]:
    cols = set(df.columns)
    required = {"ts_code", "ann_date", "end_date", "holder_num"}
    missing = sorted(required - cols)
    out = normalize_holdernumber_frame(df) if not df.empty else df.copy()
    return {
        "rows": int(len(df)),
        "columns": sorted(str(c) for c in df.columns),
        "required_fields_present": not missing,
        "missing_required_fields": missing,
        "tickers": int(out["ts_code"].nunique()) if "ts_code" in out.columns else 0,
        "ann_date_nonnull_rate": round(float(out["ann_date"].replace("nan", pd.NA).notna().mean()), 4) if "ann_date" in out.columns and len(out) else None,
        "end_date_nonnull_rate": round(float(out["end_date"].replace("nan", pd.NA).notna().mean()), 4) if "end_date" in out.columns and len(out) else None,
        "holder_num_nonnull_rate": round(float(out["holder_num"].notna().mean()), 4) if "holder_num" in out.columns and len(out) else None,
        "ann_date_min": None if "ann_date" not in out.columns or out.empty else str(out["ann_date"].min()),
        "ann_date_max": None if "ann_date" not in out.columns or out.empty else str(out["ann_date"].max()),
    }


def build_holdernumber_statement_features(holder_df: pd.DataFrame) -> pd.DataFrame:
    out = normalize_holdernumber_frame(holder_df)
    required = {"ts_code", "ann_date", "end_date", "holder_num"}
    if out.empty or not required.issubset(out.columns):
        return pd.DataFrame()
    out = out.dropna(subset=["ts_code", "ann_date", "end_date", "holder_num"]).copy()
    out = out[out["holder_num"] > 0].copy()
    if out.empty:
        return out
    out = out.sort_values(["ts_code", "end_date", "ann_date"]).drop_duplicates(["ts_code", "end_date"], keep="last")
    out = out.sort_values(["ts_code", "end_date"])
    g = out.groupby("ts_code", group_keys=False)
    out["holder_num_change_qoq"] = g["holder_num"].pct_change(1)
    out["holder_num_change_yoy"] = g["holder_num"].pct_change(4)
    # Lower or declining holder count means more concentrated ownership / less retail crowding.
    out["low_shareholder_crowding_raw"] = -out["holder_num_change_qoq"].where(out["holder_num_change_qoq"].notna(), -out["holder_num"].rank(pct=True))
    return out


def asof_merge_holdernumber_features(feature_cache: pd.DataFrame, holder_features: pd.DataFrame) -> pd.DataFrame:
    if feature_cache.empty or holder_features.empty:
        return pd.DataFrame()
    left = feature_cache.copy()
    if "ticker" in left.columns and "ts_code" not in left.columns:
        left["ts_code"] = left["ticker"].astype(str)
    if "date" not in left.columns or "ts_code" not in left.columns:
        return pd.DataFrame()
    left["date_dt"] = pd.to_datetime(left["date"].astype(str), errors="coerce")
    right = holder_features.copy()
    right["ann_date_dt"] = pd.to_datetime(right["ann_date"].astype(str), errors="coerce")
    left = left.dropna(subset=["date_dt", "ts_code"]).sort_values(["date_dt", "ts_code"]).reset_index(drop=True)
    right = right.dropna(subset=["ann_date_dt", "ts_code"]).sort_values(["ann_date_dt", "ts_code"]).reset_index(drop=True)
    parts: list[pd.DataFrame] = []
    keep_cols = ["ts_code", "ann_date_dt", "ann_date", "end_date", "holder_num", "holder_num_change_qoq", "holder_num_change_yoy", "low_shareholder_crowding_raw"]
    for code, lpart in left.groupby("ts_code", sort=False):
        rpart = right[right["ts_code"] == code][keep_cols].sort_values("ann_date_dt")
        if rpart.empty:
            continue
        merged = pd.merge_asof(
            lpart.sort_values("date_dt"),
            rpart.sort_values("ann_date_dt"),
            left_on="date_dt",
            right_on="ann_date_dt",
            direction="backward",
            suffixes=("", "_holder"),
        )
        parts.append(merged)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code_holder" in out.columns:
        out = out.drop(columns=["ts_code_holder"])
    return out


def build_shareholder_crowding_features(asof_frame: pd.DataFrame) -> pd.DataFrame:
    if asof_frame.empty:
        return pd.DataFrame()
    out = asof_frame.copy()
    required = {"date", "ticker", "forward_return_5d", "industry_relative_book_yield", "roe", "low_shareholder_crowding_raw"}
    missing = sorted(required - set(out.columns))
    if missing:
        return pd.DataFrame()
    for col in ["forward_return_5d", "industry_relative_book_yield", "roe", "holder_num_change_qoq", "holder_num_change_yoy", "low_shareholder_crowding_raw", "turnover", "turnover_shock_5_20"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["value_quality_baseline"] = out["industry_relative_book_yield"] + out["roe"]
    out["baseline_z"] = date_zscore(out["value_quality_baseline"], out["date"])
    out["low_shareholder_crowding"] = date_zscore(out["low_shareholder_crowding_raw"], out["date"])
    out["shareholder_crowding_confirmation"] = out["baseline_z"] + out["low_shareholder_crowding"]
    return out.dropna(subset=["date", "ticker", "forward_return_5d", "value_quality_baseline", "low_shareholder_crowding", "shareholder_crowding_confirmation"])


def build_shareholder_crowding_report(features: pd.DataFrame, *, config: ShareholderCrowdingConfig | None = None) -> dict[str, Any]:
    cfg = config or ShareholderCrowdingConfig()
    if features.empty:
        return {"coverage": {"rows": 0, "dates": 0, "tickers": 0}, "decision": {"decision": "stop_shareholder_crowding_no_feature_overlap", "reasons": ["empty_features"]}}
    coverage = {
        "rows": int(len(features)),
        "dates": int(features["date"].nunique()),
        "tickers": int(features["ticker"].nunique()),
        "low_shareholder_crowding_nonnull_rate": round(float(features["low_shareholder_crowding"].notna().mean()), 4),
    }
    diagnostics = {
        "baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "low_shareholder_crowding": bucket_spread(features, "low_shareholder_crowding", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "confirmation": bucket_spread(features, "shareholder_crowding_confirmation", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
    }
    correlations = {
        "low_shareholder_vs_baseline": spearman_corr(features["low_shareholder_crowding"], features["value_quality_baseline"]),
        "confirmation_vs_baseline": spearman_corr(features["shareholder_crowding_confirmation"], features["value_quality_baseline"]),
        "low_shareholder_vs_turnover": spearman_corr(features["low_shareholder_crowding"], features["turnover"]) if "turnover" in features.columns else None,
        "low_shareholder_vs_turnover_shock_5_20": spearman_corr(features["low_shareholder_crowding"], features["turnover_shock_5_20"]) if "turnover_shock_5_20" in features.columns else None,
    }
    reasons: list[str] = []
    if coverage["rows"] < cfg.min_rows:
        reasons.append("coverage_rows_too_low")
    if coverage["dates"] < cfg.min_dates:
        reasons.append("coverage_dates_too_low")
    if coverage["tickers"] < cfg.min_tickers:
        reasons.append("coverage_tickers_too_low")
    corr = correlations["low_shareholder_vs_baseline"]
    if corr is not None and abs(corr) >= cfg.max_baseline_corr:
        reasons.append("too_correlated_with_baseline")
    baseline_spread = diagnostics["baseline"].get("spread_mean")
    confirmation_spread = diagnostics["confirmation"].get("spread_mean")
    source_spread = diagnostics["low_shareholder_crowding"].get("spread_mean")
    if source_spread is None or source_spread <= 0:
        reasons.append("source_signal_not_positive")
    if confirmation_spread is None or (baseline_spread is not None and confirmation_spread <= baseline_spread):
        reasons.append("confirmation_not_incremental_vs_local_baseline")
    if confirmation_spread is None or confirmation_spread <= cfg.benchmark_spread:
        reasons.append("confirmation_not_above_value_quality_benchmark")
    if any(r.startswith("coverage_") for r in reasons):
        decision = "stop_shareholder_crowding_coverage_insufficient"
    elif "too_correlated_with_baseline" in reasons:
        decision = "stop_shareholder_crowding_duplicate_signal"
    elif "source_signal_not_positive" in reasons or "confirmation_not_incremental_vs_local_baseline" in reasons or "confirmation_not_above_value_quality_benchmark" in reasons:
        decision = "stop_shareholder_crowding_not_incremental"
    else:
        decision = "proceed_shareholder_crowding_controlled_probe_plan"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "correlations": correlations,
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "decision": {"decision": decision, "reasons": reasons or ["shareholder_crowding_readonly_signal_passed"]},
    }
