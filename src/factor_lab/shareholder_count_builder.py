from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import bucket_spread, date_zscore, spearman_corr

DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD = 0.0062253011


@dataclass(frozen=True)
class ShareholderCountConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_rows: int = 1000
    min_dates: int = 250
    max_baseline_corr: float = 0.85


def normalize_holdernumber_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["ts_code", "ann_date", "end_date"]:
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace(".0", "", regex=False)
    if "holder_num" in out.columns:
        out["holder_num"] = pd.to_numeric(out["holder_num"], errors="coerce")
    return out.dropna(subset=["ts_code", "ann_date", "end_date", "holder_num"]).copy()


def add_shareholder_change_features(holder: pd.DataFrame) -> pd.DataFrame:
    h = normalize_holdernumber_frame(holder)
    if h.empty:
        return h
    h["ann_dt"] = pd.to_datetime(h["ann_date"], format="%Y%m%d", errors="coerce")
    h["end_dt"] = pd.to_datetime(h["end_date"], format="%Y%m%d", errors="coerce")
    h = h.dropna(subset=["ann_dt", "end_dt"]).sort_values(["ts_code", "end_dt", "ann_dt"]).copy()
    h = h.drop_duplicates(["ts_code", "end_date"], keep="last")
    import numpy as np
    h["holder_num_log"] = pd.Series(np.log(h["holder_num"].where(h["holder_num"] > 0)), index=h.index)
    h["holder_num_qoq_log_change"] = h.groupby("ts_code")["holder_num_log"].diff(1)
    h["holder_num_yoy_log_change"] = h.groupby("ts_code")["holder_num_log"].diff(4)
    return h


def build_daily_asof_shareholder_frame(base_features: pd.DataFrame, holder: pd.DataFrame, *, start_date: str = "2020-06-01", end_date: str = "2023-12-31") -> pd.DataFrame:
    base = base_features.copy()
    base["date_dt"] = pd.to_datetime(base["date"], errors="coerce")
    base["ts_code"] = base.get("ticker", base.get("ts_code")).astype(str)
    base = base[(base["date_dt"] >= pd.to_datetime(start_date)) & (base["date_dt"] <= pd.to_datetime(end_date))].sort_values(["ts_code", "date_dt"]).copy()
    h = add_shareholder_change_features(holder)
    if h.empty or base.empty:
        return pd.DataFrame()
    keep = ["ts_code", "ann_dt", "ann_date", "end_date", "holder_num", "holder_num_qoq_log_change", "holder_num_yoy_log_change"]
    h = h[keep].dropna(subset=["ann_dt", "ts_code"]).sort_values(["ts_code", "ann_dt"])
    parts: list[pd.DataFrame] = []
    for ticker, g in base.groupby("ts_code", sort=False):
        hg = h[h["ts_code"] == ticker].sort_values("ann_dt")
        if hg.empty:
            continue
        merged = pd.merge_asof(g.sort_values("date_dt"), hg, left_on="date_dt", right_on="ann_dt", by="ts_code", direction="backward")
        parts.append(merged)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if not out.empty:
        out["date"] = out["date_dt"].dt.strftime("%Y-%m-%d")
        out["ticker"] = out["ts_code"].astype(str)
    return out


def build_shareholder_crowding_features(asof: pd.DataFrame) -> pd.DataFrame:
    if asof.empty:
        return pd.DataFrame()
    out = asof.copy()
    required = {"date", "ticker", "forward_return_5d", "industry_relative_book_yield", "roe", "holder_num_qoq_log_change"}
    missing = sorted(required - set(out.columns))
    if missing:
        out["_missing_required_columns"] = ",".join(missing)
        return out.iloc[0:0].copy()
    for col in ["forward_return_5d", "industry_relative_book_yield", "roe", "holder_num", "holder_num_qoq_log_change", "holder_num_yoy_log_change", "turnover", "turnover_shock_5_20"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    out["value_quality_baseline"] = out["industry_relative_book_yield"] + out["roe"]
    out["baseline_z"] = date_zscore(out["value_quality_baseline"], out["date"])
    out["low_shareholder_crowding_qoq"] = -date_zscore(out["holder_num_qoq_log_change"], out["date"])
    if "holder_num_yoy_log_change" in out.columns:
        out["low_shareholder_crowding_yoy"] = -date_zscore(out["holder_num_yoy_log_change"], out["date"])
    else:
        out["low_shareholder_crowding_yoy"] = pd.NA
    out["shareholder_crowding_confirmation_qoq"] = out["baseline_z"] + out["low_shareholder_crowding_qoq"]
    out["shareholder_crowding_confirmation_yoy"] = out["baseline_z"] + out["low_shareholder_crowding_yoy"]
    return out.dropna(subset=["date", "ticker", "forward_return_5d", "value_quality_baseline", "low_shareholder_crowding_qoq"])


def build_shareholder_probe_report(features: pd.DataFrame, *, config: ShareholderCountConfig | None = None) -> dict[str, Any]:
    cfg = config or ShareholderCountConfig()
    if features.empty:
        return {"coverage": {"rows": 0}, "decision": {"decision": "shareholder_count_failed", "reasons": ["empty_feature_overlap"]}}
    coverage = {
        "rows": int(len(features)),
        "dates": int(features["date"].nunique()),
        "tickers": int(features["ticker"].nunique()),
        "holder_num_nonnull_rate": round(float(features["holder_num"].notna().mean()), 4) if "holder_num" in features.columns else None,
        "qoq_nonnull_rate": round(float(features["holder_num_qoq_log_change"].notna().mean()), 4) if "holder_num_qoq_log_change" in features.columns else None,
        "yoy_nonnull_rate": round(float(features["holder_num_yoy_log_change"].notna().mean()), 4) if "holder_num_yoy_log_change" in features.columns else None,
        "min_ann_date": str(features["ann_date"].dropna().min()) if "ann_date" in features.columns and features["ann_date"].notna().any() else None,
        "max_ann_date": str(features["ann_date"].dropna().max()) if "ann_date" in features.columns and features["ann_date"].notna().any() else None,
    }
    diagnostics = {
        "baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "low_shareholder_crowding_qoq": bucket_spread(features, "low_shareholder_crowding_qoq", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "shareholder_confirmation_qoq": bucket_spread(features, "shareholder_crowding_confirmation_qoq", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "low_shareholder_crowding_yoy": bucket_spread(features.dropna(subset=["low_shareholder_crowding_yoy"]), "low_shareholder_crowding_yoy", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
        "shareholder_confirmation_yoy": bucket_spread(features.dropna(subset=["shareholder_crowding_confirmation_yoy"]), "shareholder_crowding_confirmation_yoy", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
    }
    correlations = {
        "qoq_vs_baseline": spearman_corr(features["low_shareholder_crowding_qoq"], features["value_quality_baseline"]),
        "qoq_vs_turnover": spearman_corr(features["low_shareholder_crowding_qoq"], features["turnover"]) if "turnover" in features.columns else None,
        "confirmation_qoq_vs_baseline": spearman_corr(features["shareholder_crowding_confirmation_qoq"], features["value_quality_baseline"]),
        "yoy_vs_baseline": spearman_corr(features["low_shareholder_crowding_yoy"], features["value_quality_baseline"]),
    }
    base_spread = diagnostics["baseline"].get("spread_mean")
    candidates = [
        ("qoq", diagnostics["shareholder_confirmation_qoq"].get("spread_mean")),
        ("yoy", diagnostics["shareholder_confirmation_yoy"].get("spread_mean")),
    ]
    best_name, best_spread = sorted(candidates, key=lambda x: -999 if x[1] is None else float(x[1]), reverse=True)[0]
    reasons: list[str] = []
    if coverage["rows"] < cfg.min_rows:
        reasons.append("rows_too_low")
    if coverage["dates"] < cfg.min_dates:
        reasons.append("dates_too_low")
    if best_spread is None or base_spread is None or best_spread <= base_spread:
        reasons.append("confirmation_not_incremental_vs_local_baseline")
    if best_spread is None or best_spread <= cfg.benchmark_spread:
        reasons.append("confirmation_not_above_value_quality_benchmark")
    corr = correlations.get(f"{best_name}_vs_baseline")
    if corr is not None and abs(float(corr)) >= cfg.max_baseline_corr:
        reasons.append("shareholder_signal_too_correlated_with_baseline")
    if reasons:
        decision = "stop_shareholder_count_not_incremental"
        if "rows_too_low" in reasons or "dates_too_low" in reasons:
            decision = "need_shareholder_count_coverage_extension"
    else:
        decision = "proceed_shareholder_crowding_controlled_probe_plan"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "correlations": correlations,
        "best_confirmation": {"variant": best_name, "spread_mean": best_spread},
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "decision": {"decision": decision, "reasons": reasons or ["shareholder_count_passed_readonly_checks"]},
    }
