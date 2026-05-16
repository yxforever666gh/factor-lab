from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import bucket_spread, date_zscore, spearman_corr
from factor_lab.shareholder_crowding_source import DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD


@dataclass(frozen=True)
class OwnershipConcentrationConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_rows: int = 1000
    min_dates: int = 100
    min_tickers: int = 30
    max_baseline_corr: float = 0.85


def normalize_top10_frame(df: pd.DataFrame, *, endpoint: str | None = None) -> pd.DataFrame:
    out = df.copy()
    if endpoint is not None and "endpoint" not in out.columns:
        out["endpoint"] = endpoint
    for col in ("ts_code", "ann_date", "f_ann_date", "end_date", "holder_name", "holder_type", "endpoint"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace("-", "", regex=False).replace({"nan": pd.NA, "None": pd.NA})
    if "ann_date" not in out.columns and "f_ann_date" in out.columns:
        out["ann_date"] = out["f_ann_date"]
    for col in ("hold_amount", "hold_ratio", "hold_float_ratio", "float_ratio", "hold_change"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def top10_preflight(endpoint_frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    total_rows = 0
    tickers: set[str] = set()
    pit_safe_endpoints = 0
    for endpoint, frame in endpoint_frames.items():
        df = normalize_top10_frame(frame, endpoint=endpoint) if frame is not None else pd.DataFrame()
        rows = int(len(df))
        total_rows += rows
        if "ts_code" in df.columns:
            tickers.update(df["ts_code"].dropna().astype(str).tolist())
        required = {"ts_code", "ann_date", "end_date"}
        missing = sorted(required - set(df.columns))
        if not missing and rows:
            pit_safe_endpoints += 1
        reports[endpoint] = {
            "rows": rows,
            "tickers": int(df["ts_code"].nunique()) if "ts_code" in df.columns and rows else 0,
            "columns": sorted(str(c) for c in df.columns),
            "required_fields_present": not missing,
            "missing_required_fields": missing,
            "ann_date_nonnull_rate": round(float(df["ann_date"].replace("nan", pd.NA).notna().mean()), 4) if "ann_date" in df.columns and rows else None,
            "end_date_nonnull_rate": round(float(df["end_date"].replace("nan", pd.NA).notna().mean()), 4) if "end_date" in df.columns and rows else None,
        }
    return {
        "rows": total_rows,
        "tickers": int(len(tickers)),
        "pit_safe_endpoints": pit_safe_endpoints,
        "endpoint_reports": reports,
    }


def _is_fund_like(df: pd.DataFrame) -> pd.Series:
    text = pd.Series("", index=df.index, dtype="object")
    for col in ("holder_type", "holder_name"):
        if col in df.columns:
            text = text + " " + df[col].fillna("").astype(str)
    pattern = "基金|证券|保险|社保|信托|资管|投资|资产管理|银行|养老金|QFII|RQFII"
    return text.str.contains(pattern, case=False, regex=True, na=False)


def _is_hkscc(df: pd.DataFrame) -> pd.Series:
    text = pd.Series("", index=df.index, dtype="object")
    for col in ("holder_name", "holder_type"):
        if col in df.columns:
            text = text + " " + df[col].fillna("").astype(str)
    return text.str.contains("香港中央结算|香港中央|HKSCC", case=False, regex=True, na=False)


def build_top10_statement_features(endpoint_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for endpoint, frame in endpoint_frames.items():
        df = normalize_top10_frame(frame, endpoint=endpoint)
        required = {"ts_code", "ann_date", "end_date"}
        if df.empty or not required.issubset(df.columns):
            continue
        df = df.dropna(subset=["ts_code", "ann_date", "end_date"]).copy()
        if df.empty:
            continue
        df["is_fund_like"] = _is_fund_like(df)
        df["is_hkscc"] = _is_hkscc(df)
        ratio_col = "hold_float_ratio" if endpoint == "top10_floatholders" and "hold_float_ratio" in df.columns else "hold_ratio"
        if ratio_col not in df.columns:
            continue
        df[ratio_col] = pd.to_numeric(df[ratio_col], errors="coerce")
        df["hold_change"] = pd.to_numeric(df["hold_change"], errors="coerce") if "hold_change" in df.columns else pd.NA
        grouped = []
        for keys, g in df.groupby(["endpoint", "ts_code", "ann_date", "end_date"], dropna=True):
            endpoint_name, ts_code, ann_date, end_date = keys
            ratios = pd.to_numeric(g[ratio_col], errors="coerce")
            fund_mask = g["is_fund_like"].fillna(False)
            hkscc_mask = g["is_hkscc"].fillna(False)
            row = {
                "endpoint": endpoint_name,
                "ts_code": ts_code,
                "ann_date": ann_date,
                "end_date": end_date,
                "top10_holder_count": int(len(g)),
                "top10_ratio_sum": float(ratios.sum(skipna=True)) if ratios.notna().any() else pd.NA,
                "fund_like_ratio_sum": float(ratios[fund_mask].sum(skipna=True)) if ratios.notna().any() else pd.NA,
                "hkscc_ratio": float(ratios[hkscc_mask].sum(skipna=True)) if ratios.notna().any() else pd.NA,
                "top10_hold_change_sum": float(pd.to_numeric(g["hold_change"], errors="coerce").sum(skipna=True)) if "hold_change" in g.columns else pd.NA,
            }
            grouped.append(row)
        if grouped:
            agg = pd.DataFrame(grouped)
            prefix = "top10_float" if endpoint == "top10_floatholders" else "top10_hold"
            agg = agg.rename(columns={
                "top10_ratio_sum": f"{prefix}_ratio_sum",
                "fund_like_ratio_sum": f"{prefix}_fund_like_ratio_sum",
                "hkscc_ratio": f"{prefix}_hkscc_ratio",
                "top10_hold_change_sum": f"{prefix}_hold_change_sum",
            })
            ratio_cols = [c for c in agg.columns if c.endswith("ratio_sum") or c.endswith("hkscc_ratio")]
            agg = agg.sort_values(["ts_code", "end_date", "ann_date"]).drop_duplicates(["endpoint", "ts_code", "end_date"], keep="last")
            agg = agg.sort_values(["endpoint", "ts_code", "end_date"])
            for col in ratio_cols:
                agg[f"{col}_change"] = agg.groupby(["endpoint", "ts_code"])[col].diff(1)
            parts.append(agg)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True, sort=False)
    out = out.sort_values(["ts_code", "ann_date", "endpoint"]).reset_index(drop=True)
    return out


def asof_merge_top10_features(feature_cache: pd.DataFrame, statement_features: pd.DataFrame) -> pd.DataFrame:
    if feature_cache.empty or statement_features.empty:
        return pd.DataFrame()
    left = feature_cache.copy()
    if "ticker" in left.columns and "ts_code" not in left.columns:
        left["ts_code"] = left["ticker"].astype(str)
    if "date" not in left.columns or "ts_code" not in left.columns:
        return pd.DataFrame()
    left["date_dt"] = pd.to_datetime(left["date"].astype(str), errors="coerce")
    right = statement_features.copy()
    right["ann_date_dt"] = pd.to_datetime(right["ann_date"].astype(str), errors="coerce")
    left = left.dropna(subset=["date_dt", "ts_code"]).sort_values(["date_dt", "ts_code"]).reset_index(drop=True)
    right = right.dropna(subset=["ann_date_dt", "ts_code"]).sort_values(["ann_date_dt", "ts_code"]).reset_index(drop=True)
    feature_cols = [c for c in right.columns if c not in {"ts_code", "ann_date_dt"}]
    parts: list[pd.DataFrame] = []
    for code, lpart in left.groupby("ts_code", sort=False):
        rpart = right[right["ts_code"] == code][["ts_code", "ann_date_dt"] + feature_cols].sort_values("ann_date_dt")
        if rpart.empty:
            continue
        merged = pd.merge_asof(
            lpart.sort_values("date_dt"),
            rpart,
            left_on="date_dt",
            right_on="ann_date_dt",
            direction="backward",
            suffixes=("", "_top10"),
        )
        parts.append(merged)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code_top10" in out.columns:
        out = out.drop(columns=["ts_code_top10"])
    return out


def build_ownership_concentration_features(asof_frame: pd.DataFrame) -> pd.DataFrame:
    if asof_frame.empty:
        return pd.DataFrame()
    out = asof_frame.copy()
    required = {"date", "ticker", "forward_return_5d", "industry_relative_book_yield", "roe"}
    if not required.issubset(out.columns):
        return pd.DataFrame()
    for col in out.columns:
        if col in {"date", "ticker", "ts_code", "industry", "endpoint", "ann_date", "end_date"}:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["value_quality_baseline"] = out["industry_relative_book_yield"] + out["roe"]
    out["baseline_z"] = date_zscore(out["value_quality_baseline"], out["date"])
    base_cols = [
        "top10_float_ratio_sum",
        "top10_hold_ratio_sum",
        "top10_float_fund_like_ratio_sum",
        "top10_hold_fund_like_ratio_sum",
        "top10_float_hkscc_ratio",
        "top10_hold_hkscc_ratio",
        "top10_float_ratio_sum_change",
        "top10_hold_ratio_sum_change",
        "top10_float_fund_like_ratio_sum_change",
        "top10_hold_fund_like_ratio_sum_change",
        "top10_float_hkscc_ratio_change",
        "top10_hold_hkscc_ratio_change",
    ]
    created: list[str] = []
    for col in base_cols:
        if col not in out.columns or out[col].notna().sum() == 0:
            continue
        z = f"{col}_z"
        out[z] = date_zscore(out[col], out["date"])
        for direction, sign in (("high", 1.0), ("low", -1.0)):
            signal = f"{direction}_{col}"
            confirm = f"conf_{direction}_{col}"
            out[signal] = sign * out[z]
            out[confirm] = out["baseline_z"] + out[signal]
            created.extend([signal, confirm])
    subset = ["date", "ticker", "forward_return_5d", "value_quality_baseline"]
    out = out.dropna(subset=subset)
    out.attrs["ownership_signal_columns"] = created
    return out


def build_ownership_concentration_report(features: pd.DataFrame, *, config: OwnershipConcentrationConfig | None = None) -> dict[str, Any]:
    cfg = config or OwnershipConcentrationConfig()
    if features.empty:
        return {"coverage": {"rows": 0, "dates": 0, "tickers": 0}, "decision": {"decision": "stop_ownership_concentration_no_feature_overlap", "reasons": ["empty_features"]}}
    signal_cols = [c for c in features.columns if c.startswith(("high_top10_", "low_top10_", "conf_high_top10_", "conf_low_top10_"))]
    signal_cols = [c for c in signal_cols if pd.to_numeric(features[c], errors="coerce").notna().sum() > 0]
    coverage = {
        "rows": int(len(features)),
        "dates": int(features["date"].nunique()),
        "tickers": int(features["ticker"].nunique()),
        "signal_columns": signal_cols,
    }
    diagnostics: dict[str, Any] = {
        "baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
    }
    correlations: dict[str, Any] = {}
    for col in signal_cols:
        diagnostics[col] = bucket_spread(features, col, quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile)
        correlations[f"{col}_vs_baseline"] = spearman_corr(features[col], features["value_quality_baseline"])
        if "turnover" in features.columns:
            correlations[f"{col}_vs_turnover"] = spearman_corr(features[col], features["turnover"])
        if "turnover_shock_5_20" in features.columns:
            correlations[f"{col}_vs_turnover_shock_5_20"] = spearman_corr(features[col], features["turnover_shock_5_20"])
    best_signal = None
    best_spread = None
    for col, diag in diagnostics.items():
        if col == "baseline":
            continue
        spread = diag.get("spread_mean") if isinstance(diag, dict) else None
        if spread is not None and (best_spread is None or spread > best_spread):
            best_signal = col
            best_spread = spread
    baseline_spread = diagnostics["baseline"].get("spread_mean")
    reasons: list[str] = []
    if coverage["rows"] < cfg.min_rows:
        reasons.append("coverage_rows_too_low")
    if coverage["dates"] < cfg.min_dates:
        reasons.append("coverage_dates_too_low")
    if coverage["tickers"] < cfg.min_tickers:
        reasons.append("coverage_tickers_too_low")
    if not signal_cols:
        reasons.append("no_usable_signal_columns")
    if best_spread is None or best_spread <= 0:
        reasons.append("best_signal_not_positive")
    if best_spread is None or (baseline_spread is not None and best_spread <= baseline_spread):
        reasons.append("best_signal_not_incremental_vs_local_baseline")
    if best_spread is None or best_spread <= cfg.benchmark_spread:
        reasons.append("best_signal_not_above_value_quality_benchmark")
    if best_signal is not None:
        corr = correlations.get(f"{best_signal}_vs_baseline")
        if corr is not None and abs(corr) >= cfg.max_baseline_corr:
            reasons.append("best_signal_too_correlated_with_baseline")
    if any(r.startswith("coverage_") for r in reasons):
        decision = "stop_ownership_concentration_coverage_insufficient"
    elif "best_signal_too_correlated_with_baseline" in reasons:
        decision = "stop_ownership_concentration_duplicate_signal"
    elif reasons:
        decision = "stop_ownership_concentration_not_incremental"
    else:
        decision = "proceed_ownership_concentration_controlled_probe_plan"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "correlations": correlations,
        "best_signal": {"name": best_signal, "spread_mean": best_spread},
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "decision": {"decision": decision, "reasons": reasons or ["ownership_concentration_readonly_signal_passed"]},
    }
