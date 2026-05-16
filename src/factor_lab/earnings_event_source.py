from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import bucket_spread, date_zscore, spearman_corr
from factor_lab.shareholder_crowding_source import DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD


@dataclass(frozen=True)
class EarningsEventConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_rows: int = 1000
    min_dates: int = 100
    min_tickers: int = 25
    max_baseline_corr: float = 0.85


def _clean_date_col(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace("-", "", regex=False).replace({"nan": pd.NA, "None": pd.NA, "NaT": pd.NA})


def normalize_event_frame(df: pd.DataFrame, *, endpoint: str) -> pd.DataFrame:
    out = df.copy()
    out["endpoint"] = endpoint
    if "ann_date" not in out.columns and "f_ann_date" in out.columns:
        out["ann_date"] = out["f_ann_date"]
    for col in ("ts_code", "ann_date", "f_ann_date", "end_date", "type", "summary", "perf_summary", "endpoint"):
        if col in out.columns:
            out[col] = _clean_date_col(out[col]) if col in {"ann_date", "f_ann_date", "end_date"} else out[col].astype(str).replace({"nan": pd.NA, "None": pd.NA})
    numeric_cols = [
        "p_change_min", "p_change_max", "net_profit_min", "net_profit_max", "last_parent_net", "first_ann_date",
        "revenue", "operate_profit", "total_profit", "n_income", "total_assets", "total_hldr_eqy_exc_min_int",
        "diluted_eps", "diluted_roe", "yoy_net_profit", "bps",
        "n_income_log_qoq", "n_income_log_yoy", "revenue_log_qoq", "revenue_log_yoy", "diluted_roe_qoq", "diluted_roe_yoy",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def event_preflight(endpoint_frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    total_rows = 0
    tickers: set[str] = set()
    pit_safe_endpoints = 0
    for endpoint, frame in endpoint_frames.items():
        df = normalize_event_frame(frame, endpoint=endpoint) if frame is not None else pd.DataFrame()
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
    return {"rows": total_rows, "tickers": int(len(tickers)), "pit_safe_endpoints": pit_safe_endpoints, "endpoint_reports": reports}


def _forecast_type_score(series: pd.Series) -> pd.Series:
    text = series.fillna("").astype(str)
    positive = text.str.contains("预增|略增|扭亏|续盈", regex=True, na=False)
    negative = text.str.contains("预减|略减|首亏|续亏|增亏", regex=True, na=False)
    score = pd.Series(0.0, index=series.index)
    score.loc[positive] = 1.0
    score.loc[negative] = -1.0
    return score


def build_earnings_event_statement_features(endpoint_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for endpoint, frame in endpoint_frames.items():
        df = normalize_event_frame(frame, endpoint=endpoint)
        required = {"ts_code", "ann_date", "end_date"}
        if df.empty or not required.issubset(df.columns):
            continue
        df = df.dropna(subset=["ts_code", "ann_date", "end_date"]).copy()
        if df.empty:
            continue
        df = df.sort_values(["ts_code", "end_date", "ann_date"]).drop_duplicates(["ts_code", "end_date"], keep="last")
        df = df.sort_values(["endpoint", "ts_code", "end_date"])
        if endpoint == "forecast":
            if "type" in df.columns:
                df["forecast_type_score"] = _forecast_type_score(df["type"])
            if {"p_change_min", "p_change_max"}.issubset(df.columns):
                df["forecast_p_change_mid"] = pd.concat([df["p_change_min"], df["p_change_max"]], axis=1).mean(axis=1)
            elif "p_change_min" in df.columns:
                df["forecast_p_change_mid"] = df["p_change_min"]
            elif "p_change_max" in df.columns:
                df["forecast_p_change_mid"] = df["p_change_max"]
            if {"net_profit_min", "net_profit_max"}.issubset(df.columns):
                df["forecast_net_profit_mid"] = pd.concat([df["net_profit_min"], df["net_profit_max"]], axis=1).mean(axis=1)
            elif "net_profit_min" in df.columns:
                df["forecast_net_profit_mid"] = df["net_profit_min"]
            elif "net_profit_max" in df.columns:
                df["forecast_net_profit_mid"] = df["net_profit_max"]
            keep = [c for c in ["endpoint", "ts_code", "ann_date", "end_date", "forecast_type_score", "forecast_p_change_mid", "forecast_net_profit_mid"] if c in df.columns]
            out = df[keep].copy()
            for col in ["forecast_type_score", "forecast_p_change_mid", "forecast_net_profit_mid"]:
                if col in out.columns:
                    out[f"{col}_qoq"] = out.groupby("ts_code")[col].diff(1)
            parts.append(out)
        elif endpoint == "express":
            if "n_income" in df.columns:
                income = pd.to_numeric(df["n_income"], errors="coerce")
                sign = income.where(income.abs() > 0).abs()
                df["express_n_income_log"] = income.where(income > 0).map(lambda x: pd.NA if pd.isna(x) else float(pd.np.log1p(x))) if False else pd.NA
            import numpy as np
            if "n_income" in df.columns:
                df["express_n_income_log"] = pd.to_numeric(df["n_income"], errors="coerce").where(lambda s: s > 0).pipe(lambda s: pd.Series(np.log1p(s), index=s.index))
            if "revenue" in df.columns:
                df["express_revenue_log"] = pd.to_numeric(df["revenue"], errors="coerce").where(lambda s: s > 0).pipe(lambda s: pd.Series(np.log1p(s), index=s.index))
            for raw, outcol in (("n_income_log_qoq", "express_n_income_log_qoq"), ("n_income_log_yoy", "express_n_income_log_yoy"), ("revenue_log_qoq", "express_revenue_log_qoq"), ("revenue_log_yoy", "express_revenue_log_yoy"), ("diluted_roe_qoq", "express_diluted_roe_qoq"), ("diluted_roe_yoy", "express_diluted_roe_yoy"), ("diluted_roe", "express_diluted_roe"), ("yoy_net_profit", "express_yoy_net_profit")):
                if raw in df.columns:
                    df[outcol] = pd.to_numeric(df[raw], errors="coerce")
            for col in ["express_n_income_log", "express_revenue_log", "express_diluted_roe"]:
                if col in df.columns:
                    df[f"{col}_qoq_calc"] = df.groupby("ts_code")[col].diff(1)
            keep = [c for c in df.columns if c in {"endpoint", "ts_code", "ann_date", "end_date"} or c.startswith("express_")]
            parts.append(df[keep].copy())
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True, sort=False)
    return out.sort_values(["ts_code", "ann_date", "endpoint"]).reset_index(drop=True)


def asof_merge_earnings_event_features(feature_cache: pd.DataFrame, statement_features: pd.DataFrame) -> pd.DataFrame:
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
        merged = pd.merge_asof(lpart.sort_values("date_dt"), rpart, left_on="date_dt", right_on="ann_date_dt", direction="backward", suffixes=("", "_event"))
        parts.append(merged)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code_event" in out.columns:
        out = out.drop(columns=["ts_code_event"])
    return out


def build_earnings_event_features(asof_frame: pd.DataFrame) -> pd.DataFrame:
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
    base_cols = [c for c in out.columns if c.startswith(("forecast_", "express_")) and not c.endswith("_z")]
    created: list[str] = []
    for col in base_cols:
        if pd.to_numeric(out[col], errors="coerce").notna().sum() == 0:
            continue
        z = f"{col}_z"
        out[z] = date_zscore(out[col], out["date"])
        for direction, sign in (("high", 1.0), ("low", -1.0)):
            signal = f"{direction}_{col}"
            confirm = f"conf_{direction}_{col}"
            out[signal] = sign * out[z]
            out[confirm] = out["baseline_z"] + out[signal]
            created.extend([signal, confirm])
    out.attrs["earnings_event_signal_columns"] = created
    return out.dropna(subset=["date", "ticker", "forward_return_5d", "value_quality_baseline"])


def build_earnings_event_report(features: pd.DataFrame, *, config: EarningsEventConfig | None = None) -> dict[str, Any]:
    cfg = config or EarningsEventConfig()
    if features.empty:
        return {"coverage": {"rows": 0, "dates": 0, "tickers": 0}, "decision": {"decision": "stop_earnings_event_no_feature_overlap", "reasons": ["empty_features"]}}
    signal_cols = [c for c in features.columns if c.startswith(("high_forecast_", "low_forecast_", "conf_high_forecast_", "conf_low_forecast_", "high_express_", "low_express_", "conf_high_express_", "conf_low_express_"))]
    signal_cols = [c for c in signal_cols if pd.to_numeric(features[c], errors="coerce").notna().sum() > 0]
    coverage = {"rows": int(len(features)), "dates": int(features["date"].nunique()), "tickers": int(features["ticker"].nunique()), "signal_columns": signal_cols}
    diagnostics: dict[str, Any] = {"baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile)}
    correlations: dict[str, Any] = {}
    for col in signal_cols:
        diagnostics[col] = bucket_spread(features, col, quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile)
        correlations[f"{col}_vs_baseline"] = spearman_corr(features[col], features["value_quality_baseline"])
        if "turnover" in features.columns:
            correlations[f"{col}_vs_turnover"] = spearman_corr(features[col], features["turnover"])
    best_signal = None
    best_spread = None
    for col, diag in diagnostics.items():
        if col == "baseline" or not isinstance(diag, dict):
            continue
        spread = diag.get("spread_mean")
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
    if best_signal is None or best_spread is None:
        reasons.append("no_valid_event_signal")
    elif best_spread <= 0:
        reasons.append("best_signal_not_positive")
    elif baseline_spread is not None and best_spread <= baseline_spread:
        reasons.append("best_signal_not_incremental_vs_local_baseline")
    if best_spread is None or best_spread <= cfg.benchmark_spread:
        reasons.append("best_signal_not_above_value_quality_benchmark")
    if best_signal is not None:
        corr = correlations.get(f"{best_signal}_vs_baseline")
        if corr is not None and abs(corr) >= cfg.max_baseline_corr:
            reasons.append("best_signal_too_correlated_with_baseline")
    if any(r.startswith("coverage_") for r in reasons):
        decision = "stop_earnings_event_coverage_insufficient"
    elif "best_signal_too_correlated_with_baseline" in reasons:
        decision = "stop_earnings_event_duplicate_signal"
    elif any(r in reasons for r in ["no_valid_event_signal", "best_signal_not_positive", "best_signal_not_incremental_vs_local_baseline", "best_signal_not_above_value_quality_benchmark"]):
        decision = "stop_earnings_event_not_incremental"
    else:
        decision = "proceed_earnings_event_controlled_probe_plan"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "correlations": correlations,
        "best_signal": {"name": best_signal, "spread_mean": best_spread, "diagnostic": diagnostics.get(best_signal) if best_signal else None},
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "decision": {"decision": decision, "reasons": reasons or ["earnings_event_readonly_signal_passed"]},
    }
