from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import bucket_spread, date_zscore, spearman_corr
from factor_lab.shareholder_crowding_source import DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD


@dataclass(frozen=True)
class PledgeSourceConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_rows: int = 1000
    min_dates: int = 100
    min_tickers: int = 20
    max_baseline_corr: float = 0.85
    max_turnover_corr: float = 0.85


DATE_COLUMNS = ("ann_date", "f_ann_date", "end_date", "start_date", "release_date", "trade_date")
NUMERIC_COLUMNS = (
    "pledge_ratio",
    "pledge_share_ratio",
    "total_share",
    "pledge_amount",
    "pledged_amount",
    "pledge_amt",
    "rest_pledge",
    "total_pledge",
    "p_total_ratio",
    "h_total_ratio",
    "holder_num",
)


def _clean_string(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace("-", "", regex=False).replace({"nan": pd.NA, "None": pd.NA, "NaT": pd.NA})


def normalize_pledge_frame(df: pd.DataFrame, *, endpoint: str) -> pd.DataFrame:
    out = df.copy()
    out["endpoint"] = endpoint
    rename = {
        "ts_code": "ts_code",
        "ann_date": "ann_date",
        "f_ann_date": "f_ann_date",
        "end_date": "end_date",
        "start_date": "start_date",
        "release_date": "release_date",
        "pledge_ratio": "pledge_ratio",
        "pledge_share_ratio": "pledge_share_ratio",
        "p_total_ratio": "p_total_ratio",
        "h_total_ratio": "h_total_ratio",
        "pledge_amount": "pledge_amount",
        "pledged_amount": "pledged_amount",
        "pledge_amt": "pledge_amt",
        "total_share": "total_share",
        "rest_pledge": "rest_pledge",
        "total_pledge": "total_pledge",
        "holder_num": "holder_num",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
    if "ann_date" not in out.columns:
        if "f_ann_date" in out.columns:
            out["ann_date"] = out["f_ann_date"]
        elif endpoint == "pledge_detail" and "start_date" in out.columns:
            # Conservative fallback: event is only known from start_date when no announcement field exists.
            out["ann_date"] = out["start_date"]
    for col in ("ts_code", "endpoint"):
        if col in out.columns:
            out[col] = out[col].astype(str).replace({"nan": pd.NA, "None": pd.NA})
    for col in DATE_COLUMNS:
        if col in out.columns:
            out[col] = _clean_string(out[col])
    for col in NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def pledge_preflight(endpoint_frames: dict[str, pd.DataFrame]) -> dict[str, Any]:
    reports: dict[str, Any] = {}
    total_rows = 0
    tickers: set[str] = set()
    pit_safe_endpoints = 0
    for endpoint, frame in endpoint_frames.items():
        df = normalize_pledge_frame(frame, endpoint=endpoint) if frame is not None else pd.DataFrame()
        rows = int(len(df))
        total_rows += rows
        if "ts_code" in df.columns:
            tickers.update(df["ts_code"].dropna().astype(str).tolist())
        required = {"ts_code", "ann_date"}
        missing = sorted(required - set(df.columns))
        signal_fields = sorted(set(NUMERIC_COLUMNS) & set(df.columns))
        if not missing and rows:
            pit_safe_endpoints += 1
        reports[endpoint] = {
            "rows": rows,
            "tickers": int(df["ts_code"].nunique()) if "ts_code" in df.columns and rows else 0,
            "columns": sorted(str(c) for c in df.columns),
            "required_fields_present": not missing,
            "missing_required_fields": missing,
            "available_signal_fields": signal_fields,
            "signal_fields_present": bool(signal_fields),
            "ann_date_nonnull_rate": round(float(df["ann_date"].replace("nan", pd.NA).notna().mean()), 4) if "ann_date" in df.columns and rows else None,
        }
    return {"rows": total_rows, "tickers": int(len(tickers)), "pit_safe_endpoints": pit_safe_endpoints, "endpoint_reports": reports}


def _first_existing(df: pd.DataFrame, cols: tuple[str, ...]) -> pd.Series:
    out = pd.Series(pd.NA, index=df.index, dtype="object")
    for col in cols:
        if col in df.columns:
            values = pd.to_numeric(df[col], errors="coerce")
            out = out.where(out.notna(), values)
    return pd.to_numeric(out, errors="coerce")


def build_pledge_statement_features(endpoint_frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for endpoint, frame in endpoint_frames.items():
        df = normalize_pledge_frame(frame, endpoint=endpoint)
        if df.empty or not {"ts_code", "ann_date"}.issubset(df.columns):
            continue
        df = df.dropna(subset=["ts_code", "ann_date"]).copy()
        if df.empty:
            continue
        df["pledge_ratio_raw"] = _first_existing(df, ("pledge_ratio", "pledge_share_ratio", "p_total_ratio", "h_total_ratio"))
        df["pledge_amount_raw"] = _first_existing(df, ("pledge_amount", "pledged_amount", "pledge_amt", "rest_pledge", "total_pledge"))
        if "end_date" not in df.columns:
            df["end_date"] = df["ann_date"]
        rows: list[dict[str, Any]] = []
        for (ts_code, ann_date, end_date), g in df.groupby(["ts_code", "ann_date", "end_date"], dropna=True):
            ratio = pd.to_numeric(g["pledge_ratio_raw"], errors="coerce")
            amount = pd.to_numeric(g["pledge_amount_raw"], errors="coerce")
            rows.append({
                "endpoint": endpoint,
                "ts_code": str(ts_code),
                "ann_date": str(ann_date),
                "end_date": str(end_date),
                "pledge_record_count": int(len(g)),
                "pledge_ratio_mean": float(ratio.mean(skipna=True)) if ratio.notna().any() else pd.NA,
                "pledge_ratio_max": float(ratio.max(skipna=True)) if ratio.notna().any() else pd.NA,
                "pledge_amount_sum": float(amount.sum(skipna=True)) if amount.notna().any() else pd.NA,
            })
        if rows:
            agg = pd.DataFrame(rows).sort_values(["endpoint", "ts_code", "ann_date"])
            for col in ("pledge_ratio_mean", "pledge_ratio_max", "pledge_amount_sum", "pledge_record_count"):
                agg[f"{col}_change"] = pd.to_numeric(agg[col], errors="coerce").groupby([agg["endpoint"], agg["ts_code"]]).diff(1)
            parts.append(agg)
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True, sort=False)
    return out.sort_values(["ts_code", "ann_date", "endpoint"]).reset_index(drop=True)


def asof_merge_pledge_features(feature_cache: pd.DataFrame, statement_features: pd.DataFrame) -> pd.DataFrame:
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
        parts.append(pd.merge_asof(lpart.sort_values("date_dt"), rpart, left_on="date_dt", right_on="ann_date_dt", direction="backward", suffixes=("", "_pledge")))
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    if "ts_code_pledge" in out.columns:
        out = out.drop(columns=["ts_code_pledge"])
    return out


def build_pledge_readonly_features(asof_frame: pd.DataFrame) -> pd.DataFrame:
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
    base_cols = [c for c in out.columns if c.startswith("pledge_") and not c.endswith("_z")]
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
    out.attrs["pledge_signal_columns"] = created
    return out.dropna(subset=["date", "ticker", "forward_return_5d", "value_quality_baseline"])


def build_pledge_report(features: pd.DataFrame, *, config: PledgeSourceConfig | None = None) -> dict[str, Any]:
    cfg = config or PledgeSourceConfig()
    if features.empty:
        return {"coverage": {"rows": 0, "dates": 0, "tickers": 0}, "decision": {"decision": "stop_pledge_no_feature_overlap", "reasons": ["empty_features"]}}
    signal_cols = [c for c in features.columns if c.startswith(("high_pledge_", "low_pledge_", "conf_high_pledge_", "conf_low_pledge_"))]
    signal_cols = [c for c in signal_cols if pd.to_numeric(features[c], errors="coerce").notna().sum() > 0]
    coverage = {"rows": int(len(features)), "dates": int(features["date"].nunique()), "tickers": int(features["ticker"].nunique()), "signal_columns": signal_cols}
    diagnostics: dict[str, Any] = {"baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile)}
    correlations: dict[str, Any] = {}
    for col in signal_cols:
        diagnostics[col] = bucket_spread(features, col, quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile)
        correlations[f"{col}_vs_baseline"] = spearman_corr(features[col], features["value_quality_baseline"])
        if "turnover" in features.columns:
            correlations[f"{col}_vs_turnover"] = spearman_corr(features[col], features["turnover"])
        if "size_inv" in features.columns:
            correlations[f"{col}_vs_size_inv"] = spearman_corr(features[col], features["size_inv"])
    ranked = [(name, item) for name, item in diagnostics.items() if name != "baseline" and isinstance(item, dict) and item.get("spread_mean") is not None]
    ranked.sort(key=lambda kv: float(kv[1].get("spread_mean") or -999), reverse=True)
    best_name, best_diag = (ranked[0] if ranked else (None, {}))
    best_spread = best_diag.get("spread_mean") if isinstance(best_diag, dict) else None
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
    if best_name is not None:
        base_corr = correlations.get(f"{best_name}_vs_baseline")
        turnover_corr = correlations.get(f"{best_name}_vs_turnover")
        if base_corr is not None and abs(base_corr) >= cfg.max_baseline_corr:
            reasons.append("best_signal_too_correlated_with_baseline")
        if turnover_corr is not None and abs(turnover_corr) >= cfg.max_turnover_corr:
            reasons.append("best_signal_too_correlated_with_turnover")
    if any(r.startswith("coverage_") for r in reasons):
        decision = "stop_pledge_coverage_insufficient"
    elif any("too_correlated" in r for r in reasons):
        decision = "stop_pledge_duplicate_signal"
    elif reasons:
        decision = "stop_pledge_not_incremental"
    else:
        decision = "proceed_controlled_pledge_probe_plan"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "correlations": correlations,
        "best_signal": {
            "name": best_name,
            "spread_mean": best_spread,
            "positive_rate": best_diag.get("spread_positive_rate") if isinstance(best_diag, dict) else None,
            "observations": best_diag.get("observations") if isinstance(best_diag, dict) else None,
        },
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "decision": {"decision": decision, "reasons": reasons or ["pledge_readonly_signal_passed"]},
    }
