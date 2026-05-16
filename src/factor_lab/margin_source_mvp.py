from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

STOCK_PREFIXES = (
    "000", "001", "002", "003", "300", "301",
    "600", "601", "603", "605", "688", "689",
    "430", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "920",
)

FEATURE_CORRELATION_COLUMNS = (
    "turnover",
    "turnover_shock_5_20",
    "volatility_20",
    "volatility_60",
    "total_mv",
    "size_inv",
    "pb",
    "earnings_yield",
)


def is_stock_like_ts_code(ts_code: str) -> bool:
    code = str(ts_code or "")
    prefix = code.split(".")[0]
    return prefix.startswith(STOCK_PREFIXES)


def classify_tickers(df: pd.DataFrame, code_col: str = "ts_code") -> dict[str, Any]:
    if df.empty or code_col not in df.columns:
        return {"rows": int(len(df)), "unique_tickers": 0, "stock_like_tickers": 0, "non_stock_like_tickers": 0, "stock_like_ratio": None}
    codes = pd.Series(df[code_col].dropna().astype(str).unique())
    stock_like = codes.map(is_stock_like_ts_code)
    total = int(len(codes))
    stock_count = int(stock_like.sum())
    return {
        "rows": int(len(df)),
        "unique_tickers": total,
        "stock_like_tickers": stock_count,
        "non_stock_like_tickers": int(total - stock_count),
        "stock_like_ratio": round(stock_count / total, 4) if total else None,
    }


def field_sanity(df: pd.DataFrame, fields: tuple[str, ...]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in fields:
        if field not in df.columns:
            out[field] = {"available": False}
            continue
        s = pd.to_numeric(df[field], errors="coerce")
        nonnull = s.notna()
        out[field] = {
            "available": True,
            "nonnull_rate": round(float(nonnull.mean()), 4) if len(s) else None,
            "zero_rate": round(float((s == 0).mean()), 4) if len(s) else None,
            "negative_rate": round(float((s < 0).mean()), 4) if len(s) else None,
            "median": None if s.dropna().empty else float(s.dropna().median()),
            "p95": None if s.dropna().empty else float(s.dropna().quantile(0.95)),
        }
    return out


def choose_feature_cache(cache_dir: str | Path = "artifacts/tushare_cache") -> Path | None:
    root = Path(cache_dir)
    candidates = sorted(root.glob("tushare_*.csv"), key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0] if candidates else None


def load_feature_cache(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
    if "ticker" in df.columns:
        df["ts_code"] = df["ticker"].astype(str)
    return df


def normalize_margin_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "trade_date" in out.columns:
        out["trade_date"] = out["trade_date"].astype(str)
    if "ts_code" in out.columns:
        out["ts_code"] = out["ts_code"].astype(str)
    for col in ["rzye", "rqye", "rzmre", "rzche", "rzrqye", "rqyl", "rqchl", "rqmcl"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def merge_margin_with_features(margin: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    if margin.empty or features.empty:
        return pd.DataFrame()
    if "date" not in features.columns or "ts_code" not in features.columns:
        return pd.DataFrame()
    left = margin.copy()
    right = features.copy()
    left["date"] = left["trade_date"].astype(str)
    return left.merge(right, on=["date", "ts_code"], how="inner", suffixes=("_margin", ""))


def add_margin_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mv = pd.to_numeric(out.get("total_mv"), errors="coerce") if "total_mv" in out.columns else pd.Series(index=out.index, dtype="float64")
    # Tushare total_mv is usually in 10k CNY; margin balances are CNY. Ratio is rough but stable enough for redundancy precheck.
    mv_cny = mv * 10000.0
    denom = mv_cny.where(mv_cny > 0)
    if "rzye" in out.columns:
        out["margin_balance_to_mv"] = pd.to_numeric(out["rzye"], errors="coerce") / denom
    if "rqye" in out.columns:
        out["short_balance_to_mv"] = pd.to_numeric(out["rqye"], errors="coerce") / denom
    if "rzmre" in out.columns and "rzche" in out.columns:
        out["margin_activity_to_mv"] = (pd.to_numeric(out["rzmre"], errors="coerce") + pd.to_numeric(out["rzche"], errors="coerce")) / denom
        out["margin_net_buy_to_mv"] = (pd.to_numeric(out["rzmre"], errors="coerce") - pd.to_numeric(out["rzche"], errors="coerce")) / denom
    return out


def correlation_precheck(merged: pd.DataFrame) -> dict[str, Any]:
    if merged.empty:
        return {"available": False, "reason": "no_overlap"}
    data = add_margin_features(merged)
    margin_cols = [c for c in ["margin_balance_to_mv", "short_balance_to_mv", "margin_activity_to_mv", "margin_net_buy_to_mv"] if c in data.columns]
    feature_cols = [c for c in FEATURE_CORRELATION_COLUMNS if c in data.columns]
    correlations: dict[str, dict[str, float | None]] = {}
    for mcol in margin_cols:
        ms = pd.to_numeric(data[mcol], errors="coerce")
        correlations[mcol] = {}
        for fcol in feature_cols:
            fs = pd.to_numeric(data[fcol], errors="coerce")
            valid = ms.notna() & fs.notna()
            if int(valid.sum()) < 10:
                correlations[mcol][fcol] = None
            else:
                corr = ms[valid].rank().corr(fs[valid].rank(), method="pearson")
                correlations[mcol][fcol] = round(float(corr), 4)
    max_abs_turnover_like = 0.0
    primary_abs_turnover_like = 0.0
    turnover_like_cols = ("turnover", "turnover_shock_5_20", "volatility_20", "volatility_60")
    for mcol, m in correlations.items():
        for f in turnover_like_cols:
            v = m.get(f)
            if v is not None:
                abs_v = abs(float(v))
                max_abs_turnover_like = max(max_abs_turnover_like, abs_v)
                if mcol == "margin_balance_to_mv":
                    primary_abs_turnover_like = max(primary_abs_turnover_like, abs_v)
    primary_flag = "high" if primary_abs_turnover_like >= 0.80 else ("medium" if primary_abs_turnover_like >= 0.55 else "low")
    any_flag = "high" if max_abs_turnover_like >= 0.80 else ("medium" if max_abs_turnover_like >= 0.55 else "low")
    return {
        "available": True,
        "rows": int(len(data)),
        "margin_feature_columns": margin_cols,
        "feature_columns": feature_cols,
        "spearman": correlations,
        "primary_margin_balance_max_abs_turnover_like_corr": round(primary_abs_turnover_like, 4),
        "primary_margin_balance_redundancy_flag": primary_flag,
        "max_abs_turnover_like_corr": round(max_abs_turnover_like, 4),
        "redundancy_flag": any_flag,
    }


def decide_margin_mvp(coverage: dict[str, Any], sanity: dict[str, Any], corr: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    stock_like_ratio = coverage.get("overall", {}).get("stock_like_ratio")
    overlap_rows = coverage.get("feature_overlap_rows", 0)
    if stock_like_ratio is None or stock_like_ratio < 0.5:
        reasons.append("stock_like_coverage_too_low")
    rzye = sanity.get("rzye", {})
    if not rzye.get("available") or (rzye.get("nonnull_rate") is not None and rzye.get("nonnull_rate") < 0.9):
        reasons.append("rzye_not_reliable")
    if overlap_rows <= 0:
        reasons.append("no_feature_store_overlap")
    if corr.get("primary_margin_balance_redundancy_flag") == "high":
        return {"decision": "margin_redundant_with_turnover", "reasons": reasons + ["primary_margin_balance_high_turnover_like_correlation"]}
    if reasons == ["no_feature_store_overlap"] or ("no_feature_store_overlap" in reasons and len(reasons) == 1):
        return {"decision": "margin_data_usable_but_needs_feature_store", "reasons": reasons}
    if reasons:
        return {"decision": "margin_mvp_blocked", "reasons": reasons}
    return {"decision": "proceed_margin_factor_probe_plan", "reasons": ["margin_data_has_stock_coverage_fields_overlap_and_not_highly_redundant"]}
