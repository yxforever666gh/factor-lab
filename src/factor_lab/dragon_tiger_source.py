from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.margin_feature_builder import bucket_spread, date_zscore, spearman_corr
from factor_lab.shareholder_crowding_source import DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD


@dataclass(frozen=True)
class DragonTigerConfig:
    quantiles: int = 5
    long_quantile: int = 3
    short_quantile: int = 0
    benchmark_spread: float = DEFAULT_VALUE_ROUTE_BENCHMARK_SPREAD
    min_rows: int = 1000
    min_dates: int = 100
    min_tickers: int = 20
    max_baseline_corr: float = 0.85
    max_turnover_corr: float = 0.85


NUMERIC_COLUMNS = (
    "close",
    "pct_change",
    "turnover_rate",
    "amount",
    "l_sell",
    "l_buy",
    "l_amount",
    "net_amount",
    "net_rate",
    "amount_rate",
    "float_values",
)


def normalize_top_list_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    rename = {
        "trade_date": "trade_date",
        "ts_code": "ts_code",
        "name": "name",
        "close": "close",
        "pct_change": "pct_change",
        "turnover_rate": "turnover_rate",
        "amount": "amount",
        "l_sell": "l_sell",
        "l_buy": "l_buy",
        "l_amount": "l_amount",
        "net_amount": "net_amount",
        "net_rate": "net_rate",
        "amount_rate": "amount_rate",
        "float_values": "float_values",
        "reason": "reason",
    }
    out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
    for col in ("trade_date", "ts_code"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace("-", "", regex=False).replace({"nan": pd.NA, "None": pd.NA})
    for col in NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def dragon_tiger_preflight(df: pd.DataFrame) -> dict[str, Any]:
    out = normalize_top_list_frame(df) if df is not None and not df.empty else pd.DataFrame(columns=list(df.columns) if df is not None else [])
    required = {"trade_date", "ts_code"}
    signal_any = {"net_amount", "l_buy", "l_sell", "amount"}
    missing_required = sorted(required - set(out.columns))
    available_signal = sorted(signal_any & set(out.columns))
    return {
        "rows": int(len(out)),
        "columns": sorted(str(c) for c in out.columns),
        "required_fields_present": not missing_required,
        "missing_required_fields": missing_required,
        "available_signal_fields": available_signal,
        "signal_fields_present": bool(available_signal),
        "tickers": int(out["ts_code"].nunique()) if "ts_code" in out.columns else 0,
        "dates": int(out["trade_date"].nunique()) if "trade_date" in out.columns else 0,
        "trade_date_min": None if "trade_date" not in out.columns or out.empty else str(out["trade_date"].min()),
        "trade_date_max": None if "trade_date" not in out.columns or out.empty else str(out["trade_date"].max()),
        "net_amount_nonnull_rate": round(float(out["net_amount"].notna().mean()), 4) if "net_amount" in out.columns and len(out) else None,
    }


def build_dragon_tiger_daily_events(top_list: pd.DataFrame) -> pd.DataFrame:
    out = normalize_top_list_frame(top_list)
    required = {"trade_date", "ts_code"}
    if out.empty or not required.issubset(out.columns):
        return pd.DataFrame()
    out = out.dropna(subset=["trade_date", "ts_code"]).copy()
    if out.empty:
        return out
    for col in NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "net_amount" not in out.columns:
        if {"l_buy", "l_sell"}.issubset(out.columns):
            out["net_amount"] = out["l_buy"] - out["l_sell"]
        else:
            out["net_amount"] = pd.NA
    if "l_buy" not in out.columns:
        out["l_buy"] = pd.NA
    if "l_sell" not in out.columns:
        out["l_sell"] = pd.NA
    if "amount" not in out.columns:
        out["amount"] = pd.NA
    if "turnover_rate" not in out.columns:
        out["turnover_rate"] = pd.NA
    grouped = out.groupby(["trade_date", "ts_code"], dropna=True)
    rows: list[dict[str, Any]] = []
    for (trade_date, ts_code), g in grouped:
        net = pd.to_numeric(g["net_amount"], errors="coerce")
        buy = pd.to_numeric(g["l_buy"], errors="coerce")
        sell = pd.to_numeric(g["l_sell"], errors="coerce")
        amount = pd.to_numeric(g["amount"], errors="coerce")
        turnover = pd.to_numeric(g["turnover_rate"], errors="coerce")
        denom = (buy + sell).replace(0, pd.NA)
        imbalance = ((buy - sell) / denom).replace([float("inf"), float("-inf")], pd.NA)
        rows.append(
            {
                "trade_date": str(trade_date),
                "ts_code": str(ts_code),
                "dt_event_count": int(len(g)),
                "dt_net_amount_sum": float(net.sum(skipna=True)) if net.notna().any() else pd.NA,
                "dt_buy_sum": float(buy.sum(skipna=True)) if buy.notna().any() else pd.NA,
                "dt_sell_sum": float(sell.sum(skipna=True)) if sell.notna().any() else pd.NA,
                "dt_amount_sum": float(amount.sum(skipna=True)) if amount.notna().any() else pd.NA,
                "dt_turnover_rate_mean": float(turnover.mean(skipna=True)) if turnover.notna().any() else pd.NA,
                "dt_buy_sell_imbalance": float(imbalance.mean(skipna=True)) if imbalance.notna().any() else pd.NA,
            }
        )
    daily = pd.DataFrame(rows)
    if daily.empty:
        return daily
    daily = daily.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    mv_denom = daily["dt_amount_sum"].where(pd.to_numeric(daily["dt_amount_sum"], errors="coerce") > 0)
    daily["dt_net_amount_to_amount"] = pd.to_numeric(daily["dt_net_amount_sum"], errors="coerce") / mv_denom
    return daily


def asof_merge_dragon_tiger_features(feature_cache: pd.DataFrame, daily_events: pd.DataFrame, *, windows: tuple[int, ...] = (5, 20)) -> pd.DataFrame:
    if feature_cache.empty:
        return pd.DataFrame()
    left = feature_cache.copy()
    if "ticker" in left.columns and "ts_code" not in left.columns:
        left["ts_code"] = left["ticker"].astype(str)
    if "date" not in left.columns or "ts_code" not in left.columns:
        return pd.DataFrame()
    left["date"] = pd.to_datetime(left["date"].astype(str), errors="coerce").dt.strftime("%Y%m%d")
    left = left.dropna(subset=["date", "ts_code"]).copy()
    if left.empty:
        return pd.DataFrame()
    events = build_dragon_tiger_daily_events(daily_events) if "dt_event_count" not in daily_events.columns else daily_events.copy()
    if events.empty:
        out = left.copy()
        for w in windows:
            out[f"dt_event_count_{w}d"] = 0.0
            out[f"dt_net_amount_sum_{w}d"] = 0.0
            out[f"dt_buy_sell_imbalance_{w}d"] = pd.NA
            out[f"dt_net_amount_to_amount_{w}d"] = pd.NA
        return out
    events["trade_date"] = pd.to_datetime(events["trade_date"].astype(str), errors="coerce").dt.strftime("%Y%m%d")
    numeric_event_cols = [c for c in events.columns if c not in {"trade_date", "ts_code"}]
    for col in numeric_event_cols:
        events[col] = pd.to_numeric(events[col], errors="coerce")
    out_parts: list[pd.DataFrame] = []
    all_codes = sorted(set(left["ts_code"].dropna().astype(str)) | set(events["ts_code"].dropna().astype(str)))
    for code in all_codes:
        lpart = left[left["ts_code"].astype(str) == code].sort_values("date").copy()
        if lpart.empty:
            continue
        dates = pd.to_datetime(lpart["date"], errors="coerce")
        epart = events[events["ts_code"].astype(str) == code].sort_values("trade_date").copy()
        for w in windows:
            lpart[f"dt_event_count_{w}d"] = 0.0
            lpart[f"dt_net_amount_sum_{w}d"] = 0.0
            lpart[f"dt_buy_sum_{w}d"] = 0.0
            lpart[f"dt_sell_sum_{w}d"] = 0.0
            lpart[f"dt_buy_sell_imbalance_{w}d"] = pd.NA
            lpart[f"dt_net_amount_to_amount_{w}d"] = pd.NA
        if not epart.empty:
            edates = pd.to_datetime(epart["trade_date"], errors="coerce")
            for idx, dt in dates.items():
                if pd.isna(dt):
                    continue
                for w in windows:
                    start = dt - pd.Timedelta(days=w - 1)
                    mask = (edates >= start) & (edates <= dt)
                    g = epart.loc[mask]
                    if g.empty:
                        continue
                    lpart.loc[idx, f"dt_event_count_{w}d"] = float(pd.to_numeric(g["dt_event_count"], errors="coerce").sum(skipna=True))
                    lpart.loc[idx, f"dt_net_amount_sum_{w}d"] = float(pd.to_numeric(g["dt_net_amount_sum"], errors="coerce").sum(skipna=True))
                    lpart.loc[idx, f"dt_buy_sum_{w}d"] = float(pd.to_numeric(g["dt_buy_sum"], errors="coerce").sum(skipna=True))
                    lpart.loc[idx, f"dt_sell_sum_{w}d"] = float(pd.to_numeric(g["dt_sell_sum"], errors="coerce").sum(skipna=True))
                    lpart.loc[idx, f"dt_buy_sell_imbalance_{w}d"] = pd.to_numeric(g["dt_buy_sell_imbalance"], errors="coerce").mean(skipna=True)
                    amount = pd.to_numeric(g["dt_amount_sum"], errors="coerce").sum(skipna=True) if "dt_amount_sum" in g.columns else pd.NA
                    net = pd.to_numeric(g["dt_net_amount_sum"], errors="coerce").sum(skipna=True)
                    if pd.notna(amount) and amount != 0:
                        lpart.loc[idx, f"dt_net_amount_to_amount_{w}d"] = float(net / amount)
        out_parts.append(lpart)
    return pd.concat(out_parts, ignore_index=True) if out_parts else pd.DataFrame()


def build_dragon_tiger_readonly_features(asof_frame: pd.DataFrame) -> pd.DataFrame:
    if asof_frame.empty:
        return pd.DataFrame()
    out = asof_frame.copy()
    required = {"date", "ticker", "forward_return_5d", "industry_relative_book_yield", "roe"}
    if not required.issubset(out.columns):
        return pd.DataFrame()
    for col in out.columns:
        if col in {"date", "ticker", "ts_code", "industry"}:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["value_quality_baseline"] = out["industry_relative_book_yield"] + out["roe"]
    out["baseline_z"] = date_zscore(out["value_quality_baseline"], out["date"])
    created: list[str] = []
    source_cols = [c for c in out.columns if c.startswith("dt_") and c.endswith(("_5d", "_20d"))]
    for col in source_cols:
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
    out.attrs["dragon_tiger_signal_columns"] = created
    return out.dropna(subset=["date", "ticker", "forward_return_5d", "value_quality_baseline"])


def build_dragon_tiger_report(features: pd.DataFrame, *, config: DragonTigerConfig | None = None) -> dict[str, Any]:
    cfg = config or DragonTigerConfig()
    if features.empty:
        return {"coverage": {"rows": 0, "dates": 0, "tickers": 0}, "decision": {"decision": "stop_dragon_tiger_no_feature_overlap", "reasons": ["empty_features"]}}
    signal_cols = [c for c in features.columns if c.startswith(("high_dt_", "low_dt_", "conf_high_dt_", "conf_low_dt_"))]
    signal_cols = [c for c in signal_cols if pd.to_numeric(features[c], errors="coerce").notna().sum() > 0]
    coverage = {
        "rows": int(len(features)),
        "dates": int(features["date"].nunique()),
        "tickers": int(features["ticker"].nunique()),
        "signal_columns": signal_cols,
        "event_active_rows": int((pd.to_numeric(features.get("dt_event_count_20d", pd.Series(index=features.index)), errors="coerce").fillna(0) > 0).sum()),
    }
    diagnostics: dict[str, Any] = {
        "baseline": bucket_spread(features, "value_quality_baseline", quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile),
    }
    for col in signal_cols:
        diagnostics[col] = bucket_spread(features, col, quantiles=cfg.quantiles, long_quantile=cfg.long_quantile, short_quantile=cfg.short_quantile)
    ranked = [(name, item) for name, item in diagnostics.items() if name != "baseline" and isinstance(item, dict) and item.get("spread_mean") is not None]
    ranked.sort(key=lambda kv: float(kv[1].get("spread_mean") or -999), reverse=True)
    best_name, best_diag = (ranked[0] if ranked else (None, {}))
    correlations = {}
    if best_name:
        correlations["best_vs_baseline"] = spearman_corr(features[best_name], features["value_quality_baseline"])
        correlations["best_vs_turnover"] = spearman_corr(features[best_name], features["turnover"]) if "turnover" in features.columns else None
        correlations["best_vs_momentum_20"] = spearman_corr(features[best_name], features["momentum_20"]) if "momentum_20" in features.columns else None
    baseline_spread = diagnostics["baseline"].get("spread_mean")
    best_spread = best_diag.get("spread_mean") if isinstance(best_diag, dict) else None
    reasons: list[str] = []
    if coverage["rows"] < cfg.min_rows:
        reasons.append("coverage_rows_too_low")
    if coverage["dates"] < cfg.min_dates:
        reasons.append("coverage_dates_too_low")
    if coverage["tickers"] < cfg.min_tickers:
        reasons.append("coverage_tickers_too_low")
    if coverage["event_active_rows"] <= 0:
        reasons.append("no_event_overlap")
    if best_name is None or best_spread is None:
        reasons.append("no_usable_signal_spread")
    else:
        if baseline_spread is not None and best_spread <= baseline_spread:
            reasons.append("best_signal_not_incremental_vs_local_baseline")
        if best_spread <= cfg.benchmark_spread:
            reasons.append("best_signal_not_above_value_quality_benchmark")
    best_base_corr = correlations.get("best_vs_baseline")
    if best_base_corr is not None and abs(best_base_corr) >= cfg.max_baseline_corr:
        reasons.append("best_signal_too_correlated_with_baseline")
    best_turnover_corr = correlations.get("best_vs_turnover")
    if best_turnover_corr is not None and abs(best_turnover_corr) >= cfg.max_turnover_corr:
        reasons.append("best_signal_too_turnover_like")
    if any(r.startswith("coverage_") for r in reasons) or "no_event_overlap" in reasons:
        decision = "stop_dragon_tiger_coverage_insufficient"
    elif "best_signal_not_above_value_quality_benchmark" in reasons or "best_signal_not_incremental_vs_local_baseline" in reasons or "no_usable_signal_spread" in reasons:
        decision = "stop_dragon_tiger_not_incremental"
    elif "best_signal_too_correlated_with_baseline" in reasons or "best_signal_too_turnover_like" in reasons:
        decision = "stop_dragon_tiger_duplicate_signal"
    else:
        decision = "proceed_controlled_dragon_tiger_probe_plan"
    return {
        "coverage": coverage,
        "diagnostics": diagnostics,
        "best_signal": {"name": best_name, "spread_mean": best_spread, "positive_rate": best_diag.get("spread_positive_rate") if isinstance(best_diag, dict) else None, "observations": best_diag.get("observations") if isinstance(best_diag, dict) else None},
        "correlations": correlations,
        "benchmark": {"value_quality_no_distress_bucket_spread": cfg.benchmark_spread},
        "decision": {"decision": decision, "reasons": reasons or ["readonly_signal_passed_preliminary_checks"]},
    }
