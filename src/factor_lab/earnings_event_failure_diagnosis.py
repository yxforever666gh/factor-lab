from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_BENCHMARK_SPREAD = 0.0062253011


def _to_numeric_frame(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def bucket_profile(
    df: pd.DataFrame,
    *,
    signal_col: str,
    return_col: str = "forward_return_5d",
    quantiles: int = 5,
) -> dict[str, Any]:
    required = {"date", "ticker", signal_col, return_col}
    if df.empty or not required.issubset(df.columns):
        return {"quantiles": quantiles, "bucket_returns": {}, "spreads": {}, "observations": 0}
    data = _to_numeric_frame(df[["date", "ticker", signal_col, return_col]].copy(), [signal_col, return_col])
    data = data.dropna(subset=["date", "ticker", signal_col, return_col])
    if data.empty:
        return {"quantiles": quantiles, "bucket_returns": {}, "spreads": {}, "observations": 0}
    rows: list[dict[str, Any]] = []
    for date, part in data.groupby("date", sort=True):
        if len(part) < quantiles:
            continue
        try:
            ranks = part[signal_col].rank(method="first")
            buckets = pd.qcut(ranks, quantiles, labels=False, duplicates="drop")
        except Exception:
            continue
        tmp = part.copy()
        tmp["bucket"] = pd.to_numeric(buckets, errors="coerce")
        if tmp["bucket"].nunique(dropna=True) < quantiles:
            continue
        for bucket, bpart in tmp.groupby("bucket"):
            rows.append({
                "date": date,
                "bucket": int(bucket),
                "mean_return": float(bpart[return_col].mean()),
                "count": int(len(bpart)),
            })
    bucket_date = pd.DataFrame(rows)
    if bucket_date.empty:
        return {"quantiles": quantiles, "bucket_returns": {}, "spreads": {}, "observations": 0}
    pivot = bucket_date.pivot(index="date", columns="bucket", values="mean_return")
    bucket_returns = {f"Q{int(col)}": round(float(pivot[col].mean()), 10) for col in pivot.columns if pd.notna(pivot[col].mean())}
    spreads: dict[str, Any] = {}
    for long_q, short_q in ((3, 0), (4, 0), (4, 1), (3, 1)):
        if long_q in pivot.columns and short_q in pivot.columns:
            series = pivot[long_q] - pivot[short_q]
            series = series.dropna()
            spreads[f"Q{long_q}-Q{short_q}"] = {
                "spread_mean": round(float(series.mean()), 10) if len(series) else None,
                "positive_rate": round(float((series > 0).mean()), 4) if len(series) else None,
                "observations": int(len(series)),
            }
    best_pair = None
    if spreads:
        best_pair = max(spreads.items(), key=lambda kv: kv[1]["spread_mean"] if kv[1].get("spread_mean") is not None else -999)
    return {
        "quantiles": quantiles,
        "bucket_returns": bucket_returns,
        "spreads": spreads,
        "best_pair": {"pair": best_pair[0], **best_pair[1]} if best_pair else None,
        "observations": int(len(data)),
        "dates": int(data["date"].nunique()),
        "tickers": int(data["ticker"].nunique()),
    }


def ticker_concentration_diagnostics(
    df: pd.DataFrame,
    *,
    signal_col: str,
    return_col: str = "forward_return_5d",
    benchmark_spread: float = DEFAULT_BENCHMARK_SPREAD,
    quantiles: int = 5,
) -> dict[str, Any]:
    data = _to_numeric_frame(df.copy(), [signal_col, return_col])
    active = data.dropna(subset=["date", "ticker", signal_col, return_col])
    base = bucket_profile(active, signal_col=signal_col, return_col=return_col, quantiles=quantiles)
    q30 = (base.get("spreads") or {}).get("Q3-Q0", {})
    base_spread = q30.get("spread_mean")
    ticker_rows = active.groupby("ticker").agg(
        rows=(signal_col, "size"),
        dates=("date", "nunique"),
        mean_signal=(signal_col, "mean"),
        mean_return=(return_col, "mean"),
    ).reset_index()
    ticker_rows = ticker_rows.sort_values(["rows", "mean_return"], ascending=[False, False])
    total_rows = max(int(len(active)), 1)
    top_tickers = []
    for row in ticker_rows.head(10).to_dict(orient="records"):
        top_tickers.append({
            "ticker": str(row["ticker"]),
            "rows": int(row["rows"]),
            "row_share": round(float(row["rows"]) / total_rows, 4),
            "dates": int(row["dates"]),
            "mean_signal": round(float(row["mean_signal"]), 10) if pd.notna(row["mean_signal"]) else None,
            "mean_forward_return_5d": round(float(row["mean_return"]), 10) if pd.notna(row["mean_return"]) else None,
        })
    loo: list[dict[str, Any]] = []
    for ticker in ticker_rows["ticker"].head(20):
        sub = active[active["ticker"] != ticker]
        prof = bucket_profile(sub, signal_col=signal_col, return_col=return_col, quantiles=quantiles)
        spread = ((prof.get("spreads") or {}).get("Q3-Q0") or {}).get("spread_mean")
        loo.append({
            "excluded_ticker": str(ticker),
            "q3_q0_spread": spread,
            "above_benchmark": bool(spread is not None and spread > benchmark_spread),
        })
    min_loo = min((x["q3_q0_spread"] for x in loo if x["q3_q0_spread"] is not None), default=None)
    return {
        "active_rows": int(len(active)),
        "active_dates": int(active["date"].nunique()) if len(active) else 0,
        "active_tickers": int(active["ticker"].nunique()) if len(active) else 0,
        "nonnull_signal_rows": int(data[signal_col].notna().sum()) if signal_col in data.columns else 0,
        "nonnull_signal_tickers": int(data.dropna(subset=[signal_col])["ticker"].nunique()) if signal_col in data.columns and "ticker" in data.columns else 0,
        "base_q3_q0_spread": base_spread,
        "top_tickers_by_active_rows": top_tickers,
        "leave_one_ticker_out": loo,
        "min_leave_one_out_q3_q0_spread": min_loo,
        "all_leave_one_out_above_benchmark": bool(loo and all(x["above_benchmark"] for x in loo)),
        "profile": base,
    }


def signal_turnover_proxy(df: pd.DataFrame, *, signal_col: str) -> dict[str, Any]:
    if df.empty or not {"date", "ticker", signal_col}.issubset(df.columns):
        return {"mean_abs_diff": None, "mean_presence_flip_rate": None}
    data = df[["date", "ticker", signal_col]].copy()
    data["date_dt"] = pd.to_datetime(data["date"].astype(str), errors="coerce")
    data[signal_col] = pd.to_numeric(data[signal_col], errors="coerce")
    data = data.sort_values(["ticker", "date_dt"])
    data["signal_diff_abs"] = data.groupby("ticker")[signal_col].diff().abs()
    present = data[signal_col].notna().astype(int)
    data["presence_flip"] = present.groupby(data["ticker"]).diff().abs()
    return {
        "mean_abs_diff": round(float(data["signal_diff_abs"].mean()), 10) if data["signal_diff_abs"].notna().any() else None,
        "median_abs_diff": round(float(data["signal_diff_abs"].median()), 10) if data["signal_diff_abs"].notna().any() else None,
        "mean_presence_flip_rate": round(float(data["presence_flip"].mean()), 10) if data["presence_flip"].notna().any() else None,
    }


def load_json_if_exists(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_failure_diagnosis(
    dataset: pd.DataFrame,
    *,
    signal_col: str = "high_express_diluted_roe_yoy",
    benchmark_spread: float = DEFAULT_BENCHMARK_SPREAD,
    summary: dict[str, Any] | None = None,
    rolling_results: list[dict[str, Any]] | None = None,
    split_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    concentration = ticker_concentration_diagnostics(dataset, signal_col=signal_col, benchmark_spread=benchmark_spread)
    turnover = signal_turnover_proxy(dataset, signal_col=signal_col)
    rolling = rolling_results or []
    split = split_results or []
    rolling_pass_count = sum(1 for r in rolling if r.get("pass_gate") is True)
    split_pass_count = sum(1 for r in split if r.get("pass_gate") is True)
    q30 = ((concentration.get("profile") or {}).get("spreads") or {}).get("Q3-Q0", {})
    best_pair = (concentration.get("profile") or {}).get("best_pair") or {}
    reasons: list[str] = []
    if concentration["active_tickers"] < 20:
        reasons.append("controlled_overlay_active_tickers_too_few")
    if concentration["nonnull_signal_tickers"] <= 10:
        reasons.append("signal_coverage_depends_on_few_tickers")
    if not concentration["all_leave_one_out_above_benchmark"]:
        reasons.append("leave_one_ticker_out_not_stably_above_benchmark")
    if q30.get("spread_mean") is None or q30.get("spread_mean") <= benchmark_spread:
        reasons.append("diagnostic_q3_q0_not_above_benchmark")
    if best_pair.get("pair") and best_pair.get("pair") != "Q3-Q0":
        reasons.append("best_bucket_pair_not_policy_pair")
    if rolling and rolling_pass_count == 0:
        reasons.append("rolling_workflow_all_failed_standard_gate")
    if split and split_pass_count == 0:
        reasons.append("split_workflow_all_failed_standard_gate")
    standard = ((summary or {}).get("controlled_workflow") or {}).get("standard_result") or {}
    if standard.get("pass_gate") is False:
        reasons.append(f"standard_gate_failed_{standard.get('fail_reason') or 'unknown'}")
    decision = "stop_earnings_event_not_robust" if reasons else "continue_readonly_bounded_universe_revalidation_plan_only"
    return {
        "schema_version": 1,
        "scope": "earnings_event_readonly_failure_diagnosis_no_enqueue",
        "signal_col": signal_col,
        "benchmark_spread": benchmark_spread,
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_workflow_run": True,
        "coverage": {
            "dataset_rows": int(len(dataset)),
            "dataset_dates": int(dataset["date"].nunique()) if "date" in dataset.columns else 0,
            "dataset_tickers": int(dataset["ticker"].nunique()) if "ticker" in dataset.columns else 0,
            "nonnull_signal_rows": concentration["nonnull_signal_rows"],
            "nonnull_signal_tickers": concentration["nonnull_signal_tickers"],
            "active_rows": concentration["active_rows"],
            "active_dates": concentration["active_dates"],
            "active_tickers": concentration["active_tickers"],
        },
        "bucket_profile": concentration["profile"],
        "ticker_concentration": {k: v for k, v in concentration.items() if k != "profile"},
        "turnover_proxy": turnover,
        "workflow_instability": {
            "rolling_count": len(rolling),
            "rolling_pass_count": rolling_pass_count,
            "split_count": len(split),
            "split_pass_count": split_pass_count,
            "standard_pass_gate": standard.get("pass_gate"),
            "standard_fail_reason": standard.get("fail_reason"),
            "standard_sharpe_net": standard.get("sharpe_net"),
        },
        "decision": {"decision": decision, "reasons": reasons},
    }
