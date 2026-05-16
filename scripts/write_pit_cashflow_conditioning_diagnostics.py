#!/usr/bin/env python3
"""Write read-only PIT cashflow conditioning diagnostics around value-quality."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


VALUE_DATASET = Path("artifacts/value_route_bucket_aware/runs/value_quality_no_distress_bucket_aware/dataset.csv")
CASHFLOW_DATASET = Path("artifacts/pit_cashflow_post_denominator_probe/run_cashflow_only_denominator_repaired/dataset.csv")
OUTPUT_DIR = Path("artifacts/pit_cashflow_conditioning")
BENCHMARK_SPREAD = 0.006225


def _safe_quantile_by_date(frame: pd.DataFrame, column: str, q: int = 5) -> pd.Series:
    def assign(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        mask = s.notna()
        out = pd.Series(np.nan, index=s.index)
        if mask.sum() < q:
            return out
        try:
            out.loc[mask] = pd.qcut(s.loc[mask], q, labels=False, duplicates="drop")
        except ValueError:
            return out
        return out

    return frame.groupby("date", group_keys=False)[column].apply(assign)


def _date_industry_zscore(frame: pd.DataFrame, column: str) -> pd.Series:
    def z(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        std = s.std(ddof=0)
        if not np.isfinite(std) or std == 0:
            return pd.Series(np.nan, index=s.index)
        return (s - s.mean()) / std

    return frame.groupby(["date", "industry"], group_keys=False)[column].apply(z)


def _winsorize_by_date(frame: pd.DataFrame, column: str, low: float = 0.01, high: float = 0.99) -> pd.Series:
    def w(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        if s.notna().sum() < 5:
            return s
        return s.clip(s.quantile(low), s.quantile(high))

    return frame.groupby("date", group_keys=False)[column].apply(w)


def _bucket_spread(frame: pd.DataFrame, score_col: str, long_q: int = 3, short_q: int = 0, q: int = 5) -> dict:
    tmp = frame[["date", score_col, "forward_return_5d"]].copy()
    tmp[score_col] = pd.to_numeric(tmp[score_col], errors="coerce")
    tmp["forward_return_5d"] = pd.to_numeric(tmp["forward_return_5d"], errors="coerce")
    tmp = tmp.dropna()
    tmp["bucket"] = _safe_quantile_by_date(tmp, score_col, q=q)
    spreads = []
    obs_dates = 0
    long_obs = 0
    short_obs = 0
    for _, g in tmp.dropna(subset=["bucket"]).groupby("date"):
        long = g.loc[g["bucket"] == long_q, "forward_return_5d"]
        short = g.loc[g["bucket"] == short_q, "forward_return_5d"]
        if len(long) == 0 or len(short) == 0:
            continue
        obs_dates += 1
        long_obs += int(len(long))
        short_obs += int(len(short))
        spreads.append(float(long.mean() - short.mean()))
    arr = np.array(spreads, dtype=float)
    return {
        "spread_mean": float(np.nanmean(arr)) if len(arr) else None,
        "spread_std": float(np.nanstd(arr, ddof=0)) if len(arr) else None,
        "observations": int(len(arr)),
        "long_obs": int(long_obs),
        "short_obs": int(short_obs),
        "long_quantile": long_q,
        "short_quantile": short_q,
    }


def _subset_value_bucket_spread(frame: pd.DataFrame, subset_col: str, allowed_values: set[int]) -> dict:
    tmp = frame.copy()
    tmp["value_bucket"] = _safe_quantile_by_date(tmp, "value_quality_score", q=5)
    tmp["cashflow_bucket"] = _safe_quantile_by_date(tmp, subset_col, q=5)
    tmp = tmp.dropna(subset=["value_bucket", "cashflow_bucket", "forward_return_5d"])
    spreads = []
    long_obs = 0
    short_obs = 0
    obs_dates = 0
    for _, g in tmp.groupby("date"):
        long = g[(g["value_bucket"] == 3) & (g["cashflow_bucket"].astype(int).isin(allowed_values))]["forward_return_5d"]
        short = g[g["value_bucket"] == 0]["forward_return_5d"]
        if len(long) == 0 or len(short) == 0:
            continue
        obs_dates += 1
        long_obs += int(len(long))
        short_obs += int(len(short))
        spreads.append(float(long.mean() - short.mean()))
    arr = np.array(spreads, dtype=float)
    return {
        "spread_mean": float(np.nanmean(arr)) if len(arr) else None,
        "spread_std": float(np.nanstd(arr, ddof=0)) if len(arr) else None,
        "observations": int(len(arr)),
        "long_obs": int(long_obs),
        "short_obs": int(short_obs),
        "cashflow_long_bucket_filter": sorted(int(x) for x in allowed_values),
    }


def _cashflow_within_value_bucket(frame: pd.DataFrame, value_bucket: int = 3) -> dict:
    tmp = frame.copy()
    tmp["value_bucket"] = _safe_quantile_by_date(tmp, "value_quality_score", q=5)
    tmp = tmp[tmp["value_bucket"] == value_bucket].copy()
    tmp["cashflow_bucket"] = _safe_quantile_by_date(tmp, "cashflow", q=5)
    rows = []
    for b, g in tmp.dropna(subset=["cashflow_bucket", "forward_return_5d"]).groupby("cashflow_bucket"):
        rows.append({
            "cashflow_bucket": int(b),
            "mean_forward_return_5d": float(g["forward_return_5d"].mean()),
            "count": int(len(g)),
        })
    rows = sorted(rows, key=lambda x: x["cashflow_bucket"])
    q4 = next((r for r in rows if r["cashflow_bucket"] == 4), None)
    q0 = next((r for r in rows if r["cashflow_bucket"] == 0), None)
    spread = None if not q4 or not q0 else float(q4["mean_forward_return_5d"] - q0["mean_forward_return_5d"])
    return {"value_bucket": value_bucket, "bucket_returns": rows, "q4_minus_q0_mean_return": spread}


def build_report() -> dict:
    value = pd.read_csv(VALUE_DATASET)
    cash = pd.read_csv(CASHFLOW_DATASET, usecols=["date", "ticker", "operating_cashflow_to_profit"])
    value["date"] = value["date"].astype(str)
    cash["date"] = cash["date"].astype(str)
    cash = cash.rename(columns={"operating_cashflow_to_profit": "cashflow_repaired"})
    merged = value.merge(cash, on=["date", "ticker"], how="left", validate="one_to_one")
    merged["cashflow"] = pd.to_numeric(merged["cashflow_repaired"], errors="coerce")
    merged["value_quality_score"] = pd.to_numeric(merged["industry_relative_book_yield"], errors="coerce") + pd.to_numeric(merged["roe"], errors="coerce")
    merged["forward_return_5d"] = pd.to_numeric(merged["forward_return_5d"], errors="coerce")

    merged["cashflow_winsor"] = _winsorize_by_date(merged, "cashflow")
    merged["cashflow_z_date_industry"] = _date_industry_zscore(merged, "cashflow_winsor")
    merged["value_plus_cashflow_z"] = merged["value_quality_score"] + merged["cashflow_z_date_industry"].fillna(0.0)

    baseline = _bucket_spread(merged, "value_quality_score", 3, 0)
    cashflow_confirm = _bucket_spread(merged.dropna(subset=["cashflow_z_date_industry"]), "value_plus_cashflow_z", 3, 0)
    high_cash_filter = _subset_value_bucket_spread(merged, "cashflow", {2, 3, 4})
    top_cash_filter = _subset_value_bucket_spread(merged, "cashflow", {3, 4})
    exclude_bottom_cash = _subset_value_bucket_spread(merged, "cashflow", {1, 2, 3, 4})
    value_q3_cashflow_shape = _cashflow_within_value_bucket(merged, 3)

    candidate_results = {
        "baseline_value_quality_q3_q0": baseline,
        "value_plus_cashflow_z_q3_q0": cashflow_confirm,
        "value_q3_exclude_cashflow_bottom_q0": exclude_bottom_cash,
        "value_q3_cashflow_top_60pct_vs_value_q0": high_cash_filter,
        "value_q3_cashflow_top_40pct_vs_value_q0": top_cash_filter,
    }
    improvements = {}
    base = baseline.get("spread_mean")
    for name, result in candidate_results.items():
        val = result.get("spread_mean")
        improvements[name] = None if val is None or base is None else float(val - base)

    best_name = max(
        [k for k, v in candidate_results.items() if v.get("spread_mean") is not None],
        key=lambda k: candidate_results[k]["spread_mean"],
    )
    best_spread = candidate_results[best_name]["spread_mean"]
    beats_benchmark = best_spread is not None and best_spread > BENCHMARK_SPREAD
    improves_baseline = improvements.get(best_name) is not None and improvements[best_name] > 0
    decision = "stop_cashflow_conditioning_non_incremental"
    if beats_benchmark and improves_baseline:
        decision = "eligible_for_single_controlled_conditioning_probe"
    elif improves_baseline:
        decision = "diagnostic_positive_but_below_benchmark_no_workflow"

    return {
        "schema_version": 1,
        "value_dataset": str(VALUE_DATASET),
        "cashflow_dataset": str(CASHFLOW_DATASET),
        "row_count": int(len(merged)),
        "merged_cashflow_coverage": float(merged["cashflow"].notna().mean()),
        "benchmark_value_quality_spread": BENCHMARK_SPREAD,
        "candidate_results": candidate_results,
        "improvements_vs_recomputed_baseline": improvements,
        "value_q3_cashflow_shape": value_q3_cashflow_shape,
        "best_candidate": best_name,
        "best_candidate_spread": float(best_spread) if best_spread is not None else None,
        "best_candidate_gap_vs_benchmark": None if best_spread is None else float(best_spread - BENCHMARK_SPREAD),
        "decision": decision,
        "reasons": _decision_reasons(decision, best_name, best_spread, improvements),
    }


def _decision_reasons(decision: str, best_name: str, best_spread: float | None, improvements: dict) -> list[str]:
    reasons = []
    if best_spread is None:
        return ["no_valid_candidate_spread"]
    if best_spread <= BENCHMARK_SPREAD:
        reasons.append("best_cashflow_conditioning_does_not_beat_value_quality_no_distress_benchmark")
    if improvements.get(best_name) is not None and improvements[best_name] <= 0:
        reasons.append("best_candidate_does_not_improve_recomputed_baseline")
    if decision == "diagnostic_positive_but_below_benchmark_no_workflow":
        reasons.append("conditioning_improves_local_baseline_but_not_enough_for_controlled_workflow")
    if not reasons:
        reasons.append("cashflow_conditioning_beats_benchmark_and_baseline")
    return reasons


def write_markdown(report: dict, path: Path) -> None:
    lines = [
        "# PIT cashflow conditioning diagnostics",
        "",
        f"Decision: `{report['decision']}`",
        "",
        f"Rows: `{report['row_count']}`",
        f"Merged cashflow coverage: `{report['merged_cashflow_coverage']:.4f}`",
        f"Benchmark value_quality_no_distress spread: `{report['benchmark_value_quality_spread']:.6f}`",
        "",
        "## Candidate results",
    ]
    for name, result in report["candidate_results"].items():
        spread = result.get("spread_mean")
        imp = report["improvements_vs_recomputed_baseline"].get(name)
        lines.extend([
            f"### {name}",
            f"- spread_mean: `{spread}`",
            f"- observations: `{result.get('observations')}`",
            f"- improvement_vs_recomputed_baseline: `{imp}`",
            "",
        ])
    lines.extend([
        "## Value Q3 cashflow shape",
        json.dumps(report["value_q3_cashflow_shape"], ensure_ascii=False, indent=2),
        "",
        "## Reasons",
    ])
    lines.extend([f"- {r}" for r in report["reasons"]])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = build_report()
    (OUTPUT_DIR / "cashflow_conditioning_diagnostics.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_markdown(report, OUTPUT_DIR / "cashflow_conditioning_diagnostics.md")
    print(json.dumps({
        "decision": report["decision"],
        "best_candidate": report["best_candidate"],
        "best_candidate_spread": report["best_candidate_spread"],
        "best_candidate_gap_vs_benchmark": report["best_candidate_gap_vs_benchmark"],
        "merged_cashflow_coverage": report["merged_cashflow_coverage"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
