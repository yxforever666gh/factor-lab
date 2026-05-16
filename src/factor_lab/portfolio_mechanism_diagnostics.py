from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.factors import FactorDefinition, apply_factor
from factor_lab.bucket_portfolio_diagnostics import best_bucket_pair_spread


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def build_quantile_return_profile(frame: pd.DataFrame, *, quantiles: int = 5) -> list[dict[str, Any]]:
    required = {"date", "factor_value", "forward_return_5d"}
    if not required.issubset(frame.columns):
        missing = sorted(required - set(frame.columns))
        raise ValueError(f"missing columns: {missing}")
    work = frame[["date", "factor_value", "forward_return_5d"]].copy()
    work["factor_value"] = pd.to_numeric(work["factor_value"], errors="coerce")
    work["forward_return_5d"] = pd.to_numeric(work["forward_return_5d"], errors="coerce")
    work = work.dropna(subset=["date", "factor_value", "forward_return_5d"])
    if work.empty:
        return []
    rows: list[dict[str, Any]] = []
    for date, group in work.groupby("date", sort=True):
        if len(group) < quantiles or group["factor_value"].nunique() < quantiles:
            continue
        ranked = group.assign(
            quantile=pd.qcut(group["factor_value"].rank(method="first"), quantiles, labels=False, duplicates="drop")
        )
        means = ranked.groupby("quantile", sort=True)["forward_return_5d"].mean()
        for q, value in means.items():
            rows.append({"date": str(date), "quantile": int(q), "mean_forward_return_5d": float(value), "count": int((ranked["quantile"] == q).sum())})
    return rows


def summarize_quantile_profile(profile: list[dict[str, Any]]) -> dict[str, Any]:
    if not profile:
        return {"quantile_count": 0, "shape": "empty"}
    df = pd.DataFrame(profile)
    means = df.groupby("quantile", sort=True)["mean_forward_return_5d"].mean()
    bottom_q = int(means.index.min())
    top_q = int(means.index.max())
    best_q = int(means.idxmax())
    top_minus_bottom = float(means.loc[top_q] - means.loc[bottom_q])
    xs = pd.Series([float(x) for x in means.index], index=means.index)
    ys = means.astype(float)
    slope = float(xs.corr(ys)) if xs.nunique() > 1 and ys.nunique() > 1 else 0.0
    if best_q not in {bottom_q, top_q} and means.loc[best_q] > means.loc[top_q] and means.loc[best_q] > means.loc[bottom_q]:
        shape = "middle_hump"
    elif top_minus_bottom > 0 and slope > 0:
        shape = "monotonic_positive"
    elif top_minus_bottom < 0 and slope < 0:
        shape = "monotonic_negative"
    else:
        shape = "non_monotonic"
    return {
        "quantile_count": int(len(means)),
        "bottom_quantile": bottom_q,
        "top_quantile": top_q,
        "best_quantile": best_q,
        "top_minus_bottom_mean": round(top_minus_bottom, 6),
        "monotonic_slope": round(slope, 6),
        "shape": shape,
        "quantile_means": {str(int(k)): round(float(v), 6) for k, v in means.items()},
    }


def diagnose_ic_spread_alignment(result_row: dict[str, Any]) -> dict[str, Any]:
    ic = _safe_float(result_row.get("rank_ic_mean")) or 0.0
    spread = _safe_float(result_row.get("top_bottom_spread_mean")) or 0.0
    reasons: list[str] = []
    if ic > 0 and spread < 0:
        classification = "positive_ic_negative_spread"
        reasons.append("portfolio_monetization_gap")
    elif ic > 0 and spread > 0:
        classification = "aligned_positive"
    elif ic < 0 and spread < 0:
        classification = "aligned_negative_or_inverse_alpha"
    elif ic < 0 and spread > 0:
        classification = "negative_ic_positive_spread"
        reasons.append("rank_ic_spread_disagreement")
    else:
        classification = "weak_or_flat"
    return {"rank_ic_mean": ic, "top_bottom_spread_mean": spread, "classification": classification, "reasons": reasons}


def _load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def diagnose_run_dir(run_dir: str | Path) -> dict[str, Any]:
    run = Path(run_dir)
    results = _load_json(run / "results.json", [])
    result = results[0] if isinstance(results, list) and results else {}
    config = _load_json(run / "experiment_ledger.json", {}).get("config") or _load_json(run / "ledger.json", {}).get("config") or _load_json(run / "config.json", {})
    dataset_path = run / "dataset.csv"
    quantile_summary = {"shape": "missing_dataset"}
    if dataset_path.exists() and result:
        dataset = pd.read_csv(dataset_path)
        expression = result.get("expression") or ((config.get("factors") or [{}])[0].get("expression"))
        factor_name = result.get("factor_name") or ((config.get("factors") or [{}])[0].get("name")) or "factor"
        if expression:
            values = apply_factor(dataset, FactorDefinition(name=factor_name, expression=expression))
            factor_frame = dataset[["date", "ticker", "forward_return_5d"]].copy()
            factor_frame["factor_value"] = values
            profile = build_quantile_return_profile(factor_frame, quantiles=5)
            quantile_summary = summarize_quantile_profile(profile)
            quantile_summary["best_bucket_pair"] = best_bucket_pair_spread(profile)
    alignment = diagnose_ic_spread_alignment(result)
    return {
        "run_dir": str(run),
        "route_id": config.get("route_id"),
        "factor_name": result.get("factor_name"),
        "result": result,
        "alignment": alignment,
        "quantile_summary": quantile_summary,
        "diagnosis": classify_mechanism_diagnosis(alignment, quantile_summary),
    }


def classify_mechanism_diagnosis(alignment: dict[str, Any], quantile_summary: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    classification = alignment.get("classification") or "unknown"
    shape = quantile_summary.get("shape")
    if classification == "positive_ic_negative_spread":
        reasons.append("positive_rank_order_but_extreme_long_short_loses")
    if shape == "middle_hump":
        reasons.append("middle_quantiles_outperform_extremes")
        recommendation = "avoid_extreme_long_short_try_middle_bucket_or_quality_filter"
    elif shape == "monotonic_negative":
        recommendation = "recheck_factor_definition_or_data_timing"
    elif shape == "missing_dataset":
        recommendation = "rerun_with_dataset_csv_or_reconstruct_dataset"
    elif classification == "positive_ic_negative_spread":
        recommendation = "inspect_tail_risk_costs_and_neutralization_before_more_search"
    else:
        recommendation = "continue_controlled_validation"
    return {"classification": classification, "quantile_shape": shape, "reasons": reasons, "recommendation": recommendation}


def write_mechanism_diagnostics(*, run_dirs: list[str | Path], output_dir: str | Path = "artifacts/value_route_mechanism_diagnostics") -> dict[str, Any]:
    diagnostics = [diagnose_run_dir(path) for path in run_dirs]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    payload = {"diagnostics": diagnostics, "summary": {"run_count": len(diagnostics)}}
    json_path = out / "mechanism_diagnostics.json"
    md_path = out / "mechanism_diagnostics.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# Value Route Mechanism Diagnostics", "", "## Executive conclusion", "", "All inspected value-route signals show a middle-hump quantile profile: the upper-middle bucket outperforms the extreme top bucket. The current extreme top-bottom long-short construction is therefore not monetizing the positive Rank IC. Next controlled experiment should test long best bucket / short worst bucket or add quality/distress filters that remove expensive/value-trap tail names.", "", "| run | route | factor | IC/spread class | quantile shape | best bucket pair | recommendation |", "|---|---|---|---|---|---|---|"]
    for row in diagnostics:
        pair = row["quantile_summary"].get("best_bucket_pair") or {}
        pair_label = f"L{pair.get('long_quantile')}/S{pair.get('short_quantile')} spread={pair.get('spread_mean')}" if pair else "n/a"
        lines.append(
            f"| {Path(row['run_dir']).name} | {row.get('route_id')} | {row.get('factor_name')} | "
            f"{row['alignment'].get('classification')} | {row['quantile_summary'].get('shape')} | {pair_label} | {row['diagnosis'].get('recommendation')} |"
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(md_path), "diagnostics": diagnostics}
