from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.pit_value_trap_attribution import _bucket_spread_for_field, _rank_ic_summary

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET = ROOT / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation" / "dataset.csv"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_value_trap_field_fix"
DEFAULT_FIELDS = ["operating_cashflow_to_profit", "debt_to_assets", "netprofit_yoy", "tr_yoy"]
RETURN_FIELD = "forward_return_5d"


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _fill_date_industry_median(df: pd.DataFrame, field: str) -> pd.Series:
    s = _numeric(df[field])
    keys = [df["date"]]
    if "industry" in df.columns:
        keys.append(df["industry"].fillna("UNKNOWN"))
    med = s.groupby(keys).transform("median")
    date_med = s.groupby(df["date"]).transform("median")
    return s.fillna(med).fillna(date_med).fillna(s.median())


def _fill_date_median(df: pd.DataFrame, field: str) -> pd.Series:
    s = _numeric(df[field])
    return s.fillna(s.groupby(df["date"]).transform("median")).fillna(s.median())


def _high_coverage_universe(df: pd.DataFrame, field: str, min_coverage: float = 0.60) -> pd.DataFrame:
    if "ticker" not in df.columns:
        return df.iloc[0:0].copy()
    cov = _numeric(df[field]).notna().groupby(df["ticker"]).mean()
    keep = set(cov[cov >= min_coverage].index)
    return df[df["ticker"].isin(keep)].copy()


def _evaluate_variant(df: pd.DataFrame, field: str, variant: str) -> dict[str, Any]:
    work = df[["date", RETURN_FIELD]].copy()
    if variant == "dropna":
        work[field] = _numeric(df[field])
    elif variant == "date_industry_median_fill":
        work[field] = _fill_date_industry_median(df, field)
    elif variant == "date_median_fill":
        work[field] = _fill_date_median(df, field)
    elif variant == "missing_penalty_flag":
        values = _numeric(df[field])
        work[field] = values.fillna(_fill_date_industry_median(df, field)) - values.isna().astype(float)
    elif variant == "high_coverage_universe_only":
        reduced = _high_coverage_universe(df, field)
        if reduced.empty:
            return {"variant": variant, "rows": 0, "coverage": 0.0, "rank_ic_mean": None, "top_bottom_spread_mean": None}
        work = reduced[["date", RETURN_FIELD]].copy()
        work[field] = _numeric(reduced[field])
    else:
        raise ValueError(f"unknown variant: {variant}")
    ic = _rank_ic_summary(work, field, RETURN_FIELD)
    bucket = _bucket_spread_for_field(work, field, RETURN_FIELD)
    return {
        "variant": variant,
        "rows": int(len(work)),
        "coverage": float(work[field].notna().mean()) if len(work) else 0.0,
        "rank_ic_mean": ic.get("rank_ic_mean"),
        "rank_ic_ir": ic.get("rank_ic_ir"),
        "ic_observations": ic.get("observations"),
        "top_bottom_spread_mean": bucket.get("top_bottom_spread_mean"),
        "bucket_observations": bucket.get("observations"),
    }


def _fragility(variants: list[dict[str, Any]]) -> str:
    ic_values = [v.get("rank_ic_mean") for v in variants if v.get("rank_ic_mean") is not None]
    if not ic_values:
        return "unusable"
    positives = sum(float(v) > 0.005 for v in ic_values)
    negatives = sum(float(v) < -0.005 for v in ic_values)
    if positives and negatives:
        return "direction_changes_by_missing_treatment"
    if positives == 1:
        return "single_treatment_positive_only"
    return "stable_or_consistently_weak"


def build_missing_value_diagnostics(df: pd.DataFrame, fields: list[str] | None = None) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows = []
    treatments = ["dropna", "date_industry_median_fill", "date_median_fill", "missing_penalty_flag", "high_coverage_universe_only"]
    for field in fields:
        if field not in df.columns:
            rows.append({"field": field, "exists": False, "variants": [], "fragility": "missing_field"})
            continue
        variants = [_evaluate_variant(df, field, t) for t in treatments]
        rows.append({"field": field, "exists": True, "variants": variants, "fragility": _fragility(variants)})
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def diagnostics_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Missing Value Diagnostics", "", f"Generated: {payload.get('generated_at_utc')}", ""]
    for row in payload.get("fields", []):
        lines.append(f"## {row.get('field')}")
        lines.append(f"Fragility: {row.get('fragility')}")
        for v in row.get("variants", []):
            lines.append(f"- {v['variant']}: coverage={v['coverage']}, ic={v['rank_ic_mean']}, spread={v['top_bottom_spread_mean']}")
        lines.append("")
    return "\n".join(lines)


def write_missing_value_diagnostics(dataset_path: str | Path = DEFAULT_DATASET, output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(dataset_path)
    payload = build_missing_value_diagnostics(df)
    (output_dir / "missing_value_diagnostics.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "missing_value_diagnostics.md").write_text(diagnostics_to_markdown(payload), encoding="utf-8")
    return payload
