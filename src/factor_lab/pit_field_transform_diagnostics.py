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


def _winsorize_by_date_industry(df: pd.DataFrame, field: str, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    s = _numeric(df[field])
    keys = [df["date"]]
    if "industry" in df.columns:
        keys.append(df["industry"].fillna("UNKNOWN"))
    def clip_group(g: pd.Series) -> pd.Series:
        clean = g.dropna()
        if clean.empty:
            return g
        lo, hi = clean.quantile([lower, upper])
        return g.clip(lo, hi)
    return s.groupby(keys).transform(clip_group)


def _zscore_by_date_industry(df: pd.DataFrame, values: pd.Series) -> pd.Series:
    keys = [df["date"]]
    if "industry" in df.columns:
        keys.append(df["industry"].fillna("UNKNOWN"))
    def z(g: pd.Series) -> pd.Series:
        std = g.std(ddof=0)
        if not std or pd.isna(std):
            return g * 0
        return (g - g.mean()) / std
    return values.groupby(keys).transform(z)


def _variant_series(df: pd.DataFrame, field: str, variant: str) -> pd.Series:
    raw = _numeric(df[field])
    if variant == "raw":
        return raw
    if variant == "reversed":
        return -raw
    if variant == "winsorized_zscore":
        return _zscore_by_date_industry(df, _winsorize_by_date_industry(df, field))
    if variant == "reversed_winsorized_zscore":
        return -_zscore_by_date_industry(df, _winsorize_by_date_industry(df, field))
    raise ValueError(f"unknown variant: {variant}")


def build_field_transform_diagnostics(df: pd.DataFrame, fields: list[str] | None = None) -> dict[str, Any]:
    fields = fields or DEFAULT_FIELDS
    rows: list[dict[str, Any]] = []
    for field in fields:
        if field not in df.columns:
            rows.append({"field": field, "exists": False, "variants": []})
            continue
        variants = []
        for variant in ["raw", "reversed", "winsorized_zscore", "reversed_winsorized_zscore"]:
            tmp = df[["date", RETURN_FIELD]].copy()
            tmp[field] = _variant_series(df, field, variant)
            ic = _rank_ic_summary(tmp, field, RETURN_FIELD)
            bucket = _bucket_spread_for_field(tmp, field, RETURN_FIELD)
            coverage = float(tmp[field].notna().mean()) if len(tmp) else 0.0
            variants.append({
                "variant": variant,
                "coverage": coverage,
                "rank_ic_mean": ic.get("rank_ic_mean"),
                "rank_ic_ir": ic.get("rank_ic_ir"),
                "ic_observations": ic.get("observations"),
                "top_bottom_spread_mean": bucket.get("top_bottom_spread_mean"),
                "bottom_top_spread_mean": bucket.get("bottom_top_spread_mean"),
                "bucket_observations": bucket.get("observations"),
            })
        best = max(
            variants,
            key=lambda r: (-999 if r.get("rank_ic_mean") is None else float(r["rank_ic_mean"])),
        )
        rows.append({"field": field, "exists": True, "best_variant_by_ic": best["variant"], "variants": variants})
    return {"schema_version": 1, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "fields": rows}


def diagnostics_to_markdown(payload: dict[str, Any]) -> str:
    lines = ["# PIT Field Transform Diagnostics", "", f"Generated: {payload.get('generated_at_utc')}", ""]
    for row in payload.get("fields", []):
        lines.append(f"## {row.get('field')}")
        lines.append(f"Best variant by IC: {row.get('best_variant_by_ic')}")
        for v in row.get("variants", []):
            lines.append(f"- {v['variant']}: coverage={v['coverage']}, ic={v['rank_ic_mean']}, spread={v['top_bottom_spread_mean']}")
        lines.append("")
    return "\n".join(lines)


def write_field_transform_diagnostics(dataset_path: str | Path = DEFAULT_DATASET, output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(dataset_path)
    payload = build_field_transform_diagnostics(df)
    (output_dir / "field_transform_diagnostics.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "field_transform_diagnostics.md").write_text(diagnostics_to_markdown(payload), encoding="utf-8")
    return payload
