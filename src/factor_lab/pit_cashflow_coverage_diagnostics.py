from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET = ROOT / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation" / "dataset.csv"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_value_trap_field_fix"
CASHFLOW_FIELD = "operating_cashflow_to_profit"
RETURN_FIELD = "forward_return_5d"


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _coverage(series: pd.Series) -> float:
    if len(series) == 0:
        return 0.0
    return float(_numeric(series).notna().mean())


def _category_for_row(row: pd.Series) -> str:
    value = pd.to_numeric(pd.Series([row.get(CASHFLOW_FIELD)]), errors="coerce").iloc[0]
    if pd.notna(value):
        return "available"
    pit_validated = row.get("pit_feature_validated")
    if isinstance(pit_validated, str):
        pit_validated = pit_validated.lower() == "true"
    blocked = str(row.get("pit_feature_blocked_reason") or "")
    warnings = str(row.get("pit_feature_warnings") or "")
    if "zero_denominator" in blocked or "zero_denominator" in warnings:
        return "zero_or_invalid_net_profit"
    if pit_validated is False:
        return "PIT_asof_blocked"
    if pd.isna(row.get("pit_source_ann_date")) or pd.isna(row.get("pit_source_end_date")):
        return "PIT_asof_blocked"
    # Dataset currently does not retain the raw cashflow statement numerator/denominator.
    # If profit indicators exist but cashflow ratio is missing, the most likely source is
    # the cashflow statement numerator missing/not merged rather than the PIT shell itself.
    profit_like = ["netprofit_yoy", "profit_yoy", "roe"]
    if any(col in row.index and pd.notna(pd.to_numeric(pd.Series([row.get(col)]), errors="coerce").iloc[0]) for col in profit_like):
        return "missing_cashflow_statement_or_numerator"
    return "merge_key_or_mapping_gap"


def _coverage_by_group(df: pd.DataFrame, group_field: str, value_field: str = CASHFLOW_FIELD) -> list[dict[str, Any]]:
    if group_field not in df.columns or value_field not in df.columns:
        return []
    rows = []
    for key, group in df.groupby(df[group_field].fillna("UNKNOWN")):
        rows.append({
            group_field: str(key),
            "rows": int(len(group)),
            "coverage": _coverage(group[value_field]),
            "non_null_rows": int(_numeric(group[value_field]).notna().sum()),
        })
    return sorted(rows, key=lambda x: (x["coverage"], -x["rows"]))


def build_cashflow_coverage_diagnostics(df: pd.DataFrame) -> dict[str, Any]:
    work = df.copy()
    if "date" in work.columns:
        work["year"] = pd.to_datetime(work["date"], errors="coerce").dt.year.astype("Int64").astype(str).replace("<NA>", "UNKNOWN")
    else:
        work["year"] = "UNKNOWN"
    if CASHFLOW_FIELD not in work.columns:
        work[CASHFLOW_FIELD] = pd.NA
    work["cashflow_missing_category"] = work.apply(_category_for_row, axis=1)
    category_counts = work["cashflow_missing_category"].value_counts(dropna=False).to_dict()
    category_rows = [
        {"category": str(k), "rows": int(v), "share": float(v / len(work)) if len(work) else 0.0}
        for k, v in category_counts.items()
    ]
    overall_coverage = _coverage(work[CASHFLOW_FIELD])
    non_null = int(_numeric(work[CASHFLOW_FIELD]).notna().sum())
    year_rows = _coverage_by_group(work, "year")
    industry_rows = _coverage_by_group(work, "industry")[:20]
    ticker_rows = _coverage_by_group(work, "ticker")[:20]
    diagnosis = "coverage_ok"
    if overall_coverage < 0.30:
        if category_counts.get("missing_cashflow_statement_or_numerator", 0) > category_counts.get("PIT_asof_blocked", 0):
            diagnosis = "cashflow_statement_or_numerator_gap"
        elif category_counts.get("PIT_asof_blocked", 0) > 0:
            diagnosis = "pit_asof_or_announcement_gap"
        else:
            diagnosis = "mapping_or_formula_gap"
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(work)),
        "field": CASHFLOW_FIELD,
        "coverage": overall_coverage,
        "non_null_rows": non_null,
        "missing_rows": int(len(work) - non_null),
        "category_breakdown": category_rows,
        "coverage_by_year": year_rows,
        "lowest_industry_coverage": industry_rows,
        "lowest_ticker_coverage": ticker_rows,
        "diagnosis": diagnosis,
        "limitations": [
            "dataset.csv does not retain raw cashflow numerator or net-profit denominator columns, so numerator/denominator attribution is inferred from PIT metadata and profit-like fields",
        ],
        "hard_stop_triggered": overall_coverage < 0.30,
    }


def diagnostics_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# PIT Cashflow Coverage Diagnostics",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Rows: {payload.get('row_count')}",
        f"Field: {payload.get('field')}",
        f"Coverage: {payload.get('coverage')}",
        f"Diagnosis: {payload.get('diagnosis')}",
        f"Hard stop triggered: {payload.get('hard_stop_triggered')}",
        "",
        "## Missing category breakdown",
    ]
    for row in payload.get("category_breakdown", []):
        lines.append(f"- {row['category']}: rows={row['rows']}, share={row['share']}")
    lines += ["", "## Lowest industry coverage"]
    for row in payload.get("lowest_industry_coverage", [])[:10]:
        lines.append(f"- {row.get('industry')}: coverage={row.get('coverage')}, rows={row.get('rows')}")
    lines += ["", "## Limitations"]
    for item in payload.get("limitations", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_cashflow_coverage_diagnostics(dataset_path: str | Path = DEFAULT_DATASET, output_dir: str | Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(dataset_path)
    payload = build_cashflow_coverage_diagnostics(df)
    (output_dir / "cashflow_coverage_diagnostics.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output_dir / "cashflow_coverage_diagnostics.md").write_text(diagnostics_to_markdown(payload), encoding="utf-8")
    return payload
