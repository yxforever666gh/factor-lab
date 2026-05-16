from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATASET = ROOT / "artifacts" / "value_route_bucket_aware" / "daemon_runs" / "value_trap_filter_quality_confirmation" / "dataset.csv"
DEFAULT_CACHE_DIR = ROOT / "artifacts" / "tushare_cache"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_cashflow_source_audit"
CASHFLOW_FIELD = "operating_cashflow_to_profit"
RAW_NUMERATOR_CANDIDATES = [
    "pit_cashflow_numerator_raw",
    "cashflow__n_cashflow_act",
    "n_cashflow_act",
    "net_cash_flows_oper_act",
    "c_fr_sale_sg",
]
RAW_DENOMINATOR_CANDIDATES = [
    "pit_cashflow_denominator_raw",
    "cashflow__net_profit",
    "net_profit",
]


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _coverage(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns or len(df) == 0:
        return 0.0
    return float(_numeric(df[column]).notna().mean())


def expected_pit_financial_cache_path(dataset: pd.DataFrame, cache_dir: str | Path = DEFAULT_CACHE_DIR) -> Path | None:
    if dataset.empty or not {"ticker", "date"}.issubset(dataset.columns):
        return None
    tickers = sorted(str(t) for t in dataset["ticker"].dropna().unique())
    if not tickers:
        return None
    dates = pd.to_datetime(dataset["date"], errors="coerce")
    if dates.dropna().empty:
        return None
    digest = hashlib.sha1(",".join(tickers).encode("utf-8")).hexdigest()[:12]
    start = dates.min().strftime("%Y-%m-%d")
    end = dates.max().strftime("%Y-%m-%d")
    return Path(cache_dir) / f"pit_financial_{start}_{end}_{len(tickers)}_{digest}_v2.csv"


def _read_csv_if_exists(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _first_present(columns: list[str], candidates: list[str]) -> str | None:
    for name in candidates:
        if name in columns:
            return name
    return None


def _cell(row: pd.Series, name: str | None) -> Any:
    if not name:
        return pd.NA
    value = row.get(name)
    if isinstance(value, pd.Series):
        non_null = value.dropna()
        return non_null.iloc[0] if not non_null.empty else pd.NA
    return value


def _missing_category(row: pd.Series, *, numerator_col: str | None, denominator_col: str | None) -> str:
    value = pd.to_numeric(pd.Series([_cell(row, CASHFLOW_FIELD)]), errors="coerce").iloc[0]
    if pd.notna(value):
        return "available"
    pit_validated = _cell(row, "pit_feature_validated")
    if isinstance(pit_validated, str):
        pit_validated = pit_validated.lower() == "true"
    if pit_validated is False:
        return "PIT_ann_date_blocked"
    if pd.isna(_cell(row, "pit_source_ann_date")) or pd.isna(_cell(row, "pit_source_end_date")):
        return "merge_key_mismatch"
    if numerator_col is None and denominator_col is None:
        return "output_column_dropped_before_dataset"
    numerator = pd.to_numeric(pd.Series([_cell(row, numerator_col)]), errors="coerce").iloc[0] if numerator_col else pd.NA
    denominator = pd.to_numeric(pd.Series([_cell(row, denominator_col)]), errors="coerce").iloc[0] if denominator_col else pd.NA
    if pd.isna(numerator):
        return "raw_cashflow_fetched_but_missing_field"
    if pd.isna(denominator) or denominator == 0:
        return "denominator_missing_or_zero"
    return "mapping_or_formula_gap"


def build_cashflow_source_audit(
    dataset: pd.DataFrame,
    *,
    cache_dir: str | Path = DEFAULT_CACHE_DIR,
    pit_cache: pd.DataFrame | None = None,
) -> dict[str, Any]:
    cache_path = expected_pit_financial_cache_path(dataset, cache_dir)
    pit_cache_df = pit_cache if pit_cache is not None else _read_csv_if_exists(cache_path)
    dataset_columns = list(dataset.columns)
    cache_columns = list(pit_cache_df.columns) if not pit_cache_df.empty else []
    combined_columns = list(dict.fromkeys(dataset_columns + cache_columns))
    numerator_col = _first_present(combined_columns, RAW_NUMERATOR_CANDIDATES)
    denominator_col = _first_present(combined_columns, RAW_DENOMINATOR_CANDIDATES)

    inspect_df = dataset.copy()
    if not pit_cache_df.empty and {"ticker", "date"}.issubset(pit_cache_df.columns):
        cache_extra_cols = [c for c in [numerator_col, denominator_col] if c and c not in inspect_df.columns and c in pit_cache_df.columns]
        if cache_extra_cols:
            cache = pit_cache_df[["ticker", "date", *cache_extra_cols]].copy()
            inspect_df["date"] = pd.to_datetime(inspect_df["date"], errors="coerce")
            cache["date"] = pd.to_datetime(cache["date"], errors="coerce")
            inspect_df = inspect_df.merge(cache, on=["ticker", "date"], how="left")

    categories = inspect_df.apply(_missing_category, axis=1, numerator_col=numerator_col, denominator_col=denominator_col)
    category_counts = categories.value_counts(dropna=False).to_dict()
    raw_cache_exists = bool(cache_path and cache_path.exists())
    raw_inputs_retained = bool(numerator_col or denominator_col)
    coverage = _coverage(inspect_df, CASHFLOW_FIELD)
    final_diagnosis = "coverage_ok"
    if coverage < 0.30:
        if not raw_cache_exists:
            final_diagnosis = "raw_cashflow_not_fetched_or_cache_missing"
        elif not raw_inputs_retained:
            final_diagnosis = "output_column_dropped_before_dataset"
        elif category_counts.get("denominator_missing_or_zero", 0) >= category_counts.get("raw_cashflow_fetched_but_missing_field", 0):
            final_diagnosis = "denominator_missing_or_zero"
        else:
            final_diagnosis = "raw_cashflow_fetched_but_missing_field"

    source_summary = {
        "cache_path": str(cache_path) if cache_path else None,
        "cache_exists": raw_cache_exists,
        "cache_columns": cache_columns,
        "dataset_has_raw_numerator": any(c in dataset_columns for c in RAW_NUMERATOR_CANDIDATES),
        "dataset_has_raw_denominator": any(c in dataset_columns for c in RAW_DENOMINATOR_CANDIDATES),
        "cache_has_raw_numerator": any(c in cache_columns for c in RAW_NUMERATOR_CANDIDATES),
        "cache_has_raw_denominator": any(c in cache_columns for c in RAW_DENOMINATOR_CANDIDATES),
        "selected_numerator_column": numerator_col,
        "selected_denominator_column": denominator_col,
    }
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(dataset)),
        "cashflow_coverage": coverage,
        "non_null_rows": int(_numeric(inspect_df[CASHFLOW_FIELD]).notna().sum()) if CASHFLOW_FIELD in inspect_df.columns else 0,
        "source_summary": source_summary,
        "missingness_split": [
            {"category": str(k), "rows": int(v), "share": float(v / len(inspect_df)) if len(inspect_df) else 0.0}
            for k, v in category_counts.items()
        ],
        "final_diagnosis": final_diagnosis,
        "raw_source_proof_level": "direct" if raw_inputs_retained else "blocked_by_missing_raw_retention",
        "recommendations": _recommendations(final_diagnosis, source_summary),
    }


def _recommendations(final_diagnosis: str, source_summary: dict[str, Any]) -> list[str]:
    if final_diagnosis == "output_column_dropped_before_dataset":
        return [
            "add diagnostic-only retention for PIT cashflow numerator and denominator",
            "regenerate a PIT diagnostic dataset/cache before deciding whether cashflow is truly unusable",
            "do not run more value-trap workflows until raw cashflow provenance is direct",
        ]
    if final_diagnosis == "raw_cashflow_not_fetched_or_cache_missing":
        return ["inspect Tushare cashflow fetch path and cache creation before further backtests"]
    if final_diagnosis == "denominator_missing_or_zero":
        return ["test alternate profit denominator fields and zero-denominator handling"]
    if final_diagnosis == "raw_cashflow_fetched_but_missing_field":
        return ["verify cashflow API field name mapping and fallback numerator candidates"]
    return ["cashflow source audit passed; use normal attribution gates before any new workflow"]


def audit_to_markdown(payload: dict[str, Any]) -> str:
    src = payload.get("source_summary", {})
    lines = [
        "# PIT Cashflow Source Audit",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        f"Rows: {payload.get('row_count')}",
        f"Cashflow coverage: {payload.get('cashflow_coverage')}",
        f"Final diagnosis: {payload.get('final_diagnosis')}",
        f"Proof level: {payload.get('raw_source_proof_level')}",
        "",
        "## Source summary",
        f"- cache_path: {src.get('cache_path')}",
        f"- cache_exists: {src.get('cache_exists')}",
        f"- dataset_has_raw_numerator: {src.get('dataset_has_raw_numerator')}",
        f"- dataset_has_raw_denominator: {src.get('dataset_has_raw_denominator')}",
        f"- cache_has_raw_numerator: {src.get('cache_has_raw_numerator')}",
        f"- cache_has_raw_denominator: {src.get('cache_has_raw_denominator')}",
        "",
        "## Missingness split",
    ]
    for row in payload.get("missingness_split", []):
        lines.append(f"- {row['category']}: rows={row['rows']}, share={row['share']}")
    lines += ["", "## Recommendations"]
    for item in payload.get("recommendations", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_cashflow_source_audit(
    dataset_path: str | Path = DEFAULT_DATASET,
    cache_dir: str | Path = DEFAULT_CACHE_DIR,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    dataset = pd.read_csv(dataset_path)
    payload = build_cashflow_source_audit(dataset, cache_dir=cache_dir)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "source_audit.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "source_audit.md").write_text(audit_to_markdown(payload), encoding="utf-8")
    return payload
