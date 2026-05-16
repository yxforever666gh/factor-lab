from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = ROOT / "artifacts" / "pit_cashflow_denominator_audit"

DENOMINATOR_CANDIDATES = [
    {
        "candidate": "cashflow.net_profit",
        "columns": ["pit_cashflow_denominator_raw", "cashflow__net_profit", "net_profit"],
        "eligible_denominator": True,
        "reason": "direct profit amount from cashflow statement",
    },
    {
        "candidate": "income.n_income_attr_p",
        "columns": ["pit_cashflow_income_n_income_attr_p_raw", "income__n_income_attr_p", "n_income_attr_p"],
        "eligible_denominator": True,
        "reason": "PIT-safe parent-company net profit amount from income statement",
    },
    {
        "candidate": "income.total_profit",
        "columns": ["pit_cashflow_income_total_profit_raw", "income__total_profit", "total_profit"],
        "eligible_denominator": True,
        "reason": "PIT-safe total profit amount fallback from income statement",
    },
    {
        "candidate": "fina_indicator.netprofit_yoy",
        "columns": ["netprofit_yoy", "profit_yoy", "fina_indicator__netprofit_yoy", "fina_indicator__q_netprofit_yoy"],
        "eligible_denominator": False,
        "reason": "growth rate, not a profit amount denominator",
    },
    {
        "candidate": "fina_indicator.netprofit_margin",
        "columns": ["netprofit_margin", "fina_indicator__netprofit_margin"],
        "eligible_denominator": False,
        "reason": "margin percentage, not usable unless revenue/profit reconstruction is separately proven",
    },
]


def _first_present(columns: set[str], names: list[str]) -> str | None:
    for name in names:
        if name in columns:
            return name
    return None


def _series(df: pd.DataFrame, column: str | None) -> pd.Series:
    if column and column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series([pd.NA] * len(df), index=df.index, dtype="Float64")


def _ratio(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _selected_column_for_candidate(df: pd.DataFrame, candidate: str, column: str | None) -> str | None:
    if column is None and "pit_cashflow_denominator_source" in df.columns and candidate in {"income.n_income_attr_p", "income.total_profit"}:
        return "pit_cashflow_denominator_raw" if "pit_cashflow_denominator_raw" in df.columns else None
    return column


def _candidate_values(df: pd.DataFrame, candidate: str, column: str | None) -> pd.Series:
    source_col = "pit_cashflow_denominator_source"
    column = _selected_column_for_candidate(df, candidate, column)
    values = _series(df, column)
    if source_col in df.columns:
        source = df[source_col].astype("string")
        source_map = {
            "cashflow.net_profit": "tushare.cashflow.net_profit",
            "income.n_income_attr_p": "tushare.income.n_income_attr_p",
            "income.total_profit": "tushare.income.total_profit",
        }
        expected = source_map.get(candidate)
        if expected:
            values = values.where(source == expected)
    return values


def build_cashflow_denominator_audit(
    frame: pd.DataFrame,
    *,
    viability_threshold: float = 0.60,
) -> dict[str, Any]:
    rows = int(len(frame))
    columns = set(frame.columns)
    candidate_rows: list[dict[str, Any]] = []
    viable_order: list[str] = []

    for spec in DENOMINATOR_CANDIDATES:
        column = _first_present(columns, list(spec["columns"]))
        column = _selected_column_for_candidate(frame, str(spec["candidate"]), column)
        values = _candidate_values(frame, str(spec["candidate"]), column)
        non_null = int(values.notna().sum())
        non_zero = int((values.notna() & (values != 0)).sum())
        nonzero_coverage = _ratio(non_zero, rows)
        is_eligible = bool(spec["eligible_denominator"])
        is_viable = is_eligible and nonzero_coverage >= viability_threshold
        if is_eligible and column:
            viable_order.append(str(spec["candidate"]))
        candidate_rows.append(
            {
                "candidate": spec["candidate"],
                "selected_column": column,
                "eligible_denominator": is_eligible,
                "reason": spec["reason"],
                "non_null_rows": non_null,
                "non_zero_rows": non_zero,
                "non_null_coverage": round(_ratio(non_null, rows), 6),
                "nonzero_coverage": round(nonzero_coverage, 6),
                "viable_at_threshold": is_viable,
            }
        )

    base = next((row for row in candidate_rows if row["candidate"] == "cashflow.net_profit"), None)
    best = max(
        [row for row in candidate_rows if row["eligible_denominator"]],
        key=lambda row: row["nonzero_coverage"],
        default=None,
    )
    recommended = ["cashflow.net_profit"] + [name for name in viable_order if name != "cashflow.net_profit"]
    best_cov = float(best["nonzero_coverage"]) if best else 0.0
    base_cov = float(base["nonzero_coverage"]) if base else 0.0

    if best_cov >= viability_threshold and best_cov > base_cov:
        decision = "cashflow_denominator_fixed_candidate"
    elif best_cov >= viability_threshold:
        decision = "cashflow_current_denominator_viable"
    elif best_cov >= 0.30:
        decision = "cashflow_denominator_partial_candidate_needs_more_evidence"
    else:
        decision = "cashflow_unusable_current_source"

    hard_stops = []
    if best_cov < 0.30:
        hard_stops.append("best_denominator_coverage_below_30pct")
    if not any(row["candidate"] == "income.n_income_attr_p" and row["selected_column"] for row in candidate_rows):
        hard_stops.append("income_profit_amount_not_retained_or_not_fetched")

    return {
        "rows": rows,
        "viability_threshold": viability_threshold,
        "candidates": candidate_rows,
        "baseline_candidate": "cashflow.net_profit",
        "baseline_nonzero_coverage": round(base_cov, 6),
        "best_candidate": best["candidate"] if best else None,
        "best_nonzero_coverage": round(best_cov, 6),
        "recommended_fallback_order": recommended,
        "decision": decision,
        "hard_stops": hard_stops,
    }


def audit_to_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# PIT Cashflow Denominator Audit",
        "",
        f"- rows: {audit.get('rows')}",
        f"- decision: {audit.get('decision')}",
        f"- baseline_nonzero_coverage: {audit.get('baseline_nonzero_coverage')}",
        f"- best_candidate: {audit.get('best_candidate')}",
        f"- best_nonzero_coverage: {audit.get('best_nonzero_coverage')}",
        f"- recommended_fallback_order: {audit.get('recommended_fallback_order')}",
        f"- hard_stops: {audit.get('hard_stops')}",
        "",
        "## Candidates",
    ]
    for row in audit.get("candidates", []):
        lines.extend(
            [
                f"### {row.get('candidate')}",
                f"- selected_column: {row.get('selected_column')}",
                f"- eligible_denominator: {row.get('eligible_denominator')}",
                f"- nonzero_coverage: {row.get('nonzero_coverage')}",
                f"- viable_at_threshold: {row.get('viable_at_threshold')}",
                f"- reason: {row.get('reason')}",
                "",
            ]
        )
    return "\n".join(lines).strip() + "\n"


def write_cashflow_denominator_audit(
    dataset_path: str | Path = ROOT / "artifacts" / "pit_cashflow_source_audit" / "diagnostic_dataset.csv",
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(dataset_path)
    audit = build_cashflow_denominator_audit(frame)
    (output / "denominator_audit.json").write_text(__import__("json").dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "denominator_audit.md").write_text(audit_to_markdown(audit), encoding="utf-8")
    return audit
