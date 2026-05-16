from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from factor_lab.online_data_source_preflight import classify_pit_control, detect_date_fields


@dataclass(frozen=True)
class InstitutionalHoldingConfig:
    min_rows: int = 100
    min_tickers: int = 10
    allowed_pit_controls: tuple[str, ...] = ("announcement_date_pit", "trade_date_observable")


def normalize_holding_frame(df: pd.DataFrame, *, endpoint: str) -> pd.DataFrame:
    out = df.copy()
    out["endpoint"] = endpoint
    for col in ("ts_code", "ann_date", "f_ann_date", "end_date", "holder_name"):
        if col in out.columns:
            out[col] = out[col].astype(str).str.replace("-", "", regex=False).replace({"nan": pd.NA, "None": pd.NA})
    for col in ("hold_amount", "hold_ratio", "hold_float_ratio", "float_ratio", "hold_change"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def endpoint_schema_report(df: pd.DataFrame, *, endpoint: str) -> dict[str, Any]:
    cols = tuple(str(c) for c in df.columns) if df is not None else ()
    date_fields = detect_date_fields(cols)
    pit_control = classify_pit_control(date_fields)
    tickers = int(df["ts_code"].nunique()) if df is not None and "ts_code" in df.columns and len(df) else 0
    return {
        "endpoint": endpoint,
        "rows": int(len(df)) if df is not None else 0,
        "tickers": tickers,
        "columns": list(cols),
        "date_fields": list(date_fields),
        "pit_control": pit_control,
        "sample": df.head(3).to_dict(orient="records") if df is not None and len(df) else [],
    }


def build_institutional_holding_source_report(
    endpoint_frames: dict[str, pd.DataFrame],
    *,
    config: InstitutionalHoldingConfig | None = None,
) -> dict[str, Any]:
    cfg = config or InstitutionalHoldingConfig()
    endpoint_reports = {name: endpoint_schema_report(df, endpoint=name) for name, df in endpoint_frames.items()}
    usable = [r for r in endpoint_reports.values() if r["rows"] > 0]
    total_rows = int(sum(r["rows"] for r in usable))
    tickers = int(len(set().union(*[
        set(endpoint_frames[name]["ts_code"].dropna().astype(str).tolist())
        for name, r in endpoint_reports.items()
        if r["rows"] > 0 and "ts_code" in endpoint_frames[name].columns
    ]))) if usable else 0
    pit_safe = [r for r in usable if r["pit_control"] in cfg.allowed_pit_controls]
    reasons: list[str] = []
    if not usable:
        reasons.append("no_successful_endpoint_rows")
    if total_rows < cfg.min_rows:
        reasons.append("rows_too_low")
    if tickers < cfg.min_tickers:
        reasons.append("tickers_too_low")
    if usable and not pit_safe:
        reasons.append("not_pit_safe_end_date_only_or_missing_announcement_date")
    if not reasons:
        decision = "proceed_institutional_holding_readonly_feature_plan"
    elif "not_pit_safe_end_date_only_or_missing_announcement_date" in reasons:
        decision = "stop_institutional_holding_not_pit_safe"
    elif "no_successful_endpoint_rows" in reasons:
        decision = "stop_institutional_holding_no_access_or_empty"
    else:
        decision = "stop_institutional_holding_coverage_insufficient"
    return {
        "coverage": {"rows": total_rows, "tickers": tickers, "endpoints_with_rows": len(usable)},
        "endpoint_reports": endpoint_reports,
        "decision": {"decision": decision, "reasons": reasons or ["bounded_sample_has_pit_safe_rows"]},
    }
