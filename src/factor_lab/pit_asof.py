
from __future__ import annotations

from typing import Any
import pandas as pd


def _parse_date(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.replace("-", "", regex=False)
    return pd.to_datetime(text, format="%Y%m%d", errors="coerce")


def normalize_statement_dates(
    df: pd.DataFrame,
    *,
    ann_col: str = "ann_date",
    f_ann_col: str = "f_ann_date",
    end_col: str = "end_date",
) -> pd.DataFrame:
    out = df.copy()
    if end_col not in out.columns:
        raise ValueError("end_date column required for PIT statements")
    if ann_col not in out.columns and f_ann_col not in out.columns:
        out["effective_ann_date"] = pd.NaT
        out["pit_validated"] = False
        out["pit_blocked_reason"] = "missing_announcement_date"
        return out
    if ann_col in out.columns:
        out["_ann_dt"] = _parse_date(out[ann_col])
    else:
        out["_ann_dt"] = pd.NaT
    if f_ann_col in out.columns:
        out["_f_ann_dt"] = _parse_date(out[f_ann_col])
    else:
        out["_f_ann_dt"] = pd.NaT
    out["effective_ann_date"] = out["_f_ann_dt"].where(out["_f_ann_dt"].notna(), out["_ann_dt"])
    f_ann_raw = out[f_ann_col] if f_ann_col in out.columns else pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
    ann_raw = out[ann_col] if ann_col in out.columns else pd.Series([pd.NA] * len(out), index=out.index, dtype="object")
    out["source_ann_date"] = f_ann_raw.where(f_ann_raw.notna(), ann_raw)
    out["source_end_date"] = out[end_col]
    out["_end_dt"] = _parse_date(out[end_col])
    out["pit_validated"] = out["effective_ann_date"].notna()
    out["pit_blocked_reason"] = out["pit_validated"].map(lambda ok: "" if ok else "missing_announcement_date")
    out["ann_before_end"] = (out["effective_ann_date"].notna()) & (out["_end_dt"].notna()) & (out["effective_ann_date"] < out["_end_dt"])
    return out


def select_latest_statement_asof(
    statements: pd.DataFrame,
    trade_dates: pd.DataFrame,
    *,
    code_col: str = "ts_code",
    trade_code_col: str | None = None,
    trade_date_col: str = "date",
    ann_date_col: str = "effective_ann_date",
    end_date_col: str = "end_date",
    source_table: str = "financial_statement",
    source_fields: list[str] | None = None,
) -> pd.DataFrame:
    if statements.empty or trade_dates.empty:
        return trade_dates.copy()
    trade_code_col = trade_code_col or code_col
    st = statements.copy()
    if ann_date_col not in st.columns:
        st = normalize_statement_dates(st, end_col=end_date_col)
        ann_date_col = "effective_ann_date"
    tr = trade_dates.copy()
    tr["_trade_dt"] = _parse_date(tr[trade_date_col])
    st = st[st[ann_date_col].notna()].copy()
    if end_date_col in st.columns:
        st["_dup_code_end_date"] = st.duplicated([code_col, end_date_col], keep=False)
        st["duplicate_history_count"] = st.groupby([code_col, end_date_col])[end_date_col].transform("size")
    else:
        st["_dup_code_end_date"] = False
        st["duplicate_history_count"] = 1
    keep_cols = [code_col, ann_date_col, end_date_col, "source_ann_date", "source_end_date", "_dup_code_end_date", "duplicate_history_count"]
    for c in source_fields or []:
        if c in st.columns and c not in keep_cols:
            keep_cols.append(c)
    st = st[[c for c in keep_cols if c in st.columns]].sort_values([code_col, ann_date_col, end_date_col])
    rows: list[dict[str, Any]] = []
    by_code = {code: frame for code, frame in st.groupby(code_col)}
    for rec in tr.to_dict(orient="records"):
        code = rec.get(trade_code_col)
        t = rec.get("_trade_dt")
        frame = by_code.get(code)
        base = dict(rec)
        if frame is None or pd.isna(t):
            base.update({"pit_validated": False, "pit_blocked_reason": "no_statement_asof", "source_table": source_table})
            rows.append(base)
            continue
        eligible = frame[frame[ann_date_col] <= t]
        if eligible.empty:
            base.update({"pit_validated": False, "pit_blocked_reason": "no_statement_asof", "source_table": source_table})
            rows.append(base)
            continue
        latest = eligible.sort_values([ann_date_col, end_date_col]).iloc[-1].to_dict()
        for k, v in latest.items():
            if k != code_col:
                base[k] = v
        base.update({"pit_validated": True, "pit_blocked_reason": "", "source_table": source_table, "source_field": ",".join(source_fields or [])})
        base["duplicate_code_end_date_flag"] = bool(latest.get("_dup_code_end_date", False))
        rows.append(base)
    out = pd.DataFrame(rows)
    return out.drop(columns=["_trade_dt"], errors="ignore")
