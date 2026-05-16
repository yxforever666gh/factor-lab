from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

KEY_COLUMNS = ("date", "ticker")


def _normalize_date_key(series: pd.Series) -> pd.Series:
    raw = series.astype(str).str.strip()
    yyyymmdd = raw.str.fullmatch(r"\d{8}")
    out = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    if yyyymmdd.any():
        out.loc[yyyymmdd] = pd.to_datetime(raw.loc[yyyymmdd], format="%Y%m%d", errors="coerce")
    if (~yyyymmdd).any():
        out.loc[~yyyymmdd] = pd.to_datetime(series.loc[~yyyymmdd], errors="coerce")
    return out.dt.strftime("%Y-%m-%d")


def normalize_overlay_frame(overlay: pd.DataFrame) -> pd.DataFrame:
    out = overlay.copy()
    if "ts_code" in out.columns and "ticker" not in out.columns:
        out["ticker"] = out["ts_code"]
    missing = [c for c in KEY_COLUMNS if c not in out.columns]
    if missing:
        raise ValueError(f"feature overlay missing key columns: {missing}")
    out["date"] = _normalize_date_key(out["date"])
    out["ticker"] = out["ticker"].astype(str)
    out = out.dropna(subset=list(KEY_COLUMNS))
    out = out.drop_duplicates(subset=list(KEY_COLUMNS), keep="last")
    return out


def apply_feature_overlay(frame: pd.DataFrame, overlay: pd.DataFrame, *, columns: list[str] | None = None) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    base = frame.copy()
    if "date" not in base.columns or "ticker" not in base.columns:
        raise ValueError("base frame must contain date and ticker for feature overlay")
    base_date_original = base["date"].copy()
    base["_overlay_date_key"] = _normalize_date_key(base["date"])
    base["_overlay_ticker_key"] = base["ticker"].astype(str)
    ov = normalize_overlay_frame(overlay)
    ov = ov.rename(columns={"date": "_overlay_date_key", "ticker": "_overlay_ticker_key"})
    selected = ["_overlay_date_key", "_overlay_ticker_key"]
    available_overlay_cols = [c for c in ov.columns if c not in {"_overlay_date_key", "_overlay_ticker_key", "ts_code"}]
    if columns:
        missing = sorted(set(columns) - set(available_overlay_cols))
        if missing:
            raise ValueError(f"feature overlay missing requested columns: {missing}")
        selected.extend(columns)
    else:
        selected.extend(available_overlay_cols)
    merged = base.merge(ov[selected], on=["_overlay_date_key", "_overlay_ticker_key"], how="left", suffixes=("", "_overlay"))
    merged["date"] = base_date_original.values
    return merged.drop(columns=["_overlay_date_key", "_overlay_ticker_key"], errors="ignore")


def apply_feature_overlay_from_config(frame: pd.DataFrame, config: dict[str, Any]) -> pd.DataFrame:
    path = config.get("feature_overlay_csv") or config.get("feature_overlay_path")
    if not path:
        return frame
    overlay_path = Path(path)
    if not overlay_path.exists():
        raise FileNotFoundError(f"feature overlay csv not found: {overlay_path}")
    overlay = pd.read_csv(overlay_path)
    columns = config.get("feature_overlay_columns")
    if columns is not None:
        columns = [str(c) for c in columns]
    return apply_feature_overlay(frame, overlay, columns=columns)
