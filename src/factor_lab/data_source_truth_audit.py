from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MARKET_DAILY_FIELDS = {
    "close",
    "return_1d",
    "forward_return_5d",
    "turnover",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "total_mv",
    "dividend_yield",
}
DERIVED_MARKET_FIELDS = {
    "momentum_20",
    "momentum_60",
    "momentum_120",
    "momentum_60_skip_5",
    "turnover_shock_5_20",
    "volatility_20",
    "volatility_60",
    "earnings_yield",
    "book_yield",
    "ps_yield",
    "size_inv",
    "industry_relative_pb",
    "industry_relative_pe",
    "industry_relative_book_yield",
    "industry_relative_earnings_yield",
    "roe_delta",
    "roe_yoy",
}
LEGACY_AMBIGUOUS_FIELDS = {"roe"}
PIT_FINANCIAL_FIELDS = {
    "operating_cashflow_to_profit",
    "debt_to_asset",
    "debt_to_assets",
    "profit_yoy",
    "netprofit_yoy",
    "revenue_yoy",
    "tr_yoy",
    "pit_feature_validated",
    "pit_source_ann_date",
    "pit_source_end_date",
}
EXTERNAL_CANDIDATE_FIELDS = {
    "shareholder_count",
    "institutional_holding_ratio",
    "margin_financing_balance",
    "short_selling_balance",
    "analyst_forecast_revision",
    "earnings_preannouncement",
    "earnings_express",
    "dragon_tiger_net_buy",
    "buyback_amount",
    "pledge_ratio",
    "insider_trade_amount",
    "order_book_imbalance",
    "news_sentiment",
}


@dataclass(frozen=True)
class CacheFileInventory:
    path: str
    rows: int
    columns: list[str]
    min_date: str | None
    max_date: str | None
    ticker_count: int | None
    kind: str


@dataclass(frozen=True)
class FieldTruth:
    field_name: str
    source_table: str | None
    source_file: str | None
    provider: str
    category: str
    date_range_available: dict[str, str | None]
    row_count: int
    ticker_count: int | None
    coverage_by_year: dict[str, float]
    median_coverage_by_trade_date: float | None
    pit_required: bool
    pit_status: str
    announcement_date_used: str
    asof_join_verified: bool
    future_leakage_risk: str
    mechanism_supported: list[str]
    decision: str
    notes: str


def _kind(path: Path) -> str:
    name = path.name
    if name.startswith("pit_financial_"):
        return "pit_financial_cache"
    if name.startswith("tushare_") or name.startswith("master_tushare_"):
        return "tushare_feature_cache"
    return "other_cache"


def _read_csv_head_and_stats(path: Path) -> CacheFileInventory:
    import pandas as pd

    df = pd.read_csv(path, low_memory=False)
    date_col = "date" if "date" in df.columns else "trade_date" if "trade_date" in df.columns else None
    ticker_col = "ticker" if "ticker" in df.columns else "ts_code" if "ts_code" in df.columns else None
    min_date = max_date = None
    if date_col:
        dates = pd.to_datetime(df[date_col], errors="coerce")
        if dates.notna().any():
            min_date = dates.min().strftime("%Y-%m-%d")
            max_date = dates.max().strftime("%Y-%m-%d")
    ticker_count = int(df[ticker_col].nunique()) if ticker_col else None
    return CacheFileInventory(
        path=str(path),
        rows=int(len(df)),
        columns=list(df.columns),
        min_date=min_date,
        max_date=max_date,
        ticker_count=ticker_count,
        kind=_kind(path),
    )


def collect_cache_file_inventory(cache_dir: str | Path) -> list[dict[str, Any]]:
    cache = Path(cache_dir)
    if not cache.exists():
        return []
    out: list[dict[str, Any]] = []
    for path in sorted(cache.glob("*.csv")):
        try:
            out.append(asdict(_read_csv_head_and_stats(path)))
        except Exception as exc:
            out.append({"path": str(path), "rows": 0, "columns": [], "min_date": None, "max_date": None, "ticker_count": None, "kind": _kind(path), "error": f"{type(exc).__name__}: {exc}"})
    return out


def inspect_feature_schema() -> dict[str, Any]:
    try:
        from factor_lab.feature_schema import TUSHARE_AVAILABLE_FEATURE_COLUMNS, TUSHARE_BLOCKED_FEATURE_COLUMNS, TUSHARE_FEATURE_COLUMNS
    except Exception:
        return {"all": [], "available": [], "blocked": []}
    return {
        "all": sorted(TUSHARE_FEATURE_COLUMNS),
        "available": sorted(TUSHARE_AVAILABLE_FEATURE_COLUMNS),
        "blocked": sorted(TUSHARE_BLOCKED_FEATURE_COLUMNS),
    }


def _date_col(df: Any) -> str | None:
    if "date" in df.columns:
        return "date"
    if "trade_date" in df.columns:
        return "trade_date"
    return None


def _ticker_col(df: Any) -> str | None:
    if "ticker" in df.columns:
        return "ticker"
    if "ts_code" in df.columns:
        return "ts_code"
    return None


def compute_field_coverage_by_year(frame: Any, fields: list[str]) -> dict[str, dict[str, float]]:
    import pandas as pd

    if frame is None or frame.empty:
        return {field: {} for field in fields}
    date_col = _date_col(frame)
    if not date_col:
        return {field: {} for field in fields}
    dates = pd.to_datetime(frame[date_col], errors="coerce")
    tmp = frame.copy()
    tmp["__year"] = dates.dt.year.astype("Int64")
    out: dict[str, dict[str, float]] = {}
    for field in fields:
        if field not in tmp.columns:
            out[field] = {}
            continue
        by_year = tmp.groupby("__year")[field].apply(lambda s: float(s.notna().mean()) if len(s) else 0.0)
        out[field] = {str(int(k)): round(float(v), 4) for k, v in by_year.items() if k is not None and str(k) != "<NA>"}
    return out


def median_coverage_by_trade_date(frame: Any, field: str) -> float | None:
    if frame is None or frame.empty or field not in frame.columns:
        return None
    date_col = _date_col(frame)
    ticker_col = _ticker_col(frame)
    if not date_col or not ticker_col:
        return None
    import pandas as pd

    tmp = frame[[date_col, ticker_col, field]].copy()
    tmp["__present"] = tmp[field].notna().astype(float)
    present = tmp.groupby(date_col)["__present"].sum()
    universe = tmp.groupby(date_col)[ticker_col].nunique().replace(0, pd.NA)
    per_date = present / universe
    if len(per_date) == 0:
        return None
    return round(float(per_date.median()), 4)


def classify_pit_status(field: str, *, present: bool, source_kind: str | None, has_pit_validated: bool = False, has_ann_date: bool = False) -> tuple[str, str, str, bool, str]:
    if not present:
        return "blocked", "blocked_missing_field", "high", False, "field not present in inspected cache/schema"
    if field in MARKET_DAILY_FIELDS or source_kind == "tushare_feature_cache" and field in {"industry"}:
        return "market_daily_observed", "usable", "low", False, "daily market/provider-observed field"
    if field in DERIVED_MARKET_FIELDS:
        return "market_daily_observed", "usable", "low", False, "derived only from contemporaneous/history market fields"
    if field in LEGACY_AMBIGUOUS_FIELDS:
        return "legacy_ambiguous", "ambiguous_legacy", "medium", True, "legacy ROE is derived from PE/PB unless PIT provenance is explicitly present"
    if field in PIT_FINANCIAL_FIELDS:
        if source_kind == "pit_financial_cache" and has_pit_validated and has_ann_date:
            return "strict_pit", "usable", "low", True, "PIT financial cache has validation marker and source announcement date"
        if source_kind == "pit_financial_cache":
            return "legacy_ambiguous", "blocked_pit", "medium", True, "PIT cache exists but validation or announcement date marker is missing"
        return "blocked", "blocked_pit", "high", True, "PIT financial field not backed by inspected PIT cache"
    return "blocked", "blocked_missing_field", "high", False, "not part of current surfaced data source"


def _mechanisms_for_field(field: str) -> list[str]:
    mapping = {
        "close": ["price history", "return calculation"],
        "return_1d": ["return/risk", "momentum/reversal"],
        "forward_return_5d": ["evaluation target only"],
        "turnover": ["liquidity", "weak crowding proxy"],
        "turnover_shock_5_20": ["attention/liquidity shock", "weak crowding proxy"],
        "volatility_20": ["risk control", "crowding proxy"],
        "volatility_60": ["risk control", "crowding proxy"],
        "pe_ttm": ["market-implied valuation"],
        "pb": ["market-implied valuation"],
        "earnings_yield": ["value"],
        "book_yield": ["value"],
        "industry_relative_book_yield": ["industry-relative value"],
        "industry_relative_earnings_yield": ["industry-relative value"],
        "operating_cashflow_to_profit": ["cash conversion", "monitor-only cashflow quality"],
        "debt_to_asset": ["distress/leverage"],
        "debt_to_assets": ["distress/leverage"],
        "profit_yoy": ["profitability improvement"],
        "netprofit_yoy": ["profitability improvement"],
        "revenue_yoy": ["revenue growth"],
        "tr_yoy": ["revenue growth"],
    }
    return mapping.get(field, [])


def _source_table_for_field(field: str) -> str | None:
    if field in {"pe_ttm", "pb", "ps_ttm", "total_mv", "turnover", "dividend_yield"}:
        return "tushare.daily_basic"
    if field in {"close", "return_1d", "forward_return_5d"}:
        return "tushare.daily"
    if field in {"operating_cashflow_to_profit"}:
        return "tushare.cashflow+income as-of"
    if field in {"debt_to_asset", "debt_to_assets"}:
        return "tushare.fina_indicator/balancesheet as-of"
    if field in {"profit_yoy", "netprofit_yoy", "revenue_yoy", "tr_yoy"}:
        return "tushare.fina_indicator as-of"
    return None


def _best_inventory_for_field(inventory: list[dict[str, Any]], field: str) -> dict[str, Any] | None:
    candidates = [item for item in inventory if field in item.get("columns", [])]
    if not candidates:
        return None
    def score(item: dict[str, Any]) -> tuple[int, int, int, int]:
        path = str(item.get("path") or "")
        kind_bonus = 1 if item.get("kind") == "pit_financial_cache" and field in PIT_FINANCIAL_FIELDS else 0
        version_bonus = 2 if "_diag_v2" in path else 1 if "_v2" in path else 0
        return (kind_bonus, version_bonus, int(item.get("rows") or 0), len(item.get("columns") or []))
    return sorted(candidates, key=score, reverse=True)[0]


def _load_csv(path: str | None):
    if not path:
        return None
    import pandas as pd
    return pd.read_csv(path, low_memory=False)


def build_data_source_truth_audit(project_root: str | Path = ".") -> dict[str, Any]:
    root = Path(project_root)
    cache_dir = root / "artifacts" / "tushare_cache"
    inventory = collect_cache_file_inventory(cache_dir)
    schema = inspect_feature_schema()
    fields = sorted(set(schema.get("all", [])) | PIT_FINANCIAL_FIELDS | EXTERNAL_CANDIDATE_FIELDS | {"shareholder_count", "institutional_holding_ratio"})
    rows: list[dict[str, Any]] = []
    loaded: dict[str, Any] = {}
    for field in fields:
        inv = _best_inventory_for_field(inventory, field)
        df = None
        if inv and inv.get("path"):
            path = inv["path"]
            if path not in loaded:
                loaded[path] = _load_csv(path)
            df = loaded[path]
        present = bool(inv and field in inv.get("columns", []))
        source_kind = inv.get("kind") if inv else None
        has_pit_validated = bool(df is not None and "pit_feature_validated" in getattr(df, "columns", []))
        has_ann_date = bool(df is not None and "pit_source_ann_date" in getattr(df, "columns", []))
        pit_status, decision, leakage, pit_required, notes = classify_pit_status(field, present=present, source_kind=source_kind, has_pit_validated=has_pit_validated, has_ann_date=has_ann_date)
        if field in schema.get("blocked", []) and decision == "usable":
            decision = "monitor_only" if field == "operating_cashflow_to_profit" else "ambiguous_legacy"
            notes += "; feature_schema still marks this as blocked/needs gated use"
        coverage = compute_field_coverage_by_year(df, [field]).get(field, {}) if df is not None else {}
        row = FieldTruth(
            field_name=field,
            source_table=_source_table_for_field(field),
            source_file=str(Path(inv["path"]).relative_to(root)) if inv and inv.get("path") and str(inv["path"]).startswith(str(root)) else (inv.get("path") if inv else None),
            provider="tushare/cache" if source_kind in {"tushare_feature_cache", "pit_financial_cache"} else "external_candidate",
            category="PIT financial" if field in PIT_FINANCIAL_FIELDS else "price/volume/valuation" if field in MARKET_DAILY_FIELDS | DERIVED_MARKET_FIELDS else "legacy derived" if field in LEGACY_AMBIGUOUS_FIELDS else "external candidate",
            date_range_available={"start": inv.get("min_date") if inv else None, "end": inv.get("max_date") if inv else None},
            row_count=int(inv.get("rows") or 0) if inv else 0,
            ticker_count=inv.get("ticker_count") if inv else None,
            coverage_by_year=coverage,
            median_coverage_by_trade_date=median_coverage_by_trade_date(df, field) if df is not None else None,
            pit_required=pit_required,
            pit_status=pit_status,
            announcement_date_used="ann_date/f_ann_date via pit_source_ann_date" if pit_status == "strict_pit" else "not_applicable" if not pit_required else "none_or_unverified",
            asof_join_verified=pit_status == "strict_pit",
            future_leakage_risk=leakage,
            mechanism_supported=_mechanisms_for_field(field),
            decision=decision,
            notes=notes,
        )
        rows.append(asdict(row))
    summary = {
        "usable": sum(1 for r in rows if r["decision"] == "usable"),
        "monitor_only": sum(1 for r in rows if r["decision"] == "monitor_only"),
        "ambiguous_legacy": sum(1 for r in rows if r["decision"] == "ambiguous_legacy"),
        "blocked": sum(1 for r in rows if str(r["decision"]).startswith("blocked")),
        "strict_pit_fields": [r["field_name"] for r in rows if r["pit_status"] == "strict_pit"],
        "external_missing_fields": [r["field_name"] for r in rows if r["category"] == "external candidate" and r["decision"].startswith("blocked")],
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "read-only cache/schema data-source truth audit; no network, no queue write, no daemon start",
        "no_network": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        "cache_dir": str(cache_dir),
        "inventory": inventory,
        "feature_schema": schema,
        "summary": summary,
        "fields": rows,
    }


def _markdown(report: dict[str, Any]) -> str:
    lines = ["# Data Source Truth Audit", "", f"Generated: {report.get('generated_at')}", "", "Scope: read-only cache/schema audit. No network, no queue write, no daemon start.", "", "## Summary"]
    summary = report.get("summary", {})
    for key in ["usable", "monitor_only", "ambiguous_legacy", "blocked"]:
        lines.append(f"- {key}: {summary.get(key)}")
    lines.append(f"- strict PIT fields: {', '.join(summary.get('strict_pit_fields', [])) or 'none'}")
    lines.append("")
    lines.append("## Field truth table")
    lines.append("| field | provider | category | PIT status | decision | date range | median date coverage | mechanism | notes |")
    lines.append("|---|---|---|---|---|---|---:|---|---|")
    for r in report.get("fields", []):
        dr = r.get("date_range_available", {})
        date_range = f"{dr.get('start') or ''}..{dr.get('end') or ''}"
        mech = ", ".join(r.get("mechanism_supported") or [])
        notes = str(r.get("notes") or "").replace("|", "/")[:160]
        lines.append(f"| {r.get('field_name')} | {r.get('provider')} | {r.get('category')} | {r.get('pit_status')} | {r.get('decision')} | {date_range} | {r.get('median_coverage_by_trade_date')} | {mech} | {notes} |")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("- Existing cache mainly supports market/valuation/liquidity fields plus a limited PIT financial slice.")
    lines.append("- True crowding/ownership/analyst/event/microstructure fields are absent and must be treated as data-source expansion candidates, not usable factors.")
    lines.append("- Financial fields should remain gated by PIT provenance and prior route closure evidence; PIT safety is not alpha evidence.")
    return "\n".join(lines) + "\n"


def write_data_source_truth_audit(project_root: str | Path = ".", output_dir: str | Path = "artifacts/data_source_truth_audit", knowledge_dir: str | Path = "knowledge") -> dict[str, Any]:
    root = Path(project_root)
    report = build_data_source_truth_audit(root)
    out = root / output_dir
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "data_source_truth_audit.json"
    md_path = out / "data_source_truth_audit.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md = _markdown(report)
    md_path.write_text(md, encoding="utf-8")
    kdir = root / knowledge_dir
    kdir.mkdir(parents=True, exist_ok=True)
    (kdir / "data_source_truth_table.md").write_text(md, encoding="utf-8")
    report["json_path"] = str(json_path)
    report["markdown_path"] = str(md_path)
    report["knowledge_path"] = str(kdir / "data_source_truth_table.md")
    return report
