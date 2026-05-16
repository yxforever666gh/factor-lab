#!/usr/bin/env python3
"""P0 PIT data preflight for Tushare and Diemeng.

Checks coverage, announcement-date availability, and obvious future-function risks.
This script does not run factors, write queues, or start daemons.
"""
from __future__ import annotations

import ast
import json
import os
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/data_preflight")
JSON_OUT = ARTIFACT_DIR / "p0_pit_data_preflight_2026-05-06.json"
MD_OUT = ARTIFACT_DIR / "p0_pit_data_preflight_2026-05-06.md"
BASE_DIEMENG = "https://mg.diemeng.chat/api"
PERIODS = ["20181231", "20191231", "20201231", "20211231", "20221231", "20231231"]
P0_TABLES = ["income", "balancesheet", "cashflow", "fina_indicator"]
DIEMENG_TABLES = ["income", "balancesheet", "cashflow", "financial_indicator"]
MIN_COVERAGE = 0.70
MIN_ANN_RATE = 0.95
TUSHARE_SAMPLE_SIZE = 40


def _legacy_credential(name: str) -> str | None:
    """Read one-off credentials from older probe scripts if env is absent.

    This is only to avoid reprinting secrets in shell commands. The report never stores keys.
    """
    probe = Path("scripts/probe_p0_data_sources.py")
    if not probe.exists():
        return None
    try:
        tree = ast.parse(probe.read_text(encoding="utf-8"))
    except Exception:
        return None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    value = None
                    try:
                        value = ast.literal_eval(node.value)
                    except Exception:
                        # Older one-off probe stores credentials as:
                        # os.environ.get("NAME") or "literal".
                        if isinstance(node.value, ast.BoolOp) and isinstance(node.value.op, ast.Or):
                            for part in reversed(node.value.values):
                                if isinstance(part, ast.Constant) and isinstance(part.value, str):
                                    value = part.value
                                    break
                    if isinstance(value, str) and value:
                        return value
    return None


def _token(name: str) -> str | None:
    env = os.environ.get(name)
    if env:
        return env
    return _legacy_credential(name)


def _json_safe(v: Any) -> Any:
    try:
        import pandas as pd  # type: ignore
        if pd.isna(v):
            return None
    except Exception:
        pass
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:
            pass
    return v


def _summarize_financial_df(df: Any, code_col: str = "ts_code") -> dict[str, Any]:
    try:
        import pandas as pd  # type: ignore
    except Exception:
        pd = None
    rows = int(len(df)) if df is not None else 0
    cols = list(getattr(df, "columns", [])) if df is not None else []
    out: dict[str, Any] = {
        "rows": rows,
        "columns": cols[:80],
        "has_ann_date": "ann_date" in cols,
        "has_f_ann_date": "f_ann_date" in cols,
        "has_end_date": "end_date" in cols,
        "unique_codes": int(df[code_col].nunique()) if rows and code_col in cols else None,
        "ann_date_nonnull_rate": None,
        "f_ann_date_nonnull_rate": None,
        "end_date_nonnull_rate": None,
        "ann_before_end_count": None,
        "duplicate_code_end_date_count": None,
        "sample_dates": [],
    }
    if rows:
        if "ann_date" in cols:
            out["ann_date_nonnull_rate"] = round(float(df["ann_date"].notna().mean()), 4)
        if "f_ann_date" in cols:
            out["f_ann_date_nonnull_rate"] = round(float(df["f_ann_date"].notna().mean()), 4)
        if "end_date" in cols:
            out["end_date_nonnull_rate"] = round(float(df["end_date"].notna().mean()), 4)
        if "ann_date" in cols and "end_date" in cols and pd is not None:
            ann = pd.to_datetime(df["ann_date"].astype(str), errors="coerce")
            end = pd.to_datetime(df["end_date"].astype(str), errors="coerce")
            out["ann_before_end_count"] = int(((ann.notna()) & (end.notna()) & (ann < end)).sum())
        if code_col in cols and "end_date" in cols:
            out["duplicate_code_end_date_count"] = int(df.duplicated([code_col, "end_date"]).sum())
        sample_cols = [c for c in [code_col, "ann_date", "f_ann_date", "end_date"] if c in cols]
        if sample_cols:
            out["sample_dates"] = [
                {k: _json_safe(v) for k, v in rec.items()}
                for rec in df[sample_cols].head(5).to_dict(orient="records")
            ]
    return out


def _decision(rows: int, universe: int | None, ann_rate: float | None, has_ann: bool) -> dict[str, Any]:
    coverage = None
    if universe:
        coverage = round(rows / universe, 4)
    reasons: list[str] = []
    if not rows:
        reasons.append("no_rows")
    if not has_ann:
        reasons.append("missing_ann_date")
    if ann_rate is not None and ann_rate < MIN_ANN_RATE:
        reasons.append("ann_date_rate_below_95pct")
    if coverage is not None and coverage < MIN_COVERAGE:
        reasons.append("coverage_below_70pct")
    return {
        "coverage_vs_active_universe": coverage,
        "pit_safe_candidate": not reasons,
        "reasons": reasons,
    }


def _tushare_preflight(token: str | None) -> dict[str, Any]:
    result: dict[str, Any] = {"available": False, "periods": {}, "summary": {}}
    if not token:
        result["error"] = "missing_token"
        return result
    try:
        import tushare as ts  # type: ignore
        import pandas as pd  # type: ignore
    except Exception as e:
        result["error"] = f"import_failed: {type(e).__name__}: {e}"
        return result
    try:
        pro = ts.pro_api(token)
        result["available"] = True
        stock_basic = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,industry,list_date")
        result["stock_basic"] = {"rows": int(len(stock_basic)), "columns": list(stock_basic.columns)}
    except Exception as e:
        result["error"] = f"client_or_stock_basic_failed: {type(e).__name__}: {e}"
        return result

    def active_universe_for_period(period: str) -> int:
        cutoff = period[:4] + "1231"
        if "list_date" not in stock_basic.columns:
            return int(len(stock_basic))
        return int((stock_basic["list_date"].fillna("99999999") <= cutoff).sum())

    sample_universe = min(TUSHARE_SAMPLE_SIZE, int(len(stock_basic)))
    sample_codes = list(stock_basic["ts_code"].head(sample_universe))
    start_date = f"{PERIODS[0][:4]}0101"
    # Annual reports for the final fiscal year are usually announced in the
    # following calendar year. Fetch one extra year by announcement date so
    # period=YYYY1231 rows are not incorrectly treated as missing.
    end_date = f"{int(PERIODS[-1][:4]) + 1}1231"

    # This Tushare account requires ts_code for financial endpoints. Fetch each
    # sampled stock once over the full date range, then slice by end_date. This
    # keeps the bounded preflight honest without multiplying calls by periods.
    all_tables: dict[str, Any] = {}
    for table in P0_TABLES:
        frames = []
        fn = getattr(pro, table)
        for code in sample_codes:
            try:
                df = fn(ts_code=code, start_date=start_date, end_date=end_date)
                if df is not None and len(df):
                    frames.append(df)
            except Exception:
                pass
            time.sleep(0.01)
        if frames:
            all_tables[table] = pd.concat(frames, ignore_index=True).drop_duplicates()
        else:
            all_tables[table] = pd.DataFrame()

    for period in PERIODS:
        universe = sample_universe
        result["periods"][period] = {"active_universe_estimate": universe, "tables": {}}
        for table in P0_TABLES:
            try:
                df_all = all_tables.get(table, pd.DataFrame())
                df = df_all[df_all["end_date"].astype(str) == period].copy() if len(df_all) and "end_date" in df_all.columns else pd.DataFrame()
                summary = _summarize_financial_df(df, "ts_code")
                dec = _decision(
                    int(summary["unique_codes"] or summary["rows"]),
                    universe,
                    summary.get("ann_date_nonnull_rate"),
                    bool(summary.get("has_ann_date")),
                )
                summary["decision"] = dec
                result["periods"][period]["tables"][table] = summary
            except Exception as e:
                result["periods"][period]["tables"][table] = {"error": f"{type(e).__name__}: {e}", "decision": {"pit_safe_candidate": False, "reasons": ["api_error"]}}

    table_rollup: dict[str, Any] = {}
    for table in P0_TABLES:
        safe = 0
        total = 0
        coverages = []
        ann_rates = []
        dupes = 0
        ann_before_end = 0
        for pdata in result["periods"].values():
            rec = pdata["tables"].get(table, {})
            total += 1
            if rec.get("decision", {}).get("pit_safe_candidate"):
                safe += 1
            cov = rec.get("decision", {}).get("coverage_vs_active_universe")
            if cov is not None:
                coverages.append(cov)
            ar = rec.get("ann_date_nonnull_rate")
            if ar is not None:
                ann_rates.append(ar)
            dupes += int(rec.get("duplicate_code_end_date_count") or 0)
            ann_before_end += int(rec.get("ann_before_end_count") or 0)
        table_rollup[table] = {
            "safe_periods": safe,
            "total_periods": total,
            "min_coverage": min(coverages) if coverages else None,
            "avg_coverage": round(sum(coverages)/len(coverages), 4) if coverages else None,
            "min_ann_date_rate": min(ann_rates) if ann_rates else None,
            "duplicate_code_end_date_count_total": dupes,
            "ann_before_end_count_total": ann_before_end,
            "ready_for_pit_research": safe == total and ann_before_end == 0,
        }
    result["summary"]["tables"] = table_rollup
    result["summary"]["ready_for_p0_value_trap_experiment"] = all(v["ready_for_pit_research"] for v in table_rollup.values())
    return result


def _http_get(path: str, params: dict[str, Any], key: str | None, timeout: int = 40) -> tuple[bool, Any]:
    if not key:
        return False, {"error": "missing_api_key"}
    import urllib.parse
    url = BASE_DIEMENG + path + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        method="GET",
        headers={"apiKey": key, "X-API-Key": key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return True, json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"raw": raw[:500]}
        return False, {"status": e.code, **body}
    except Exception as e:
        return False, {"error": f"{type(e).__name__}: {e}"}


def _http_post(path: str, body: dict[str, Any], key: str | None, timeout: int = 40) -> tuple[bool, Any]:
    if not key:
        return False, {"error": "missing_api_key"}
    req = urllib.request.Request(
        BASE_DIEMENG + path,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"apiKey": key, "X-API-Key": key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return True, json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"raw": raw[:500]}
        return False, {"status": e.code, **body}
    except Exception as e:
        return False, {"error": f"{type(e).__name__}: {e}"}


def _rows(obj: Any) -> list[dict[str, Any]]:
    if not isinstance(obj, dict):
        return []
    for k in ("data", "result"):
        v = obj.get(k)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
        if isinstance(v, dict):
            for sk in ("list", "rows", "items", "records"):
                sv = v.get(sk)
                if isinstance(sv, list):
                    return [x for x in sv if isinstance(x, dict)]
    return []


def _diemeng_preflight(key: str | None) -> dict[str, Any]:
    result: dict[str, Any] = {"available": False, "periods": {}, "summary": {}}
    if not key:
        result["error"] = "missing_api_key"
        return result
    ok, obj = _http_get("/stock/list", {"page": 1, "page_size": 10000}, key)
    stock_rows = _rows(obj)
    if not ok and not stock_rows:
        result["error"] = obj
        return result
    result["available"] = True
    result["stock_list"] = {"rows": len(stock_rows), "columns": sorted({k for r in stock_rows[:10] for k in r.keys()})}
    universe_by_period: dict[str, int] = {}
    for period in PERIODS:
        cutoff = f"{period[:4]}-12-31"
        cnt = 0
        for r in stock_rows:
            ld = str(r.get("list_date") or "9999-99-99")
            if ld <= cutoff:
                cnt += 1
        universe_by_period[period] = cnt or len(stock_rows)

    def fetch_table(path: str, period: str) -> list[dict[str, Any]]:
        end_date = f"{period[:4]}-{period[4:6]}-{period[6:8]}"
        all_rows: list[dict[str, Any]] = []
        for page in range(0, 5):
            ok, obj = _http_post(path, {"end_date": end_date, "page": page, "page_size": 10000}, key)
            rs = _rows(obj)
            if not rs:
                if page == 0:
                    # Some endpoints may be 1-based despite docs; try page=1 once.
                    ok1, obj1 = _http_post(path, {"end_date": end_date, "page": 1, "page_size": 10000}, key)
                    rs1 = _rows(obj1)
                    if rs1:
                        all_rows.extend(rs1)
                        if len(rs1) < 10000:
                            break
                        continue
                break
            all_rows.extend(rs)
            if len(rs) < 10000:
                break
            time.sleep(0.2)
        return all_rows

    try:
        import pandas as pd  # type: ignore
    except Exception:
        pd = None

    path_map = {
        "income": "/stock/income",
        "balancesheet": "/stock/balancesheet",
        "cashflow": "/stock/cashflow",
        "financial_indicator": "/stock/financial_indicator",
    }
    for period in PERIODS:
        universe = universe_by_period.get(period)
        result["periods"][period] = {"active_universe_estimate": universe, "tables": {}}
        for table, path in path_map.items():
            try:
                rs = fetch_table(path, period)
                if pd is not None:
                    df = pd.DataFrame(rs)
                    summary = _summarize_financial_df(df, "stock_code")
                else:
                    cols = sorted({k for r in rs[:20] for k in r.keys()})
                    summary = {"rows": len(rs), "columns": cols, "has_ann_date": "ann_date" in cols, "has_f_ann_date": "f_ann_date" in cols, "has_end_date": "end_date" in cols, "unique_codes": len({r.get("stock_code") for r in rs})}
                dec = _decision(
                    int(summary.get("unique_codes") or summary.get("rows") or 0),
                    universe,
                    summary.get("ann_date_nonnull_rate"),
                    bool(summary.get("has_ann_date")),
                )
                summary["decision"] = dec
                result["periods"][period]["tables"][table] = summary
            except Exception as e:
                result["periods"][period]["tables"][table] = {"error": f"{type(e).__name__}: {e}", "decision": {"pit_safe_candidate": False, "reasons": ["api_error"]}}
            time.sleep(0.2)

    table_rollup: dict[str, Any] = {}
    for table in DIEMENG_TABLES:
        safe = 0
        total = 0
        coverages = []
        ann_rates = []
        dupes = 0
        ann_before_end = 0
        for pdata in result["periods"].values():
            rec = pdata["tables"].get(table, {})
            total += 1
            if rec.get("decision", {}).get("pit_safe_candidate"):
                safe += 1
            cov = rec.get("decision", {}).get("coverage_vs_active_universe")
            if cov is not None:
                coverages.append(cov)
            ar = rec.get("ann_date_nonnull_rate")
            if ar is not None:
                ann_rates.append(ar)
            dupes += int(rec.get("duplicate_code_end_date_count") or 0)
            ann_before_end += int(rec.get("ann_before_end_count") or 0)
        table_rollup[table] = {
            "safe_periods": safe,
            "total_periods": total,
            "min_coverage": min(coverages) if coverages else None,
            "avg_coverage": round(sum(coverages)/len(coverages), 4) if coverages else None,
            "min_ann_date_rate": min(ann_rates) if ann_rates else None,
            "duplicate_code_end_date_count_total": dupes,
            "ann_before_end_count_total": ann_before_end,
            "ready_for_pit_research": safe == total and ann_before_end == 0,
        }
    result["summary"]["tables"] = table_rollup
    result["summary"]["ready_for_p0_value_trap_experiment"] = all(v["ready_for_pit_research"] for v in table_rollup.values())
    return result


def _write_md(report: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# P0 PIT Data Preflight — 2026-05-06")
    lines.append("")
    lines.append("Scope: coverage, announcement-date availability, and future-function risk only. No factor run, no queue write, no daemon start.")
    lines.append("")
    lines.append("## Executive decision")
    t_ready = report.get("tushare", {}).get("summary", {}).get("ready_for_p0_value_trap_experiment")
    d_ready = report.get("diemeng", {}).get("summary", {}).get("ready_for_p0_value_trap_experiment")
    if t_ready:
        lines.append("- Tushare: READY as primary PIT financial source for the next gated value-trap preflight/experiment.")
    else:
        lines.append("- Tushare: NOT READY or incomplete; inspect table reasons before using in backtests.")
    if d_ready:
        lines.append("- Diemeng: READY as supplemental PIT financial source for cross-checking/backup.")
    else:
        lines.append("- Diemeng: NOT READY as standalone PIT source in this run; use only after resolving listed blockers.")
    lines.append("- Still required before factors: build as-of feature join using ann_date/f_ann_date; do not merge financial statements on end_date alone.")
    lines.append("")
    for source in ("tushare", "diemeng"):
        lines.append(f"## {source.title()} table rollup")
        src = report.get(source, {})
        if src.get("error"):
            lines.append(f"- Error: `{src['error']}`")
            lines.append("")
            continue
        lines.append(f"- Available: {src.get('available')}")
        if source == "tushare":
            lines.append(f"- Stock basic rows: {src.get('stock_basic', {}).get('rows')}")
        else:
            lines.append(f"- Stock list rows: {src.get('stock_list', {}).get('rows')}")
        lines.append("")
        lines.append("| table | safe periods | min coverage | min ann_date rate | duplicate code/end_date | ann before end | ready |")
        lines.append("|---|---:|---:|---:|---:|---:|---|")
        for table, rec in src.get("summary", {}).get("tables", {}).items():
            lines.append(
                f"| {table} | {rec.get('safe_periods')}/{rec.get('total_periods')} | {rec.get('min_coverage')} | {rec.get('min_ann_date_rate')} | {rec.get('duplicate_code_end_date_count_total')} | {rec.get('ann_before_end_count_total')} | {rec.get('ready_for_pit_research')} |"
            )
        lines.append("")
    lines.append("## Remaining blockers / caveats")
    lines.append("- PIT-safe data availability does not by itself prove alpha quality; it only allows a safe next experiment.")
    lines.append("- Need a production as-of join layer that stores source end_date, ann_date/f_ann_date, table, and field provenance per feature value.")
    lines.append("- Need tradability filter consolidation: ST, suspension, delisting, new listings, limit-up/down, liquidity.")
    lines.append("- Need industry classification decision: Tushare/Diemeng industry is enough for first pass, but SW/CSI industry would be better later.")
    lines.append("- Diemeng should remain supplemental until daily_basic-equivalent valuation fields and historical tradability filters are verified.")
    MD_OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    global PERIODS, TUSHARE_SAMPLE_SIZE, JSON_OUT, MD_OUT
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-size", type=int, default=TUSHARE_SAMPLE_SIZE)
    parser.add_argument("--full-market", action="store_true")
    parser.add_argument("--start-year", type=int, default=2018)
    parser.add_argument("--end-year", type=int, default=2023)
    parser.add_argument("--json-out", default=str(ARTIFACT_DIR / "p0_pit_data_preflight_latest.json"))
    parser.add_argument("--md-out", default=str(ARTIFACT_DIR / "p0_pit_data_preflight_latest.md"))
    args = parser.parse_args()
    PERIODS = [f"{year}1231" for year in range(int(args.start_year), int(args.end_year) + 1)]
    TUSHARE_SAMPLE_SIZE = 1000000 if args.full_market else int(args.sample_size)
    JSON_OUT = Path(args.json_out)
    MD_OUT = Path(args.md_out)
    JSON_OUT.parent.mkdir(parents=True, exist_ok=True)
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "full_market" if args.full_market else "sample",
        "sample_size": TUSHARE_SAMPLE_SIZE,
        "scope": "P0 PIT data preflight: coverage, announcement dates, future-function risk, field availability",
        "thresholds": {"min_coverage": MIN_COVERAGE, "min_ann_date_rate": MIN_ANN_RATE},
        "periods": PERIODS,
        "credentials_redacted": True,
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
    }
    tushare = _tushare_preflight(_token("TUSHARE_TOKEN"))
    diemeng = _diemeng_preflight(_token("DIEMENG_API_KEY"))
    report["tushare"] = tushare
    report["diemeng"] = diemeng
    report["sources"] = {"tushare": tushare, "diemeng": diemeng}
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_md(report)
    printable = {
        "json": str(JSON_OUT),
        "markdown": str(MD_OUT),
        "mode": report["mode"],
        "sample_size": report["sample_size"],
        "tushare_ready": report["tushare"].get("summary", {}).get("ready_for_p0_value_trap_experiment"),
        "diemeng_ready": report["diemeng"].get("summary", {}).get("ready_for_p0_value_trap_experiment"),
        "tushare_tables": report["tushare"].get("summary", {}).get("tables"),
        "diemeng_tables": report["diemeng"].get("summary", {}).get("tables"),
    }
    print(json.dumps(printable, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
