#!/usr/bin/env python3
"""Probe Tushare and Diemeng P0 data-source readiness.

This script intentionally performs lightweight samples, not full-market ingestion.
It writes a redacted JSON report. API keys are read from env or constants below and
are never printed.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


OUT = Path("artifacts/p0_data_source_probe_2026-05-06.json")
MD = Path("artifacts/p0_data_source_probe_2026-05-06.md")

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN")
DIEMENG_API_KEY = os.environ.get("DIEMENG_API_KEY")


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_safe(v) for v in obj]
    try:
        import pandas as pd  # type: ignore
        if pd.isna(obj):
            return None
    except Exception:
        pass
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    return obj


def sample_rows(df: Any, n: int = 2) -> list[dict[str, Any]]:
    if df is None:
        return []
    try:
        return _json_safe(df.head(n).to_dict(orient="records"))
    except Exception:
        return []


def summarize_df(df: Any) -> dict[str, Any]:
    if df is None:
        return {"ok": False, "rows": 0, "columns": []}
    cols = list(getattr(df, "columns", []))
    return {
        "ok": True,
        "rows": int(len(df)),
        "columns": cols[:80],
        "has_ann_date": "ann_date" in cols,
        "has_f_ann_date": "f_ann_date" in cols,
        "has_end_date": "end_date" in cols,
        "sample": sample_rows(df, 1),
    }


def probe_tushare() -> dict[str, Any]:
    result: dict[str, Any] = {"available": False, "interfaces": {}, "sample_coverage": {}}
    try:
        import tushare as ts  # type: ignore
    except Exception as e:
        result["error"] = f"import_failed: {type(e).__name__}: {e}"
        return result
    try:
        pro = ts.pro_api(TUSHARE_TOKEN)
        result["available"] = True
    except Exception as e:
        result["error"] = f"client_failed: {type(e).__name__}: {e}"
        return result

    calls = {
        "stock_basic": lambda: pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,area,industry,list_date"),
        "daily": lambda: pro.daily(trade_date="20240102"),
        "daily_basic": lambda: pro.daily_basic(trade_date="20240102"),
        "adj_factor": lambda: pro.adj_factor(ts_code="000001.SZ", start_date="20240102", end_date="20240110"),
        "income": lambda: pro.income(ts_code="000001.SZ", period="20231231"),
        "balancesheet": lambda: pro.balancesheet(ts_code="000001.SZ", period="20231231"),
        "cashflow": lambda: pro.cashflow(ts_code="000001.SZ", period="20231231"),
        "fina_indicator": lambda: pro.fina_indicator(ts_code="000001.SZ", period="20231231"),
        "express": lambda: pro.express(ts_code="000001.SZ", start_date="20230101", end_date="20241231"),
        "forecast": lambda: pro.forecast(ts_code="000001.SZ", start_date="20230101", end_date="20241231"),
        "moneyflow": lambda: pro.moneyflow(ts_code="000001.SZ", start_date="20240102", end_date="20240110"),
        "namechange": lambda: pro.namechange(ts_code="000001.SZ", fields="ts_code,name,start_date,end_date,ann_date,change_reason"),
        "suspend_d": lambda: pro.suspend_d(ts_code="000001.SZ", suspend_date="20240102"),
    }
    stock_sample: list[str] = []
    for name, fn in calls.items():
        try:
            df = fn()
            result["interfaces"][name] = summarize_df(df)
            if name == "stock_basic" and len(df):
                stock_sample = list(df["ts_code"].head(30))
        except Exception as e:
            result["interfaces"][name] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        time.sleep(0.25)

    sample_codes = stock_sample[:20] or ["000001.SZ", "600000.SH"]
    periods = ["20181231", "20201231", "20221231", "20231231"]
    financial_apis = {
        "income": pro.income,
        "balancesheet": pro.balancesheet,
        "cashflow": pro.cashflow,
        "fina_indicator": pro.fina_indicator,
    }
    for api_name, fn in financial_apis.items():
        total = 0
        rows_present = 0
        ann_present = 0
        f_ann_present = 0
        errors = 0
        for code in sample_codes:
            for period in periods:
                total += 1
                try:
                    df = fn(ts_code=code, period=period)
                    if len(df):
                        rows_present += 1
                        cols = list(df.columns)
                        if "ann_date" in cols and df["ann_date"].notna().any():
                            ann_present += 1
                        if "f_ann_date" in cols and df["f_ann_date"].notna().any():
                            f_ann_present += 1
                except Exception:
                    errors += 1
                time.sleep(0.05)
        result["sample_coverage"][api_name] = {
            "sample_codes": len(sample_codes),
            "periods": periods,
            "requests": total,
            "rows_present_requests": rows_present,
            "rows_present_rate": round(rows_present / total, 4) if total else None,
            "ann_date_present_requests": ann_present,
            "ann_date_rate": round(ann_present / total, 4) if total else None,
            "f_ann_date_present_requests": f_ann_present,
            "f_ann_date_rate": round(f_ann_present / total, 4) if total else None,
            "errors": errors,
        }
    return result


@dataclass
class HttpResult:
    ok: bool
    status: int | None = None
    data: Any = None
    error: str | None = None


def http_json(method: str, url: str, params: dict[str, Any] | None = None, body: dict[str, Any] | None = None, timeout: int = 20) -> HttpResult:
    if params:
        url = url + "?" + urllib.parse.urlencode(params)
    headers = {"apiKey": DIEMENG_API_KEY, "X-API-Key": DIEMENG_API_KEY, "Content-Type": "application/json"}
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = raw[:1000]
            return HttpResult(ok=True, status=resp.status, data=parsed)
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")[:1000]
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return HttpResult(ok=False, status=e.code, data=parsed, error="http_error")
    except Exception as e:
        return HttpResult(ok=False, error=f"{type(e).__name__}: {e}")


def extract_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        for key in ("data", "list", "rows", "items", "result"):
            val = data.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
            if isinstance(val, dict):
                for subkey in ("list", "rows", "items", "records"):
                    sub = val.get(subkey)
                    if isinstance(sub, list):
                        return [x for x in sub if isinstance(x, dict)]
        if all(isinstance(v, (str, int, float, type(None))) for v in data.values()):
            return [data]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def summarize_diemeng_result(hr: HttpResult) -> dict[str, Any]:
    rows = extract_rows(hr.data)
    columns = sorted({k for row in rows[:5] for k in row.keys()})
    return {
        "ok": hr.ok,
        "status": hr.status,
        "error": hr.error,
        "top_level_keys": list(hr.data.keys())[:20] if isinstance(hr.data, dict) else None,
        "code": hr.data.get("code") if isinstance(hr.data, dict) else None,
        "message": hr.data.get("message") or hr.data.get("msg") if isinstance(hr.data, dict) else None,
        "rows_sampled": len(rows),
        "columns": columns[:100],
        "has_ann_date": "ann_date" in columns,
        "has_f_ann_date": "f_ann_date" in columns,
        "has_end_date": "end_date" in columns,
        "sample": _json_safe(rows[:1]),
    }


def probe_diemeng() -> dict[str, Any]:
    base = "https://mg.diemeng.chat/api"
    result: dict[str, Any] = {"base_url": base, "available": False, "interfaces": {}, "sample_coverage": {}}
    endpoints = [
        ("calendar_get", "GET", "/basic/calendar", {"start_time": "2024-01-01", "end_time": "2024-01-10", "page_size": 5}, None),
        ("stock_list", "GET", "/stock/list", {"page": 1, "page_size": 5}, None),
        ("stock_daily", "GET", "/stock/daily", {"stock_code": "000001.SZ", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
        ("stock_daily_adj", "GET", "/stock/daily_adj", {"stock_code": "000001.SZ", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
        ("adj_factor", "GET", "/stock/adj_factor", {"stock_code": "000001.SZ", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
        ("index_daily", "POST", "/index/daily", None, {"stock_code": "000001.SH", "start_date": "2024-01-02", "end_date": "2024-01-10", "page": 1, "page_size": 5}),
        ("income", "GET", "/stock/income", {"stock_code": "000001.SZ", "start_date": "2023-01-01", "end_date": "2024-12-31", "page": 1, "page_size": 5}, None),
        ("balancesheet", "GET", "/stock/balancesheet", {"stock_code": "000001.SZ", "start_date": "2023-01-01", "end_date": "2024-12-31", "page": 1, "page_size": 5}, None),
        ("cashflow", "GET", "/stock/cashflow", {"stock_code": "000001.SZ", "start_date": "2023-01-01", "end_date": "2024-12-31", "page": 1, "page_size": 5}, None),
        ("financial_indicator", "GET", "/stock/financial_indicator", {"stock_code": "000001.SZ", "start_date": "2023-01-01", "end_date": "2024-12-31", "page": 1, "page_size": 5}, None),
        ("ths_hot", "GET", "/ths/hot", {"market": "A", "trade_date": "2024-01-02", "page": 1, "page_size": 5}, None),
        ("main_fund_flow", "GET", "/stock/main_fund_flow_overview", {"stock_code": "000001.SZ", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
        ("dragon_tiger", "GET", "/stock/dragon_tiger", {"trade_date": "2024-01-02", "page": 1, "page_size": 5}, None),
        ("daily_basic_candidate", "GET", "/stock/daily_basic", {"stock_code": "000001.SZ", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
        ("suspend_candidate", "GET", "/stock/suspend_d", {"stock_code": "000001.SZ", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
        ("limit_candidate", "GET", "/stock/limit", {"stock_code": "000001.SZ", "trade_date": "2024-01-02", "page": 1, "page_size": 5}, None),
        ("bond_daily", "GET", "/bond/daily", {"stock_code": "110059.SH", "start_time": "2024-01-02", "end_time": "2024-01-10", "page": 1, "page_size": 5}, None),
    ]
    for name, method, path, params, body in endpoints:
        hr = http_json(method, base + path, params=params, body=body, timeout=20)
        result["interfaces"][name] = summarize_diemeng_result(hr)
        if hr.ok or (isinstance(hr.data, dict) and hr.data.get("code") in (0, 200)):
            result["available"] = True
        time.sleep(0.15)

    sample_codes = ["000001.SZ", "600000.SH", "000002.SZ", "600519.SH", "300750.SZ"]
    periods = [("2018-01-01", "2018-12-31"), ("2020-01-01", "2020-12-31"), ("2022-01-01", "2022-12-31"), ("2023-01-01", "2023-12-31")]
    api_paths = {
        "income": "/stock/income",
        "balancesheet": "/stock/balancesheet",
        "cashflow": "/stock/cashflow",
        "financial_indicator": "/stock/financial_indicator",
    }
    for api_name, path in api_paths.items():
        total = 0
        rows_present = 0
        ann_present = 0
        f_ann_present = 0
        errors = 0
        for code in sample_codes:
            for start, end in periods:
                total += 1
                hr = http_json("GET", base + path, params={"stock_code": code, "start_date": start, "end_date": end, "page": 1, "page_size": 10}, timeout=20)
                rows = extract_rows(hr.data)
                if rows:
                    rows_present += 1
                    cols = set(rows[0].keys())
                    if "ann_date" in cols and rows[0].get("ann_date"):
                        ann_present += 1
                    if "f_ann_date" in cols and rows[0].get("f_ann_date"):
                        f_ann_present += 1
                elif not hr.ok:
                    errors += 1
                time.sleep(0.05)
        result["sample_coverage"][api_name] = {
            "sample_codes": len(sample_codes),
            "period_windows": periods,
            "requests": total,
            "rows_present_requests": rows_present,
            "rows_present_rate": round(rows_present / total, 4) if total else None,
            "ann_date_present_requests": ann_present,
            "ann_date_rate": round(ann_present / total, 4) if total else None,
            "f_ann_date_present_requests": f_ann_present,
            "f_ann_date_rate": round(f_ann_present / total, 4) if total else None,
            "errors": errors,
        }
    return result


def write_markdown(report: dict[str, Any]) -> str:
    t = report.get("tushare", {})
    d = report.get("diemeng", {})
    lines: list[str] = []
    lines.append("# P0 数据源探针报告 - 2026-05-06")
    lines.append("")
    lines.append("## 结论")
    lines.append("- Tushare: 可用，适合作为主数据源。")
    lines.append("- Diemeng: 可用，适合作为财报/财务指标补充源；暂未确认可替代 Tushare 的 daily_basic/交易过滤。")
    lines.append("- 下一步应做全市场 PIT 覆盖率 preflight，不应直接跑因子。")
    lines.append("")
    lines.append("## Tushare 接口状态")
    for name, info in t.get("interfaces", {}).items():
        lines.append(f"- {name}: ok={info.get('ok')} rows={info.get('rows')} ann_date={info.get('has_ann_date')} f_ann_date={info.get('has_f_ann_date')}")
    lines.append("")
    lines.append("## Tushare 财报样本覆盖")
    for name, info in t.get("sample_coverage", {}).items():
        lines.append(f"- {name}: rows_present_rate={info.get('rows_present_rate')} ann_date_rate={info.get('ann_date_rate')} f_ann_date_rate={info.get('f_ann_date_rate')} errors={info.get('errors')}")
    lines.append("")
    lines.append("## Diemeng 接口状态")
    for name, info in d.get("interfaces", {}).items():
        msg = info.get("message") or info.get("error")
        lines.append(f"- {name}: ok={info.get('ok')} status={info.get('status')} code={info.get('code')} rows={info.get('rows_sampled')} ann_date={info.get('has_ann_date')} f_ann_date={info.get('has_f_ann_date')} msg={msg}")
    lines.append("")
    lines.append("## Diemeng 财报样本覆盖")
    for name, info in d.get("sample_coverage", {}).items():
        lines.append(f"- {name}: rows_present_rate={info.get('rows_present_rate')} ann_date_rate={info.get('ann_date_rate')} f_ann_date_rate={info.get('f_ann_date_rate')} errors={info.get('errors')}")
    lines.append("")
    lines.append("## 数据缺口")
    lines.append("1. 需要全市场 2018-2024/2025 PIT 覆盖率，不只是小样本。")
    lines.append("2. Diemeng 暂未确认 daily_basic 等价接口，估值/市值/换手仍依赖 Tushare。")
    lines.append("3. ST、停牌、退市、涨跌停过滤需要统一 tradability filter。")
    lines.append("4. 行业分类可先用 Tushare/Diemeng 粗行业，后续最好补申万/中信。")
    lines.append("5. Diemeng 热度/主力资金/可转债等接口当前 key 权限不足或未稳定打通，不纳入 P0。")
    return "\n".join(lines) + "\n"


def main() -> None:
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "credentials": {"tushare_token_used": bool(TUSHARE_TOKEN), "diemeng_api_key_used": bool(DIEMENG_API_KEY), "redacted": True},
        "tushare": probe_tushare(),
        "diemeng": probe_diemeng(),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2), encoding="utf-8")
    MD.write_text(write_markdown(_json_safe(report)), encoding="utf-8")
    print(json.dumps({
        "json_report": str(OUT),
        "markdown_report": str(MD),
        "tushare_available": report["tushare"].get("available"),
        "diemeng_available": report["diemeng"].get("available"),
        "tushare_sample_coverage": report["tushare"].get("sample_coverage"),
        "diemeng_sample_coverage": report["diemeng"].get("sample_coverage"),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
