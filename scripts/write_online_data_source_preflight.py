#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from factor_lab.online_data_source_preflight import (
    DIEMENG_CANDIDATES,
    TUSHARE_CANDIDATES,
    EndpointProbeResult,
    build_final_decision,
    detect_date_fields,
    evaluate_specs,
    rank_decisions,
    safe_records_from_frame,
)
from factor_lab.settings import load_env_file

load_env_file()

ARTIFACT_DIR = Path("artifacts/online_data_source_preflight")
JSON_OUT = ARTIFACT_DIR / "online_data_source_preflight.json"
MD_OUT = ARTIFACT_DIR / "online_data_source_preflight.md"
KNOWLEDGE_OUT = Path("knowledge/online_data_source_candidates.md")
BASE_DIEMENG = "https://mg.diemeng.chat/api"

START_DATE = "20230101"
END_DATE = "20231231"
SAMPLE_TRADE_DATE = "20231229"
SAMPLE_PERIOD = "20231231"


def _token(name: str) -> str | None:
    return os.environ.get(name) or None


def _json_safe_error(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text[:500]


def _try_tushare_call(pro: Any, endpoint: str, sample_code: str | None) -> Any:
    fn = getattr(pro, endpoint)
    variants: list[dict[str, Any]] = []
    if endpoint in {"margin", "margin_detail", "top_list", "block_trade"}:
        variants.extend([
            {"trade_date": SAMPLE_TRADE_DATE},
            {"start_date": START_DATE, "end_date": END_DATE},
        ])
    if endpoint in {"forecast", "express", "repurchase", "stk_holdertrade", "report_rc"}:
        variants.extend([
            {"start_date": START_DATE, "end_date": END_DATE},
            {"ann_date": SAMPLE_TRADE_DATE},
        ])
    if endpoint in {"top10_holders", "top10_floatholders", "pledge_stat", "pledge_detail", "stk_holdernumber"}:
        if sample_code:
            variants.extend([
                {"ts_code": sample_code, "start_date": START_DATE, "end_date": END_DATE},
                {"ts_code": sample_code, "end_date": SAMPLE_PERIOD},
                {"ts_code": sample_code},
            ])
        variants.append({"start_date": START_DATE, "end_date": END_DATE})
    if not variants:
        variants.append({"start_date": START_DATE, "end_date": END_DATE})
    last_error: Exception | None = None
    for params in variants:
        try:
            df = fn(**params)
            if df is not None and len(df) > 0:
                return df
        except Exception as exc:
            last_error = exc
        time.sleep(0.05)
    if last_error:
        raise last_error
    return None


def _tushare_caller(pro: Any, sample_code: str | None):
    def call(spec: Any, endpoint: str) -> EndpointProbeResult:
        if not hasattr(pro, endpoint):
            return EndpointProbeResult("tushare", spec.source_id, endpoint, False, 0, (), (), "method_not_found")
        try:
            df = _try_tushare_call(pro, endpoint, sample_code)
            cols = tuple(str(c) for c in list(getattr(df, "columns", []))) if df is not None else ()
            rows = int(len(df)) if df is not None else 0
            return EndpointProbeResult(
                provider="tushare",
                source_id=spec.source_id,
                endpoint=endpoint,
                success=rows > 0,
                rows=rows,
                columns=cols,
                date_fields=detect_date_fields(cols),
                sample=safe_records_from_frame(df),
                error=None if rows > 0 else "empty_result",
            )
        except Exception as exc:
            return EndpointProbeResult("tushare", spec.source_id, endpoint, False, 0, (), (), _json_safe_error(exc))
    return call


def _rows(obj: Any) -> list[dict[str, Any]]:
    if not isinstance(obj, dict):
        return []
    for key in ("data", "result"):
        val = obj.get(key)
        if isinstance(val, list):
            return [r for r in val if isinstance(r, dict)]
        if isinstance(val, dict):
            for sk in ("list", "rows", "items", "records"):
                sv = val.get(sk)
                if isinstance(sv, list):
                    return [r for r in sv if isinstance(r, dict)]
    return []


def _diemeng_post(path: str, key: str | None, body: dict[str, Any]) -> tuple[bool, Any]:
    if not key:
        return False, {"error": "missing_api_key"}
    req = urllib.request.Request(
        BASE_DIEMENG + path,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={"apiKey": key, "X-API-Key": key, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return True, json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = {"raw": raw[:500]}
        return False, {"status": exc.code, **body}
    except Exception as exc:
        return False, {"error": _json_safe_error(exc)}


def _diemeng_caller(key: str | None):
    def call(spec: Any, endpoint: str) -> EndpointProbeResult:
        variants = [
            {"start_date": "2023-01-01", "end_date": "2023-12-31", "page": 1, "page_size": 100},
            {"trade_date": "2023-12-29", "page": 1, "page_size": 100},
            {"end_date": "2023-12-31", "page": 1, "page_size": 100},
        ]
        last_obj: Any = None
        for body in variants:
            ok, obj = _diemeng_post(endpoint, key, body)
            last_obj = obj
            rs = _rows(obj)
            if ok and rs:
                cols = tuple(sorted({str(k) for row in rs[:20] for k in row.keys()}))
                sample = tuple(rs[:3])
                return EndpointProbeResult("diemeng", spec.source_id, endpoint, True, len(rs), cols, detect_date_fields(cols), None, sample)
            time.sleep(0.05)
        err = "empty_result"
        if isinstance(last_obj, dict):
            err = json.dumps(last_obj, ensure_ascii=False)[:500]
        return EndpointProbeResult("diemeng", spec.source_id, endpoint, False, 0, (), (), err)
    return call


def _write_md(report: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# Online Data Source Preflight")
    lines.append("")
    lines.append("Scope: bounded API sample only. No factor run, no queue write, no daemon start, no full-market pull.")
    lines.append("")
    final = report["final_decision"]
    lines.append("## Final decision")
    lines.append(f"- Decision: `{final['decision']}`")
    lines.append(f"- Reason: {final['reason']}")
    selected = final.get("selected_source")
    if selected:
        lines.append(f"- Selected source: {selected['provider']} / {selected['source_id']} / {selected['display_name']}")
    lines.append("")
    lines.append("## Candidate ranking")
    lines.append("| rank | provider | source | recommendation | score | pit/date control | frequency | rows | endpoint | blockers |")
    lines.append("|---:|---|---|---|---:|---|---|---:|---|---|")
    for idx, item in enumerate(report["ranked_candidates"], start=1):
        best = item.get("best_endpoint") or {}
        lines.append(
            f"| {idx} | {item['provider']} | {item['display_name']} | {item['recommendation']} | {item['score']} | {item['pit_control']} | {item['frequency_hint']} | {best.get('rows')} | {best.get('endpoint')} | {', '.join(item.get('blockers') or [])} |"
        )
    lines.append("")
    lines.append("## Interpretation")
    lines.append("- `mvp_candidate` means the source returned sample rows and has a trade_date or announcement-date control suitable for a small MVP.")
    lines.append("- `manual_review_before_research` means there are rows but the date/PIT control is not enough for immediate factor research.")
    lines.append("- `blocked_no_access_or_no_rows` means this bounded probe could not obtain usable rows; it may still be available with different params or permissions, but should not be assumed usable.")
    MD_OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    token = _token("TUSHARE_TOKEN")
    diemeng_key = _token("DIEMENG_API_KEY")
    tushare_auth_ready = bool(token)
    diemeng_auth_ready = bool(diemeng_key)
    tushare_decisions = []
    tushare_error = None
    sample_code = None
    if token:
        try:
            import tushare as ts  # type: ignore
            pro = ts.pro_api(token)
            stock_basic = pro.stock_basic(exchange="", list_status="L", fields="ts_code,name,industry,list_date")
            if stock_basic is not None and len(stock_basic):
                sample_code = str(stock_basic.iloc[0]["ts_code"])
            tushare_decisions = evaluate_specs(TUSHARE_CANDIDATES, _tushare_caller(pro, sample_code))
        except Exception as exc:
            tushare_error = _json_safe_error(exc)
    diemeng_decisions = evaluate_specs(DIEMENG_CANDIDATES, _diemeng_caller(diemeng_key)) if diemeng_key else []
    decisions = tushare_decisions + diemeng_decisions
    ranked = rank_decisions(decisions)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "bounded online source preflight for new alpha information sources",
        "credentials_redacted": True,
        "auth_ready": {"tushare": tushare_auth_ready, "diemeng": diemeng_auth_ready},
        "sample_params": {"start_date": START_DATE, "end_date": END_DATE, "trade_date": SAMPLE_TRADE_DATE, "period": SAMPLE_PERIOD, "sample_code": sample_code},
        "tushare_error": tushare_error,
        "no_factor_run": True,
        "no_queue_write": True,
        "no_daemon_start": True,
        "no_full_market_pull": True,
        "ranked_candidates": [d.to_dict() for d in ranked],
        "final_decision": build_final_decision(ranked),
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    _write_md(report)
    print(json.dumps({
        "json": str(JSON_OUT),
        "markdown": str(MD_OUT),
        "knowledge": str(KNOWLEDGE_OUT),
        "auth_ready": report["auth_ready"],
        "final_decision": report["final_decision"],
        "top_candidates": report["ranked_candidates"][:5],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
