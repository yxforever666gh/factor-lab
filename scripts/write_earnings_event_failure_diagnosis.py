#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from factor_lab.earnings_event_failure_diagnosis import build_failure_diagnosis, load_json_if_exists

BASE = Path("artifacts/earnings_event_controlled_probe")
RUN_DIR = BASE / "high_express_diluted_roe_yoy_workflow"
DATASET = RUN_DIR / "dataset.csv"
JSON_OUT = BASE / "failure_diagnosis.json"
MD_OUT = BASE / "failure_diagnosis.md"
KNOWLEDGE_OUT = Path("knowledge/earnings_event_failure_diagnosis.md")


def _json_safe(obj: Any) -> Any:
    if hasattr(obj, "item"):
        try:
            return obj.item()
        except Exception:
            pass
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return str(obj)


def _write_md(report: dict[str, Any]) -> str:
    decision = report.get("decision", {})
    cov = report.get("coverage", {})
    profile = report.get("bucket_profile", {})
    spreads = profile.get("spreads", {})
    ticker_diag = report.get("ticker_concentration", {})
    workflow = report.get("workflow_instability", {})
    lines = [
        "# 第 13 轮：earnings event failure diagnosis",
        "",
        "Scope: read-only diagnosis only. No workflow enqueue, no daemon start, no new API pull.",
        "",
        "## Decision",
        f"- Decision: `{decision.get('decision')}`",
        f"- Reasons: {', '.join(decision.get('reasons') or [])}",
        f"- Benchmark: {report.get('benchmark_spread')}",
        "",
        "## Coverage",
        f"- Dataset rows: {cov.get('dataset_rows')}",
        f"- Dataset dates: {cov.get('dataset_dates')}",
        f"- Dataset tickers: {cov.get('dataset_tickers')}",
        f"- Non-null signal rows: {cov.get('nonnull_signal_rows')}",
        f"- Non-null signal tickers: {cov.get('nonnull_signal_tickers')}",
        f"- Active diagnostic rows: {cov.get('active_rows')}",
        f"- Active diagnostic dates: {cov.get('active_dates')}",
        f"- Active diagnostic tickers: {cov.get('active_tickers')}",
        "",
        "## Bucket spreads",
    ]
    for pair, item in sorted(spreads.items()):
        lines.append(f"- {pair}: spread={item.get('spread_mean')}, positive_rate={item.get('positive_rate')}, obs={item.get('observations')}")
    best_pair = profile.get("best_pair") or {}
    lines.extend([
        f"- Best pair: {best_pair.get('pair')} spread={best_pair.get('spread_mean')}",
        "",
        "## Ticker concentration / leave-one-out",
        f"- Min leave-one-ticker-out Q3-Q0 spread: {ticker_diag.get('min_leave_one_out_q3_q0_spread')}",
        f"- All leave-one-out above benchmark: {ticker_diag.get('all_leave_one_out_above_benchmark')}",
        "- Top active tickers:",
    ])
    for row in ticker_diag.get("top_tickers_by_active_rows") or []:
        lines.append(f"  - {row.get('ticker')}: rows={row.get('rows')}, row_share={row.get('row_share')}, mean_return={row.get('mean_forward_return_5d')}")
    lines.extend([
        "",
        "## Workflow instability evidence",
        f"- Standard pass_gate: {workflow.get('standard_pass_gate')}",
        f"- Standard fail_reason: {workflow.get('standard_fail_reason')}",
        f"- Standard sharpe_net: {workflow.get('standard_sharpe_net')}",
        f"- Rolling pass count: {workflow.get('rolling_pass_count')} / {workflow.get('rolling_count')}",
        f"- Split pass count: {workflow.get('split_pass_count')} / {workflow.get('split_count')}",
        "",
        "## Interpretation",
        "- 虽然早前 bucket-aware Q3-Q0 略高于 benchmark，但本诊断发现信号覆盖极窄且 standard/rolling/split workflow gate 全部失败。",
        "- 因此 earnings event 当前不能进入扩展或 controlled workflow；应停止该路线并切换下一数据源/机制。",
    ])
    text = "\n".join(lines) + "\n"
    MD_OUT.write_text(text, encoding="utf-8")
    KNOWLEDGE_OUT.parent.mkdir(parents=True, exist_ok=True)
    KNOWLEDGE_OUT.write_text(text, encoding="utf-8")
    return text


def main() -> None:
    if not DATASET.exists():
        raise FileNotFoundError(DATASET)
    BASE.mkdir(parents=True, exist_ok=True)
    dataset = pd.read_csv(DATASET)
    summary = load_json_if_exists(BASE / "controlled_workflow_summary.json") or {}
    rolling = load_json_if_exists(RUN_DIR / "rolling_results.json") or []
    split = load_json_if_exists(RUN_DIR / "split_results.json") or []
    report = build_failure_diagnosis(dataset, summary=summary, rolling_results=rolling, split_results=split)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["inputs"] = {
        "dataset": str(DATASET),
        "summary": str(BASE / "controlled_workflow_summary.json"),
        "rolling_results": str(RUN_DIR / "rolling_results.json"),
        "split_results": str(RUN_DIR / "split_results.json"),
    }
    JSON_OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=_json_safe), encoding="utf-8")
    _write_md(report)
    print(json.dumps({"artifact": str(JSON_OUT), "decision": report.get("decision"), "coverage": report.get("coverage"), "q3_q0": (report.get("bucket_profile", {}).get("spreads", {}).get("Q3-Q0"))}, ensure_ascii=False, indent=2, default=_json_safe))


if __name__ == "__main__":
    main()
