from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

BLOCKED_ACTIONS = [
    "controlled_backtest",
    "queue_write",
    "timer_enable",
    "broad_daemon_restore",
    "auto_promotion",
    "live_trading",
]
REQUIRED_FIELDS = ["profit_yoy", "debt_to_asset", "operating_cashflow_to_profit"]


def _parse_date_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.replace(r"\.0$", "", regex=True)
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    return parsed.fillna(pd.to_datetime(series, errors="coerce"))


def build_proxy_pit_alignment_review(*, run_id: str, pit_frame: pd.DataFrame, pit_path: str) -> dict[str, Any]:
    frame = pit_frame.copy()
    if frame.empty:
        decision = "blocked_empty_pit_frame"
        usable = frame
    else:
        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        frame["pit_source_ann_date_parsed"] = _parse_date_series(frame["pit_source_ann_date"]) if "pit_source_ann_date" in frame.columns else pd.NaT
        frame["pit_source_end_date_parsed"] = _parse_date_series(frame["pit_source_end_date"]) if "pit_source_end_date" in frame.columns else pd.NaT
        field_mask = pd.Series(True, index=frame.index)
        for field in REQUIRED_FIELDS:
            field_mask &= frame[field].notna() if field in frame.columns else False
        validated = frame["pit_feature_validated"].fillna(False).astype(bool) if "pit_feature_validated" in frame.columns else pd.Series(False, index=frame.index)
        ann_aligned = frame["pit_source_ann_date_parsed"].notna() & (frame["pit_source_ann_date_parsed"] <= frame["date"])
        end_aligned = frame["pit_source_end_date_parsed"].notna() & (frame["pit_source_end_date_parsed"] <= frame["date"])
        usable = frame[field_mask & validated & ann_aligned & end_aligned]
        decision = "prepare_proxy_cheap_screen_plan" if len(usable) / max(1, len(frame)) >= 0.60 else "block_proxy_pit_alignment"
    total_rows = int(len(frame))
    usable_rows = int(len(usable))
    usable_coverage = float(usable_rows / total_rows) if total_rows else 0.0
    field_coverage = {field: float(frame[field].notna().mean()) if field in frame.columns and total_rows else 0.0 for field in REQUIRED_FIELDS}
    validated_coverage = float(frame["pit_feature_validated"].fillna(False).astype(bool).mean()) if "pit_feature_validated" in frame.columns and total_rows else 0.0
    ann_alignment_coverage = float((frame["pit_source_ann_date_parsed"].notna() & (frame["pit_source_ann_date_parsed"] <= frame["date"])).mean()) if total_rows and "pit_source_ann_date_parsed" in frame.columns else 0.0
    end_alignment_coverage = float((frame["pit_source_end_date_parsed"].notna() & (frame["pit_source_end_date_parsed"] <= frame["date"])).mean()) if total_rows and "pit_source_end_date_parsed" in frame.columns else 0.0
    return {
        "schema_version": 1,
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "proxy_pit_alignment_review",
        "pit_path": pit_path,
        "decision": decision,
        "recommended_next_step": "write_proxy_cheap_screen_plan" if decision == "prepare_proxy_cheap_screen_plan" else "inspect_pit_alignment_blockers",
        "total_rows": total_rows,
        "usable_rows": usable_rows,
        "usable_coverage": usable_coverage,
        "field_coverage": field_coverage,
        "pit_feature_validated_coverage": validated_coverage,
        "ann_date_alignment_coverage": ann_alignment_coverage,
        "end_date_alignment_coverage": end_alignment_coverage,
        "required_fields": REQUIRED_FIELDS,
        "controlled_execution_allowed": False,
        "queue_write_allowed": False,
        "timer_enable_allowed": False,
        "blocked_actions": BLOCKED_ACTIONS,
        "next_allowed_actions": ["write_proxy_cheap_screen_plan"] if decision == "prepare_proxy_cheap_screen_plan" else ["inspect_pit_alignment_blockers"],
    }


def proxy_pit_alignment_review_to_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Proxy PIT Alignment Review",
        "",
        f"decision: {report.get('decision')}",
        f"recommended_next_step: {report.get('recommended_next_step')}",
        f"controlled_execution_allowed: {report.get('controlled_execution_allowed')}",
        f"queue_write_allowed: {report.get('queue_write_allowed')}",
        "",
        "## Coverage",
        f"- total_rows: {report.get('total_rows')}",
        f"- usable_rows: {report.get('usable_rows')}",
        f"- usable_coverage: {report.get('usable_coverage')}",
        f"- pit_feature_validated_coverage: {report.get('pit_feature_validated_coverage')}",
        f"- ann_date_alignment_coverage: {report.get('ann_date_alignment_coverage')}",
        f"- end_date_alignment_coverage: {report.get('end_date_alignment_coverage')}",
        "",
        "## Field coverage",
    ]
    for field, value in (report.get("field_coverage") or {}).items():
        lines.append(f"- {field}: {value}")
    lines += ["", "## Blocked actions"]
    lines.extend(f"- {action}" for action in report.get("blocked_actions") or [])
    return "\n".join(lines).rstrip() + "\n"


def write_proxy_pit_alignment_review(report: dict[str, Any], output_dir: str | Path) -> dict[str, Path]:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "proxy_pit_alignment.json"
    markdown_path = out / "proxy_pit_alignment.md"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(proxy_pit_alignment_review_to_markdown(report), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}
