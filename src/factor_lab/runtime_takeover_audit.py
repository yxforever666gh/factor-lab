from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from factor_lab.runtime_takeover_policy import RuntimeTakeoverPolicy, load_runtime_takeover_policy

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = ROOT / "artifacts" / "factor_lab.db"
DEFAULT_ACCEPTANCE_PATH = ROOT / "artifacts" / "post_h_acceptance" / "acceptance.json"


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _now(value: str | None = None) -> datetime:
    return _parse_time(value) or datetime.now(timezone.utc)


def _safe_rows(conn: sqlite3.Connection, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    try:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(query, params).fetchall()]
    except sqlite3.Error:
        return []


def _top(rows: list[dict[str, Any]], key: str, limit: int = 10) -> list[dict[str, Any]]:
    counts = Counter(str(row.get(key) or "") for row in rows if row.get(key))
    return [{key: item, "count": count} for item, count in counts.most_common(limit)]


def _acceptance_passed(acceptance_path: str | Path | None) -> bool:
    if acceptance_path is None:
        return False
    path = Path(acceptance_path)
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    return bool(payload.get("ok")) and int(payload.get("passed_round_count") or 0) >= int(payload.get("requested_rounds") or 3)


def _daemon_decision(*, acceptance_path: str | Path | None) -> dict[str, Any]:
    if _acceptance_passed(acceptance_path):
        return {
            "pause_broad_daemon": True,
            "allow_controlled_only_daemon": True,
            "controlled_only_reason": "post_h_three_round_acceptance_passed",
        }
    return {
        "pause_broad_daemon": True,
        "allow_controlled_only_daemon": False,
        "controlled_only_reason": "missing_or_failed_post_h_acceptance",
    }


def build_runtime_takeover_audit(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    policy: RuntimeTakeoverPolicy | None = None,
    now_utc: str | None = None,
    acceptance_path: str | Path | None = DEFAULT_ACCEPTANCE_PATH,
) -> dict[str, Any]:
    policy = policy or load_runtime_takeover_policy()
    now = _now(now_utc)
    since_24h = (now - timedelta(hours=24)).isoformat()
    since_7d = (now - timedelta(days=7)).isoformat()
    db_path = Path(db_path)
    if not db_path.exists():
        return {"schema_version": 1, "error": f"database not found: {db_path}", "recommendations": ["pause_daemon"]}

    conn = sqlite3.connect(db_path)
    try:
        workflow_24h = _safe_rows(conn, "SELECT * FROM workflow_runs WHERE created_at_utc >= ?", (since_24h,))
        workflow_7d = _safe_rows(conn, "SELECT * FROM workflow_runs WHERE created_at_utc >= ?", (since_7d,))
        pending = _safe_rows(conn, "SELECT * FROM research_tasks WHERE status='pending'")
        eval_24h = _safe_rows(conn, "SELECT * FROM factor_evaluations WHERE created_at_utc >= ?", (since_24h,))
    finally:
        conn.close()

    pending_decisions = Counter()
    pending_rows: list[dict[str, Any]] = []
    for task in pending:
        decision = policy.evaluate_task(task)
        pending_decisions[decision["decision"]] += 1
        pending_rows.append({
            "task_id": task.get("task_id"),
            "task_type": task.get("task_type"),
            "worker_note": task.get("worker_note"),
            "policy_decision": decision,
        })

    old_path_markers = ["candidate_earnings_yield_book_yield_recent", "rolling_30d_back", "rolling_60d_back"]
    old_path_count = sum(
        1 for row in workflow_24h
        if any(marker in str(row.get("config_path") or "") or marker in str(row.get("output_dir") or "") for marker in old_path_markers)
    )
    coverage_failures = [row for row in eval_24h if "coverage_too_low" in str(row.get("rejection_reason") or "")]
    split_failures = [row for row in eval_24h if "too_many_split_failures" in str(row.get("rejection_reason") or "")]

    recommendations: list[str] = []
    daemon_decision = _daemon_decision(acceptance_path=acceptance_path)
    if pending_decisions.get("block", 0):
        recommendations.append("clear_or_quarantine_legacy_pending")
    if daemon_decision["allow_controlled_only_daemon"]:
        recommendations.append("pause_broad_daemon")
        recommendations.append("allow_controlled_only_daemon")
    elif old_path_count or len(coverage_failures) > 0 or not recommendations:
        recommendations.append("pause_daemon")

    return {
        "schema_version": 1,
        "generated_at_utc": now.isoformat(),
        "policy": {"enabled": policy.enabled, "mode": policy.mode},
        "workflow_runs": {
            "last_24h": len(workflow_24h),
            "last_7d": len(workflow_7d),
            "old_path_count_24h": old_path_count,
            "top_config_path_24h": _top(workflow_24h, "config_path"),
            "top_output_dir_24h": _top(workflow_24h, "output_dir"),
        },
        "factor_evaluations": {
            "last_24h": len(eval_24h),
            "coverage_too_low_after_full_run": len(coverage_failures),
            "too_many_split_failures_after_full_run": len(split_failures),
        },
        "pending_policy_decisions": dict(pending_decisions),
        "pending_tasks": pending_rows[:50],
        "daemon_decision": daemon_decision,
        "recommendations": recommendations,
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Runtime Takeover Audit",
        "",
        f"Generated: {payload.get('generated_at_utc')}",
        "",
        "## Workflow runs",
        f"- Last 24h: {payload.get('workflow_runs', {}).get('last_24h', 0)}",
        f"- Old path count 24h: {payload.get('workflow_runs', {}).get('old_path_count_24h', 0)}",
        "",
        "## Factor evaluation failures",
        f"- coverage_too_low after full run: {payload.get('factor_evaluations', {}).get('coverage_too_low_after_full_run', 0)}",
        f"- too_many_split_failures after full run: {payload.get('factor_evaluations', {}).get('too_many_split_failures_after_full_run', 0)}",
        "",
        "## Pending policy decisions",
        f"- {payload.get('pending_policy_decisions', {})}",
        "",
        "## Daemon decision",
        f"- pause_broad_daemon: {payload.get('daemon_decision', {}).get('pause_broad_daemon')}",
        f"- allow_controlled_only_daemon: {payload.get('daemon_decision', {}).get('allow_controlled_only_daemon')}",
        f"- reason: {payload.get('daemon_decision', {}).get('controlled_only_reason')}",
        "",
        "## Recommendations",
    ]
    for item in payload.get("recommendations", []):
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def write_runtime_takeover_audit(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    json_path: str | Path = ROOT / "artifacts" / "runtime_takeover_audit.json",
    markdown_path: str | Path = ROOT / "artifacts" / "runtime_takeover_audit.md",
    now_utc: str | None = None,
    acceptance_path: str | Path | None = DEFAULT_ACCEPTANCE_PATH,
) -> dict[str, Any]:
    payload = build_runtime_takeover_audit(db_path=db_path, now_utc=now_utc, acceptance_path=acceptance_path)
    json_path = Path(json_path)
    markdown_path = Path(markdown_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(_markdown(payload), encoding="utf-8")
    return payload
