from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


DEFAULT_DB_PATH = Path("artifacts") / "factor_lab.db"
DEFAULT_JSON_PATH = Path("artifacts") / "research_waste_audit.json"
DEFAULT_MARKDOWN_PATH = Path("artifacts") / "research_waste_audit.md"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return bool(row)


def _rows_as_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    columns = [column[0] for column in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _scalar(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> Any:
    row = conn.execute(query, tuple(params)).fetchone()
    return row[0] if row else None


def _top_rows(conn: sqlite3.Connection, query: str, params: Iterable[Any] = ()) -> list[dict[str, Any]]:
    return _rows_as_dicts(conn.execute(query, tuple(params)))


def _workflow_summary(conn: sqlite3.Connection, since_24h: str, since_7d: str) -> dict[str, Any]:
    if not _table_exists(conn, "workflow_runs"):
        return {"available": False, "last_24h": {}, "last_7d": {}}

    def counts(since: str) -> dict[str, int]:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                COALESCE(SUM(CASE WHEN status = 'finished' THEN 1 ELSE 0 END), 0) AS finished,
                COALESCE(SUM(CASE WHEN status != 'finished' THEN 1 ELSE 0 END), 0) AS non_finished
            FROM workflow_runs
            WHERE created_at_utc >= ?
            """,
            (since,),
        ).fetchone()
        return {"total": int(row[0] or 0), "finished": int(row[1] or 0), "non_finished": int(row[2] or 0)}

    return {
        "available": True,
        "total_all_time": int(_scalar(conn, "SELECT COUNT(*) FROM workflow_runs") or 0),
        "first_created_at_utc": _scalar(conn, "SELECT MIN(created_at_utc) FROM workflow_runs"),
        "latest_created_at_utc": _scalar(conn, "SELECT MAX(created_at_utc) FROM workflow_runs"),
        "last_24h": counts(since_24h),
        "last_7d": counts(since_7d),
    }


def _duplicate_summary_for_window(conn: sqlite3.Connection, since: str) -> dict[str, Any]:
    config_path_top = _top_rows(
        conn,
        """
        SELECT config_path, COUNT(*) AS run_count
        FROM workflow_runs
        WHERE created_at_utc >= ?
        GROUP BY config_path
        ORDER BY run_count DESC, config_path ASC
        LIMIT 20
        """,
        (since,),
    )
    window_top = _top_rows(
        conn,
        """
        SELECT start_date, end_date, universe_limit, COUNT(*) AS run_count
        FROM workflow_runs
        WHERE created_at_utc >= ?
        GROUP BY start_date, end_date, universe_limit
        ORDER BY run_count DESC, start_date ASC, end_date ASC
        LIMIT 20
        """,
        (since,),
    )
    fingerprint_top = _top_rows(
        conn,
        """
        SELECT config_fingerprint, COUNT(*) AS run_count
        FROM workflow_runs
        WHERE created_at_utc >= ? AND config_fingerprint IS NOT NULL AND config_fingerprint != ''
        GROUP BY config_fingerprint
        HAVING COUNT(*) > 1
        ORDER BY run_count DESC, config_fingerprint ASC
        LIMIT 20
        """,
        (since,),
    )
    duplicate_fingerprint_runs = sum(int(row["run_count"]) for row in fingerprint_top)
    total = int(_scalar(conn, "SELECT COUNT(*) FROM workflow_runs WHERE created_at_utc >= ?", (since,)) or 0)
    return {
        "config_path_top": config_path_top,
        "window_top": window_top,
        "config_fingerprint_top": fingerprint_top,
        "duplicate_config_fingerprint_run_count": duplicate_fingerprint_runs,
        "duplicate_config_fingerprint_ratio": round((duplicate_fingerprint_runs / total) if total else 0.0, 4),
    }


def _duplicate_summary(conn: sqlite3.Connection, since_24h: str, since_7d: str) -> dict[str, Any]:
    if not _table_exists(conn, "workflow_runs"):
        return {"available": False}
    last_24h = _duplicate_summary_for_window(conn, since_24h)
    last_7d = _duplicate_summary_for_window(conn, since_7d)
    return {
        "available": True,
        "last_24h": last_24h,
        "last_7d": last_7d,
        # Backward-compatible aliases for existing consumers/tests.
        "config_path_top": last_24h["config_path_top"],
        "window_top": last_24h["window_top"],
        "config_fingerprint_top": last_24h["config_fingerprint_top"],
        "duplicate_config_fingerprint_run_count_24h": last_24h["duplicate_config_fingerprint_run_count"],
        "duplicate_config_fingerprint_ratio_24h": last_24h["duplicate_config_fingerprint_ratio"],
        "duplicate_config_fingerprint_run_count_7d": last_7d["duplicate_config_fingerprint_run_count"],
        "duplicate_config_fingerprint_ratio_7d": last_7d["duplicate_config_fingerprint_ratio"],
    }


def _research_task_summary(conn: sqlite3.Connection, since_24h: str) -> dict[str, Any]:
    if not _table_exists(conn, "research_tasks"):
        return {"available": False}
    return {
        "available": True,
        "status_counts": _top_rows(
            conn,
            "SELECT status, COUNT(*) AS task_count FROM research_tasks GROUP BY status ORDER BY task_count DESC",
        ),
        "fingerprint_top_24h": _top_rows(
            conn,
            """
            SELECT fingerprint, COUNT(*) AS task_count
            FROM research_tasks
            WHERE created_at_utc >= ? AND fingerprint IS NOT NULL AND fingerprint != ''
            GROUP BY fingerprint
            HAVING COUNT(*) > 1
            ORDER BY task_count DESC, fingerprint ASC
            LIMIT 20
            """,
            (since_24h,),
        ),
    }


def _factor_evaluation_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, "factor_evaluations"):
        return {"available": False, "rejection_reasons_top": []}
    return {
        "available": True,
        "total": int(_scalar(conn, "SELECT COUNT(*) FROM factor_evaluations") or 0),
        "status_counts": _top_rows(
            conn,
            """
            SELECT COALESCE(status, 'unknown') AS status, COALESCE(pass_flag, 0) AS pass_flag, COUNT(*) AS evaluation_count
            FROM factor_evaluations
            GROUP BY COALESCE(status, 'unknown'), COALESCE(pass_flag, 0)
            ORDER BY evaluation_count DESC
            LIMIT 20
            """,
        ),
        "rejection_reasons_top": _top_rows(
            conn,
            """
            SELECT rejection_reason, COUNT(*) AS evaluation_count
            FROM factor_evaluations
            WHERE rejection_reason IS NOT NULL AND rejection_reason != ''
            GROUP BY rejection_reason
            ORDER BY evaluation_count DESC, rejection_reason ASC
            LIMIT 20
            """,
        ),
    }


def _candidate_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, "factor_candidates"):
        return {"available": False, "status_counts": []}
    return {
        "available": True,
        "status_counts": _top_rows(
            conn,
            """
            SELECT status, COUNT(*) AS candidate_count,
                   AVG(avg_final_score) AS avg_final_score,
                   AVG(pass_rate) AS avg_pass_rate
            FROM factor_candidates
            GROUP BY status
            ORDER BY candidate_count DESC, status ASC
            """,
        ),
        "high_pass_candidates": _top_rows(
            conn,
            """
            SELECT name, status, evaluation_count, avg_final_score, best_final_score,
                   latest_final_score, pass_rate, rejection_reason, next_action
            FROM factor_candidates
            WHERE evaluation_count >= 5
            ORDER BY pass_rate DESC, avg_final_score DESC
            LIMIT 20
            """,
        ),
    }


def build_research_waste_audit(*, db_path: str | Path = DEFAULT_DB_PATH, now: datetime | None = None) -> dict[str, Any]:
    db_path = Path(db_path)
    now = now or _utc_now()
    since_24h = _iso(now - timedelta(hours=24))
    since_7d = _iso(now - timedelta(days=7))
    audit: dict[str, Any] = {
        "schema_version": "research_waste_audit.v1",
        "generated_at_utc": _iso(now),
        "db_path": str(db_path),
        "windows": {"last_24h_since": since_24h, "last_7d_since": since_7d},
    }
    if not db_path.exists():
        audit["error"] = f"database not found: {db_path}"
        return audit

    conn = sqlite3.connect(db_path)
    try:
        audit["workflow_runs"] = _workflow_summary(conn, since_24h, since_7d)
        audit["duplicates"] = _duplicate_summary(conn, since_24h, since_7d)
        audit["research_tasks"] = _research_task_summary(conn, since_24h)
        audit["factor_evaluations"] = _factor_evaluation_summary(conn)
        audit["factor_candidates"] = _candidate_summary(conn)
        duplicate_ratio = (audit.get("duplicates") or {}).get("duplicate_config_fingerprint_ratio_24h") or 0.0
        top_rejection = ((audit.get("factor_evaluations") or {}).get("rejection_reasons_top") or [{}])[0]
        audit["low_value_repeat_indicators"] = {
            "duplicate_config_fingerprint_ratio_24h": duplicate_ratio,
            "duplicate_config_fingerprint_ratio_7d": (audit.get("duplicates") or {}).get("duplicate_config_fingerprint_ratio_7d") or 0.0,
            "top_repeated_config_path": (((audit.get("duplicates") or {}).get("config_path_top") or [{}])[0]).get("config_path"),
            "top_repeated_window": (((audit.get("duplicates") or {}).get("window_top") or [{}])[0]),
            "top_rejection_reason": top_rejection.get("rejection_reason"),
        }
        audit["recommended_blockers"] = [
            {
                "blocker": "workflow_equivalence_duplicate_control",
                "reason": "Equivalent workflow runs should not be treated as new evidence.",
                "trigger": "same workflow_experiment_fingerprint already finished within governance window",
            },
            {
                "blocker": "coverage_preflight_before_full_run",
                "reason": f"Top rejection reason is {top_rejection.get('rejection_reason') or 'unknown'}.",
                "trigger": "factor/date/ticker coverage below configured threshold",
            },
        ]
        audit["recommendations"] = [
            "Equivalent workflow fingerprinting is enabled for new queued workflow tasks; keep monitoring 24h duplicate ratio.",
            f"Prioritize preflight coverage gates; top rejection reason is {top_rejection.get('rejection_reason')}." if top_rejection.get("rejection_reason") else "No rejection reason data available.",
            "Treat repeated identical windows/configs as repeated evidence, not new alpha evidence.",
        ]
    finally:
        conn.close()
    return audit


def render_research_waste_markdown(audit: dict[str, Any]) -> str:
    lines = [
        "# Research Waste Audit",
        "",
        f"Generated: `{audit.get('generated_at_utc')}`",
        f"Database: `{audit.get('db_path')}`",
        "",
    ]
    if audit.get("error"):
        lines += [f"**Error:** {audit['error']}", ""]
        return "\n".join(lines)

    workflow = audit.get("workflow_runs") or {}
    last_24h = workflow.get("last_24h") or {}
    lines += [
        "## Workflow Runs",
        "",
        f"- Total all time: {workflow.get('total_all_time', 0)}",
        f"- Last 24h total: {last_24h.get('total', 0)}",
        f"- Last 24h finished: {last_24h.get('finished', 0)}",
        f"- Last 24h non-finished: {last_24h.get('non_finished', 0)}",
        f"- Last 7d total: {(workflow.get('last_7d') or {}).get('total', 0)}",
        f"- Last 7d finished: {(workflow.get('last_7d') or {}).get('finished', 0)}",
        "",
        "## Duplicate Evidence",
        "",
        f"- Duplicate config fingerprint runs 24h: {(audit.get('duplicates') or {}).get('duplicate_config_fingerprint_run_count_24h', 0)}",
        f"- Duplicate config fingerprint ratio 24h: {(audit.get('duplicates') or {}).get('duplicate_config_fingerprint_ratio_24h', 0)}",
        f"- Duplicate config fingerprint runs 7d: {(audit.get('duplicates') or {}).get('duplicate_config_fingerprint_run_count_7d', 0)}",
        f"- Duplicate config fingerprint ratio 7d: {(audit.get('duplicates') or {}).get('duplicate_config_fingerprint_ratio_7d', 0)}",
        "",
        "### Top Config Paths",
        "",
    ]
    for row in (audit.get("duplicates") or {}).get("config_path_top") or []:
        lines.append(f"- `{row.get('config_path')}`: {row.get('run_count')}")
    lines += ["", "### Top Windows", ""]
    for row in (audit.get("duplicates") or {}).get("window_top") or []:
        lines.append(f"- `{row.get('start_date')} → {row.get('end_date')}` universe={row.get('universe_limit')}: {row.get('run_count')}")
    lines += ["", "## Research Task Status", ""]
    for row in (audit.get("research_tasks") or {}).get("status_counts") or []:
        lines.append(f"- `{row.get('status')}`: {row.get('task_count')}")
    lines += ["", "## Rejection Reasons", ""]
    for row in (audit.get("factor_evaluations") or {}).get("rejection_reasons_top") or []:
        lines.append(f"- `{row.get('rejection_reason')}`: {row.get('evaluation_count')}")
    lines += ["", "## Candidate Status", ""]
    for row in (audit.get("factor_candidates") or {}).get("status_counts") or []:
        lines.append(f"- `{row.get('status')}`: {row.get('candidate_count')}")
    lines += ["", "## Recommended Blockers", ""]
    for row in audit.get("recommended_blockers") or []:
        lines.append(f"- `{row.get('blocker')}`: {row.get('reason')} Trigger: {row.get('trigger')}")
    lines += ["", "## Recommendations", ""]
    for item in audit.get("recommendations") or []:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def write_research_waste_audit(
    *,
    db_path: str | Path = DEFAULT_DB_PATH,
    json_path: str | Path = DEFAULT_JSON_PATH,
    markdown_path: str | Path = DEFAULT_MARKDOWN_PATH,
    now: datetime | None = None,
) -> dict[str, Any]:
    audit = build_research_waste_audit(db_path=db_path, now=now)
    json_path = Path(json_path)
    markdown_path = Path(markdown_path)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_research_waste_markdown(audit), encoding="utf-8")
    return audit
