from __future__ import annotations

import html
from pathlib import Path
import sqlite3

from factor_lab.db_views import ensure_views


def build_index_page(db_path: str | Path, output_path: str | Path) -> None:
    ensure_views(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    latest_runs = cur.execute(
        "SELECT run_id, created_at_utc, status, config_path FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 10"
    ).fetchall()
    latest_candidates = cur.execute(
        "SELECT factor_name, COUNT(*) FROM factor_results WHERE variant='candidate' GROUP BY factor_name ORDER BY COUNT(*) DESC, factor_name ASC"
    ).fetchall()
    candidate_leaderboard = cur.execute(
        """
        SELECT name, family, factor_role, status, research_stage, COALESCE(latest_recent_final_score, latest_final_score, 0), evaluation_count, window_count
        FROM v_factor_candidate_leaderboard
        ORDER BY COALESCE(latest_recent_final_score, latest_final_score, -999) DESC, evaluation_count DESC
        LIMIT 10
        """
    ).fetchall()
    failed_runs = cur.execute(
        "SELECT run_id, created_at_utc, config_path FROM workflow_runs WHERE status='failed' ORDER BY created_at_utc DESC LIMIT 10"
    ).fetchall()
    family_summary = cur.execute(
        "SELECT family, candidate_count, promising_count, testing_count, rejected_count, ROUND(avg_latest_score, 6) FROM v_candidate_family_summary ORDER BY COALESCE(avg_latest_score, -999) DESC, candidate_count DESC LIMIT 10"
    ).fetchall()
    relationship_pairs = cur.execute(
        "SELECT left_name, right_name, relationship_type, ROUND(strength, 6) FROM v_candidate_relationship_pairs ORDER BY COALESCE(strength, 0) DESC, updated_at_utc DESC LIMIT 10"
    ).fetchall()

    def table(headers, rows):
        return (
            "<table><thead><tr>"
            + "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
            + "</tr></thead><tbody>"
            + "".join(
                "<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row) + "</tr>"
                for row in rows
            )
            + "</tbody></table>"
        )

    page = f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <title>Factor Lab Index</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 24px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px 10px; text-align: left; }}
    th {{ background: #f5f5f5; }}
    .links a {{ display: inline-block; margin-right: 12px; }}
  </style>
</head>
<body>
  <h1>Factor Lab Index</h1>
  <div class=\"links\">
    <a href=\"report.html\">HTML report</a>
    <a href=\"sqlite_report.md\">SQLite markdown report</a>
  </div>
  <h2>Candidate Leaderboard</h2>
  {table(['Name','Family','Role','Status','Stage','Latest Score','Evaluations','Windows'], candidate_leaderboard)}
  <h2>Latest Runs</h2>
  {table(['Run ID','Created At UTC','Status','Config'], latest_runs)}
  <h2>Stable Candidates</h2>
  {table(['Factor','Candidate Runs'], latest_candidates)}
  <h2>Candidate Families</h2>
  {table(['Family','Candidates','Promising','Testing','Rejected','Avg Latest'], family_summary)}
  <h2>Candidate Relationship Pairs</h2>
  {table(['Left','Right','Type','Strength'], relationship_pairs)}
  <h2>Failed Runs</h2>
  {table(['Run ID','Created At UTC','Config'], failed_runs) if failed_runs else '<p>None</p>'}
</body>
</html>
"""
    Path(output_path).write_text(page, encoding="utf-8")
