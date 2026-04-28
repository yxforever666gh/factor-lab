from __future__ import annotations

import html
import sqlite3
from pathlib import Path

from factor_lab.candidate_graph import build_graph_artifacts
from factor_lab.db_views import ensure_views


def _table(headers, rows):
    thead = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{html.escape(str(cell))}</td>" for cell in row) + "</tr>")
    return f"<table><thead><tr>{thead}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def build_html_report(db_path: str | Path, output_path: str | Path) -> None:
    ensure_views(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    top_factors = cur.execute(
        "SELECT factor_name, ROUND(avg_score, 6), runs FROM v_factor_score_avg ORDER BY avg_score DESC LIMIT 10"
    ).fetchall()
    stable_candidates = cur.execute(
        "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC"
    ).fetchall()
    portfolio_avg = cur.execute(
        "SELECT strategy_name, ROUND(avg_sharpe, 6), ROUND(avg_return, 6), runs FROM v_portfolio_strategy_avg ORDER BY avg_sharpe DESC"
    ).fetchall()
    latest_runs = cur.execute(
        "SELECT run_id, created_at_utc, data_source, start_date, end_date, status FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 10"
    ).fetchall()
    family_summary = cur.execute(
        "SELECT family, candidate_count, promising_count, testing_count, rejected_count, ROUND(avg_latest_score, 6), evaluation_count, window_count FROM v_candidate_family_summary ORDER BY COALESCE(avg_latest_score, -999) DESC, candidate_count DESC LIMIT 10"
    ).fetchall()
    relationship_pairs = cur.execute(
        "SELECT left_name, right_name, relationship_type, ROUND(strength, 6), run_id FROM v_candidate_relationship_pairs ORDER BY COALESCE(strength, 0) DESC, updated_at_utc DESC LIMIT 12"
    ).fetchall()

    html_doc = f"""
<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <title>Factor Lab Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #222; }}
    h1, h2 {{ margin-top: 28px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 24px; }}
    th, td {{ border: 1px solid #ddd; padding: 8px 10px; text-align: left; }}
    th {{ background: #f5f5f5; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 24px; }}
    .note {{ color: #555; }}
  </style>
</head>
<body>
  <h1>Factor Lab HTML Report</h1>
  <p class=\"note\">Auto-generated from SQLite experiment data.</p>

  <div class=\"grid\">
    <section>
      <h2>Top Factors by Average Score</h2>
      {_table(['Factor', 'Avg Score', 'Runs'], top_factors)}
    </section>

    <section>
      <h2>Stable Candidates</h2>
      {_table(['Factor', 'Candidate Runs'], stable_candidates)}
    </section>

    <section>
      <h2>Portfolio Strategy Averages</h2>
      {_table(['Strategy', 'Avg Sharpe', 'Avg Return', 'Runs'], portfolio_avg)}
    </section>

    <section>
      <h2>Candidate Families</h2>
      {_table(['Family', 'Candidates', 'Promising', 'Testing', 'Rejected', 'Avg Latest', 'Evaluations', 'Windows'], family_summary)}
    </section>

    <section>
      <h2>Candidate Relationship Pairs</h2>
      {_table(['Left', 'Right', 'Type', 'Strength', 'Run ID'], relationship_pairs)}
    </section>

    <section>
      <h2>Latest Workflow Runs</h2>
      {_table(['Run ID', 'Created At UTC', 'Source', 'Start', 'End', 'Status'], latest_runs)}
    </section>
  </div>
</body>
</html>
"""
    Path(output_path).write_text(html_doc, encoding="utf-8")
    build_graph_artifacts(db_path, Path(output_path).parent)
