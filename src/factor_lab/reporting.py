from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from factor_lab.candidate_graph import build_graph_artifacts
from factor_lab.db_views import ensure_views
def write_sqlite_report(db_path: str | Path, output_path: str | Path) -> None:
    ensure_views(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    top_factors = cur.execute(
        """
        SELECT factor_name, AVG(score) AS avg_score, COUNT(*) AS runs
        FROM factor_results
        WHERE variant = 'raw_scored'
        GROUP BY factor_name
        ORDER BY avg_score DESC
        LIMIT 10
        """
    ).fetchall()
    candidate_leaderboard = cur.execute(
        """
        SELECT name, family, factor_role, status, research_stage, evaluation_count, window_count,
               ROUND(avg_final_score, 6), ROUND(best_final_score, 6), ROUND(latest_final_score, 6),
               ROUND(latest_recent_final_score, 6), ROUND(pass_rate, 4), COALESCE(next_action, '-')
        FROM v_factor_candidate_leaderboard
        ORDER BY COALESCE(latest_recent_final_score, latest_final_score, -999) DESC, evaluation_count DESC
        LIMIT 10
        """
    ).fetchall()
    lines = [
        '# SQLite Experiment Report',
        '',
        '## Candidate Leaderboard',
        '',
    ]
    for row in candidate_leaderboard:
        lines.append(
            f"- {row[0]} | family={row[1]} | role={row[2]} | status={row[3]} | stage={row[4]} | evals={row[5]} | windows={row[6]} | avg={row[7]} | best={row[8]} | latest={row[9]} | latest_recent={row[10]} | pass_rate={row[11]} | next={row[12]}"
        )
    latest_run = cur.execute(
        "SELECT output_dir FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    output_dir = Path(latest_run[0]) if latest_run and latest_run[0] else None
    if output_dir and (output_dir / 'candidate_status_snapshot.json').exists():
        snapshot = json.loads((output_dir / 'candidate_status_snapshot.json').read_text(encoding='utf-8'))
        lines.extend(['', '## Candidate Status Snapshot', ''])
        for row in snapshot[:20]:
            lines.append(
                f"- {row['factor_name']} | role={row.get('factor_role')} | stage={row.get('research_stage')} | raw={row.get('raw_pass')} | neutral={row.get('neutralized_pass')} | rolling={row.get('rolling_pass')} | split_fail={row.get('split_fail_count')} | reasons={'; '.join(row.get('blocking_reasons') or []) or '-'}"
            )
    if output_dir and (output_dir / 'rolling_summary.json').exists():
        rolling_summary = json.loads((output_dir / 'rolling_summary.json').read_text(encoding='utf-8'))
        lines.extend(['', '## Rolling Stability Summary', ''])
        for row in rolling_summary[:20]:
            lines.append(
                f"- {row['factor_name']} | windows={row.get('window_count')} | pass_rate={row.get('pass_rate')} | flips={row.get('sign_flip_count')} | avgIC={row.get('avg_rank_ic_mean')} | ic_std={row.get('rank_ic_std')} | stability={row.get('stability_score')} | pass={row.get('pass_gate')}"
            )
    lines.extend(['', '## Top Factors by Average Score', ''])
    for name, avg_score, runs in top_factors:
        lines.append(f"- {name} | avg_score={avg_score:.6f} | runs={runs}")
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
    build_graph_artifacts(db_path, Path(output_path).parent)