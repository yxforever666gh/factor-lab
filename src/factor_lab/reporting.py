from __future__ import annotations

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

    stable_candidates = cur.execute(
        """
        SELECT factor_name, COUNT(*) AS runs
        FROM factor_results
        WHERE variant = 'candidate'
        GROUP BY factor_name
        ORDER BY runs DESC, factor_name ASC
        """
    ).fetchall()

    top_portfolios = cur.execute(
        """
        SELECT strategy_name, AVG(sharpe) AS avg_sharpe, AVG(annual_return) AS avg_return, COUNT(*) AS runs
        FROM portfolio_results
        GROUP BY strategy_name
        ORDER BY avg_sharpe DESC
        """
    ).fetchall()

    candidate_leaderboard = cur.execute(
        """
        SELECT name, family, status, evaluation_count, window_count,
               ROUND(avg_final_score, 6), ROUND(best_final_score, 6), ROUND(latest_final_score, 6),
               ROUND(pass_rate, 4), COALESCE(next_action, '-')
        FROM v_factor_candidate_leaderboard
        ORDER BY COALESCE(latest_final_score, -999) DESC, evaluation_count DESC
        LIMIT 10
        """
    ).fetchall()
    family_summary = cur.execute(
        """
        SELECT family, candidate_count, promising_count, testing_count, rejected_count,
               ROUND(avg_candidate_score, 6), ROUND(avg_latest_score, 6), evaluation_count, window_count
        FROM v_candidate_family_summary
        ORDER BY COALESCE(avg_latest_score, -999) DESC, candidate_count DESC, family ASC
        LIMIT 10
        """
    ).fetchall()
    relationship_pairs = cur.execute(
        """
        SELECT left_name, right_name, relationship_type, ROUND(strength, 6), run_id
        FROM v_candidate_relationship_pairs
        ORDER BY COALESCE(strength, 0) DESC, updated_at_utc DESC
        LIMIT 12
        """
    ).fetchall()

    lines = [
        "# SQLite Experiment Report",
        "",
        "## Candidate Leaderboard",
        "",
    ]
    for row in candidate_leaderboard:
        lines.append(
            f"- {row[0]} | family={row[1]} | status={row[2]} | evals={row[3]} | windows={row[4]} | avg={row[5]} | best={row[6]} | latest={row[7]} | pass_rate={row[8]} | next={row[9]}"
        )

    lines.extend(["", "## Candidate Families", ""])
    for row in family_summary:
        lines.append(
            f"- {row[0]} | candidates={row[1]} | promising={row[2]} | testing={row[3]} | rejected={row[4]} | avg_candidate={row[5]} | avg_latest={row[6]} | evals={row[7]} | windows={row[8]}"
        )

    lines.extend(["", "## Candidate Relationship Pairs", ""])
    for left_name, right_name, rel_type, strength, run_id in relationship_pairs:
        lines.append(f"- {left_name} <-> {right_name} | type={rel_type} | strength={strength} | run_id={run_id}")

    lines.extend(["", "## Top Factors by Average Score", ""])
    for name, avg_score, runs in top_factors:
        lines.append(f"- {name} | avg_score={avg_score:.6f} | runs={runs}")

    lines.extend(["", "## Stable Candidates", ""])
    for name, runs in stable_candidates:
        lines.append(f"- {name} | candidate_runs={runs}")

    lines.extend(["", "## Portfolio Strategy Averages", ""])
    for strategy_name, avg_sharpe, avg_return, runs in top_portfolios:
        lines.append(
            f"- {strategy_name} | avg_sharpe={avg_sharpe:.6f} | avg_return={avg_return:.6f} | runs={runs}"
        )

    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
    build_graph_artifacts(db_path, Path(output_path).parent)
