from __future__ import annotations

import sqlite3
from pathlib import Path


def write_sqlite_report(db_path: str | Path, output_path: str | Path) -> None:
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

    lines = [
        "# SQLite Experiment Report",
        "",
        "## Top Factors by Average Score",
        "",
    ]
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
