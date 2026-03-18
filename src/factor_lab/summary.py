from __future__ import annotations

import json
from pathlib import Path
import sqlite3


def build_run_summary(db_path: str | Path, output_path: str | Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    latest_run = cur.execute(
        "SELECT run_id, created_at_utc, config_path FROM workflow_runs ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()
    stable_candidates = cur.execute(
        "SELECT factor_name, COUNT(*) FROM factor_results WHERE variant='candidate' GROUP BY factor_name ORDER BY COUNT(*) DESC, factor_name ASC LIMIT 5"
    ).fetchall()
    best_portfolio = cur.execute(
        "SELECT strategy_name, AVG(sharpe) AS avg_sharpe FROM portfolio_results GROUP BY strategy_name ORDER BY avg_sharpe DESC LIMIT 1"
    ).fetchone()

    if not latest_run:
        Path(output_path).write_text("No runs yet.", encoding="utf-8")
        return

    lines = [
        f"Latest run: {latest_run[0]} ({latest_run[1]}) from {latest_run[2]}",
        f"Best average strategy: {best_portfolio[0]} (avg_sharpe={best_portfolio[1]:.6f})" if best_portfolio else "Best average strategy: n/a",
        "Stable candidates: " + (", ".join(name for name, _ in stable_candidates) if stable_candidates else "none"),
    ]
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
