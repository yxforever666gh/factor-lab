from __future__ import annotations

import json
from pathlib import Path
import sqlite3


def build_change_report(db_path: str | Path, output_path: str | Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    runs = cur.execute(
        "SELECT run_id, created_at_utc, config_path FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 2"
    ).fetchall()

    if len(runs) < 2:
        Path(output_path).write_text("Not enough finished runs for change detection.", encoding="utf-8")
        return

    latest_run_id, latest_at, latest_config = runs[0]
    prev_run_id, prev_at, prev_config = runs[1]

    def factor_set(run_id: str, variant: str) -> set[str]:
        rows = cur.execute(
            "SELECT factor_name FROM factor_results WHERE run_id = ? AND variant = ?",
            (run_id, variant),
        ).fetchall()
        return {row[0] for row in rows}

    latest_candidates = factor_set(latest_run_id, "candidate")
    prev_candidates = factor_set(prev_run_id, "candidate")
    latest_graveyard = factor_set(latest_run_id, "graveyard")
    prev_graveyard = factor_set(prev_run_id, "graveyard")

    entered_candidates = sorted(latest_candidates - prev_candidates)
    left_candidates = sorted(prev_candidates - latest_candidates)
    entered_graveyard = sorted(latest_graveyard - prev_graveyard)
    left_graveyard = sorted(prev_graveyard - latest_graveyard)

    lines = [
        "# Change Detection",
        "",
        f"Latest run: {latest_run_id} | {latest_at} | {latest_config}",
        f"Previous run: {prev_run_id} | {prev_at} | {prev_config}",
        "",
        "## Candidate Pool Changes",
        f"- Entered: {', '.join(entered_candidates) if entered_candidates else 'none'}",
        f"- Left: {', '.join(left_candidates) if left_candidates else 'none'}",
        "",
        "## Graveyard Changes",
        f"- Entered: {', '.join(entered_graveyard) if entered_graveyard else 'none'}",
        f"- Left: {', '.join(left_graveyard) if left_graveyard else 'none'}",
    ]
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
