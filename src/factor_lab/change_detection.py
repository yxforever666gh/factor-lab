from __future__ import annotations

from pathlib import Path
import sqlite3


def build_change_report(db_path: str | Path, output_path: str | Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    runs = cur.execute(
        "SELECT run_id, created_at_utc, config_path FROM workflow_runs WHERE status='finished' ORDER BY created_at_utc DESC LIMIT 2"
    ).fetchall()

    if len(runs) < 2:
        Path(output_path).write_text("暂无足够的已完成运行，暂时无法比较变化。", encoding="utf-8")
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

    candidate_summary = []
    if entered_candidates:
        candidate_summary.append(f"新进入候选：{'、'.join(entered_candidates)}")
    if left_candidates:
        candidate_summary.append(f"退出候选：{'、'.join(left_candidates)}")
    if not candidate_summary:
        candidate_summary.append("候选池没有变化。")

    graveyard_summary = []
    if entered_graveyard:
        graveyard_summary.append(f"新进入墓地：{'、'.join(entered_graveyard)}")
    if left_graveyard:
        graveyard_summary.append(f"离开墓地：{'、'.join(left_graveyard)}")
    if not graveyard_summary:
        graveyard_summary.append("墓地没有变化。")

    lines = [
        f"最新一次完成运行：{latest_config}（{latest_at}）。",
        f"上一轮完成运行：{prev_config}（{prev_at}）。",
        "",
        "候选池变化：",
        *[f"- {item}" for item in candidate_summary],
        "",
        "墓地变化：",
        *[f"- {item}" for item in graveyard_summary],
    ]
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
