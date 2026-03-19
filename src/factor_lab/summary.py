from __future__ import annotations

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
        Path(output_path).write_text("暂无运行记录。", encoding="utf-8")
        return

    run_id, created_at_utc, config_path = latest_run
    candidates_text = "、".join(name for name, _ in stable_candidates) if stable_candidates else "暂无"
    strategy_text = (
        f"当前长期平均表现最好的策略是 {best_portfolio[0]}，平均夏普 {best_portfolio[1]:.2f}。"
        if best_portfolio else
        "当前还没有可用的策略统计。"
    )

    lines = [
        f"最新一次完成的研究任务来自 {config_path}。",
        f"运行时间：{created_at_utc}。",
        strategy_text,
        f"目前最稳定的候选因子：{candidates_text}。",
    ]
    Path(output_path).write_text("\n".join(lines), encoding="utf-8")
