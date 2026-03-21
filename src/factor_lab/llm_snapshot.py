from __future__ import annotations

import json
from pathlib import Path
import sqlite3

from factor_lab.conservative_mode import conservative_policy_from_portfolio


def build_snapshot(db_path: str | Path, output_path: str | Path) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    latest_run = cur.execute(
        """
        SELECT run_id, created_at_utc, config_path, data_source, start_date, end_date, status, dataset_rows, factor_count
        FROM workflow_runs
        WHERE status = 'finished'
        ORDER BY created_at_utc DESC
        LIMIT 1
        """
    ).fetchone()

    top_scores = [dict(row) for row in cur.execute(
        "SELECT factor_name, ROUND(avg_score, 6) AS avg_score, runs FROM v_factor_score_avg ORDER BY avg_score DESC LIMIT 10"
    ).fetchall()]

    stable_candidates = [dict(row) for row in cur.execute(
        "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC LIMIT 10"
    ).fetchall()]

    portfolios = [dict(row) for row in cur.execute(
        "SELECT strategy_name, ROUND(avg_sharpe, 6) AS avg_sharpe, ROUND(avg_return, 6) AS avg_return, runs FROM v_portfolio_strategy_avg ORDER BY avg_sharpe DESC LIMIT 10"
    ).fetchall()]

    latest_candidates = []
    latest_graveyard = []
    latest_representatives = []
    if latest_run:
        run_id = latest_run["run_id"]
        latest_candidates = [row[0] for row in cur.execute(
            "SELECT factor_name FROM factor_results WHERE run_id = ? AND variant = 'candidate' ORDER BY factor_name ASC",
            (run_id,),
        ).fetchall()]
        latest_graveyard = [row[0] for row in cur.execute(
            "SELECT factor_name FROM factor_results WHERE run_id = ? AND variant = 'graveyard' ORDER BY factor_name ASC",
            (run_id,),
        ).fetchall()]
        latest_representatives = [row[0] for row in cur.execute(
            "SELECT factor_name FROM factor_results WHERE run_id = ? AND variant = 'raw_scored' ORDER BY score DESC LIMIT 5",
            (run_id,),
        ).fetchall()]

    root = Path(db_path).parent
    latest_summary_path = root / "latest_summary.txt"
    change_report_path = root / "change_report.md"
    recommendation_weights_path = root / "llm_recommendation_weights.json"
    recommendation_history_path = root / "llm_recommendation_history.json"
    recommendation_context_path = root / "llm_recommendation_context.json"
    paper_portfolio_stability_path = root / "paper_portfolio" / "portfolio_stability_score.json"
    paper_portfolio_retro_path = root / "paper_portfolio" / "portfolio_retrospective.json"

    paper_portfolio_stability = json.loads(paper_portfolio_stability_path.read_text(encoding="utf-8")) if paper_portfolio_stability_path.exists() else {}

    payload = {
        "latest_run": dict(latest_run) if latest_run else None,
        "top_scores": top_scores,
        "stable_candidates": stable_candidates,
        "portfolio_averages": portfolios,
        "latest_candidates": latest_candidates,
        "latest_graveyard": latest_graveyard,
        "latest_top_ranked_factors": latest_representatives,
        "latest_summary": latest_summary_path.read_text(encoding="utf-8") if latest_summary_path.exists() else "",
        "change_report": change_report_path.read_text(encoding="utf-8") if change_report_path.exists() else "",
        "recommendation_weights": json.loads(recommendation_weights_path.read_text(encoding="utf-8")) if recommendation_weights_path.exists() else {},
        "recommendation_history_tail": json.loads(recommendation_history_path.read_text(encoding="utf-8"))[-5:] if recommendation_history_path.exists() else [],
        "recommendation_context": json.loads(recommendation_context_path.read_text(encoding="utf-8")) if recommendation_context_path.exists() else {},
        "paper_portfolio_stability": paper_portfolio_stability,
        "paper_portfolio_retrospective": json.loads(paper_portfolio_retro_path.read_text(encoding="utf-8")) if paper_portfolio_retro_path.exists() else {},
        "conservative_policy": conservative_policy_from_portfolio(paper_portfolio_stability),
    }
    Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload
