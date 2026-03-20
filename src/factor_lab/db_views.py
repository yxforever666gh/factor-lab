from __future__ import annotations

import sqlite3
from pathlib import Path


VIEWS_SQL = """
DROP VIEW IF EXISTS v_factor_score_avg;
CREATE VIEW v_factor_score_avg AS
SELECT
  factor_name,
  AVG(score) AS avg_score,
  COUNT(*) AS runs
FROM factor_results
WHERE variant = 'raw_scored'
GROUP BY factor_name;

DROP VIEW IF EXISTS v_stable_candidates;
CREATE VIEW v_stable_candidates AS
SELECT
  factor_name,
  COUNT(*) AS candidate_runs
FROM factor_results
WHERE variant = 'candidate'
GROUP BY factor_name;

DROP VIEW IF EXISTS v_portfolio_strategy_avg;
CREATE VIEW v_portfolio_strategy_avg AS
SELECT
  strategy_name,
  AVG(sharpe) AS avg_sharpe,
  AVG(annual_return) AS avg_return,
  COUNT(*) AS runs
FROM portfolio_results
GROUP BY strategy_name;

DROP VIEW IF EXISTS v_factor_candidate_leaderboard;
CREATE VIEW v_factor_candidate_leaderboard AS
SELECT
  id,
  name,
  family,
  status,
  evaluation_count,
  window_count,
  avg_final_score,
  best_final_score,
  latest_final_score,
  pass_rate,
  next_action,
  rejection_reason,
  updated_at_utc
FROM factor_candidates;
"""


def ensure_views(db_path: str | Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(VIEWS_SQL)
    conn.commit()
    conn.close()
