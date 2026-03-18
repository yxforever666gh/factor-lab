from __future__ import annotations

import sqlite3
from pathlib import Path


VIEWS_SQL = """
CREATE VIEW IF NOT EXISTS v_factor_score_avg AS
SELECT
  factor_name,
  AVG(score) AS avg_score,
  COUNT(*) AS runs
FROM factor_results
WHERE variant = 'raw_scored'
GROUP BY factor_name;

CREATE VIEW IF NOT EXISTS v_stable_candidates AS
SELECT
  factor_name,
  COUNT(*) AS candidate_runs
FROM factor_results
WHERE variant = 'candidate'
GROUP BY factor_name;

CREATE VIEW IF NOT EXISTS v_portfolio_strategy_avg AS
SELECT
  strategy_name,
  AVG(sharpe) AS avg_sharpe,
  AVG(annual_return) AS avg_return,
  COUNT(*) AS runs
FROM portfolio_results
GROUP BY strategy_name;
"""


def ensure_views(db_path: str | Path) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(VIEWS_SQL)
    conn.commit()
    conn.close()
