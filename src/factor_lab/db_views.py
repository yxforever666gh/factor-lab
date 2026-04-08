from __future__ import annotations

import sqlite3
from pathlib import Path

from factor_lab.storage import ExperimentStore


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
  factor_role,
  status,
  research_stage,
  evaluation_count,
  window_count,
  avg_final_score,
  best_final_score,
  latest_final_score,
  latest_recent_final_score,
  pass_rate,
  next_action,
  rejection_reason,
  updated_at_utc
FROM factor_candidates;

DROP VIEW IF EXISTS v_candidate_family_summary;
CREATE VIEW v_candidate_family_summary AS
SELECT
  COALESCE(family, 'other') AS family,
  COUNT(*) AS candidate_count,
  SUM(CASE WHEN research_stage = 'candidate' THEN 1 ELSE 0 END) AS promising_count,
  SUM(CASE WHEN research_stage IN ('watchlist', 'explore') THEN 1 ELSE 0 END) AS testing_count,
  SUM(CASE WHEN research_stage = 'graveyard' THEN 1 ELSE 0 END) AS rejected_count,
  ROUND(AVG(avg_final_score), 6) AS avg_candidate_score,
  ROUND(AVG(COALESCE(latest_recent_final_score, latest_final_score)), 6) AS avg_latest_score,
  ROUND(MAX(best_final_score), 6) AS best_score,
  SUM(evaluation_count) AS evaluation_count,
  SUM(window_count) AS window_count
FROM factor_candidates
GROUP BY COALESCE(family, 'other');

DROP VIEW IF EXISTS v_candidate_relationship_pairs;
CREATE VIEW v_candidate_relationship_pairs AS
SELECT
  r.left_candidate_id,
  l.name AS left_name,
  r.right_candidate_id,
  rr.name AS right_name,
  r.relationship_type,
  ROUND(r.strength, 6) AS strength,
  r.run_id,
  r.updated_at_utc
FROM candidate_relationships r
LEFT JOIN factor_candidates l ON l.id = r.left_candidate_id
LEFT JOIN factor_candidates rr ON rr.id = r.right_candidate_id;

DROP VIEW IF EXISTS v_candidate_risk_summary;
CREATE VIEW v_candidate_risk_summary AS
SELECT
  rp.candidate_id,
  fc.name AS candidate_name,
  fc.family,
  fc.status AS candidate_status,
  rp.risk_level,
  ROUND(rp.risk_score, 6) AS risk_score,
  ROUND(rp.robustness_score, 6) AS robustness_score,
  ROUND(rp.family_context_score, 6) AS family_context_score,
  ROUND(rp.graph_context_score, 6) AS graph_context_score,
  rp.evaluation_count,
  rp.passing_check_count,
  rp.failing_check_count,
  rp.summary,
  rp.updated_at_utc
FROM candidate_risk_profile rp
LEFT JOIN factor_candidates fc ON fc.id = rp.candidate_id;

DROP VIEW IF EXISTS v_candidate_family_risk_summary;
CREATE VIEW v_candidate_family_risk_summary AS
SELECT
  COALESCE(fc.family, 'other') AS family,
  COUNT(*) AS candidate_count,
  ROUND(AVG(rp.risk_score), 6) AS avg_risk_score,
  SUM(CASE WHEN rp.risk_level = 'high' THEN 1 ELSE 0 END) AS high_risk_count,
  SUM(CASE WHEN rp.risk_level = 'medium' THEN 1 ELSE 0 END) AS medium_risk_count,
  SUM(CASE WHEN rp.risk_level = 'low' THEN 1 ELSE 0 END) AS low_risk_count,
  ROUND(AVG(rp.robustness_score), 6) AS avg_robustness_score,
  ROUND(MAX(rp.risk_score), 6) AS max_risk_score
FROM candidate_risk_profile rp
LEFT JOIN factor_candidates fc ON fc.id = rp.candidate_id
GROUP BY COALESCE(fc.family, 'other');

DROP VIEW IF EXISTS v_exposure_leaderboard;
CREATE VIEW v_exposure_leaderboard AS
SELECT
  run_id,
  factor_name,
  exposure_type,
  exposure_label,
  strength_score,
  raw_rank_ic_mean,
  raw_rank_ic_ir,
  neutralized_rank_ic_mean,
  neutralized_pass_gate,
  split_fail_count,
  crowding_peers,
  recommended_max_weight,
  status,
  updated_at_utc
FROM exposure_factors;
"""


def ensure_views(db_path: str | Path) -> None:
    bootstrap = ExperimentStore(db_path)
    bootstrap.conn.close()
    conn = sqlite3.connect(db_path)
    conn.executescript(VIEWS_SQL)
    conn.commit()
    conn.close()
