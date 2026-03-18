from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable


SCHEMA = """
CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id TEXT PRIMARY KEY,
    created_at_utc TEXT NOT NULL,
    config_path TEXT NOT NULL,
    output_dir TEXT NOT NULL,
    data_source TEXT NOT NULL,
    start_date TEXT,
    end_date TEXT,
    universe_limit INTEGER,
    factor_count INTEGER,
    dataset_rows INTEGER,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS factor_results (
    run_id TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    variant TEXT NOT NULL,
    expression TEXT,
    rank_ic_mean REAL,
    rank_ic_ir REAL,
    top_bottom_spread_mean REAL,
    pass_gate INTEGER,
    fail_reason TEXT,
    score REAL,
    split_fail_count INTEGER,
    high_corr_peers_json TEXT,
    PRIMARY KEY (run_id, factor_name, variant)
);

CREATE TABLE IF NOT EXISTS portfolio_results (
    run_id TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    annual_return REAL,
    annual_volatility REAL,
    sharpe REAL,
    max_drawdown REAL,
    avg_turnover REAL,
    observations INTEGER,
    PRIMARY KEY (run_id, strategy_name)
);

CREATE TABLE IF NOT EXISTS run_artifacts (
    run_id TEXT NOT NULL,
    artifact_name TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    PRIMARY KEY (run_id, artifact_name)
);
"""


class ExperimentStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def insert_run(self, payload: dict) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO workflow_runs (
                run_id, created_at_utc, config_path, output_dir, data_source,
                start_date, end_date, universe_limit, factor_count, dataset_rows, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["run_id"],
                payload["created_at_utc"],
                payload["config_path"],
                payload["output_dir"],
                payload["data_source"],
                payload.get("start_date"),
                payload.get("end_date"),
                payload.get("universe_limit"),
                payload.get("factor_count"),
                payload.get("dataset_rows"),
                payload["status"],
            ),
        )
        self.conn.commit()

    def insert_factor_rows(self, rows: Iterable[dict]) -> None:
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO factor_results (
                run_id, factor_name, variant, expression, rank_ic_mean, rank_ic_ir,
                top_bottom_spread_mean, pass_gate, fail_reason, score, split_fail_count, high_corr_peers_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["run_id"],
                    row["factor_name"],
                    row["variant"],
                    row.get("expression"),
                    row.get("rank_ic_mean"),
                    row.get("rank_ic_ir"),
                    row.get("top_bottom_spread_mean"),
                    int(bool(row.get("pass_gate"))),
                    row.get("fail_reason"),
                    row.get("score"),
                    row.get("split_fail_count"),
                    json.dumps(row.get("high_corr_peers", []), ensure_ascii=False),
                )
                for row in rows
            ],
        )
        self.conn.commit()

    def insert_portfolio_rows(self, run_id: str, rows: Iterable[dict]) -> None:
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO portfolio_results (
                run_id, strategy_name, annual_return, annual_volatility, sharpe,
                max_drawdown, avg_turnover, observations
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    run_id,
                    row["strategy_name"],
                    row.get("annual_return"),
                    row.get("annual_volatility"),
                    row.get("sharpe"),
                    row.get("max_drawdown"),
                    row.get("avg_turnover"),
                    row.get("observations"),
                )
                for row in rows
            ],
        )
        self.conn.commit()

    def insert_artifacts(self, run_id: str, rows: Iterable[tuple[str, str]]) -> None:
        self.conn.executemany(
            "INSERT OR REPLACE INTO run_artifacts (run_id, artifact_name, artifact_path) VALUES (?, ?, ?)",
            [(run_id, name, path) for name, path in rows],
        )
        self.conn.commit()
