from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


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
    status TEXT NOT NULL,
    config_fingerprint TEXT,
    rerun_of_run_id TEXT
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

CREATE TABLE IF NOT EXISTS research_tasks (
    task_id TEXT PRIMARY KEY,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    fingerprint TEXT,
    payload_json TEXT NOT NULL,
    parent_task_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at_utc TEXT NOT NULL,
    started_at_utc TEXT,
    finished_at_utc TEXT,
    worker_note TEXT
);
"""


class ExperimentStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(workflow_runs)").fetchall()}
        if "config_fingerprint" not in cols:
            self.conn.execute("ALTER TABLE workflow_runs ADD COLUMN config_fingerprint TEXT")
        if "rerun_of_run_id" not in cols:
            self.conn.execute("ALTER TABLE workflow_runs ADD COLUMN rerun_of_run_id TEXT")

        task_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(research_tasks)").fetchall()}
        if task_cols and "worker_note" not in task_cols:
            self.conn.execute("ALTER TABLE research_tasks ADD COLUMN worker_note TEXT")

    def insert_run(self, payload: dict) -> None:
        self.conn.execute(
            """
            INSERT OR REPLACE INTO workflow_runs (
                run_id, created_at_utc, config_path, output_dir, data_source,
                start_date, end_date, universe_limit, factor_count, dataset_rows, status,
                config_fingerprint, rerun_of_run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                payload.get("config_fingerprint"),
                payload.get("rerun_of_run_id"),
            ),
        )
        self.conn.commit()

    def find_latest_finished_run(self, config_fingerprint: str) -> tuple[str, str] | None:
        row = self.conn.execute(
            """
            SELECT run_id, created_at_utc
            FROM workflow_runs
            WHERE config_fingerprint = ? AND status = 'finished'
            ORDER BY created_at_utc DESC
            LIMIT 1
            """,
            (config_fingerprint,),
        ).fetchone()
        return row if row else None

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

    def enqueue_research_task(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int = 100,
        fingerprint: str | None = None,
        parent_task_id: str | None = None,
        worker_note: str | None = None,
    ) -> str:
        if fingerprint:
            existing = self.conn.execute(
                """
                SELECT task_id FROM research_tasks
                WHERE fingerprint = ? AND status IN ('pending', 'running', 'finished')
                ORDER BY created_at_utc DESC
                LIMIT 1
                """,
                (fingerprint,),
            ).fetchone()
            if existing:
                return existing[0]
        task_id = str(uuid4())
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO research_tasks (
                task_id, task_type, status, priority, fingerprint, payload_json,
                parent_task_id, attempt_count, last_error, created_at_utc,
                started_at_utc, finished_at_utc, worker_note
            ) VALUES (?, ?, 'pending', ?, ?, ?, ?, 0, NULL, ?, NULL, NULL, ?)
            """,
            (
                task_id,
                task_type,
                priority,
                fingerprint,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                parent_task_id,
                now,
                worker_note,
            ),
        )
        self.conn.commit()
        return task_id

    def claim_next_research_task(self) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                   parent_task_id, attempt_count, last_error, created_at_utc,
                   started_at_utc, finished_at_utc, worker_note
            FROM research_tasks
            WHERE status = 'pending'
            ORDER BY priority ASC, created_at_utc ASC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE research_tasks SET status='running', started_at_utc=?, attempt_count=attempt_count+1 WHERE task_id=?",
            (now, row[0]),
        )
        self.conn.commit()
        return self.get_research_task(row[0])

    def get_research_task(self, task_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                   parent_task_id, attempt_count, last_error, created_at_utc,
                   started_at_utc, finished_at_utc, worker_note
            FROM research_tasks WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        if not row:
            return None
        payload = dict(zip([c[0] for c in self.conn.execute("SELECT * FROM research_tasks WHERE 0").description], row))
        payload["payload"] = json.loads(payload.pop("payload_json"))
        return payload

    def finish_research_task(self, task_id: str, status: str, last_error: str | None = None, worker_note: str | None = None) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE research_tasks SET status=?, finished_at_utc=?, last_error=?, worker_note=COALESCE(?, worker_note) WHERE task_id=?",
            (status, now, last_error, worker_note, task_id),
        )
        self.conn.commit()

    def list_research_tasks(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                   parent_task_id, attempt_count, last_error, created_at_utc,
                   started_at_utc, finished_at_utc, worker_note
            FROM research_tasks
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        columns = [
            'task_id', 'task_type', 'status', 'priority', 'fingerprint', 'payload_json',
            'parent_task_id', 'attempt_count', 'last_error', 'created_at_utc',
            'started_at_utc', 'finished_at_utc', 'worker_note'
        ]
        result = []
        for row in rows:
            item = dict(zip(columns, row))
            item['payload'] = json.loads(item.pop('payload_json'))
            result.append(item)
        return result
