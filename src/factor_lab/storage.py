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

CREATE TABLE IF NOT EXISTS factor_candidates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    family TEXT,
    definition_json TEXT NOT NULL,
    expression TEXT,
    origin_task_id TEXT,
    origin_run_id TEXT,
    factor_role TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    research_stage TEXT NOT NULL DEFAULT 'explore',
    evaluation_count INTEGER NOT NULL DEFAULT 0,
    window_count INTEGER NOT NULL DEFAULT 0,
    avg_final_score REAL,
    best_final_score REAL,
    latest_final_score REAL,
    latest_recent_final_score REAL,
    pass_rate REAL,
    summary TEXT,
    next_action TEXT,
    rejection_reason TEXT,
    duplicate_of TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS exposure_factors (
    run_id TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    exposure_type TEXT NOT NULL,
    exposure_label TEXT,
    bucket_key TEXT,
    bucket_label TEXT,
    effective_bucket_key TEXT,
    effective_bucket_label TEXT,
    strength_score REAL,
    raw_rank_ic_mean REAL,
    raw_rank_ic_ir REAL,
    neutralized_rank_ic_mean REAL,
    neutralized_pass_gate INTEGER,
    retention_industry REAL,
    retention_industry_size REAL,
    retention_full REAL,
    industry_top1_weight REAL,
    industry_hhi REAL,
    turnover_daily REAL,
    net_metric REAL,
    liquidity_bottom20_retention REAL,
    strength_subscore REAL,
    robustness_subscore REAL,
    controllability_subscore REAL,
    implementability_subscore REAL,
    novelty_subscore REAL,
    total_score REAL,
    split_fail_count INTEGER,
    crowding_peers INTEGER,
    recommended_max_weight REAL,
    status TEXT NOT NULL,
    hard_flags_json TEXT,
    notes_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (run_id, factor_name)
);

CREATE TABLE IF NOT EXISTS factor_evaluations (
    id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    run_id TEXT,
    task_id TEXT,
    window_label TEXT,
    market_scope TEXT,
    sample_size INTEGER,
    observations INTEGER,
    return_metric REAL,
    sharpe_like REAL,
    max_drawdown REAL,
    turnover REAL,
    coverage REAL,
    raw_rank_ic_mean REAL,
    neutralized_rank_ic_mean REAL,
    split_fail_count INTEGER,
    high_corr_peer_count INTEGER,
    robust_pass_count INTEGER,
    robust_total_count INTEGER,
    stability_score REAL,
    quality_score REAL,
    final_score REAL,
    pass_flag INTEGER,
    status TEXT,
    rejection_reason TEXT,
    notes_json TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_hypotheses (
    id TEXT PRIMARY KEY,
    candidate_id TEXT UNIQUE,
    title TEXT NOT NULL,
    family TEXT,
    hypothesis_text TEXT,
    status TEXT,
    evidence_for_json TEXT,
    evidence_against_json TEXT,
    next_action TEXT,
    last_reviewed_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_theses (
    id TEXT PRIMARY KEY,
    candidate_id TEXT UNIQUE,
    thesis_id TEXT NOT NULL,
    title TEXT NOT NULL,
    family TEXT,
    thesis_type TEXT,
    institutional_bucket_key TEXT,
    institutional_bucket_label TEXT,
    thesis_text TEXT,
    mechanism_rationale TEXT,
    status TEXT,
    invalidation_json TEXT,
    representative_candidate TEXT,
    representative_rank INTEGER,
    representative_count INTEGER,
    roster_json TEXT,
    source_context_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_relationships (
    left_candidate_id TEXT NOT NULL,
    right_candidate_id TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    run_id TEXT,
    strength REAL,
    details_json TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    PRIMARY KEY (left_candidate_id, right_candidate_id, relationship_type)
);

CREATE TABLE IF NOT EXISTS candidate_robustness_checks (
    id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    run_id TEXT,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL,
    score REAL,
    weight REAL,
    evidence_json TEXT,
    rationale TEXT,
    created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_risk_profile (
    candidate_id TEXT PRIMARY KEY,
    run_id TEXT,
    risk_level TEXT NOT NULL,
    risk_score REAL NOT NULL,
    robustness_score REAL,
    family_context_score REAL,
    graph_context_score REAL,
    evaluation_count INTEGER NOT NULL DEFAULT 0,
    passing_check_count INTEGER NOT NULL DEFAULT 0,
    failing_check_count INTEGER NOT NULL DEFAULT 0,
    summary TEXT,
    key_risks_json TEXT,
    mitigations_json TEXT,
    profile_json TEXT,
    updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS research_trial_log (
    source_task_id TEXT PRIMARY KEY,
    fingerprint TEXT,
    family TEXT NOT NULL,
    category TEXT,
    candidate_name TEXT,
    status TEXT NOT NULL,
    outcome_label TEXT NOT NULL,
    knowledge_gain_count INTEGER NOT NULL DEFAULT 0,
    pressure_weight REAL NOT NULL DEFAULT 1.0,
    created_at_utc TEXT NOT NULL,
    details_json TEXT
);
"""


class ExperimentStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        try:
            self.conn.executescript(SCHEMA)
        except sqlite3.DatabaseError as exc:
            # Rare SQLite edge case: if the process crashed mid-migration, we may end up with an orphan
            # auto-index entry referencing a missing table definition. This blocks any subsequent schema work.
            # Best-effort repair: drop the orphan index from sqlite_master and retry.
            msg = str(exc)
            if "orphan index" in msg and "sqlite_autoindex_" in msg:
                try:
                    name = msg.split("(", 1)[-1].split(")", 1)[0]
                except Exception:
                    name = None
                if name:
                    self._drop_orphan_index(name)
                    self.conn.executescript(SCHEMA)
                else:
                    raise
            else:
                raise
        self._migrate()
        self.conn.commit()

    def _drop_orphan_index(self, index_name: str) -> None:
        # Danger zone: writable_schema is normally off-limits; we use it only to recover from a corrupted schema
        # state where SQLite reports an orphan index.
        self.conn.execute("PRAGMA writable_schema=ON")
        self.conn.execute("DELETE FROM sqlite_master WHERE type='index' AND name=?", (index_name,))
        self.conn.execute("PRAGMA writable_schema=OFF")
        self.conn.commit()
        # VACUUM/REINDEX must run outside a transaction.
        self.conn.isolation_level = None
        try:
            self.conn.execute("VACUUM")
            self.conn.execute("REINDEX")
        finally:
            self.conn.isolation_level = ""  # restore default implicit transactions

    def _migrate(self) -> None:
        cols = {row[1] for row in self.conn.execute("PRAGMA table_info(workflow_runs)").fetchall()}
        if "config_fingerprint" not in cols:
            self.conn.execute("ALTER TABLE workflow_runs ADD COLUMN config_fingerprint TEXT")
        if "rerun_of_run_id" not in cols:
            self.conn.execute("ALTER TABLE workflow_runs ADD COLUMN rerun_of_run_id TEXT")

        candidate_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(factor_candidates)").fetchall()}
        if candidate_cols and "latest_recent_final_score" not in candidate_cols:
            self.conn.execute("ALTER TABLE factor_candidates ADD COLUMN latest_recent_final_score REAL")
        if candidate_cols and "factor_role" not in candidate_cols:
            self.conn.execute("ALTER TABLE factor_candidates ADD COLUMN factor_role TEXT")
        if candidate_cols and "research_stage" not in candidate_cols:
            self.conn.execute("ALTER TABLE factor_candidates ADD COLUMN research_stage TEXT DEFAULT 'explore'")

        task_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(research_tasks)").fetchall()}
        if task_cols and "worker_note" not in task_cols:
            self.conn.execute("ALTER TABLE research_tasks ADD COLUMN worker_note TEXT")

        risk_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(candidate_risk_profile)").fetchall()}
        if risk_cols and "profile_json" not in risk_cols:
            self.conn.execute("ALTER TABLE candidate_risk_profile ADD COLUMN profile_json TEXT")

        exposure_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(exposure_factors)").fetchall()}
        exposure_additions = {
            "bucket_key": "TEXT",
            "bucket_label": "TEXT",
            "effective_bucket_key": "TEXT",
            "effective_bucket_label": "TEXT",
            "retention_industry": "REAL",
            "retention_industry_size": "REAL",
            "retention_full": "REAL",
            "industry_top1_weight": "REAL",
            "industry_hhi": "REAL",
            "turnover_daily": "REAL",
            "net_metric": "REAL",
            "liquidity_bottom20_retention": "REAL",
            "strength_subscore": "REAL",
            "robustness_subscore": "REAL",
            "controllability_subscore": "REAL",
            "implementability_subscore": "REAL",
            "novelty_subscore": "REAL",
            "total_score": "REAL",
            "hard_flags_json": "TEXT",
        }
        for col_name, col_type in exposure_additions.items():
            if exposure_cols and col_name not in exposure_cols:
                self.conn.execute(f"ALTER TABLE exposure_factors ADD COLUMN {col_name} {col_type}")

        trial_cols = {row[1] for row in self.conn.execute("PRAGMA table_info(research_trial_log)").fetchall()}
        if trial_cols and "details_json" not in trial_cols:
            self.conn.execute("ALTER TABLE research_trial_log ADD COLUMN details_json TEXT")

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

    def upsert_exposure_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        payload_rows = []
        for row in rows:
            payload_rows.append(
                (
                    row["run_id"],
                    row["factor_name"],
                    row["exposure_type"],
                    row.get("exposure_label"),
                    row.get("bucket_key"),
                    row.get("bucket_label"),
                    row.get("effective_bucket_key"),
                    row.get("effective_bucket_label"),
                    row.get("strength_score"),
                    row.get("raw_rank_ic_mean"),
                    row.get("raw_rank_ic_ir"),
                    row.get("neutralized_rank_ic_mean"),
                    (int(bool(row.get("neutralized_pass_gate"))) if row.get("neutralized_pass_gate") is not None else None),
                    row.get("retention_industry"),
                    row.get("retention_industry_size"),
                    row.get("retention_full"),
                    row.get("industry_top1_weight"),
                    row.get("industry_hhi"),
                    row.get("turnover_daily"),
                    row.get("net_metric"),
                    row.get("liquidity_bottom20_retention"),
                    row.get("strength_subscore"),
                    row.get("robustness_subscore"),
                    row.get("controllability_subscore"),
                    row.get("implementability_subscore"),
                    row.get("novelty_subscore"),
                    row.get("total_score"),
                    row.get("split_fail_count"),
                    row.get("crowding_peers"),
                    row.get("recommended_max_weight"),
                    row.get("status") or "watch",
                    json.dumps(row.get("hard_flags") or [], ensure_ascii=False),
                    json.dumps(row.get("notes") or {}, ensure_ascii=False),
                    row.get("created_at_utc") or now,
                    row.get("updated_at_utc") or now,
                )
            )

        self.conn.executemany(
            """
            INSERT OR REPLACE INTO exposure_factors (
                run_id, factor_name, exposure_type, exposure_label,
                bucket_key, bucket_label, effective_bucket_key, effective_bucket_label,
                strength_score, raw_rank_ic_mean, raw_rank_ic_ir, neutralized_rank_ic_mean, neutralized_pass_gate,
                retention_industry, retention_industry_size, retention_full,
                industry_top1_weight, industry_hhi, turnover_daily, net_metric, liquidity_bottom20_retention,
                strength_subscore, robustness_subscore, controllability_subscore, implementability_subscore, novelty_subscore,
                total_score,
                split_fail_count, crowding_peers, recommended_max_weight, status, hard_flags_json, notes_json,
                created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload_rows,
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
                WHERE fingerprint = ? AND status IN ('pending', 'running')
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

    def claim_next_research_task(self, blocked_task_types: list[str] | tuple[str, ...] | None = None) -> dict[str, Any] | None:
        blocked_task_types = tuple(task_type for task_type in (blocked_task_types or []) if task_type)
        query = """
            SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                   parent_task_id, attempt_count, last_error, created_at_utc,
                   started_at_utc, finished_at_utc, worker_note
            FROM research_tasks
            WHERE status = 'pending'
        """
        params: list[Any] = []
        if blocked_task_types:
            placeholders = ", ".join("?" for _ in blocked_task_types)
            query += f" AND task_type NOT IN ({placeholders})"
            params.extend(blocked_task_types)
        query += " ORDER BY priority ASC, created_at_utc ASC LIMIT 1"
        row = self.conn.execute(query, params).fetchone()
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

    def _decode_research_task_rows(self, rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
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
        return self._decode_research_task_rows(rows)

    def list_research_tasks_by_status(
        self,
        statuses: list[str] | tuple[str, ...],
        *,
        limit: int = 1000,
        oldest_first: bool = False,
    ) -> list[dict[str, Any]]:
        wanted = tuple(str(status).strip() for status in statuses if str(status).strip())
        if not wanted:
            return []
        order = "ASC" if oldest_first else "DESC"
        placeholders = ", ".join("?" for _ in wanted)
        rows = self.conn.execute(
            f"""
            SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                   parent_task_id, attempt_count, last_error, created_at_utc,
                   started_at_utc, finished_at_utc, worker_note
            FROM research_tasks
            WHERE status IN ({placeholders})
            ORDER BY COALESCE(started_at_utc, created_at_utc) {order}, created_at_utc {order}
            LIMIT ?
            """,
            (*wanted, limit),
        ).fetchall()
        return self._decode_research_task_rows(rows)

    def upsert_factor_candidate(
        self,
        *,
        name: str,
        family: str | None,
        definition: dict[str, Any],
        expression: str | None = None,
        origin_task_id: str | None = None,
        origin_run_id: str | None = None,
        factor_role: str | None = None,
    ) -> str:
        now = datetime.now(timezone.utc).isoformat()
        row = self.conn.execute("SELECT id FROM factor_candidates WHERE name = ?", (name,)).fetchone()
        if row:
            candidate_id = row[0]
            self.conn.execute(
                """
                UPDATE factor_candidates
                SET family = COALESCE(?, family),
                    definition_json = ?,
                    expression = COALESCE(?, expression),
                    origin_task_id = COALESCE(?, origin_task_id),
                    origin_run_id = COALESCE(?, origin_run_id),
                    factor_role = COALESCE(?, factor_role),
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    family,
                    json.dumps(definition, ensure_ascii=False, sort_keys=True),
                    expression,
                    origin_task_id,
                    origin_run_id,
                    factor_role,
                    now,
                    candidate_id,
                ),
            )
        else:
            candidate_id = str(uuid4())
            self.conn.execute(
                """
                INSERT INTO factor_candidates (
                    id, name, family, definition_json, expression, origin_task_id, origin_run_id,
                    factor_role, status, research_stage, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new', 'explore', ?, ?)
                """,
                (
                    candidate_id,
                    name,
                    family,
                    json.dumps(definition, ensure_ascii=False, sort_keys=True),
                    expression,
                    origin_task_id,
                    origin_run_id,
                    factor_role,
                    now,
                    now,
                ),
            )
        self.conn.commit()
        return candidate_id

    def insert_factor_evaluation(self, payload: dict[str, Any]) -> str:
        evaluation_id = payload.get("id") or str(uuid4())
        self.conn.execute(
            """
            INSERT INTO factor_evaluations (
                id, candidate_id, run_id, task_id, window_label, market_scope, sample_size, observations,
                return_metric, sharpe_like, max_drawdown, turnover, coverage,
                raw_rank_ic_mean, neutralized_rank_ic_mean, split_fail_count, high_corr_peer_count,
                robust_pass_count, robust_total_count, stability_score, quality_score, final_score,
                pass_flag, status, rejection_reason, notes_json, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                evaluation_id,
                payload["candidate_id"],
                payload.get("run_id"),
                payload.get("task_id"),
                payload.get("window_label"),
                payload.get("market_scope"),
                payload.get("sample_size"),
                payload.get("observations"),
                payload.get("return_metric"),
                payload.get("sharpe_like"),
                payload.get("max_drawdown"),
                payload.get("turnover"),
                payload.get("coverage"),
                payload.get("raw_rank_ic_mean"),
                payload.get("neutralized_rank_ic_mean"),
                payload.get("split_fail_count"),
                payload.get("high_corr_peer_count"),
                payload.get("robust_pass_count"),
                payload.get("robust_total_count"),
                payload.get("stability_score"),
                payload.get("quality_score"),
                payload.get("final_score"),
                int(bool(payload.get("pass_flag"))),
                payload.get("status"),
                payload.get("rejection_reason"),
                json.dumps(payload.get("notes") or {}, ensure_ascii=False, sort_keys=True),
                payload.get("created_at_utc") or datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.conn.commit()
        return evaluation_id

    def list_factor_evaluations(self, candidate_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if candidate_id:
            where = "WHERE candidate_id = ?"
            params.append(candidate_id)
        params.append(limit)
        rows = self.conn.execute(
            f"""
            SELECT id, candidate_id, run_id, task_id, window_label, market_scope, sample_size, observations,
                   return_metric, sharpe_like, max_drawdown, turnover, coverage,
                   raw_rank_ic_mean, neutralized_rank_ic_mean, split_fail_count, high_corr_peer_count,
                   robust_pass_count, robust_total_count, stability_score, quality_score, final_score,
                   pass_flag, status, rejection_reason, notes_json, created_at_utc
            FROM factor_evaluations
            {where}
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        columns = [
            "id", "candidate_id", "run_id", "task_id", "window_label", "market_scope", "sample_size", "observations",
            "return_metric", "sharpe_like", "max_drawdown", "turnover", "coverage",
            "raw_rank_ic_mean", "neutralized_rank_ic_mean", "split_fail_count", "high_corr_peer_count",
            "robust_pass_count", "robust_total_count", "stability_score", "quality_score", "final_score",
            "pass_flag", "status", "rejection_reason", "notes_json", "created_at_utc",
        ]
        items = []
        for row in rows:
            item = dict(zip(columns, row))
            item["notes"] = json.loads(item.pop("notes_json") or "{}")
            items.append(item)
        return items

    def list_factor_candidates(self, limit: int = 100, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            where = f"WHERE status IN ({placeholders})"
            params.extend(statuses)
        params.append(limit)
        rows = self.conn.execute(
            f"""
            SELECT id, name, family, definition_json, expression, origin_task_id, origin_run_id, factor_role, status, research_stage,
                   evaluation_count, window_count, avg_final_score, best_final_score, latest_final_score,
                   latest_recent_final_score, pass_rate, summary, next_action, rejection_reason, duplicate_of, created_at_utc, updated_at_utc
            FROM factor_candidates
            {where}
            ORDER BY COALESCE(latest_recent_final_score, latest_final_score, -999) DESC, updated_at_utc DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        columns = [
            "id", "name", "family", "definition_json", "expression", "origin_task_id", "origin_run_id", "factor_role", "status", "research_stage",
            "evaluation_count", "window_count", "avg_final_score", "best_final_score", "latest_final_score",
            "latest_recent_final_score", "pass_rate", "summary", "next_action", "rejection_reason", "duplicate_of", "created_at_utc", "updated_at_utc",
        ]
        items = []
        for row in rows:
            item = dict(zip(columns, row))
            item["definition"] = json.loads(item.pop("definition_json") or "{}")
            items.append(item)
        return items

    def get_factor_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        rows = self.list_factor_candidates(limit=1000)
        for row in rows:
            if row["id"] == candidate_id:
                return row
        return None

    def refresh_factor_candidate(self, candidate_id: str, summary: dict[str, Any]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            UPDATE factor_candidates
            SET status = ?, research_stage = ?, evaluation_count = ?, window_count = ?, avg_final_score = ?, best_final_score = ?,
                latest_final_score = ?, latest_recent_final_score = ?, pass_rate = ?, summary = ?, next_action = ?, rejection_reason = ?, updated_at_utc = ?
            WHERE id = ?
            """,
            (
                summary.get("status") or "new",
                summary.get("research_stage") or "explore",
                summary.get("evaluation_count") or 0,
                summary.get("window_count") or 0,
                summary.get("avg_final_score"),
                summary.get("best_final_score"),
                summary.get("latest_final_score"),
                summary.get("latest_recent_final_score"),
                summary.get("pass_rate"),
                summary.get("summary"),
                summary.get("next_action"),
                summary.get("rejection_reason"),
                now,
                candidate_id,
            ),
        )
        self.conn.commit()

    def upsert_research_hypothesis(self, candidate_id: str, payload: dict[str, Any]) -> str:
        now = datetime.now(timezone.utc).isoformat()
        row = self.conn.execute("SELECT id FROM research_hypotheses WHERE candidate_id = ?", (candidate_id,)).fetchone()
        if row:
            hypothesis_id = row[0]
            self.conn.execute(
                """
                UPDATE research_hypotheses
                SET title = ?, family = ?, hypothesis_text = ?, status = ?, evidence_for_json = ?,
                    evidence_against_json = ?, next_action = ?, last_reviewed_at_utc = ?
                WHERE id = ?
                """,
                (
                    payload["title"],
                    payload.get("family"),
                    payload.get("hypothesis_text"),
                    payload.get("status"),
                    payload.get("evidence_for_json"),
                    payload.get("evidence_against_json"),
                    payload.get("next_action"),
                    now,
                    hypothesis_id,
                ),
            )
        else:
            hypothesis_id = str(uuid4())
            self.conn.execute(
                """
                INSERT INTO research_hypotheses (
                    id, candidate_id, title, family, hypothesis_text, status,
                    evidence_for_json, evidence_against_json, next_action, last_reviewed_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    hypothesis_id,
                    candidate_id,
                    payload["title"],
                    payload.get("family"),
                    payload.get("hypothesis_text"),
                    payload.get("status"),
                    payload.get("evidence_for_json"),
                    payload.get("evidence_against_json"),
                    payload.get("next_action"),
                    now,
                ),
            )
        self.conn.commit()
        return hypothesis_id

    def get_hypothesis_for_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT id, candidate_id, title, family, hypothesis_text, status,
                   evidence_for_json, evidence_against_json, next_action, last_reviewed_at_utc
            FROM research_hypotheses
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        if not row:
            return None
        columns = [
            "id", "candidate_id", "title", "family", "hypothesis_text", "status",
            "evidence_for_json", "evidence_against_json", "next_action", "last_reviewed_at_utc",
        ]
        item = dict(zip(columns, row))
        item["evidence_for"] = json.loads(item.pop("evidence_for_json") or "[]")
        item["evidence_against"] = json.loads(item.pop("evidence_against_json") or "[]")
        return item

    def upsert_research_thesis(self, candidate_id: str, payload: dict[str, Any]) -> str:
        now = datetime.now(timezone.utc).isoformat()
        row = self.conn.execute("SELECT id FROM research_theses WHERE candidate_id = ?", (candidate_id,)).fetchone()
        if row:
            thesis_row_id = row[0]
            self.conn.execute(
                """
                UPDATE research_theses
                SET thesis_id = ?, title = ?, family = ?, thesis_type = ?,
                    institutional_bucket_key = ?, institutional_bucket_label = ?,
                    thesis_text = ?, mechanism_rationale = ?, status = ?, invalidation_json = ?,
                    representative_candidate = ?, representative_rank = ?, representative_count = ?,
                    roster_json = ?, source_context_json = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    payload["thesis_id"],
                    payload["title"],
                    payload.get("family"),
                    payload.get("thesis_type"),
                    payload.get("institutional_bucket_key"),
                    payload.get("institutional_bucket_label"),
                    payload.get("thesis_text"),
                    payload.get("mechanism_rationale"),
                    payload.get("status"),
                    payload.get("invalidation_json"),
                    payload.get("representative_candidate"),
                    payload.get("representative_rank"),
                    payload.get("representative_count"),
                    payload.get("roster_json"),
                    payload.get("source_context_json"),
                    now,
                    thesis_row_id,
                ),
            )
        else:
            thesis_row_id = str(uuid4())
            self.conn.execute(
                """
                INSERT INTO research_theses (
                    id, candidate_id, thesis_id, title, family, thesis_type,
                    institutional_bucket_key, institutional_bucket_label, thesis_text,
                    mechanism_rationale, status, invalidation_json, representative_candidate,
                    representative_rank, representative_count, roster_json, source_context_json,
                    created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thesis_row_id,
                    candidate_id,
                    payload["thesis_id"],
                    payload["title"],
                    payload.get("family"),
                    payload.get("thesis_type"),
                    payload.get("institutional_bucket_key"),
                    payload.get("institutional_bucket_label"),
                    payload.get("thesis_text"),
                    payload.get("mechanism_rationale"),
                    payload.get("status"),
                    payload.get("invalidation_json"),
                    payload.get("representative_candidate"),
                    payload.get("representative_rank"),
                    payload.get("representative_count"),
                    payload.get("roster_json"),
                    payload.get("source_context_json"),
                    now,
                    now,
                ),
            )
        self.conn.commit()
        return thesis_row_id

    def get_research_thesis_for_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        row = self.conn.execute(
            """
            SELECT id, candidate_id, thesis_id, title, family, thesis_type,
                   institutional_bucket_key, institutional_bucket_label, thesis_text,
                   mechanism_rationale, status, invalidation_json, representative_candidate,
                   representative_rank, representative_count, roster_json, source_context_json,
                   created_at_utc, updated_at_utc
            FROM research_theses
            WHERE candidate_id = ?
            """,
            (candidate_id,),
        ).fetchone()
        if not row:
            return None
        columns = [
            "id", "candidate_id", "thesis_id", "title", "family", "thesis_type",
            "institutional_bucket_key", "institutional_bucket_label", "thesis_text",
            "mechanism_rationale", "status", "invalidation_json", "representative_candidate",
            "representative_rank", "representative_count", "roster_json", "source_context_json",
            "created_at_utc", "updated_at_utc",
        ]
        item = dict(zip(columns, row))
        item["invalidation"] = json.loads(item.pop("invalidation_json") or "[]")
        item["roster"] = json.loads(item.pop("roster_json") or "[]")
        item["source_context"] = json.loads(item.pop("source_context_json") or "{}")
        return item

    def upsert_candidate_relationship(
        self,
        *,
        left_candidate_id: str,
        right_candidate_id: str,
        relationship_type: str,
        run_id: str | None = None,
        strength: float | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        if left_candidate_id == right_candidate_id:
            return
        if left_candidate_id > right_candidate_id:
            left_candidate_id, right_candidate_id = right_candidate_id, left_candidate_id
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO candidate_relationships (
                left_candidate_id, right_candidate_id, relationship_type, run_id, strength,
                details_json, created_at_utc, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(left_candidate_id, right_candidate_id, relationship_type)
            DO UPDATE SET run_id=excluded.run_id,
                          strength=excluded.strength,
                          details_json=excluded.details_json,
                          updated_at_utc=excluded.updated_at_utc
            """,
            (
                left_candidate_id,
                right_candidate_id,
                relationship_type,
                run_id,
                strength,
                json.dumps(details or {}, ensure_ascii=False, sort_keys=True),
                now,
                now,
            ),
        )
        self.conn.commit()

    def list_candidate_relationships(self, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT r.left_candidate_id, l.name, r.right_candidate_id, rr.name,
                   r.relationship_type, r.run_id, r.strength, r.details_json,
                   r.created_at_utc, r.updated_at_utc
            FROM candidate_relationships r
            LEFT JOIN factor_candidates l ON l.id = r.left_candidate_id
            LEFT JOIN factor_candidates rr ON rr.id = r.right_candidate_id
            ORDER BY COALESCE(r.strength, 0) DESC, r.updated_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        items = []
        for row in rows:
            items.append(
                {
                    "left_candidate_id": row[0],
                    "left_name": row[1],
                    "right_candidate_id": row[2],
                    "right_name": row[3],
                    "relationship_type": row[4],
                    "run_id": row[5],
                    "strength": row[6],
                    "details": json.loads(row[7] or "{}"),
                    "created_at_utc": row[8],
                    "updated_at_utc": row[9],
                }
            )
        return items

    def insert_candidate_robustness_checks(self, rows: Iterable[dict[str, Any]]) -> None:
        payload = []
        for row in rows:
            payload.append((
                row.get("id") or str(uuid4()),
                row["candidate_id"],
                row.get("run_id"),
                row["check_name"],
                row.get("status") or "unknown",
                row.get("severity") or "medium",
                row.get("score"),
                row.get("weight"),
                json.dumps(row.get("evidence") or {}, ensure_ascii=False, sort_keys=True),
                row.get("rationale"),
                row.get("created_at_utc") or datetime.now(timezone.utc).isoformat(),
            ))
        self.conn.executemany(
            """
            INSERT INTO candidate_robustness_checks (
                id, candidate_id, run_id, check_name, status, severity, score, weight,
                evidence_json, rationale, created_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        self.conn.commit()

    def replace_candidate_risk_profile(self, candidate_id: str, payload: dict[str, Any]) -> None:
        now = payload.get("updated_at_utc") or datetime.now(timezone.utc).isoformat()
        self.conn.execute("DELETE FROM candidate_robustness_checks WHERE candidate_id = ?", (candidate_id,))
        checks = list(payload.get("checks") or [])
        if checks:
            self.insert_candidate_robustness_checks([
                {**row, "candidate_id": candidate_id, "run_id": payload.get("run_id"), "created_at_utc": now}
                for row in checks
            ])
        self.conn.execute(
            """
            INSERT INTO candidate_risk_profile (
                candidate_id, run_id, risk_level, risk_score, robustness_score, family_context_score, graph_context_score,
                evaluation_count, passing_check_count, failing_check_count, summary, key_risks_json, mitigations_json,
                profile_json, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(candidate_id) DO UPDATE SET
                run_id=excluded.run_id,
                risk_level=excluded.risk_level,
                risk_score=excluded.risk_score,
                robustness_score=excluded.robustness_score,
                family_context_score=excluded.family_context_score,
                graph_context_score=excluded.graph_context_score,
                evaluation_count=excluded.evaluation_count,
                passing_check_count=excluded.passing_check_count,
                failing_check_count=excluded.failing_check_count,
                summary=excluded.summary,
                key_risks_json=excluded.key_risks_json,
                mitigations_json=excluded.mitigations_json,
                profile_json=excluded.profile_json,
                updated_at_utc=excluded.updated_at_utc
            """,
            (
                candidate_id,
                payload.get("run_id"),
                payload.get("risk_level") or "unknown",
                payload.get("risk_score") or 0.0,
                payload.get("robustness_score"),
                payload.get("family_context_score"),
                payload.get("graph_context_score"),
                int(payload.get("evaluation_count") or 0),
                int(payload.get("passing_check_count") or 0),
                int(payload.get("failing_check_count") or 0),
                payload.get("summary"),
                json.dumps(payload.get("key_risks") or [], ensure_ascii=False),
                json.dumps(payload.get("mitigations") or [], ensure_ascii=False),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                now,
            ),
        )
        self.conn.commit()

    def list_candidate_risk_profiles(self, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT rp.candidate_id, fc.name, fc.family, fc.status, rp.run_id, rp.risk_level, rp.risk_score,
                   rp.robustness_score, rp.family_context_score, rp.graph_context_score, rp.evaluation_count,
                   rp.passing_check_count, rp.failing_check_count, rp.summary, rp.key_risks_json,
                   rp.mitigations_json, rp.profile_json, rp.updated_at_utc
            FROM candidate_risk_profile rp
            LEFT JOIN factor_candidates fc ON fc.id = rp.candidate_id
            ORDER BY rp.risk_score DESC, rp.updated_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        items = []
        for row in rows:
            items.append({
                "candidate_id": row[0], "candidate_name": row[1], "family": row[2], "candidate_status": row[3],
                "run_id": row[4], "risk_level": row[5], "risk_score": row[6], "robustness_score": row[7],
                "family_context_score": row[8], "graph_context_score": row[9], "evaluation_count": row[10],
                "passing_check_count": row[11], "failing_check_count": row[12], "summary": row[13],
                "key_risks": json.loads(row[14] or '[]'), "mitigations": json.loads(row[15] or '[]'),
                "profile": json.loads(row[16] or '{}'), "updated_at_utc": row[17],
            })
        return items

    def list_candidate_robustness_checks(self, candidate_id: str | None = None, limit: int = 5000) -> list[dict[str, Any]]:
        where = ''
        params: list[Any] = []
        if candidate_id:
            where = 'WHERE candidate_id = ?'
            params.append(candidate_id)
        params.append(limit)
        rows = self.conn.execute(
            f"""
            SELECT id, candidate_id, run_id, check_name, status, severity, score, weight, evidence_json, rationale, created_at_utc
            FROM candidate_robustness_checks
            {where}
            ORDER BY created_at_utc DESC, check_name ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        items = []
        for row in rows:
            items.append({
                "id": row[0], "candidate_id": row[1], "run_id": row[2], "check_name": row[3], "status": row[4],
                "severity": row[5], "score": row[6], "weight": row[7], "evidence": json.loads(row[8] or '{}'),
                "rationale": row[9], "created_at_utc": row[10],
            })
        return items

    def upsert_research_trial_log(self, payload: dict[str, Any]) -> None:
        self.conn.execute(
            """
            INSERT INTO research_trial_log (
                source_task_id, fingerprint, family, category, candidate_name, status, outcome_label,
                knowledge_gain_count, pressure_weight, created_at_utc, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_task_id) DO UPDATE SET
                fingerprint=excluded.fingerprint,
                family=excluded.family,
                category=excluded.category,
                candidate_name=excluded.candidate_name,
                status=excluded.status,
                outcome_label=excluded.outcome_label,
                knowledge_gain_count=excluded.knowledge_gain_count,
                pressure_weight=excluded.pressure_weight,
                created_at_utc=excluded.created_at_utc,
                details_json=excluded.details_json
            """,
            (
                payload["source_task_id"],
                payload.get("fingerprint"),
                payload.get("family") or "other",
                payload.get("category"),
                payload.get("candidate_name"),
                payload.get("status") or "pending",
                payload.get("outcome_label") or "pending",
                int(payload.get("knowledge_gain_count") or 0),
                float(payload.get("pressure_weight") or 1.0),
                payload.get("created_at_utc") or datetime.now(timezone.utc).isoformat(),
                json.dumps(payload.get("details") or {}, ensure_ascii=False, sort_keys=True),
            ),
        )
        self.conn.commit()

    def sync_research_trial_logs_from_tasks(self, limit: int = 500) -> list[dict[str, Any]]:
        from factor_lab.research_trials import build_trial_log_entry
        rows = self.conn.execute(
            """
            SELECT task_id, task_type, status, priority, fingerprint, payload_json, parent_task_id,
                   attempt_count, last_error, created_at_utc, started_at_utc, finished_at_utc, worker_note
            FROM research_tasks
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        synced = []
        for row in rows:
            task = {
                "task_id": row[0], "task_type": row[1], "status": row[2], "priority": row[3],
                "fingerprint": row[4], "payload": json.loads(row[5] or '{}'), "parent_task_id": row[6],
                "attempt_count": row[7], "last_error": row[8], "created_at_utc": row[9],
                "started_at_utc": row[10], "finished_at_utc": row[11], "worker_note": row[12],
            }
            payload = build_trial_log_entry(task)
            self.upsert_research_trial_log(payload)
            synced.append(payload)
        return synced

    def list_research_trial_logs(self, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT source_task_id, fingerprint, family, category, candidate_name, status, outcome_label,
                   knowledge_gain_count, pressure_weight, created_at_utc, details_json
            FROM research_trial_log
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [{
            "source_task_id": row[0], "fingerprint": row[1], "family": row[2], "category": row[3],
            "candidate_name": row[4], "status": row[5], "outcome_label": row[6],
            "knowledge_gain_count": row[7], "pressure_weight": row[8], "created_at_utc": row[9],
            "details": json.loads(row[10] or '{}'),
        } for row in rows]

    def summarize_research_trials(self, limit: int = 1000) -> dict[str, dict[str, Any]]:
        from factor_lab.research_trials import build_family_trial_summary
        return build_family_trial_summary(self.list_research_trial_logs(limit=limit))

    def top_promising_candidates(self, limit: int = 5) -> list[dict[str, Any]]:
        return self.list_factor_candidates(limit=limit, statuses=["promising", "testing"])
