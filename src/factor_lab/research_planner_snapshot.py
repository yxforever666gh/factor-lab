from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from factor_lab.candidate_graph import build_graph_artifacts
from factor_lab.research_runtime_state import queue_budget_snapshot, recent_failure_stats, exploration_health


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def build_research_planner_snapshot(db_path: str | Path, output_path: str | Path) -> dict[str, Any]:
    db_path = Path(db_path)
    root = db_path.parent
    build_graph_artifacts(db_path, root)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        latest_runs = [dict(row) for row in cur.execute(
            """
            SELECT run_id, created_at_utc, config_path, data_source, start_date, end_date,
                   status, dataset_rows, factor_count
            FROM workflow_runs
            ORDER BY created_at_utc DESC
            LIMIT 20
            """
        ).fetchall()]

        latest_run = latest_runs[0] if latest_runs else None
        latest_candidates: list[str] = []
        latest_graveyard: list[str] = []
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

        stable_candidates = [dict(row) for row in cur.execute(
            "SELECT factor_name, candidate_runs FROM v_stable_candidates ORDER BY candidate_runs DESC, factor_name ASC LIMIT 20"
        ).fetchall()]
        top_scores = [dict(row) for row in cur.execute(
            "SELECT factor_name, ROUND(avg_score, 6) AS avg_score, runs FROM v_factor_score_avg ORDER BY avg_score DESC LIMIT 20"
        ).fetchall()]

        store_conn = sqlite3.connect(db_path)
        store_conn.row_factory = sqlite3.Row
        try:
            tasks = [dict(row) for row in store_conn.execute(
                """
                SELECT task_id, task_type, status, priority, fingerprint, payload_json,
                       parent_task_id, attempt_count, last_error, created_at_utc,
                       started_at_utc, finished_at_utc, worker_note
                FROM research_tasks
                ORDER BY created_at_utc DESC
                LIMIT 100
                """
            ).fetchall()]
        finally:
            store_conn.close()

        queue_counts = {
            "pending": len([t for t in tasks if t["status"] == "pending"]),
            "running": len([t for t in tasks if t["status"] == "running"]),
            "finished": len([t for t in tasks if t["status"] == "finished"]),
            "failed": len([t for t in tasks if t["status"] == "failed"]),
        }

        generated_config_dir = root / "generated_configs"
        generated_configs = sorted([p.name for p in generated_config_dir.glob("*.json")]) if generated_config_dir.exists() else []

        heartbeat_path = root / "system_heartbeat.jsonl"
        heartbeat_tail = []
        if heartbeat_path.exists():
            heartbeat_tail = [json.loads(line) for line in heartbeat_path.read_text(encoding="utf-8").splitlines()[-20:] if line.strip()]

        daemon_status = _read_json(root / "research_daemon_status.json", {})
        recommendation_context = _read_json(root / "llm_recommendation_context.json", {})
        recommendation_weights = _read_json(root / "llm_recommendation_weights.json", {})
        llm_status = _read_json(root / "llm_status.json", {})
        candidate_graph_context = _read_json(root / "candidate_graph_context.json", {})
        family_summary = candidate_graph_context.get("families") or _read_json(root / "family_summary.json", [])
        candidate_clusters = candidate_graph_context.get("clusters") or _read_json(root / "candidate_clusters.json", [])
        cluster_representatives = candidate_graph_context.get("cluster_representatives") or _read_json(root / "cluster_representatives.json", [])
        candidate_context = candidate_graph_context.get("candidate_context") or []
        relationship_summary = candidate_graph_context.get("relationship_summary") or {}
        family_recommendations = [
            {
                "family": row.get("family"),
                "recommended_action": row.get("recommended_action"),
                "family_score": row.get("family_score"),
                "primary_candidate": row.get("primary_candidate"),
                "duplicate_pressure": row.get("duplicate_pressure"),
                "representative_count": row.get("representative_count"),
                "trial_pressure": row.get("trial_pressure"),
                "false_positive_pressure": row.get("false_positive_pressure"),
                "trial_count": row.get("trial_count"),
                "family_risk_score": row.get("family_risk_score"),
                "family_risk_profile": row.get("family_risk_profile") or {},
            }
            for row in family_summary
        ]

        from factor_lab.storage import ExperimentStore
        store = ExperimentStore(db_path)
        store.sync_research_trial_logs_from_tasks(limit=500)
        research_trial_summary = store.summarize_research_trials(limit=1000)
        queue_budget = queue_budget_snapshot(store)
        failure_state = recent_failure_stats(store)
        exploration_state = exploration_health(store)

        knowledge_gain_counter = {
            "stable_candidate_confirmed": 0,
            "repeated_graveyard_confirmed": 0,
            "neutralization_diagnosis_requested": 0,
            "exploration_candidate_survived": 0,
            "exploration_graveyard_identified": 0,
            "no_significant_information_gain": 0,
        }
        for task in tasks[:50]:
            payload = json.loads(task["payload_json"]) if task.get("payload_json") else {}
            gains = [g for g in (payload.get("knowledge_gain") or []) if g]
            note = task.get("worker_note") or ""
            if "knowledge_gain=" in note:
                gains.extend([x.strip() for x in note.split("knowledge_gain=", 1)[-1].split(",") if x.strip()])
            for gain in gains:
                if gain in knowledge_gain_counter:
                    knowledge_gain_counter[gain] += 1

        payload = {
            "latest_run": latest_run,
            "latest_runs": latest_runs,
            "latest_candidates": latest_candidates,
            "latest_graveyard": latest_graveyard,
            "stable_candidates": stable_candidates,
            "top_scores": top_scores,
            "queue_counts": queue_counts,
            "queue_budget": queue_budget,
            "failure_state": failure_state,
            "exploration_state": exploration_state,
            "daemon_status": daemon_status,
            "heartbeat_tail": heartbeat_tail,
            "generated_configs": generated_configs,
            "recommendation_context": recommendation_context,
            "recommendation_weights": recommendation_weights,
            "llm_status": llm_status,
            "family_summary": family_summary,
            "family_recommendations": family_recommendations,
            "candidate_clusters": candidate_clusters,
            "cluster_representatives": cluster_representatives,
            "candidate_context": candidate_context,
            "relationship_summary": relationship_summary,
            "knowledge_gain_counter": knowledge_gain_counter,
            "research_trial_summary": research_trial_summary,
            "recent_research_tasks": [
                {
                    **{k: v for k, v in task.items() if k != "payload_json"},
                    "payload": json.loads(task["payload_json"]) if task.get("payload_json") else {},
                }
                for task in tasks[:30]
            ],
        }
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
    finally:
        conn.close()
