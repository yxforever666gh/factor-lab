from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from factor_lab.candidate_graph import build_graph_artifacts
from factor_lab.analyst_signal_bridge import load_analyst_signals
from factor_lab.analyst_feedback_context import build_analyst_feedback_context
from factor_lab.research_runtime_state import queue_budget_snapshot, recent_failure_stats, exploration_health
from factor_lab.frontier_policy import build_frontier_focus
from factor_lab.promotion_scorecard import build_promotion_scorecard
from factor_lab.approved_universe import write_approved_candidate_universe, resolve_recent_finished_runs, load_run_candidate_artifacts
from factor_lab.candidate_failure_dossier import build_candidate_failure_dossiers
from factor_lab.research_learning import build_research_learning
from factor_lab.novelty_judge import write_novelty_judgments
from factor_lab.allocator_governance_auditor import write_allocator_governance_audit
from factor_lab.decision_ab_judge import write_decision_ab_report
from factor_lab.failure_analyst_enhancement import write_failure_analyst_enhancement
from factor_lab.factor_pipeline_integration import build_integrated_factor_reports


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

        approved_universe = write_approved_candidate_universe(
            db_path=db_path,
            output_path=root / "approved_candidate_universe.json",
            debug_output_path=root / "approved_candidate_universe_debug.json",
            lifecycle_output_path=root / "approved_candidate_universe_lifecycle.json",
            governance_output_path=root / "approved_candidate_universe_governance.json",
        )

        daemon_status = _read_json(root / "research_daemon_status.json", {})
        recommendation_context = _read_json(root / "llm_recommendation_context.json", {})
        recommendation_weights = _read_json(root / "llm_recommendation_weights.json", {})
        recommendation_history = _read_json(root / "llm_recommendation_history.json", [])
        llm_status = _read_json(root / "llm_status.json", {})
        candidate_graph_context = _read_json(root / "candidate_graph_context.json", {})
        repair_feedback = _read_json(root / "repair_feedback.json", {})
        repair_metrics = _read_json(root / "repair_metrics.json", {})
        promotion_scorecard = build_promotion_scorecard(db_path=db_path, limit=20)
        frontier_focus = build_frontier_focus(promotion_scorecard)
        candidate_risk_profiles = _read_json(root / "candidate_risk_profiles.json", [])
        risk_by_name = {row.get("candidate_name"): row for row in candidate_risk_profiles if row.get("candidate_name")}
        family_summary = candidate_graph_context.get("families") or _read_json(root / "family_summary.json", [])
        candidate_clusters = candidate_graph_context.get("clusters") or _read_json(root / "candidate_clusters.json", [])
        cluster_representatives = candidate_graph_context.get("cluster_representatives") or _read_json(root / "cluster_representatives.json", [])
        candidate_context = []
        for row in (candidate_graph_context.get("candidate_context") or []):
            risk = risk_by_name.get(row.get("candidate_name")) or {}
            candidate_context.append({
                **row,
                "risk_level": risk.get("risk_level"),
                "risk_score": risk.get("risk_score"),
                "robustness_score": risk.get("robustness_score"),
                "fragile": bool((risk.get("profile") or {}).get("fragile")),
                "acceptance_gate": (risk.get("profile") or {}).get("acceptance_gate") or {},
            })
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
        failure_focus_names: list[str] = []
        for group in (
            frontier_focus.get("robust_candidates") or [],
            frontier_focus.get("soft_robust_candidates") or [],
            frontier_focus.get("preferred_candidates") or [],
        ):
            for name in group:
                if name and name not in failure_focus_names:
                    failure_focus_names.append(name)
        candidate_failure_dossiers = build_candidate_failure_dossiers(
            store.list_factor_candidates(limit=2000),
            store.list_factor_evaluations(limit=5000),
            store.list_candidate_relationships(limit=5000),
            focus_names=failure_focus_names[:12],
            limit=12,
        )
        queue_budget = queue_budget_snapshot(store)
        failure_state = recent_failure_stats(store)
        exploration_state = exploration_health(store)
        analyst_signals = load_analyst_signals(root)
        analyst_feedback_context = build_analyst_feedback_context(root)
        research_flow_state = _read_json(root / "research_flow_state.json", {})
        research_learning = build_research_learning(root / "research_memory.json")
        representative_failure_dossiers = research_learning.get("representative_failure_dossiers") or {}
        failure_question_cards = research_learning.get("failure_question_cards") or []
        frontier_representatives: list[dict[str, Any]] = []
        for name in failure_focus_names[:12]:
            if not name:
                continue
            rep_row = next((row for row in cluster_representatives if row.get("representative_candidate") == name or row.get("primary_candidate") == name), {})
            frontier_representatives.append(
                {
                    "candidate_name": name,
                    "cluster_id": rep_row.get("cluster_id"),
                    "representative_rank": rep_row.get("representative_rank"),
                    "representative_count": rep_row.get("representative_count"),
                    "failure_dossier": representative_failure_dossiers.get(name) or {},
                }
            )

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

        novelty_judge = write_novelty_judgments({
            "promotion_scorecard": promotion_scorecard,
            "candidate_failure_dossiers": candidate_failure_dossiers,
            "approved_universe": approved_universe,
        }, root)
        allocator_governance_audit = write_allocator_governance_audit(
            approved_universe=approved_universe,
            artifacts_dir=root,
            current_portfolio=_read_json(root / "paper_portfolio" / "current_portfolio.json", {}),
        )
        failure_analyst_enhancement = write_failure_analyst_enhancement(
            {
                "candidate_failure_dossiers": candidate_failure_dossiers,
                "representative_failure_dossiers": representative_failure_dossiers,
                "failure_question_cards": failure_question_cards,
            },
            root,
        )
        integrated_factor_reports = build_integrated_factor_reports(
            approved_universe=approved_universe,
            recent_artifacts=[load_run_candidate_artifacts(run) for run in resolve_recent_finished_runs(db_path, limit=12)],
            artifacts_dir=root,
        )

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
            "repair_feedback": repair_feedback,
            "repair_metrics": repair_metrics,
            "recommendation_context": recommendation_context,
            "recommendation_weights": recommendation_weights,
            "recommendation_history_tail": recommendation_history[-10:],
            "llm_status": llm_status,
            "family_summary": family_summary,
            "family_recommendations": family_recommendations,
            "candidate_clusters": candidate_clusters,
            "cluster_representatives": cluster_representatives,
            "candidate_context": candidate_context,
            "relationship_summary": relationship_summary,
            "candidate_failure_dossiers": candidate_failure_dossiers,
            "knowledge_gain_counter": knowledge_gain_counter,
            "research_trial_summary": research_trial_summary,
            "analyst_signals": analyst_signals,
            "analyst_feedback_context": analyst_feedback_context,
            "research_flow_state": research_flow_state,
            "research_learning": research_learning,
            "representative_failure_dossiers": representative_failure_dossiers,
            "failure_question_cards": failure_question_cards,
            "frontier_representatives": frontier_representatives,
            "promotion_scorecard": promotion_scorecard,
            "frontier_focus": frontier_focus,
            "approved_universe": approved_universe,
            "approved_universe_summary": approved_universe.get("summary") or {},
            "approved_universe_governance_summary": (approved_universe.get("governance") or {}).get("summary") or {},
            "approved_universe_budget_summary": approved_universe.get("budget_summary") or {},
            "approved_universe_names": [row.get("factor_name") for row in (approved_universe.get("rows") or []) if row.get("factor_name")],
            "approved_universe_debug_tail": (approved_universe.get("debug_rows") or [])[:20],
            "novelty_judge": novelty_judge,
            "allocator_governance_audit": allocator_governance_audit,
            "failure_analyst_enhancement": failure_analyst_enhancement,
            "integrated_factor_reports": integrated_factor_reports,
            "recent_research_tasks": [
                {
                    **{k: v for k, v in task.items() if k != "payload_json"},
                    "payload": json.loads(task["payload_json"]) if task.get("payload_json") else {},
                }
                for task in tasks[:30]
            ],
        }
        decision_ab = write_decision_ab_report(payload, root)
        payload["decision_ab"] = decision_ab
        Path(output_path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
    finally:
        conn.close()
