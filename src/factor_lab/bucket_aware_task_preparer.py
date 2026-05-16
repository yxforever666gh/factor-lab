from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from factor_lab.controlled_route_policy import load_controlled_route_policy, route_decision_rank
from factor_lab.dedup import workflow_experiment_fingerprint
from factor_lab.storage import ExperimentStore
from factor_lab.value_sleeve_policy import load_value_sleeve_policy, route_sleeve_action
from factor_lab.workflow_admission_adapter import enforce_workflow_admission


def _payload_from_config(path: Path, *, output_dir: str | None = None) -> dict:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    payload = {
        "config_path": str(path),
        "output_dir": output_dir or str(cfg.get("output_dir") or f"artifacts/value_route_bucket_aware/daemon_runs/{cfg.get('route_id', path.stem)}"),
        "mechanism_id": cfg.get("mechanism_id"),
        "route_id": cfg.get("route_id"),
        "followup_type": cfg.get("followup_type"),
        "expected_new_evidence": cfg.get("expected_new_evidence"),
        "required_data_fields": cfg.get("required_data_fields") or [],
        "factors": cfg.get("factors") or [],
        "portfolio_construction": cfg.get("portfolio_construction") or {},
        "source": cfg.get("source") or "bucket_aware_controlled_validation",
    }
    return payload


def _has_recent_finished_equivalent(db_path: str | Path, fingerprint: str) -> bool:
    try:
        conn = sqlite3.connect(db_path)
        row = conn.execute(
            """
            SELECT task_id FROM research_tasks
            WHERE fingerprint = ? AND status = 'finished'
            ORDER BY finished_at_utc DESC
            LIMIT 1
            """,
            (fingerprint,),
        ).fetchone()
        return row is not None
    except sqlite3.Error:
        return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


def prepare_bucket_aware_tasks(
    *,
    config_paths: Iterable[str | Path] | None = None,
    db_path: str | Path = "artifacts/factor_lab.db",
    dry_run: bool = True,
    limit: int | None = None,
    priority: int = 0,
    force_new: bool = False,
    route_history_counts: dict[str, int] | None = None,
    route_policy: dict | None = None,
    route_id: str | None = None,
    followup_type: str | None = None,
    value_sleeve_policy: dict | None = None,
    value_sleeve_policy_path: str | Path | None = None,
) -> dict:
    paths = [Path(p) for p in (config_paths or sorted(Path("artifacts/value_route_bucket_aware").glob("*_bucket_aware.json")))]
    admitted_rows = []
    blocked_rows = []
    enqueued = []
    store = None if dry_run else ExperimentStore(Path(db_path))
    history = route_history_counts or {}
    policy = route_policy if route_policy is not None else load_controlled_route_policy()
    policy_routes = (policy or {}).get("routes") or {}
    if value_sleeve_policy is not None:
        sleeve_policy = value_sleeve_policy
    elif value_sleeve_policy_path is not None:
        sleeve_policy = load_value_sleeve_policy(value_sleeve_policy_path)
    else:
        sleeve_policy = load_value_sleeve_policy()
    try:
        for path in paths:
            cfg = json.loads(path.read_text(encoding="utf-8"))
            if route_id is not None and str(cfg.get("route_id") or "") != route_id:
                continue
            if followup_type is not None and str(cfg.get("followup_type") or "") != followup_type:
                continue
            payload = _payload_from_config(path)
            task = {"task_type": "workflow", "payload": payload, "worker_note": f"bucket_aware｜{payload.get('route_id')}"}
            admission = enforce_workflow_admission(task)
            row = {"config_path": str(path), "payload": payload, "admission": {"decision": admission.get("decision"), "reasons": admission.get("reasons") or []}}
            route = str(payload.get("route_id") or "")
            sleeve_action = route_sleeve_action(route, sleeve_policy)
            row["value_sleeve_role"] = sleeve_action.get("role")
            row["value_sleeve_action"] = sleeve_action.get("action")
            row["value_sleeve_admission_rank"] = int(sleeve_action.get("admission_rank", 99))
            row["value_sleeve_reason"] = sleeve_action.get("reason", "")
            if admission.get("decision") != "allow":
                blocked_rows.append(row)
                continue
            fingerprint = f"bucket_aware::{workflow_experiment_fingerprint(cfg)}"
            if not force_new and _has_recent_finished_equivalent(db_path, fingerprint):
                row["admission"] = {"decision": "skip", "reasons": ["recent_equivalent_evidence_exists"]}
                blocked_rows.append(row)
                continue
            row["selection_reason"] = "least_recent_route" if history else "default_order"
            route_policy_row = policy_routes.get(route) if isinstance(policy_routes, dict) else None
            row["route_policy_decision"] = (route_policy_row or {}).get("decision", "neutral") if isinstance(route_policy_row, dict) else "neutral"
            row["route_policy_reason"] = (route_policy_row or {}).get("reason", "") if isinstance(route_policy_row, dict) else ""
            if row["route_policy_decision"] == "demote":
                row["admission"] = {"decision": "skip", "reasons": [row["route_policy_reason"] or "route_policy_demoted"]}
                blocked_rows.append(row)
                continue
            admitted_rows.append(row)
        admitted_rows.sort(key=lambda row: (route_decision_rank(row.get("route_policy_decision")), int(row.get("value_sleeve_admission_rank", 99)), int(history.get(str(row["payload"].get("route_id")), 0)), str(row["payload"].get("route_id") or ""), str(row["config_path"])))
        blocked_rows.sort(key=lambda row: (int(row.get("value_sleeve_admission_rank", 99)), str(row["payload"].get("route_id") or ""), str(row["config_path"])))
        selected_rows = admitted_rows[: max(0, int(limit))] if limit is not None else admitted_rows
        tasks = selected_rows + blocked_rows
        for row in selected_rows:
            if not dry_run and store is not None:
                cfg = json.loads(Path(row["config_path"]).read_text(encoding="utf-8"))
                fingerprint = workflow_experiment_fingerprint(cfg)
                if force_new:
                    fingerprint = f"{fingerprint}::forced::{uuid4()}"
                task_id = store.enqueue_research_task(
                    task_type="workflow",
                    payload=row["payload"],
                    priority=priority,
                    fingerprint=f"bucket_aware::{fingerprint}",
                    worker_note=f"bucket_aware｜{row['payload'].get('route_id')}",
                )
                row["task_id"] = task_id
                enqueued.append(task_id)
    finally:
        if store is not None and hasattr(store, "conn"):
            store.conn.close()
    return {
        "dry_run": dry_run,
        "would_enqueue_count": sum(1 for t in tasks if t["admission"]["decision"] == "allow"),
        "enqueued_count": len(enqueued),
        "task_ids": enqueued,
        "tasks": tasks,
    }
