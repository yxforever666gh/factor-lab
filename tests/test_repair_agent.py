import json
from datetime import datetime, timezone
from pathlib import Path

from factor_lab import research_queue
from factor_lab.agent_briefs import build_repair_agent_brief
from factor_lab.repair_agent_engine import build_repair_response
from factor_lab import repair_playbooks, repair_runtime
from factor_lab.repair_playbooks import execute_repair_actions, repair_stale_running_tasks
from factor_lab.repair_runtime import build_repair_runtime_snapshot, write_repair_summary_artifacts
from factor_lab.repair_verifier import verify_repair_actions
from factor_lab.storage import ExperimentStore


def test_repair_engine_identifies_output_state_drift_for_completed_stale_task(tmp_path):
    brief = build_repair_agent_brief(
        {
            "daemon_status": {"state": "running"},
            "queue_budget": {"baseline": 0, "validation": 0, "exploration": 0},
            "queue_counts": {"pending": 0, "running": 1, "finished": 0, "failed": 0},
            "failure_state": {"consecutive_failures": 0, "cooldown_active": False},
            "blocked_lane_status": {},
            "route_status": {"healthy": True},
            "resource_pressure": {},
            "heartbeat_gap": {"available": True, "seconds_since_last": 20},
            "recent_research_tasks": [],
            "recent_failed_or_risky_tasks": [],
            "stale_running_candidates": [{"task_id": "stale-1", "outputs_complete": True, "output_dir": str(tmp_path)}],
            "status_file_consistency": {},
            "open_incidents": [],
        },
        {"open_questions": []},
        {},
        tmp_path / "repair_brief.json",
    )

    response = build_repair_response({"context_id": "ctx-1", "inputs": brief["inputs"]})

    assert response["incident_type"] == "output_state_drift"
    assert response["repair_mode"] == "repair"
    assert response["recommended_actions"][0]["action_type"] == "recover_outputs_and_finalize"


def test_repair_playbook_marks_stale_running_task_failed(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    out = tmp_path / "workflow_out"
    out.mkdir(parents=True, exist_ok=True)
    for name in ["results.json", "factor_scores.json", "factor_graveyard.json", "summary.md", "portfolio_results.json", "timing.json"]:
        (out / name).write_text("{}", encoding="utf-8")
    task_state = out / "task_state.json"
    task_state.write_text(json.dumps({"status": "running"}, ensure_ascii=False), encoding="utf-8")

    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": str(tmp_path / "cfg.json"), "output_dir": str(out)},
        priority=1,
        fingerprint="workflow::stale-repair",
        worker_note="validation｜stale",
    )
    store.conn.execute(
        "UPDATE research_tasks SET status='running', started_at_utc=? WHERE task_id=?",
        ("2026-01-01T00:00:00+00:00", task_id),
    )
    store.conn.commit()

    repaired = repair_stale_running_tasks(store, [task_id])
    row = store.get_research_task(task_id)
    state = json.loads(task_state.read_text(encoding="utf-8"))

    assert repaired == [task_id]
    assert row["status"] == "failed"
    assert row["last_error"] == "stale_running_task_repaired_after_outputs_written"
    assert state["status"] == "failed"


def test_build_repair_runtime_snapshot_detects_stale_running_candidate(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    out = tmp_path / "workflow_out"
    out.mkdir(parents=True, exist_ok=True)
    for name in ["results.json", "factor_scores.json", "factor_graveyard.json", "summary.md", "portfolio_results.json", "timing.json"]:
        (out / name).write_text("{}", encoding="utf-8")

    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": str(tmp_path / "cfg.json"), "output_dir": str(out)},
        priority=1,
        fingerprint="workflow::snapshot-stale",
        worker_note="validation｜snapshot",
    )
    store.conn.execute(
        "UPDATE research_tasks SET status='running', started_at_utc=? WHERE task_id=?",
        ("2026-01-01T00:00:00+00:00", task_id),
    )
    store.conn.commit()

    snapshot = build_repair_runtime_snapshot(store, tmp_path / "repair_runtime_snapshot.json", stale_minutes=10)

    assert snapshot["stale_running_candidates"]
    assert snapshot["stale_running_candidates"][0]["task_id"] == task_id



def test_observation_only_repairs_do_not_create_incident_log(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    monkeypatch.setattr(repair_runtime, "REPAIR_INCIDENTS_PATH", tmp_path / "repair_incidents.jsonl")
    monkeypatch.setattr(repair_runtime, "REPAIR_OBSERVATIONS_PATH", tmp_path / "runtime_observations.jsonl")
    monkeypatch.setattr(repair_runtime, "REPAIR_INCIDENT_STATE_PATH", tmp_path / "runtime_incident_state.json")
    monkeypatch.setattr(repair_runtime, "REPAIR_FEEDBACK_PATH", tmp_path / "repair_feedback.json")
    monkeypatch.setattr(repair_runtime, "REPAIR_METRICS_PATH", tmp_path / "repair_metrics.json")
    monkeypatch.setattr(repair_playbooks, "append_runtime_observation", repair_runtime.append_runtime_observation)
    monkeypatch.setattr(repair_playbooks, "append_repair_incident", repair_runtime.append_repair_incident)
    monkeypatch.setattr(repair_playbooks, "load_runtime_incident_state", repair_runtime.load_runtime_incident_state)
    monkeypatch.setattr(repair_playbooks, "write_runtime_incident_state", repair_runtime.write_runtime_incident_state)
    monkeypatch.setattr(repair_playbooks, "write_repair_summary_artifacts", repair_runtime.write_repair_summary_artifacts)

    response = {
        "incident_type": "unknown",
        "severity": "low",
        "repair_mode": "observe",
        "recommended_actions": [{"action_type": "mark_incident_only", "target": "none", "risk_level": "low"}],
        "suspected_root_causes": [],
        "summary_markdown": "observe",
    }

    execution = execute_repair_actions(response, store=store, auto_only=True)

    assert execution["status"] == "observed"
    assert not (tmp_path / "repair_incidents.jsonl").exists()
    assert (tmp_path / "runtime_observations.jsonl").exists()



def test_reseed_queue_failed_no_effect_when_no_tasks_seeded(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    monkeypatch.setattr(repair_runtime, "REPAIR_INCIDENTS_PATH", tmp_path / "repair_incidents.jsonl")
    monkeypatch.setattr(repair_runtime, "REPAIR_OBSERVATIONS_PATH", tmp_path / "runtime_observations.jsonl")
    monkeypatch.setattr(repair_runtime, "REPAIR_INCIDENT_STATE_PATH", tmp_path / "runtime_incident_state.json")
    monkeypatch.setattr(repair_runtime, "REPAIR_FEEDBACK_PATH", tmp_path / "repair_feedback.json")
    monkeypatch.setattr(repair_runtime, "REPAIR_METRICS_PATH", tmp_path / "repair_metrics.json")
    monkeypatch.setattr(repair_playbooks, "append_runtime_observation", repair_runtime.append_runtime_observation)
    monkeypatch.setattr(repair_playbooks, "append_repair_incident", repair_runtime.append_repair_incident)
    monkeypatch.setattr(repair_playbooks, "load_runtime_incident_state", repair_runtime.load_runtime_incident_state)
    monkeypatch.setattr(repair_playbooks, "write_runtime_incident_state", repair_runtime.write_runtime_incident_state)
    monkeypatch.setattr(repair_playbooks, "write_repair_summary_artifacts", repair_runtime.write_repair_summary_artifacts)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])

    response = {
        "incident_type": "queue_stall",
        "severity": "medium",
        "repair_mode": "repair",
        "recommended_actions": [{"action_type": "reseed_queue", "target": "baseline", "risk_level": "low"}],
        "suspected_root_causes": [{"cause": "queue empty"}],
        "summary_markdown": "queue stall",
    }

    execution = execute_repair_actions(response, store=store, auto_only=True)
    verification = verify_repair_actions(response, execution, store=store)

    assert execution["actions"][0]["status"] == "failed_no_effect"
    assert verification["overall_status"] == "repair_failed"



def test_write_repair_summary_artifacts_exposes_feedback(tmp_path, monkeypatch):
    monkeypatch.setattr(repair_runtime, "REPAIR_INCIDENTS_PATH", tmp_path / "repair_incidents.jsonl")
    monkeypatch.setattr(repair_runtime, "REPAIR_OBSERVATIONS_PATH", tmp_path / "runtime_observations.jsonl")
    monkeypatch.setattr(repair_runtime, "REPAIR_INCIDENT_STATE_PATH", tmp_path / "runtime_incident_state.json")
    monkeypatch.setattr(repair_runtime, "REPAIR_FEEDBACK_PATH", tmp_path / "repair_feedback.json")
    monkeypatch.setattr(repair_runtime, "REPAIR_METRICS_PATH", tmp_path / "repair_metrics.json")

    repair_runtime.write_runtime_incident_state(
        {
            "incidents": {
                "sig-1": {
                    "incident_signature": "sig-1",
                    "incident_type": "blocked_lane_deadlock",
                    "target": "generated_batch",
                    "status": "active",
                    "first_seen_at_utc": "2026-04-15T00:00:00+00:00",
                    "last_seen_at_utc": "2099-04-15T00:00:00+00:00"
                }
            }
        }
    )
    repair_runtime.append_repair_incident({"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "incident_type": "unknown", "actions": [{"action_type": "mark_incident_only", "status": "noop"}]})
    repair_runtime.append_repair_incident({"recorded_at_utc": datetime.now(timezone.utc).isoformat(), "incident_type": "blocked_lane_deadlock", "actions": []})
    payload = write_repair_summary_artifacts()

    assert payload["repair_feedback"]["active_incident_count"] == 1
    assert payload["repair_feedback"]["blocked_families"] == ["generated_batch"]
    assert payload["repair_metrics"]["incident_type_counts_24h"] == {"blocked_lane_deadlock": 1}



def test_run_orchestrator_writes_repair_artifacts(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    pending_id = store.enqueue_research_task(
        task_type="diagnostic",
        payload={"diagnostic_type": "opportunity_diagnose", "source_output_dir": str(tmp_path), "output_dir": str(tmp_path / "diag-out")},
        priority=1,
        worker_note="validation｜diag",
    )

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "run_research_planner_pipeline", lambda: {"injected_count": 0, "opportunity_execution": {"injected_count": 0}, "recovery_used": False, "research_flow_state": {"state": "unknown"}})
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: [])
    monkeypatch.setattr(research_queue, "execute_task", lambda task: "diagnostic finished: opportunity_diagnose")
    monkeypatch.setattr(research_queue, "update_research_memory_from_task_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "evaluate_opportunity_from_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)

    assert pending_id == result["processed"][0]["task_id"]
    assert (tmp_path / "repair_runtime_snapshot.json").exists()
    assert (tmp_path / "repair_agent_brief.json").exists()
    assert (tmp_path / "repair_agent_response.json").exists()
    assert (tmp_path / "repair_action_plan.json").exists()
    assert (tmp_path / "repair_verification.json").exists()
    assert "repair" in result
