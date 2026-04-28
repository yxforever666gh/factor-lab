import json
from pathlib import Path

from factor_lab import research_queue
from factor_lab.storage import ExperimentStore


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_validate_generated_batch_payload_rejects_missing_lineage(tmp_path):
    cfg = _write_json(
        tmp_path / "bad_generated_config.json",
        {
            "factors": [
                {"name": "mom_20", "expression": "close"},
                {"name": "value_ep", "expression": "open"},
                {
                    "name": "bad_factor",
                    "expression": "residualize(close, open)",
                    "generator_operator": "residualize_against_peer",
                    "left_factor_name": None,
                    "right_factor_name": "value_ep",
                },
            ]
        },
    )
    batch = _write_json(
        tmp_path / "generated_batch.json",
        {"jobs": [{"name": "recent_45d", "config_path": str(cfg)}]},
    )
    task = {"task_type": "generated_batch", "payload": {"batch_path": str(batch), "output_dir": str(tmp_path / "out")}}

    ok, error = research_queue.validate_generated_batch_payload(task)

    assert ok is False
    assert error is not None
    assert "generated config missing lineage fields" in error


def test_validate_generated_batch_payload_rejects_invalid_date_range(tmp_path):
    cfg = _write_json(
        tmp_path / "bad_dates_generated_config.json",
        {
            "start_date": "2026-03-18",
            "end_date": "2026-02-01",
            "factors": [
                {"name": "mom_20", "expression": "close"},
            ],
        },
    )
    batch = _write_json(
        tmp_path / "generated_batch_bad_dates.json",
        {"jobs": [{"name": "recent_45d", "config_path": str(cfg)}]},
    )
    task = {"task_type": "generated_batch", "payload": {"batch_path": str(batch), "output_dir": str(tmp_path / "out")}}

    ok, error = research_queue.validate_generated_batch_payload(task)

    assert ok is False
    assert error is not None
    assert "generated config invalid date range" in error


def test_classify_task_failure_marks_missing_base_factor_as_deterministic():
    task = {"task_type": "generated_batch", "payload": {}}
    kind = research_queue.classify_task_failure(task, "missing base factor for generated operator: None, None")
    assert kind == "deterministic"


def test_resource_exhaustion_reason_detects_rss_limit():
    assert (
        research_queue._resource_exhaustion_reason(
            "research task worker rss exceeded limit: 2050MB >= 2048MB"
        )
        == "worker_rss_exceeded"
    )


def test_execute_task_uses_subprocess_for_workflow(monkeypatch):
    calls = {}

    class DummyProc:
        def __init__(self):
            self.pid = 123
            self.returncode = 0

        def poll(self):
            return 0

        def communicate(self):
            return json.dumps({"ok": True, "summary": "workflow finished: configs/x.json"}, ensure_ascii=False), ""

    def fake_popen(command, stdout=None, stderr=None, text=None):
        calls['command'] = command
        return DummyProc()

    monkeypatch.setattr(research_queue.subprocess, 'Popen', fake_popen)
    summary = research_queue.execute_task({"task_type": "workflow", "payload": {"config_path": "configs/x.json", "output_dir": "artifacts/x"}})

    assert "run_research_task_worker.py" in calls['command'][1]
    assert summary == "workflow finished: configs/x.json"


def test_execute_task_kills_worker_on_rss_limit(monkeypatch):
    class DummyProc:
        def __init__(self):
            self.pid = 321
            self.returncode = None
            self.killed = False

        def poll(self):
            return None if not self.killed else -9

        def kill(self):
            self.killed = True
            self.returncode = -9

        def communicate(self):
            return "", ""

    proc = DummyProc()
    monkeypatch.setenv('RESEARCH_TASK_WORKER_RSS_LIMIT_MB', '100')
    monkeypatch.setenv('RESEARCH_TASK_WORKER_TIMEOUT_SECONDS', '999')
    monkeypatch.setattr(research_queue.subprocess, 'Popen', lambda *args, **kwargs: proc)
    monkeypatch.setattr(research_queue, '_read_pid_rss_mb', lambda pid: 200)
    monkeypatch.setattr(research_queue.time, 'sleep', lambda _: None)

    try:
        research_queue.execute_task({"task_type": "workflow", "payload": {"config_path": "configs/x.json", "output_dir": "artifacts/x"}})
        assert False, 'expected RuntimeError'
    except RuntimeError as exc:
        assert 'rss exceeded limit' in str(exc)


def test_run_orchestrator_recovers_completed_generated_batch_outputs(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    out = tmp_path / "batch_out"
    out.mkdir(parents=True, exist_ok=True)
    (out / 'batch_summary.json').write_text('[]', encoding='utf-8')
    (out / 'batch_comparison.json').write_text('{}', encoding='utf-8')

    task_id = store.enqueue_research_task(
        task_type="generated_batch",
        payload={"batch_path": str(tmp_path / 'x.json'), "output_dir": str(out)},
        priority=1,
        worker_note="exploration｜generated",
    )

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(research_queue, "run_research_planner_pipeline", lambda: {"injected_count": 0, "opportunity_execution": {"injected_count": 0}, "recovery_used": False, "research_flow_state": {"state": "unknown"}})
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "recent_failure_stats", lambda store, limit=20, task_type=None: {"consecutive_failures": 0, "cooldown_active": False})
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: [])
    monkeypatch.setattr(research_queue, "execute_task", lambda task: (_ for _ in ()).throw(RuntimeError('research task worker failed with code -15')))
    monkeypatch.setattr(research_queue, "update_research_memory_from_task_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "evaluate_opportunity_from_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)

    assert result["processed"][0]["status"] == "finished_recovered"
    assert store.get_research_task(task_id)["status"] == "finished"


def test_cleanup_stale_running_tasks_reaches_old_running_rows_outside_recent_window(tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    stale_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": str(tmp_path / "stale.json"), "output_dir": str(tmp_path / "stale-out")},
        priority=1,
        fingerprint="workflow::stale",
        worker_note="validation｜stale",
    )
    stale_started = "2026-01-01T00:00:00+00:00"
    store.conn.execute(
        "UPDATE research_tasks SET status='running', started_at_utc=? WHERE task_id=?",
        (stale_started, stale_id),
    )
    store.conn.commit()

    for idx in range(350):
        finished_id = store.enqueue_research_task(
            task_type="diagnostic",
            payload={"opportunity_id": f"opp-{idx}"},
            priority=10,
            worker_note=f"diagnostic｜fresh-{idx}",
        )
        store.finish_research_task(finished_id, status="finished", worker_note=f"diagnostic finished: {idx}")

    recent_ids = {task["task_id"] for task in store.list_research_tasks(limit=300)}
    assert stale_id not in recent_ids

    cleaned = research_queue._cleanup_stale_running_tasks(store, stale_minutes=10)
    repaired = store.get_research_task(stale_id)

    assert cleaned == [stale_id]
    assert repaired["status"] == "failed"
    assert repaired["last_error"] == "stale_running_task_cleaned"
    assert "auto_cleaned_stale_running" in (repaired["worker_note"] or "")


def test_run_orchestrator_auto_quarantines_budget_risky_blocked_pending(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    for idx in range(3):
        failed_id = store.enqueue_research_task(
            task_type="generated_batch",
            payload={"batch_path": str(tmp_path / f"bad-{idx}.json"), "output_dir": str(tmp_path / f"out-{idx}"), "execution_mode": "cheap_screen", "target_candidates": [], "opportunity_type": "probe", "opportunity_id": f"opp-{idx}"},
            priority=5,
            worker_note="retry｜auto",
        )
        store.finish_research_task(failed_id, status="failed", last_error="research task worker rss exceeded limit: 2050MB >= 2048MB")

    batch_path = tmp_path / 'pending.json'
    batch_path.write_text('{}', encoding='utf-8')
    out_dir = tmp_path / 'pending-out'
    out_dir.mkdir()
    pending_id = store.enqueue_research_task(
        task_type="generated_batch",
        payload={"batch_path": str(batch_path), "output_dir": str(out_dir), "execution_mode": "cheap_screen", "target_candidates": [], "opportunity_type": "probe", "opportunity_id": "opp-pending"},
        priority=1,
        worker_note="exploration｜generated",
    )

    planner_called = {"value": False}

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(research_queue, "run_research_planner_pipeline", lambda: planner_called.__setitem__('value', True) or {"injected_count": 0, "opportunity_execution": {"injected_count": 0}, "recovery_used": False, "research_flow_state": {"state": "unknown"}})
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: [])
    monkeypatch.setattr(research_queue, "update_research_memory_from_task_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)

    assert planner_called["value"] is True
    assert store.get_research_task(pending_id)["status"] == "finished"
    assert result["processed"] == []


def test_run_orchestrator_auto_quarantines_repeated_resource_exhausted_family(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    batch_path = tmp_path / "repeated.json"
    batch_path.write_text("{}", encoding="utf-8")
    out_dir = tmp_path / "repeated-out"
    out_dir.mkdir()

    failed_ids = []
    for _ in range(3):
        failed_id = store.enqueue_research_task(
            task_type="generated_batch",
            payload={"batch_path": str(batch_path), "output_dir": str(out_dir), "opportunity_id": "opp-repeat"},
            priority=5,
            worker_note="retry｜auto",
        )
        store.finish_research_task(
            failed_id,
            status="failed",
            last_error="research task worker rss exceeded limit: 2050MB >= 2048MB",
        )
        failed_ids.append(failed_id)

    pending_id = store.enqueue_research_task(
        task_type="generated_batch",
        payload={"batch_path": str(batch_path), "output_dir": str(out_dir), "opportunity_id": "opp-repeat"},
        priority=1,
        worker_note="exploration｜generated",
    )

    planner_called = {"value": False}

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        research_queue,
        "run_research_planner_pipeline",
        lambda: planner_called.__setitem__("value", True)
        or {"injected_count": 0, "opportunity_execution": {"injected_count": 0}, "recovery_used": False, "research_flow_state": {"state": "unknown"}},
    )
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: [])
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)

    assert planner_called["value"] is True
    assert result["processed"] == []
    assert not batch_path.exists()
    assert not out_dir.exists()
    assert store.get_research_task(pending_id)["status"] == "finished"
    for failed_id in failed_ids:
        assert store.get_research_task(failed_id)["status"] == "finished"


def test_run_orchestrator_skips_blocked_generated_batch_family(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    for idx in range(3):
        failed_id = store.enqueue_research_task(
            task_type="generated_batch",
            payload={"batch_path": f"bad-{idx}.json", "output_dir": f"out-{idx}"},
            priority=5,
            worker_note="retry｜auto",
        )
        store.finish_research_task(failed_id, status="failed", last_error="missing base factor for generated operator: None, None")

    blocked_pending_id = store.enqueue_research_task(
        task_type="generated_batch",
        payload={"batch_path": "still-bad.json", "output_dir": "out-pending"},
        priority=1,
        worker_note="exploration｜generated",
    )
    diagnostic_id = store.enqueue_research_task(
        task_type="diagnostic",
        payload={"diagnostic_type": "smoke"},
        priority=10,
        worker_note="validation｜diagnostic",
    )

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(research_queue, "execute_task", lambda task: f"ran {task['task_type']}")
    monkeypatch.setattr(research_queue, "enqueue_followup_tasks", lambda store, task: [])
    monkeypatch.setattr(research_queue, "update_research_memory_from_task_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "evaluate_opportunity_from_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)

    assert result["blocked_task_types"] == ["generated_batch"]
    assert result["blocked_lane_status"]["active"] is True
    assert result["blocked_lane_status"]["only_blocked_pending"] is True
    lane = result["blocked_lane_status"]["lanes"][0]
    assert lane["task_type"] == "generated_batch"
    assert lane["root_cause"] == "generated_config_missing_base_factor"
    assert lane["pending_count"] == 1
    assert "missing base factor" in lane["latest_error"]
    assert result["processed"][0]["task_id"] == diagnostic_id
    assert store.get_research_task(diagnostic_id)["status"] == "finished"
    assert store.get_research_task(blocked_pending_id)["status"] == "pending"


def test_run_orchestrator_guardrail_reports_only_blocked_pending_lane(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    for idx in range(3):
        failed_id = store.enqueue_research_task(
            task_type="generated_batch",
            payload={"batch_path": f"bad-{idx}.json", "output_dir": f"out-{idx}"},
            priority=5,
            worker_note="retry｜auto",
        )
        store.finish_research_task(failed_id, status="failed", last_error="missing base factor for generated operator: None, None")

    blocked_pending_id = store.enqueue_research_task(
        task_type="generated_batch",
        payload={"batch_path": "still-bad.json", "output_dir": "out-pending"},
        priority=1,
        worker_note="exploration｜generated",
    )

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])

    result = research_queue.run_orchestrator(max_tasks=1)

    assert result["guardrail"] == "circuit_open"
    assert result["blocked_task_types"] == ["generated_batch"]
    assert result["blocked_lane_status"]["active"] is True
    assert result["blocked_lane_status"]["only_blocked_pending"] is True
    assert result["blocked_lane_status"]["blocked_pending_count"] == 1
    assert result["blocked_lane_status"]["unblocked_pending_count"] == 0
    assert "generated_batch blocked" in result["blocked_lane_status"]["summary"]
    assert store.get_research_task(blocked_pending_id)["status"] == "pending"


def test_run_orchestrator_refills_under_target_backlog_before_queue_empty(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    original_id = store.enqueue_research_task(
        task_type="diagnostic",
        payload={"diagnostic_type": "seed", "source_output_dir": str(tmp_path)},
        priority=1,
        worker_note="validation｜seed",
    )

    planner_calls = {"count": 0}

    def fake_planner():
        planner_calls["count"] += 1
        store.enqueue_research_task(
            task_type="diagnostic",
            payload={"diagnostic_type": "topup", "source_output_dir": str(tmp_path)},
            priority=5,
            worker_note="validation｜topup",
        )
        return {
            "injected_count": 1,
            "opportunity_execution": {"injected_count": 0},
            "recovery_used": False,
            "research_flow_state": {"state": "healthy"},
        }

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "REFILL_STATE_PATH", tmp_path / "refill.json")
    monkeypatch.setenv("RESEARCH_QUEUE_TARGET_VALIDATION_BACKLOG", "2")
    monkeypatch.setenv("RESEARCH_QUEUE_TARGET_EXPLORATION_BACKLOG", "1")
    monkeypatch.setenv("RESEARCH_QUEUE_REFILL_COOLDOWN_SECONDS", "0")
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(research_queue, "run_research_planner_pipeline", fake_planner)
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: [])
    monkeypatch.setattr(research_queue, "execute_task", lambda task: f"done:{task['task_id']}")
    monkeypatch.setattr(research_queue, "update_research_memory_from_task_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "evaluate_opportunity_from_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)
    pending = [task for task in store.list_research_tasks(limit=10) if task["status"] == "pending"]

    assert planner_calls["count"] == 1
    assert result["processed"][0]["task_id"] == original_id
    assert len(pending) == 1
    assert pending[0]["worker_note"] == "validation｜topup"


def test_run_orchestrator_quarantines_generated_batch_resource_exhaustion_without_retry(monkeypatch, tmp_path):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)

    batch_path = tmp_path / "single.json"
    batch_path.write_text("{}", encoding="utf-8")
    out_dir = tmp_path / "single-out"
    out_dir.mkdir()

    task_id = store.enqueue_research_task(
        task_type="generated_batch",
        payload={"batch_path": str(batch_path), "output_dir": str(out_dir), "opportunity_id": "opp-single"},
        priority=1,
        worker_note="exploration｜generated",
    )

    monkeypatch.setattr(research_queue, "DB_PATH", db_path)
    monkeypatch.setattr(research_queue, "append_heartbeat", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "_cleanup_stale_running_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(research_queue, "run_research_planner_pipeline", lambda: {"injected_count": 0, "opportunity_execution": {"injected_count": 0}, "recovery_used": False, "research_flow_state": {"state": "unknown"}})
    monkeypatch.setattr(research_queue, "can_reseed_baseline", lambda store: False)
    monkeypatch.setattr(research_queue, "enqueue_baseline_tasks", lambda store: [])
    monkeypatch.setattr(research_queue, "maybe_expand_research_space", lambda store, max_new_tasks=4, allow_repeat=True: [])
    monkeypatch.setattr(
        research_queue,
        "execute_task",
        lambda task: (_ for _ in ()).throw(RuntimeError("research task worker rss exceeded limit: 2050MB >= 2048MB")),
    )
    monkeypatch.setattr(research_queue, "update_research_memory_from_task_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "evaluate_opportunity_from_task", lambda *args, **kwargs: None)
    monkeypatch.setattr(research_queue, "update_opportunity_state", lambda *args, **kwargs: None)

    result = research_queue.run_orchestrator(max_tasks=1)
    tasks = store.list_research_tasks(limit=10)

    assert result["processed"][0]["status"] == "quarantined"
    assert result["processed"][0]["retry_task_id"] is None
    assert store.get_research_task(task_id)["status"] == "finished"
    assert len([task for task in tasks if task["status"] == "pending"]) == 0
    assert not batch_path.exists()
    assert not out_dir.exists()
