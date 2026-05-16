from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_daemon():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_research_daemon.py"
    spec = importlib.util.spec_from_file_location("run_research_daemon_loop_once_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_run_daemon_loop_once_processes_task_and_updates_status(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "run_orchestrator", lambda max_tasks: {"processed": [{"status": "finished", "summary": "ok"}]})
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))
    monkeypatch.setattr(daemon, "emit_wake_event", lambda text: None)
    monkeypatch.setattr(daemon, "process_report_refresh_requests", lambda: None)

    result = daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(controlled_only=False),
        processed_tasks_total=0,
        loop_index=1,
    )

    assert result.processed_count == 1
    assert result.processed_tasks_total == 1
    assert result.state == "running"
    assert result.exit_reason is None
    assert statuses[-1][0] == "running"
    assert statuses[-1][1]["loop_index"] == 1


def test_run_daemon_loop_once_idle_exits_when_configured(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "run_orchestrator", lambda max_tasks: {"processed": [], "remaining_preview": []})
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))
    monkeypatch.setattr(daemon, "process_report_refresh_requests", lambda: None)
    monkeypatch.setattr(daemon, "maybe_run_prewarm", lambda: None)
    monkeypatch.setattr(daemon, "report_refresh_requested", lambda: False)

    result = daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(exit_when_idle=True, idle_sleep_seconds=2, controlled_only=False),
        processed_tasks_total=0,
        loop_index=2,
    )

    assert result.state == "idle"
    assert result.exit_reason == "idle_no_pending"
    assert result.sleep_seconds == 0
    assert statuses[-1][1]["exit_reason"] == "idle_no_pending"


def test_run_daemon_loop_once_pending_but_unclaimable_exits_when_configured(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "run_orchestrator", lambda max_tasks: {
        "processed": [],
        "remaining_preview": [{"status": "pending", "task_type": "workflow"}],
        "blocked_task_types": ["workflow"],
    })
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))

    result = daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(exit_when_no_claimable=True, controlled_only=False),
        processed_tasks_total=0,
        loop_index=3,
    )

    assert result.state == "running"
    assert result.processed_count == 0
    assert result.exit_reason == "pending_not_claimable"
    assert statuses[-1][1]["pending_after_count"] == 1


def test_run_daemon_loop_once_controlled_only_refuses_when_no_admitted_workflow(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    called = {"orchestrator": False}
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 0, "pending_count": 2})
    monkeypatch.setattr(daemon, "run_orchestrator", lambda max_tasks: called.__setitem__("orchestrator", True))
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))

    result = daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(controlled_only=True, exit_when_no_claimable=True),
        processed_tasks_total=0,
        loop_index=4,
    )

    assert result.exit_reason == "no_admitted_workflow"
    assert called["orchestrator"] is False
    assert statuses[-1][1]["controlled_restart"]["would_run_count"] == 0


def test_run_daemon_loop_once_controlled_only_caps_max_tasks_to_would_run(monkeypatch):
    daemon = _load_daemon()
    seen = {}
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 4, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 1, "pending_count": 3})
    def fake_run_orchestrator(max_tasks, **kwargs):
        seen["max_tasks"] = max_tasks
        return {"processed": []}
    monkeypatch.setattr(daemon, "run_orchestrator", fake_run_orchestrator)
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: None)
    monkeypatch.setattr(daemon, "process_report_refresh_requests", lambda: None)
    monkeypatch.setattr(daemon, "maybe_run_prewarm", lambda: None)
    monkeypatch.setattr(daemon, "report_refresh_requested", lambda: False)

    daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(controlled_only=True),
        processed_tasks_total=0,
        loop_index=5,
    )

    assert seen["max_tasks"] == 1


def test_run_daemon_loop_once_controlled_only_skips_scheduler_route_probe(monkeypatch):
    daemon = _load_daemon()
    seen = {}
    throttle_calls = []

    def fake_compute_dynamic_throttle(**kwargs):
        throttle_calls.append(kwargs)
        return {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100}

    monkeypatch.setattr(daemon, "compute_dynamic_throttle", fake_compute_dynamic_throttle)
    monkeypatch.setattr(daemon, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 1, "pending_count": 3})
    def fake_run_orchestrator(max_tasks, **kwargs):
        seen["max_tasks"] = max_tasks
        return {"processed": []}

    monkeypatch.setattr(daemon, "run_orchestrator", fake_run_orchestrator)
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: None)
    monkeypatch.setattr(daemon, "process_report_refresh_requests", lambda: None)
    monkeypatch.setattr(daemon, "maybe_run_prewarm", lambda: None)
    monkeypatch.setattr(daemon, "report_refresh_requested", lambda: False)

    daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(controlled_only=True),
        processed_tasks_total=0,
        loop_index=6,
    )

    assert throttle_calls[0]["skip_route_probe"] is True


def test_run_daemon_loop_once_controlled_only_passes_skip_preclaim_refill_to_orchestrator(monkeypatch):
    daemon = _load_daemon()
    seen = {}
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "dry_run_controlled_restart", lambda db_path: {"would_run_count": 1, "pending_count": 3})

    def fake_run_orchestrator(max_tasks, **kwargs):
        seen["max_tasks"] = max_tasks
        seen.update(kwargs)
        return {"processed": []}

    monkeypatch.setattr(daemon, "run_orchestrator", fake_run_orchestrator)
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: None)
    monkeypatch.setattr(daemon, "process_report_refresh_requests", lambda: None)
    monkeypatch.setattr(daemon, "maybe_run_prewarm", lambda: None)
    monkeypatch.setattr(daemon, "report_refresh_requested", lambda: False)

    daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(controlled_only=True),
        processed_tasks_total=0,
        loop_index=7,
    )

    assert seen["skip_preclaim_refill"] is True


def test_handle_stop_marks_shutdown_requested_and_writes_status(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))
    monkeypatch.setenv("RESEARCH_DAEMON_EXIT_IMMEDIATELY_ON_STOP", "0")

    daemon.RUNNING = True
    daemon.SHUTDOWN_REQUESTED = False

    daemon.handle_stop(daemon.signal.SIGTERM, None)

    assert daemon.RUNNING is False
    assert daemon.SHUTDOWN_REQUESTED is True
    assert statuses[-1][0] == "stopping"
    assert statuses[-1][1]["signal"] == daemon.signal.SIGTERM


def test_handle_stop_can_exit_immediately_after_status(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))
    monkeypatch.setenv("RESEARCH_DAEMON_EXIT_IMMEDIATELY_ON_STOP", "1")

    daemon.RUNNING = True
    daemon.SHUTDOWN_REQUESTED = False

    try:
        daemon.handle_stop(daemon.signal.SIGTERM, None)
        assert False, "expected SystemExit"
    except SystemExit as exc:
        assert exc.code == 0

    assert daemon.RUNNING is False
    assert daemon.SHUTDOWN_REQUESTED is True
    assert statuses[-1][0] == "stopping"


def test_handle_stop_marks_running_tasks_interrupted_before_immediate_exit(monkeypatch):
    daemon = _load_daemon()
    calls = []
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: None)
    monkeypatch.setattr(daemon, "mark_running_tasks_interrupted", lambda reason: calls.append(reason))
    monkeypatch.setenv("RESEARCH_DAEMON_EXIT_IMMEDIATELY_ON_STOP", "1")

    try:
        daemon.handle_stop(daemon.signal.SIGTERM, None)
        assert False, "expected SystemExit"
    except SystemExit:
        pass

    assert calls == ["daemon_shutdown_requested"]


def test_run_daemon_loop_once_does_not_claim_after_shutdown_requested(monkeypatch):
    daemon = _load_daemon()
    statuses = []
    called = {"orchestrator": False}

    daemon.SHUTDOWN_REQUESTED = True
    monkeypatch.setattr(daemon, "compute_dynamic_throttle", lambda **kw: {"dynamic_max_tasks": 1, "dynamic_batch_workers": 1, "rss_mb": 100})
    monkeypatch.setattr(daemon, "run_orchestrator", lambda max_tasks: called.__setitem__("orchestrator", True))
    monkeypatch.setattr(daemon, "write_status", lambda state, **extra: statuses.append((state, extra)))

    result = daemon.run_daemon_loop_once(
        config=daemon.DaemonRuntimeConfig(controlled_only=False),
        processed_tasks_total=0,
        loop_index=9,
    )

    assert called["orchestrator"] is False
    assert result.exit_reason == "shutdown_requested"
    assert statuses[-1][0] == "stopping"
