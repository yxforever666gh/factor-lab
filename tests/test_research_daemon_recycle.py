import importlib.util
from pathlib import Path

from factor_lab import batch


def _load_daemon_module():
    path = Path(__file__).resolve().parents[1] / 'scripts' / 'run_research_daemon.py'
    spec = importlib.util.spec_from_file_location('run_research_daemon_test', path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_read_rss_mb_from_status_file(tmp_path):
    daemon = _load_daemon_module()
    status = tmp_path / 'status'
    status.write_text('Name:\tpython\nVmRSS:\t  123456 kB\n', encoding='utf-8')
    assert daemon.read_rss_mb(status) == 120


def test_should_recycle_for_task_budget_when_immediate(monkeypatch):
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_RECYCLE_MODE', 'immediate')
    assert daemon.should_recycle_daemon(processed_tasks_total=5, max_tasks_before_restart=5, rss_limit_mb=0, rss_mb=10) == 'task_budget_reached'


def test_should_recycle_for_task_budget_only_when_idle_by_default(monkeypatch):
    daemon = _load_daemon_module()
    monkeypatch.delenv('RESEARCH_DAEMON_RECYCLE_MODE', raising=False)
    assert daemon.should_recycle_daemon(processed_tasks_total=5, max_tasks_before_restart=5, rss_limit_mb=0, rss_mb=10, idle=False) is None
    assert daemon.should_recycle_daemon(processed_tasks_total=5, max_tasks_before_restart=5, rss_limit_mb=0, rss_mb=10, idle=True) == 'task_budget_reached'


def test_should_recycle_for_rss_limit():
    daemon = _load_daemon_module()
    assert daemon.should_recycle_daemon(processed_tasks_total=1, max_tasks_before_restart=100, rss_limit_mb=256, rss_mb=300) == 'rss_limit_exceeded'


def test_batch_worker_count_env(monkeypatch):
    monkeypatch.setenv('FACTOR_LAB_BATCH_MAX_WORKERS', '1')
    assert batch.batch_max_workers() == 1


def test_batch_worker_count_clamps(monkeypatch):
    monkeypatch.setenv('FACTOR_LAB_BATCH_MAX_WORKERS', '99')
    assert batch.batch_max_workers() == 4


def test_merge_status_fields_prefers_later_values():
    daemon = _load_daemon_module()
    merged = daemon.merge_status_fields({"rss_mb": 100, "mode": "a"}, {"rss_mb": 200, "other": True})
    assert merged["rss_mb"] == 200
    assert merged["mode"] == "a"
    assert merged["other"] is True


def test_orchestrator_status_context_exposes_blocked_lane_summary():
    daemon = _load_daemon_module()
    context = daemon.orchestrator_status_context(
        {
            "blocked_task_types": ["generated_batch"],
            "blocked_lane_status": {
                "summary": "generated_batch blocked｜原因=缺少 base factor｜连续失败=3｜pending=1",
                "blocked_pending_count": 1,
                "unblocked_pending_count": 0,
                "lanes": [{"task_type": "generated_batch"}],
            },
        }
    )
    assert context["blocked_task_types"] == ["generated_batch"]
    assert context["blocked_lane_status"]["lanes"][0]["task_type"] == "generated_batch"
    assert context["blocked_lane_summary"].startswith("generated_batch blocked")
    assert context["blocked_pending_count"] == 1
    assert context["unblocked_pending_count"] == 0


def test_compute_dynamic_throttle_pushes_harder_when_cpu_headroom(monkeypatch):
    daemon = _load_daemon_module()
    monkeypatch.setattr(
        daemon,
        "read_system_load",
        lambda: {"load1": 1.2, "load5": 1.0, "load15": 0.9, "cpu_usage_ratio": 0.15},
    )
    monkeypatch.setattr(
        daemon,
        "read_meminfo_mb",
        lambda: {"mem_total_mb": 16000, "mem_available_mb": 12000, "swap_total_mb": 0, "swap_free_mb": 0},
    )
    monkeypatch.setattr(daemon, "read_rss_mb", lambda status_path=Path("/proc/self/status"): 256)
    monkeypatch.setattr(daemon, "user_idle_snapshot", lambda: {"mode": "background_idle", "idle_seconds": 900})
    monkeypatch.setattr(daemon, "route_status_snapshot", lambda: {"healthy": True, "resolved_mode": "direct"})

    payload = daemon.compute_dynamic_throttle(base_max_tasks=3, rss_limit_mb=2048)

    assert payload["mode"] == "background_idle"
    assert payload["dynamic_max_tasks"] >= 4
    assert payload["dynamic_batch_workers"] >= 2


def test_daemon_uses_configurable_artifacts_dir_for_status_files(tmp_path, monkeypatch):
    custom_artifacts = tmp_path / 'custom-artifacts'
    monkeypatch.setenv('FACTOR_LAB_ARTIFACTS_DIR', str(custom_artifacts))
    daemon = _load_daemon_module()

    daemon.write_status('running', marker='ok')

    status_path = custom_artifacts / 'research_daemon_status.json'
    history_path = custom_artifacts / 'research_daemon_status_history.jsonl'
    lock_path = custom_artifacts / 'research_daemon.lock'

    assert status_path.exists()
    assert history_path.exists()
    payload = daemon.json.loads(status_path.read_text(encoding='utf-8'))
    assert payload['state'] == 'running'
    assert payload['marker'] == 'ok'
    assert daemon._status_path() == status_path
    assert daemon._status_history_path() == history_path
    assert daemon._lock_path() == lock_path
