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


def test_should_recycle_for_task_budget():
    daemon = _load_daemon_module()
    assert daemon.should_recycle_daemon(processed_tasks_total=5, max_tasks_before_restart=5, rss_limit_mb=0, rss_mb=10) == 'task_budget_reached'


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
