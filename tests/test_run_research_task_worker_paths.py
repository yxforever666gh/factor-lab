import importlib.util
import json
import sys
from pathlib import Path


def _load_worker_module():
    path = Path(__file__).resolve().parents[1] / 'scripts' / 'run_research_task_worker.py'
    spec = importlib.util.spec_from_file_location('run_research_task_worker_test', path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_schedule_report_refresh_uses_configurable_project_root(tmp_path, monkeypatch):
    custom_root = tmp_path / 'custom-root'
    custom_artifacts = tmp_path / 'custom-artifacts'
    monkeypatch.setenv('FACTOR_LAB_ROOT', str(custom_root))
    monkeypatch.setenv('FACTOR_LAB_ARTIFACTS_DIR', str(custom_artifacts))
    monkeypatch.setenv('RESEARCH_REPORT_REFRESH_MODE', 'sync')
    worker = _load_worker_module()

    captured = {}

    def fake_run(command, cwd, capture_output, text, timeout, check):
        captured['command'] = command
        captured['cwd'] = cwd
        return type('Completed', (), {'returncode': 0, 'stdout': '1\n', 'stderr': ''})()

    monkeypatch.setattr(worker.subprocess, 'run', fake_run)

    refreshed, error = worker.schedule_report_refresh(source='workflow')

    assert refreshed is True
    assert error is None
    assert captured['cwd'] == custom_root
    assert f"sys.path.insert(0, {str(custom_root / 'src')!r}); " in captured['command'][2]


def test_generated_batch_uses_configurable_artifacts_dir(tmp_path, monkeypatch):
    custom_artifacts = tmp_path / 'custom-artifacts'
    monkeypatch.setenv('FACTOR_LAB_ARTIFACTS_DIR', str(custom_artifacts))
    worker = _load_worker_module()

    captured = {}
    monkeypatch.setattr(worker, 'run_batch', lambda *args, **kwargs: None)
    monkeypatch.setattr(worker, 'schedule_report_refresh', lambda source: (False, 'reports_refresh=deferred'))

    def fake_summarize(output_dir, feedback_path):
        captured['feedback_path'] = feedback_path
        return {'batch_summary': []}

    def fake_write_bridge_status(path, payload):
        captured['status_path'] = path
        captured['payload'] = payload

    monkeypatch.setattr(worker, 'summarize_generated_batch_run', fake_summarize)
    monkeypatch.setattr(worker, 'write_bridge_status', fake_write_bridge_status)
    calls = []
    monkeypatch.setattr(worker, '_run_data_quality_hook', lambda **kwargs: calls.append(kwargs))

    task = {
        'task_type': 'generated_batch',
        'payload': {
            'batch_path': str(tmp_path / 'batch.json'),
            'output_dir': str(tmp_path / 'output'),
        },
    }

    monkeypatch.setattr(worker, 'validate_generated_batch_payload', lambda task: (True, None))
    monkeypatch.setattr(sys, 'argv', ['run_research_task_worker.py', json.dumps(task, ensure_ascii=False)])

    assert worker.main() == 0
    assert captured['feedback_path'] == str(custom_artifacts / 'llm_plan_feedback.json')
    assert captured['status_path'] == str(custom_artifacts / 'llm_status.json')
    assert captured['payload']['feedback_path'] == str(custom_artifacts / 'llm_plan_feedback.json')
    assert calls and calls[0]['task_type'] == 'generated_batch'


def test_batch_runs_data_quality_hook_on_failure(tmp_path, monkeypatch):
    worker = _load_worker_module()
    calls = []

    def fake_run_batch(*args, **kwargs):
        raise RuntimeError('batch exploded')

    monkeypatch.setattr(worker, 'run_batch', fake_run_batch)
    monkeypatch.setattr(worker, '_run_data_quality_hook', lambda **kwargs: calls.append(kwargs))
    task = {
        'task_id': 'task-1',
        'task_type': 'batch',
        'payload': {'config_path': str(tmp_path / 'batch.json'), 'output_dir': str(tmp_path / 'output')},
    }
    monkeypatch.setattr(sys, 'argv', ['run_research_task_worker.py', json.dumps(task, ensure_ascii=False)])

    try:
        worker.main()
    except RuntimeError:
        pass
    else:
        raise AssertionError('batch failure should be re-raised')

    assert calls
    assert calls[0]['task_type'] == 'batch'
    assert calls[0]['last_error'] == 'batch exploded'
