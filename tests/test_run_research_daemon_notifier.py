import importlib.util
from pathlib import Path
from unittest.mock import MagicMock


def _load_daemon_module():
    path = Path(__file__).resolve().parents[1] / 'scripts' / 'run_research_daemon.py'
    spec = importlib.util.spec_from_file_location('run_research_daemon_test', path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_emit_wake_event_via_hermes_native_disabled_when_env_zero(monkeypatch):
    """When RESEARCH_DAEMON_WAKE_EVENTS=0, notifier should return 'disabled' status."""
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_WAKE_EVENTS', '0')
    
    status = daemon._emit_wake_event_via_hermes_native('test message')
    
    assert status == 'disabled'


def test_emit_wake_event_via_hermes_native_unavailable_when_cli_missing(monkeypatch):
    """When hermes_native CLI is missing, notifier should return 'unavailable' status without crashing."""
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_WAKE_EVENTS', '1')
    # Mock shutil.which to return None (hermes_native not found)
    monkeypatch.setattr(daemon.shutil, 'which', lambda cmd: None)
    
    status = daemon._emit_wake_event_via_hermes_native('test message')
    
    assert status == 'unavailable'


def test_emit_wake_event_via_hermes_native_delivered_when_cli_succeeds(monkeypatch):
    """When hermes_native CLI exists and succeeds, notifier should return 'delivered' status."""
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_WAKE_EVENTS', '1')
    # Mock shutil.which to return a path (hermes_native found)
    monkeypatch.setattr(daemon.shutil, 'which', lambda cmd: '/usr/bin/hermes_native')
    
    # Mock subprocess.run to simulate successful execution
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ''
    mock_result.stderr = ''
    monkeypatch.setattr(daemon.subprocess, 'run', lambda *args, **kwargs: mock_result)
    
    status = daemon._emit_wake_event_via_hermes_native('test message')
    
    assert status == 'delivered'


def test_emit_wake_event_via_hermes_native_failed_when_cli_errors(monkeypatch):
    """When hermes_native CLI exists but fails, notifier should return 'failed' status."""
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_WAKE_EVENTS', '1')
    # Mock shutil.which to return a path (hermes_native found)
    monkeypatch.setattr(daemon.shutil, 'which', lambda cmd: '/usr/bin/hermes_native')
    
    # Mock subprocess.run to simulate failed execution
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = ''
    mock_result.stderr = 'error: command failed'
    monkeypatch.setattr(daemon.subprocess, 'run', lambda *args, **kwargs: mock_result)
    
    status = daemon._emit_wake_event_via_hermes_native('test message')
    
    assert status == 'failed'


def test_emit_wake_event_wrapper_maintains_backward_compatibility(monkeypatch):
    """The emit_wake_event() wrapper should maintain backward compatibility."""
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_WAKE_EVENTS', '0')
    
    # Should not raise, just return None (no-op)
    result = daemon.emit_wake_event('test message')
    
    assert result is None


def test_emit_wake_event_does_not_crash_when_hermes_native_missing(monkeypatch):
    """The daemon should not crash when hermes_native is missing, even with wake events enabled."""
    daemon = _load_daemon_module()
    monkeypatch.setenv('RESEARCH_DAEMON_WAKE_EVENTS', '1')
    monkeypatch.setattr(daemon.shutil, 'which', lambda cmd: None)
    
    # Should not raise
    result = daemon.emit_wake_event('test message')
    
    assert result is None
