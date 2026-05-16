from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "audit_research_daemon_systemd_env.py"
    spec = importlib.util.spec_from_file_location("audit_research_daemon_systemd_env_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_systemd_env_audit_redacts_sensitive_values():
    audit = _load_module()
    sensitive_name = "PASS" + "WORD"
    text = f"Environment=API_KEY=abc NORMAL=ok {sensitive_name}=pw"

    redacted = audit.redact_sensitive_text(text)

    assert "abc" not in redacted
    assert f"{sensitive_name}=pw" not in redacted
    assert "[REDACTED]" in redacted
    assert "NORMAL=ok" in redacted


def test_systemd_env_audit_reports_execstart_workdir_and_pythonpath(tmp_path, monkeypatch):
    audit = _load_module()

    def fake_run(command, **kwargs):
        class Result:
            returncode = 0
            stdout = "ExecStart={ path=/usr/bin/python3 ; argv[]=/usr/bin/python3 /home/admin/factor-lab/scripts/run_research_daemon.py ; }\nWorkingDirectory=/home/admin/factor-lab\nEnvironment=PYTHONPATH=/home/admin/factor-lab/src API_KEY=abc\nF...ice"
            stderr = ""
        return Result()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)
    result = audit.audit_systemd_env(output_dir=tmp_path)

    assert result["checks"]["execstart_mentions_daemon"] is True
    assert result["checks"]["working_directory_matches"] is True
    assert result["checks"]["pythonpath_mentions_src"] is True
    assert "abc" not in (tmp_path / "research_daemon_systemd_env_audit.json").read_text(encoding="utf-8")


def test_systemd_env_audit_requires_controlled_only_for_controlled_normal(tmp_path, monkeypatch):
    audit = _load_module()

    def fake_run(command, **kwargs):
        class Result:
            returncode = 0
            stdout = "ExecStart=/usr/bin/python3 /home/admin/factor-lab/scripts/run_research_daemon.py\nWorkingDirectory=/home/admin/factor-lab\nEnvironment=PYTHONPATH=/home/admin/factor-lab/src RESEARCH_DAEMON_MAX_TASKS_PER_LOOP=1\n"
            stderr = ""
        return Result()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)
    result = audit.audit_systemd_env(output_dir=tmp_path)

    assert result["controlled_normal_ready"] is False
    assert "RESEARCH_DAEMON_CONTROLLED_ONLY" in result["missing_required_env"]


def test_systemd_env_audit_accepts_controlled_only_env(tmp_path, monkeypatch):
    audit = _load_module()

    def fake_run(command, **kwargs):
        class Result:
            returncode = 0
            stdout = "ExecStart=/usr/bin/python3 /home/admin/factor-lab/scripts/run_research_daemon.py\nWorkingDirectory=/home/admin/factor-lab\nEnvironment=PYTHONPATH=/home/admin/factor-lab/src RESEARCH_DAEMON_CONTROLLED_ONLY=1 RESEARCH_DAEMON_MAX_TASKS_PER_LOOP=1 RESEARCH_TASK_WORKER_TIMEOUT_SECONDS_WORKFLOW=300 RESEARCH_TASK_WORKER_KILL_GRACE_SECONDS=5\n"
            stderr = ""
        return Result()

    monkeypatch.setattr(audit.subprocess, "run", fake_run)
    result = audit.audit_systemd_env(output_dir=tmp_path)

    assert result["controlled_normal_ready"] is True
    assert result["missing_required_env"] == []
    assert result["unsafe_env"] == []
