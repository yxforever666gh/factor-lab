from __future__ import annotations

import subprocess

from factor_lab import research_queue


def test_heavy_worker_ignores_warning_stderr_when_stdout_json_is_ok(monkeypatch):
    task = {"task_id": "t", "task_type": "workflow", "payload": {}}

    class FakeProc:
        pid = 123
        returncode = 0
        def poll(self):
            return 0
        def communicate(self):
            return ('{"ok": true, "summary": "workflow finished"}\n', "RequestsDependencyWarning: noisy warning\n")

    monkeypatch.setattr(subprocess, "Popen", lambda *args, **kwargs: FakeProc())

    assert research_queue._execute_heavy_task_in_subprocess(task) == "workflow finished"
