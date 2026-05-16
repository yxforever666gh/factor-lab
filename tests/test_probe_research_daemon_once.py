from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "probe_research_daemon_once.py"
    spec = importlib.util.spec_from_file_location("probe_research_daemon_once_test", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_redact_sensitive_values():
    probe = _load_module()
    payload = {
        "NORMAL": "ok",
        "API_KEY": "secret",
        "nested": {"PASSWORD": "pw", "safe": "value"},
    }

    redacted = probe.redact_sensitive(payload)

    assert redacted["NORMAL"] == "ok"
    assert redacted["API_KEY"] == "[REDACTED]"
    assert redacted["nested"]["PASSWORD"] == "[REDACTED]"
    assert redacted["nested"]["safe"] == "value"


def test_probe_stops_and_resets_service_in_finally(tmp_path, monkeypatch):
    probe = _load_module()
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(list(command))
        if command[:3] == ["systemctl", "--user", "start"]:
            raise RuntimeError("boom after start")
        class Result:
            returncode = 0
            stdout = "ok"
            stderr = ""
        return Result()

    monkeypatch.setattr(probe.subprocess, "run", fake_run)
    monkeypatch.setattr(probe.time, "sleep", lambda seconds: None)

    result = probe.probe_daemon_once(seconds=0, output_dir=tmp_path)

    assert result["ok"] is False
    assert ["systemctl", "--user", "stop", probe.SERVICE_NAME] in calls
    assert ["systemctl", "--user", "reset-failed", probe.SERVICE_NAME] in calls
    written = json.loads((tmp_path / "daemon_probe_once.json").read_text(encoding="utf-8"))
    assert written["ok"] is False
