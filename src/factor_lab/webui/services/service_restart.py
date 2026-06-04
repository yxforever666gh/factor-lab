from __future__ import annotations

import subprocess
from typing import Any, Callable


def restart_research_daemon_after_settings_save(
    subprocess_run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> dict[str, Any]:
    """Restart the user-level research daemon after WebUI settings change.

    The optional runner keeps this helper testable while preserving compatibility
    with tests that monkeypatch ``factor_lab.webui_app.subprocess.run`` through
    the webui_app compatibility wrapper.
    """
    runner = subprocess_run or subprocess.run
    try:
        completed = runner(
            ["systemctl", "--user", "restart", "factor-lab-research-daemon.service"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return {
            "ok": completed.returncode == 0,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
        }
    except Exception as exc:
        return {"ok": False, "returncode": None, "stdout": "", "stderr": str(exc)}
