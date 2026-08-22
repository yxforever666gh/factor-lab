from __future__ import annotations

import subprocess
from typing import Any, Callable


def restart_research_daemon_after_settings_save(
    subprocess_run: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> dict[str, Any]:
    """Acknowledge settings without restarting the retired legacy daemon.

    Research OS workers load their explicit configuration at the next Dagster
    run.  Keeping this compatibility function avoids changing the form route,
    while deliberately guaranteeing that a settings POST cannot wake the old
    SQLite/JSON autonomous queue.
    """
    _ = subprocess_run
    return {
        "ok": True,
        "restarted": False,
        "reason": "legacy_research_daemon_retired",
        "applies": "next_research_os_run",
        "returncode": None,
        "stdout": "",
        "stderr": "",
    }
