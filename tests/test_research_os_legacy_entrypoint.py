from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_legacy_autonomous_daemon_cannot_create_new_candidates() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "run_research_daemon.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "retired_legacy_entrypoint"
    assert payload["reason"] == "research_os_is_authoritative"
    assert payload["live_trading_enabled"] is False


def test_legacy_worker_and_expanded_rounds_cannot_write_candidates() -> None:
    invocations = (
        ("scripts/run_research_task_worker.py", "{}"),
        ("scripts/run_expanded_long_only_research.py", "--phase", "rounds"),
        ("scripts/run_first_workflow.py",),
    )
    for invocation in invocations:
        completed = subprocess.run(
            [sys.executable, *invocation],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert completed.returncode == 2, (invocation, completed.stderr)
        payload = json.loads(completed.stdout.strip().splitlines()[-1])
        assert payload["status"] == "retired_legacy_entrypoint"
        assert payload["candidate_written"] is False
        assert payload["live_trading_enabled"] is False
