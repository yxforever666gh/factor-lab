from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
RETIRED_WRITERS = (
    ("build_research_candidate_pool.py",),
    ("build_approved_candidate_universe.py",),
    ("run_research_orchestrator.py",),
    ("seed_research_queue.py",),
    ("build_paper_portfolio.py",),
    ("run_harvest_cycle.py",),
    ("run_harvest_autonomous_research_controller.py",),
    ("run_harvest_evolution_loop.py",),
    ("run_harvest_strategy_governor.py",),
    ("run_harvest_agent_once.py",),
    ("admit_pledge_controlled_probe_task.py", "--write"),
    ("prepare_bucket_aware_tasks.py", "--write"),
    ("run_controlled_admission_feeder.py", "--write"),
    ("apply_strategy_plan.py",),
    ("prepare_gated_research_task.py", "--write"),
    ("run_controlled_orchestrator_once.py", "--allow-empty"),
    ("run_research_daemon.py",),
    ("run_research_task_worker.py", '{"task_type":"workflow","payload":{}}'),
    ("run_post_h_controlled_restart_acceptance.py", "--one-shot"),
)


@pytest.mark.parametrize("invocation", RETIRED_WRITERS, ids=lambda row: row[0])
def test_legacy_candidate_and_portfolio_writers_are_fail_closed(
    invocation: tuple[str, ...],
    tmp_path: Path,
) -> None:
    script_name, *arguments = invocation
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT / "src")
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / script_name), *arguments],
        cwd=tmp_path,
        env=environment,
        text=True,
        capture_output=True,
        timeout=30,
    )

    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "retired_legacy_entrypoint"
    assert payload["candidate_written"] is False
    assert not (tmp_path / "artifacts").exists()
