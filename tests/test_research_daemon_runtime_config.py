from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_daemon():
    path = Path(__file__).resolve().parents[1] / "scripts" / "run_research_daemon.py"
    spec = importlib.util.spec_from_file_location("run_research_daemon_runtime_config_test", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_daemon_runtime_config_defaults():
    daemon = _load_daemon()

    cfg = daemon.load_runtime_config({})

    assert cfg.idle_sleep_seconds == 60
    assert cfg.base_max_tasks_per_loop == 1
    assert cfg.max_tasks_before_restart == 8
    assert cfg.rss_limit_mb == 2048
    assert cfg.max_loops == 0
    assert cfg.exit_when_idle is False
    assert cfg.exit_when_no_claimable is False
    assert cfg.controlled_only is True


def test_daemon_runtime_config_supports_bounded_exit_flags():
    daemon = _load_daemon()

    cfg = daemon.load_runtime_config({
        "RESEARCH_DAEMON_IDLE_SECONDS": "2",
        "RESEARCH_DAEMON_MAX_TASKS": "3",
        "RESEARCH_DAEMON_MAX_LOOPS": "4",
        "RESEARCH_DAEMON_EXIT_WHEN_IDLE": "1",
        "RESEARCH_DAEMON_EXIT_WHEN_NO_CLAIMABLE": "true",
        "RESEARCH_DAEMON_CONTROLLED_ONLY": "0",
    })

    assert cfg.idle_sleep_seconds == 2
    assert cfg.base_max_tasks_per_loop == 3
    assert cfg.max_loops == 4
    assert cfg.exit_when_idle is True
    assert cfg.exit_when_no_claimable is True
    assert cfg.controlled_only is False
