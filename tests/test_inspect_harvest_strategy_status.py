import json
import subprocess
import sys

from factor_lab.harvest_strategy_governor import run_harvest_strategy_governor


def test_inspect_harvest_strategy_status_without_pointer(tmp_path):
    result = subprocess.run(
        [sys.executable, "scripts/inspect_harvest_strategy_status.py", "--root", str(tmp_path)],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["strategy_status"] == "missing"


def test_inspect_harvest_strategy_status_prints_latest_plan(tmp_path):
    run_harvest_strategy_governor(tmp_path, write=True, strategy_run_id="strategy_test")

    result = subprocess.run(
        [sys.executable, "scripts/inspect_harvest_strategy_status.py", "--root", str(tmp_path)],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["strategy_status"] == "available"
    assert payload["strategy_run_id"] == "strategy_test"
    assert "no_live_trading" in payload["safety"]
