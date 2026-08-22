import json
import subprocess
import sys


def test_run_harvest_strategy_governor_cli_help():
    result = subprocess.run(
        [sys.executable, "scripts/run_harvest_strategy_governor.py", "--help"],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0
    assert "--lookback-cycles" in result.stdout
    assert "--max-next-backtests" in result.stdout


def test_run_harvest_strategy_governor_cli_dry_run_no_pointer(tmp_path):
    result = subprocess.run(
        [sys.executable, "scripts/run_harvest_strategy_governor.py", "--root", str(tmp_path), "--lookback-cycles", "2"],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "retired_legacy_entrypoint"
    assert payload["candidate_written"] is False
    assert not (tmp_path / "artifacts/harvest_agent/latest_strategy_run.json").exists()


def test_run_harvest_strategy_governor_cli_write_creates_pointer(tmp_path):
    result = subprocess.run(
        [sys.executable, "scripts/run_harvest_strategy_governor.py", "--root", str(tmp_path), "--write"],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "retired_legacy_entrypoint"
    assert payload["candidate_written"] is False
    assert not (tmp_path / "artifacts/harvest_agent/latest_strategy_run.json").exists()
