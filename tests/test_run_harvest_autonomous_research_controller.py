import json
import subprocess
import sys


def test_run_harvest_autonomous_research_controller_cli_help():
    result = subprocess.run(
        [sys.executable, "scripts/run_harvest_autonomous_research_controller.py", "--help"],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0
    assert "--max-cycles" in result.stdout
    assert "--allow-controlled-execution" in result.stdout
    assert "--use-latest-strategy-plan" in result.stdout


def test_run_harvest_autonomous_research_controller_cli_dry_run_json(tmp_path):
    # Missing v3 plan is still a valid controller stop, and verifies CLI JSON contract.
    result = subprocess.run(
        [
            sys.executable,
            "scripts/run_harvest_autonomous_research_controller.py",
            "--root",
            str(tmp_path),
            "--max-cycles",
            "1",
            "--max-backtests",
            "10",
        ],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 2, result.stderr
    payload = json.loads(result.stdout)
    assert payload["status"] == "retired_legacy_entrypoint"
    assert payload["candidate_written"] is False
    assert not (tmp_path / "artifacts").exists()
