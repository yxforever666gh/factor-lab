import json
import subprocess
import sys


def test_inspect_harvest_controller_status_reports_latest_run(tmp_path):
    base = tmp_path / "artifacts/harvest_agent"
    run_dir = base / "controller_runs/controller_test"
    run_dir.mkdir(parents=True)
    (base / "latest_controller_run.json").write_text(json.dumps({"controller_run_id": "controller_test", "artifacts_dir": str(run_dir)}))
    (run_dir / "controller_summary.json").write_text(json.dumps({
        "controller_run_id": "controller_test",
        "cycles_run": 1,
        "executed_backtest_count": 7,
        "stop_reason": "budget_exhausted",
        "started_systemd_daemon": False,
        "scheduled_timer_enabled": False,
    }))
    (run_dir / "controller_ledger.jsonl").write_text(json.dumps({
        "cycle_id": "cycle_0051",
        "branch": "cost_robustness_branch",
        "oos_class": "fail",
        "research_decision": "risk_reduction_branch",
    }) + "\n")

    result = subprocess.run(
        [sys.executable, "scripts/inspect_harvest_controller_status.py", "--root", str(tmp_path)],
        cwd=".",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    assert result.returncode == 0, result.stderr
    assert "controller_test" in result.stdout
    assert "cycle_0051" in result.stdout
    assert "cost_robustness_branch" in result.stdout
    assert "budget_exhausted" in result.stdout
    assert "started_systemd_daemon: False" in result.stdout
