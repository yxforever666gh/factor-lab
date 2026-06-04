import json

from factor_lab.harvest_controller_ledger import append_controller_event, write_controller_summary


def test_append_controller_event_writes_jsonl(tmp_path):
    run_dir = tmp_path / "controller_1"
    append_controller_event(run_dir, {"event_index": 1, "cycle_id": "cycle_0001"})
    append_controller_event(run_dir, {"event_index": 2, "stop_reason": "budget_exhausted"})

    lines = (run_dir / "controller_ledger.jsonl").read_text().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["cycle_id"] == "cycle_0001"
    assert json.loads(lines[1])["stop_reason"] == "budget_exhausted"


def test_write_controller_summary_totals_events_and_preserves_safety(tmp_path):
    run_dir = tmp_path / "controller_1"
    summary = write_controller_summary(run_dir, [
        {"cycle_id": "cycle_0001", "executed_backtest_count": 12},
        {"cycle_id": "cycle_0002", "executed_backtest_count": 18, "stop_reason": "route_stop"},
    ])

    assert summary["cycles_run"] == 2
    assert summary["executed_backtest_count"] == 30
    assert summary["stop_reason"] == "route_stop"
    assert summary["started_systemd_daemon"] is False
    assert summary["scheduled_timer_enabled"] is False
    assert (run_dir / "controller_summary.json").exists()
    assert "route_stop" in (run_dir / "controller_summary.md").read_text()
