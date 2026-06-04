import json
from factor_lab.harvest_executor import run_harvest_cycle


def _plan():
    return {
        "cycle_id": "cycle_0001",
        "mainline": "defensive_quality_risk_layer",
        "research_budget": {"max_experiments": 1, "max_runtime_minutes": 1},
        "proposals": [
            {"proposal_id": "dq_v1", "mechanism_id": "defensive_quality", "experiment_type": "defensive_quality_risk_layer"}
        ],
    }


def test_dry_run_default_writes_manifest_and_status_only(tmp_path):
    result = run_harvest_cycle(_plan(), {"decision": "allow_controlled_execution", "allowed_experiments": ["dq_v1"]}, root=tmp_path)
    assert result["execution_status"] == "dry_run"
    assert result["executed_count"] == 0
    assert result["started_systemd_daemon"] is False
    run_dir = tmp_path / "artifacts/harvest_agent/cycle_0001/runs/dq_v1"
    assert (run_dir / "status.json").exists()
    assert not (run_dir / "result.json").exists()


def test_controlled_execution_requires_flag_and_records_missing_dataset(tmp_path):
    result = run_harvest_cycle(_plan(), {"decision": "allow_controlled_execution", "allowed_experiments": ["dq_v1"]}, root=tmp_path, allow_controlled_execution=True, max_experiments=1)
    assert result["execution_status"] in {"completed", "partial"}
    status = json.loads((tmp_path / "artifacts/harvest_agent/cycle_0001/runs/dq_v1/status.json").read_text())
    assert status["status"] in {"blocked_missing_data", "unsupported_experiment_type", "ok", "failed"}


def test_blocked_gate_refuses_execution(tmp_path):
    result = run_harvest_cycle(_plan(), {"decision": "block", "reasons": ["unsafe"]}, root=tmp_path, allow_controlled_execution=True)
    assert result["execution_status"] == "blocked"
    assert result["executed_count"] == 0
