import json
from factor_lab.harvest_execution_manifest import build_execution_manifest, write_execution_manifest


def _plan():
    return {
        "cycle_id": "cycle_0001",
        "mainline": "defensive_quality_risk_layer",
        "research_budget": {"max_experiments": 1, "max_runtime_minutes": 5},
        "proposals": [
            {"proposal_id": "dq_v1", "mechanism_id": "defensive_quality", "experiment_type": "defensive_quality_risk_layer", "required_fields": ["roe"]},
            {"proposal_id": "dq_v2", "mechanism_id": "defensive_quality", "experiment_type": "controlled_backtest"},
        ],
    }


def test_manifest_caps_experiments_and_uses_harvest_runs_path(tmp_path):
    manifest = build_execution_manifest(_plan(), {"decision": "allow_controlled_execution", "allowed_experiments": ["dq_v1", "dq_v2"]}, root=tmp_path)
    assert manifest["manifest_status"] == "ready"
    assert len(manifest["experiments"]) == 1
    exp = manifest["experiments"][0]
    assert exp["experiment_id"] == "dq_v1"
    assert "artifacts/harvest_agent/cycle_0001/runs/dq_v1" in exp["output_dir"]
    assert exp["timeout_seconds"] == 300


def test_manifest_blocks_non_admitted_gate(tmp_path):
    manifest = build_execution_manifest(_plan(), {"decision": "block", "reasons": ["duplicate"]}, root=tmp_path)
    assert manifest["manifest_status"] == "blocked"
    assert manifest["experiments"] == []


def test_write_execution_manifest(tmp_path):
    path = write_execution_manifest(_plan(), {"decision": "allow_dry_run", "allowed_experiments": ["dq_v1"]}, root=tmp_path)
    data = json.loads(path.read_text())
    assert data["execution_mode"] == "dry_run"
