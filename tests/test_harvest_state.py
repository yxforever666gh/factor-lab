import json

from factor_lab.harvest_state import build_harvest_state_snapshot, write_harvest_state_snapshot


def test_missing_optional_files_do_not_crash(tmp_path):
    snapshot = build_harvest_state_snapshot(root=tmp_path)
    assert snapshot["schema_version"] == 1
    assert snapshot["current_blockers"] == []
    assert snapshot["promoted_bucket_aware_routes"] == []


def test_old_path_pollution_creates_blocker(tmp_path):
    audit = tmp_path / "artifacts/runtime_takeover_audit.json"
    audit.parent.mkdir(parents=True)
    audit.write_text(json.dumps({"polluted_paths": ["artifacts/generated/old_run"]}))
    snapshot = build_harvest_state_snapshot(root=tmp_path)
    assert "old_path_pollution_detected" in snapshot["current_blockers"]


def test_promoted_bucket_aware_routes_are_included(tmp_path):
    p = tmp_path / "artifacts/controlled_route_policy.json"
    p.parent.mkdir(parents=True)
    p.write_text(json.dumps({"promoted_routes": [{"route_id": "value_quality_no_distress", "oos_status": "bucket_aware_pass"}]}))
    snapshot = build_harvest_state_snapshot(root=tmp_path)
    assert "value_quality_no_distress" in snapshot["promoted_bucket_aware_routes"]


def test_data_blockers_and_latest_verdict_are_included(tmp_path):
    k = tmp_path / "knowledge/data_blockers.json"
    k.parent.mkdir(parents=True)
    k.write_text(json.dumps({"blocked_fields": ["analyst_revision"]}))
    latest = tmp_path / "artifacts/harvest_agent/latest_cycle.json"
    latest.parent.mkdir(parents=True)
    latest.write_text(json.dumps({"cycle_id": "cycle_0007", "verdict": {"decision": "hold"}}))
    snapshot = build_harvest_state_snapshot(root=tmp_path)
    assert snapshot["data_blockers"]["blocked_fields"] == ["analyst_revision"]
    assert snapshot["latest_harvest_verdict"] == {"decision": "hold"}


def test_write_snapshot_uses_harvest_namespace(tmp_path):
    out = write_harvest_state_snapshot("cycle_0001", root=tmp_path)
    assert "artifacts/harvest_agent/cycle_0001/state_snapshot.json" in out.as_posix()
    assert out.exists()
