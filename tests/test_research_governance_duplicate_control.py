from datetime import datetime, timedelta, timezone

from factor_lab.research_runtime_state import recently_finished_same_fingerprint
from factor_lab.storage import ExperimentStore


def test_recently_finished_same_fingerprint_blocks_workflow_equivalent_repeat_by_governance(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    policy_path = tmp_path / "research_governance.json"
    policy_path.write_text(
        '{"duplicate_control": {"enabled": true, "max_equivalent_runs_24h": 1, "max_equivalent_runs_7d": 3}}',
        encoding="utf-8",
    )
    monkeypatch.setattr("factor_lab.research_runtime_state.GOVERNANCE_CONFIG_PATH", policy_path)

    fingerprint = "workflow::equiv-abc"
    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": "configs/a.json", "output_dir": "artifacts/a"},
        fingerprint=fingerprint,
        worker_note="baseline｜test",
    )
    store.finish_research_task(task_id, status="finished")

    assert recently_finished_same_fingerprint(store, fingerprint, cooldown_minutes=0, task_type="workflow") is True


def test_recently_finished_same_fingerprint_respects_disabled_governance(tmp_path, monkeypatch):
    db_path = tmp_path / "factor_lab.db"
    store = ExperimentStore(db_path)
    policy_path = tmp_path / "research_governance.json"
    policy_path.write_text('{"duplicate_control": {"enabled": false}}', encoding="utf-8")
    monkeypatch.setattr("factor_lab.research_runtime_state.GOVERNANCE_CONFIG_PATH", policy_path)

    fingerprint = "workflow::equiv-abc"
    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": "configs/a.json", "output_dir": "artifacts/a"},
        fingerprint=fingerprint,
        worker_note="baseline｜test",
    )
    old_finished = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    store.conn.execute(
        "UPDATE research_tasks SET status='finished', finished_at_utc=? WHERE task_id=?",
        (old_finished, task_id),
    )
    store.conn.commit()

    assert recently_finished_same_fingerprint(store, fingerprint, cooldown_minutes=1, task_type="workflow") is False
