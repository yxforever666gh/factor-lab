import json
from pathlib import Path

from factor_lab import research_queue
from factor_lab.research_task_governance import govern_research_task_spec, govern_workflow_task_spec, write_gate_decision
from factor_lab.storage import ExperimentStore


def test_govern_workflow_task_spec_allows_valid_workflow(tmp_path, monkeypatch):
    store = ExperimentStore(tmp_path / "factor_lab.db")
    config = {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "universe_limit": 100,
        "factors": [{"name": "value", "expression": "book_yield"}],
    }
    task_spec = {
        "task_type": "workflow",
        "priority": 10,
        "payload": {
            "config_path": "configs/a.json",
            "output_dir": "artifacts/a",
            "hypothesis": "value should work",
            "expected_information_gain": ["window_stability_check"],
            "falsification_criteria": ["neutralized IC <= 0"],
            "budget_bucket": "robustness_validation",
        },
        "fingerprint": "workflow::unique",
        "worker_note": "validation｜value",
    }

    result = govern_workflow_task_spec(
        task_spec,
        config=config,
        store=store,
        available_fields={"book_yield"},
        used_counts={"total": 0, "robustness_validation": 0},
    )

    assert result["decision"] == "allow"
    assert result["task_spec"] == task_spec
    assert result["proposal"].factor_names == ["value"]


def test_govern_workflow_task_spec_blocks_and_drops_duplicate(tmp_path, monkeypatch):
    store = ExperimentStore(tmp_path / "factor_lab.db")
    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": "configs/a.json", "output_dir": "artifacts/a"},
        fingerprint="workflow::dup",
    )
    store.finish_research_task(task_id, status="finished")
    config = {"start_date": "2024-01-01", "end_date": "2024-12-31", "factors": [{"name": "value", "expression": "book_yield"}]}
    task_spec = {
        "task_type": "workflow",
        "payload": {"config_path": "configs/a.json", "output_dir": "artifacts/b", "hypothesis": "value should work"},
        "fingerprint": "workflow::dup",
        "worker_note": "validation｜value",
    }

    result = govern_workflow_task_spec(task_spec, config=config, store=store, available_fields={"book_yield"})

    assert result["decision"] == "block"
    assert result["task_spec"] is None
    assert "equivalent experiment already finished within governance window" in result["gate"]["reasons"]


def test_write_gate_decision_appends_jsonl(tmp_path):
    path = tmp_path / "gate.jsonl"

    write_gate_decision(
        path=path,
        source="unit-test",
        proposal_id="p1",
        gate={"decision": "block", "reasons": ["x"], "expected_information_gain_score": 0.2, "budget_bucket": "pure_exploration"},
        budget={"decision": "allow", "reasons": []},
    )

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["source"] == "unit-test"
    assert payload["decision"] == "block"
    assert payload["reasons"] == ["x"]


def test_baseline_reseed_applies_governance_to_workflow_seed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs" / "tushare_workflow.json").write_text(
        '{"start_date":"2024-01-01","end_date":"2024-12-31","factors":[{"name":"value","expression":"book_yield"}]}',
        encoding="utf-8",
    )
    (tmp_path / "configs" / "tushare_batch.json").write_text('{"jobs": []}', encoding="utf-8")
    store = ExperimentStore(tmp_path / "factor_lab.db")
    governed = []

    def fake_govern(task_spec, *, config, store, used_counts=None, source=None, **kwargs):
        governed.append((task_spec, config, source))
        return {"decision": "block", "task_spec": None, "gate": {"reasons": ["blocked for test"]}, "budget": {}}

    monkeypatch.setattr(research_queue, "govern_workflow_task_spec", fake_govern)
    monkeypatch.setattr(research_queue, "recently_finished_same_fingerprint", lambda *args, **kwargs: False)

    result = research_queue.enqueue_baseline_tasks_with_diagnostics(store)

    assert governed
    assert governed[0][2] == "baseline_reseed"
    assert all(task["task_type"] != "workflow" for task in store.list_research_tasks(limit=20))
    assert any(row.get("reason") == "governance_blocked" for row in result["skipped"])


def test_govern_research_task_spec_allows_generated_batch_with_specialized_proposal(tmp_path):
    store = ExperimentStore(tmp_path / "factor_lab.db")
    task_spec = {
        "task_type": "generated_batch",
        "priority": 40,
        "payload": {
            "batch_path": "artifacts/generated_batch.json",
            "output_dir": "artifacts/generated_batch_run",
            "hypothesis": "mechanism generated batch should cheaply screen new branches",
            "expected_information_gain": ["new_branch_opened"],
            "falsification_criteria": ["cheap screen rejects all generated candidates"],
            "budget_bucket": "pure_exploration",
        },
        "fingerprint": "generated_batch::unique",
        "worker_note": "exploration｜generated batch",
    }

    result = govern_research_task_spec(task_spec, store=store, used_counts={"total": 0, "pure_exploration": 0})

    assert result["decision"] == "allow"
    assert result["proposal"].experiment_type == "generated_batch"
    assert result["proposal"].budget_bucket == "pure_exploration"
    assert result["task_spec"]["payload"]["governance"]["decision"] == "allow"
