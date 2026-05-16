from factor_lab.research_experiment_gate import evaluate_research_experiment_gate
from factor_lab.research_experiment_proposal import ResearchExperimentProposal
from factor_lab.storage import ExperimentStore


def _proposal(**overrides):
    payload = dict(
        proposal_id="p1",
        experiment_type="generated_candidate",
        factor_names=["gen_a"],
        expressions=["book_yield + roe"],
        mechanism_id="value_quality_no_distress",
        hypothesis="cheap quality should outperform",
        expected_information_gain=["mechanism_validation"],
        required_data_fields=["book_yield", "roe"],
        start_date="2024-01-01",
        end_date="2024-12-31",
        universe_limit=100,
        horizon="60d",
        parent_candidates=[],
        novelty_claim="value plus quality",
        falsification_criteria=["neutralized IC <= 0"],
        promote_if=["stable"],
        stop_if=["coverage low"],
        source_agent="planner",
        budget_bucket="mechanism_validation",
    )
    payload.update(overrides)
    return ResearchExperimentProposal(**payload)


def test_gate_allows_valid_mechanism_proposal(tmp_path):
    store = ExperimentStore(tmp_path / "factor_lab.db")

    result = evaluate_research_experiment_gate(
        _proposal(),
        available_fields={"book_yield", "roe"},
        store=store,
        fingerprint="workflow::unique",
    )

    assert result["decision"] == "allow"
    assert result["expected_information_gain_score"] > 0


def test_gate_blocks_missing_required_data_field(tmp_path):
    store = ExperimentStore(tmp_path / "factor_lab.db")

    result = evaluate_research_experiment_gate(
        _proposal(required_data_fields=["book_yield", "cashflow_to_profit"]),
        available_fields={"book_yield", "roe"},
        store=store,
        fingerprint="workflow::unique",
    )

    assert result["decision"] == "block"
    assert "missing required data fields: cashflow_to_profit" in result["reasons"]


def test_gate_downgrades_mechanical_generated_candidate_without_mechanism(tmp_path):
    store = ExperimentStore(tmp_path / "factor_lab.db")

    result = evaluate_research_experiment_gate(
        _proposal(mechanism_id=None, novelty_claim=None, hypothesis="some test"),
        available_fields={"book_yield", "roe"},
        store=store,
        fingerprint="workflow::unique",
    )

    assert result["decision"] == "cheap_screen_only"
    assert "generated candidate lacks mechanism_id or novelty_claim" in result["reasons"]


def test_gate_blocks_duplicate_finished_fingerprint(tmp_path):
    store = ExperimentStore(tmp_path / "factor_lab.db")
    task_id = store.enqueue_research_task(
        task_type="workflow",
        payload={"config_path": "configs/a.json", "output_dir": "artifacts/a"},
        fingerprint="workflow::dup",
    )
    store.finish_research_task(task_id, status="finished")

    result = evaluate_research_experiment_gate(
        _proposal(),
        available_fields={"book_yield", "roe"},
        store=store,
        fingerprint="workflow::dup",
    )

    assert result["decision"] == "block"
    assert "equivalent experiment already finished within governance window" in result["reasons"]
