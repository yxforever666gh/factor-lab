import json
from pathlib import Path

from factor_lab.factors import resolve_factor_definitions
from factor_lab.llm_agent import SingleLLMAgent
from factor_lab.plan_validator import validate_plan
from factor_lab.proposal_to_batch import generate_batch_from_plan, load_base_config


REPO_ROOT = Path(__file__).resolve().parents[1]
BASE_CONFIG_PATH = REPO_ROOT / "configs" / "tushare_workflow.json"


def test_load_base_config_materializes_factor_family_config():
    config = load_base_config(BASE_CONFIG_PATH)

    assert "factors" in config
    assert "factor_family_config" not in config
    assert len(config["factors"]) >= 8
    assert {row["name"] for row in config["factors"]}.issuperset({"mom_20", "mom_60", "book_yield"})


def test_generate_batch_from_plan_writes_materialized_workflow(tmp_path):
    output_path = tmp_path / "generated_batch.json"
    plan = {
        "focus_factors": ["mom_20", "mom_60"],
        "keep_as_core_candidates": ["mom_20"],
        "review_graveyard": [],
        "portfolio_checks": ["compare_all_factors_vs_candidates_only"],
        "rationale": "test batch generation",
    }

    batch = generate_batch_from_plan(plan, base_config_path=BASE_CONFIG_PATH, output_path=output_path)
    workflow_path = output_path.with_name("generated_workflow_from_llm.json")
    workflow = json.loads(workflow_path.read_text(encoding="utf-8"))

    assert batch["jobs"][0]["config_path"] == str(workflow_path)
    assert batch["schema_version"] == "factor_lab.generated_batch.v2"
    assert batch["job_count"] == 1
    assert "factor_family_config" not in workflow
    assert workflow["schema_version"] == "factor_lab.generated_config.v2"
    assert workflow["artifact_type"] == "generated_workflow_config"
    assert workflow["dependency_graph"]["node_count"] >= 2
    assert [row["name"] for row in workflow["factors"]] == ["mom_20", "mom_60"]


def test_mock_llm_plan_stays_validator_compatible_with_hybrid_snapshot_names():
    base_config = json.loads(BASE_CONFIG_PATH.read_text(encoding="utf-8"))
    allowed = {row["name"] for row in resolve_factor_definitions(base_config, config_dir=BASE_CONFIG_PATH.parent)}
    agent = SingleLLMAgent(provider="mock")
    snapshot = {
        "latest_candidates": [],
        "latest_graveyard": ["mom_20", "mom_60"],
        "latest_top_ranked_factors": ["hybrid_mom_20_mom_plus_value", "mom_20"],
        "stable_candidates": [
            {"factor_name": "hybrid_mom_20_liquidity_turnover_shock"},
            {"factor_name": "mom_20"},
            {"factor_name": "earnings_yield"},
        ],
        "top_scores": [
            {"factor_name": "hybrid_mom_20_liquidity_turnover_shock"},
            {"factor_name": "mom_60"},
        ],
        "paper_portfolio_stability": {"stability_score": 0.88, "label": "高稳定"},
    }

    plan = agent.generate_plan(snapshot)
    validation = validate_plan(plan, allowed, paper_portfolio_stability=snapshot["paper_portfolio_stability"])

    assert validation["valid"] is True
    assert plan["focus_factors"]
    assert all(name in allowed for name in plan["focus_factors"])
    assert all(not name.startswith("hybrid_") for name in plan["focus_factors"])
