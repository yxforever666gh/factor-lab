import json
from pathlib import Path

from factor_lab.opportunity_to_tasks import map_opportunity_to_task


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_opportunity_generated_config_materializes_factors():
    opportunity = {
        "opportunity_id": "opp-test-expand",
        "opportunity_type": "expand",
        "question": "test expand opportunity",
        "hypothesis": "test hypothesis",
        "target_family": "stable_candidate_validation",
        "target_candidates": ["mom_20", "mom_60"],
        "expected_knowledge_gain": ["window_stability_check"],
        "priority": 0.5,
    }

    task = map_opportunity_to_task(opportunity)
    assert task is not None
    assert task["task_type"] == "generated_batch"

    batch_path = REPO_ROOT / task["payload"]["batch_path"]
    batch = json.loads(batch_path.read_text(encoding="utf-8"))
    config_path = REPO_ROOT / batch["jobs"][0]["config_path"]
    config = json.loads(config_path.read_text(encoding="utf-8"))

    assert batch["schema_version"] == "factor_lab.generated_batch.v2"
    assert batch["job_count"] >= 1
    assert "factor_family_config" not in config
    assert config["schema_version"] == "factor_lab.generated_config.v2"
    assert config["artifact_type"] == "generated_workflow_config"
    assert config["dependency_graph"]["node_count"] >= 2
    assert [row["name"] for row in config["factors"]] == ["mom_20", "mom_60"]


def test_opportunity_cheap_screen_config_is_lightened(monkeypatch):
    monkeypatch.setenv("RESEARCH_OPPORTUNITY_LIGHT_UNIVERSE_LIMIT", "24")
    monkeypatch.setenv("RESEARCH_OPPORTUNITY_LIGHT_ROLLING_WINDOW", "19")
    monkeypatch.setenv("RESEARCH_OPPORTUNITY_LIGHT_ROLLING_STEP", "5")

    opportunity = {
        "opportunity_id": "opp-test-probe",
        "opportunity_type": "probe",
        "question": "test probe opportunity",
        "hypothesis": "test hypothesis",
        "target_family": "stable_candidate_validation",
        "target_candidates": ["mom_20", "mom_60"],
        "expected_knowledge_gain": ["new_branch_opened"],
        "priority": 0.7,
        "execution_mode": "cheap_screen",
    }

    task = map_opportunity_to_task(opportunity)
    assert task is not None
    assert task["task_type"] == "generated_batch"

    batch_path = REPO_ROOT / task["payload"]["batch_path"]
    batch = json.loads(batch_path.read_text(encoding="utf-8"))
    config_path = REPO_ROOT / batch["jobs"][0]["config_path"]
    config = json.loads(config_path.read_text(encoding="utf-8"))

    assert config["universe_limit"] == 24
    assert config["rolling_validation"]["window_size"] == 19
    assert config["rolling_validation"]["step_size"] == 5
    assert config["research_profile"] == "opportunity_cheap_screen"
    assert config["refresh_global_risk"] is False
    assert config["refresh_exposure_track"] is False
