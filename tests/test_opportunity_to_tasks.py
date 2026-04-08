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

    assert "factor_family_config" not in config
    assert [row["name"] for row in config["factors"]] == ["mom_20", "mom_60"]
