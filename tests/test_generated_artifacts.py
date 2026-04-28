import json
from pathlib import Path

from factor_lab.generated_artifacts import upgrade_generated_batch, upgrade_generated_config
from factor_lab.research_queue import validate_generated_batch_payload


def test_upgrade_generated_config_adds_dependency_graph_metadata():
    payload = upgrade_generated_config(
        {
            "factors": [
                {"name": "mom_20", "expression": "close"},
                {"name": "value_ep", "expression": "open"},
                {
                    "name": "gen__spread",
                    "expression": "close - open",
                    "generator_operator": "combine_sub",
                    "left_factor_name": "mom_20",
                    "right_factor_name": "value_ep",
                },
            ]
        },
        source="test",
    )

    assert payload["schema_version"] == "factor_lab.generated_config.v2"
    assert payload["artifact_type"] == "generated_workflow_config"
    assert payload["dependency_graph"]["generated_factor_count"] == 1
    assert payload["dependency_graph"]["generated_factors"][0]["inputs"] == ["mom_20", "value_ep"]
    assert payload["dependency_graph"]["edges"] == [
        {"from": "mom_20", "to": "gen__spread", "role": "left"},
        {"from": "value_ep", "to": "gen__spread", "role": "right"},
    ]


def test_validate_generated_batch_payload_accepts_legacy_artifacts_without_schema(tmp_path: Path):
    cfg = tmp_path / "legacy_generated_config.json"
    cfg.write_text(
        json.dumps(
            {
                "factors": [
                    {"name": "mom_20", "expression": "close"},
                    {"name": "value_ep", "expression": "open"},
                    {
                        "name": "gen__spread",
                        "expression": "close - open",
                        "generator_operator": "combine_sub",
                        "left_factor_name": "mom_20",
                        "right_factor_name": "value_ep",
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    batch = tmp_path / "legacy_generated_batch.json"
    batch.write_text(
        json.dumps({"jobs": [{"name": "recent_45d", "config_path": str(cfg)}]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    ok, error = validate_generated_batch_payload({"task_type": "generated_batch", "payload": {"batch_path": str(batch)}})

    assert ok is True
    assert error is None
    upgraded_batch = upgrade_generated_batch(json.loads(batch.read_text(encoding="utf-8")), source="test")
    assert upgraded_batch["schema_version"] == "factor_lab.generated_batch.v2"
