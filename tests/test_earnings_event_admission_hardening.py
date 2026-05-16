import json

from factor_lab.runtime_takeover_policy import load_runtime_takeover_policy
from factor_lab.workflow_admission_adapter import enforce_workflow_admission


def test_config_local_feature_overlay_columns_are_available_to_admission(tmp_path):
    cfg = {
        "route_id": "earnings_event_express_diluted_roe_yoy",
        "mechanism_id": "earnings_event_quality_revision",
        "feature_overlay_csv": "artifacts/earnings_event_source_mvp/earnings_event_daily_asof_features.csv",
        "feature_overlay_columns": ["high_express_diluted_roe_yoy"],
        "required_data_fields": [],
        "factors": [
            {
                "name": "high_express_diluted_roe_yoy",
                "expression": "high_express_diluted_roe_yoy",
            }
        ],
        "portfolio_construction": {"mode": "bucket_pair", "quantiles": 5, "long_quantile": 3, "short_quantile": 0},
    }
    path = tmp_path / "earnings_event_overlay.json"
    path.write_text(json.dumps(cfg), encoding="utf-8")

    result = enforce_workflow_admission(
        {"task_type": "workflow", "payload": {"config_path": str(path), "output_dir": str(tmp_path / "out")}}
    )

    assert result["decision"] == "allow"
    assert result["reasons"] == []
    assert result["admission"]["coverage_preflight"]["missing_fields"] == []


def test_takeover_policy_allows_explicit_controlled_non_value_route():
    policy = load_runtime_takeover_policy(
        {
            "enabled": True,
            "allowed_value_routes": ["value_quality_no_distress"],
            "allowed_controlled_routes": ["earnings_event_express_diluted_roe_yoy"],
        }
    )

    decision = policy.evaluate_task(
        {
            "task_type": "workflow",
            "payload": {
                "route_id": "earnings_event_express_diluted_roe_yoy",
                "mechanism_id": "earnings_event_quality_revision",
            },
        }
    )

    assert decision["decision"] == "allow"
    assert decision["reasons"] == []


def test_takeover_policy_still_blocks_unlisted_controlled_route():
    policy = load_runtime_takeover_policy(
        {
            "enabled": True,
            "allowed_value_routes": ["value_quality_no_distress"],
            "allowed_controlled_routes": ["earnings_event_express_diluted_roe_yoy"],
        }
    )

    decision = policy.evaluate_task(
        {
            "task_type": "workflow",
            "payload": {
                "route_id": "unlisted_event_route",
                "mechanism_id": "unlisted_event_mechanism",
            },
        }
    )

    assert decision["decision"] == "block"
    assert "route_not_allowed_in_takeover" in decision["reasons"]
