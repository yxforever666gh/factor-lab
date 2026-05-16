import json

from factor_lab.value_sleeve_policy import (
    build_value_sleeve_policy,
    load_value_sleeve_policy,
    route_sleeve_action,
    write_value_sleeve_policy,
)


def test_build_policy_collapses_value_routes_from_decision(tmp_path):
    decision_path = tmp_path / "value_sleeve_decision.json"
    scorecard_path = tmp_path / "route_scorecard.json"
    decision_path.write_text(json.dumps({
        "decision": "collapse_to_value_sleeve_with_primary_route",
        "primary_route": "value_quality_no_distress",
        "confirmation_route": "value_momentum_confirmation",
        "low_weight_route": "industry_relative_value",
    }), encoding="utf-8")
    scorecard_path.write_text(json.dumps({"routes": {"value_quality_no_distress": {"score": 1.0}}}), encoding="utf-8")

    policy = build_value_sleeve_policy(decision_path=decision_path, scorecard_path=scorecard_path)

    assert policy["decision"] == "collapse_to_value_sleeve_with_primary_route"
    assert policy["primary_route"] == "value_quality_no_distress"
    assert policy["confirmation_route"] == "value_momentum_confirmation"
    assert policy["low_weight_route"] == "industry_relative_value"
    assert policy["routes"]["value_quality_no_distress"]["action"] == "prioritize_primary"
    assert policy["routes"]["value_momentum_confirmation"]["action"] == "confirmation_only"
    assert policy["routes"]["industry_relative_value"]["action"] == "cap_or_skip_duplicate"


def test_build_policy_is_safe_when_artifacts_missing(tmp_path):
    policy = build_value_sleeve_policy(decision_path=tmp_path / "missing.json", scorecard_path=tmp_path / "missing_scorecard.json")

    assert policy["decision"] == "no_sleeve_policy"
    assert route_sleeve_action("unknown", policy)["action"] == "no_sleeve_policy"


def test_write_and_load_value_sleeve_policy(tmp_path):
    decision_path = tmp_path / "decision.json"
    scorecard_path = tmp_path / "scorecard.json"
    json_path = tmp_path / "policy.json"
    md_path = tmp_path / "policy.md"
    decision_path.write_text(json.dumps({"decision": "collapse_to_value_sleeve_with_primary_route"}), encoding="utf-8")
    scorecard_path.write_text(json.dumps({}), encoding="utf-8")

    policy = write_value_sleeve_policy(json_path=json_path, markdown_path=md_path, decision_path=decision_path, scorecard_path=scorecard_path)

    assert json.loads(json_path.read_text(encoding="utf-8"))["decision"] == policy["decision"]
    assert load_value_sleeve_policy(json_path)["primary_route"] == "value_quality_no_distress"
    text = md_path.read_text(encoding="utf-8")
    assert "Value Sleeve Policy" in text
    assert "Primary route: value_quality_no_distress" in text
