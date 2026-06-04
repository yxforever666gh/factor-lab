from factor_lab.harvest_reviewer_decision import normalize_reviewer_decision, validate_reviewer_decision


def test_reviewer_decision_contains_required_fields():
    decision = normalize_reviewer_decision({"decision": "allow"})
    assert decision["decision"] == "allow"
    assert decision["reasons"] == []
    assert decision["required_changes"] == []
    assert decision["overfit_risk"] == "unknown"
    assert validate_reviewer_decision(decision)["valid"] is True


def test_reviewer_can_downgrade_allow_to_cheap_screen_only():
    decision = normalize_reviewer_decision(
        {"decision": "allow", "overfit_risk": "high", "reasons": ["weak novelty"]}
    )
    assert decision["decision"] == "cheap_screen_only"
    assert decision["manual_review_required"] is False


def test_reviewer_can_downgrade_allow_to_manual_review():
    decision = normalize_reviewer_decision(
        {"decision": "allow", "required_changes": ["external_data_source"]}
    )
    assert decision["decision"] == "manual_review"
    assert decision["manual_review_required"] is True


def test_block_and_manual_review_are_valid_reviewer_decisions():
    assert validate_reviewer_decision({"decision": "block"})["valid"] is True
    assert validate_reviewer_decision({"decision": "manual_review"})["valid"] is True
