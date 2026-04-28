from factor_lab.candidate_generator import _operators_for


def test_operators_for_prefers_mapped_operators_first():
    policy = {
        "enabled_operators": ["combine_add", "combine_sub", "combine_ratio", "combine_primary_bias"],
        "failure_reason_operator_map": {
            "boundary_confirmed": ["combine_primary_bias", "combine_add"]
        },
    }

    ops = _operators_for(["boundary_confirmed"], policy)

    assert ops[:2] == ["combine_primary_bias", "combine_add"]


def test_operators_for_respects_operator_learning_feedback():
    policy = {
        "enabled_operators": ["combine_add", "combine_sub", "combine_ratio"],
        "failure_reason_operator_map": {},
    }
    operator_stats = {
        "combine_ratio": {"recommended_action": "upweight"},
        "combine_add": {"recommended_action": "downweight"},
    }

    ops = _operators_for([], policy, operator_stats)

    assert ops[0] == "combine_ratio"
    assert ops[-1] == "combine_add"


def test_operators_for_prefers_family_operator_feedback_over_global():
    policy = {
        "enabled_operators": ["combine_add", "combine_sub", "combine_ratio"],
        "failure_reason_operator_map": {},
    }
    operator_stats = {"combine_ratio": {"recommended_action": "upweight"}}
    family_operator_stats = {"momentum": {"combine_sub": {"recommended_action": "upweight"}}}

    ops = _operators_for([], policy, operator_stats, target_family="momentum", family_operator_stats=family_operator_stats)

    assert ops[0] == "combine_sub"


def test_operators_for_prefers_source_specific_operator_order():
    policy = {
        "enabled_operators": ["combine_add", "combine_sub", "residualize_against_peer", "orthogonalize_against_peer"],
        "failure_reason_operator_map": {},
        "source_operator_preferences": {
            "stable_plus_graveyard": ["residualize_against_peer", "orthogonalize_against_peer"]
        },
    }

    ops = _operators_for([], policy, {}, source="stable_plus_graveyard")

    assert ops[:2] == ["residualize_against_peer", "orthogonalize_against_peer"]
