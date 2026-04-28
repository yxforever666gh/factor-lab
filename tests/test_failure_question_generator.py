from factor_lab.exploration_pools import NEW_MECHANISM_POOL
from factor_lab.failure_question_generator import build_failure_question_cards


def test_build_failure_question_cards_emits_targeted_cards_from_representative_dossier():
    dossiers = {
        "mom_20": {
            "recommended_action": "diagnose",
            "regime_dependency": "short_window_only",
            "parent_delta_status": "non_incremental",
            "neutralized_break_count": 1,
            "evidence": ["retention weak"],
        }
    }

    cards = build_failure_question_cards(dossiers)
    card_types = {row["question_type"] for row in cards}

    assert "neutralization_collapse" in card_types
    assert "parent_non_incremental" in card_types
    assert "medium_long_persistence" in card_types
    assert all(row["target_pool"] == NEW_MECHANISM_POOL for row in cards)
    parent_card = next(row for row in cards if row["question_type"] == "parent_non_incremental")
    assert parent_card["preferred_context_mode"] == "far_family"
    assert "combine_sub" in parent_card["allowed_operators"]
    assert "orthogonalize_against_peer" not in parent_card["allowed_operators"]
