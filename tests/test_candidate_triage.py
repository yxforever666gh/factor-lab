import json

from factor_lab.candidate_triage import build_candidate_triage_model, score_generation_proposal


def test_candidate_triage_model_learns_empirical_operator_and_source_rates():
    memory = {
        "generated_candidate_outcomes": [
            {"source": "hypothesis_template", "operator": "combine_sub", "target_family": "quality", "outcome_class": "high_value_success"},
            {"source": "hypothesis_template", "operator": "combine_sub", "target_family": "quality", "outcome_class": "useful_success"},
            {"source": "hypothesis_template", "operator": "combine_sub", "target_family": "quality", "outcome_class": "high_value_failure"},
            {"source": "stable_plus_graveyard", "operator": "combine_add", "target_family": "momentum", "outcome_class": "ordinary_failure"},
        ]
    }

    model = build_candidate_triage_model(memory)

    assert model["source_success"]["hypothesis_template"]["success_rate"] > model["prior_success_rate"]
    assert model["operator_success"]["combine_sub"]["success_rate"] > model["prior_success_rate"]


def test_candidate_triage_scores_cross_family_template_higher_than_crowded_repeat():
    snapshot = {
        "frontier_focus": {"preferred_candidates": ["mom_20", "mom_plus_value"]},
    }
    factor_context = {
        "quality_roe": {"family": "quality", "relationship_count": 1},
        "size_small": {"family": "other", "relationship_count": 1},
        "mom_20": {"family": "momentum", "relationship_count": 11},
        "book_yield": {"family": "value", "relationship_count": 10},
    }
    model = {
        "prior_success_rate": 0.4,
        "minimum_samples": 1,
        "source_success": {
            "hypothesis_template": {"success_rate": 0.7, "total": 4},
            "stable_plus_graveyard": {"success_rate": 0.2, "total": 4},
        },
        "operator_success": {
            "combine_sub": {"success_rate": 0.65, "total": 4},
            "combine_add": {"success_rate": 0.2, "total": 4},
        },
        "family_success": {
            "quality": {"success_rate": 0.68, "total": 4},
            "momentum": {"success_rate": 0.25, "total": 4},
        },
    }
    good = {
        "candidate_id": "gen__combine_sub__quality_roe__size_small",
        "base_factors": ["quality_roe", "size_small"],
        "operator": "combine_sub",
        "target_family": "quality",
        "source": "hypothesis_template",
        "expected_information_gain": ["new_branch_opened", "candidate_survival_check"],
        "cheap_screen": {"score": 0.82},
    }
    bad = {
        "candidate_id": "gen__combine_add__mom_20__book_yield",
        "base_factors": ["mom_20", "book_yield"],
        "operator": "combine_add",
        "target_family": "momentum",
        "source": "stable_plus_graveyard",
        "expected_information_gain": ["candidate_survival_check"],
        "cheap_screen": {"score": 0.56},
    }

    good_score = score_generation_proposal(good, snapshot=snapshot, factor_context=factor_context, model=model)
    bad_score = score_generation_proposal(bad, snapshot=snapshot, factor_context=factor_context, model=model)

    assert good_score["score"] > bad_score["score"]
    assert good_score["label"] in {"medium", "high"}
