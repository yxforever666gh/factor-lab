from factor_lab.exploration_budget import build_exploration_budget
from factor_lab.opportunity_policy import allocate_opportunity_budget
from factor_lab.opportunity_scorer import score_opportunity
from factor_lab.research_opportunity_engine import _should_pre_suppress_question


class DummyStore:
    pass


def test_allocate_opportunity_budget_carries_autonomy_epistemic_bias():
    snapshot = {"research_flow_state": {"state": "recovering"}}
    learning = {"types": {}, "families": {}}

    payload = allocate_opportunity_budget(snapshot, learning)

    assert "autonomy_policy" in payload
    assert "regime_context" in payload
    assert payload["budget"]["diagnose"] >= 2
    assert payload["budget"]["probe"] >= 2
    assert sum(payload["bandit_allocations"].values()) == 4


def test_high_epistemic_repeat_question_is_not_pre_suppressed(monkeypatch):
    monkeypatch.setattr(
        "factor_lab.research_opportunity_engine.recently_finished_same_fingerprint",
        lambda *args, **kwargs: True,
    )
    autonomy_policy = {"principles": {"objective": ["epistemic_gain"]}}
    question = {
        "question_id": "q1",
        "question_type": "diagnose",
        "target_family": "graveyard_diagnosis",
        "target_candidates": [],
        "expected_knowledge_gain": ["boundary_confirmed"],
        "sources": ["research_questions"],
    }

    suppressed = _should_pre_suppress_question(question, DummyStore(), autonomy_policy)

    assert suppressed is False


def test_regime_aware_scorer_prefers_diagnose_when_frontier_is_crowded_and_fragile():
    snapshot = {
        "frontier_focus": {
            "preferred_candidates": ["a", "b", "c"],
            "robust_candidates": [],
            "soft_robust_candidates": [],
            "dedupe_candidates": ["a", "b", "c"],
            "regime_sensitive_candidates": [],
            "summary": {"duplicate_suppress_count": 14, "drop_count": 8},
        },
        "relationship_summary": {"duplicate_of": 6},
        "knowledge_gain_counter": {"no_significant_information_gain": 2},
    }
    diagnose = score_opportunity({"question_type": "diagnose", "target_family": "graveyard_diagnosis"}, snapshot)
    expand = score_opportunity({"question_type": "expand", "target_family": "graveyard_diagnosis"}, snapshot)

    assert diagnose["priority"] > expand["priority"]
    assert diagnose["regime"] in {"crowded_frontier", "fragile_frontier"}


def test_allocate_opportunity_budget_bandit_and_regime_push_budget_toward_diagnose_and_probe():
    snapshot = {
        "frontier_focus": {
            "preferred_candidates": ["a", "b", "c"],
            "robust_candidates": [],
            "soft_robust_candidates": [],
            "regime_sensitive_candidates": ["x", "y", "z"],
            "dedupe_candidates": ["a", "b", "c"],
            "summary": {"duplicate_suppress_count": 12, "drop_count": 6, "watchlist_count": 3},
        },
        "relationship_summary": {"duplicate_of": 5},
        "queue_budget": {"exploration": 1, "validation": 0, "baseline": 0},
    }
    learning = {
        "types": {
            "diagnose": {"count": 2, "success_rate": 0.6, "epistemic_value_score": 0.8, "recommended_action": "upweight"},
            "probe": {"count": 1, "success_rate": 0.4, "epistemic_value_score": 0.7, "recommended_action": "upweight"},
            "expand": {"count": 8, "success_rate": 0.1, "epistemic_value_score": -0.3, "recommended_action": "downweight"},
            "confirm": {"count": 6, "success_rate": 0.2, "epistemic_value_score": -0.2, "recommended_action": "downweight"},
        },
        "families": {},
    }

    payload = allocate_opportunity_budget(snapshot, learning)

    assert payload["budget"]["diagnose"] >= payload["budget"]["expand"]
    assert payload["budget"]["probe"] >= 2
    assert payload["bandit_scores"]["diagnose"] > payload["bandit_scores"]["expand"]


def test_allocate_opportunity_budget_uses_representative_failure_dossiers_to_bias_diagnose():
    snapshot = {
        "research_flow_state": {"state": "ready"},
        "representative_failure_dossiers": {
            "rep_a": {
                "recommended_action": "diagnose",
                "regime_dependency": "short_window_only",
                "parent_delta_status": "non_incremental",
            }
        },
    }
    learning = {"types": {}, "families": {}}

    payload = allocate_opportunity_budget(snapshot, learning)

    assert payload["budget"]["diagnose"] >= 3
    assert payload["representative_failure_summary"]["diagnose_count"] == 1
    assert "representative_failure_bias_to_diagnose" in payload["reasons"]



def test_allocate_opportunity_budget_shifts_from_low_yield_exploration_to_validation():
    snapshot = {"research_flow_state": {"state": "recovered"}}
    learning = {
        "types": {
            "expand": {"recent_no_gain_count": 2, "recent_gain_count": 0, "recent_resource_exhaustion_count": 1, "cooldown_active": True, "recommended_action": "downweight"},
            "recombine": {"recent_no_gain_count": 2, "recent_gain_count": 0, "recent_resource_exhaustion_count": 1, "cooldown_active": True, "recommended_action": "downweight"},
            "probe": {"recent_no_gain_count": 1, "recent_gain_count": 0, "recent_resource_exhaustion_count": 0, "cooldown_active": False, "recommended_action": "keep"},
        },
        "families": {},
    }

    payload = allocate_opportunity_budget(snapshot, learning)

    assert payload["budget"]["diagnose"] >= payload["budget"]["expand"]
    assert payload["budget"]["confirm"] >= 2
    assert "recent_low_yield_exploration_shift_to_validation" in payload["reasons"]
    assert payload["exploration_pressure"]["recent_resource_exhaustion_count"] >= 2


def test_exploration_budget_inherits_regime_bias_for_fragile_frontier():
    snapshot = {
        "frontier_focus": {
            "preferred_candidates": ["a", "b", "c"],
            "robust_candidates": [],
            "soft_robust_candidates": [],
            "dedupe_candidates": ["a", "b", "c"],
            "summary": {"duplicate_suppress_count": 12},
        },
        "relationship_summary": {"duplicate_of": 4},
    }

    payload = build_exploration_budget(snapshot)

    assert payload["regime_context"]["regime"] in {"crowded_frontier", "fragile_frontier"}
    assert payload["budget"]["diagnose"] >= payload["budget"]["expand"]
