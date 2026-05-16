import json
from pathlib import Path

from factor_lab.opportunity_diagnostics import build_opportunity_review
from factor_lab.opportunity_policy import should_bypass_recent_fingerprint
from factor_lab.opportunity_scorer import score_opportunity
from factor_lab.research_flow_state import derive_research_flow_state
from factor_lab.research_runtime_state import task_repeat_cooldown_minutes


def test_task_repeat_cooldown_shortens_for_diagnostic_and_high_epistemic(monkeypatch):
    monkeypatch.delenv("RESEARCH_TASK_REPEAT_COOLDOWN_DIAGNOSTIC_MINUTES", raising=False)
    cooldown = task_repeat_cooldown_minutes(
        task_type="diagnostic",
        payload={"expected_information_gain": ["boundary_confirmed"]},
    )

    assert cooldown == 30


def test_build_opportunity_review_soft_allows_high_epistemic_low_confidence(tmp_path, monkeypatch):
    monkeypatch.delenv("RESEARCH_OPPORTUNITY_HARD_BLOCK_CONFIDENCE", raising=False)
    monkeypatch.delenv("RESEARCH_OPPORTUNITY_REVIEW_CONFIDENCE", raising=False)
    monkeypatch.setattr("factor_lab.opportunity_diagnostics.OPPORTUNITY_LEARNING_PATH", tmp_path / "opportunity_learning.json")
    (tmp_path / "opportunity_learning.json").write_text(json.dumps({}, ensure_ascii=False), encoding="utf-8")
    store_path = tmp_path / "research_opportunity_store.json"
    output_path = tmp_path / "opportunity_review.json"
    store_path.write_text(
        json.dumps(
            {
                "opportunities": {
                    "opp-1": {
                        "opportunity_id": "opp-1",
                        "opportunity_type": "probe",
                        "priority": 0.81,
                        "novelty_score": 0.66,
                        "confidence": 0.55,
                        "expected_knowledge_gain": ["boundary_confirmed"],
                        "state": "proposed",
                    }
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    review = build_opportunity_review(store_path=store_path, output_path=output_path)

    assert "opp-1" not in review["blocks"]
    assert review["downweights"]["opp-1"]["reason"] == "soft_low_confidence"


def test_build_opportunity_review_blocks_low_confidence_cooldown_pattern(tmp_path, monkeypatch):
    monkeypatch.setattr("factor_lab.opportunity_diagnostics.OPPORTUNITY_LEARNING_PATH", tmp_path / "opportunity_learning.json")
    (tmp_path / "opportunity_learning.json").write_text(
        json.dumps(
            {
                "families": {},
                "templates": {
                    "probe::graveyard_diagnosis::root": {
                        "cooldown_active": True,
                        "cooldown_reason": "repeated_no_gain",
                    }
                },
                "patterns": {},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    store_path = tmp_path / "research_opportunity_store.json"
    output_path = tmp_path / "opportunity_review.json"
    store_path.write_text(
        json.dumps(
            {
                "opportunities": {
                    "opp-1": {
                        "opportunity_id": "opp-1",
                        "opportunity_type": "probe",
                        "target_family": "graveyard_diagnosis",
                        "priority": 0.45,
                        "novelty_score": 0.51,
                        "confidence": 0.56,
                        "expected_knowledge_gain": [],
                        "state": "proposed",
                    }
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    review = build_opportunity_review(store_path=store_path, output_path=output_path)

    assert review["blocks"]["opp-1"]["reason"] == "repeated_no_gain"


def test_score_opportunity_penalizes_cooldown_pattern():
    base_snapshot = {"research_learning": {}, "research_flow_state": {"state": "recovered"}}
    cooled_snapshot = {
        "research_learning": {
            "templates": {
                "recombine::graveyard_diagnosis::root": {
                    "cooldown_active": True,
                    "cooldown_reason": "resource_exhaustion",
                    "recent_resource_exhaustion_count": 2,
                }
            },
            "patterns": {
                "recombine::graveyard_diagnosis::root::pair_target::exploration_candidate_survived::pre_eval": {
                    "cooldown_active": True,
                    "cooldown_reason": "repeated_no_gain",
                    "recent_no_gain_count": 3,
                    "recent_gain_count": 0,
                }
            },
        },
        "research_flow_state": {"state": "recovered"},
    }
    question = {
        "question_type": "recombine",
        "target_family": "graveyard_diagnosis",
        "target_candidates": ["mom_20", "value_ep"],
        "expected_knowledge_gain": ["exploration_candidate_survived"],
    }

    cooled = score_opportunity(question, cooled_snapshot)
    base = score_opportunity(question, base_snapshot)

    assert cooled["priority"] < base["priority"]
    assert "cooldown" in cooled["score_rationale"]


def test_should_bypass_recent_fingerprint_for_high_epistemic_probe():
    payload = should_bypass_recent_fingerprint(
        {
            "opportunity_type": "probe",
            "priority": 0.8,
            "novelty_score": 0.62,
            "confidence": 0.55,
            "expected_knowledge_gain": ["new_branch_opened"],
        }
    )

    assert payload["allow_bypass"] is True
    assert payload["reason"] == "high_epistemic_gain_override"


def test_derive_research_flow_state_soft_lands_to_recovered(tmp_path, monkeypatch):
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    (artifacts / "research_memory.json").write_text(
        json.dumps(
            {
                "fallback_history": [
                    {"branch_id": "fallback_stable_candidate_validation", "has_gain": True}
                ],
                "branch_lifecycle": {
                    "fallback_stable_candidate_validation": {"state": "validating"}
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("factor_lab.research_flow_state.ARTIFACTS", artifacts)

    payload = derive_research_flow_state(
        snapshot={"queue_counts": {"pending": 0, "running": 0, "finished": 12, "failed": 0}},
        candidate_pool={"tasks": [{"id": "cand-1"}]},
        recovery_used=False,
        injected_count=0,
    )

    assert payload["state"] == "recovered"
    assert "recovery_branches_stable_under_load" in payload["reasons"]
