import json
from pathlib import Path

from factor_lab.research_candidate_pool import build_research_candidate_pool


def test_candidate_pool_emits_approved_universe_followup_task(tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    output_path = tmp_path / "candidate_pool.json"

    snapshot = {
        "latest_run": {
            "config_path": "artifacts/generated_configs/rolling_recent_45d.json",
            "output_dir": "artifacts/generated_recent_45d",
            "end_date": "2026-03-20",
        },
        "generated_configs": [],
        "frontier_focus": {
            "short_window_candidates": [],
            "preferred_candidates": [],
            "robust_candidates": [],
            "soft_robust_candidates": [],
            "secondary_candidates": [],
            "suppressed_candidates": [],
        },
        "latest_graveyard": [],
        "queue_budget": {"baseline": 0, "validation": 0, "exploration": 0},
        "exploration_state": {"should_throttle": False},
        "failure_state": {"cooldown_active": False},
        "candidate_context": [
            {
                "candidate_id": "c1",
                "candidate_name": "book_yield",
                "family": "value",
                "candidate_status": "testing",
                "relationship_count": 2,
                "lineage_count": 1,
                "family_score": 80,
                "family_recommended_action": "continue",
                "cluster": {},
            }
        ],
        "family_summary": [{"family": "value", "family_score": 80, "recommended_action": "continue"}],
        "cluster_representatives": [
            {
                "representative_candidate": "book_yield",
                "primary_candidate": "book_yield",
                "representative_candidates": ["book_yield"],
                "cluster_members": ["book_yield"],
                "representative_rank": 1,
                "representative_count": 1,
                "is_representative": True,
                "is_primary_representative": True,
            }
        ],
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "book_yield",
                    "quality_total_score": 82,
                    "quality_classification": "needs-validation",
                    "quality_classification_label": "继续验证",
                    "quality_promotion_decision": "keep_validating",
                    "quality_summary": "book_yield needs validation",
                    "approved_universe_member": True,
                    "approved_universe_reason": "scorecard_quality_gate",
                    "quality_scores": {"incremental_value": 12},
                    "candidate_status": "testing",
                }
            ]
        },
        "approved_universe": {
            "selection_policy_version": "approved-universe-v1",
            "rows": [
                {
                    "factor_name": "book_yield",
                    "expression": "book_yield",
                    "approved_reason": "scorecard_quality_gate",
                    "lifecycle_state": "approved",
                    "allocated_weight": 0.6,
                }
            ],
        },
        "approved_universe_names": ["book_yield"],
        "approved_universe_summary": {"approved_count": 1},
        "candidate_failure_dossiers": [
            {
                "candidate_name": "book_yield",
                "recommended_action": "keep_validating",
                "regime_dependency": "cross_window_supported",
                "parent_delta_status": "incremental",
                "failure_modes": [],
            }
        ],
        "representative_failure_dossiers": {},
        "relationship_summary": {"duplicate_of": 0, "refinement_of": 0, "hybrid_of": 0, "same_family": 0},
        "family_recommendations": [{"family": "value", "family_risk_score": 20, "recommended_action": "continue"}],
        "research_trial_summary": {},
        "analyst_signals": {},
        "recent_research_tasks": [],
        "research_learning": {"representative_candidate_stats": {}},
        "knowledge_gain_counter": {},
    }
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")

    payload = build_research_candidate_pool(snapshot_path, output_path)

    assert payload["summary"]["approved_universe_names"] == ["book_yield"]
    approved_tasks = [row for row in payload["tasks"] if row.get("approved_universe_alignment")]
    assert approved_tasks
    assert approved_tasks[0]["approved_universe_alignment"]["focus_overlap"] == ["book_yield"]
    assert approved_tasks[0]["approved_universe_alignment"]["state_summary"] == ["approved"]
    assert approved_tasks[0]["approved_universe_alignment"]["budget_weight"] == 0.6


def test_candidate_pool_emits_borderline_targeted_validation_task(tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    output_path = tmp_path / "candidate_pool.json"
    snapshot = {
        "latest_run": {
            "config_path": "artifacts/generated_configs/rolling_recent_45d.json",
            "output_dir": "artifacts/generated_recent_45d",
            "end_date": "2026-03-20",
        },
        "generated_configs": [],
        "frontier_focus": {
            "short_window_candidates": [],
            "preferred_candidates": [],
            "robust_candidates": [],
            "soft_robust_candidates": [],
            "secondary_candidates": [],
            "suppressed_candidates": [],
        },
        "latest_graveyard": [],
        "queue_budget": {"baseline": 0, "validation": 0, "exploration": 0},
        "exploration_state": {"should_throttle": False},
        "failure_state": {"cooldown_active": False},
        "candidate_context": [
            {"candidate_name": "quality_roe", "family": "quality", "candidate_status": "testing", "cluster": {}},
        ],
        "family_summary": [{"family": "quality", "family_score": 80, "recommended_action": "continue"}],
        "cluster_representatives": [],
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "quality_roe",
                    "quality_total_score": 49,
                    "quality_classification": "duplicate-suppress",
                    "quality_promotion_decision": "suppress",
                    "quality_scores": {"incremental_value": 11},
                    "quality_hard_flags": {"duplicate_risk": True, "evidence_missing": True},
                }
            ]
        },
        "approved_universe": {"rows": []},
        "approved_universe_names": [],
        "approved_universe_summary": {},
        "candidate_failure_dossiers": [
            {"candidate_name": "quality_roe", "recommended_action": "keep_validating", "parent_delta_status": "unknown", "regime_dependency": "unclear", "failure_modes": []},
        ],
        "novelty_judge": {"rows": [{"candidate_name": "quality_roe", "novelty_class": "near_neighbor_soft", "recommended_action": "keep_validating"}]},
        "representative_failure_dossiers": {},
        "relationship_summary": {"duplicate_of": 0, "refinement_of": 0, "hybrid_of": 0, "same_family": 0},
        "family_recommendations": [{"family": "quality", "family_risk_score": 20, "recommended_action": "continue"}],
        "research_trial_summary": {},
        "analyst_signals": {},
        "recent_research_tasks": [],
        "research_learning": {"representative_candidate_stats": {}},
        "knowledge_gain_counter": {},
    }
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    payload = build_research_candidate_pool(snapshot_path, output_path)
    assert any((row.get("payload") or {}).get("diagnostic_type") == "borderline_candidate_targeted_validation" for row in payload["tasks"])
