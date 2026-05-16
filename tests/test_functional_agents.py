import json

from factor_lab.novelty_judge import build_novelty_judgments
from factor_lab.allocator_governance_auditor import build_allocator_governance_audit
from factor_lab.decision_ab_judge import build_decision_ab_report
from factor_lab.failure_analyst_enhancement import build_failure_analyst_enhancement
from factor_lab.au_zero_diagnosis import build_au_zero_diagnosis
from factor_lab.artifact_consistency import build_artifact_consistency_report
from factor_lab.factor_quality_effect_report import build_factor_quality_effect_report
from factor_lab.research_planner import ResearchPlannerAgent
from factor_lab.research_candidate_pool import build_research_candidate_pool
from factor_lab.quality_not_proven_root_cause import build_quality_not_proven_root_cause_report


def test_novelty_judge_classifies_soft_and_hard_duplicate_routes():
    snapshot = {
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "dup_factor",
                    "duplicate_peer_count": 1,
                    "refinement_peer_count": 0,
                    "high_corr_peer_count": 0,
                    "quality_classification": "duplicate-suppress",
                    "quality_scores": {"incremental_value": 2},
                },
                {
                    "factor_name": "new_factor",
                    "duplicate_peer_count": 0,
                    "refinement_peer_count": 0,
                    "high_corr_peer_count": 0,
                    "quality_scores": {"incremental_value": 15},
                    "quality_promotion_decision": "keep_validating",
                },
            ]
        },
        "candidate_failure_dossiers": [
            {"candidate_name": "dup_factor", "parent_delta_status": "non_incremental"},
            {"candidate_name": "new_factor", "parent_delta_status": "incremental"},
        ],
        "approved_universe": {"rows": []},
    }

    payload = build_novelty_judgments(snapshot)
    row_map = {row["candidate_name"]: row for row in payload["rows"]}
    assert row_map["dup_factor"]["novelty_class"] == "duplicate_like_hard"
    assert row_map["dup_factor"]["recommended_action"] == "suppress"
    assert row_map["new_factor"]["novelty_class"] == "meaningful_extension"


def test_novelty_judge_soft_routes_high_corr_incremental_candidates():
    snapshot = {
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "soft_factor",
                    "duplicate_peer_count": 0,
                    "refinement_peer_count": 1,
                    "high_corr_peer_count": 3,
                    "quality_scores": {"incremental_value": 9},
                    "quality_classification": "needs-validation",
                }
            ]
        },
        "candidate_failure_dossiers": [
            {"candidate_name": "soft_factor", "parent_delta_status": "unknown"},
        ],
        "approved_universe": {"rows": []},
    }
    payload = build_novelty_judgments(snapshot)
    row = payload["rows"][0]
    assert row["novelty_class"] == "near_neighbor_soft"
    assert row["soft_route"] is True
    assert row["recommended_action"] == "keep_validating"


def test_allocator_governance_auditor_flags_inconsistent_weight_and_shadow_followup():
    approved_universe = {
        "rows": [
            {
                "factor_name": "alpha_1",
                "lifecycle_state": "approved",
                "governance_action": "keep",
                "allocated_weight": 0.4,
                "max_weight": 0.2,
                "approval_tier": "core",
                "portfolio_bucket": "core_alpha",
            },
            {
                "factor_name": "alpha_2",
                "lifecycle_state": "shadow",
                "governance_action": "keep",
                "allocated_weight": 0.1,
                "max_weight": 0.2,
                "approval_tier": "bridge",
                "portfolio_bucket": "recent_probe",
            },
        ],
        "budget_summary": {"bucket_allocations": {"core_alpha": 0.8}},
        "summary": {"state_counts": {"approved": 1, "shadow": 1}},
    }
    current_portfolio = {"selected_factors": [{"name": "alpha_2"}]}
    payload = build_allocator_governance_audit(approved_universe=approved_universe, current_portfolio=current_portfolio)

    alloc = {row["factor_name"]: row for row in payload["allocation"]["rows"]}
    gov = {row["factor_name"]: row for row in payload["governance"]["rows"]}
    assert alloc["alpha_1"]["allocation_audit"] == "inconsistent"
    assert gov["alpha_2"]["state_transition_audit"] == "suspicious"


def test_decision_ab_judge_recommends_adopt_for_clean_positive_deltas():
    snapshot = {
        "novelty_judge": {"summary": {"class_counts": {"duplicate_like": 0}}},
        "approved_universe_summary": {"approved_count": 2},
        "approved_universe_governance_summary": {"action_counts": {"keep": 2}},
        "approved_universe_budget_summary": {"bucket_allocations": {"core_alpha": 1.0}},
        "promotion_scorecard": {
            "summary": {
                "stable_alpha_candidate_count": 1,
                "needs_validation_count": 1,
                "duplicate_suppress_count": 0,
            }
        },
    }
    report = build_decision_ab_report(snapshot)
    assert report["recommendation"] == "adopt"
    assert report["budget_matched"] is True


def test_failure_analyst_enhancement_emits_reroute_and_stop_recommendations():
    snapshot = {
        "candidate_failure_dossiers": [
            {
                "candidate_name": "dup_factor",
                "recommended_action": "suppress",
                "parent_delta_status": "non_incremental",
                "regime_dependency": "short_window_only",
                "failure_modes": ["short_to_medium_decay"],
            },
            {
                "candidate_name": "fragile_factor",
                "recommended_action": "diagnose",
                "parent_delta_status": "incremental",
                "regime_dependency": "exposure_dependent",
                "failure_modes": ["neutralized_break"],
            },
        ],
        "representative_failure_dossiers": {},
        "failure_question_cards": [
            {"card_id": "q1", "candidate_name": "dup_factor", "priority": 80},
            {"card_id": "q2", "candidate_name": "fragile_factor", "priority": 70},
        ],
    }
    payload = build_failure_analyst_enhancement(snapshot)
    assert payload["summary"]["reroute_count"] >= 1
    stop_map = {row["candidate_name"]: row for row in payload["stop_or_continue_recommendation"]}
    assert stop_map["dup_factor"]["recommendation"] == "stop"
    reroute_map = {row["candidate_name"]: row for row in payload["reroute_proposals"]}
    assert reroute_map["fragile_factor"]["to_route"] == "neutralization_diagnosis"


def test_candidate_pool_uses_failure_analyst_enhancement_to_reroute_validation(tmp_path):
    snapshot_path = tmp_path / "snapshot.json"
    output_path = tmp_path / "candidate_pool.json"
    snapshot = {
        "latest_run": {"config_path": "artifacts/generated_configs/rolling_recent_45d.json", "output_dir": "artifacts/generated_recent_45d", "end_date": "2026-03-20"},
        "generated_configs": [],
        "frontier_focus": {"short_window_candidates": [], "preferred_candidates": [], "robust_candidates": [], "soft_robust_candidates": [], "secondary_candidates": [], "suppressed_candidates": []},
        "latest_graveyard": [],
        "queue_budget": {"baseline": 0, "validation": 0, "exploration": 0},
        "exploration_state": {"should_throttle": False},
        "failure_state": {"cooldown_active": False},
        "candidate_context": [{"candidate_name": "book_yield", "family": "value", "candidate_status": "testing", "cluster": {}}],
        "family_summary": [{"family": "value", "family_score": 80, "recommended_action": "continue"}],
        "cluster_representatives": [],
        "promotion_scorecard": {"rows": []},
        "approved_universe": {"selection_policy_version": "approved-universe-v2", "rows": [{"factor_name": "book_yield", "expression": "book_yield", "lifecycle_state": "approved", "allocated_weight": 0.6}]},
        "approved_universe_names": ["book_yield"],
        "approved_universe_summary": {"approved_count": 1},
        "candidate_failure_dossiers": [],
        "representative_failure_dossiers": {},
        "failure_analyst_enhancement": {
            "stop_or_continue_recommendation": [{"candidate_name": "book_yield", "recommendation": "reroute"}],
            "reroute_proposals": [{"candidate_name": "book_yield", "to_route": "medium_horizon_validation"}],
            "question_cards_v2": [{"candidate_name": "book_yield", "priority": 90}],
        },
        "relationship_summary": {"duplicate_of": 0, "refinement_of": 0, "hybrid_of": 0, "same_family": 0},
        "family_recommendations": [],
        "research_trial_summary": {},
        "analyst_signals": {},
        "recent_research_tasks": [],
        "research_learning": {"representative_candidate_stats": {}},
        "knowledge_gain_counter": {},
    }
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    payload = build_research_candidate_pool(snapshot_path, output_path)
    assert any("failure_analyst reroute" in (row.get("reason") or "") for row in payload["tasks"])


def test_research_planner_uses_novelty_judge_to_penalize_exploration_duplicates():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
        "promotion_scorecard": {"rows": []},
        "novelty_judge": {
            "rows": [
                {
                    "candidate_name": "dup_factor",
                    "novelty_class": "duplicate_like",
                    "recommended_action": "suppress",
                }
            ]
        },
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "workflow",
                "category": "exploration",
                "priority_hint": 20,
                "worker_note": "exploration｜generated_candidate:dup_factor",
                "payload": {"source": "candidate_generation"},
                "focus_candidates": [{"candidate_name": "dup_factor", "evidence_gate": {"action": "frontier_ok"}}],
                "expected_knowledge_gain": ["candidate_survival_check"],
                "reason": "duplicate exploration",
            }
        ]
    }

    result = planner.rank_tasks(snapshot, candidate_pool, None)
    assert "novelty_judge:" in result["selected_tasks"][0]["planner_reason"]


def test_quality_not_proven_root_cause_report_identifies_search_space_issue(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    (artifacts_dir / "factor_quality_effect_report.json").write_text(json.dumps({
        "final_judgment": {"factor_quality": "not_proven"}
    }, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "approved_candidate_universe.json").write_text(json.dumps({"summary": {"approved_count": 1}}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "promotion_scorecard.json").write_text(json.dumps({
        "rows": [{"factor_name": "cand1", "quality_total_score": 80, "quality_promotion_decision": "keep_validating", "quality_classification": "needs-validation", "quality_hard_flags": {"evidence_missing": True}}],
        "summary": {"stable_alpha_candidate_count": 0}
    }, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "novelty_judgments.json").write_text(json.dumps({
        "rows": [{"candidate_name": "cand1", "novelty_class": "near_neighbor_soft", "recommended_action": "keep_validating"}]
    }, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "novelty_judge_summary.json").write_text(json.dumps({"class_counts": {"near_neighbor_soft": 10}}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "novelty_calibration_report.json").write_text(json.dumps({"hard_duplicate_count": 5, "soft_neighbor_count": 10, "meaningful_extension_count": 0}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "decision_ab_report.json").write_text(json.dumps({"recommendation": "reject", "quality_delta": -5.0, "duplicate_delta": 0}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "artifact_consistency_report.json").write_text(json.dumps({"warning_count": 0}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "au_zero_diagnosis.json").write_text(json.dumps({"summary": {"direct_cause": "AU is not zero: approved_count=1"}}, ensure_ascii=False), encoding="utf-8")
    payload = build_quality_not_proven_root_cause_report(artifacts_dir)
    cause_keys = [row["cause_key"] for row in payload["root_causes"]]
    assert "search_space_too_narrow" in cause_keys
    assert payload["next_actions"][0]["action_key"] == "expand_search_space_away_from_neighbors"


def test_au_zero_diagnosis_marks_false_negative_and_effect_report(tmp_path):
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    db_path = artifacts_dir / "factor_lab.db"
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE workflow_runs (run_id TEXT, created_at_utc TEXT, config_path TEXT, output_dir TEXT, status TEXT)")
    run_dir = artifacts_dir / "run1"
    run_dir.mkdir()
    conn.execute("INSERT INTO workflow_runs VALUES (?,?,?,?,?)", ("run-1", "2026-04-21T10:00:00+00:00", "configs/test.json", str(run_dir), "finished"))
    conn.commit()
    conn.close()
    (run_dir / "candidate_status_snapshot.json").write_text(json.dumps([
        {"factor_name": "cand1", "research_stage": "candidate"}
    ], ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "approved_candidate_universe.json").write_text(json.dumps({"rows": [], "summary": {"approved_count": 0}}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "approved_candidate_universe_debug.json").write_text(json.dumps({"rows": [
        {"factor_name": "cand1", "approved": False, "rejection_reasons": ["governance_demotion"]}
    ]}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "promotion_scorecard.json").write_text(json.dumps({"rows": [
        {"factor_name": "cand1", "quality_classification": "needs-validation", "quality_promotion_decision": "keep_validating", "quality_hard_flags": {"evidence_missing": True}}
    ], "summary": {"stable_alpha_candidate_count": 0, "needs_validation_count": 1, "duplicate_suppress_count": 0}}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "novelty_judgments.json").write_text(json.dumps({"rows": [
        {"candidate_name": "cand1", "novelty_class": "meaningful_extension_low_confidence", "recommended_action": "keep_validating"}
    ]}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "approved_candidate_universe_governance.json").write_text(json.dumps({"rows": [
        {"factor_name": "cand1", "governance_action": "demote_candidate", "negative_contribution_streak": 0}
    ]}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "paper_portfolio").mkdir()
    (artifacts_dir / "paper_portfolio" / "portfolio_contribution_report.json").write_text(json.dumps({"rows": []}, ensure_ascii=False), encoding="utf-8")
    diagnosis = build_au_zero_diagnosis(db_path, artifacts_dir)
    assert diagnosis["summary"]["au_count"] == 0
    assert diagnosis["rows"][0]["verdict"] == "possible_false_negative"
    (artifacts_dir / "latest_summary.txt").write_text("Approved Universe：0 个候选，版本 approved-universe-v2，当前入池=无。", encoding="utf-8")
    (artifacts_dir / "research_planner_snapshot.json").write_text(json.dumps({"approved_universe_summary": {"approved_count": 0}}, ensure_ascii=False), encoding="utf-8")
    (artifacts_dir / "decision_ab_report.json").write_text(json.dumps({"generated_at_utc": "2026-04-21T10:00:00+00:00", "recommendation": "keep_testing", "quality_delta": 0.0}, ensure_ascii=False), encoding="utf-8")
    consistency = build_artifact_consistency_report(db_path, artifacts_dir)
    effect = build_factor_quality_effect_report(db_path, artifacts_dir)
    assert isinstance(consistency["warnings"], list)
    assert effect["final_judgment"]["factor_quality"] in {"not_proven", "improved"}


def test_research_planner_uses_failure_analyst_enhancement_to_penalize_exploration():
    planner = ResearchPlannerAgent()
    snapshot = {
        "exploration_state": {},
        "failure_state": {},
        "knowledge_gain_counter": {},
        "analyst_signals": {},
        "promotion_scorecard": {"rows": []},
        "failure_analyst_enhancement": {
            "stop_or_continue_recommendation": [{"candidate_name": "dup_factor", "recommendation": "stop"}],
            "reroute_proposals": [{"candidate_name": "dup_factor", "to_route": "graveyard_diagnosis"}],
        },
    }
    candidate_pool = {
        "tasks": [
            {
                "task_type": "workflow",
                "category": "exploration",
                "priority_hint": 20,
                "worker_note": "exploration｜generated_candidate:dup_factor",
                "payload": {"source": "candidate_generation"},
                "focus_candidates": [{"candidate_name": "dup_factor", "evidence_gate": {"action": "frontier_ok"}}],
                "expected_knowledge_gain": ["candidate_survival_check"],
                "reason": "explore failure-prone factor",
            }
        ]
    }
    result = planner.rank_tasks(snapshot, candidate_pool, None)
    assert "failure_analyst:" in result["selected_tasks"][0]["planner_reason"]
