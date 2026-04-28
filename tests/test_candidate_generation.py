import json
from pathlib import Path

from factor_lab import candidate_generator as candidate_generator_module
from factor_lab.candidate_compiler import compile_candidate_generation_plan
from factor_lab.candidate_generator import build_candidate_generation_plan


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_candidate_generation_plan_produces_proposals_with_cheap_screen(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 2, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 2},
        ],
    }
    memory = {
        "execution_feedback": [
            {"outcome_class": "high_value_failure", "focus_candidates": ["book_yield"]}
        ]
    }
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"
    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["policy_name"] == "openclaw_candidate_generation_policy"
    assert len(payload["proposals"]) >= 1
    assert payload["quality_throttle"]["quality_priority_mode"] is False
    assert "cheap_screen" in payload["proposals"][0]
    assert "triage" in payload["proposals"][0]


def test_candidate_generation_emits_hypothesis_template_proposal(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": [],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 2, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "liquidity_turnover_shock", "family": "liquidity", "relationship_count": 1},
        ],
        "research_flow_state": {"state": "ready"},
        "failure_state": {"cooldown_active": False},
    }
    memory = {"execution_feedback": []}
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"
    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert any(row["source"] == "hypothesis_template" for row in payload["proposals"])
    assert any(row.get("hypothesis_template_id") == "liquidity_shock_reversal" for row in payload["proposals"])



def test_candidate_generation_cools_down_base_pair_after_repeated_failures(tmp_path):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 1},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 1},
        ],
    }
    memory = {
        "execution_feedback": [],
        "generated_candidate_outcomes": [
            {"base_factors": ["mom_20", "book_yield"], "operator": "combine_add", "outcome_class": "high_value_failure"},
            {"base_factors": ["mom_20", "book_yield"], "operator": "combine_sub", "outcome_class": "high_value_failure"}
        ],
    }
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert not any(row["base_factors"] == ["mom_20", "book_yield"] for row in payload["proposals"])



def test_candidate_generation_prefers_family_gap_seed_for_new_branch(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": [],
        "family_summary": [
            {"family": "momentum", "representative_count": 1, "duplicate_pressure": 8, "avg_latest_score": 2.0, "family_score": 35.0, "recommended_action": "refine"},
            {"family": "quality", "representative_count": 0, "duplicate_pressure": 0, "avg_latest_score": 6.8, "family_score": 28.0, "recommended_action": "explore_new_branch"},
        ],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 2, "acceptance_gate": {"status": "pass"}, "is_primary_candidate": True, "robustness_score": 0.7},
            {"candidate_name": "quality_roe", "family": "quality", "relationship_count": 1, "is_primary_candidate": True, "robustness_score": 0.6},
        ],
    }
    memory = {"execution_feedback": []}
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"
    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert "quality_roe" in payload["quality_throttle"]["family_gap_seeds"]



def test_candidate_generation_quality_throttle_preserves_exploration_floor_without_true_fault(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "frontier_focus": {"robust_candidates": ["mom_20"]},
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 2, "acceptance_gate": {}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 2},
        ],
        "research_flow_state": {"state": "ready"},
        "failure_state": {"cooldown_active": False},
    }
    memory = {"execution_feedback": []}
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"

    learning_path.write_text(json.dumps({"research_mode": {"mode": "diagnosis_heavy"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["quality_throttle"]["quality_priority_mode"] is True
    assert payload["quality_throttle"]["severe_quality_hold"] is False
    assert payload["quality_throttle"]["exploration_floor"]["exploration_floor_slots"] == 2
    assert len(payload["proposals"]) >= 1



def test_candidate_generation_front_gate_and_dossier_bias_reduce_old_space_pairs(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "frontier_focus": {"robust_candidates": ["mom_20"]},
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 2, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 2},
            {"candidate_name": "liquidity_turnover_shock", "family": "liquidity", "relationship_count": 1},
        ],
        "promotion_scorecard": {
            "rows": [
                {
                    "factor_name": "mom_20",
                    "quality_classification": "needs-validation",
                    "retention_industry": 0.1,
                    "net_metric": -0.2,
                    "latest_recent_final_score": 1.1,
                }
            ]
        },
        "representative_failure_dossiers": {
            "mom_20": {
                "recommended_action": "diagnose",
                "regime_dependency": "short_window_only",
                "parent_delta_status": "non_incremental",
            }
        },
        "research_flow_state": {"state": "ready"},
        "failure_state": {"cooldown_active": False},
    }
    memory = {"execution_feedback": []}
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"

    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert "mom_20" in payload["quality_throttle"]["front_gate_blocked_candidates"]
    assert payload["quality_throttle"]["new_mechanism_bias"] is True
    assert not any(row["base_factors"] == ["mom_20", "book_yield"] for row in payload["proposals"])



def test_candidate_generation_true_fault_recovery_can_hold_new_proposals(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "frontier_focus": {"robust_candidates": ["mom_20"]},
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 2, "acceptance_gate": {}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 2},
        ],
        "research_flow_state": {"state": "recovering"},
        "failure_state": {"cooldown_active": True},
    }
    memory = {"execution_feedback": []}
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"

    learning_path.write_text(json.dumps({"research_mode": {"mode": "diagnosis_heavy"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["quality_throttle"]["severe_quality_hold"] is True
    assert payload["quality_throttle"]["exploration_floor"]["exploration_floor_slots"] == 0
    assert payload["proposals"] == []


def test_candidate_generation_relaxed_regime_admission_can_reach_later_templates(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [],
        "latest_graveyard": ["book_yield"],
        "frontier_focus": {
            "preferred_candidates": ["mom_20", "quality_roe", "mom_plus_value"],
            "short_window_candidates": ["mom_20", "quality_roe", "mom_plus_value"],
            "dedupe_candidates": ["mom_20", "quality_roe", "mom_plus_value"],
            "robust_candidates": [],
            "soft_robust_candidates": [],
            "summary": {"duplicate_suppress_count": 12},
        },
        "relationship_summary": {"duplicate_of": 10, "refinement_of": 30},
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 3},
            {"candidate_name": "quality_roe", "family": "quality", "relationship_count": 2},
            {"candidate_name": "mom_plus_value", "family": "hybrid", "relationship_count": 3},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 2},
            {"candidate_name": "liquidity_turnover_shock", "family": "liquidity", "relationship_count": 1},
            {"candidate_name": "size_small", "family": "size", "relationship_count": 1},
            {"candidate_name": "value_ep", "family": "value", "relationship_count": 1},
            {"candidate_name": "book_yield_plus_earnings_yield", "family": "value", "relationship_count": 1},
        ],
    }
    memory = {
        "execution_feedback": [],
        "generated_candidate_outcomes": [
            {"base_factors": ["mom_20", "liquidity_turnover_shock"], "operator": "combine_sub", "outcome_class": "high_value_failure"},
            {"base_factors": ["mom_20", "liquidity_turnover_shock"], "operator": "combine_add", "outcome_class": "high_value_failure"},
            {"base_factors": ["quality_roe", "size_small"], "operator": "combine_sub", "outcome_class": "high_value_failure"},
            {"base_factors": ["quality_roe", "size_small"], "operator": "combine_add", "outcome_class": "high_value_failure"},
        ],
    }
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"

    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["quality_throttle"]["relaxed_admission"] is True
    assert payload["quality_throttle"]["candidate_floor"] >= 2
    assert len(payload["proposals"]) >= 2
    assert any(row.get("source") == "hypothesis_template" for row in payload["proposals"])
    assert any(row.get("base_factors") not in (["mom_20", "liquidity_turnover_shock"], ["quality_roe", "size_small"]) for row in payload["proposals"])


def test_candidate_generation_tolerates_corrupted_memory_file(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 1, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 1},
        ],
    }
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"

    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text('{"generated_candidate_outcomes": ["broken"', encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["proposals"]


def test_candidate_compiler_supports_primitive_library_factors(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "proposals": [
                    {
                        "candidate_id": "gen__combine_sub__quality_roe__size_small",
                        "base_factors": ["quality_roe", "size_small"],
                        "operator": "combine_sub",
                        "target_family": "quality",
                        "rationale": "template proposal",
                        "expected_information_gain": ["new_branch_opened"],
                        "cheap_screen": {"pass": True, "score": 0.8},
                        "hypothesis_template_id": "small_cap_quality_activation"
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    tasks = compile_candidate_generation_plan(plan_path)

    assert len(tasks) == 1
    assert tasks[0]["payload"]["candidate_generation_context"]["hypothesis_template_id"] == "small_cap_quality_activation"
    assert tasks[0]["payload"]["triage"]["label"] in {"low", "medium", "high"}



def test_candidate_generation_failure_questions_feed_new_mechanism_pool(tmp_path, monkeypatch):
    snapshot = {
        "stable_candidates": [{"factor_name": "mom_20"}],
        "latest_graveyard": ["book_yield"],
        "candidate_context": [
            {"candidate_name": "mom_20", "family": "momentum", "relationship_count": 1, "acceptance_gate": {"status": "pass"}},
            {"candidate_name": "book_yield", "family": "value", "relationship_count": 1},
            {"candidate_name": "quality_roe", "family": "quality", "relationship_count": 1},
        ],
        "failure_question_cards": [
            {
                "card_id": "question::mom_20::parent_non_incremental",
                "candidate_name": "mom_20",
                "question_type": "parent_non_incremental",
                "prompt": "探索更远的跨 family 增量机制",
                "route_bias": "far_family_incremental",
                "expected_information_gain": ["new_branch_opened"],
                "target_pool": "new_mechanism_exploration",
                "priority": 95,
            }
        ],
    }
    memory = {"execution_feedback": []}
    learning_path = tmp_path / "research_learning.json"
    evidence_path = tmp_path / "research_evidence_policy.json"
    snapshot_path = tmp_path / "snapshot.json"
    memory_path = tmp_path / "memory.json"
    output_path = tmp_path / "plan.json"

    learning_path.write_text(json.dumps({"research_mode": {"mode": "balanced"}}, ensure_ascii=False), encoding="utf-8")
    evidence_path.write_text(
        json.dumps(
            {
                "frontier_gate": {
                    "pass_statuses": ["pass"],
                    "validation_statuses": ["monitor", "blocked"],
                    "missing_status": "missing",
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    memory_path.write_text(json.dumps(memory, ensure_ascii=False), encoding="utf-8")

    monkeypatch.setattr(candidate_generator_module, "LEARNING_PATH", learning_path)
    monkeypatch.setattr(candidate_generator_module, "EVIDENCE_POLICY_PATH", evidence_path)

    payload = build_candidate_generation_plan(snapshot_path, memory_path, output_path)

    assert payload["quality_throttle"]["pool_budgets"]["new_mechanism_exploration"] >= 1
    assert any(row.get("question_card_id") == "question::mom_20::parent_non_incremental" for row in payload["proposals"])
    assert any(row.get("exploration_pool") == "new_mechanism_exploration" for row in payload["proposals"])
    question_proposals = [row for row in payload["proposals"] if row.get("question_card_id") == "question::mom_20::parent_non_incremental"]
    assert question_proposals
    assert all(row.get("operator") not in {"orthogonalize_against_peer", "residualize_against_peer"} for row in question_proposals)



def test_candidate_compiler_emits_workflow_tasks_only_for_passed_proposals(tmp_path):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "proposals": [
                    {
                        "candidate_id": "gen__combine_primary_bias__mom_20__book_yield",
                        "base_factors": ["mom_20", "book_yield"],
                        "operator": "combine_primary_bias",
                        "target_family": "momentum",
                        "rationale": "test proposal",
                        "expected_information_gain": ["candidate_survival_check"],
                        "cheap_screen": {"pass": True, "score": 0.8},
                    },
                    {
                        "candidate_id": "gen__combine_sub__mom_20__book_yield",
                        "base_factors": ["mom_20", "book_yield"],
                        "operator": "combine_sub",
                        "target_family": "momentum",
                        "rationale": "rejected proposal",
                        "expected_information_gain": ["candidate_survival_check"],
                        "cheap_screen": {"pass": False, "score": 0.2},
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    tasks = compile_candidate_generation_plan(plan_path)

    assert len(tasks) == 1
    assert tasks[0]["task_type"] == "workflow"
    assert tasks[0]["payload"]["source"] == "candidate_generation"
    assert tasks[0]["payload"]["candidate_generation_context"]["operator"] == "combine_primary_bias"
