from factor_lab.harvest_planner import build_harvest_cycle_plan
from factor_lab.harvest_reviewer_decision import normalize_reviewer_decision
from factor_lab.harvest_gate import check_harvest_gate


def plan():
    return build_harvest_cycle_plan({"promoted_bucket_aware_routes": ["value_quality_no_distress"]})


def test_gate_allows_dry_run_by_default():
    d = check_harvest_gate(plan(), reviewer_decision=normalize_reviewer_decision({"decision": "allow"}))
    assert d["decision"] == "allow_dry_run"
    assert d["allowed_experiments"]


def test_gate_allows_controlled_execution_with_explicit_flag():
    d = check_harvest_gate(plan(), reviewer_decision={"decision": "allow"}, allow_controlled_execution=True)
    assert d["decision"] == "allow_controlled_execution"


def test_gate_blocks_unsupported_mainline_missing_fields_and_duplicates():
    p = plan()
    p["cycle_charter"]["mainline"] = "arbitrary_factor_search"
    p["proposals"][0]["required_fields"].append("not_available")
    d = check_harvest_gate(p, reviewer_decision={"decision": "allow"}, recent_experiment_ids=[p["proposals"][0]["proposal_id"]])
    assert d["decision"] == "block"
    assert "unsupported_mainline" in d["reasons"]
    assert "missing_required_fields" in d["reasons"]
    assert "duplicate_equivalent_experiment" in d["reasons"]


def test_gate_blocks_legacy_broad_path_and_live_promotion():
    p = plan()
    p["proposals"][0]["output_path"] = "artifacts/generated/broad/run"
    p["proposals"][0]["paper_portfolio_promotion"] = True
    d = check_harvest_gate(p, reviewer_decision={"decision": "allow"})
    assert d["decision"] == "block"
    assert "legacy_broad_path_requested" in d["reasons"]
    assert "paper_portfolio_promotion_requested" in d["reasons"]


def test_gate_respects_reviewer_block_manual_review_and_cheap_screen():
    assert check_harvest_gate(plan(), reviewer_decision={"decision": "block"})["decision"] == "block"
    assert check_harvest_gate(plan(), reviewer_decision={"decision": "manual_review"})["decision"] == "manual_review"
    assert check_harvest_gate(plan(), reviewer_decision={"decision": "cheap_screen_only"})["decision"] == "cheap_screen_only"


def test_gate_blocks_missing_proposal_quality_fields():
    p = plan()
    del p["proposals"][0]["mechanism_id"]
    p["proposals"][0]["falsification_criteria"] = []
    d = check_harvest_gate(p, reviewer_decision={"decision": "allow"})
    assert d["decision"] == "block"
    assert "missing_mechanism_id" in d["reasons"]
    assert "missing_falsification_criteria" in d["reasons"]
