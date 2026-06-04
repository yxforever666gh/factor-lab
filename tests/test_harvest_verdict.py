from factor_lab.harvest_verdict import build_verdict


def _ledger(row):
    return {"cycle_id": "cycle_0001", "evidence": [row]}


def test_promote_to_manual_review_for_eligible_candidate():
    v = build_verdict(_ledger({"experiment_id": "e1", "mechanism_id": "m1", "promotion_eligible": True, "soft_score_average": 4.2, "failure_class": None, "information_gain": "positive_progress"}))
    assert v["decision"] == "promote_to_manual_review"
    assert v["manual_approval_required"] is True
    assert v["promoted_candidates"] == ["m1"]


def test_stop_on_consecutive_no_information_gain():
    row = {"experiment_id": "e1", "mechanism_id": "m1", "promotion_eligible": False, "failure_class": "duplicate_equivalent_experiment", "information_gain": "duplicate_or_low_information"}
    v = build_verdict(_ledger(row), previous_no_information_gain_count=1)
    assert v["decision"] == "stop_no_information_gain"


def test_manual_review_required_for_manual_failure_class():
    v = build_verdict(_ledger({"experiment_id": "e1", "mechanism_id": "m1", "failure_class": "manual_review_required", "information_gain": "execution_failure"}))
    assert v["decision"] == "manual_review_required"
