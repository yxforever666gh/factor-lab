from factor_lab.harvest_scorecard import score_evidence_row


def test_scorecard_all_hard_gates_pass_and_promotable():
    row = {"metrics": {"rank_ic_mean": 0.03, "rank_ic_ir": 0.2, "sharpe_net": 0.8, "max_drawdown": -0.2, "coverage": 0.9, "bucket_pair_spread_net": 0.004}, "evidence_quality": {"duplicate_status": "independent_followup", "data_quality_status": "pass"}, "failure_class": None, "information_gain": "positive_progress"}
    scored = score_evidence_row(row)
    assert all(scored["hard_gates"].values())
    assert scored["promotion_eligible"] is True
    assert scored["soft_score_average"] >= 4


def test_scorecard_fails_on_drawdown_and_duplicate():
    row = {"metrics": {"rank_ic_mean": 0.03, "rank_ic_ir": 0.2, "sharpe_net": 0.8, "max_drawdown": -0.5, "coverage": 0.9}, "evidence_quality": {"duplicate_status": "duplicate"}, "failure_class": "duplicate_equivalent_experiment", "information_gain": "duplicate_or_low_information"}
    scored = score_evidence_row(row)
    assert scored["hard_gates"]["duplicate_gate"] is False
    assert scored["hard_gates"]["risk_gate"] is False
    assert scored["promotion_eligible"] is False
