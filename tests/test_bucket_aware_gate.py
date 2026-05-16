from factor_lab.bucket_aware_gate import evaluate_bucket_aware_gate


def test_bucket_aware_gate_passes_positive_bucket_spread_with_rank_ic():
    result = evaluate_bucket_aware_gate(
        factor_result={"rank_ic_mean": 0.03},
        bucket_result={"spread_mean": 0.006},
        thresholds={"min_rank_ic": 0.02, "min_bucket_spread": 0.001},
    )

    assert result["decision"] == "pass"
    assert result["reasons"] == []


def test_bucket_aware_gate_fails_low_bucket_spread():
    result = evaluate_bucket_aware_gate(
        factor_result={"rank_ic_mean": 0.03},
        bucket_result={"spread_mean": 0.0001},
        thresholds={"min_rank_ic": 0.02, "min_bucket_spread": 0.001},
    )

    assert result["decision"] == "fail"
    assert "bucket_spread<0.001" in result["reasons"]


def test_bucket_aware_gate_fails_missing_bucket_result():
    result = evaluate_bucket_aware_gate(factor_result={"rank_ic_mean": 0.03}, bucket_result=None)

    assert result["decision"] == "fail"
    assert "missing_bucket_aware_result" in result["reasons"]
