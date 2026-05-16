
from factor_lab.value_route_scorecard import build_scorecard_rows


def test_scorecard_roles_and_tail_degradation():
    rows = build_scorecard_rows()
    by = {r["route_id"]: r for r in rows}
    q = by["value_quality_no_distress"]
    assert round(q["tail_degradation_ratio"], 3) == 0.504
    assert q["recommended_role"] == "primary_candidate"
    assert q["preliminary_weight"] == 0.5
    assert by["industry_relative_value"]["recommended_role"] == "low_weight_core_value_candidate"
