from factor_lab.harvest_data_request import build_harvest_data_request


def test_existing_required_fields_not_blocked():
    out = build_harvest_data_request({"required_fields":["book_yield"]}, {"book_yield"})
    assert out["blocked"] is False
    assert out["missing_required_fields"] == []


def test_missing_required_fields_blocked():
    out = build_harvest_data_request({"required_fields":["cashflow"]}, {"book_yield"})
    assert out["blocked"] is True
    assert out["missing_required_fields"] == ["cashflow"]


def test_value_quality_recommends_value_trap_fields():
    out = build_harvest_data_request({"mechanism_id":"value_quality_no_distress", "required_fields":["earnings_yield"]}, {"earnings_yield"})
    assert out["blocked"] is False
    assert "debt_to_asset" in out["recommended_data"]
    assert "operating_cashflow_to_profit" in out["recommended_data"]
