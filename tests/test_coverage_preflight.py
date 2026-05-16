from factor_lab.coverage_preflight import evaluate_factor_coverage
from factor_lab.expression_validation import validate_expression


def test_expression_validation_accepts_registered_transform_function_names():
    result = validate_expression("zscore_by_date_industry(book_yield) + zscore_by_date(roe)", available_fields={"book_yield", "roe"})

    assert result.ok
    assert result.unknown_fields == []
    assert set(result.resolved_fields) == {"book_yield", "roe"}


def test_evaluate_factor_coverage_recommends_block_for_missing_required_fields():
    report = evaluate_factor_coverage(
        factor={"name": "bad", "expression": "book_yield + debt_to_asset", "required_data_fields": ["book_yield", "debt_to_asset"]},
        available_fields={"book_yield"},
    )

    assert report["recommendation"] == "block"
    assert report["missing_fields"] == ["debt_to_asset"]


def test_evaluate_factor_coverage_recommends_full_run_when_fields_and_ratio_ok():
    report = evaluate_factor_coverage(
        factor={"name": "ok", "expression": "book_yield + roe", "required_data_fields": ["book_yield", "roe"]},
        available_fields={"book_yield", "roe"},
        valid_ratio=0.8,
        min_full_run_coverage=0.6,
    )

    assert report["recommendation"] == "full_run"
    assert report["coverage_status"] == "ready"
