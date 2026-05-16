from factor_lab.data_coverage_preflight import build_mechanism_data_gap_report


def test_build_mechanism_data_gap_report_flags_missing_template_fields():
    report = build_mechanism_data_gap_report(
        templates={
            "cashflow_quality": {
                "mechanism_id": "cashflow_quality",
                "required_data_fields": ["roe", "ocfps", "cashflow_to_debt"],
            }
        },
        available_fields={"roe"},
    )

    assert report["summary"]["template_count"] == 1
    assert report["summary"]["blocked_template_count"] == 1
    row = report["templates"][0]
    assert row["mechanism_id"] == "cashflow_quality"
    assert row["coverage_status"] == "blocked_missing_fields"
    assert row["missing_fields"] == ["cashflow_to_debt", "ocfps"]


def test_build_mechanism_data_gap_report_marks_available_templates_ready():
    report = build_mechanism_data_gap_report(
        templates={"value_quality_filter": {"mechanism_id": "value_quality_filter", "required_data_fields": ["earnings_yield", "pb"]}},
        available_fields={"earnings_yield", "pb"},
    )

    assert report["templates"][0]["coverage_status"] == "ready"
    assert report["summary"]["ready_template_count"] == 1
