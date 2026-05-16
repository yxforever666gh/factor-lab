from factor_lab.workflow_admission import evaluate_workflow_admission


def test_workflow_admission_blocks_missing_mechanism_for_full_workflow():
    decision = evaluate_workflow_admission(
        {"task_type": "workflow", "payload": {"config_path": "x.json", "factors": [{"name": "x", "expression": "book_yield"}]}}
    )

    assert decision["decision"] == "block"
    assert "missing_mechanism_id" in decision["reasons"]


def test_workflow_admission_blocks_missing_required_fields():
    decision = evaluate_workflow_admission(
        {
            "task_type": "workflow",
            "payload": {
                "mechanism_id": "value_trap_exclusion",
                "route_id": "value_trap_exclusion",
                "required_data_fields": ["book_yield", "debt_to_asset"],
                "factors": [{"name": "x", "expression": "book_yield + debt_to_asset"}],
            },
        },
        available_fields={"book_yield"},
    )

    assert decision["decision"] == "block"
    assert "missing_required_fields" in decision["reasons"]
    assert decision["coverage_preflight"]["missing_fields"] == ["debt_to_asset"]


def test_workflow_admission_allows_mechanized_ready_value_route():
    decision = evaluate_workflow_admission(
        {
            "task_type": "workflow",
            "payload": {
                "mechanism_id": "industry_relative_value",
                "route_id": "industry_relative_value",
                "required_data_fields": ["book_yield", "industry"],
                "factors": [{"name": "x", "expression": "book_yield"}],
            },
        },
        available_fields={"book_yield", "industry"},
    )

    assert decision["decision"] == "allow"
    assert decision["reasons"] == []
