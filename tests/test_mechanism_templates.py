from factor_lab.mechanism_templates import apply_mechanism_template, load_mechanism_templates


def test_load_mechanism_templates_contains_value_quality_template():
    registry = load_mechanism_templates()

    template = registry.get("value_quality_filter")
    assert template is not None
    assert template["target_family"] == "value"
    assert "quality_roe" in template["required_data_fields"]
    assert template["budget_bucket"] == "mechanism_validation"


def test_apply_mechanism_template_adds_required_governance_metadata():
    proposal = {"base_factors": ["earnings_yield", "quality_roe"], "target_family": "value"}

    enriched = apply_mechanism_template(proposal, template_id="value_quality_filter")

    assert enriched["mechanism_id"] == "value_quality_filter"
    assert enriched["hypothesis"]
    assert enriched["required_data_fields"] == ["earnings_yield", "quality_roe", "pb"]
    assert enriched["expected_regime"] == "cheap_but_profitable"
    assert enriched["falsification_criteria"]
    assert enriched["budget_bucket"] == "mechanism_validation"
