from factor_lab.research_os.dsl import Availability, FieldSpec
from factor_lab.research_os.proposals import validate_llm_proposal


def _payload():
    return {
        "preregistration": {
            "hypothesis_id": "hyp_value_quality",
            "economic_mechanism": "cheap profitable firms may be underpriced",
            "direction": "positive",
            "falsification_criteria": ["outer OOS excess is non-positive"],
            "allowed_variants": ["industry_neutral"],
            "stop_rules": ["stop after two diagnostics"],
        },
        "factor": {
            "factor_id": "value_quality_v1",
            "family": "value_quality",
            "name": "value quality",
            "mechanism": "cheap profitable firms may be underpriced",
            "expression": {
                "nodes": [
                    {"id": "raw", "op": "field", "field": "book_to_price"},
                    {"id": "ranked", "op": "rank", "input": "raw"},
                ],
                "output": "ranked",
            },
            "direction": "higher_is_better",
            "falsification_criteria": ["outer OOS excess is non-positive"],
            "allowed_variants": ["industry_neutral"],
        },
    }


def test_llm_can_propose_valid_dsl_but_not_a_decision() -> None:
    fields = [FieldSpec("book_to_price", availability=Availability.CLOSE)]
    accepted = validate_llm_proposal(_payload(), field_specs=fields)
    assert accepted.accepted

    payload = _payload()
    payload["promotion"] = {"pass_gate": True, "sharpe": 3.0}
    rejected = validate_llm_proposal(payload, field_specs=fields)
    assert not rejected.accepted
    assert any("decision_authority_forbidden" in item for item in rejected.violations)


def test_llm_cannot_propose_future_label_expression() -> None:
    payload = _payload()
    payload["factor"]["expression"]["nodes"][0]["field"] = "forward_return_5d"
    review = validate_llm_proposal(
        payload,
        field_specs=[FieldSpec("forward_return_5d")],
    )
    assert not review.accepted
    assert any("forward" in item for item in review.violations)
