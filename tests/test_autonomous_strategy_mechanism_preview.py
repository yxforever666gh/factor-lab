from factor_lab.autonomous_strategy_mechanism_preview import build_mechanism_researcher_preview_response


def test_mechanism_preview_response_proposes_catalyst_routes_safely():
    response = build_mechanism_researcher_preview_response(run_id='x', request_pack={})
    assert response['decision_recommendation'] == 'switch_mechanism_route'
    assert len(response['candidate_routes']) == 3
    assert all(route['economic_mechanism'] for route in response['candidate_routes'])
    assert response['controlled_execution_allowed'] is False
    assert response['queue_write_allowed'] is False
    assert response['forbidden_actions_observed'] == []
    assert 'earnings_revision_valuation_repair_v2' in {r['route_id'] for r in response['candidate_routes']}
