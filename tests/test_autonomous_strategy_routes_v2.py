from factor_lab.autonomous_strategy_routes_v2 import build_route_registry_v2


def response():
    return {"worker_key":"factor_lab_mechanism_researcher","decision_recommendation":"switch_mechanism_route","candidate_routes":[
        {"route_id":"earnings_revision_valuation_repair_v2","mechanism_family":"earnings_revision_valuation_repair","required_fields":["forecast_eps"],"data_status":"request_data","cheap_screens":[],"falsification_criteria":[]},
        {"route_id":"industry_cycle_inflection_value_anchor_v1","mechanism_family":"industry_cycle_inflection_with_value_anchor","required_fields":["industry_return_60d"],"data_status":"derivable_from_available_market_history","cheap_screens":[],"falsification_criteria":[]},
        {"route_id":"balance_sheet_improvement_recovery_v1","mechanism_family":"balance_sheet_improvement_recovery","required_fields":["debt_to_asset_delta"],"data_status":"proxy_available_requires_review","cheap_screens":[],"falsification_criteria":[]},
    ]}


def test_route_registry_v2_prioritizes_derivable_industry_cycle_route():
    reg=build_route_registry_v2(run_id='x',preview_response=response())
    assert reg['schema_version']==2
    assert reg['top_route_id']=='industry_cycle_inflection_value_anchor_v1'
    statuses={r['route_id']:r['route_status'] for r in reg['routes']}
    assert statuses['industry_cycle_inflection_value_anchor_v1']=='field_resolution_candidate'
    assert statuses['earnings_revision_valuation_repair_v2']=='request_data_candidate'
    assert reg['controlled_execution_allowed'] is False
    assert reg['queue_write_allowed'] is False
