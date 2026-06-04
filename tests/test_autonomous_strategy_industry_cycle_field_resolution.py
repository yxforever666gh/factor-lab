from factor_lab.autonomous_strategy_industry_cycle_field_resolution import build_industry_cycle_field_resolution


def registry():
    return {"top_route_id":"industry_cycle_inflection_value_anchor_v1","routes":[{"route_id":"industry_cycle_inflection_value_anchor_v1","required_fields":["industry","industry_return_60d","industry_relative_pb","industry_relative_earnings_yield","date","ticker","forward_return_5d"]}]}


def test_industry_cycle_field_resolution_marks_industry_return_derivable():
    report=build_industry_cycle_field_resolution(run_id='x',route_registry_v2=registry(),available_fields={"industry","date","ticker","forward_return_5d","return_1d","industry_relative_pb","industry_relative_earnings_yield"})
    statuses={r['field']:r['resolution_status'] for r in report['field_resolutions']}
    assert statuses['industry_return_60d']=='derivable'
    assert report['ready_for_derivation_specs'] is True
    assert report['queue_write_allowed'] is False


def test_industry_cycle_field_resolution_blocks_when_return_source_missing():
    report=build_industry_cycle_field_resolution(run_id='x',route_registry_v2=registry(),available_fields={"industry","date","ticker","forward_return_5d"})
    statuses={r['field']:r['resolution_status'] for r in report['field_resolutions']}
    assert statuses['industry_return_60d']=='missing_source_for_derivation'
    assert report['ready_for_derivation_specs'] is False
