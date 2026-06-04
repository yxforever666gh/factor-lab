from factor_lab.autonomous_strategy_distress_route_verdict import build_distress_route_verdict


def test_distress_route_verdict_requests_data_when_pit_not_ready():
    v=build_distress_route_verdict(run_id='x',pit_preflight={'ready_for_proxy_distress_screen':False},distress_screen={})
    assert v['verdict']=='request_data'
    assert v['queue_write_allowed'] is False


def test_distress_route_verdict_stops_when_screen_fails():
    v=build_distress_route_verdict(run_id='x',pit_preflight={'ready_for_proxy_distress_screen':True,'decision':'use_proxy'},distress_screen={'overall_status':'fail','recommended_next_step':'stop_route'})
    assert v['verdict']=='stop_route'
    assert 'distress_screen_failed' in v['reason_codes']
    assert v['controlled_execution_allowed'] is False


def test_distress_route_verdict_manual_review_when_screen_candidate_passes():
    v=build_distress_route_verdict(run_id='x',pit_preflight={'ready_for_proxy_distress_screen':True},distress_screen={'overall_status':'manual_review','recommended_next_step':'manual_review_distress_repaired_screen'})
    assert v['verdict']=='manual_review_before_controlled_backtest'
