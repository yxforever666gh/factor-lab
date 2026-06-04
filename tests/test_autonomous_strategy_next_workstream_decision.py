from factor_lab.autonomous_strategy_next_workstream_decision import build_next_workstream_decision


def test_next_workstream_requests_new_mechanism_after_failed_proxy_report():
    report = build_next_workstream_decision(run_id="r", proxy_report={"alpha_status": "failed"})
    assert report["decision"] == "request_new_mechanism"
    assert report["recommended_next_step"] == "write_new_mechanism_request_v2"
    assert report["controlled_execution_allowed"] is False
    assert report["queue_write_allowed"] is False
