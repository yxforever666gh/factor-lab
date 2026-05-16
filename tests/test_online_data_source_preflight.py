from factor_lab.online_data_source_preflight import (
    CandidateSpec,
    EndpointProbeResult,
    build_final_decision,
    classify_pit_control,
    detect_date_fields,
    evaluate_specs,
    score_candidate,
)


def test_detect_date_fields_and_pit_classification():
    fields = detect_date_fields(["ts_code", "trade_date", "margin_balance"])
    assert fields == ("trade_date",)
    assert classify_pit_control(fields) == "trade_date_observable"
    assert classify_pit_control(("ann_date", "end_date")) == "announcement_date_pit"
    assert classify_pit_control(("end_date",)) == "end_date_only_not_pit_safe"


def test_score_margin_daily_trade_date_as_mvp_candidate():
    spec = CandidateSpec("margin_financing", "融资融券", "tushare", ("margin_detail",), "crowding", "daily", True)
    best = EndpointProbeResult("tushare", "margin_financing", "margin_detail", True, 20, ("ts_code", "trade_date", "rzye"), ("trade_date",))
    decision = score_candidate(spec, best)
    assert decision.recommendation == "mvp_candidate"
    assert decision.pit_control == "trade_date_observable"
    assert decision.score >= 80


def test_score_blocks_end_date_only_source_for_research():
    spec = CandidateSpec("institutional_holding", "机构持仓", "tushare", ("top10_holders",), "ownership", "quarterly", True)
    best = EndpointProbeResult("tushare", "institutional_holding", "top10_holders", True, 10, ("ts_code", "end_date", "holder_name"), ("end_date",))
    decision = score_candidate(spec, best)
    assert decision.recommendation == "manual_review_before_research"
    assert "not_pit_safe" in decision.blockers


def test_build_final_decision_prefers_margin_mvp():
    spec = CandidateSpec("margin_financing", "融资融券", "tushare", ("margin_detail",), "crowding", "daily", True)
    best = EndpointProbeResult("tushare", "margin_financing", "margin_detail", True, 10, ("trade_date",), ("trade_date",))
    decision = score_candidate(spec, best)
    final = build_final_decision([decision])
    assert final["decision"] == "proceed_margin_mvp"
    assert final["selected_source"]["source_id"] == "margin_financing"


def test_evaluate_specs_chooses_successful_endpoint_over_empty_first_endpoint():
    specs = (CandidateSpec("dragon_tiger", "龙虎榜", "tushare", ("bad", "top_list"), "flow", "daily_event"),)

    def caller(spec, endpoint):
        if endpoint == "bad":
            return EndpointProbeResult("tushare", spec.source_id, endpoint, False, 0, (), (), "empty")
        return EndpointProbeResult("tushare", spec.source_id, endpoint, True, 5, ("trade_date",), ("trade_date",))

    decisions = evaluate_specs(specs, caller)
    assert decisions[0].best_endpoint.endpoint == "top_list"
    assert decisions[0].recommendation in {"secondary_candidate", "mvp_candidate"}
