from factor_lab.autonomous_research_verdict import write_verdict_payload

def test_current_blocker_with_improvement_continues():
    v=write_verdict_payload({'evidence':[{'information_gain_class':'positive_progress','max_drawdown':-0.4}]})
    assert v['verdict']=='continue_same_mainline'

def test_passing_drawdown_requests_manual_review():
    v=write_verdict_payload({'evidence':[{'information_gain_class':'positive_progress','max_drawdown':-0.3,'constraints_pass':True}]})
    assert v['verdict']=='promote_to_paper_review_manual_approval'
    assert v['manual_approval_required'] is True

def test_missing_data_blocks():
    v=write_verdict_payload({'evidence':[{'information_gain_class':'blocked_missing_data'}]})
    assert v['verdict']=='blocked_needs_data_or_manual_review'

def test_consecutive_no_gain_stops():
    v=write_verdict_payload({'evidence':[{'information_gain_class':'negative_but_informative'}]}, previous_no_gain_count=1)
    assert v['verdict']=='stop_no_information_gain'
