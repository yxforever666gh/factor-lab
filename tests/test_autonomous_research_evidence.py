import json
from factor_lab.autonomous_research_evidence import build_evidence_ledger

def test_parses_successful_repair_output(tmp_path):
    r=tmp_path/'runs/x'; r.mkdir(parents=True); (r/'result.json').write_text(json.dumps({'status':'ok','max_drawdown':-0.3,'baseline_max_drawdown':-0.49,'sharpe':1.0}))
    ledger=build_evidence_ledger({'experiments':[{'experiment_id':'x','output_dir':str(r)}]})
    assert ledger['evidence'][0]['information_gain_class']=='positive_progress'

def test_handles_missing_metrics():
    ledger=build_evidence_ledger({'experiments':[{'experiment_id':'x','output_dir':'missing'}]})
    assert ledger['evidence'][0]['information_gain_class']=='execution_failure'

def test_improvement_below_threshold_positive_progress(tmp_path):
    r=tmp_path/'r'; r.mkdir(); (r/'result.json').write_text(json.dumps({'status':'ok','max_drawdown':-0.4,'baseline_max_drawdown':-0.49}))
    assert build_evidence_ledger({'experiments':[{'experiment_id':'x','output_dir':str(r)}]})['evidence'][0]['information_gain_class']=='positive_progress'
