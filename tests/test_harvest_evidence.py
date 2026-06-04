import json
from factor_lab.harvest_evidence import build_evidence_ledger


def test_extracts_metrics_from_multiple_result_files(tmp_path):
    run = tmp_path / "artifacts/harvest_agent/cycle_0001/runs/exp1"
    run.mkdir(parents=True)
    (run / "status.json").write_text(json.dumps({"status": "finished"}))
    (run / "results.json").write_text(json.dumps({"sharpe_net": 0.7, "max_drawdown": -0.2}))
    (run / "factor_evaluations.json").write_text(json.dumps({"rank_ic_mean": 0.03, "rank_ic_ir": 0.2, "coverage": 0.9}))
    (run / "portfolio_results.json").write_text(json.dumps({"bucket_pair_spread_net": 0.004, "turnover": 0.1}))
    manifest = {"cycle_id": "cycle_0001", "experiments": [{"experiment_id": "exp1", "output_dir": str(run), "spec": {"mechanism_id": "m1"}}]}
    ledger = build_evidence_ledger(manifest)
    row = ledger["evidence"][0]
    assert row["metrics"]["rank_ic_mean"] == 0.03
    assert row["metrics"]["sharpe_net"] == 0.7
    assert row["evidence_quality"]["oos_status"] == "pass"
    assert row["failure_class"] is None
    assert row["information_gain"] == "positive_progress"


def test_classifies_execution_failure_and_missing_fields(tmp_path):
    run = tmp_path / "runs/exp2"
    run.mkdir(parents=True)
    (run / "status.json").write_text(json.dumps({"status": "failed", "missing_fields": ["x"]}))
    manifest = {"cycle_id": "cycle_0001", "experiments": [{"experiment_id": "exp2", "output_dir": str(run)}]}
    row = build_evidence_ledger(manifest)["evidence"][0]
    assert row["failure_class"] == "missing_required_fields"
    assert row["information_gain"] == "blocked_missing_data"
