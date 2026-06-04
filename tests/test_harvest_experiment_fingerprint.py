import json

from factor_lab.harvest_experiment_fingerprint import fingerprint_plan, is_duplicate_fingerprint, write_fingerprint


def _plan(costs):
    return {
        "dataset_path": "dataset.csv",
        "actions": [
            {"type": "set_signal_columns", "signal_columns": ["industry_relative_book_yield"]},
            {"type": "restrict_costs", "cost_bps_values": costs},
            {"type": "add_filter", "field": "volatility_20", "operator": "<=", "quantile": 0.6},
        ],
    }


def test_fingerprint_plan_is_stable_and_changes_with_plan():
    assert fingerprint_plan(_plan([30, 60])) == fingerprint_plan(_plan([30, 60]))
    assert fingerprint_plan(_plan([30, 60])) != fingerprint_plan(_plan([60]))


def test_duplicate_fingerprint_scans_prior_cycles(tmp_path):
    fp = fingerprint_plan(_plan([30, 60]))
    prior = tmp_path / "artifacts/harvest_agent/cycle_0001"
    prior.mkdir(parents=True)
    (prior / "experiment_fingerprint.json").write_text(json.dumps({"fingerprint": fp}), encoding="utf-8")
    assert is_duplicate_fingerprint(tmp_path, fp) is True
    assert is_duplicate_fingerprint(tmp_path, "different") is False


def test_write_fingerprint_writes_artifact(tmp_path):
    path = write_fingerprint(tmp_path / "cycle", _plan([30]))
    payload = json.loads(path.read_text())
    assert payload["fingerprint"]
    assert payload["canonical_plan"]
