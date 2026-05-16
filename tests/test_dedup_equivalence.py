from factor_lab.dedup import config_fingerprint, experiment_equivalence_fingerprint, workflow_experiment_fingerprint


def test_experiment_equivalence_fingerprint_ignores_output_and_transient_metadata():
    base = {
        "data_source": "tushare",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "universe_limit": 100,
        "output_dir": "artifacts/run_a",
        "generated_at": "2026-01-01T00:00:00Z",
        "worker_note": "first",
        "factors": [{"name": "value", "expression": "book_yield"}],
    }
    changed_transient = {
        **base,
        "output_dir": "artifacts/run_b",
        "generated_at": "2026-01-02T00:00:00Z",
        "worker_note": "second",
    }

    assert experiment_equivalence_fingerprint(base) == experiment_equivalence_fingerprint(changed_transient)
    assert workflow_experiment_fingerprint(base) == workflow_experiment_fingerprint(changed_transient)
    assert config_fingerprint(base) != config_fingerprint(changed_transient)


def test_experiment_equivalence_fingerprint_changes_for_factor_expression():
    base = {
        "data_source": "tushare",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "universe_limit": 100,
        "factors": [{"name": "value", "expression": "book_yield"}],
    }
    changed_factor = {
        **base,
        "factors": [{"name": "value", "expression": "earnings_yield"}],
    }

    assert experiment_equivalence_fingerprint(base) != experiment_equivalence_fingerprint(changed_factor)


def test_experiment_equivalence_fingerprint_changes_for_window_or_universe():
    base = {
        "data_source": "tushare",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "universe_limit": 100,
        "factors": [{"name": "value", "expression": "book_yield"}],
    }

    assert experiment_equivalence_fingerprint(base) != experiment_equivalence_fingerprint({**base, "start_date": "2023-01-01"})
    assert experiment_equivalence_fingerprint(base) != experiment_equivalence_fingerprint({**base, "universe_limit": 300})


def test_experiment_equivalence_fingerprint_is_stable_for_factor_key_order():
    left = {
        "data_source": "tushare",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "factors": [
            {"expression": "book_yield", "name": "value", "family": "value"},
        ],
    }
    right = {
        "end_date": "2024-12-31",
        "start_date": "2024-01-01",
        "data_source": "tushare",
        "factors": [
            {"family": "value", "name": "value", "expression": "book_yield"},
        ],
    }

    assert experiment_equivalence_fingerprint(left) == experiment_equivalence_fingerprint(right)
