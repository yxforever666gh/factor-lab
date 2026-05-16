from scripts.write_earnings_event_admission_dry_run import build_admission_dry_run


def test_earnings_event_admission_dry_run_allows_after_hardening():
    payload = build_admission_dry_run()

    assert payload["dry_run"] is True
    assert payload["no_queue_write"] is True
    assert payload["no_daemon_start"] is True
    assert payload["admission"]["decision"] == "allow"
    assert payload["would_enqueue_count"] == 1
