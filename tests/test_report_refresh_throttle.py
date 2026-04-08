import json
from pathlib import Path

from factor_lab import research_queue


def test_should_refresh_reports_when_no_state(monkeypatch, tmp_path):
    monkeypatch.setattr(research_queue, 'REPORT_REFRESH_STATE_PATH', tmp_path / 'state.json')
    monkeypatch.setenv('RESEARCH_REPORT_REFRESH_MIN_SECONDS', '300')
    assert research_queue.should_refresh_reports() is True


def test_should_refresh_reports_respects_cooldown(monkeypatch, tmp_path):
    real_datetime = research_queue.datetime
    state = tmp_path / 'state.json'
    state.write_text(json.dumps({'last_refresh_at_utc': '2026-04-03T17:00:00+00:00'}), encoding='utf-8')
    monkeypatch.setattr(research_queue, 'REPORT_REFRESH_STATE_PATH', state)
    monkeypatch.setenv('RESEARCH_REPORT_REFRESH_MIN_SECONDS', '300')

    class FakeDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return real_datetime.fromisoformat('2026-04-03T17:03:00+00:00')

    monkeypatch.setattr(research_queue, 'datetime', FakeDateTime)
    assert research_queue.should_refresh_reports() is False


def test_should_refresh_reports_force(monkeypatch, tmp_path):
    monkeypatch.setattr(research_queue, 'REPORT_REFRESH_STATE_PATH', tmp_path / 'state.json')
    assert research_queue.should_refresh_reports(force=True) is True
