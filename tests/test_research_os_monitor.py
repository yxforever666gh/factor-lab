from datetime import date

import pandas as pd

from factor_lab.research_os.catalog import ResearchCatalog
from factor_lab.research_os.lifecycle import (
    SleeveHealthObservation,
    SleeveLifecycleRecord,
    SleeveState,
)
from factor_lab.research_os.monitor import LifecycleMonitor


def _bad_observation(day: date) -> SleeveHealthObservation:
    return SleeveHealthObservation(
        as_of_date=day,
        active_ir_13w=-0.2,
        active_ir_26w=-0.1,
        ic_26w=-0.02,
    )


def test_monitor_opens_exact_trading_session_sla_on_reduction(tmp_path) -> None:
    sessions = [stamp.date() for stamp in pd.bdate_range("2026-01-05", periods=80)]
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        monitor = LifecycleMonitor(catalog)
        initial = SleeveLifecycleRecord(
            sleeve_id="value_quality",
            state=SleeveState.ACTIVE,
            target_weight=0.25,
            effective_weight=0.25,
        )
        first = monitor.tick(
            initial,
            _bad_observation(date(2026, 1, 5)),
            snapshot_id="snapshot-1",
        )
        assert first.state == "active"

        from dataclasses import replace

        updated = replace(
            initial,
            consecutive_multi_alarm_checks=first.record[
                "consecutive_multi_alarm_checks"
            ],
        )
        second = monitor.tick(
            updated,
            _bad_observation(date(2026, 1, 6)),
            snapshot_id="snapshot-2",
            trading_sessions=sessions,
        )
        assert second.state == "reduced"
        assert second.recommended_action == "halve_weight"
        case = catalog.get_recovery_case(second.recovery_case_id)
        assert case is not None
        assert case.drift_event_due_at.date() == date(2026, 1, 13)
        assert case.diagnosis_due_at.date() == date(2026, 2, 3)
        assert case.earliest_recovery_review_at.date() == date(2026, 3, 31)


def test_data_failure_requires_calendar_and_fails_before_event(tmp_path) -> None:
    with ResearchCatalog(tmp_path / "catalog.db") as catalog:
        catalog.initialize_schema()
        monitor = LifecycleMonitor(catalog)
        record = SleeveLifecycleRecord(
            sleeve_id="trend", state=SleeveState.ACTIVE, target_weight=0.25
        )
        observation = SleeveHealthObservation(
            as_of_date=date(2026, 1, 5),
            active_ir_13w=0.1,
            active_ir_26w=0.1,
            ic_26w=0.01,
            data_quality_ok=False,
        )
        import pytest

        with pytest.raises(ValueError, match="60 sessions"):
            monitor.tick(record, observation, snapshot_id="broken")
