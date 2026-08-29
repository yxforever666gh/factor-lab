from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import date, datetime, timedelta
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import factor_lab.adaptive_shadow_checkpoint as checkpoint
from factor_lab.adaptive_shadow import canonical_sha256


PROTOCOL_SHA = "a" * 64
REGISTRY_SHA = "b" * 64
FORMAL_ACTIVATION = "c" * 64
SHADOW_ACTIVATION = "d" * 64
CANDIDATES = checkpoint.CHALLENGER_IDS


def _sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _formal(kind: str, sha: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "kind": kind,
        "record_sha256": sha,
        "record": {"kind": kind, "payload": payload},
    }


def _shadow(
    records: list[dict[str, Any]],
    kind: str,
    sha: str,
    payload: dict[str, Any],
    *,
    recorded_at_utc: str,
) -> dict[str, Any]:
    row = {
        "record_sha256": sha,
        "record": {
            "kind": kind,
            "previous_record_sha256": (
                records[-1]["record_sha256"] if records else None
            ),
            "recorded_at_utc": recorded_at_utc,
        },
        "payload": payload,
    }
    records.append(row)
    return row


def _protocol() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[2] / "protocols/5.9-adaptive-shadow.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _daily_path(
    start: str,
    end: str,
    opening_nav_fen: int,
    ending_nav_fen: int,
) -> list[dict[str, Any]]:
    start_day = date.fromisoformat(start)
    end_day = date.fromisoformat(end)
    intermediate = [start_day + timedelta(days=index) for index in range(10)]
    if intermediate[-1] >= end_day:
        raise AssertionError("fixture holding window must fit eleven observations")
    observation_days = [*intermediate, end_day]
    return [
        {
            "date": observation_day.isoformat(),
            "account_nav_fen": (
                ending_nav_fen
                if observation_index == len(observation_days) - 1
                else opening_nav_fen
            ),
            "benchmark_index_ppb": 1_000_000_000,
        }
        for observation_index, observation_day in enumerate(observation_days)
    ]


class Harness:
    def __init__(self) -> None:
        self.formal: list[dict[str, Any]] = [
            _formal("activation", FORMAL_ACTIVATION, {})
        ]
        self.shadow: list[dict[str, Any]] = []
        _shadow(
            self.shadow,
            "activation",
            SHADOW_ACTIVATION,
            {
                "formal_head_record_sha256": FORMAL_ACTIVATION,
                "start_after": "2026-01-01",
            },
            recorded_at_utc="2026-09-01T00:00:00Z",
        )
        self.append_calls: list[dict[str, Any]] = []
        self._latest_nav: dict[tuple[str, int], int] = {}
        self._latest_end: dict[tuple[str, int], str] = {}
        self._missed_count = 0

    def _cycle(
        self,
        expert: str,
        *,
        signal: str,
        start: str,
        end: str,
        offset: int,
        net_return_ppb: int,
        available: str,
        blocked_order_count: int,
    ) -> dict[str, Any]:
        key = (expert, offset)
        if key in self._latest_end and self._latest_end[key] != start:
            raise AssertionError(
                "same-offset fixture cycles must share their NAV boundary"
            )
        opening_nav = self._latest_nav.get(key, 1_000_000)
        ending_nav = int(
            round(opening_nav * (1.0 + net_return_ppb / 1_000_000_000.0))
        )
        self._latest_nav[key] = ending_nav
        self._latest_end[key] = end
        return {
            "signal_date": signal,
            "holding_start_date": start,
            "holding_end_date": end,
            "offset": offset,
            "net_return_ppb": net_return_ppb,
            "opening_nav_fen": opening_nav,
            "ending_nav_fen": ending_nav,
            "blocked_order_count": blocked_order_count,
            "daily_path": _daily_path(start, end, opening_nav, ending_nav),
            "observation_available_at_utc": available,
        }

    def add_cohort(
        self,
        index: int,
        *,
        signal: str,
        start: str,
        end: str,
        available: str,
        challengers: tuple[str, ...] = CANDIDATES,
        offset: int | None = None,
        blocked_by_expert: Mapping[str, int] | None = None,
    ) -> None:
        due_offset = index % 10 if offset is None else offset
        blocked = {} if blocked_by_expert is None else dict(blocked_by_expert)
        decision = _sha(f"decision-{index}")
        result = _sha(f"result-{index}")
        route_sha = _sha(f"route-{index}")
        input_sha = _sha(f"input-{index}")
        source_sha = _sha(f"source-{index}")
        sleeves = [
            {
                "offset": value,
                "targets_ppm": (
                    {f"FORMAL{index}.SZ": 1_000_000}
                    if value == due_offset
                    else {}
                ),
            }
            for value in range(10)
        ]
        plan = {
            "route_target_plan_sha256": route_sha,
            "route_target_plan": {
                "result_sha256": result,
                "input_snapshot_sha256": input_sha,
                "signal_date": signal,
                "trade_date": start,
                "due_offset": due_offset,
                "sleeve_plans": sleeves,
            },
            "source_data_snapshot_sha256": source_sha,
        }
        self.formal.append(_formal("decision", decision, {"plan": plan}))
        formal_cycle = self._cycle(
            checkpoint.CONTROL_ID,
            signal=signal,
            start=start,
            end=end,
            offset=due_offset,
            net_return_ppb=1_000_000,
            available=available,
            blocked_order_count=blocked.get(checkpoint.CONTROL_ID, 0),
        )
        formal_cycle["generation_result_sha256"] = result
        self.formal.append(
            _formal(
                "outcome",
                _sha(f"formal-outcome-{index}"),
                {
                    "decision_record_sha256": decision,
                    "cycle_outcome": formal_cycle,
                },
            )
        )
        for candidate_index, candidate in enumerate(challengers):
            plan_sha = _sha(f"shadow-plan-{index}-{candidate}")
            _shadow(
                self.shadow,
                "plan",
                plan_sha,
                {
                    "candidate_id": candidate,
                    "formal_decision_record_sha256": decision,
                    "formal_route_target_plan_sha256": route_sha,
                    "formal_input_snapshot_sha256": input_sha,
                    "source_data_snapshot_sha256": source_sha,
                    "signal_date": signal,
                    "trade_date": start,
                    "offset": due_offset,
                    "targets_ppm": {
                        f"SHADOW{candidate_index}{index}.SZ": 1_000_000
                    },
                },
                recorded_at_utc=f"{signal}T13:00:00Z",
            )
            shadow_cycle = self._cycle(
                candidate,
                signal=signal,
                start=start,
                end=end,
                offset=due_offset,
                net_return_ppb=2_000_000 + candidate_index * 1_000,
                available=available,
                blocked_order_count=blocked.get(candidate, 0),
            )
            shadow_cycle.update(
                {
                    "formal_decision_record_sha256": decision,
                    "candidate_id": candidate,
                }
            )
            _shadow(
                self.shadow,
                "outcome",
                _sha(f"shadow-outcome-{index}-{candidate}"),
                {
                    "plan_record_sha256": plan_sha,
                    "cycle_outcome": shadow_cycle,
                },
                recorded_at_utc=available,
            )

    def add_missed(
        self,
        candidate: str,
        *,
        signal: str,
        offset: int,
        missed_at_utc: str,
        reason: str = "missed_deadline",
    ) -> dict[str, Any]:
        self._missed_count += 1
        return _shadow(
            self.shadow,
            "missed",
            _sha(f"missed-{self._missed_count}-{candidate}-{signal}-{offset}"),
            {
                "candidate_id": candidate,
                "signal_date": signal,
                "offset": offset,
                "missed_at_utc": missed_at_utc,
                "reason": reason,
            },
            recorded_at_utc=missed_at_utc,
        )

    def view(self) -> checkpoint._VerifiedView:
        rows = checkpoint.evidence_bridge._assemble_evidence(
            self.formal,
            self.shadow,
        )
        formal_head = self.formal[-1]["record_sha256"]
        shadow_head = self.shadow[-1]["record_sha256"]
        quality = checkpoint._sealed_evidence_quality(
            self.shadow,
            formal_head=formal_head,
            shadow_head=shadow_head,
            deep_replay_valid=True,
        )
        return checkpoint._VerifiedView(
            protocol=_protocol(),
            protocol_sha256=PROTOCOL_SHA,
            registry_sha256=REGISTRY_SHA,
            activation_record_sha256=SHADOW_ACTIVATION,
            formal_head_record_sha256=formal_head,
            shadow_head_record_sha256=shadow_head,
            formal_records=tuple(deepcopy(self.formal)),
            shadow_records=tuple(deepcopy(self.shadow)),
            evaluation_rows=tuple(deepcopy(rows)),
            evidence_quality=deepcopy(quality),
        )

    def append(
        self,
        _root: str | Path,
        payload: dict[str, Any],
        *,
        recorded_at_utc: str,
        expected_previous_record_sha256: str | None = None,
    ) -> dict[str, Any]:
        current_head = self.shadow[-1]["record_sha256"]
        if expected_previous_record_sha256 != current_head:
            raise checkpoint.shadow_store.ShadowStoreStateError(
                "shadow store head changed before the conditional append"
            )
        unsigned = {
            key: value
            for key, value in payload.items()
            if key != "evaluation_sha256"
        }
        assert payload["evaluation_sha256"] == canonical_sha256(unsigned)
        self.append_calls.append(deepcopy(payload))
        sha = _sha(f"evaluation-{len(self.append_calls)}")
        row = _shadow(
            self.shadow,
            "evaluation",
            sha,
            deepcopy(payload),
            recorded_at_utc=recorded_at_utc,
        )
        return {
            **deepcopy(row),
            "created": True,
            "recovered_orphan": False,
        }


@pytest.fixture(autouse=True)
def _compact_cycle_parsers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        checkpoint.evidence_bridge,
        "_formal_cycle",
        lambda value: SimpleNamespace(**value),
    )
    monkeypatch.setattr(
        checkpoint.evidence_bridge,
        "_shadow_cycle",
        lambda value: SimpleNamespace(**value),
    )


def _install(monkeypatch: pytest.MonkeyPatch, harness: Harness) -> None:
    monkeypatch.setattr(
        checkpoint,
        "_load_verified_view",
        lambda *_args: harness.view(),
    )
    monkeypatch.setattr(
        checkpoint,
        "_confirm_source_heads",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        checkpoint.shadow_store,
        "append_shadow_evaluation",
        harness.append,
    )


def _run(_harness: Harness, observed_at_utc: str | datetime) -> dict[str, Any]:
    return checkpoint.checkpoint_adaptive_shadow_evaluation(
        "project",
        "formal",
        "shadow",
        observed_at_utc=observed_at_utc,
    )


def _candidate_reports(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        row["candidate_id"]: row
        for row in payload["candidate_reports"]
    }


def _previous_month(value: str) -> str:
    year, month = (int(part) for part in value.split("-"))
    if month == 1:
        return f"{year - 1:04d}-12"
    return f"{year:04d}-{month - 1:02d}"


def _scripted_evaluator(
    pass_by_row_count: Mapping[int, bool],
):
    def evaluate(
        _protocol_value: Mapping[str, Any],
        outcomes: tuple[dict[str, Any], ...] | list[dict[str, Any]],
        *,
        cutoff_date: str,
        evaluation_date: str,
        evidence_quality: Mapping[str, Any],
        prior_monthly_states: Mapping[str, Mapping[str, bool]],
    ) -> dict[str, Any]:
        pair_counts, pair_incomplete, all_complete = checkpoint._pair_progress(
            outcomes
        )
        current_pass = pass_by_row_count[len(outcomes)]
        current_month = cutoff_date[:7]
        last_closed = _previous_month(evaluation_date[:7])
        reports: list[dict[str, Any]] = []
        any_eligible = False
        for candidate in CANDIDATES:
            states = dict(prior_monthly_states[candidate])
            states[current_month] = current_pass
            streak = 0
            cursor = last_closed
            while states.get(cursor) is True:
                streak += 1
                cursor = _previous_month(cursor)
            eligible = current_pass and streak >= 3
            any_eligible = any_eligible or eligible
            reports.append(
                {
                    "candidate_id": candidate,
                    "major_gate_pass_now": current_pass,
                    "monthly_state_month": current_month,
                    "last_closed_month": last_closed,
                    "monthly_states_after_current": dict(sorted(states.items())),
                    "consecutive_monthly_pass_count": streak,
                    "gates": {"consecutive_monthly_passes": streak >= 3},
                    "conclusion": (
                        "eligible_for_major_review" if eligible else "continue"
                    ),
                }
            )
        unsigned = {
            "schema_version": 1,
            "evaluation_id": "test-scripted-checkpoint-evaluation",
            "cutoff_date": cutoff_date,
            "evaluation_date": evaluation_date,
            "control_id": checkpoint.CONTROL_ID,
            "candidate_ids": list(CANDIDATES),
            "matured_outcome_count": len(outcomes),
            "excluded_unmatured_outcome_count": 0,
            "complete_common_cohort_count": all_complete,
            "incomplete_cohort_count": sum(pair_incomplete.values()),
            "candidate_pair_complete_cohort_counts": pair_counts,
            "candidate_pair_incomplete_cohort_counts": pair_incomplete,
            "evidence_quality": dict(evidence_quality),
            "violations": {"pit": 0, "integrity": 0},
            "candidate_reports": reports,
            "conclusion": (
                "eligible_for_major_review" if any_eligible else "continue"
            ),
            "automatic_promotion_allowed": False,
            "required_transition_release": "6.0",
        }
        return {**unsigned, "evaluation_sha256": canonical_sha256(unsigned)}

    return evaluate


def test_current_evidence_fixture_has_full_daily_contract() -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )

    view = harness.view()

    assert len(view.evaluation_rows) == 3
    for row in view.evaluation_rows:
        assert row["opening_nav_fen"] > 0
        assert row["ending_nav_fen"] > 0
        assert row["blocked_order_count"] == 0
        assert len(row["daily_path"]) == 11
        assert row["daily_path"][-1]["date"] == row["end_date"]
        assert row["daily_path"][-1]["account_nav_fen"] == row["ending_nav_fen"]
    assert view.evidence_quality["deep_replay_valid"] is True
    assert set(view.evidence_quality["candidate_quality"]) == set(CANDIDATES)


def test_empty_evidence_waits_without_writing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    _install(monkeypatch, harness)

    result = _run(harness, "2026-09-20T10:00:00Z")

    assert result["status"] == "waiting"
    assert result["reason"] == "no_outcome_or_missed_evidence"
    assert harness.append_calls == []


def test_each_candidate_control_pair_progresses_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
        challengers=(CANDIDATES[0],),
    )
    _install(monkeypatch, harness)

    first = _run(harness, "2026-09-20T10:00:00Z")

    assert first["status"] == "checkpointed"
    assert first["complete_common_cohort_count"] == 0
    assert first["candidate_pair_complete_cohort_counts"] == {
        CANDIDATES[0]: 1,
        CANDIDATES[1]: 0,
    }
    reports = _candidate_reports(first["evaluation"])
    assert reports[CANDIDATES[0]]["common_cycle_count"] == 1
    assert reports[CANDIDATES[1]]["common_cycle_count"] == 0

    harness.add_cohort(
        1,
        signal="2026-10-08",
        start="2026-10-09",
        end="2026-10-20",
        available="2026-10-20T09:00:00Z",
        challengers=(CANDIDATES[1],),
    )
    second = _run(harness, "2026-10-20T10:00:00Z")

    assert second["status"] == "checkpointed"
    assert second["complete_common_cohort_count"] == 0
    assert second["candidate_pair_complete_cohort_counts"] == {
        CANDIDATES[0]: 1,
        CANDIDATES[1]: 1,
    }
    assert len(harness.append_calls) == 2


def test_same_source_fingerprint_never_appends_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )
    _install(monkeypatch, harness)

    first = _run(harness, "2026-09-20T10:00:00Z")
    repeated = _run(harness, "2026-09-20T11:00:00Z")

    assert first["status"] == "checkpointed"
    assert repeated["status"] == "waiting"
    assert repeated["reason"] == "no_new_source_evidence"
    assert repeated["source_evidence_sha256"] == first["source_evidence_sha256"]
    assert len(harness.append_calls) == 1


def test_new_missed_evidence_checkpoints_without_pair_growth_and_retires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )
    _install(monkeypatch, harness)
    first = _run(harness, "2026-09-20T10:00:00Z")
    prior_counts = first["candidate_pair_complete_cohort_counts"]

    missed = harness.add_missed(
        CANDIDATES[0],
        signal="2026-09-25",
        offset=3,
        missed_at_utc="2026-09-26T01:00:00Z",
    )
    second = _run(harness, "2026-09-26T02:00:00Z")

    assert second["status"] == "checkpointed"
    assert second["candidate_pair_complete_cohort_counts"] == prior_counts
    assert second["source_evidence_sha256"] != first["source_evidence_sha256"]
    reports = _candidate_reports(second["evaluation"])
    retired = reports[CANDIDATES[0]]
    unaffected = reports[CANDIDATES[1]]
    assert retired["candidate_evidence_quality"]["missed_record_sha256s"] == [
        missed["record_sha256"]
    ]
    assert retired["gates"]["zero_missed_deadlines"] is False
    assert retired["gates"]["zero_terminated_offsets"] is False
    assert retired["conclusion"] == "retire"
    assert unaffected["gates"]["zero_missed_deadlines"] is True
    assert unaffected["gates"]["zero_terminated_offsets"] is True
    assert unaffected["conclusion"] != "retire"
    assert len(harness.append_calls) == 2


def test_historical_checkpoints_replay_from_named_source_prefixes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )
    _install(monkeypatch, harness)
    _run(harness, "2026-09-20T10:00:00Z")
    harness.add_cohort(
        1,
        signal="2026-10-08",
        start="2026-10-09",
        end="2026-10-20",
        available="2026-10-20T09:00:00Z",
    )
    second = _run(harness, "2026-10-20T10:00:00Z")

    history = checkpoint._historical_evaluations(harness.view())

    assert history.latest_cutoff_date == "2026-10-20"
    assert history.latest_pair_counts == {
        CANDIDATES[0]: 2,
        CANDIDATES[1]: 2,
    }
    assert history.latest_source_evidence_sha256 == second["source_evidence_sha256"]
    assert history.monthly_states == {
        CANDIDATES[0]: {"2026-09": False, "2026-10": False},
        CANDIDATES[1]: {"2026-09": False, "2026-10": False},
    }

    first_evaluation = next(
        row
        for row in harness.shadow
        if row["record"]["kind"] == "evaluation"
    )
    first_evaluation["payload"]["candidate_reports"][0][
        "common_cycle_count"
    ] += 1
    with pytest.raises(
        checkpoint.AdaptiveShadowCheckpointError,
        match="deterministic replay",
    ):
        checkpoint._historical_evaluations(harness.view())


def test_later_checkpoint_overwrites_earlier_state_in_same_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    _install(monkeypatch, harness)
    monkeypatch.setattr(
        checkpoint,
        "evaluate_shadow_outcomes",
        _scripted_evaluator({3: True, 6: False}),
    )
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )
    early = _run(harness, "2026-09-20T10:00:00Z")
    harness.add_cohort(
        1,
        signal="2026-09-17",
        start="2026-09-18",
        end="2026-09-29",
        available="2026-09-29T09:00:00Z",
    )
    late = _run(harness, "2026-09-29T10:00:00Z")

    assert all(
        row["monthly_states_after_current"]["2026-09"] is True
        for row in early["evaluation"]["candidate_reports"]
    )
    assert all(
        row["monthly_states_after_current"]["2026-09"] is False
        for row in late["evaluation"]["candidate_reports"]
    )
    history = checkpoint._historical_evaluations(harness.view())
    assert history.monthly_states == {
        candidate: {"2026-09": False} for candidate in CANDIDATES
    }


def test_only_asia_shanghai_closed_months_count_toward_streak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    _install(monkeypatch, harness)
    monkeypatch.setattr(
        checkpoint,
        "evaluate_shadow_outcomes",
        _scripted_evaluator({3: True, 6: True, 9: True, 12: True}),
    )
    checkpoints = [
        (
            0,
            "2026-06-08",
            "2026-06-09",
            "2026-06-20",
            "2026-06-20T09:00:00Z",
            "2026-06-20T10:00:00Z",
        ),
        (
            1,
            "2026-07-08",
            "2026-07-09",
            "2026-07-20",
            "2026-07-20T09:00:00Z",
            "2026-07-20T10:00:00Z",
        ),
        (
            2,
            "2026-08-19",
            "2026-08-20",
            "2026-08-31",
            "2026-08-31T15:00:00Z",
            "2026-08-31T15:59:58Z",
        ),
        (
            3,
            "2026-08-18",
            "2026-08-19",
            "2026-08-31",
            "2026-08-31T15:59:59Z",
            "2026-08-31T16:00:00Z",
        ),
    ]
    results: list[dict[str, Any]] = []
    for index, signal, start, end, available, observed in checkpoints:
        harness.add_cohort(
            index,
            signal=signal,
            start=start,
            end=end,
            available=available,
        )
        results.append(_run(harness, observed))

    before_midnight = results[2]["evaluation"]
    after_midnight = results[3]["evaluation"]
    assert before_midnight["evaluation_date"] == "2026-08-31"
    assert after_midnight["evaluation_date"] == "2026-09-01"
    for row in before_midnight["candidate_reports"]:
        assert row["last_closed_month"] == "2026-07"
        assert row["consecutive_monthly_pass_count"] == 2
        assert row["gates"]["consecutive_monthly_passes"] is False
    for row in after_midnight["candidate_reports"]:
        assert row["last_closed_month"] == "2026-08"
        assert row["consecutive_monthly_pass_count"] == 3
        assert row["gates"]["consecutive_monthly_passes"] is True
        assert row["conclusion"] == "eligible_for_major_review"


def test_conditional_append_fails_closed_on_concurrent_source_head_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )
    _install(monkeypatch, harness)

    def race_after_confirmation(*_args: Any, **_kwargs: Any) -> None:
        harness.add_missed(
            CANDIDATES[0],
            signal="2026-09-21",
            offset=4,
            missed_at_utc="2026-09-21T10:00:00Z",
        )

    monkeypatch.setattr(
        checkpoint,
        "_confirm_source_heads",
        race_after_confirmation,
    )

    with pytest.raises(
        checkpoint.shadow_store.ShadowStoreStateError,
        match="head changed",
    ):
        _run(harness, "2026-09-21T11:00:00Z")
    assert harness.append_calls == []
    assert harness.shadow[-1]["record"]["kind"] == "missed"


def test_observation_clock_cannot_predate_sealed_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = Harness()
    harness.add_cohort(
        0,
        signal="2026-09-08",
        start="2026-09-09",
        end="2026-09-20",
        available="2026-09-20T09:00:00Z",
    )
    _install(monkeypatch, harness)

    with pytest.raises(
        checkpoint.AdaptiveShadowCheckpointError,
        match="availability",
    ):
        _run(harness, "2026-09-20T08:59:59Z")
    assert harness.append_calls == []
