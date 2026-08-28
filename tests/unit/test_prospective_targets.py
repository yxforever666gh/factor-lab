from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from factor_lab.prospective_targets import (
    DeploymentSpec,
    GENERATOR_ID,
    GenerationResult,
    InputSnapshot,
    SLEEVE_CAPITAL_FEN,
    TargetGenerationError,
    TenSleeveState,
    calendar_prefix_payload,
    calendar_prefix_sha256,
    compare_cohort_parity,
    generate_fixed_core_targets,
    rank_fixed_core_tickers,
    replay_fixed_core_cohorts,
    select_with_retention,
)
from factor_lab.research.signals import evaluate_expression


ROOT = Path(__file__).resolve().parents[2]
AUTHORITATIVE_RUN = ROOT / "runtime" / "runs" / "88009f1e5309b268"
FEATURES = ROOT / "runtime" / "data" / "top500" / "features.parquet"
EXECUTION = ROOT / "runtime" / "data" / "top500" / "execution.parquet"
FIXED_CORE_NAME = (
    "causal_blend__earnings_yield_over_pb__value_defensive_rank__w0p7.json"
)
HASHES = {
    "activation_record_sha256": "a" * 64,
    "implementation_upgrade_record_sha256": "b" * 64,
    "deployment_protocol_sha256": "c" * 64,
    "source_data_snapshot_sha256": "d" * 64,
    "target_rows_sha256": "e" * 64,
    "input_sources_sha256": "f" * 64,
    "membership_artifact_sha256": "1" * 64,
}


def _dates(start: str, count: int) -> list[str]:
    return pd.date_range(start, periods=count, freq="D").strftime("%Y-%m-%d").tolist()


def _aligned_rows(signal_date: str, count: int = 12) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(count):
        value = index + 1
        rows.append(
            {
                "date": signal_date,
                "ticker": f"T{index:02d}",
                "eligible": True,
                "universe_member": True,
                "earnings_yield": value,
                "pb": 1,
                "book_yield": value,
                "volatility_20": count + 1 - value,
            }
        )
    return rows


def _deployment(prefix: list[str]) -> DeploymentSpec:
    return DeploymentSpec(
        calendar_anchor=prefix[0],
        calendar_prefix_count=len(prefix),
        calendar_prefix_last_session=prefix[-1],
        calendar_prefix_sha256=calendar_prefix_sha256(prefix),
        activation_record_sha256=HASHES["activation_record_sha256"],
        implementation_upgrade_record_sha256=HASHES[
            "implementation_upgrade_record_sha256"
        ],
        deployment_protocol_sha256=HASHES["deployment_protocol_sha256"],
    )


def _snapshot(
    *,
    sessions: list[str],
    signal_index: int,
    rows: list[dict[str, object]] | None = None,
    skipped: list[str] | tuple[str, ...] = (),
) -> InputSnapshot:
    signal_date = sessions[signal_index]
    return InputSnapshot(
        signal_date=signal_date,
        calendar_sessions=sessions[: signal_index + 2],
        rows=rows or _aligned_rows(signal_date),
        source_data_snapshot_sha256=HASHES["source_data_snapshot_sha256"],
        target_rows_sha256=HASHES["target_rows_sha256"],
        input_sources_sha256=HASHES["input_sources_sha256"],
        membership_artifact_sha256=HASHES["membership_artifact_sha256"],
        source_build_checkpoint_utc=f"{signal_date}T08:45:00Z",
        max_available_at_utc=f"{signal_date}T08:30:00Z",
        information_cutoff_utc=f"{signal_date}T09:00:00Z",
        signal_close_utc=f"{signal_date}T07:00:00Z",
        admission_deadline_utc=f"{signal_date}T10:00:00Z",
        skipped_sessions=skipped,
    )


def _wrong_literal_rank(rows: list[dict[str, object]]) -> tuple[str, ...]:
    """The prohibited algebraic rewrite used only to lock boundary vectors."""

    frame = pd.DataFrame(rows).sort_values(["date", "ticker"]).reset_index(drop=True)
    control = evaluate_expression(frame, "earnings_yield / pb")
    defensive = evaluate_expression(
        frame, "rank(book_yield) + rank(earnings_yield) + rank(-volatility_20)"
    )
    dates = frame["date"]
    control_rank = control.groupby(dates, sort=False).rank(method="average", pct=True)
    defensive_rank = defensive.groupby(dates, sort=False).rank(method="average", pct=True)
    wrong = 0.3 * control_rank + 0.7 * defensive_rank
    ranked = frame.assign(score=wrong).sort_values(
        ["score", "ticker"], ascending=[False, True]
    )
    return tuple(ranked["ticker"])


def _top10_boundary_rows() -> list[dict[str, object]]:
    # This vector has an exact rational tie at the tenth boundary.  The frozen
    # binary64 operation gives T10 one ulp over T09; literal 0.3 ties them and
    # ticker ordering incorrectly selects T09.
    control = [3, 4, 7, 9, 6, 5, 2, 11, 10, 1, 8]
    book = [6, 11, 10, 9, 3, 2, 8, 7, 4, 5, 1]
    inverse_volatility = [10, 4, 8, 5, 3, 6, 7, 1, 11, 9, 2]
    rows = _aligned_rows("2026-08-24", len(control))
    for index, row in enumerate(rows):
        row["earnings_yield"] = control[index]
        row["book_yield"] = book[index]
        row["volatility_20"] = len(control) + 1 - inverse_volatility[index]
    return rows


def _retention_boundary_rows() -> list[dict[str, object]]:
    # This vector moves T03/T02 across the Top-15 retention boundary under the
    # same prohibited rewrite.
    control = [3, 9, 1, 8, 16, 4, 11, 12, 7, 10, 5, 14, 15, 2, 13, 6]
    book = [10, 8, 3, 5, 1, 12, 13, 14, 7, 6, 4, 16, 9, 11, 15, 2]
    inverse_volatility = [10, 11, 12, 1, 5, 13, 3, 2, 4, 15, 6, 9, 14, 8, 16, 7]
    rows = _aligned_rows("2026-08-24", len(control))
    for index, row in enumerate(rows):
        row["earnings_yield"] = control[index]
        row["book_yield"] = book[index]
        row["volatility_20"] = len(control) + 1 - inverse_volatility[index]
    return rows


def test_calendar_prefix_has_one_explicit_canonical_payload_and_hash() -> None:
    sessions = ["2026-08-20", "2026-08-21"]

    assert calendar_prefix_payload(sessions) == {
        "schema_version": 1,
        "anchor": "2026-08-20",
        "count": 2,
        "sessions": sessions,
    }
    assert (
        calendar_prefix_sha256(sessions)
        == "971a3ea4af8f8fca5e620c18a7d03a7a02eda581334a9b951980e7cb17b90cfd"
    )


def test_release_manifest_binds_generator_protocol_calendar_and_vectors() -> None:
    manifest_path = ROOT / "protocols" / "5.2-target-generator.json"
    vectors_path = ROOT / "protocols" / "5.2-target-test-vectors.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    vectors = json.loads(vectors_path.read_text(encoding="utf-8"))

    assert manifest["generator_id"] == GENERATOR_ID
    assert manifest["activation_protocol_sha256"] == hashlib.sha256(
        (ROOT / "protocols" / "5.0.json").read_bytes()
    ).hexdigest()
    assert manifest["calendar_prefix"]["sha256"] == (
        "49b71c0b4482569d56b00cca8d468c3ec417379ac2b03e2d3afea32e312ef67f"
    )
    assert manifest["test_vectors"]["sha256"] == hashlib.sha256(
        vectors_path.read_bytes()
    ).hexdigest()
    assert vectors["historical_parity"] == {
        "authoritative_run_id": "88009f1e5309b268",
        "cohort_count": 2329,
        "expected_sha256": "81c92f159532d0c701d7fe6a74df034de8ba8afcd2bc8e893b793ac1a4d540a8",
    }


def test_binary64_top10_boundary_keeps_original_one_minus_weight_order() -> None:
    rows = _top10_boundary_rows()

    correct = rank_fixed_core_tickers(rows)
    wrong = _wrong_literal_rank(rows)

    assert correct[:10][-1] == "T10"
    assert "T09" not in correct[:10]
    assert wrong[:10][-1] == "T09"
    assert "T10" not in wrong[:10]


def test_binary64_top15_boundary_controls_retention_membership() -> None:
    rows = _retention_boundary_rows()

    correct = rank_fixed_core_tickers(rows)
    wrong = _wrong_literal_rank(rows)
    correct_targets = select_with_retention(correct, previous_targets=["T03"])
    wrong_targets = select_with_retention(wrong, previous_targets=["T03"])

    assert correct[14:16] == ("T03", "T02")
    assert wrong[14:16] == ("T02", "T03")
    assert "T03" in correct_targets and "T00" not in correct_targets
    assert "T03" not in wrong_targets and "T00" in wrong_targets


def test_first_ten_sessions_seed_one_independent_sleeve_and_invest_gradually() -> None:
    sessions = _dates("2026-08-01", 21)
    prefix = sessions[:10]
    deployment = _deployment(prefix)
    state = TenSleeveState.genesis(deployment)

    assert all(not sleeve.initialized for sleeve in state.sleeves)
    assert all(sleeve.capital_fen == SLEEVE_CAPITAL_FEN for sleeve in state.sleeves)
    for step, signal_index in enumerate(range(10, 20), start=1):
        snapshot = _snapshot(sessions=sessions, signal_index=signal_index)
        result = generate_fixed_core_targets(
            deployment=deployment,
            input_snapshot=snapshot,
            previous_state=state,
        )
        assert result.calendar_index == signal_index
        assert result.due_offset == signal_index % 10
        assert sum(plan["action"] == "seed" for plan in result.sleeve_plans) == 1
        assert result.aggregate_cash_ppm == 1_000_000 - step * 100_000
        assert sum(result.aggregate_targets_ppm.values()) + result.aggregate_cash_ppm == 1_000_000
        assert all(weight % 10_000 == 0 for weight in result.aggregate_targets_ppm.values())
        assert DeploymentSpec.from_mapping(deployment.to_dict()) == deployment
        assert InputSnapshot.from_mapping(snapshot.to_dict()).snapshot_sha256 == snapshot.snapshot_sha256
        assert GenerationResult.from_mapping(result.to_dict()).result_sha256 == result.result_sha256
        state = result.next_state

    assert all(sleeve.initialized for sleeve in state.sleeves)
    assert state.last_processed_calendar_index == 19
    assert state.last_processed_session == sessions[19]
    assert state.state_sha256 == TenSleeveState.from_mapping(state.to_dict()).state_sha256


def test_target_envelopes_reject_boolean_schema_version() -> None:
    sessions = _dates("2026-08-01", 12)
    deployment = _deployment(sessions[:10])
    deployment_payload = deployment.to_dict()
    deployment_payload.pop("deployment_sha256")
    calendar = deployment_payload.pop("calendar")
    deployment_payload.update(
        {
            "calendar_anchor": calendar["anchor"],
            "calendar_prefix_count": calendar["prefix_count"],
            "calendar_prefix_last_session": calendar["prefix_last_session"],
            "calendar_prefix_sha256": calendar["prefix_sha256"],
            "schema_version": True,
        }
    )
    with pytest.raises(TargetGenerationError, match="schema_version"):
        DeploymentSpec(**deployment_payload)

    state_payload = TenSleeveState.genesis(deployment).to_dict()
    state_payload.pop("state_sha256")
    state_payload["schema_version"] = True
    with pytest.raises(TargetGenerationError, match="schema_version"):
        TenSleeveState(**state_payload)


def test_due_sleeve_uses_only_its_own_retention_and_other_sleeves_carry() -> None:
    sessions = _dates("2026-08-01", 22)
    deployment = _deployment(sessions[:10])
    state = TenSleeveState.genesis(deployment)
    for signal_index in range(10, 20):
        result = generate_fixed_core_targets(
            deployment=deployment,
            input_snapshot=_snapshot(sessions=sessions, signal_index=signal_index),
            previous_state=state,
        )
        state = result.next_state
    before = state

    # Index 20 revisits only offset 0.  The boundary vector makes its prior
    # holdings relevant while all other offsets must remain byte-identical.
    result = generate_fixed_core_targets(
        deployment=deployment,
        input_snapshot=_snapshot(
            sessions=sessions,
            signal_index=20,
            rows=[{**row, "date": sessions[20]} for row in _retention_boundary_rows()],
        ),
        previous_state=before,
    )

    assert result.due_offset == 0
    assert result.sleeve_plans[0]["action"] == "rebalance"
    assert all(plan["action"] == "carry" for plan in result.sleeve_plans[1:])
    assert result.next_state.sleeves[1:] == before.sleeves[1:]
    assert result.next_state.sleeves[0].last_calendar_index == 20


def test_skipped_sessions_are_exact_evidence_and_do_not_seed_missed_offsets() -> None:
    sessions = _dates("2026-08-01", 14)
    deployment = _deployment(sessions[:10])
    genesis = TenSleeveState.genesis(deployment)

    with pytest.raises(TargetGenerationError, match="skipped_sessions"):
        generate_fixed_core_targets(
            deployment=deployment,
            input_snapshot=_snapshot(sessions=sessions, signal_index=12, skipped=[]),
            previous_state=genesis,
        )

    result = generate_fixed_core_targets(
        deployment=deployment,
        input_snapshot=_snapshot(
            sessions=sessions,
            signal_index=12,
            skipped=sessions[10:12],
        ),
        previous_state=genesis,
    )
    assert result.due_offset == 2
    assert result.skipped_sessions == tuple(sessions[10:12])
    assert not result.next_state.sleeves[0].initialized
    assert not result.next_state.sleeves[1].initialized
    assert result.next_state.sleeves[2].initialized
    assert result.aggregate_cash_ppm == 900_000


def test_snapshot_is_order_invariant_but_rejects_labels_future_rows_and_future_calendar() -> None:
    sessions = _dates("2026-08-01", 12)
    rows = _aligned_rows(sessions[10])
    left = _snapshot(sessions=sessions, signal_index=10, rows=rows)
    right = _snapshot(
        sessions=sessions,
        signal_index=10,
        rows=[dict(reversed(list(row.items()))) for row in reversed(rows)],
    )
    assert left.snapshot_sha256 == right.snapshot_sha256

    labelled = [{**row, "forward_return_10": 0.1} for row in rows]
    with pytest.raises(TargetGenerationError, match="forbidden_or_unknown"):
        _snapshot(sessions=sessions, signal_index=10, rows=labelled)

    mixed_date = [dict(row) for row in rows]
    mixed_date[0]["date"] = sessions[11]
    with pytest.raises(TargetGenerationError, match="past/future rows"):
        _snapshot(sessions=sessions, signal_index=10, rows=mixed_date)

    future_calendar = InputSnapshot(
            signal_date=sessions[10],
            calendar_sessions=sessions + ["2026-08-13"],
            rows=rows,
            source_data_snapshot_sha256=HASHES["source_data_snapshot_sha256"],
            target_rows_sha256=HASHES["target_rows_sha256"],
            input_sources_sha256=HASHES["input_sources_sha256"],
            membership_artifact_sha256=HASHES["membership_artifact_sha256"],
            source_build_checkpoint_utc=f"{sessions[10]}T08:45:00Z",
            max_available_at_utc=f"{sessions[10]}T08:30:00Z",
            information_cutoff_utc=f"{sessions[10]}T09:00:00Z",
            signal_close_utc=f"{sessions[10]}T07:00:00Z",
            admission_deadline_utc=f"{sessions[10]}T10:00:00Z",
        )
    deployment = _deployment(sessions[:10])
    with pytest.raises(TargetGenerationError, match="next-session trade date"):
        generate_fixed_core_targets(
            deployment=deployment,
            input_snapshot=future_calendar,
            previous_state=TenSleeveState.genesis(deployment),
        )


def test_generator_rejects_wrong_route_prefix_state_hash_and_late_inputs() -> None:
    sessions = _dates("2026-08-01", 12)
    prefix = sessions[:10]
    with pytest.raises(TargetGenerationError, match="route is frozen"):
        DeploymentSpec(
            calendar_anchor=prefix[0],
            calendar_prefix_count=len(prefix),
            calendar_prefix_last_session=prefix[-1],
            calendar_prefix_sha256=calendar_prefix_sha256(prefix),
            activation_record_sha256="a" * 64,
            implementation_upgrade_record_sha256="b" * 64,
            deployment_protocol_sha256="c" * 64,
            route="online_full",
        )

    deployment = _deployment(prefix)
    state = TenSleeveState.genesis(deployment)
    corrupted = state.to_dict()
    corrupted["last_processed_session"] = sessions[8]
    with pytest.raises(TargetGenerationError, match="state_sha256"):
        TenSleeveState.from_mapping(corrupted)

    changed_prefix = list(sessions[:12])
    changed_prefix[4] = sessions[3]  # duplicate/non-increasing is fail-closed
    with pytest.raises(TargetGenerationError, match="strictly increasing"):
        InputSnapshot(
            signal_date=sessions[10],
            calendar_sessions=changed_prefix,
            rows=_aligned_rows(sessions[10]),
            source_data_snapshot_sha256="d" * 64,
            target_rows_sha256="e" * 64,
            input_sources_sha256="f" * 64,
            membership_artifact_sha256="1" * 64,
            source_build_checkpoint_utc=f"{sessions[10]}T08:45:00Z",
            max_available_at_utc=f"{sessions[10]}T08:30:00Z",
            information_cutoff_utc=f"{sessions[10]}T09:00:00Z",
            signal_close_utc=f"{sessions[10]}T07:00:00Z",
            admission_deadline_utc=f"{sessions[10]}T10:00:00Z",
        )

    with pytest.raises(TargetGenerationError, match="timestamps must satisfy"):
        InputSnapshot(
            signal_date=sessions[10],
            calendar_sessions=sessions,
            rows=_aligned_rows(sessions[10]),
            source_data_snapshot_sha256="d" * 64,
            target_rows_sha256="e" * 64,
            input_sources_sha256="f" * 64,
            membership_artifact_sha256="1" * 64,
            source_build_checkpoint_utc=f"{sessions[10]}T10:00:00Z",
            max_available_at_utc=f"{sessions[10]}T08:30:00Z",
            information_cutoff_utc=f"{sessions[10]}T09:00:00Z",
            signal_close_utc=f"{sessions[10]}T07:00:00Z",
            admission_deadline_utc=f"{sessions[10]}T10:30:00Z",
        )

    with pytest.raises(TargetGenerationError, match="timestamps must satisfy"):
        InputSnapshot(
            signal_date=sessions[10],
            calendar_sessions=sessions,
            rows=_aligned_rows(sessions[10]),
            source_data_snapshot_sha256="d" * 64,
            target_rows_sha256="e" * 64,
            input_sources_sha256="f" * 64,
            membership_artifact_sha256="1" * 64,
            source_build_checkpoint_utc=f"{sessions[10]}T08:45:00Z",
            max_available_at_utc=f"{sessions[10]}T06:30:00Z",
            information_cutoff_utc=f"{sessions[10]}T09:00:00Z",
            signal_close_utc=f"{sessions[10]}T07:00:00Z",
            admission_deadline_utc=f"{sessions[10]}T10:00:00Z",
        )


def test_small_fixture_replays_independent_offsets_and_parity_envelope() -> None:
    sessions = _dates("2024-01-01", 11)
    rows = [row for signal_date in sessions for row in _aligned_rows(signal_date)]

    generated = replay_fixed_core_cohorts(rows, sessions)
    expected_targets = {f"T{index:02d}": 100_000 for index in range(2, 12)}
    expected = [
        {
            "signal_date": signal_date,
            "calendar_index": index,
            "offset": index % 10,
            "targets_ppm": expected_targets,
        }
        for index, signal_date in enumerate(sessions)
    ]
    parity = compare_cohort_parity(generated, expected)

    assert len(generated) == 11
    assert generated[0]["offset"] == 0
    assert generated[10]["offset"] == 0
    assert parity["passed"] is True
    assert parity["matched_count"] == 11
    assert parity["generated_sha256"] == parity["expected_sha256"]


@pytest.mark.skipif(
    not (FEATURES.exists() and EXECUTION.exists() and AUTHORITATIVE_RUN.exists()),
    reason="authoritative local 5.0 run artifacts are not available",
)
def test_authoritative_run_replays_all_2329_fixed_core_cohorts() -> None:
    # This is intentionally a release-parity test, not production I/O in the
    # generator.  Only the eight signal allowlist columns are ever loaded.
    columns = [
        "date",
        "ticker",
        "eligible",
        "universe_member",
        "earnings_yield",
        "pb",
        "book_yield",
        "volatility_20",
    ]
    features = pd.read_parquet(FEATURES, columns=columns)
    execution_dates = pd.read_parquet(EXECUTION, columns=["date"])["date"]
    sessions = sorted(pd.to_datetime(execution_dates).dt.strftime("%Y-%m-%d").unique())

    assert len(sessions) == 2340
    assert sessions[0] == "2017-01-03"
    assert sessions[-1] == "2026-08-21"
    assert (
        calendar_prefix_sha256(sessions)
        == "49b71c0b4482569d56b00cca8d468c3ec417379ac2b03e2d3afea32e312ef67f"
    )

    # holding_days=10 requires the next-open trade session plus ten holding
    # sessions, exactly matching long_only's len(calendar)-11 boundary.
    generated = replay_fixed_core_cohorts(
        features,
        sessions,
        required_future_sessions=11,
    )
    index_by_date = {session: index for index, session in enumerate(sessions)}
    expected: list[dict[str, object]] = []
    prior_targets: dict[tuple[int, str], list[str]] = {}
    for offset in range(10):
        path = (
            AUTHORITATIVE_RUN
            / "adaptive"
            / f"offset-{offset:02d}"
            / "shadows"
            / FIXED_CORE_NAME
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        previous: list[str] = []
        for cohort in payload["result"]["period_target_weights"]:
            signal_date = str(cohort["signal_date"])
            prior_targets[(offset, signal_date)] = previous
            expected.append(
                {
                    "signal_date": signal_date,
                    "calendar_index": index_by_date[signal_date],
                    "offset": offset,
                    "targets_ppm": {
                        ticker: int(round(float(weight) * 1_000_000))
                        for ticker, weight in cohort["target_weights"].items()
                    },
                }
            )
            previous = list(cohort["target_weights"])

    parity = compare_cohort_parity(generated, expected)
    assert len(generated) == 2329
    assert len(expected) == 2329
    assert parity["mismatch_count"] == 0, parity
    assert parity["matched_count"] == 2329
    assert parity["generated_sha256"] == parity["expected_sha256"]
    assert (
        parity["generated_sha256"]
        == "81c92f159532d0c701d7fe6a74df034de8ba8afcd2bc8e893b793ac1a4d540a8"
    )

    generated_by_key = {
        (row["offset"], row["signal_date"]): set(row["targets_ppm"])
        for row in generated
    }
    for offset, signal_date, correct_ticker, rewritten_ticker in (
        (2, "2019-02-27", "600000.SH", "601169.SH"),
        (9, "2021-08-09", "600104.SH", "601166.SH"),
    ):
        day = features[
            pd.to_datetime(features["date"]).dt.strftime("%Y-%m-%d") == signal_date
        ]
        day = day[day["eligible"].fillna(False) & day["universe_member"].fillna(False)]
        wrong_targets = set(
            select_with_retention(
                _wrong_literal_rank(day.to_dict(orient="records")),
                prior_targets[(offset, signal_date)],
            )
        )
        actual_targets = generated_by_key[(offset, signal_date)]
        assert correct_ticker in actual_targets and rewritten_ticker not in actual_targets
        assert rewritten_ticker in wrong_targets and correct_ticker not in wrong_targets
