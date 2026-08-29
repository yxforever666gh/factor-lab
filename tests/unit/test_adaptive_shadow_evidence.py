from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import factor_lab.adaptive_shadow_evidence as evidence
from factor_lab.adaptive_shadow_evaluation import evaluate_shadow_outcomes


HEAD = "a" * 64
DECISION = "d" * 64
ROUTE_SHA = "4" * 64
INPUT_SHA = "5" * 64
SOURCE_SHA = "6" * 64
RESULT_SHA = "7" * 64
SIGNAL = "2026-09-10"
START = "2026-09-11"
END = "2026-09-25"
CANDIDATES = evidence.CHALLENGER_IDS
HOLDING_DATES = (
    "2026-09-11",
    "2026-09-14",
    "2026-09-15",
    "2026-09-16",
    "2026-09-17",
    "2026-09-18",
    "2026-09-21",
    "2026-09-22",
    "2026-09-23",
    "2026-09-24",
    "2026-09-25",
)


def _formal(kind: str, sha: str, payload: dict) -> dict:
    return {
        "kind": kind,
        "record_sha256": sha,
        "record": {"kind": kind, "payload": payload},
    }


def _shadow(kind: str, sha: str, payload: dict) -> dict:
    return {
        "record_sha256": sha,
        "record": {"kind": kind},
        "payload": payload,
    }


def _route() -> dict:
    sleeves = [
        {
            "offset": offset,
            "targets_ppm": {"FORMAL.SZ": 1_000_000} if offset == 0 else {},
        }
        for offset in range(10)
    ]
    return {
        "result_sha256": RESULT_SHA,
        "input_snapshot_sha256": INPUT_SHA,
        "signal_date": SIGNAL,
        "trade_date": START,
        "due_offset": 0,
        "sleeve_plans": sleeves,
    }


def _formal_cycle() -> dict:
    net_return_ppb = 1_234_500
    opening_nav_fen = 1_000_000
    ending_nav_fen = opening_nav_fen + evidence.ppb_to_ppm(net_return_ppb)
    return {
        "generation_result_sha256": RESULT_SHA,
        "signal_date": SIGNAL,
        "holding_start_date": START,
        "holding_end_date": END,
        "offset": 0,
        "net_return_ppb": net_return_ppb,
        "opening_nav_fen": opening_nav_fen,
        "ending_nav_fen": ending_nav_fen,
        "blocked_order_count": 0,
        "daily_path": [
            {
                "date": date,
                "account_nav_fen": (
                    ending_nav_fen if date == END else opening_nav_fen
                ),
                "benchmark_index_ppb": 1_000_000_000,
            }
            for date in HOLDING_DATES
        ],
    }


def _shadow_cycle(candidate: str, net_return_ppb: int) -> dict:
    opening_nav_fen = 1_000_000
    ending_nav_fen = opening_nav_fen + evidence.ppb_to_ppm(net_return_ppb)
    return {
        "formal_decision_record_sha256": DECISION,
        "candidate_id": candidate,
        "signal_date": SIGNAL,
        "holding_start_date": START,
        "holding_end_date": END,
        "offset": 0,
        "net_return_ppb": net_return_ppb,
        "opening_nav_fen": opening_nav_fen,
        "ending_nav_fen": ending_nav_fen,
        "blocked_order_count": 0,
        "daily_path": [
            {
                "date": date,
                "account_nav_fen": (
                    ending_nav_fen if date == END else opening_nav_fen
                ),
                "benchmark_index_ppb": 1_000_000_000,
            }
            for date in HOLDING_DATES
        ],
    }


def _plan(candidate: str, target: str) -> dict:
    return {
        "candidate_id": candidate,
        "formal_decision_record_sha256": DECISION,
        "formal_route_target_plan_sha256": ROUTE_SHA,
        "formal_input_snapshot_sha256": INPUT_SHA,
        "source_data_snapshot_sha256": SOURCE_SHA,
        "signal_date": SIGNAL,
        "trade_date": START,
        "offset": 0,
        "targets_ppm": {target: 1_000_000},
    }


def _records() -> tuple[list[dict], list[dict]]:
    formal_plan = {
        "route_target_plan_sha256": ROUTE_SHA,
        "route_target_plan": _route(),
        "source_data_snapshot_sha256": SOURCE_SHA,
    }
    formal_records = [
        _formal("activation", HEAD, {}),
        _formal("decision", DECISION, {"plan": formal_plan}),
        _formal(
            "outcome",
            "e" * 64,
            {
                "decision_record_sha256": DECISION,
                "cycle_outcome": _formal_cycle(),
            },
        ),
    ]
    first_plan, second_plan = "1" * 64, "2" * 64
    shadow_records = [
        _shadow(
            "activation",
            "b" * 64,
            {"formal_head_record_sha256": HEAD, "start_after": "2026-09-01"},
        ),
        _shadow("plan", first_plan, _plan(CANDIDATES[0], "TURNOVER.SZ")),
        _shadow(
            "outcome",
            "3" * 64,
            {
                "plan_record_sha256": first_plan,
                "cycle_outcome": _shadow_cycle(CANDIDATES[0], 2_345_500),
            },
        ),
        _shadow("plan", second_plan, _plan(CANDIDATES[1], "VOLATILITY.SZ")),
        _shadow(
            "outcome",
            "8" * 64,
            {
                "plan_record_sha256": second_plan,
                "cycle_outcome": _shadow_cycle(CANDIDATES[1], -2_344_500),
            },
        ),
    ]
    return formal_records, shadow_records


@pytest.fixture(autouse=True)
def _compact_cycles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(evidence, "_formal_cycle", lambda value: SimpleNamespace(**value))
    monkeypatch.setattr(evidence, "_shadow_cycle", lambda value: SimpleNamespace(**value))


def test_extracts_only_replayable_control_and_challenger_evaluation_rows() -> None:
    formal, shadow = _records()

    rows = evidence._assemble_evidence(formal, shadow)

    assert [row["candidate_id"] for row in rows] == [evidence.CONTROL_ID, *CANDIDATES]
    assert [row["net_return_ppm"] for row in rows] == [1_234, 2_346, -2_344]
    assert all(row["formal_decision_record_sha256"] == DECISION for row in rows)
    assert rows[0]["plan_targets_sha256"] == evidence.canonical_plan_targets_sha256(
        {"FORMAL.SZ": 1_000_000}
    )
    assert len({row["plan_targets_sha256"] for row in rows}) == 3

    protocol = json.loads(
        (Path(__file__).parents[2] / "protocols/5.9-adaptive-shadow.json").read_text(
            encoding="utf-8"
        )
    )
    report = evaluate_shadow_outcomes(
            protocol,
            rows,
            cutoff_date=END,
            evaluation_date=END,
            evidence_quality={
                "pit_violation_count": 0,
                "integrity_violation_count": 0,
                "deep_replay_valid": True,
                "candidate_quality": {
                    candidate: {
                        "missed_deadline_count": 0,
                        "missed_record_count": 0,
                        "terminated_offset_count": 0,
                        "terminated_offsets": [],
                        "missed_record_sha256s": [],
                    }
                    for candidate in CANDIDATES
                },
            },
    )
    assert report["complete_common_cohort_count"] == 1


def test_plan_without_outcome_never_becomes_evidence() -> None:
    formal, shadow = _records()
    shadow = [row for row in shadow if row["record"]["kind"] != "outcome"]

    rows = evidence._assemble_evidence(formal, shadow)

    assert [row["candidate_id"] for row in rows] == [evidence.CONTROL_ID]


@pytest.mark.parametrize(
    "case",
    ["missing_decision", "route_sha", "input_sha", "source_sha", "cohort_collision"],
)
def test_rejects_missing_or_crossed_formal_bindings(case: str) -> None:
    formal, shadow = _records()
    plan = shadow[1]["payload"]
    cycle = shadow[2]["payload"]["cycle_outcome"]
    if case == "missing_decision":
        plan["formal_decision_record_sha256"] = "f" * 64
        cycle["formal_decision_record_sha256"] = "f" * 64
    elif case == "route_sha":
        plan["formal_route_target_plan_sha256"] = "f" * 64
    elif case == "input_sha":
        plan["formal_input_snapshot_sha256"] = "f" * 64
    elif case == "source_sha":
        plan["source_data_snapshot_sha256"] = "f" * 64
    else:
        other_decision = deepcopy(formal[1])
        other_decision["record_sha256"] = "c" * 64
        formal.insert(2, other_decision)
        formal[-1]["record"]["payload"]["decision_record_sha256"] = "c" * 64

    with pytest.raises(evidence.AdaptiveShadowEvidenceError, match="binding|formal decision|formal decisions"):
        evidence._assemble_evidence(formal, shadow)


@pytest.mark.parametrize("case", ["calendar", "duplicate"])
def test_rejects_ambiguous_challenger_cohorts(case: str) -> None:
    formal, shadow = _records()
    if case == "calendar":
        shadow[2]["payload"]["cycle_outcome"]["holding_end_date"] = "2026-09-24"
    else:
        duplicate = deepcopy(shadow[2])
        duplicate["record_sha256"] = "9" * 64
        shadow.append(duplicate)

    with pytest.raises(evidence.AdaptiveShadowEvidenceError, match="calendar|duplicate"):
        evidence._assemble_evidence(formal, shadow)


def test_rejects_activation_bound_to_another_formal_chain() -> None:
    formal, shadow = _records()
    shadow[0]["payload"]["formal_head_record_sha256"] = "f" * 64

    with pytest.raises(evidence.AdaptiveShadowEvidenceError, match="activation binds no record"):
        evidence._assemble_evidence(formal, shadow)


def _snapshot(root: Path) -> list[tuple[str, bytes]]:
    return sorted(
        (path.relative_to(root).as_posix(), path.read_bytes())
        for path in root.rglob("*")
        if path.is_file()
    )


@pytest.mark.parametrize("store_kind", ["formal", "shadow"])
def test_invalid_audit_is_rejected_without_writing_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store_kind: str
) -> None:
    root = tmp_path / store_kind
    if store_kind == "formal":
        layout = evidence.formal_ledger.LedgerLayout.at(root)
        for path in (
            layout.root,
            layout.records,
            layout.snapshots,
            layout.plans,
            layout.bundles,
            layout.inputs,
            layout.executions,
            layout.release_runners,
            layout.dispatch,
            layout.verification_cache,
        ):
            path.mkdir(parents=True, exist_ok=True)
        layout.lock_path.write_bytes(b"\0")
        monkeypatch.setattr(
            evidence.formal_ledger,
            "audit_ledger",
            lambda _root, **_kwargs: {"valid": False},
        )
        call = evidence._audited_formal_records
    else:
        layout = evidence.shadow_store.ShadowLayout.at(root)
        layout.records.mkdir(parents=True)
        layout.artifacts.mkdir()
        layout.lock_path.write_bytes(b"\0")
        monkeypatch.setattr(
            evidence.shadow_store,
            "audit_shadow_store",
            lambda _root: {"integrity_valid": False, "activated": True},
        )
        call = evidence._audited_shadow_records
    before = _snapshot(root)

    with pytest.raises(evidence.AdaptiveShadowEvidenceError, match="audit"):
        call(root)

    assert _snapshot(root) == before
