from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from factor_lab.adaptive_shadow import canonical_json_bytes, canonical_sha256
from factor_lab.adaptive_shadow_planning import (
    AdaptiveShadowPlanningError,
    build_registry_from_protocol,
    build_shadow_plan_payloads,
    validate_protocol_mapping,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
COMMIT = "d" * 40


@pytest.fixture
def protocol() -> dict[str, Any]:
    path = Path(__file__).parents[2] / "protocols" / "5.9-adaptive-shadow.json"
    return json.loads(path.read_text(encoding="utf-8"))


@dataclass(frozen=True)
class VerifiedInput:
    signal_date: str
    trade_date: str
    snapshot_sha256: str
    shadow_target_rows_sha256: str
    shadow_target_frame: list[dict[str, Any]]


def _rows() -> list[dict[str, Any]]:
    return [
        {
            "date": "2026-09-10",
            "ticker": f"T{index:03d}",
            "shadow_eligible": True,
            "low_turnover_20_v1": float(100 - index),
            "low_volatility_252_v1": float(index),
        }
        for index in range(16)
    ]


def _input(rows: list[dict[str, Any]] | None = None) -> VerifiedInput:
    return VerifiedInput(
        signal_date="2026-09-10",
        trade_date="2026-09-11",
        snapshot_sha256=SHA_A,
        shadow_target_rows_sha256=SHA_B,
        shadow_target_frame=_rows() if rows is None else rows,
    )


def _registry(protocol: dict[str, Any]):
    return build_registry_from_protocol(
        protocol,
        release_tag="5.9",
        commit_oid=COMMIT,
        released_at_utc="2026-09-01T00:00:00Z",
        start_after="2026-09-01",
    )


def _formal() -> dict[str, Any]:
    route = {
        "route": "fixed_core_full",
        "input_snapshot_sha256": SHA_C,
        "signal_date": "2026-09-10",
        "trade_date": "2026-09-11",
        "calendar_index": 20,
        "due_offset": 0,
    }
    return {
        "decision_session": "2026-09-11",
        "source_data_snapshot_sha256": SHA_A,
        "admission_deadline_utc": "2026-09-11T01:15:00Z",
        "route_target_plan": route,
        "route_target_plan_sha256": canonical_sha256(route),
    }


def _build(
    protocol: dict[str, Any],
    *,
    source: VerifiedInput | None = None,
    formal: dict[str, Any] | None = None,
    prior: dict[str, dict[int, list[str]]] | None = None,
):
    return build_shadow_plan_payloads(
        protocol,
        _registry(protocol),
        _input() if source is None else source,
        _formal() if formal is None else formal,
        "e" * 64,
        {} if prior is None else prior,
        "2026-09-10T12:00:00Z",
    )


def test_protocol_builds_exact_raw_lineage_registry(protocol: dict[str, Any]) -> None:
    checked = validate_protocol_mapping(protocol)
    registry = _registry(protocol)

    assert checked.candidate_ids == (
        "low_turnover_20_v1",
        "low_volatility_252_v1",
    )
    assert [candidate.required_fields for candidate in registry.candidates] == [
        ("turnover_rate",),
        ("close_hfq",),
    ]
    assert [candidate.selection.top_n for candidate in registry.candidates] == [10, 10]
    assert [candidate.selection.retention_n for candidate in registry.candidates] == [15, 15]
    assert [candidate.direction for candidate in registry.candidates] == [1, 1]


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("registry", "ordered_candidates"), list(reversed(["low_turnover_20_v1", "low_volatility_252_v1"]))),
        (("registry", "candidates", 0, "formula"), "close_hfq[t]"),
        (("registry", "candidates", 1, "direction"), "lower_is_better"),
        (("registry", "candidates", 0, "position_count"), 9),
        (("registry", "candidates", 0, "retention_buffer"), 4),
        (("registry", "selection_freeze", "post_selected"), False),
        (("feature_lineage", "no_financial_features"), False),
        (("comparison", "cross_candidate_completeness_required"), True),
        (("comparison", "healthy_candidate_offsets_continue"), False),
        (("admission", "missed_deadline_rule"), "forbid_every_candidate"),
        (("formal_route", "mutation_allowed"), True),
        (("formal_route", "automatic_promotion_allowed"), True),
        (("evaluation", "automatic_promotion_allowed"), True),
    ],
)
def test_protocol_tampering_fails_closed(
    protocol: dict[str, Any], path: tuple[Any, ...], value: Any
) -> None:
    changed = deepcopy(protocol)
    cursor: Any = changed
    for key in path[:-1]:
        cursor = cursor[key]
    cursor[path[-1]] = value
    with pytest.raises(AdaptiveShadowPlanningError):
        validate_protocol_mapping(changed)


def test_protocol_binds_exact_pre_audit_selection_freeze_bytes(
    protocol: dict[str, Any],
) -> None:
    project_root = Path(__file__).parents[2]
    disclosure = protocol["registry"]["selection_freeze"]
    artifact = project_root / disclosure["artifact_relative_path"]
    raw = artifact.read_bytes()
    payload = json.loads(raw)

    assert hashlib.sha256(raw).hexdigest() == disclosure["artifact_sha256"]
    payload_sha = payload.pop("payload_sha256")
    frozen_payload_bytes = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    assert hashlib.sha256(frozen_payload_bytes).hexdigest() == payload_sha
    assert payload_sha == disclosure["finalist_definition_payload_sha256"]
    assert payload["frozen_before_audit_evaluation"] is True
    assert disclosure["registry_composition_selected_after_audit"] is True
    assert disclosure["post_selected"] is True
    assert disclosure["historical_winner_claim_allowed"] is False


def test_two_candidates_keep_independent_offset_retention(protocol: dict[str, Any]) -> None:
    result = _build(
        protocol,
        prior={
            "low_turnover_20_v1": {0: ["T014"]},
            "low_volatility_252_v1": {0: ["T001"]},
        },
    )
    by_id = {str(plan["candidate_id"]): plan for plan in result.plan_payloads}

    turnover = by_id["low_turnover_20_v1"]["targets_ppm"]
    volatility = by_id["low_volatility_252_v1"]["targets_ppm"]
    assert "T014" in turnover and "T009" not in turnover
    assert "T001" in volatility and "T006" not in volatility
    assert len(turnover) == len(volatility) == 10
    assert sum(turnover.values()) == sum(volatility.values()) == 1_000_000
    assert {plan["offset"] for plan in result.plan_payloads} == {0}
    assert all("due_offset" not in plan for plan in result.plan_payloads)
    assert all(plan["cash_ppm"] == 0 for plan in result.plan_payloads)


def test_appending_future_rows_does_not_change_historical_targets(
    protocol: dict[str, Any],
) -> None:
    baseline = _build(protocol)
    future = {
        "date": "2026-09-12",
        "ticker": "FUTURE",
        "shadow_eligible": True,
        "low_turnover_20_v1": 1e100,
        "low_volatility_252_v1": 1e100,
    }
    appended = _build(protocol, source=replace(_input(), shadow_target_frame=[*_rows(), future]))

    assert [plan["targets_ppm"] for plan in baseline.plan_payloads] == [
        plan["targets_ppm"] for plan in appended.plan_payloads
    ]


@pytest.mark.parametrize("tamper", ["route", "source", "signal", "trade", "deadline", "input_hash", "offset"])
def test_formal_plan_and_input_tampering_is_rejected(
    protocol: dict[str, Any], tamper: str
) -> None:
    formal = _formal()
    source = _input()
    if tamper == "route":
        formal["route_target_plan"]["route"] = "anything_else"
    elif tamper == "source":
        formal["source_data_snapshot_sha256"] = "f" * 64
    elif tamper == "signal":
        source = replace(source, signal_date="2026-09-09")
    elif tamper == "trade":
        source = replace(source, trade_date="2026-09-12")
    elif tamper == "deadline":
        formal["admission_deadline_utc"] = "2026-09-11T01:15:01Z"
    elif tamper == "input_hash":
        formal["route_target_plan"]["input_snapshot_sha256"] = "f" * 64
    else:
        formal["route_target_plan"]["due_offset"] = 1
    if tamper in {"source", "signal", "trade", "deadline"}:
        pass
    elif tamper == "offset":
        formal["route_target_plan_sha256"] = canonical_sha256(formal["route_target_plan"])
    with pytest.raises(AdaptiveShadowPlanningError):
        _build(protocol, source=source, formal=formal)


def test_every_store_payload_is_integer_canonical_json(protocol: dict[str, Any]) -> None:
    result = _build(protocol)
    raw = [canonical_json_bytes(plan) for plan in result.plan_payloads]
    raw.append(canonical_json_bytes(result.to_payload()))
    assert all(blob.startswith(b"{") for blob in raw)
    assert set(result.to_payload()) == {"shadow_plans"}
