"""Pure integration planning for the 5.9 adaptive-shadow tournament.

The functions in this module accept already-loaded, already-verified values.
They do not read a filesystem or clock and do not mutate either the formal
prospective ledger or an adaptive-shadow runtime.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import re
from typing import Any

from .adaptive_shadow import (
    AdaptiveShadowError,
    CandidateSpec,
    Registry,
    SelectionSpec,
    assess_plan_timing,
    canonical_json_bytes,
    canonical_sha256,
    generate_targets,
)


PLAN_SCHEMA_VERSION = 1
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_IDS = ("low_turnover_20_v1", "low_volatility_252_v1")
_EXPECTED_CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "id": "low_turnover_20_v1",
        "version": 1,
        "hypothesis": "Stocks with lower mean daily turnover over the trailing 20 official observations exhibit slower ownership rotation and lower implementation churn over the fixed holding horizon.",
        "formula": "-mean(turnover_rate[i], i=t-19..t)",
        "required_fields": ["turnover_rate"],
        "lookback_sessions": 20,
        "minimum_observations": 20,
        "direction": "higher_is_better",
        "signal_cutoff": "provider_complete_signal_close",
        "entry_rule": "next_official_session_open",
        "position_count": 10,
        "retention_buffer": 5,
        "selection_disclosure": "Its definition was frozen as a finalist before the 2025-2026 audit was opened, but this two-candidate registry was authored after that audit and is therefore explicitly post-selected. The unverified-vintage historical bridge and the 2017-2022 train / 2023-2024 validation / 2025-2026 audit diagnostics did not establish robust superiority to formal fixed_core_full. No historical-winner claim is allowed; only timely signals sealed after activation count toward evaluation.",
    },
    {
        "id": "low_volatility_252_v1",
        "version": 1,
        "hypothesis": "Stocks with lower realized volatility over the trailing 252 adjusted-close returns may preserve capital over the fixed holding horizon, with incremental value assessed against the defensive formal route.",
        "formula": "-std(log(close_hfq[i] / close_hfq[i-1]), i=t-251..t, ddof=1)",
        "required_fields": ["close_hfq"],
        "lookback_sessions": 253,
        "minimum_observations": 253,
        "direction": "higher_is_better",
        "signal_cutoff": "provider_complete_signal_close",
        "entry_rule": "next_official_session_open",
        "position_count": 10,
        "retention_buffer": 5,
        "selection_disclosure": "Its definition was frozen as a finalist before the 2025-2026 audit was opened, but this two-candidate registry was authored after that audit and is therefore explicitly post-selected. This defensive score overlaps the formal route's low-volatility exposure, and the unverified-vintage 2017-2026 diagnostics did not establish robust superiority to formal fixed_core_full. No historical-winner claim is allowed; only timely signals sealed after activation count toward evaluation.",
    },
)
_EXPECTED_SELECTION_FREEZE: Mapping[str, Any] = {
    "artifact_relative_path": "protocols/5.9-selection-freeze.json",
    "artifact_sha256": "8c6211633904e3d3075602ae32dc1d96eca32ad27401a500726ea1e655d64679",
    "finalist_definition_payload_sha256": "14d471e13d189a6657f8f76295054524ec97dfc4e0dfaa68ceea882549d098ae",
    "selection_data_end": "2024-12-31",
    "audit_start": "2025-01-01",
    "candidate_parameters_changed_after_audit": False,
    "registry_composition_selected_after_audit": True,
    "post_selected": True,
    "historical_winner_claim_allowed": False,
}


class AdaptiveShadowPlanningError(AdaptiveShadowError):
    """Raised when a protocol or cross-artifact planning binding fails closed."""


def _fail(message: str) -> None:
    raise AdaptiveShadowPlanningError(message)


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        _fail(f"{label} must be an object")
    return dict(value)


def _text(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        _fail(f"{label} must be a canonical non-empty string")
    return value


def _sha(value: Any, *, label: str) -> str:
    result = _text(value, label=label)
    if not _SHA256_RE.fullmatch(result):
        _fail(f"{label} must be a lowercase SHA-256")
    return result


def _date(value: Any, *, label: str) -> str:
    result = _text(value, label=label)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise AdaptiveShadowPlanningError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != result:
        _fail(f"{label} must be a canonical ISO date")
    return result


def _utc(value: Any, *, label: str) -> str:
    if isinstance(value, str):
        raw = _text(value, label=label)
        try:
            parsed = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise AdaptiveShadowPlanningError(
                f"{label} must be an ISO timestamp"
            ) from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        _fail(f"{label} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        _fail(f"{label} must be timezone-aware")
    parsed = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if parsed.microsecond else "seconds"
    return parsed.isoformat(timespec=timespec).replace("+00:00", "Z")


def _integer(value: Any, *, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        _fail(f"{label} must be an integer >= {minimum}")
    return value


def _require_values(value: Mapping[str, Any], expected: Mapping[str, Any], label: str) -> None:
    for key, required in expected.items():
        if value.get(key) != required:
            _fail(f"{label}.{key} differs from the frozen 5.9 protocol")


@dataclass(frozen=True)
class ValidatedShadowProtocol:
    protocol_version: str
    implementation_release: str
    candidate_rows: tuple[Mapping[str, Any], ...]

    @property
    def candidate_ids(self) -> tuple[str, ...]:
        return tuple(str(row["id"]) for row in self.candidate_rows)


def validate_protocol_mapping(protocol: Mapping[str, Any]) -> ValidatedShadowProtocol:
    """Validate the complete execution-critical 5.9 protocol mapping."""

    root = _mapping(protocol, label="protocol")
    _require_values(
        root,
        {
            "schema_version": 1,
            "protocol_version": "5.9-adaptive-shadow-v1",
            "implementation_release": "5.9",
            "evidence_class": "prospective_shadow_hypotheses_only",
            "capital_authority": "none",
        },
        "protocol",
    )
    formal = _mapping(root.get("formal_route"), label="formal_route")
    _require_values(
        formal,
        {
            "protocol_version": "5.0",
            "route": "fixed_core_full",
            "mutation_allowed": False,
            "automatic_promotion_allowed": False,
        },
        "formal_route",
    )
    comparison = _mapping(root.get("comparison"), label="comparison")
    _require_values(
        comparison,
        {
            "control": "formal_fixed_core_full",
            "same_signal_date": True,
            "same_membership_snapshot": True,
            "same_execution_calendar": True,
            "same_initial_capital_fen": True,
            "same_cost_model": True,
            "same_holding_window": True,
            "same_offset_schedule": True,
            "cohort_unit": "candidate_control_pair",
            "complete_candidate_control_pairs_only": True,
            "cross_candidate_completeness_required": False,
            "single_candidate_offset_miss_effect": (
                "permanently_terminate_only_that_candidate_offset"
            ),
            "healthy_candidate_offsets_continue": True,
        },
        "comparison",
    )
    registry = _mapping(root.get("registry"), label="registry")
    if registry.get("maximum_challengers") != 3:
        _fail("registry.maximum_challengers must equal 3")
    if registry.get("ordered_candidates") != list(_EXPECTED_IDS):
        _fail("registry.ordered_candidates differs from the frozen order")
    candidates = registry.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != 2:
        _fail("registry.candidates must contain exactly two candidates")
    normalized: list[Mapping[str, Any]] = []
    for index, expected in enumerate(_EXPECTED_CANDIDATES):
        candidate = _mapping(candidates[index], label=f"registry.candidates[{index}]")
        if candidate != expected:
            _fail(f"registry.candidates[{index}] differs from the frozen 5.9 candidate")
        normalized.append(candidate)
    if registry.get("candidate_change_policy") != (
        "new_minor_release_before_next_signal_no_backfill"
    ):
        _fail("registry candidate change policy differs from 5.9")
    selection_freeze = _mapping(
        registry.get("selection_freeze"), label="registry.selection_freeze"
    )
    if selection_freeze != _EXPECTED_SELECTION_FREEZE:
        _fail("registry.selection_freeze differs from the disclosed 5.9 freeze")
    lineage = _mapping(root.get("feature_lineage"), label="feature_lineage")
    if (
        lineage.get("no_financial_features") is not True
        or lineage.get("no_future_labels") is not True
    ):
        _fail("5.9 shadow features must exclude financial fields and future labels")
    evaluation = _mapping(root.get("evaluation"), label="evaluation")
    if evaluation.get("automatic_promotion_allowed") is not False:
        _fail("5.9 evaluation cannot auto-promote")
    admission = _mapping(root.get("admission"), label="admission")
    _require_values(
        admission,
        {
            "registry_start_rule": "signal_date_strictly_after_activation_start_after",
            "plan_rule": "create_only_at_or_before_formal_trade_deadline",
            "missed_deadline_rule": (
                "append_candidate_offset_missed_and_forbid_only_that_candidate_offset_"
                "backfill_forever"
            ),
            "formal_input_reference_required": True,
            "formal_decision_reference_required": True,
        },
        "admission",
    )
    return ValidatedShadowProtocol(
        protocol_version=str(root["protocol_version"]),
        implementation_release=str(root["implementation_release"]),
        candidate_rows=tuple(normalized),
    )


def build_registry_from_protocol(
    protocol: Mapping[str, Any],
    *,
    release_tag: str,
    commit_oid: str,
    released_at_utc: str | datetime,
    start_after: str | date,
) -> Registry:
    """Construct the challenger registry from a validated mapping, with no I/O."""

    checked = validate_protocol_mapping(protocol)
    if release_tag != checked.implementation_release:
        _fail("release_tag must equal protocol implementation_release")
    start = start_after.isoformat() if isinstance(start_after, date) else start_after
    candidates = tuple(
        CandidateSpec(
            candidate_id=str(row["id"]),
            version=int(row["version"]),
            formula=str(row["formula"]),
            required_fields=tuple(str(value) for value in row["required_fields"]),
            direction=1,
            selection=SelectionSpec(
                top_n=int(row["position_count"]),
                retention_n=int(row["position_count"]) + int(row["retention_buffer"]),
            ),
            selection_disclosure=str(row["selection_disclosure"]),
            start_after=_date(start, label="start_after"),
        )
        for row in checked.candidate_rows
    )
    return Registry(
        protocol_version=checked.protocol_version,
        release_tag=release_tag,
        commit_oid=commit_oid,
        released_at_utc=released_at_utc,
        candidates=candidates,
    )


def _object_value(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        if name not in value:
            _fail(f"verified input is missing {name}")
        return value[name]
    try:
        return getattr(value, name)
    except AttributeError as exc:
        raise AdaptiveShadowPlanningError(
            f"verified input is missing {name}"
        ) from exc


def _frame_records(value: Any) -> list[dict[str, Any]]:
    if hasattr(value, "to_dict"):
        records = value.to_dict(orient="records")
    else:
        records = value
    if isinstance(records, (str, bytes, bytearray)) or not isinstance(records, Sequence):
        _fail("shadow_target_frame must expose a sequence of records")
    output: list[dict[str, Any]] = []
    for index, row in enumerate(records):
        if not isinstance(row, Mapping):
            _fail(f"shadow_target_frame row {index} must be an object")
        output.append(dict(row))
    return output


def _prior_targets(
    value: Mapping[str, Mapping[int, Sequence[str]]] | None,
    *,
    candidate_ids: Sequence[str],
    due_offset: int,
) -> dict[str, tuple[str, ...]]:
    raw = {} if value is None else _mapping(value, label="prior targets")
    unknown = set(raw) - set(candidate_ids)
    if unknown:
        _fail("prior targets contain an unregistered candidate")
    output: dict[str, tuple[str, ...]] = {}
    for candidate_id in candidate_ids:
        offsets = _mapping(raw.get(candidate_id, {}), label=f"prior targets {candidate_id}")
        for offset in offsets:
            if type(offset) is not int or not 0 <= offset < 10:
                _fail("prior target offset must be an integer in [0, 9]")
        selected = offsets.get(due_offset, ())
        if isinstance(selected, (str, bytes, bytearray)) or not isinstance(selected, Sequence):
            _fail("prior targets for an offset must be a sequence")
        output[candidate_id] = tuple(str(ticker) for ticker in selected)
    return output


@dataclass(frozen=True)
class ShadowPlanningResult:
    plan_payloads: tuple[Mapping[str, Any], ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "shadow_plans": [dict(value) for value in self.plan_payloads],
        }


def build_shadow_plan_payloads(
    protocol: Mapping[str, Any],
    registry: Registry,
    verified_input: Any,
    formal_plan: Mapping[str, Any],
    formal_decision_record_sha256: str,
    prior_targets_by_candidate_offset: Mapping[str, Mapping[int, Sequence[str]]] | None,
    created_at_utc: str | datetime,
) -> ShadowPlanningResult:
    """Cross-bind a formal decision and verified shadow frame into two plans."""

    checked = validate_protocol_mapping(protocol)
    if not isinstance(registry, Registry):
        _fail("registry must be Registry")
    if registry.protocol_version != checked.protocol_version:
        _fail("registry protocol version differs from protocol")
    candidate_ids = checked.candidate_ids
    if tuple(candidate.candidate_id for candidate in registry.candidates) != tuple(sorted(candidate_ids)):
        _fail("registry candidates differ from protocol")
    formal = _mapping(formal_plan, label="formal_plan")
    route = _mapping(formal.get("route_target_plan"), label="formal route target plan")
    if route.get("route") != "fixed_core_full":
        _fail("formal route must remain fixed_core_full")
    route_sha = _sha(formal.get("route_target_plan_sha256"), label="formal route plan sha256")
    if canonical_sha256(route) != route_sha:
        _fail("formal route plan SHA does not match its canonical payload")
    decision_sha = _sha(formal_decision_record_sha256, label="formal decision record sha256")
    source_sha = _sha(_object_value(verified_input, "snapshot_sha256"), label="verified input snapshot sha256")
    if _sha(formal.get("source_data_snapshot_sha256"), label="formal source data snapshot sha256") != source_sha:
        _fail("formal plan binds another source data snapshot")
    shadow_sha = _sha(_object_value(verified_input, "shadow_target_rows_sha256"), label="shadow target rows sha256")
    signal_date = _date(_object_value(verified_input, "signal_date"), label="verified signal_date")
    trade_date = _date(_object_value(verified_input, "trade_date"), label="verified trade_date")
    if _date(route.get("signal_date"), label="formal signal_date") != signal_date:
        _fail("formal plan signal_date differs from verified input")
    if _date(route.get("trade_date"), label="formal trade_date") != trade_date:
        _fail("formal plan trade_date differs from verified input")
    if "decision_session" in formal and _date(formal["decision_session"], label="formal decision_session") != trade_date:
        _fail("formal decision_session differs from trade_date")
    calendar_index = _integer(route.get("calendar_index"), label="formal calendar_index")
    due_offset = _integer(route.get("due_offset"), label="formal due_offset")
    if due_offset >= 10 or due_offset != calendar_index % 10:
        _fail("formal due_offset must equal calendar_index modulo ten")
    formal_input_sha = _sha(route.get("input_snapshot_sha256"), label="formal input snapshot sha256")
    deadline = _utc(formal.get("admission_deadline_utc"), label="formal admission deadline")
    if deadline != f"{trade_date}T01:15:00Z":
        _fail("formal admission deadline differs from the released trade deadline")
    created = _utc(created_at_utc, label="created_at_utc")
    prior = _prior_targets(
        prior_targets_by_candidate_offset,
        candidate_ids=candidate_ids,
        due_offset=due_offset,
    )
    source_rows = _frame_records(_object_value(verified_input, "shadow_target_frame"))
    plans: list[Mapping[str, Any]] = []
    for candidate_id in candidate_ids:
        candidate = registry.candidate(candidate_id)
        if len(candidate.required_fields) != 1:
            _fail("5.9 projected shadow scores require exactly one declared field slot")
        score_field = candidate.required_fields[0]
        timing = assess_plan_timing(
            registry=registry,
            candidate_id=candidate_id,
            signal_date=signal_date,
            created_at_utc=created,
            deadline_at_utc=deadline,
        )
        if not timing.admissible:
            _fail(f"shadow plan is inadmissible: {timing.reason}")
        # The verified data layer already evaluated the disclosed formula.  A
        # candidate-specific projection lets the generic generator observe only
        # its declared raw-field slot and never the other challenger's score.
        projected_rows = [
            {
                "date": row.get("date"),
                "ticker": row.get("ticker"),
                "eligible": row.get("shadow_eligible"),
                score_field: row.get(candidate_id),
            }
            for row in source_rows
        ]
        target = generate_targets(
            registry=registry,
            candidate_id=candidate_id,
            signal_date=signal_date,
            rows=projected_rows,
            score=lambda row, field=score_field: row[field],
            previous_targets=prior[candidate_id],
        )
        payload: dict[str, Any] = {
            "schema_version": PLAN_SCHEMA_VERSION,
            "plan_type": "adaptive_shadow_target",
            "candidate_id": candidate_id,
            "candidate_version": candidate.version,
            "signal_date": signal_date,
            "trade_date": trade_date,
            "offset": due_offset,
            "registry_sha256": registry.sha256,
            "candidate_sha256": candidate.sha256,
            "formal_decision_record_sha256": decision_sha,
            "formal_route_target_plan_sha256": route_sha,
            "formal_input_snapshot_sha256": formal_input_sha,
            "source_data_snapshot_sha256": source_sha,
            "shadow_target_rows_sha256": shadow_sha,
            "targets_ppm": dict(target.targets_ppm),
            "cash_ppm": 0,
            "admission_deadline_utc": deadline,
            "created_at_utc": created,
        }
        canonical_json_bytes(payload)
        plans.append(payload)
    return ShadowPlanningResult(tuple(plans))


__all__ = [
    "AdaptiveShadowPlanningError",
    "ShadowPlanningResult",
    "ValidatedShadowProtocol",
    "build_registry_from_protocol",
    "build_shadow_plan_payloads",
    "validate_protocol_mapping",
]
