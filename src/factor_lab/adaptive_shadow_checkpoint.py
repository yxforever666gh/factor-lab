"""Append-only evaluation checkpoints for the 5.9 adaptive shadow.

The evaluator itself is pure.  This module supplies the stateful boundary: it
audits both evidence chains, reconstructs every prior checkpoint from the
chain heads that checkpoint named, and appends when candidate-control pair
evidence or a candidate-specific fatal admission miss changes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import re
from typing import Any
from zoneinfo import ZoneInfo

from .adaptive_shadow import canonical_json_bytes, canonical_sha256
from .adaptive_shadow_evaluation import (
    CONTROL_ID,
    evaluate_shadow_outcomes,
)
from .adaptive_shadow_planning import build_registry_from_protocol
from . import adaptive_shadow_evidence as evidence_bridge
from . import adaptive_shadow_store as shadow_store
from .adaptive_shadow_runtime import load_release_bound_protocol
from .adaptive_shadow_controller import audit_adaptive_shadow_runtime
from . import prospective_ledger as formal_ledger


CHECKPOINT_TYPE = "adaptive_shadow_evaluation_checkpoint"
PROTOCOL_RELATIVE_PATH = "protocols/5.9-adaptive-shadow.json"
CHALLENGER_IDS = evidence_bridge.CHALLENGER_IDS
EXPERT_IDS = (CONTROL_ID, *CHALLENGER_IDS)
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class AdaptiveShadowCheckpointError(ValueError):
    """Raised when a checkpoint cannot be reproduced from sealed evidence."""


@dataclass(frozen=True, slots=True)
class _VerifiedView:
    protocol: dict[str, Any]
    protocol_sha256: str
    registry_sha256: str
    activation_record_sha256: str
    formal_head_record_sha256: str
    shadow_head_record_sha256: str
    formal_records: tuple[dict[str, Any], ...]
    shadow_records: tuple[dict[str, Any], ...]
    evaluation_rows: tuple[dict[str, Any], ...]
    evidence_quality: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _HistoricalState:
    latest_source_evidence_sha256: str | None
    latest_cutoff_date: str | None
    latest_pair_counts: dict[str, int]
    monthly_states: dict[str, dict[str, bool]]


def _mapping(value: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AdaptiveShadowCheckpointError(f"{label} must be an object")
    return dict(value)


def _sha(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise AdaptiveShadowCheckpointError(f"{label} must be a lowercase SHA-256")
    return value


def _day(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise AdaptiveShadowCheckpointError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise AdaptiveShadowCheckpointError(
            f"{label} must be a canonical ISO date"
        ) from exc
    if parsed.isoformat() != value:
        raise AdaptiveShadowCheckpointError(f"{label} must be a canonical ISO date")
    return value


def _utc(value: Any, *, label: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value and value == value.strip():
        try:
            parsed = datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
        except ValueError as exc:
            raise AdaptiveShadowCheckpointError(
                f"{label} must be a timezone-aware ISO timestamp"
            ) from exc
    else:
        raise AdaptiveShadowCheckpointError(
            f"{label} must be a timezone-aware ISO timestamp"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdaptiveShadowCheckpointError(f"{label} must be timezone-aware")
    normalized = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _utc_value(value: Any, *, label: str) -> datetime:
    return datetime.fromisoformat(_utc(value, label=label).replace("Z", "+00:00"))


def _record_kind(metadata: Mapping[str, Any]) -> str:
    record = _mapping(metadata.get("record"), label="shadow record envelope")
    kind = record.get("kind")
    if not isinstance(kind, str):
        raise AdaptiveShadowCheckpointError("shadow record kind is invalid")
    return kind


def _record_sha(metadata: Mapping[str, Any], *, label: str) -> str:
    return _sha(metadata.get("record_sha256"), label=label)


def _sealed_evidence_quality(
    shadow_records: Sequence[Mapping[str, Any]],
    *,
    formal_head: str,
    shadow_head: str,
    deep_replay_valid: bool,
) -> dict[str, Any]:
    candidates: dict[str, dict[str, Any]] = {
        candidate: {
            "missed_deadline_count": 0,
            "missed_record_count": 0,
            "terminated_offset_count": 0,
            "terminated_offsets": [],
            "missed_record_sha256s": [],
        }
        for candidate in CHALLENGER_IDS
    }
    terminated: dict[str, set[int]] = {
        candidate: set() for candidate in CHALLENGER_IDS
    }
    outcome_count = 0
    for metadata in shadow_records:
        kind = _record_kind(metadata)
        if kind == "outcome":
            outcome_count += 1
            continue
        if kind != "missed":
            continue
        payload = _mapping(metadata.get("payload"), label="shadow missed payload")
        candidate = payload.get("candidate_id")
        offset = payload.get("offset")
        reason = payload.get("reason")
        if (
            candidate not in CHALLENGER_IDS
            or type(offset) is not int
            or not 0 <= offset < 10
            or reason
            not in {"missed_deadline", "account_terminated_after_prior_miss"}
        ):
            raise AdaptiveShadowCheckpointError("shadow missed evidence is invalid")
        row = candidates[str(candidate)]
        row["missed_record_count"] += 1
        row["missed_record_sha256s"].append(
            _record_sha(metadata, label="missed record SHA")
        )
        terminated[str(candidate)].add(offset)
        if reason == "missed_deadline":
            row["missed_deadline_count"] += 1
    for candidate, offsets in terminated.items():
        candidates[candidate]["terminated_offsets"] = sorted(offsets)
        candidates[candidate]["terminated_offset_count"] = len(offsets)
    return {
        "schema_version": 1,
        "quality_type": "adaptive_shadow_sealed_evidence_quality",
        "pit_violation_count": 0,
        "integrity_violation_count": 0,
        "deep_replay_valid": deep_replay_valid,
        "formal_source_head_record_sha256": formal_head,
        "shadow_source_head_record_sha256": shadow_head,
        "deep_replayed_outcome_count": outcome_count,
        "candidate_quality": candidates,
    }


def _load_verified_view(
    project_root: str | Path,
    formal_ledger_root: str | Path,
    shadow_store_root: str | Path,
) -> _VerifiedView:
    """Obtain two identical audited evidence reads, then bind the protocol."""

    try:
        deep_audit = audit_adaptive_shadow_runtime(
            project_root,
            formal_ledger_root,
            shadow_store_root,
        )
        if deep_audit.get("valid") is not True:
            raise AdaptiveShadowCheckpointError(
                "shadow runtime failed independent external replay"
            )
        public = evidence_bridge.load_adaptive_shadow_evidence(
            formal_ledger_root,
            shadow_store_root,
        )
        formal_records, formal_head = evidence_bridge._audited_formal_records(
            formal_ledger_root
        )
        shadow_records, shadow_head = evidence_bridge._audited_shadow_records(
            shadow_store_root
        )
        rows = evidence_bridge._assemble_evidence(
            formal_records,
            shadow_records,
        )
    except Exception as exc:
        if isinstance(exc, AdaptiveShadowCheckpointError):
            raise
        raise AdaptiveShadowCheckpointError(
            "formal/shadow evidence failed its audited checkpoint read"
        ) from exc
    if (
        formal_head != public.formal_head_record_sha256
        or shadow_head != public.shadow_head_record_sha256
        or formal_head != deep_audit.get("formal_head_record_sha256")
        or shadow_head != deep_audit.get("shadow_head_record_sha256")
        or canonical_json_bytes(rows) != canonical_json_bytes(public.evaluation_rows)
    ):
        raise AdaptiveShadowCheckpointError(
            "formal or shadow evidence changed across the checkpoint read"
        )
    if not shadow_records or _record_kind(shadow_records[0]) != "activation":
        raise AdaptiveShadowCheckpointError("shadow activation must be the first record")
    activation = _mapping(
        shadow_records[0].get("payload"),
        label="shadow activation",
    )
    root = Path(project_root).expanduser().resolve()
    protocol_path = (root / PROTOCOL_RELATIVE_PATH).resolve()
    try:
        if protocol_path.relative_to(root).as_posix() != PROTOCOL_RELATIVE_PATH:
            raise AdaptiveShadowCheckpointError(
                "adaptive-shadow checkpoint protocol path differs from the frozen path"
            )
    except ValueError as exc:
        raise AdaptiveShadowCheckpointError(
            "adaptive-shadow checkpoint protocol must be inside the project"
        ) from exc
    try:
        protocol, protocol_sha256, relative = load_release_bound_protocol(
            root,
            protocol_path,
            release_commit_oid=str(activation.get("release_commit_oid") or ""),
        )
        registry = build_registry_from_protocol(
            protocol,
            release_tag=str(activation.get("release_tag") or ""),
            commit_oid=str(activation.get("release_commit_oid") or ""),
            released_at_utc=str(activation.get("released_at_utc") or ""),
            start_after=str(activation.get("start_after") or ""),
        )
    except Exception as exc:
        if isinstance(exc, AdaptiveShadowCheckpointError):
            raise
        raise AdaptiveShadowCheckpointError(
            "release-bound 5.9 adaptive-shadow protocol is invalid"
        ) from exc
    if relative != PROTOCOL_RELATIVE_PATH:
        raise AdaptiveShadowCheckpointError(
            "adaptive-shadow checkpoint protocol path differs from the frozen path"
        )
    if (
        protocol.get("schema_version") != 1
        or protocol.get("protocol_version") != "5.9-adaptive-shadow-v1"
        or protocol.get("implementation_release") != "5.9"
    ):
        raise AdaptiveShadowCheckpointError("checkpoint protocol is not frozen 5.9")
    if protocol_sha256 != activation.get("protocol_sha256"):
        raise AdaptiveShadowCheckpointError(
            "checkpoint protocol SHA differs from shadow activation"
        )
    if (
        registry.sha256 != activation.get("registry_sha256")
        or canonical_json_bytes(registry.to_payload())
        != canonical_json_bytes(activation.get("registry"))
    ):
        raise AdaptiveShadowCheckpointError(
            "checkpoint registry differs from shadow activation"
        )
    return _VerifiedView(
        protocol=dict(protocol),
        protocol_sha256=protocol_sha256,
        registry_sha256=registry.sha256,
        activation_record_sha256=_record_sha(
            shadow_records[0], label="activation record SHA"
        ),
        formal_head_record_sha256=formal_head,
        shadow_head_record_sha256=shadow_head,
        formal_records=tuple(dict(row) for row in formal_records),
        shadow_records=tuple(dict(row) for row in shadow_records),
        evaluation_rows=tuple(dict(row) for row in rows),
        evidence_quality=_sealed_evidence_quality(
            shadow_records,
            formal_head=formal_head,
            shadow_head=shadow_head,
            deep_replay_valid=True,
        ),
    )


def _pair_progress(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, int], dict[str, int], int]:
    by_cohort: dict[tuple[str, str, int], dict[str, dict[str, Any]]] = {}
    for index, value in enumerate(rows):
        row = _mapping(value, label=f"evaluation row {index}")
        candidate = row.get("candidate_id")
        if candidate not in EXPERT_IDS:
            raise AdaptiveShadowCheckpointError("evaluation row has an unknown expert")
        signal = _day(row.get("signal_date"), label="evaluation signal_date")
        end = _day(row.get("end_date"), label="evaluation end_date")
        offset = row.get("offset")
        if type(offset) is not int or not 0 <= offset < 10 or end <= signal:
            raise AdaptiveShadowCheckpointError("evaluation row cohort is invalid")
        decision_sha = _sha(
            row.get("formal_decision_record_sha256"),
            label="evaluation formal decision SHA",
        )
        normalized = dict(row)
        normalized["formal_decision_record_sha256"] = decision_sha
        cohort = (signal, end, offset)
        experts = by_cohort.setdefault(cohort, {})
        if candidate in experts:
            raise AdaptiveShadowCheckpointError("duplicate expert in evaluation cohort")
        experts[str(candidate)] = normalized
    complete: dict[str, int] = {}
    incomplete: dict[str, int] = {}
    for candidate in CHALLENGER_IDS:
        relevant = [
            experts
            for experts in by_cohort.values()
            if CONTROL_ID in experts or candidate in experts
        ]
        complete[candidate] = sum(
            CONTROL_ID in experts and candidate in experts for experts in relevant
        )
        incomplete[candidate] = len(relevant) - complete[candidate]
    all_complete = sum(
        set(experts) == set(EXPERT_IDS) for experts in by_cohort.values()
    )
    return complete, incomplete, all_complete


def _availability_index(
    formal_records: Sequence[Mapping[str, Any]],
    shadow_records: Sequence[Mapping[str, Any]],
) -> dict[tuple[str, str, str, int, str], str]:
    result: dict[tuple[str, str, str, int, str], str] = {}
    for metadata in formal_records:
        record = _mapping(metadata.get("record"), label="formal record")
        if record.get("kind") != "outcome":
            continue
        payload = _mapping(record.get("payload"), label="formal outcome")
        cycle = payload.get("cycle_outcome")
        if not isinstance(cycle, Mapping):
            continue
        cycle = dict(cycle)
        key = (
            CONTROL_ID,
            _day(cycle.get("signal_date"), label="formal outcome signal_date"),
            _day(cycle.get("holding_end_date"), label="formal outcome end_date"),
            cycle.get("offset"),
            _sha(
                payload.get("decision_record_sha256"),
                label="formal outcome decision SHA",
            ),
        )
        if type(key[3]) is not int or not 0 <= key[3] < 10:
            raise AdaptiveShadowCheckpointError("formal outcome offset is invalid")
        available = _utc(
            cycle.get("observation_available_at_utc"),
            label="formal outcome availability",
        )
        if key in result:
            raise AdaptiveShadowCheckpointError("duplicate formal outcome availability")
        result[key] = available
    for metadata in shadow_records:
        if _record_kind(metadata) != "outcome":
            continue
        payload = _mapping(metadata.get("payload"), label="shadow outcome")
        cycle = _mapping(payload.get("cycle_outcome"), label="shadow cycle outcome")
        candidate = cycle.get("candidate_id")
        if candidate not in CHALLENGER_IDS:
            raise AdaptiveShadowCheckpointError("shadow outcome candidate is invalid")
        offset = cycle.get("offset")
        if type(offset) is not int or not 0 <= offset < 10:
            raise AdaptiveShadowCheckpointError("shadow outcome offset is invalid")
        key = (
            str(candidate),
            _day(cycle.get("signal_date"), label="shadow outcome signal_date"),
            _day(cycle.get("holding_end_date"), label="shadow outcome end_date"),
            offset,
            _sha(
                cycle.get("formal_decision_record_sha256"),
                label="shadow outcome decision SHA",
            ),
        )
        available = _utc(
            cycle.get("observation_available_at_utc"),
            label="shadow outcome availability",
        )
        if key in result:
            raise AdaptiveShadowCheckpointError("duplicate shadow outcome availability")
        result[key] = available
    return result


def _maximum_relevant_availability(
    complete_rows: Sequence[Mapping[str, Any]],
    formal_records: Sequence[Mapping[str, Any]],
    shadow_records: Sequence[Mapping[str, Any]],
) -> str:
    index = _availability_index(formal_records, shadow_records)
    values: list[str] = []
    for row in complete_rows:
        key = (
            str(row["candidate_id"]),
            str(row["signal_date"]),
            str(row["end_date"]),
            int(row["offset"]),
            str(row["formal_decision_record_sha256"]),
        )
        try:
            values.append(index[key])
        except KeyError as exc:
            raise AdaptiveShadowCheckpointError(
                "complete cohort lacks outcome availability evidence"
            ) from exc
    if not values:
        raise AdaptiveShadowCheckpointError(
            "a checkpoint requires at least one complete cohort"
        )
    return max(values, key=lambda value: _utc_value(value, label="outcome availability"))


def _evidence_cutoff(
    rows: Sequence[Mapping[str, Any]],
    shadow_records: Sequence[Mapping[str, Any]],
) -> str | None:
    dates = [
        _day(row.get("end_date"), label="evaluation row end_date") for row in rows
    ]
    dates.extend(
        _day(
            _mapping(metadata.get("payload"), label="missed payload").get(
                "signal_date"
            ),
            label="missed signal_date",
        )
        for metadata in shadow_records
        if _record_kind(metadata) == "missed"
    )
    return max(dates) if dates else None


def _source_evidence_sha256(
    rows: Sequence[Mapping[str, Any]], evidence_quality: Mapping[str, Any]
) -> str:
    quality = dict(evidence_quality)
    quality.pop("formal_source_head_record_sha256", None)
    quality.pop("shadow_source_head_record_sha256", None)
    return canonical_sha256(
        {
            "ordered_evaluation_rows": [dict(row) for row in rows],
            "evidence_quality_without_chain_heads": quality,
        }
    )


def _maximum_evidence_availability(
    rows: Sequence[Mapping[str, Any]],
    formal_records: Sequence[Mapping[str, Any]],
    shadow_records: Sequence[Mapping[str, Any]],
) -> str:
    values: list[str] = []
    if rows:
        values.append(
            _maximum_relevant_availability(rows, formal_records, shadow_records)
        )
    for metadata in shadow_records:
        if _record_kind(metadata) != "missed":
            continue
        payload = _mapping(metadata.get("payload"), label="missed payload")
        values.extend(
            (
                _utc(payload.get("missed_at_utc"), label="missed availability"),
                _utc(
                    _mapping(metadata.get("record"), label="missed record").get(
                        "recorded_at_utc"
                    ),
                    label="missed recorded_at_utc",
                ),
            )
        )
    if not values:
        raise AdaptiveShadowCheckpointError(
            "a checkpoint requires outcome or missed-deadline evidence"
        )
    return max(values, key=lambda value: _utc_value(value, label="evidence availability"))


def _bound_evaluation(
    report: Mapping[str, Any],
    *,
    observed_at_utc: str,
    relevant_outcome_available_at_utc: str,
    protocol_sha256: str,
    registry_sha256: str,
    activation_record_sha256: str,
    formal_source_head_record_sha256: str,
    shadow_source_head_record_sha256: str,
    evaluation_rows: Sequence[Mapping[str, Any]],
    source_evidence_sha256: str,
) -> dict[str, Any]:
    payload = dict(report)
    payload.pop("evaluation_sha256", None)
    payload.update(
        {
            "checkpoint_type": CHECKPOINT_TYPE,
            "observed_at_utc": observed_at_utc,
            "relevant_outcome_available_at_utc": (
                relevant_outcome_available_at_utc
            ),
            "protocol_sha256": protocol_sha256,
            "registry_sha256": registry_sha256,
            "activation_record_sha256": activation_record_sha256,
            "formal_source_head_record_sha256": (
                formal_source_head_record_sha256
            ),
            "shadow_source_head_record_sha256": (
                shadow_source_head_record_sha256
            ),
            "source_evaluation_row_count": len(evaluation_rows),
            "ordered_evaluation_rows_sha256": canonical_sha256(
                [dict(row) for row in evaluation_rows]
            ),
            "source_evidence_sha256": _sha(
                source_evidence_sha256,
                label="source evidence SHA",
            ),
            "latest_evidence_cutoff_date": str(report["cutoff_date"]),
        }
    )
    payload["evaluation_sha256"] = canonical_sha256(payload)
    return payload


def _candidate_checkpoint_states(
    payload: Mapping[str, Any],
) -> dict[str, tuple[bool, str]]:
    cutoff = _day(payload.get("cutoff_date"), label="evaluation cutoff_date")
    expected_month = cutoff[:7]
    reports = payload.get("candidate_reports")
    if not isinstance(reports, list) or len(reports) != len(CHALLENGER_IDS):
        raise AdaptiveShadowCheckpointError("evaluation candidate reports are invalid")
    parsed: dict[str, tuple[bool, str]] = {}
    for value in reports:
        report = _mapping(value, label="evaluation candidate report")
        candidate = report.get("candidate_id")
        passed = report.get("major_gate_pass_now")
        month = report.get("monthly_state_month")
        if (
            candidate not in CHALLENGER_IDS
            or type(passed) is not bool
            or month != expected_month
        ):
            raise AdaptiveShadowCheckpointError(
                "evaluation candidate pass evidence is invalid"
            )
        if candidate in parsed:
            raise AdaptiveShadowCheckpointError("duplicate evaluation candidate report")
        parsed[str(candidate)] = (passed, expected_month)
    if set(parsed) != set(CHALLENGER_IDS):
        raise AdaptiveShadowCheckpointError("evaluation candidate reports are incomplete")
    return parsed


def _historical_evaluations(
    view: _VerifiedView,
) -> _HistoricalState:
    """Replay each prior checkpoint from the source prefixes it committed to."""

    formal_indexes = {
        _record_sha(row, label="formal record SHA"): index
        for index, row in enumerate(view.formal_records)
    }
    shadow_indexes = {
        _record_sha(row, label="shadow record SHA"): index
        for index, row in enumerate(view.shadow_records)
    }
    monthly_states: dict[str, dict[str, bool]] = {
        candidate: {} for candidate in CHALLENGER_IDS
    }
    latest_pair_counts = {candidate: 0 for candidate in CHALLENGER_IDS}
    latest_source_evidence_sha256: str | None = None
    latest_cutoff: str | None = None
    for record_index, metadata in enumerate(view.shadow_records):
        if _record_kind(metadata) != "evaluation":
            continue
        payload = _mapping(metadata.get("payload"), label="evaluation payload")
        if payload.get("checkpoint_type") != CHECKPOINT_TYPE:
            raise AdaptiveShadowCheckpointError(
                "shadow store contains a non-replayable evaluation record"
            )
        formal_source = _sha(
            payload.get("formal_source_head_record_sha256"),
            label="evaluation formal source head",
        )
        shadow_source = _sha(
            payload.get("shadow_source_head_record_sha256"),
            label="evaluation shadow source head",
        )
        record = _mapping(metadata.get("record"), label="evaluation record")
        if (
            formal_source not in formal_indexes
            or shadow_source not in shadow_indexes
            or shadow_indexes[shadow_source] >= record_index
            or record.get("previous_record_sha256") != shadow_source
        ):
            raise AdaptiveShadowCheckpointError(
                "evaluation source head binding is invalid"
            )
        if (
            payload.get("protocol_sha256") != view.protocol_sha256
            or payload.get("registry_sha256") != view.registry_sha256
            or payload.get("activation_record_sha256")
            != view.activation_record_sha256
        ):
            raise AdaptiveShadowCheckpointError(
                "historical evaluation protocol/registry binding differs"
            )
        formal_prefix = view.formal_records[: formal_indexes[formal_source] + 1]
        shadow_prefix = view.shadow_records[: shadow_indexes[shadow_source] + 1]
        try:
            rows = evidence_bridge._assemble_evidence(
                formal_prefix,
                shadow_prefix,
            )
        except Exception as exc:
            raise AdaptiveShadowCheckpointError(
                "historical evaluation source prefix fails evidence replay"
            ) from exc
        cutoff = _evidence_cutoff(rows, shadow_prefix)
        if cutoff is None:
            raise AdaptiveShadowCheckpointError(
                "historical evaluation has no source evidence"
            )
        quality = _sealed_evidence_quality(
            shadow_prefix,
            formal_head=formal_source,
            shadow_head=shadow_source,
            deep_replay_valid=True,
        )
        source_evidence_sha256 = _source_evidence_sha256(rows, quality)
        relevant_available = _maximum_evidence_availability(
            rows,
            formal_prefix,
            shadow_prefix,
        )
        observed = _utc(
            payload.get("observed_at_utc"), label="evaluation observed_at_utc"
        )
        if (
            _utc_value(observed, label="evaluation observed_at_utc")
            < _utc_value(relevant_available, label="outcome availability")
            or _utc(record.get("recorded_at_utc"), label="evaluation recorded_at_utc")
            != observed
        ):
            raise AdaptiveShadowCheckpointError(
                "historical evaluation predates relevant outcome availability"
            )
        replay = evaluate_shadow_outcomes(
            view.protocol,
            rows,
            cutoff_date=cutoff,
            evaluation_date=_utc_value(
                observed,
                label="evaluation observed_at_utc",
            )
            .astimezone(ZoneInfo("Asia/Shanghai"))
            .date()
            .isoformat(),
            evidence_quality=quality,
            prior_monthly_states=monthly_states,
        )
        expected = _bound_evaluation(
            replay,
            observed_at_utc=observed,
            relevant_outcome_available_at_utc=relevant_available,
            protocol_sha256=view.protocol_sha256,
            registry_sha256=view.registry_sha256,
            activation_record_sha256=view.activation_record_sha256,
            formal_source_head_record_sha256=formal_source,
            shadow_source_head_record_sha256=shadow_source,
            evaluation_rows=rows,
            source_evidence_sha256=source_evidence_sha256,
        )
        if canonical_json_bytes(expected) != canonical_json_bytes(payload):
            raise AdaptiveShadowCheckpointError(
                "historical evaluation record differs from deterministic replay"
            )
        pair_counts, pair_incomplete, all_complete = _pair_progress(rows)
        if (
            payload.get("candidate_pair_complete_cohort_counts") != pair_counts
            or payload.get("candidate_pair_incomplete_cohort_counts")
            != pair_incomplete
            or payload.get("complete_common_cohort_count") != all_complete
            or any(
                pair_counts[candidate] < latest_pair_counts[candidate]
                for candidate in CHALLENGER_IDS
            )
        ):
            raise AdaptiveShadowCheckpointError(
                "historical evaluation pair progress is invalid"
            )
        if latest_cutoff is not None and cutoff < latest_cutoff:
            raise AdaptiveShadowCheckpointError(
                "historical evaluation cutoff moved backward"
            )
        if source_evidence_sha256 == latest_source_evidence_sha256:
            raise AdaptiveShadowCheckpointError(
                "historical evaluation repeats identical source evidence"
            )
        latest_source_evidence_sha256 = source_evidence_sha256
        latest_pair_counts = pair_counts
        latest_cutoff = cutoff
        for candidate, (passed, month) in _candidate_checkpoint_states(payload).items():
            monthly_states[candidate][month] = passed
    return _HistoricalState(
        latest_source_evidence_sha256=latest_source_evidence_sha256,
        latest_cutoff_date=latest_cutoff,
        latest_pair_counts=latest_pair_counts,
        monthly_states={
            candidate: dict(sorted(states.items()))
            for candidate, states in monthly_states.items()
        },
    )


def _confirm_source_heads(
    formal_ledger_root: str | Path,
    shadow_store_root: str | Path,
    *,
    formal_head: str,
    shadow_head: str,
) -> None:
    """Narrow the read/write race before the append-only store takes its lock."""

    formal = formal_ledger.audit_ledger(
        formal_ledger_root,
        refresh_verification_cache=False,
    )
    shadow = shadow_store.audit_shadow_store(shadow_store_root)
    if (
        formal.get("valid") is not True
        or formal.get("head_record_sha256") != formal_head
        or shadow.get("integrity_valid") is not True
        or shadow.get("head_record_sha256") != shadow_head
    ):
        raise AdaptiveShadowCheckpointError(
            "formal or shadow source head changed before checkpoint append"
        )


def checkpoint_adaptive_shadow_evaluation(
    project_root: str | Path,
    formal_ledger_root: str | Path,
    shadow_store_root: str | Path,
    *,
    observed_at_utc: str | datetime,
) -> dict[str, Any]:
    """Append one evaluation iff sealed pair evidence or fatal misses changed.

    ``observed_at_utc`` is deliberately mandatory.  No ambient clock is read,
    and the supplied instant must be no earlier than every outcome included in
    the checkpoint.
    """

    observed = _utc(observed_at_utc, label="observed_at_utc")
    view = _load_verified_view(project_root, formal_ledger_root, shadow_store_root)
    pair_counts, pair_incomplete, all_complete = _pair_progress(
        view.evaluation_rows
    )
    history = _historical_evaluations(view)
    source_evidence_sha256 = _source_evidence_sha256(
        view.evaluation_rows,
        view.evidence_quality,
    )
    cutoff = _evidence_cutoff(view.evaluation_rows, view.shadow_records)
    common = {
        "schema_version": 1,
        "checkpoint_type": CHECKPOINT_TYPE,
        "observed_at_utc": observed,
        "protocol_sha256": view.protocol_sha256,
        "registry_sha256": view.registry_sha256,
        "formal_source_head_record_sha256": view.formal_head_record_sha256,
        "shadow_source_head_record_sha256": view.shadow_head_record_sha256,
        "complete_common_cohort_count": all_complete,
        "candidate_pair_complete_cohort_counts": pair_counts,
        "candidate_pair_incomplete_cohort_counts": pair_incomplete,
        "latest_evaluation_candidate_pair_complete_cohort_counts": (
            history.latest_pair_counts
        ),
        "source_evidence_sha256": source_evidence_sha256,
        "latest_evaluation_source_evidence_sha256": (
            history.latest_source_evidence_sha256
        ),
    }
    if cutoff is None:
        return {
            **common,
            "status": "waiting",
            "reason": "no_outcome_or_missed_evidence",
            "cutoff_date": None,
        }
    if history.latest_cutoff_date is not None and cutoff < history.latest_cutoff_date:
        raise AdaptiveShadowCheckpointError("current evaluation cutoff moved backward")
    if source_evidence_sha256 == history.latest_source_evidence_sha256:
        return {
            **common,
            "status": "waiting",
            "reason": "no_new_source_evidence",
            "cutoff_date": cutoff,
        }
    relevant_available = _maximum_evidence_availability(
        view.evaluation_rows,
        view.formal_records,
        view.shadow_records,
    )
    if _utc_value(observed, label="observed_at_utc") < _utc_value(
        relevant_available,
        label="relevant outcome availability",
    ):
        raise AdaptiveShadowCheckpointError(
            "observed_at_utc predates a relevant outcome availability"
        )
    report = evaluate_shadow_outcomes(
        view.protocol,
        view.evaluation_rows,
        cutoff_date=cutoff,
        evaluation_date=_utc_value(observed, label="observed_at_utc")
        .astimezone(ZoneInfo("Asia/Shanghai"))
        .date()
        .isoformat(),
        evidence_quality=view.evidence_quality,
        prior_monthly_states=history.monthly_states,
    )
    if (
        report.get("complete_common_cohort_count") != all_complete
        or report.get("candidate_pair_complete_cohort_counts") != pair_counts
        or report.get("candidate_pair_incomplete_cohort_counts") != pair_incomplete
    ):
        raise AdaptiveShadowCheckpointError(
            "evaluator pair progress differs from audited evidence"
        )
    payload = _bound_evaluation(
        report,
        observed_at_utc=observed,
        relevant_outcome_available_at_utc=relevant_available,
        protocol_sha256=view.protocol_sha256,
        registry_sha256=view.registry_sha256,
        activation_record_sha256=view.activation_record_sha256,
        formal_source_head_record_sha256=view.formal_head_record_sha256,
        shadow_source_head_record_sha256=view.shadow_head_record_sha256,
        evaluation_rows=view.evaluation_rows,
        source_evidence_sha256=source_evidence_sha256,
    )
    _confirm_source_heads(
        formal_ledger_root,
        shadow_store_root,
        formal_head=view.formal_head_record_sha256,
        shadow_head=view.shadow_head_record_sha256,
    )
    appended = shadow_store.append_shadow_evaluation(
        shadow_store_root,
        payload,
        recorded_at_utc=observed,
        expected_previous_record_sha256=view.shadow_head_record_sha256,
    )
    record = _mapping(appended.get("record"), label="appended evaluation record")
    if record.get("previous_record_sha256") != view.shadow_head_record_sha256:
        raise AdaptiveShadowCheckpointError(
            "checkpoint append did not continue its named shadow source head"
        )
    return {
        **common,
        "status": "checkpointed" if appended.get("created") else "already_checkpointed",
        "reason": "new_sealed_pair_or_miss_evidence",
        "cutoff_date": cutoff,
        "relevant_outcome_available_at_utc": relevant_available,
        "evaluation_sha256": payload["evaluation_sha256"],
        "evaluation_record_sha256": appended.get("record_sha256"),
        "created": bool(appended.get("created")),
        "evaluation": payload,
    }


# Short alias for controller code while retaining a fully explicit public name.
checkpoint_shadow_evaluation = checkpoint_adaptive_shadow_evaluation


__all__ = [
    "AdaptiveShadowCheckpointError",
    "CHECKPOINT_TYPE",
    "PROTOCOL_RELATIVE_PATH",
    "checkpoint_adaptive_shadow_evaluation",
    "checkpoint_shadow_evaluation",
]
