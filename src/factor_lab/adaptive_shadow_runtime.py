"""Release-bound runtime orchestration for the 5.9 adaptive-shadow store.

This module deliberately has a very small write surface.  Activation reads
and audits the formal prospective ledger, verifies the shadow protocol byte
for byte against the published release commit, and then writes only to the
separate adaptive-shadow store.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

from factor_lab.adaptive_shadow import canonical_json_bytes, canonical_sha256
from factor_lab.adaptive_shadow_planning import (
    build_registry_from_protocol,
    build_shadow_plan_payloads,
)
import factor_lab.adaptive_shadow_store as _shadow_store
from factor_lab.adaptive_shadow_store import (
    activate_shadow_store,
    append_shadow_missed,
    append_shadow_plan,
    append_shadow_planning,
)
from factor_lab.data.prospective import load_prospective_input_snapshot
from factor_lab.prospective_ledger import (
    audit_ledger,
    ledger_status,
    strict_load_canonical,
)


RUNTIME_SCHEMA_VERSION = 1
_OID_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class AdaptiveShadowRuntimeError(ValueError):
    """Raised when the release or formal-ledger activation boundary fails."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AdaptiveShadowRuntimeError(
                f"adaptive-shadow protocol contains duplicate key: {key}"
            )
        result[key] = value
    return result


def _utc_text(value: str | datetime | None, *, label: str) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value and value == value.strip():
        try:
            parsed = datetime.fromisoformat(
                value[:-1] + "+00:00" if value.endswith("Z") else value
            )
        except ValueError as exc:
            raise AdaptiveShadowRuntimeError(
                f"{label} must be an ISO timestamp"
            ) from exc
    else:
        raise AdaptiveShadowRuntimeError(f"{label} must be an ISO timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdaptiveShadowRuntimeError(f"{label} must be timezone-aware")
    normalized = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _utc_value(value: str | datetime | None, *, label: str) -> datetime:
    return datetime.fromisoformat(
        _utc_text(value, label=label).replace("Z", "+00:00")
    )


def _iso_day(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise AdaptiveShadowRuntimeError(f"{label} must be a canonical ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise AdaptiveShadowRuntimeError(
            f"{label} must be a canonical ISO date"
        ) from exc
    if parsed.isoformat() != value:
        raise AdaptiveShadowRuntimeError(f"{label} must be a canonical ISO date")
    return value


def _project_file(project_root: Path, requested: str | Path) -> tuple[Path, str]:
    root = Path(project_root).expanduser().resolve()
    raw = Path(requested).expanduser()
    path = (raw if raw.is_absolute() else root / raw).resolve()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise AdaptiveShadowRuntimeError(
            "adaptive-shadow protocol must be inside the project checkout"
        ) from exc
    if path.is_symlink() or not path.is_file():
        raise AdaptiveShadowRuntimeError(
            f"adaptive-shadow protocol is missing or not a regular file: {path}"
        )
    return path, relative


def load_release_bound_protocol(
    project_root: str | Path,
    protocol_path: str | Path,
    *,
    release_commit_oid: str,
) -> tuple[dict[str, Any], str, str]:
    """Load a protocol only when its working bytes equal the tagged Git blob.

    The returned tuple is ``(mapping, sha256, project_relative_path)``.  Git is
    addressed by the already verified commit OID, never by a mutable branch.
    """

    root = Path(project_root).expanduser().resolve()
    commit_oid = str(release_commit_oid)
    if not _OID_RE.fullmatch(commit_oid):
        raise AdaptiveShadowRuntimeError(
            "release_commit_oid must be a lowercase 40-hex Git OID"
        )
    path, relative = _project_file(root, protocol_path)
    working_bytes = path.read_bytes()
    try:
        completed = subprocess.run(
            ["git", "cat-file", "blob", f"{commit_oid}:{relative}"],
            cwd=root,
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as exc:
        raise AdaptiveShadowRuntimeError(
            f"adaptive-shadow protocol is absent from release commit: {relative}"
        ) from exc
    tagged_bytes = completed.stdout
    if working_bytes != tagged_bytes:
        raise AdaptiveShadowRuntimeError(
            "adaptive-shadow protocol bytes differ from the published release commit"
        )
    try:
        payload = json.loads(
            working_bytes.decode("utf-8"), object_pairs_hook=_unique_object
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdaptiveShadowRuntimeError(
            "adaptive-shadow protocol must be unique-key UTF-8 JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise AdaptiveShadowRuntimeError(
            "adaptive-shadow protocol root must be a JSON object"
        )
    return payload, hashlib.sha256(working_bytes).hexdigest(), relative


def _formal_activation_binding(
    formal_ledger_root: str | Path,
) -> dict[str, Any]:
    """Require one valid, still pre-decision formal ledger and return its head."""

    ledger_root = Path(formal_ledger_root).expanduser().resolve()
    audit = audit_ledger(ledger_root, refresh_verification_cache=False)
    status = ledger_status(ledger_root, refresh_verification_cache=False)
    if audit.get("valid") is not True or status.get("valid") is not True:
        raise AdaptiveShadowRuntimeError(
            "formal prospective ledger must pass both audit and verified status"
        )
    audit_state = audit.get("state")
    if not isinstance(audit_state, Mapping):
        raise AdaptiveShadowRuntimeError("formal ledger audit state is missing")
    if audit_state.get("decision_count") != 0 or status.get("decision_count") != 0:
        raise AdaptiveShadowRuntimeError(
            "adaptive-shadow activation is forbidden after the first formal decision"
        )
    audit_head = audit.get("head_record_sha256")
    status_head = status.get("head_record_sha256")
    if (
        not isinstance(audit_head, str)
        or not _SHA_RE.fullmatch(audit_head)
        or status_head != audit_head
    ):
        raise AdaptiveShadowRuntimeError(
            "formal audit/status do not agree on one activated ledger head"
        )
    if audit.get("record_count") in {None, 0} or status.get("record_count") in {
        None,
        0,
    }:
        raise AdaptiveShadowRuntimeError("formal prospective ledger is not activated")
    return {
        "ledger_root": str(ledger_root),
        "head_record_sha256": audit_head,
        "head_sequence": audit.get("head_sequence"),
        "record_count": audit.get("record_count"),
        "decision_count": 0,
        "status": status.get("status"),
    }


def activate_shadow_runtime(
    project_root: str | Path,
    shadow_root: str | Path,
    formal_ledger_root: str | Path,
    *,
    protocol_path: str | Path,
    release_tag: str,
    release_tag_object_oid: str,
    release_commit_oid: str,
    start_after: str,
    released_at_utc: str,
    recorded_at_utc: str,
) -> dict[str, Any]:
    """Validate every external binding, then activate the shadow-only store."""

    object_oid = str(release_tag_object_oid)
    commit_oid = str(release_commit_oid)
    if not _OID_RE.fullmatch(object_oid) or not _OID_RE.fullmatch(commit_oid):
        raise AdaptiveShadowRuntimeError(
            "published tag object and commit must be lowercase 40-hex Git OIDs"
        )
    protocol, protocol_sha256, relative_protocol = load_release_bound_protocol(
        project_root,
        protocol_path,
        release_commit_oid=commit_oid,
    )
    registry = build_registry_from_protocol(
        protocol,
        release_tag=str(release_tag),
        commit_oid=commit_oid,
        released_at_utc=str(released_at_utc),
        start_after=str(start_after),
    )
    formal = _formal_activation_binding(formal_ledger_root)
    activation = activate_shadow_store(
        shadow_root,
        registry=registry,
        release_tag_object_oid=object_oid,
        release_commit_oid=commit_oid,
        protocol_sha256=protocol_sha256,
        formal_head_record_sha256=str(formal["head_record_sha256"]),
        released_at_utc=str(released_at_utc),
        start_after=str(start_after),
        recorded_at_utc=str(recorded_at_utc),
    )
    return {
        "schema_version": RUNTIME_SCHEMA_VERSION,
        "status": "activated" if activation.get("created") else "already_active",
        "shadow_root": str(Path(shadow_root).expanduser().resolve()),
        "protocol_path": relative_protocol,
        "protocol_sha256": protocol_sha256,
        "registry_sha256": registry.sha256,
        "formal_binding": formal,
        "activation": activation,
    }


def _shadow_evidence_snapshot(
    shadow_root: str | Path,
) -> tuple[list[dict[str, Any]], Any]:
    """Read one fully verified store view under the store's own lock."""

    layout = _shadow_store.ShadowLayout.at(shadow_root)
    with _shadow_store._lock(layout, create=False):
        records, state, _orphans = _shadow_store._load(layout)
    if state.activation is None:
        raise AdaptiveShadowRuntimeError("adaptive-shadow store is not activated")
    return records, state


def _activated_registry(
    project_root: str | Path,
    shadow_root: str | Path,
    *,
    protocol_path: str | Path,
) -> tuple[dict[str, Any], Any, list[dict[str, Any]], Any]:
    records, state = _shadow_evidence_snapshot(shadow_root)
    activation = dict(state.activation)
    protocol, protocol_sha256, _relative = load_release_bound_protocol(
        project_root,
        protocol_path,
        release_commit_oid=str(activation["release_commit_oid"]),
    )
    if protocol_sha256 != activation.get("protocol_sha256"):
        raise AdaptiveShadowRuntimeError(
            "release-bound protocol SHA differs from shadow activation"
        )
    registry = build_registry_from_protocol(
        protocol,
        release_tag=str(activation["release_tag"]),
        commit_oid=str(activation["release_commit_oid"]),
        released_at_utc=str(activation["released_at_utc"]),
        start_after=str(activation["start_after"]),
    )
    if (
        registry.sha256 != activation.get("registry_sha256")
        or canonical_json_bytes(registry.to_payload())
        != canonical_json_bytes(activation.get("registry"))
    ):
        raise AdaptiveShadowRuntimeError(
            "reconstructed registry differs from the immutable activation"
        )
    return protocol, registry, records, state


def _load_formal_decision_plan(
    formal_ledger_root: str | Path,
    formal_plan_path: str | Path,
    *,
    formal_decision_record_sha256: str,
    activation_formal_head_record_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    decision_sha = str(formal_decision_record_sha256)
    activation_head = str(activation_formal_head_record_sha256)
    if not _SHA_RE.fullmatch(decision_sha) or not _SHA_RE.fullmatch(activation_head):
        raise AdaptiveShadowRuntimeError(
            "formal decision and activation head must be lowercase SHA-256 values"
        )
    audit = audit_ledger(
        Path(formal_ledger_root).expanduser().resolve(),
        refresh_verification_cache=False,
    )
    if audit.get("valid") is not True:
        raise AdaptiveShadowRuntimeError(
            "formal prospective ledger must be valid before shadow planning"
        )
    rows = audit.get("records")
    if not isinstance(rows, list):
        raise AdaptiveShadowRuntimeError("formal ledger audit records are missing")
    by_sha = {
        str(row.get("record_sha256")): row
        for row in rows
        if isinstance(row, Mapping)
    }
    bound = by_sha.get(activation_head)
    decision = by_sha.get(decision_sha)
    if bound is None or decision is None:
        raise AdaptiveShadowRuntimeError(
            "formal activation head and decision must exist in the same ledger"
        )
    bound_sequence = bound.get("sequence")
    decision_sequence = decision.get("sequence")
    if (
        type(bound_sequence) is not int
        or type(decision_sequence) is not int
        or decision_sequence <= bound_sequence
        or decision.get("kind") != "decision"
    ):
        raise AdaptiveShadowRuntimeError(
            "formal decision does not descend from the shadow activation head"
        )
    record_path = decision.get("path")
    if not isinstance(record_path, str):
        raise AdaptiveShadowRuntimeError("formal decision record path is missing")
    record = strict_load_canonical(Path(record_path).read_bytes())
    if not isinstance(record, Mapping) or record.get("kind") != "decision":
        raise AdaptiveShadowRuntimeError("formal decision record is invalid")
    payload = record.get("payload")
    if not isinstance(payload, Mapping) or not isinstance(payload.get("plan"), Mapping):
        raise AdaptiveShadowRuntimeError("formal decision has no embedded plan")

    plan_path = Path(formal_plan_path).expanduser().resolve()
    if plan_path.is_symlink() or not plan_path.is_file():
        raise AdaptiveShadowRuntimeError(
            f"formal plan is missing or not a regular file: {plan_path}"
        )
    raw_plan = plan_path.read_bytes()
    requested_plan = strict_load_canonical(raw_plan)
    embedded_plan = dict(payload["plan"])
    if (
        not isinstance(requested_plan, dict)
        or canonical_json_bytes(embedded_plan) != raw_plan
        or payload.get("plan_sha256") != hashlib.sha256(raw_plan).hexdigest()
    ):
        raise AdaptiveShadowRuntimeError(
            "formal plan bytes do not match the sealed formal decision"
        )
    return requested_plan, {
        "ledger_root": str(Path(formal_ledger_root).expanduser().resolve()),
        "activation_head_record_sha256": activation_head,
        "decision_record_sha256": decision_sha,
        "decision_sequence": decision_sequence,
        "current_head_record_sha256": audit.get("head_record_sha256"),
    }


def _formal_plan_coordinates(
    formal_plan: Mapping[str, Any],
    verified_input: Any,
    *,
    formal_decision_record_sha256: str,
) -> dict[str, Any]:
    route = formal_plan.get("route_target_plan")
    if not isinstance(route, Mapping) or route.get("route") != "fixed_core_full":
        raise AdaptiveShadowRuntimeError(
            "formal plan must bind the fixed_core_full route"
        )
    route_sha = formal_plan.get("route_target_plan_sha256")
    if not isinstance(route_sha, str) or not _SHA_RE.fullmatch(route_sha):
        raise AdaptiveShadowRuntimeError("formal route plan SHA is invalid")
    if canonical_sha256(route) != route_sha:
        raise AdaptiveShadowRuntimeError("formal route plan SHA does not match")
    source_sha = str(verified_input.snapshot_sha256)
    if (
        not _SHA_RE.fullmatch(source_sha)
        or formal_plan.get("source_data_snapshot_sha256") != source_sha
    ):
        raise AdaptiveShadowRuntimeError(
            "formal plan and verified source snapshot are not cross-bound"
        )
    signal_date = _iso_day(verified_input.signal_date, label="input signal_date")
    trade_date = _iso_day(verified_input.trade_date, label="input trade_date")
    if route.get("signal_date") != signal_date or route.get("trade_date") != trade_date:
        raise AdaptiveShadowRuntimeError(
            "formal route dates differ from the verified source snapshot"
        )
    if (
        "decision_session" in formal_plan
        and formal_plan.get("decision_session") != trade_date
    ):
        raise AdaptiveShadowRuntimeError(
            "formal decision session differs from the verified trade date"
        )
    calendar_index = route.get("calendar_index")
    offset = route.get("due_offset")
    if (
        type(calendar_index) is not int
        or calendar_index < 0
        or type(offset) is not int
        or not 0 <= offset < 10
        or offset != calendar_index % 10
    ):
        raise AdaptiveShadowRuntimeError("formal route offset is invalid")
    formal_input_sha = route.get("input_snapshot_sha256")
    if not isinstance(formal_input_sha, str) or not _SHA_RE.fullmatch(
        formal_input_sha
    ):
        raise AdaptiveShadowRuntimeError("formal target input SHA is invalid")
    deadline = _utc_text(
        formal_plan.get("admission_deadline_utc"), label="formal deadline"
    )
    if deadline != f"{trade_date}T01:15:00Z":
        raise AdaptiveShadowRuntimeError(
            "formal admission deadline differs from the frozen trade deadline"
        )
    decision_sha = str(formal_decision_record_sha256)
    if not _SHA_RE.fullmatch(decision_sha):
        raise AdaptiveShadowRuntimeError("formal decision SHA is invalid")
    return {
        "signal_date": signal_date,
        "trade_date": trade_date,
        "calendar_index": calendar_index,
        "offset": offset,
        "deadline_utc": deadline,
        "formal_route_target_plan_sha256": route_sha,
        "formal_input_snapshot_sha256": formal_input_sha,
        "source_data_snapshot_sha256": source_sha,
        "formal_decision_record_sha256": decision_sha,
    }


def _prior_targets(
    state: Any,
    *,
    candidate_ids: tuple[str, ...],
    signal_date: str,
    due_offset: int,
) -> dict[str, dict[int, list[str]]]:
    prior: dict[str, dict[int, list[str]]] = {
        candidate_id: {} for candidate_id in candidate_ids
    }
    latest: dict[str, tuple[str, dict[str, Any]]] = {}
    for (candidate_id, prior_signal, offset), (_record_sha, _payload_sha, payload) in state.plans.items():
        if (
            candidate_id in prior
            and offset == due_offset
            and prior_signal < signal_date
            and (candidate_id not in latest or prior_signal > latest[candidate_id][0])
        ):
            latest[candidate_id] = (prior_signal, payload)
    for candidate_id, (_prior_signal, payload) in latest.items():
        targets = payload.get("targets_ppm")
        if not isinstance(targets, Mapping):
            raise AdaptiveShadowRuntimeError("stored prior shadow targets are invalid")
        prior[candidate_id][due_offset] = [str(ticker) for ticker in targets]

    return prior


def _existing_decisions(
    records: list[dict[str, Any]],
    state: Any,
    *,
    candidate_ids: tuple[str, ...],
    signal_date: str,
    due_offset: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_record_sha = {str(row["record_sha256"]): row for row in records}
    plans: dict[str, dict[str, Any]] = {}
    missed: dict[str, dict[str, Any]] = {}
    for candidate_id in candidate_ids:
        key = (candidate_id, signal_date, due_offset)
        if key in state.plans:
            record_sha = str(state.plans[key][0])
            plans[candidate_id] = by_record_sha[record_sha]
        if key in state.missed:
            record_sha = str(state.missed[key])
            missed[candidate_id] = by_record_sha[record_sha]
    return plans, missed


def _verify_existing_decision(
    payload: Mapping[str, Any],
    coordinates: Mapping[str, Any],
    *,
    registry_sha256: str,
) -> None:
    expected = {
        "signal_date": coordinates["signal_date"],
        "trade_date": coordinates["trade_date"],
        "offset": coordinates["offset"],
        "registry_sha256": registry_sha256,
        "formal_decision_record_sha256": coordinates[
            "formal_decision_record_sha256"
        ],
        "admission_deadline_utc": coordinates["deadline_utc"],
    }
    if any(payload.get(name) != value for name, value in expected.items()):
        raise AdaptiveShadowRuntimeError(
            "stored shadow decision differs from the requested formal cycle"
        )
    if payload.get("plan_type") == "adaptive_shadow_target" and (
        payload.get("formal_route_target_plan_sha256")
        != coordinates["formal_route_target_plan_sha256"]
        or payload.get("formal_input_snapshot_sha256")
        != coordinates["formal_input_snapshot_sha256"]
        or payload.get("source_data_snapshot_sha256")
        != coordinates["source_data_snapshot_sha256"]
    ):
        raise AdaptiveShadowRuntimeError(
            "stored shadow plan differs from the requested formal/source binding"
        )


def plan_shadow_runtime(
    project_root: str | Path,
    shadow_root: str | Path,
    formal_ledger_root: str | Path,
    *,
    formal_plan_path: str | Path,
    formal_decision_record_sha256: str,
    input_snapshot_path: str | Path,
    created_at_utc: str | datetime | None = None,
    protocol_path: str | Path = "protocols/5.9-adaptive-shadow.json",
) -> dict[str, Any]:
    """Seal both registered challenger plans, or permanently mark them missed."""

    created = _utc_text(created_at_utc, label="created_at_utc")
    protocol, registry, records, state = _activated_registry(
        project_root,
        shadow_root,
        protocol_path=protocol_path,
    )
    activation = dict(state.activation)
    formal_plan, formal_binding = _load_formal_decision_plan(
        formal_ledger_root,
        formal_plan_path,
        formal_decision_record_sha256=str(formal_decision_record_sha256),
        activation_formal_head_record_sha256=str(
            activation["formal_head_record_sha256"]
        ),
    )
    verified_input = load_prospective_input_snapshot(input_snapshot_path)
    coordinates = _formal_plan_coordinates(
        formal_plan,
        verified_input,
        formal_decision_record_sha256=str(formal_decision_record_sha256),
    )
    candidate_ids = tuple(candidate.candidate_id for candidate in registry.candidates)
    existing_plans, existing_missed = _existing_decisions(
        records,
        state,
        candidate_ids=candidate_ids,
        signal_date=str(coordinates["signal_date"]),
        due_offset=int(coordinates["offset"]),
    )
    for metadata in (*existing_plans.values(), *existing_missed.values()):
        _verify_existing_decision(
            metadata["payload"],
            coordinates,
            registry_sha256=registry.sha256,
        )

    prior = _prior_targets(
        state,
        candidate_ids=candidate_ids,
        signal_date=str(coordinates["signal_date"]),
        due_offset=int(coordinates["offset"]),
    )
    plan_results: dict[str, dict[str, Any]] = {
        candidate_id: {**metadata, "created": False}
        for candidate_id, metadata in existing_plans.items()
    }
    missed_results: dict[str, dict[str, Any]] = {
        candidate_id: {**metadata, "created": False}
        for candidate_id, metadata in existing_missed.items()
    }
    undecided = [
        candidate_id
        for candidate_id in candidate_ids
        if candidate_id not in plan_results and candidate_id not in missed_results
    ]
    terminated = [
        candidate_id
        for candidate_id in undecided
        if (candidate_id, int(coordinates["offset"])) in state.terminated_accounts
    ]
    for candidate_id in terminated:
        payload = {
            "schema_version": 1,
            "missed_type": "adaptive_shadow_missed",
            "candidate_id": candidate_id,
            "signal_date": coordinates["signal_date"],
            "trade_date": coordinates["trade_date"],
            "offset": coordinates["offset"],
            "registry_sha256": registry.sha256,
            "formal_decision_record_sha256": str(formal_decision_record_sha256),
            "admission_deadline_utc": coordinates["deadline_utc"],
            "missed_at_utc": created,
            "reason": "account_terminated_after_prior_miss",
        }
        missed_results[candidate_id] = append_shadow_missed(
            shadow_root,
            payload,
            recorded_at_utc=created,
        )
    undecided = [value for value in undecided if value not in set(terminated)]
    planning_result: dict[str, Any] | None = None
    sealed_plans: tuple[Mapping[str, Any], ...] | None = None
    existing_intent = state.planning_intents.get(str(formal_decision_record_sha256))
    if existing_intent is not None:
        by_record_sha = {str(row["record_sha256"]): row for row in records}
        planning_result = {
            **by_record_sha[str(existing_intent[0])],
            "created": False,
        }
        sealed_plans = tuple(
            dict(value) for value in existing_intent[2]["ordered_plan_payloads"]
        )
    deadline = _utc_value(str(coordinates["deadline_utc"]), label="deadline")
    if (
        undecided
        and sealed_plans is None
        and _utc_value(created, label="created_at_utc") <= deadline
    ):
        planning = build_shadow_plan_payloads(
            protocol,
            registry,
            verified_input,
            formal_plan,
            str(formal_decision_record_sha256),
            prior,
            created,
        )
        intent_payload = {
            "schema_version": 1,
            "planning_type": "adaptive_shadow_planning_intent",
            "registry_sha256": registry.sha256,
            "formal_decision_record_sha256": str(formal_decision_record_sha256),
            "signal_date": coordinates["signal_date"],
            "trade_date": coordinates["trade_date"],
            "offset": coordinates["offset"],
            "admission_deadline_utc": coordinates["deadline_utc"],
            "created_at_utc": created,
            "ordered_plan_payloads": [
                dict(value)
                for value in planning.plan_payloads
                if value["candidate_id"] in set(undecided)
            ],
        }
        planning_result = append_shadow_planning(
            shadow_root,
            intent_payload,
            recorded_at_utc=created,
        )
        sealed_plans = tuple(
            dict(value)
            for value in planning.plan_payloads
            if value["candidate_id"] in set(undecided)
        )
    if undecided and sealed_plans is not None:
        assert planning_result is not None
        planning_payload_sha256 = canonical_sha256(planning_result["payload"])
        generated = {
            str(payload["candidate_id"]): {
                **payload,
                "planning_record_sha256": planning_result["record_sha256"],
                "planning_payload_sha256": planning_payload_sha256,
            }
            for payload in sealed_plans
        }
        for candidate_id in undecided:
            plan_results[candidate_id] = append_shadow_plan(
                shadow_root,
                generated[candidate_id],
                recorded_at_utc=created,
            )
    elif undecided:
        for candidate_id in undecided:
            payload = {
                "schema_version": 1,
                "missed_type": "adaptive_shadow_missed",
                "candidate_id": candidate_id,
                "signal_date": coordinates["signal_date"],
                "trade_date": coordinates["trade_date"],
                "offset": coordinates["offset"],
                "registry_sha256": registry.sha256,
                "formal_decision_record_sha256": str(
                    formal_decision_record_sha256
                ),
                "admission_deadline_utc": coordinates["deadline_utc"],
                "missed_at_utc": created,
                "reason": "missed_deadline",
            }
            missed_results[candidate_id] = append_shadow_missed(
                shadow_root,
                payload,
                recorded_at_utc=created,
            )

    return {
        "schema_version": RUNTIME_SCHEMA_VERSION,
        "status": "missed" if missed_results else "planned",
        "shadow_root": str(Path(shadow_root).expanduser().resolve()),
        "registry_sha256": registry.sha256,
        "signal_date": coordinates["signal_date"],
        "trade_date": coordinates["trade_date"],
        "offset": coordinates["offset"],
        "created_at_utc": created,
        "admission_deadline_utc": coordinates["deadline_utc"],
        "source_data_snapshot_sha256": coordinates[
            "source_data_snapshot_sha256"
        ],
        "formal_binding": formal_binding,
        "prior_target_count_by_candidate": {
            candidate_id: len(prior[candidate_id].get(int(coordinates["offset"]), []))
            for candidate_id in candidate_ids
        },
        "planning_intent": planning_result,
        "plans": [plan_results[value] for value in sorted(plan_results)],
        "missed": [missed_results[value] for value in sorted(missed_results)],
    }


__all__ = [
    "AdaptiveShadowRuntimeError",
    "RUNTIME_SCHEMA_VERSION",
    "activate_shadow_runtime",
    "load_release_bound_protocol",
    "plan_shadow_runtime",
]
