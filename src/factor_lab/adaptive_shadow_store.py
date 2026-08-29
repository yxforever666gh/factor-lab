"""Append-only, artifact-first evidence store independent from the formal ledger."""
from __future__ import annotations
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
from typing import Any
from uuid import uuid4
from .adaptive_shadow import Registry, canonical_json_bytes, canonical_sha256
from .adaptive_shadow_execution import (
    AdaptiveShadowExecutionError,
    ShadowCycleOutcome,
    ShadowCyclePlan,
    ShadowExecutionSnapshot,
)
from .prospective_execution import SleeveAccountState
STORE_SCHEMA_VERSION = 1
STORE_ID = "factor-lab/adaptive-shadow/5.9"
_CANDIDATE_IDS = ("low_turnover_20_v1", "low_volatility_252_v1")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_OID_RE = re.compile(r"^[0-9a-f]{40}$")
_RECORD_RE = re.compile(
    r"^(?P<sequence>[0-9]{16})-(?P<kind>activation|planning|plan|missed|outcome|evaluation)-"
    r"(?P<sha>[0-9a-f]{64})\.json$"
)
_ARTIFACT_RE = re.compile(
    r"^(?P<kind>activation|planning|plan|missed|outcome|evaluation)-(?P<sha>[0-9a-f]{64})\.json$"
)
_RECORD_KEYS = {
    "schema_version",
    "store_id",
    "sequence",
    "kind",
    "previous_record_sha256",
    "payload_sha256",
    "payload_filename",
    "recorded_at_utc",
}
_THREAD_LOCK = threading.RLock()
class ShadowStoreError(ValueError):
    """Base error for invalid store operations."""
class ShadowStoreIntegrityError(ShadowStoreError):
    """Raised when immutable bytes, filenames, or the hash chain disagree."""
class ShadowStoreStateError(ShadowStoreError):
    """Raised when a valid payload is inadmissible in the current state."""
@dataclass(frozen=True, slots=True)
class ShadowLayout:
    root: Path
    @classmethod
    def at(cls, root: str | Path) -> "ShadowLayout":
        return cls(Path(root).expanduser().resolve())
    @property
    def records(self) -> Path:
        return self.root / "records"
    @property
    def artifacts(self) -> Path:
        return self.root / "artifacts"
    @property
    def market_windows(self) -> Path:
        return self.root / "market-windows"
    @property
    def lock_path(self) -> Path:
        return self.root / ".append.lock"
    def ensure_directories(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.records.mkdir(exist_ok=True)
        self.artifacts.mkdir(exist_ok=True)
def _layout(value: str | Path | ShadowLayout) -> ShadowLayout:
    return value if isinstance(value, ShadowLayout) else ShadowLayout.at(value)
def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ShadowStoreStateError(f"{label} must be an object")
    return dict(value)
def _exact(value: Mapping[str, Any], keys: set[str], label: str) -> dict[str, Any]:
    row = dict(value)
    if set(row) != keys:
        raise ShadowStoreStateError(f"{label} keys differ from the frozen schema")
    return row
def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ShadowStoreStateError(f"{label} must be a canonical non-empty string")
    return value
def _sha(value: Any, label: str) -> str:
    result = _text(value, label)
    if not _SHA_RE.fullmatch(result):
        raise ShadowStoreStateError(f"{label} must be a lowercase SHA-256")
    return result
def _oid(value: Any, label: str) -> str:
    result = _text(value, label)
    if not _OID_RE.fullmatch(result):
        raise ShadowStoreStateError(f"{label} must be a lowercase 40-hex Git OID")
    return result
def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ShadowStoreStateError(f"{label} must be an integer >= {minimum}")
    return value
def _day(value: Any, label: str) -> str:
    result = _text(value, label)
    try:
        parsed = date.fromisoformat(result)
    except ValueError as exc:
        raise ShadowStoreStateError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != result:
        raise ShadowStoreStateError(f"{label} must be a canonical ISO date")
    return result
def _utc(value: Any, label: str) -> str:
    if isinstance(value, str):
        raw = _text(value, label)
        try:
            parsed = datetime.fromisoformat(
                raw[:-1] + "+00:00" if raw.endswith("Z") else raw
            )
        except ValueError as exc:
            raise ShadowStoreStateError(f"{label} must be an ISO timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise ShadowStoreStateError(f"{label} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowStoreStateError(f"{label} must be timezone-aware")
    parsed = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if parsed.microsecond else "seconds"
    return parsed.isoformat(timespec=timespec).replace("+00:00", "Z")
def _utc_value(value: Any, label: str) -> datetime:
    return datetime.fromisoformat(_utc(value, label).replace("Z", "+00:00"))
def _strict_json(raw: bytes, label: str) -> dict[str, Any]:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {}
        for key, value in values:
            if key in output:
                raise ShadowStoreIntegrityError(f"duplicate JSON key in {label}: {key}")
            output[key] = value
        return output
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ShadowStoreIntegrityError(f"invalid JSON in {label}") from exc
    try:
        canonical = canonical_json_bytes(value)
    except ValueError as exc:
        raise ShadowStoreIntegrityError(f"unsupported canonical value in {label}") from exc
    if not isinstance(value, dict) or canonical != raw:
        raise ShadowStoreIntegrityError(f"non-canonical JSON in {label}")
    return value
def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
def _create_only(path: Path, content: bytes) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.is_file() and not path.is_symlink() and path.read_bytes() == content:
            return False
        raise ShadowStoreIntegrityError(f"create-only collision: {path}")
    temporary = path.parent / f".pending-{os.getpid()}-{uuid4().hex}.tmp"
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_file() and path.read_bytes() == content:
                return False
            raise ShadowStoreIntegrityError(f"concurrent create-only collision: {path}")
        except OSError as exc:
            raise ShadowStoreIntegrityError("filesystem lacks atomic hard-link publish") from exc
        _fsync_directory(path.parent)
        return True
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
@contextmanager
def _lock(layout: ShadowLayout, timeout_seconds: float = 15.0, *, create: bool = True) -> Iterator[None]:
    with _THREAD_LOCK:
        if create:
            layout.ensure_directories()
        elif not layout.records.is_dir() or not layout.artifacts.is_dir() or not layout.lock_path.is_file():
            raise ShadowStoreIntegrityError("shadow layout is incomplete")
        descriptor = os.open(layout.lock_path, os.O_RDWR | (os.O_CREAT if create else 0), 0o600)
        try:
            if os.fstat(descriptor).st_size == 0:
                os.write(descriptor, b"\0")
                os.fsync(descriptor)
            started = time.monotonic()
            if os.name == "nt":
                import msvcrt
                while True:
                    try:
                        os.lseek(descriptor, 0, os.SEEK_SET)
                        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if time.monotonic() - started >= timeout_seconds:
                            raise ShadowStoreIntegrityError("timed out acquiring shadow lock")
                        time.sleep(0.02)
                try:
                    yield
                finally:
                    os.lseek(descriptor, 0, os.SEEK_SET)
                    msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                while True:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        if time.monotonic() - started >= timeout_seconds:
                            raise ShadowStoreIntegrityError("timed out acquiring shadow lock")
                        time.sleep(0.02)
                try:
                    yield
                finally:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
@dataclass
class _State:
    activation: dict[str, Any] | None = None
    activation_sha: str | None = None
    planning_intents: dict[str, tuple[str, str, dict[str, Any], str]] = field(
        default_factory=dict
    )
    plans: dict[tuple[str, str, int], tuple[str, str, dict[str, Any]]] = field(
        default_factory=dict
    )
    plans_by_record: dict[str, dict[str, Any]] = field(default_factory=dict)
    execution_plan_sha_by_record: dict[str, str] = field(default_factory=dict)
    missed: dict[tuple[str, str, int], str] = field(default_factory=dict)
    terminated_accounts: set[tuple[str, int]] = field(default_factory=set)
    outcomes: dict[str, str] = field(default_factory=dict)
    latest_account_states: dict[tuple[str, int], dict[str, Any]] = field(
        default_factory=dict
    )
    evaluation_count: int = 0
def _candidate(state: _State, candidate_id: Any) -> dict[str, Any]:
    identifier = _text(candidate_id, "candidate_id")
    if state.activation is None:
        raise ShadowStoreStateError("shadow store is not activated")
    registry = _mapping(state.activation["registry"], "activation registry")
    for raw in registry["candidates"]:
        candidate = _mapping(raw, "registered candidate")
        if candidate.get("candidate_id") == identifier:
            return candidate
    raise ShadowStoreStateError(f"candidate is not registered: {identifier}")
def _validate_activation(payload: Mapping[str, Any]) -> dict[str, Any]:
    keys = {
        "schema_version", "activation_type", "registry", "registry_sha256",
        "release_tag", "release_tag_object_oid", "release_commit_oid",
        "protocol_sha256", "formal_head_record_sha256", "released_at_utc", "start_after",
    }
    row = _exact(payload, keys, "activation")
    if row["schema_version"] != 1 or row["activation_type"] != "adaptive_shadow_activation":
        raise ShadowStoreStateError("unsupported activation schema")
    registry = _mapping(row["registry"], "activation registry")
    if canonical_sha256(registry) != _sha(row["registry_sha256"], "registry_sha256"):
        raise ShadowStoreStateError("registry SHA does not match its payload")
    if row["release_tag"] != "5.9" or registry.get("release_tag") != "5.9":
        raise ShadowStoreStateError("adaptive shadow store is frozen to release 5.9")
    commit = _oid(row["release_commit_oid"], "release_commit_oid")
    if registry.get("commit_oid") != commit:
        raise ShadowStoreStateError("release commit differs from registry commit")
    _oid(row["release_tag_object_oid"], "release_tag_object_oid")
    _sha(row["protocol_sha256"], "protocol_sha256")
    _sha(row["formal_head_record_sha256"], "formal_head_record_sha256")
    released = _utc(row["released_at_utc"], "released_at_utc")
    start = _day(row["start_after"], "start_after")
    if registry.get("released_at_utc") != released:
        raise ShadowStoreStateError("activation release timestamp differs from registry")
    candidates = registry.get("candidates")
    if not isinstance(candidates, list) or tuple(
        sorted(str(value.get("candidate_id")) for value in candidates if isinstance(value, Mapping))
    ) != _CANDIDATE_IDS:
        raise ShadowStoreStateError("activation must bind the two frozen 5.9 challengers")
    if any(not isinstance(value, Mapping) or value.get("start_after") != start for value in candidates):
        raise ShadowStoreStateError("activation start_after differs from a candidate")
    return row
_PLAN_BASE_KEYS = {
    "schema_version", "plan_type", "candidate_id", "candidate_version",
    "signal_date", "trade_date", "offset", "registry_sha256", "candidate_sha256",
    "formal_decision_record_sha256", "formal_route_target_plan_sha256",
    "formal_input_snapshot_sha256", "source_data_snapshot_sha256",
    "shadow_target_rows_sha256", "targets_ppm", "cash_ppm",
    "admission_deadline_utc", "created_at_utc",
}
_PLAN_KEYS = _PLAN_BASE_KEYS | {
    "planning_record_sha256",
    "planning_payload_sha256",
}


_PLANNING_KEYS = {
    "schema_version", "planning_type", "registry_sha256",
    "formal_decision_record_sha256", "signal_date", "trade_date", "offset",
    "admission_deadline_utc", "created_at_utc", "ordered_plan_payloads",
}


def _plan_base_key(
    state: _State,
    row: Mapping[str, Any],
) -> tuple[tuple[str, str, int], str]:
    row = _exact(row, _PLAN_BASE_KEYS, "base plan")
    candidate = _candidate(state, row.get("candidate_id"))
    signal = _day(row.get("signal_date"), "plan signal_date")
    trade = _day(row.get("trade_date"), "plan trade_date")
    offset = _integer(row.get("offset"), "plan offset")
    if offset >= 10 or trade <= signal:
        raise ShadowStoreStateError("plan trade/offset is invalid")
    if (str(row.get("candidate_id")), offset) in state.terminated_accounts:
        raise ShadowStoreStateError("a permanently terminated candidate/offset cannot plan again")
    if row.get("candidate_version") != candidate.get("version"):
        raise ShadowStoreStateError("plan candidate version differs from registry")
    if _sha(row.get("candidate_sha256"), "candidate_sha256") != canonical_sha256(candidate):
        raise ShadowStoreStateError("plan candidate SHA differs from registry")
    assert state.activation is not None
    if _sha(row.get("registry_sha256"), "registry_sha256") != state.activation["registry_sha256"]:
        raise ShadowStoreStateError("plan registry SHA differs from activation")
    for name in (
        "formal_decision_record_sha256", "formal_route_target_plan_sha256",
        "formal_input_snapshot_sha256", "source_data_snapshot_sha256",
        "shadow_target_rows_sha256",
    ):
        _sha(row.get(name), name)
    targets = _mapping(row.get("targets_ppm"), "targets_ppm")
    top_n = int(_mapping(candidate.get("selection"), "candidate selection")["top_n"])
    if len(targets) != top_n:
        raise ShadowStoreStateError("plan target count differs from registered Top-N")
    weights = [_integer(value, "target PPM", minimum=1) for value in targets.values()]
    if any(not isinstance(ticker, str) or not ticker for ticker in targets):
        raise ShadowStoreStateError("target ticker must be non-empty")
    if sum(weights) != 1_000_000 or row.get("cash_ppm") != 0:
        raise ShadowStoreStateError("plan must be fully invested long-only")
    deadline = _utc_value(row.get("admission_deadline_utc"), "plan deadline")
    created = _utc_value(row.get("created_at_utc"), "plan created_at")
    if _utc(row.get("admission_deadline_utc"), "plan deadline") != f"{trade}T01:15:00Z":
        raise ShadowStoreStateError("plan deadline differs from formal trade deadline")
    if created > deadline:
        raise ShadowStoreStateError("plan was created after its deadline")
    released = _utc_value(state.activation["released_at_utc"], "activation release")
    if created < released or signal <= released.date().isoformat() or signal <= candidate["start_after"]:
        raise ShadowStoreStateError("plan predates activation/start_after")
    try:
        execution_plan = ShadowCyclePlan(
            registry_sha256=str(row["registry_sha256"]),
            candidate_id=str(row["candidate_id"]),
            candidate_sha256=str(row["candidate_sha256"]),
            offset=offset,
            signal_date=signal,
            trade_date=trade,
            targets_ppm=targets,
            formal_input_snapshot_sha256=str(row["formal_input_snapshot_sha256"]),
            formal_decision_record_sha256=str(row["formal_decision_record_sha256"]),
            planned_at_utc=str(row["created_at_utc"]),
            formal_trade_deadline_utc=str(row["admission_deadline_utc"]),
        )
    except (AdaptiveShadowExecutionError, TypeError) as exc:
        raise ShadowStoreStateError("plan cannot form a replayable execution plan") from exc
    return ((_text(row["candidate_id"], "candidate_id"), signal, offset), execution_plan.plan_sha256)


def _validate_planning(
    state: _State,
    payload: Mapping[str, Any],
) -> tuple[str, tuple[dict[str, Any], ...]]:
    row = _exact(payload, _PLANNING_KEYS, "planning intent")
    if row["schema_version"] != 1 or row["planning_type"] != "adaptive_shadow_planning_intent":
        raise ShadowStoreStateError("unsupported planning-intent schema")
    assert state.activation is not None
    if _sha(row["registry_sha256"], "registry_sha256") != state.activation["registry_sha256"]:
        raise ShadowStoreStateError("planning registry SHA differs from activation")
    decision_sha = _sha(
        row["formal_decision_record_sha256"],
        "formal_decision_record_sha256",
    )
    signal = _day(row["signal_date"], "planning signal_date")
    trade = _day(row["trade_date"], "planning trade_date")
    offset = _integer(row["offset"], "planning offset")
    deadline = _utc_value(row["admission_deadline_utc"], "planning deadline")
    created = _utc_value(row["created_at_utc"], "planning created_at")
    if (
        offset >= 10
        or trade <= signal
        or _utc(row["admission_deadline_utc"], "planning deadline")
        != f"{trade}T01:15:00Z"
        or created > deadline
    ):
        raise ShadowStoreStateError("planning coordinates or timing are invalid")
    plans = row["ordered_plan_payloads"]
    expected_candidates = tuple(
        candidate_id
        for candidate_id in _CANDIDATE_IDS
        if (candidate_id, offset) not in state.terminated_accounts
    )
    if (
        not expected_candidates
        or not isinstance(plans, list)
        or len(plans) != len(expected_candidates)
    ):
        raise ShadowStoreStateError("planning intent must seal every active challenger plan")
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in plans:
        plan = _exact(_mapping(value, "planning plan"), _PLAN_BASE_KEYS, "planning plan")
        key, _execution_sha = _plan_base_key(state, plan)
        expected = {
            "formal_decision_record_sha256": decision_sha,
            "registry_sha256": row["registry_sha256"],
            "signal_date": signal,
            "trade_date": trade,
            "offset": offset,
            "admission_deadline_utc": row["admission_deadline_utc"],
            "created_at_utc": row["created_at_utc"],
        }
        if any(plan.get(name) != value for name, value in expected.items()):
            raise ShadowStoreStateError("planning intent contains differently bound plans")
        if key[0] in seen:
            raise ShadowStoreStateError("planning intent repeats a challenger")
        seen.add(key[0])
        normalized.append(plan)
    if tuple(plan["candidate_id"] for plan in normalized) != expected_candidates:
        raise ShadowStoreStateError("planning intent challenger order differs")
    return decision_sha, tuple(normalized)


def _plan_key(state: _State, row: Mapping[str, Any]) -> tuple[tuple[str, str, int], str]:
    full = _exact(row, _PLAN_KEYS, "plan")
    base = {name: full[name] for name in _PLAN_BASE_KEYS}
    key, execution_sha = _plan_base_key(state, base)
    intent = state.planning_intents.get(str(base["formal_decision_record_sha256"]))
    if intent is None:
        raise ShadowStoreStateError("plan references no prior planning intent")
    if (
        _sha(full["planning_record_sha256"], "planning_record_sha256") != intent[0]
        or _sha(full["planning_payload_sha256"], "planning_payload_sha256") != intent[1]
    ):
        raise ShadowStoreStateError("plan planning-intent binding differs")
    sealed = {
        str(plan["candidate_id"]): plan
        for plan in intent[2]["ordered_plan_payloads"]
    }
    if canonical_json_bytes(sealed.get(str(base["candidate_id"]))) != canonical_json_bytes(base):
        raise ShadowStoreStateError("plan bytes differ from the pre-deadline planning intent")
    return key, execution_sha
def _validate_missed(state: _State, payload: Mapping[str, Any]) -> tuple[str, str, int]:
    keys = {
        "schema_version", "missed_type", "candidate_id", "signal_date", "trade_date",
        "offset", "registry_sha256", "formal_decision_record_sha256",
        "admission_deadline_utc", "missed_at_utc", "reason",
    }
    row = _exact(payload, keys, "missed")
    if row["schema_version"] != 1 or row["missed_type"] != "adaptive_shadow_missed":
        raise ShadowStoreStateError("unsupported missed schema")
    candidate = _candidate(state, row["candidate_id"])
    signal = _day(row["signal_date"], "missed signal_date")
    trade = _day(row["trade_date"], "missed trade_date")
    offset = _integer(row["offset"], "missed offset")
    if offset >= 10 or trade <= signal or row["reason"] not in {
        "missed_deadline",
        "account_terminated_after_prior_miss",
    }:
        raise ShadowStoreStateError("invalid missed decision")
    assert state.activation is not None
    if _sha(row["registry_sha256"], "registry_sha256") != state.activation["registry_sha256"]:
        raise ShadowStoreStateError("missed registry SHA differs from activation")
    _sha(row["formal_decision_record_sha256"], "formal decision SHA")
    deadline = _utc_value(row["admission_deadline_utc"], "missed deadline")
    missed = _utc_value(row["missed_at_utc"], "missed_at_utc")
    account_key = (str(row["candidate_id"]), offset)
    if _utc(row["admission_deadline_utc"], "missed deadline") != f"{trade}T01:15:00Z":
        raise ShadowStoreStateError("missed deadline differs from the formal trade deadline")
    if row["reason"] == "missed_deadline":
        if missed <= deadline or account_key in state.terminated_accounts:
            raise ShadowStoreStateError(
                "initial missed record requires a live account and post-deadline observation"
            )
    elif account_key not in state.terminated_accounts:
        raise ShadowStoreStateError(
            "terminated-account missed record requires a prior fatal miss"
        )
    if signal <= state.activation["start_after"] or signal <= candidate["start_after"]:
        raise ShadowStoreStateError("missed signal predates activation")
    if str(row["formal_decision_record_sha256"]) in state.planning_intents:
        raise ShadowStoreStateError(
            "a pre-deadline planning intent forbids a missed classification"
        )
    return (_text(row["candidate_id"], "candidate_id"), signal, offset)
def _validate_outcome(state: _State, payload: Mapping[str, Any]) -> tuple[str, ShadowCycleOutcome]:
    keys = {
        "schema_version", "outcome_type", "plan_record_sha256", "cycle_outcome",
        "formal_execution_snapshot_sha256",
        "shadow_market_source_contract_sha256",
        "shadow_market_bundle_sha256",
    }
    row = _exact(payload, keys, "outcome")
    if row["schema_version"] != 1 or row["outcome_type"] != "adaptive_shadow_outcome":
        raise ShadowStoreStateError("unsupported outcome schema")
    plan_sha = _sha(row["plan_record_sha256"], "plan_record_sha256")
    plan = state.plans_by_record.get(plan_sha)
    if plan is None:
        raise ShadowStoreStateError("outcome references no stored plan")
    try:
        cycle = ShadowCycleOutcome.from_mapping(_mapping(row["cycle_outcome"], "cycle_outcome"))
    except (AdaptiveShadowExecutionError, TypeError) as exc:
        raise ShadowStoreStateError("cycle_outcome is not a replayable shadow outcome") from exc
    assert state.activation is not None
    expected = {
        "registry_sha256": state.activation["registry_sha256"],
        "candidate_id": plan["candidate_id"],
        "candidate_sha256": plan["candidate_sha256"],
        "account_deployment_sha256": canonical_sha256(
            {
                "candidate_id": plan["candidate_id"],
                "candidate_sha256": plan["candidate_sha256"],
                "kind": "adaptive_shadow_candidate_accounts_v1",
                "registry_sha256": state.activation["registry_sha256"],
            }
        ),
        "target_plan_sha256": state.execution_plan_sha_by_record[plan_sha],
        "formal_input_snapshot_sha256": plan["formal_input_snapshot_sha256"],
        "formal_decision_record_sha256": plan["formal_decision_record_sha256"],
        "offset": plan["offset"],
        "signal_date": plan["signal_date"],
        "holding_start_date": plan["trade_date"],
    }
    if any(getattr(cycle, name) != value for name, value in expected.items()):
        raise ShadowStoreStateError("cycle outcome identity or formal bindings differ from plan")
    formal_execution_sha = _sha(
        row["formal_execution_snapshot_sha256"],
        "formal_execution_snapshot_sha256",
    )
    source_sha = _sha(
        row["shadow_market_source_contract_sha256"],
        "shadow_market_source_contract_sha256",
    )
    bundle_sha = _sha(row["shadow_market_bundle_sha256"], "shadow_market_bundle_sha256")
    expected_bundle_sha = canonical_sha256(
        {
            "shadow_execution_snapshot_sha256": cycle.shadow_execution_snapshot_sha256,
            "shadow_market_source_contract_sha256": source_sha,
        }
    )
    if bundle_sha != expected_bundle_sha:
        raise ShadowStoreStateError("shadow market bundle SHA differs from its bound identities")
    if formal_execution_sha == cycle.market_execution_snapshot_sha256:
        raise ShadowStoreStateError(
            "formal and route-neutral shadow market snapshot identities must remain distinct"
        )
    return plan_sha, cycle


def _validate_market_bundle(layout: ShadowLayout, payload: Mapping[str, Any]) -> None:
    """Verify every outcome-referenced market window from immutable local bytes."""

    bundle_sha = _sha(payload.get("shadow_market_bundle_sha256"), "shadow market bundle SHA")
    source_sha = _sha(
        payload.get("shadow_market_source_contract_sha256"),
        "shadow market source contract SHA",
    )
    formal_sha = _sha(
        payload.get("formal_execution_snapshot_sha256"),
        "formal execution snapshot SHA",
    )
    directory = layout.market_windows / bundle_sha
    if directory.is_symlink() or not directory.is_dir():
        raise ShadowStoreIntegrityError("outcome-referenced shadow market bundle is missing")
    entries = sorted(path.name for path in directory.iterdir())
    if entries != ["snapshot.json", "sources.json"]:
        raise ShadowStoreIntegrityError("shadow market bundle contains unexpected entries")
    snapshot_path = directory / "snapshot.json"
    sources_path = directory / "sources.json"
    if any(path.is_symlink() or not path.is_file() for path in (snapshot_path, sources_path)):
        raise ShadowStoreIntegrityError("shadow market bundle files are missing or symlinked")
    snapshot_raw = snapshot_path.read_bytes()
    sources_raw = sources_path.read_bytes()
    if hashlib.sha256(sources_raw).hexdigest() != source_sha:
        raise ShadowStoreIntegrityError("shadow market source contract hash mismatch")
    snapshot_value = _strict_json(snapshot_raw, f"market-windows/{bundle_sha}/snapshot.json")
    sources_value = _strict_json(sources_raw, f"market-windows/{bundle_sha}/sources.json")
    try:
        wrapper = ShadowExecutionSnapshot.from_mapping(snapshot_value)
    except (AdaptiveShadowExecutionError, TypeError) as exc:
        raise ShadowStoreIntegrityError("shadow market snapshot contract is invalid") from exc
    try:
        cycle = ShadowCycleOutcome.from_mapping(
            _mapping(payload.get("cycle_outcome"), "cycle_outcome")
        )
    except (AdaptiveShadowExecutionError, TypeError) as exc:
        raise ShadowStoreIntegrityError("shadow outcome contract is invalid") from exc
    expected_bundle_sha = canonical_sha256(
        {
            "shadow_execution_snapshot_sha256": wrapper.snapshot_sha256,
            "shadow_market_source_contract_sha256": source_sha,
        }
    )
    formal_bundle = sources_value.get("formal_execution_bundle")
    bindings = (
        expected_bundle_sha == bundle_sha,
        wrapper.snapshot_sha256 == cycle.shadow_execution_snapshot_sha256,
        wrapper.execution_snapshot.snapshot_sha256 == cycle.market_execution_snapshot_sha256,
        wrapper.execution_snapshot.execution_source_sha256 == source_sha,
        sources_value.get("formal_execution_snapshot_sha256") == formal_sha,
        isinstance(formal_bundle, Mapping),
        isinstance(formal_bundle, Mapping)
        and formal_bundle.get("snapshot_sha256") == formal_sha,
        sources_value.get("target_plan_sha256") == cycle.target_plan_sha256,
        sources_value.get("formal_decision_record_sha256")
        == cycle.formal_decision_record_sha256,
    )
    if not all(bindings):
        raise ShadowStoreIntegrityError("shadow outcome and market bundle bindings differ")
def _validate_evaluation(state: _State, payload: Mapping[str, Any]) -> None:
    row = _mapping(payload, "evaluation")
    if row.get("schema_version") != 1:
        raise ShadowStoreStateError("unsupported evaluation schema")
    if row.get("conclusion") not in {"continue", "retire", "eligible_for_major_review"}:
        raise ShadowStoreStateError("invalid evaluation conclusion")
    if row.get("automatic_promotion_allowed") is not False:
        raise ShadowStoreStateError("evaluation cannot auto-promote")
    if row.get("candidate_ids") != list(_CANDIDATE_IDS):
        raise ShadowStoreStateError("evaluation candidates differ from activation")
    supplied = _sha(row.get("evaluation_sha256"), "evaluation_sha256")
    unsigned = dict(row)
    unsigned.pop("evaluation_sha256")
    if canonical_sha256(unsigned) != supplied:
        raise ShadowStoreStateError("evaluation SHA does not match its payload")
def _apply(
    state: _State,
    kind: str,
    payload: Mapping[str, Any],
    record_sha: str,
    recorded_at_utc: str,
) -> None:
    if kind == "activation":
        row = _validate_activation(payload)
        if state.activation is not None:
            raise ShadowStoreStateError("shadow store is already activated")
        if _utc_value(recorded_at_utc, "recorded_at_utc") < _utc_value(row["released_at_utc"], "released_at_utc"):
            raise ShadowStoreStateError("activation cannot be recorded before release")
        state.activation, state.activation_sha = row, record_sha
        return
    if state.activation is None:
        raise ShadowStoreStateError("activation must be the first record")
    if kind == "planning":
        decision_sha, _plans = _validate_planning(state, payload)
        if decision_sha in state.planning_intents:
            raise ShadowStoreStateError("formal decision already has a planning intent")
        if any(
            key[1] == payload["signal_date"] for key in state.missed
        ):
            raise ShadowStoreStateError(
                "planning intent cannot follow a missed classification"
            )
        row = dict(payload)
        recorded = _utc_value(recorded_at_utc, "recorded_at_utc")
        created = _utc_value(row["created_at_utc"], "planning created_at_utc")
        deadline = _utc_value(row["admission_deadline_utc"], "planning deadline")
        if not created <= recorded <= deadline:
            raise ShadowStoreStateError(
                "planning intent must be recorded from creation through deadline"
            )
        state.planning_intents[decision_sha] = (
            record_sha,
            canonical_sha256(row),
            row,
            _utc(recorded_at_utc, "recorded_at_utc"),
        )
    elif kind == "plan":
        row = _exact(payload, _PLAN_KEYS, "plan")
        if row["schema_version"] != 1 or row["plan_type"] != "adaptive_shadow_target":
            raise ShadowStoreStateError("unsupported plan schema")
        key, execution_plan_sha = _plan_key(state, row)
        if key in state.plans or key in state.missed:
            raise ShadowStoreStateError("plan identity is already decided or permanently missed")
        created = _utc_value(row["created_at_utc"], "created_at_utc")
        deadline = _utc_value(row["admission_deadline_utc"], "deadline")
        intent = state.planning_intents[str(row["formal_decision_record_sha256"])]
        intent_recorded = _utc_value(intent[3], "planning intent recorded_at_utc")
        recorded = _utc_value(recorded_at_utc, "recorded_at_utc")
        if not created <= intent_recorded <= deadline or recorded < intent_recorded:
            raise ShadowStoreStateError(
                "plan lacks a pre-deadline intent or predates that intent"
            )
        state.plans[key] = (record_sha, canonical_sha256(row), row)
        state.plans_by_record[record_sha] = row
        state.execution_plan_sha_by_record[record_sha] = execution_plan_sha
    elif kind == "missed":
        key = _validate_missed(state, payload)
        if key in state.plans or key in state.missed:
            raise ShadowStoreStateError("missed identity is already decided")
        row = dict(payload)
        if _utc_value(recorded_at_utc, "recorded_at_utc") < _utc_value(row["missed_at_utc"], "missed_at_utc"):
            raise ShadowStoreStateError("missed record time cannot precede missed_at")
        state.missed[key] = record_sha
        state.terminated_accounts.add((key[0], key[2]))
    elif kind == "outcome":
        plan_sha, cycle = _validate_outcome(state, payload)
        if plan_sha in state.outcomes:
            raise ShadowStoreStateError("plan already has an outcome")
        account_key = (cycle.candidate_id, cycle.offset)
        prior_payload = state.latest_account_states.get(account_key)
        try:
            prior = (
                SleeveAccountState.genesis(
                    deployment_sha256=cycle.account_deployment_sha256,
                    offset=cycle.offset,
                )
                if prior_payload is None
                else SleeveAccountState.from_mapping(prior_payload)
            )
        except (ValueError, TypeError) as exc:
            raise ShadowStoreStateError("stored shadow account state is invalid") from exc
        if cycle.previous_account_state_sha256 != prior.state_sha256:
            raise ShadowStoreStateError(
                "shadow outcome does not continue the latest candidate/offset account"
            )
        if (
            prior.cycle_count > 0
            and prior.last_holding_end_date != cycle.holding_start_date
        ):
            raise ShadowStoreStateError(
                "shadow outcome is not continuous at the same-offset boundary"
            )
        if _utc_value(recorded_at_utc, "recorded_at_utc") < _utc_value(cycle.observation_available_at_utc, "observation available"):
            raise ShadowStoreStateError("outcome cannot be recorded before it became available")
        state.outcomes[plan_sha] = record_sha
        state.latest_account_states[account_key] = cycle.next_account_state.to_dict()
    elif kind == "evaluation":
        _validate_evaluation(state, payload)
        state.evaluation_count += 1
    else:
        raise ShadowStoreStateError(f"unsupported record kind: {kind}")
def _artifact(layout: ShadowLayout, filename: str, sha256: str) -> dict[str, Any]:
    path = layout.artifacts / filename
    if path.is_symlink() or not path.is_file():
        raise ShadowStoreIntegrityError(f"payload artifact is missing or not regular: {filename}")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != sha256:
        raise ShadowStoreIntegrityError(f"payload artifact hash mismatch: {filename}")
    return _strict_json(raw, filename)
def _load(layout: ShadowLayout) -> tuple[list[dict[str, Any]], _State, list[str]]:
    if not layout.root.exists():
        return [], _State(), []
    if layout.root.is_symlink() or not layout.root.is_dir():
        raise ShadowStoreIntegrityError("shadow root is not a regular directory")
    for directory in (layout.records, layout.artifacts):
        if directory.is_symlink() or not directory.is_dir():
            raise ShadowStoreIntegrityError(f"shadow layout is incomplete: {directory}")
    records: list[dict[str, Any]] = []
    state = _State()
    previous: str | None = None
    referenced: set[str] = set()
    paths = sorted(layout.records.iterdir())
    for expected, path in enumerate(paths, start=1):
        match = _RECORD_RE.fullmatch(path.name)
        if path.is_symlink() or not path.is_file() or match is None:
            raise ShadowStoreIntegrityError(f"unexpected record entry: {path.name}")
        raw = path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if digest != match.group("sha"):
            raise ShadowStoreIntegrityError(f"record filename/content hash mismatch: {path.name}")
        row = _exact(_strict_json(raw, path.name), _RECORD_KEYS, "record")
        if (
            row["schema_version"] != STORE_SCHEMA_VERSION
            or row["store_id"] != STORE_ID
            or row["sequence"] != expected
            or int(match.group("sequence")) != expected
            or row["kind"] != match.group("kind")
            or row["previous_record_sha256"] != previous
        ):
            raise ShadowStoreIntegrityError(f"record chain mismatch: {path.name}")
        payload_sha = _sha(row["payload_sha256"], "record payload SHA")
        filename = _text(row["payload_filename"], "record payload filename")
        if filename != f"{row['kind']}-{payload_sha}.json":
            raise ShadowStoreIntegrityError(f"record payload filename mismatch: {path.name}")
        _utc(row["recorded_at_utc"], "recorded_at_utc")
        payload = _artifact(layout, filename, payload_sha)
        if row["kind"] == "outcome":
            _validate_market_bundle(layout, payload)
        _apply(state, str(row["kind"]), payload, digest, str(row["recorded_at_utc"]))
        records.append({"record_sha256": digest, "record": row, "payload": payload, "path": str(path)})
        referenced.add(filename)
        previous = digest
    artifacts: list[str] = []
    for path in sorted(layout.artifacts.iterdir()):
        match = _ARTIFACT_RE.fullmatch(path.name)
        if path.is_symlink() or not path.is_file() or match is None:
            raise ShadowStoreIntegrityError(f"unexpected artifact entry: {path.name}")
        _artifact(layout, path.name, match.group("sha"))
        artifacts.append(path.name)
    return records, state, sorted(set(artifacts) - referenced)
def _append(
    root: str | Path | ShadowLayout,
    kind: str,
    payload: Mapping[str, Any],
    recorded_at_utc: str | datetime,
    *,
    expected_previous_record_sha256: str | None = None,
) -> dict[str, Any]:
    layout = _layout(root)
    raw_payload = canonical_json_bytes(payload)
    normalized = _strict_json(raw_payload, f"{kind} payload")
    payload_sha = hashlib.sha256(raw_payload).hexdigest()
    filename = f"{kind}-{payload_sha}.json"
    with _lock(layout):
        records, state, _orphans = _load(layout)
        for existing in records:
            record = existing["record"]
            if record["kind"] == kind and record["payload_sha256"] == payload_sha:
                return {**existing, "created": False, "recovered_orphan": False}
        sequence = len(records) + 1
        previous = records[-1]["record_sha256"] if records else None
        if expected_previous_record_sha256 is not None:
            expected_previous = _sha(
                expected_previous_record_sha256,
                "expected_previous_record_sha256",
            )
            if previous != expected_previous:
                raise ShadowStoreStateError(
                    "shadow store head changed before the conditional append"
                )
        record = {
            "schema_version": STORE_SCHEMA_VERSION,
            "store_id": STORE_ID,
            "sequence": sequence,
            "kind": kind,
            "previous_record_sha256": previous,
            "payload_sha256": payload_sha,
            "payload_filename": filename,
            "recorded_at_utc": _utc(recorded_at_utc, "recorded_at_utc"),
        }
        raw_record = canonical_json_bytes(record)
        record_sha = hashlib.sha256(raw_record).hexdigest()
        candidate = deepcopy(state)
        if kind == "outcome":
            _validate_market_bundle(layout, normalized)
        _apply(candidate, kind, normalized, record_sha, str(record["recorded_at_utc"]))
        artifact_created = _create_only(layout.artifacts / filename, raw_payload)
        path = layout.records / f"{sequence:016d}-{kind}-{record_sha}.json"
        _create_only(path, raw_record)
        return {
            "record_sha256": record_sha,
            "record": record,
            "payload": normalized,
            "path": str(path),
            "created": True,
            "recovered_orphan": not artifact_created,
        }
def activate_shadow_store(
    root: str | Path | ShadowLayout,
    *,
    registry: Registry,
    release_tag_object_oid: str,
    release_commit_oid: str,
    protocol_sha256: str,
    formal_head_record_sha256: str,
    released_at_utc: str | datetime,
    start_after: str | date,
    recorded_at_utc: str | datetime,
) -> dict[str, Any]:
    if not isinstance(registry, Registry):
        raise ShadowStoreStateError("registry must be Registry")
    released = _utc(released_at_utc, "released_at_utc")
    start = start_after.isoformat() if isinstance(start_after, date) else _day(start_after, "start_after")
    payload = {
        "schema_version": 1,
        "activation_type": "adaptive_shadow_activation",
        "registry": registry.to_payload(),
        "registry_sha256": registry.sha256,
        "release_tag": registry.release_tag,
        "release_tag_object_oid": _oid(release_tag_object_oid, "release_tag_object_oid"),
        "release_commit_oid": _oid(release_commit_oid, "release_commit_oid"),
        "protocol_sha256": _sha(protocol_sha256, "protocol_sha256"),
        "formal_head_record_sha256": _sha(formal_head_record_sha256, "formal head SHA"),
        "released_at_utc": released,
        "start_after": start,
    }
    return _append(root, "activation", payload, recorded_at_utc)
def append_shadow_plan(root: str | Path | ShadowLayout, payload: Mapping[str, Any], *, recorded_at_utc: str | datetime) -> dict[str, Any]:
    return _append(root, "plan", payload, recorded_at_utc)
def append_shadow_planning(root: str | Path | ShadowLayout, payload: Mapping[str, Any], *, recorded_at_utc: str | datetime) -> dict[str, Any]:
    return _append(root, "planning", payload, recorded_at_utc)
def append_shadow_missed(root: str | Path | ShadowLayout, payload: Mapping[str, Any], *, recorded_at_utc: str | datetime) -> dict[str, Any]:
    return _append(root, "missed", payload, recorded_at_utc)
def append_shadow_outcome(root: str | Path | ShadowLayout, payload: Mapping[str, Any], *, recorded_at_utc: str | datetime) -> dict[str, Any]:
    return _append(root, "outcome", payload, recorded_at_utc)
def append_shadow_evaluation(
    root: str | Path | ShadowLayout,
    payload: Mapping[str, Any],
    *,
    recorded_at_utc: str | datetime,
    expected_previous_record_sha256: str | None = None,
) -> dict[str, Any]:
    return _append(
        root,
        "evaluation",
        payload,
        recorded_at_utc,
        expected_previous_record_sha256=expected_previous_record_sha256,
    )
def audit_shadow_store(root: str | Path | ShadowLayout) -> dict[str, Any]:
    layout = _layout(root)
    if not layout.root.exists():
        records, state, orphans = [], _State(), []
    else:
        with _lock(layout, create=False):
            records, state, orphans = _load(layout)
    return {
        "schema_version": STORE_SCHEMA_VERSION,
        "store_id": STORE_ID,
        "integrity_valid": True,
        "activated": state.activation is not None,
        "head_sequence": len(records),
        "head_record_sha256": records[-1]["record_sha256"] if records else None,
        "activation_record_sha256": state.activation_sha,
        "registry_sha256": state.activation["registry_sha256"] if state.activation else None,
        "planning_intent_count": len(state.planning_intents),
        "plan_count": len(state.plans),
        "missed_count": len(state.missed),
        "terminated_account_count": len(state.terminated_accounts),
        "outcome_count": len(state.outcomes),
        "evaluation_count": state.evaluation_count,
        "orphan_artifact_count": len(orphans),
        "orphan_artifacts": orphans,
    }
def shadow_store_status(root: str | Path | ShadowLayout) -> dict[str, Any]:
    result = audit_shadow_store(root)
    return {**result, "status": "active" if result["activated"] else "uninitialized"}
__all__ = [
    "STORE_ID", "ShadowLayout", "ShadowStoreError", "ShadowStoreIntegrityError",
    "ShadowStoreStateError", "activate_shadow_store", "append_shadow_planning", "append_shadow_evaluation",
    "append_shadow_missed", "append_shadow_outcome", "append_shadow_plan",
    "audit_shadow_store", "shadow_store_status",
]
