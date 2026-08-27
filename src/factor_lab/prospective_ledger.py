"""Create-only, hash-chained evidence for the Factor Lab 5.0 protocol.

The research runner's JSON helpers intentionally optimize for convenient
diagnostic artifacts.  This module is deliberately separate: ledger bytes are
strictly canonical, records never overwrite an existing path, and every state
transition is revalidated while holding a cross-process lock.
"""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, time as wall_time, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Iterator, Mapping, Sequence
import unicodedata
from uuid import uuid4
from zoneinfo import ZoneInfo


SCHEMA_VERSION = 1
PLAN_SCHEMA_VERSION = 1
SNAPSHOT_SCHEMA_VERSION = 1
DEFAULT_LEDGER_ID = "factor-lab/prospective/5.0"
DEFAULT_RELEASE = "5.0"
WEIGHT_SCALE_PPM = 1_000_000
RECORD_NAME_RE = re.compile(
    r"^(?P<sequence>[0-9]{16})-(?P<kind>[a-z][a-z0-9_]*)-"
    r"(?P<sha256>[0-9a-f]{64})\.json$"
)
SNAPSHOT_NAME_RE = re.compile(
    r"^(?P<sequence>[0-9]{16})-(?P<sha256>[0-9a-f]{64})\.json$"
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
OID_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
RUN_ID_RE = re.compile(r"^[0-9a-f]{16}$")
DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
UTC_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")
TRUSTED_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{6})?Z$"
)
TICKER_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$")
RECORD_KEYS = frozenset(
    {
        "schema_version",
        "ledger_id",
        "sequence",
        "kind",
        "previous_record_sha256",
        "recorded_at_utc",
        "clock_source",
        "payload",
    }
)
AUTHORITATIVE_RUN_KEYS = frozenset(
    {
        "authoritative_run_id",
        "run_fingerprint",
        "manifest_sha256",
        "manifest_self_sha256",
        "adaptive_summary_sha256",
        "frozen_route",
        "integrity_valid",
    }
)


class LedgerError(RuntimeError):
    """Base class for prospective-ledger failures."""


class CanonicalJSONError(LedgerError, ValueError):
    """Raised when JSON cannot be represented by the frozen canonical codec."""


class LedgerIntegrityError(LedgerError):
    """Raised when existing ledger evidence is malformed or has been changed."""


class LedgerStateError(LedgerError):
    """Raised when an otherwise valid record violates the state machine."""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(chunk_size):
            digest.update(block)
    return digest.hexdigest()


def _normalize_json(value: Any, *, path: str = "$") -> Any:
    if value is None or isinstance(value, bool):
        return value
    if type(value) is int:
        return value
    if isinstance(value, float):
        raise CanonicalJSONError(f"floating-point value is forbidden at {path}")
    if isinstance(value, str):
        normalized = unicodedata.normalize("NFC", value)
        if any(0xD800 <= ord(character) <= 0xDFFF for character in normalized):
            raise CanonicalJSONError(f"Unicode surrogate is forbidden at {path}")
        return normalized
    if isinstance(value, Mapping):
        normalized_mapping: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            if not isinstance(raw_key, str):
                raise CanonicalJSONError(f"JSON object key is not a string at {path}")
            key = _normalize_json(raw_key, path=f"{path}.<key>")
            if key in normalized_mapping:
                raise CanonicalJSONError(f"duplicate key after NFC normalization at {path}: {key!r}")
            normalized_mapping[key] = _normalize_json(raw_value, path=f"{path}.{key}")
        return normalized_mapping
    if isinstance(value, (list, tuple)):
        return [
            _normalize_json(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise CanonicalJSONError(f"unsupported JSON value {type(value).__name__} at {path}")


def canonical_json_bytes(value: Any) -> bytes:
    normalized = _normalize_json(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _reject_float(token: str) -> Any:
    raise CanonicalJSONError(f"floating-point JSON token is forbidden: {token}")


def _reject_constant(token: str) -> Any:
    raise CanonicalJSONError(f"non-finite JSON token is forbidden: {token}")


def _unique_pairs(pairs: Sequence[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for raw_key, value in pairs:
        key = unicodedata.normalize("NFC", raw_key)
        if key in result:
            raise CanonicalJSONError(f"duplicate JSON key after NFC normalization: {key!r}")
        result[key] = value
    return result


def strict_load_canonical(value: bytes | bytearray | memoryview) -> Any:
    raw = bytes(value)
    if raw.startswith(b"\xef\xbb\xbf"):
        raise CanonicalJSONError("UTF-8 BOM is forbidden")
    try:
        text = raw.decode("utf-8", errors="strict")
        parsed = json.loads(
            text,
            object_pairs_hook=_unique_pairs,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except UnicodeDecodeError as exc:
        raise CanonicalJSONError("ledger JSON is not valid UTF-8") from exc
    except json.JSONDecodeError as exc:
        raise CanonicalJSONError(f"invalid JSON: {exc.msg}") from exc
    normalized = _normalize_json(parsed)
    if canonical_json_bytes(normalized) != raw:
        raise CanonicalJSONError("JSON bytes are not in canonical form")
    return normalized


def _load_json_unique(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_pairs)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LedgerIntegrityError(f"unreadable JSON file: {path}") from exc
    if not isinstance(payload, dict):
        raise LedgerIntegrityError(f"expected a JSON object: {path}")
    return payload


def _utc_text(value: datetime | str | None = None) -> str:
    if value is None:
        resolved = datetime.now(timezone.utc).replace(microsecond=0)
    elif isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("UTC timestamp must be timezone-aware")
        resolved = value.astimezone(timezone.utc).replace(microsecond=0)
    else:
        _parse_utc(value)
        return value
    return resolved.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str) or not UTC_RE.fullmatch(value):
        raise LedgerStateError(f"timestamp must be canonical UTC seconds: {value!r}")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise LedgerStateError(f"invalid UTC timestamp: {value!r}") from exc


def _parse_trusted_utc(value: Any) -> datetime:
    if not isinstance(value, str) or not TRUSTED_UTC_RE.fullmatch(value):
        raise LedgerStateError(
            f"trusted timestamp must be normalized UTC: {value!r}"
        )
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").astimezone(timezone.utc)
    except ValueError as exc:
        raise LedgerStateError(f"invalid trusted UTC timestamp: {value!r}") from exc


def _parse_date(value: Any) -> date:
    if not isinstance(value, str) or not DATE_RE.fullmatch(value):
        raise LedgerStateError(f"date must use YYYY-MM-DD: {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise LedgerStateError(f"invalid date: {value!r}") from exc


def _require_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not SHA256_RE.fullmatch(value):
        raise LedgerStateError(f"{field_name} must be a lowercase SHA-256")
    return value


def _require_oid(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not OID_RE.fullmatch(value):
        raise LedgerStateError(f"{field_name} must be a lowercase Git object id")
    return value


def _require_int(
    value: Any,
    field_name: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    if type(value) is not int:
        raise LedgerStateError(f"{field_name} must be an integer")
    if minimum is not None and value < minimum:
        raise LedgerStateError(f"{field_name} is below {minimum}")
    if maximum is not None and value > maximum:
        raise LedgerStateError(f"{field_name} is above {maximum}")
    return value


def _require_exact_keys(
    value: Any,
    keys: set[str] | frozenset[str],
    context: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LedgerStateError(f"{context} must be an object")
    actual = set(value)
    if actual != set(keys):
        missing = sorted(set(keys) - actual)
        extra = sorted(actual - set(keys))
        raise LedgerStateError(f"{context} keys differ (missing={missing}, extra={extra})")
    return value


@dataclass(frozen=True, slots=True)
class LedgerLayout:
    root: Path
    ledger_id: str = DEFAULT_LEDGER_ID

    @classmethod
    def at(
        cls,
        root: str | Path,
        *,
        ledger_id: str = DEFAULT_LEDGER_ID,
    ) -> "LedgerLayout":
        return cls(Path(root).expanduser().resolve(), ledger_id=ledger_id)

    @property
    def records(self) -> Path:
        return self.root / "records"

    @property
    def snapshots(self) -> Path:
        return self.root / "snapshots"

    @property
    def plans(self) -> Path:
        return self.root / "plans"

    @property
    def bundles(self) -> Path:
        return self.root / "bundles"

    @property
    def dispatch(self) -> Path:
        return self.root / "dispatch"

    @property
    def lock_path(self) -> Path:
        return self.root / ".append.lock"

    def ensure_directories(self) -> None:
        for path in (
            self.root,
            self.records,
            self.snapshots,
            self.plans,
            self.bundles,
            self.dispatch,
        ):
            path.mkdir(parents=True, exist_ok=True)


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_only_file(path: str | Path, content: bytes) -> bool:
    """Publish *content* without ever replacing an existing destination.

    Returns ``True`` when a new path was published and ``False`` for an
    idempotent same-byte replay.  A hard-link publish is required so a crash
    cannot expose a partially written final record.
    """

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if target.is_file() and target.read_bytes() == content:
            return False
        raise LedgerIntegrityError(f"create-only target already exists with other content: {target}")
    temporary = target.parent / f".pending-{os.getpid()}-{uuid4().hex}.tmp"
    # Windows CRT file descriptors default to text mode, where ``os.write``
    # expands every LF byte.  Ledger bytes are hashed before publication, so
    # opening the descriptor explicitly as binary is part of the integrity
    # boundary (``O_BINARY`` is zero/absent on POSIX).
    binary_flag = getattr(os, "O_BINARY", 0)
    descriptor = os.open(
        temporary,
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | binary_flag,
        0o600,
    )
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write while creating ledger evidence")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        try:
            os.link(temporary, target)
        except FileExistsError:
            if target.is_file() and target.read_bytes() == content:
                return False
            raise LedgerIntegrityError(
                f"concurrent create-only target collision: {target}"
            )
        except OSError as exc:
            raise LedgerIntegrityError(
                "filesystem cannot atomically publish ledger evidence with a hard link"
            ) from exc
        _fsync_directory(target.parent)
        return True
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


@contextmanager
def _exclusive_lock(layout: LedgerLayout, *, timeout_seconds: float = 15.0) -> Iterator[None]:
    layout.ensure_directories()
    descriptor = os.open(layout.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
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
                        raise LedgerIntegrityError("timed out acquiring prospective ledger lock")
                    time.sleep(0.05)
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
                        raise LedgerIntegrityError("timed out acquiring prospective ledger lock")
                    time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


@dataclass
class _LedgerState:
    phase: str = "unactivated"
    activation_hash: str | None = None
    activation: dict[str, Any] | None = None
    current_decision_hash: str | None = None
    current_receipt_hash: str | None = None
    decision_count: int = 0
    confirmed_observation_count: int = 0
    decision_sessions: set[str] = field(default_factory=set)
    outcome_versions: dict[str, dict[str, Any]] = field(default_factory=dict)
    superseded_versions: set[str] = field(default_factory=set)

    def public_phase(self) -> str:
        # The internal transition name describes which operation is legal
        # next.  Before the first genuinely prospective decision, however,
        # the frozen protocol requires the externally visible state to remain
        # ``awaiting_new_data``.  Do not reuse that label after an outcome:
        # once decision_count is non-zero, the ordinary cycle resumes as
        # ``awaiting_decision``.
        if self.phase == "awaiting_decision" and self.decision_count == 0:
            if self.activation is not None:
                return str(self.activation["initial_status"])
            return "awaiting_new_data"
        return self.phase

    def public(self) -> dict[str, Any]:
        visible_phase = self.public_phase()
        activation = self.activation or {}
        return {
            "phase": visible_phase,
            "activation_record_sha256": self.activation_hash,
            "authoritative_run_id": activation.get("authoritative_run_id"),
            "run_fingerprint": activation.get("run_fingerprint"),
            "manifest_sha256": activation.get("manifest_sha256"),
            "manifest_self_sha256": activation.get("manifest_self_sha256"),
            "adaptive_summary_sha256": activation.get(
                "adaptive_summary_sha256"
            ),
            "frozen_route": activation.get("frozen_route"),
            "integrity_valid": activation.get("integrity_valid"),
            "current_decision_record_sha256": self.current_decision_hash,
            "current_attestation_receipt_record_sha256": self.current_receipt_hash,
            "decision_count": self.decision_count,
            "confirmed_observation_count": self.confirmed_observation_count,
            "awaiting_new_data": visible_phase == "awaiting_new_data",
        }


def _validate_activation(payload: Any) -> Mapping[str, Any]:
    keys = {
        "protocol_id",
        "protocol_release",
        "protocol_path",
        "protocol_sha256",
        "release_tag",
        "release_tag_object_oid",
        "release_commit_oid",
        "historical_data_cutoff",
        "first_decision_signal_date_rule",
        "pretrade_deadline",
        "historical_backfill_forbidden",
        "initial_status",
        "source_evidence_run_id",
        "source_evidence_manifest_sha256",
        *AUTHORITATIVE_RUN_KEYS,
    }
    row = _require_exact_keys(payload, keys, "protocol activation payload")
    for field_name in (
        "protocol_id",
        "protocol_release",
        "protocol_path",
        "release_tag",
        "first_decision_signal_date_rule",
        "pretrade_deadline",
        "initial_status",
        "source_evidence_run_id",
    ):
        if not isinstance(row[field_name], str) or not row[field_name]:
            raise LedgerStateError(f"activation {field_name} must be a non-empty string")
    _require_sha256(row["protocol_sha256"], "protocol_sha256")
    _require_sha256(row["source_evidence_manifest_sha256"], "source_evidence_manifest_sha256")
    run_id = row["authoritative_run_id"]
    if not isinstance(run_id, str) or not RUN_ID_RE.fullmatch(run_id):
        raise LedgerStateError("authoritative_run_id must be 16 lowercase hex characters")
    fingerprint = _require_sha256(row["run_fingerprint"], "run_fingerprint")
    if fingerprint[:16] != run_id:
        raise LedgerStateError("authoritative run id differs from its fingerprint")
    for field_name in (
        "manifest_sha256",
        "manifest_self_sha256",
        "adaptive_summary_sha256",
    ):
        _require_sha256(row[field_name], field_name)
    if not isinstance(row["frozen_route"], str) or not row["frozen_route"]:
        raise LedgerStateError("activation frozen_route must be a non-empty string")
    if row["integrity_valid"] is not True:
        raise LedgerStateError("activation authoritative integrity must be true")
    _require_oid(row["release_tag_object_oid"], "release_tag_object_oid")
    _require_oid(row["release_commit_oid"], "release_commit_oid")
    _parse_date(row["historical_data_cutoff"])
    if row["protocol_release"] != row["release_tag"]:
        raise LedgerStateError("protocol release and activation tag differ")
    if row["pretrade_deadline"] != "09:15 Asia/Shanghai on trade date":
        raise LedgerStateError("activation must freeze the 09:15 Asia/Shanghai deadline")
    if row["historical_backfill_forbidden"] is not True:
        raise LedgerStateError("activation must forbid historical backfill")
    return row


PLAN_KEYS = frozenset(
    {
        "schema_version",
        "plan_type",
        "ledger_id",
        "activation_record_sha256",
        "base_head_record_sha256",
        "decision_id",
        "decision_session",
        "information_cutoff_utc",
        "input_max_available_at_utc",
        "input_snapshot_sha256",
        "model_state_sha256",
        "code_commit_oid",
        "expected_nav_fen",
        "cash_weight_ppm",
        "targets",
        "frozen_route",
        "admission_deadline_utc",
        "planned_at_utc",
        "clock_source",
    }
)


def _validate_plan(plan: Any, state: _LedgerState, *, current_head: str) -> Mapping[str, Any]:
    row = _require_exact_keys(plan, PLAN_KEYS, "decision plan")
    if row["schema_version"] != PLAN_SCHEMA_VERSION or row["plan_type"] != "prospective_decision":
        raise LedgerStateError("unsupported prospective decision plan")
    if row["ledger_id"] != DEFAULT_LEDGER_ID:
        raise LedgerStateError("decision plan ledger_id differs from the frozen ledger")
    if state.phase != "awaiting_decision" or state.activation is None:
        raise LedgerStateError(f"cannot seal a decision while phase={state.phase}")
    if row["activation_record_sha256"] != state.activation_hash:
        raise LedgerStateError("decision plan binds another activation")
    if row["base_head_record_sha256"] != current_head:
        raise LedgerStateError("decision plan is stale relative to the ledger head")
    _require_sha256(row["input_snapshot_sha256"], "input_snapshot_sha256")
    _require_sha256(row["model_state_sha256"], "model_state_sha256")
    _require_oid(row["code_commit_oid"], "code_commit_oid")
    if row["code_commit_oid"] != state.activation["release_commit_oid"]:
        raise LedgerStateError("decision plan code commit differs from activation release")
    if row["frozen_route"] != state.activation["frozen_route"]:
        raise LedgerStateError("decision plan frozen route differs from activation")
    session = _parse_date(row["decision_session"])
    historical_cutoff = _parse_date(state.activation["historical_data_cutoff"])
    if session <= historical_cutoff:
        raise LedgerStateError("decision session must be strictly after the activation cutoff")
    if row["decision_session"] in state.decision_sessions:
        raise LedgerStateError("decision session was already sealed")
    expected_id = f"{state.activation['protocol_release']}/{row['decision_session']}"
    if row["decision_id"] != expected_id:
        raise LedgerStateError(f"decision_id must equal {expected_id!r}")
    input_max = _parse_utc(row["input_max_available_at_utc"])
    information_cutoff = _parse_utc(row["information_cutoff_utc"])
    deadline = _parse_utc(row["admission_deadline_utc"])
    planned = _parse_utc(row["planned_at_utc"])
    expected_deadline = datetime.combine(
        session,
        wall_time(hour=9, minute=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc)
    if deadline != expected_deadline:
        raise LedgerStateError("decision admission deadline is not 09:15 Asia/Shanghai")
    if not input_max <= information_cutoff < deadline:
        raise LedgerStateError("decision inputs/cutoff are not strictly causal before admission")
    if planned >= deadline:
        raise LedgerStateError("decision plan was produced at or after the admission deadline")
    _require_int(row["expected_nav_fen"], "expected_nav_fen", minimum=0)
    cash = _require_int(
        row["cash_weight_ppm"], "cash_weight_ppm", minimum=0, maximum=WEIGHT_SCALE_PPM
    )
    if not isinstance(row["targets"], list) or not row["targets"]:
        raise LedgerStateError("decision targets must be a non-empty list")
    tickers: list[str] = []
    target_total = 0
    for index, target in enumerate(row["targets"]):
        resolved = _require_exact_keys(
            target, {"ticker", "target_weight_ppm"}, f"decision target[{index}]"
        )
        ticker = resolved["ticker"]
        if not isinstance(ticker, str) or not TICKER_RE.fullmatch(ticker):
            raise LedgerStateError(f"invalid canonical ticker at target[{index}]")
        tickers.append(ticker)
        target_total += _require_int(
            resolved["target_weight_ppm"],
            f"target[{index}].target_weight_ppm",
            minimum=0,
            maximum=WEIGHT_SCALE_PPM,
        )
    if tickers != sorted(tickers) or len(tickers) != len(set(tickers)):
        raise LedgerStateError("decision targets must be uniquely sorted by ticker")
    if target_total + cash != WEIGHT_SCALE_PPM:
        raise LedgerStateError("decision target and cash weights must total 1,000,000 ppm")
    if row["clock_source"] != "local_system_clock_untrusted":
        raise LedgerStateError("decision plan clock_source is not diagnostic-only")
    return row


RECEIPT_KEYS = frozenset(
    {
        "purpose",
        "snapshot_sha256",
        "snapshot_head_record_sha256",
        "decision_record_sha256",
        "request_id",
        "workflow_run_id",
        "workflow_run_attempt",
        "workflow_run_display_title",
        "workflow_run_url",
        "workflow_run_created_at_utc",
        "workflow_run_completed_at_utc",
        "workflow_path",
        "workflow_ref",
        "workflow_source_commit_oid",
        "attestation_bundle_sha256",
        "certificate_identity",
        "run_invocation_uri",
        "verified_timestamp_count",
        "verified_timestamps",
        "verified_tlog_type",
        "verified_tlog_uri",
        "verified_tlog_timestamp_utc",
        "subject_name",
        "subject_sha256",
    }
)
VERIFIED_TIMESTAMP_KEYS = frozenset({"type", "uri", "timestamp_utc"})


def _validate_receipt(payload: Any) -> Mapping[str, Any]:
    row = _require_exact_keys(payload, RECEIPT_KEYS, "attestation receipt")
    if row["purpose"] not in {"activation_canary", "decision_anchor"}:
        raise LedgerStateError("unsupported attestation receipt purpose")
    for field_name in (
        "snapshot_sha256",
        "snapshot_head_record_sha256",
        "request_id",
        "attestation_bundle_sha256",
        "subject_sha256",
    ):
        _require_sha256(row[field_name], field_name)
    if row["decision_record_sha256"] is not None:
        _require_sha256(row["decision_record_sha256"], "decision_record_sha256")
    _require_oid(row["workflow_source_commit_oid"], "workflow_source_commit_oid")
    workflow_run_id = _require_int(row["workflow_run_id"], "workflow_run_id", minimum=1)
    workflow_run_attempt = _require_int(
        row["workflow_run_attempt"], "workflow_run_attempt", minimum=1
    )
    verified_timestamp_count = _require_int(
        row["verified_timestamp_count"], "verified_timestamp_count", minimum=1
    )
    created = _parse_utc(row["workflow_run_created_at_utc"])
    completed = _parse_utc(row["workflow_run_completed_at_utc"])
    if completed < created:
        raise LedgerStateError("workflow completion precedes creation")
    for field_name in (
        "workflow_run_url",
        "workflow_run_display_title",
        "workflow_path",
        "workflow_ref",
        "certificate_identity",
        "run_invocation_uri",
        "verified_tlog_type",
        "verified_tlog_uri",
        "subject_name",
    ):
        if not isinstance(row[field_name], str) or not row[field_name]:
            raise LedgerStateError(f"receipt {field_name} must be a non-empty string")
    expected_run_url = (
        "https://github.com/yxforever666gh/factor-lab/actions/runs/"
        f"{workflow_run_id}"
    )
    if row["workflow_run_url"] != expected_run_url:
        raise LedgerStateError("workflow_run_url differs from the exact GitHub run")
    if row["workflow_run_display_title"] != f"prospective-{row['request_id']}":
        raise LedgerStateError("workflow run title differs from the request id")
    expected_invocation_uri = (
        f"{expected_run_url}/attempts/{workflow_run_attempt}"
    )
    if row["run_invocation_uri"] != expected_invocation_uri:
        raise LedgerStateError("certificate invocation differs from the exact run attempt")
    timestamps = row["verified_timestamps"]
    if not isinstance(timestamps, list) or len(timestamps) != verified_timestamp_count:
        raise LedgerStateError("verified timestamp count differs from timestamp evidence")
    normalized_timestamps: list[tuple[str, str, str, datetime]] = []
    for index, timestamp in enumerate(timestamps):
        verified = _require_exact_keys(
            timestamp,
            VERIFIED_TIMESTAMP_KEYS,
            f"verified timestamp {index}",
        )
        timestamp_type = verified["type"]
        timestamp_uri = verified["uri"]
        if not isinstance(timestamp_type, str) or not timestamp_type:
            raise LedgerStateError(f"verified timestamp {index} type must be non-empty")
        if not isinstance(timestamp_uri, str):
            raise LedgerStateError(f"verified timestamp {index} URI must be a string")
        timestamp_utc = verified["timestamp_utc"]
        normalized_timestamps.append(
            (
                timestamp_type,
                timestamp_uri,
                timestamp_utc,
                _parse_trusted_utc(timestamp_utc),
            )
        )
    if row["verified_tlog_type"] != "Tlog" or not row["verified_tlog_uri"]:
        raise LedgerStateError("receipt lacks a trusted transparency-log timestamp")
    trusted_tlog_timestamp = _parse_trusted_utc(
        row["verified_tlog_timestamp_utc"]
    )
    trusted_entry = (
        row["verified_tlog_type"],
        row["verified_tlog_uri"],
        row["verified_tlog_timestamp_utc"],
        trusted_tlog_timestamp,
    )
    tlog_entries = [
        timestamp
        for timestamp in normalized_timestamps
        if timestamp[0] == "Tlog" and timestamp[1]
    ]
    if trusted_entry not in tlog_entries or trusted_entry != min(
        tlog_entries, key=lambda timestamp: (timestamp[3], timestamp[1])
    ):
        raise LedgerStateError(
            "trusted transparency-log timestamp is not the earliest verified Tlog"
        )
    if row["subject_sha256"] != row["snapshot_sha256"]:
        raise LedgerStateError("attestation subject does not match the snapshot")
    return row


OUTCOME_KEYS = frozenset(
    {
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "holding_start_date",
        "holding_end_date",
        "observation_available_at_utc",
        "source_snapshot_sha256",
        "execution_status",
        "gross_return_ppb",
        "net_return_ppb",
        "benchmark_return_ppb",
        "turnover_ppm",
        "fees_fen",
        "ending_nav_fen",
    }
)


def _validate_outcome(payload: Any) -> Mapping[str, Any]:
    row = _require_exact_keys(payload, OUTCOME_KEYS, "prospective outcome")
    for field_name in (
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "source_snapshot_sha256",
    ):
        _require_sha256(row[field_name], field_name)
    start = _parse_date(row["holding_start_date"])
    end = _parse_date(row["holding_end_date"])
    if end < start:
        raise LedgerStateError("outcome holding_end_date precedes holding_start_date")
    _parse_utc(row["observation_available_at_utc"])
    if row["execution_status"] not in {"complete", "not_executed"}:
        raise LedgerStateError("unsupported outcome execution_status")
    for field_name in (
        "gross_return_ppb",
        "net_return_ppb",
        "benchmark_return_ppb",
    ):
        _require_int(row[field_name], field_name, minimum=-WEIGHT_SCALE_PPM * 1000)
    _require_int(row["turnover_ppm"], "turnover_ppm", minimum=0)
    _require_int(row["fees_fen"], "fees_fen", minimum=0)
    _require_int(row["ending_nav_fen"], "ending_nav_fen", minimum=0)
    return row


def _apply_record(state: _LedgerState, record: Mapping[str, Any], record_hash: str) -> None:
    kind = record["kind"]
    payload = record["payload"]
    previous_hash = record["previous_record_sha256"]
    if kind == "protocol_activation":
        activation = dict(_validate_activation(payload))
        if state.phase != "unactivated" or previous_hash is not None:
            raise LedgerStateError("protocol activation must be the first and only activation")
        state.phase = "awaiting_decision"
        state.activation_hash = record_hash
        state.activation = activation
        return
    if state.activation is None or state.activation_hash is None:
        raise LedgerStateError("ledger record appears before protocol activation")
    if kind == "decision":
        row = _require_exact_keys(payload, {"plan_sha256", "plan"}, "decision payload")
        plan = row["plan"]
        _require_sha256(row["plan_sha256"], "plan_sha256")
        if row["plan_sha256"] != sha256_bytes(canonical_json_bytes(plan)):
            raise LedgerStateError("decision plan hash differs from embedded plan")
        _validate_plan(plan, state, current_head=str(previous_hash or ""))
        state.phase = "awaiting_receipt"
        state.current_decision_hash = record_hash
        state.current_receipt_hash = None
        state.decision_count += 1
        state.decision_sessions.add(str(plan["decision_session"]))
        return
    if kind == "attestation_receipt":
        row = _validate_receipt(payload)
        expected_path = ".github/workflows/prospective-attest.yml"
        expected_ref = f"refs/tags/{state.activation['release_tag']}"
        expected_identity = (
            "https://github.com/yxforever666gh/factor-lab/"
            f"{expected_path}@{expected_ref}"
        )
        if row["workflow_path"] != expected_path or row["workflow_ref"] != expected_ref:
            raise LedgerStateError("receipt workflow identity differs from the frozen release")
        if row["workflow_source_commit_oid"] != state.activation["release_commit_oid"]:
            raise LedgerStateError("receipt workflow commit differs from the frozen release")
        if row["certificate_identity"] != expected_identity:
            raise LedgerStateError("receipt certificate identity differs from the frozen workflow")
        if row["purpose"] == "activation_canary":
            if (
                state.phase != "awaiting_decision"
                or state.decision_count != 0
                or previous_hash != state.activation_hash
                or row["snapshot_head_record_sha256"] != state.activation_hash
                or row["decision_record_sha256"] is not None
            ):
                raise LedgerStateError("activation canary does not bind the untouched activation")
            return
        if (
            state.phase != "awaiting_receipt"
            or previous_hash != state.current_decision_hash
            or row["snapshot_head_record_sha256"] != state.current_decision_hash
            or row["decision_record_sha256"] != state.current_decision_hash
        ):
            raise LedgerStateError("decision attestation receipt does not bind the current decision")
        plan = record  # quiet type checkers; the current decision is recovered by chain audit below.
        del plan
        state.phase = "awaiting_outcome"
        state.current_receipt_hash = record_hash
        return
    if kind == "outcome":
        row = dict(_validate_outcome(payload))
        if (
            state.phase != "awaiting_outcome"
            or row["decision_record_sha256"] != state.current_decision_hash
            or row["attestation_receipt_record_sha256"] != state.current_receipt_hash
            or previous_hash != state.current_receipt_hash
        ):
            raise LedgerStateError("outcome does not close the currently attested decision")
        recorded = _parse_utc(record["recorded_at_utc"])
        if recorded < _parse_utc(row["observation_available_at_utc"]):
            raise LedgerStateError("outcome was recorded before its source became available")
        state.outcome_versions[record_hash] = row
        state.confirmed_observation_count += 1
        state.phase = "awaiting_decision"
        state.current_decision_hash = None
        state.current_receipt_hash = None
        return
    if kind == "correction":
        row = _require_exact_keys(
            payload,
            {"supersedes_record_sha256", "reason", "replacement_outcome", "source_snapshot_sha256"},
            "outcome correction",
        )
        if state.phase != "awaiting_decision":
            raise LedgerStateError("correction cannot interrupt an open prospective cycle")
        supersedes = _require_sha256(row["supersedes_record_sha256"], "supersedes_record_sha256")
        _require_sha256(row["source_snapshot_sha256"], "source_snapshot_sha256")
        if supersedes not in state.outcome_versions:
            raise LedgerStateError("correction does not reference an outcome or prior correction")
        if supersedes in state.superseded_versions:
            raise LedgerStateError("correction creates a fork from an already superseded version")
        if not isinstance(row["reason"], str) or not row["reason"].strip():
            raise LedgerStateError("correction reason must be non-empty")
        replacement = dict(_validate_outcome(row["replacement_outcome"]))
        previous = state.outcome_versions[supersedes]
        for key in ("decision_record_sha256", "attestation_receipt_record_sha256"):
            if replacement[key] != previous[key]:
                raise LedgerStateError(f"correction cannot change {key}")
        state.superseded_versions.add(supersedes)
        state.outcome_versions[record_hash] = replacement
        return
    raise LedgerStateError(f"unsupported ledger record kind: {kind!r}")


def _record_metadata(path: Path, record: Mapping[str, Any], digest: str) -> dict[str, Any]:
    return {
        "sequence": int(record["sequence"]),
        "kind": str(record["kind"]),
        "record_sha256": digest,
        "path": str(path),
        "record": dict(record),
    }


def _load_record_chain(layout: LedgerLayout) -> tuple[list[dict[str, Any]], _LedgerState]:
    if not layout.records.exists():
        return [], _LedgerState()
    candidates = sorted(path for path in layout.records.iterdir() if path.is_file())
    records: list[dict[str, Any]] = []
    state = _LedgerState()
    previous_hash: str | None = None
    for expected_sequence, path in enumerate(candidates, start=1):
        match = RECORD_NAME_RE.fullmatch(path.name)
        if match is None:
            raise LedgerIntegrityError(f"unexpected file in records directory: {path.name}")
        raw = path.read_bytes()
        digest = sha256_bytes(raw)
        if digest != match.group("sha256"):
            raise LedgerIntegrityError(f"record filename/content hash mismatch: {path.name}")
        parsed = strict_load_canonical(raw)
        row = _require_exact_keys(parsed, RECORD_KEYS, f"record {path.name}")
        if row["schema_version"] != SCHEMA_VERSION or row["ledger_id"] != layout.ledger_id:
            raise LedgerIntegrityError(f"record schema or ledger id differs: {path.name}")
        if row["sequence"] != expected_sequence or int(match.group("sequence")) != expected_sequence:
            raise LedgerIntegrityError(f"record sequence gap or mismatch: {path.name}")
        if row["kind"] != match.group("kind"):
            raise LedgerIntegrityError(f"record kind differs from filename: {path.name}")
        if row["previous_record_sha256"] != previous_hash:
            raise LedgerIntegrityError(f"record hash-chain predecessor mismatch: {path.name}")
        _parse_utc(row["recorded_at_utc"])
        if row["clock_source"] != "local_system_clock_untrusted":
            raise LedgerIntegrityError(f"record clock source is not diagnostic-only: {path.name}")
        _apply_record(state, row, digest)
        records.append(_record_metadata(path, row, digest))
        previous_hash = digest
    return records, state


def _snapshot_payload(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    state: _LedgerState,
) -> dict[str, Any]:
    if not records or state.activation is None or state.activation_hash is None:
        raise LedgerStateError("cannot snapshot an unactivated ledger")
    head = records[-1]
    return {
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "ledger_id": layout.ledger_id,
        "head_sequence": int(head["sequence"]),
        "head_record_sha256": str(head["record_sha256"]),
        "activation_record_sha256": state.activation_hash,
        "protocol_id": state.activation["protocol_id"],
        "protocol_sha256": state.activation["protocol_sha256"],
        "release_tag": state.activation["release_tag"],
        "release_commit_oid": state.activation["release_commit_oid"],
        "authoritative_run_id": state.activation["authoritative_run_id"],
        "run_fingerprint": state.activation["run_fingerprint"],
        "manifest_sha256": state.activation["manifest_sha256"],
        "manifest_self_sha256": state.activation["manifest_self_sha256"],
        "adaptive_summary_sha256": state.activation[
            "adaptive_summary_sha256"
        ],
        "frozen_route": state.activation["frozen_route"],
        "integrity_valid": state.activation["integrity_valid"],
        "phase": state.public_phase(),
        "decision_count": state.decision_count,
        "confirmed_observation_count": state.confirmed_observation_count,
    }


def _seal_snapshot_unlocked(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    state: _LedgerState,
) -> dict[str, Any]:
    payload = _snapshot_payload(layout, records, state)
    raw = canonical_json_bytes(payload)
    digest = sha256_bytes(raw)
    path = layout.snapshots / f"{payload['head_sequence']:016d}-{digest}.json"
    created = create_only_file(path, raw)
    return {
        "snapshot_sha256": digest,
        "path": str(path),
        "created": created,
        "snapshot": payload,
    }


def seal_snapshot(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        records, state = _load_record_chain(layout)
        return _seal_snapshot_unlocked(layout, records, state)


def _append_record_unlocked(
    layout: LedgerLayout,
    records: list[dict[str, Any]],
    state: _LedgerState,
    *,
    kind: str,
    payload: Mapping[str, Any],
    recorded_at_utc: datetime | str | None,
) -> dict[str, Any]:
    sequence = len(records) + 1
    previous_hash = records[-1]["record_sha256"] if records else None
    record = {
        "schema_version": SCHEMA_VERSION,
        "ledger_id": layout.ledger_id,
        "sequence": sequence,
        "kind": kind,
        "previous_record_sha256": previous_hash,
        "recorded_at_utc": _utc_text(recorded_at_utc),
        "clock_source": "local_system_clock_untrusted",
        "payload": dict(payload),
    }
    raw = canonical_json_bytes(record)
    digest = sha256_bytes(raw)
    candidate_state = deepcopy(state)
    _apply_record(candidate_state, record, digest)
    path = layout.records / f"{sequence:016d}-{kind}-{digest}.json"
    create_only_file(path, raw)
    metadata = _record_metadata(path, record, digest)
    records.append(metadata)
    snapshot = _seal_snapshot_unlocked(layout, records, candidate_state)
    return {**metadata, "snapshot": snapshot}


def activate_protocol(
    ledger_root: str | Path,
    *,
    protocol_path: str | Path,
    release_tag: str,
    release_tag_object_oid: str,
    release_commit_oid: str,
    authoritative_run: Mapping[str, Any],
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    source = Path(protocol_path)
    protocol = _load_json_unique(source)
    prospective = protocol.get("prospective")
    evidence = protocol.get("source_evidence")
    routing = protocol.get("routing")
    if (
        not isinstance(prospective, Mapping)
        or not isinstance(evidence, Mapping)
        or not isinstance(routing, Mapping)
    ):
        raise LedgerStateError("protocol lacks prospective or source_evidence contracts")
    if protocol.get("release") != release_tag or prospective.get("activation_release") != release_tag:
        raise LedgerStateError("protocol release does not match requested activation tag")
    binding = dict(
        _require_exact_keys(
            authoritative_run,
            AUTHORITATIVE_RUN_KEYS,
            "authoritative run binding",
        )
    )
    allowed_routes = {
        routing.get("if_core_overlay_gate_fails"),
        routing.get("if_all_four_gates_pass"),
        routing.get("otherwise"),
    }
    if binding.get("frozen_route") not in allowed_routes:
        raise LedgerStateError("authoritative frozen route is outside protocol routing")
    payload = {
        "protocol_id": str(protocol.get("protocol_id") or ""),
        "protocol_release": str(protocol.get("release") or ""),
        "protocol_path": source.as_posix(),
        "protocol_sha256": sha256_file(source),
        "release_tag": release_tag,
        "release_tag_object_oid": release_tag_object_oid,
        "release_commit_oid": release_commit_oid,
        "historical_data_cutoff": str(prospective.get("activation_historical_cutoff") or ""),
        "first_decision_signal_date_rule": str(
            prospective.get("first_decision_signal_date_rule") or ""
        ),
        "pretrade_deadline": str(prospective.get("pretrade_deadline") or ""),
        "historical_backfill_forbidden": bool(
            prospective.get("historical_backfill_forbidden")
        ),
        "initial_status": str(prospective.get("initial_status_without_new_market_data") or ""),
        "source_evidence_run_id": str(evidence.get("authoritative_run_id") or ""),
        "source_evidence_manifest_sha256": str(evidence.get("manifest_sha256") or ""),
        **binding,
    }
    _validate_activation(payload)
    with _exclusive_lock(layout):
        records, state = _load_record_chain(layout)
        if records:
            first = records[0]
            if first["kind"] == "protocol_activation" and first["record"]["payload"] == payload:
                snapshot = _seal_snapshot_unlocked(layout, records, state)
                return {**first, "created": False, "snapshot": snapshot}
            raise LedgerStateError("prospective ledger is already activated differently")
        result = _append_record_unlocked(
            layout,
            records,
            state,
            kind="protocol_activation",
            payload=payload,
            recorded_at_utc=recorded_at_utc,
        )
        result["created"] = True
        return result


def build_decision_plan(
    ledger_root: str | Path,
    *,
    decision_session: str,
    information_cutoff_utc: str,
    input_max_available_at_utc: str,
    input_snapshot_sha256: str,
    model_state_sha256: str,
    code_commit_oid: str,
    expected_nav_fen: int,
    targets_ppm: Mapping[str, int],
    cash_weight_ppm: int = 0,
    planned_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    records, state = _load_record_chain(layout)
    if not records or state.activation is None or state.activation_hash is None:
        raise LedgerStateError("prospective ledger is not activated")
    session = _parse_date(decision_session)
    deadline = datetime.combine(
        session,
        wall_time(hour=9, minute=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc)
    targets = [
        {"ticker": str(ticker), "target_weight_ppm": weight}
        for ticker, weight in sorted(targets_ppm.items())
    ]
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan_type": "prospective_decision",
        "ledger_id": layout.ledger_id,
        "activation_record_sha256": state.activation_hash,
        "base_head_record_sha256": records[-1]["record_sha256"],
        "decision_id": f"{state.activation['protocol_release']}/{decision_session}",
        "decision_session": decision_session,
        "information_cutoff_utc": information_cutoff_utc,
        "input_max_available_at_utc": input_max_available_at_utc,
        "input_snapshot_sha256": input_snapshot_sha256,
        "model_state_sha256": model_state_sha256,
        "code_commit_oid": code_commit_oid,
        "expected_nav_fen": expected_nav_fen,
        "cash_weight_ppm": cash_weight_ppm,
        "targets": targets,
        "frozen_route": state.activation["frozen_route"],
        "admission_deadline_utc": deadline.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "planned_at_utc": _utc_text(planned_at_utc),
        "clock_source": "local_system_clock_untrusted",
    }
    _validate_plan(plan, state, current_head=records[-1]["record_sha256"])
    return plan


def store_decision_plan(
    ledger_root: str | Path,
    plan: Mapping[str, Any],
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    records, state = _load_record_chain(layout)
    if not records:
        raise LedgerStateError("prospective ledger is not activated")
    _validate_plan(plan, state, current_head=records[-1]["record_sha256"])
    raw = canonical_json_bytes(plan)
    digest = sha256_bytes(raw)
    safe_id = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(plan.get("decision_id") or "decision"))
    path = layout.plans / f"{safe_id}-{digest}.json"
    created = create_only_file(path, raw)
    return {"plan_sha256": digest, "path": str(path), "created": created}


def _load_plan(value: str | Path | bytes | Mapping[str, Any]) -> tuple[dict[str, Any], bytes]:
    if isinstance(value, Mapping):
        raw = canonical_json_bytes(value)
        return dict(value), raw
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
    else:
        raw = Path(value).read_bytes()
    parsed = strict_load_canonical(raw)
    if not isinstance(parsed, dict):
        raise LedgerStateError("decision plan must be a JSON object")
    return parsed, raw


def seal_decision(
    ledger_root: str | Path,
    plan: str | Path | bytes | Mapping[str, Any],
    *,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    resolved_plan, raw = _load_plan(plan)
    sealed_time = _utc_text(recorded_at_utc)
    with _exclusive_lock(layout):
        records, state = _load_record_chain(layout)
        if not records:
            raise LedgerStateError("prospective ledger is not activated")
        _validate_plan(resolved_plan, state, current_head=records[-1]["record_sha256"])
        if _parse_utc(sealed_time) >= _parse_utc(resolved_plan["admission_deadline_utc"]):
            raise LedgerStateError("decision was sealed at or after the admission deadline")
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="decision",
            payload={"plan_sha256": sha256_bytes(raw), "plan": resolved_plan},
            recorded_at_utc=sealed_time,
        )


def _read_snapshot(layout: LedgerLayout, digest: str) -> dict[str, Any]:
    _require_sha256(digest, "snapshot_sha256")
    matches = sorted(layout.snapshots.glob(f"*-{digest}.json"))
    if len(matches) != 1:
        raise LedgerIntegrityError(f"expected exactly one snapshot for {digest}, found {len(matches)}")
    raw = matches[0].read_bytes()
    if sha256_bytes(raw) != digest:
        raise LedgerIntegrityError("snapshot content hash differs from requested digest")
    payload = strict_load_canonical(raw)
    if not isinstance(payload, dict):
        raise LedgerIntegrityError("snapshot must be a JSON object")
    return payload


def append_attestation_receipt(
    ledger_root: str | Path,
    receipt: Mapping[str, Any],
    *,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    normalized = dict(_normalize_json(receipt))
    _validate_receipt(normalized)
    with _exclusive_lock(layout):
        records, state = _load_record_chain(layout)
        if not records:
            raise LedgerStateError("prospective ledger is not activated")
        snapshot = _read_snapshot(layout, normalized["snapshot_sha256"])
        if (
            snapshot.get("head_record_sha256") != normalized["snapshot_head_record_sha256"]
            or snapshot.get("head_record_sha256") != records[-1]["record_sha256"]
        ):
            raise LedgerStateError("receipt snapshot is not the current ledger head")
        if normalized["purpose"] == "decision_anchor":
            decision = records[-1]["record"]["payload"]["plan"]
            if _parse_trusted_utc(
                normalized["verified_tlog_timestamp_utc"]
            ) >= _parse_utc(decision["admission_deadline_utc"]):
                raise LedgerStateError(
                    "trusted transparency-log timestamp is at or after admission"
                )
            if _parse_utc(normalized["workflow_run_created_at_utc"]) >= _parse_utc(
                decision["admission_deadline_utc"]
            ):
                raise LedgerStateError("attestation workflow was created at or after admission")
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="attestation_receipt",
            payload=normalized,
            recorded_at_utc=recorded_at_utc,
        )


def append_outcome(
    ledger_root: str | Path,
    outcome: Mapping[str, Any],
    *,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    normalized = dict(_normalize_json(outcome))
    _validate_outcome(normalized)
    with _exclusive_lock(layout):
        records, state = _load_record_chain(layout)
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="outcome",
            payload=normalized,
            recorded_at_utc=recorded_at_utc,
        )


def append_correction(
    ledger_root: str | Path,
    correction: Mapping[str, Any],
    *,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    normalized = dict(_normalize_json(correction))
    with _exclusive_lock(layout):
        records, state = _load_record_chain(layout)
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="correction",
            payload=normalized,
            recorded_at_utc=recorded_at_utc,
        )


def _audit_snapshots(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if not layout.snapshots.exists():
        return issues
    by_sequence = {int(row["sequence"]): row for row in records}
    seen_sequences: set[int] = set()
    for path in sorted(item for item in layout.snapshots.iterdir() if item.is_file()):
        match = SNAPSHOT_NAME_RE.fullmatch(path.name)
        if match is None:
            issues.append({"code": "unexpected_snapshot_file", "path": str(path)})
            continue
        sequence = int(match.group("sequence"))
        if sequence in seen_sequences:
            issues.append({"code": "duplicate_snapshot_sequence", "path": str(path)})
        seen_sequences.add(sequence)
        try:
            raw = path.read_bytes()
            digest = sha256_bytes(raw)
            if digest != match.group("sha256"):
                raise LedgerIntegrityError("snapshot filename/content hash mismatch")
            snapshot = strict_load_canonical(raw)
            record = by_sequence.get(sequence)
            if not isinstance(snapshot, Mapping) or record is None:
                raise LedgerIntegrityError("snapshot references a missing record sequence")
            if (
                snapshot.get("schema_version") != SNAPSHOT_SCHEMA_VERSION
                or snapshot.get("ledger_id") != layout.ledger_id
                or snapshot.get("head_sequence") != sequence
                or snapshot.get("head_record_sha256") != record["record_sha256"]
            ):
                raise LedgerIntegrityError("snapshot head contract differs from the record chain")
        except (OSError, LedgerError, ValueError) as exc:
            issues.append(
                {"code": "invalid_snapshot", "path": str(path), "detail": str(exc)}
            )
    return issues


def audit_ledger(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    issues: list[dict[str, str]] = []
    records: list[dict[str, Any]] = []
    state = _LedgerState()
    try:
        records, state = _load_record_chain(layout)
    except (OSError, LedgerError, ValueError) as exc:
        issues.append({"code": "invalid_record_chain", "path": str(layout.records), "detail": str(exc)})
    if not issues:
        issues.extend(_audit_snapshots(layout, records))
    return {
        "schema_version": 1,
        "ledger_id": ledger_id,
        "ledger_root": str(layout.root),
        "valid": not issues,
        "record_count": len(records),
        "head_sequence": records[-1]["sequence"] if records else None,
        "head_record_sha256": records[-1]["record_sha256"] if records else None,
        "state": state.public(),
        "records": [
            {
                "sequence": row["sequence"],
                "kind": row["kind"],
                "record_sha256": row["record_sha256"],
                "path": row["path"],
            }
            for row in records
        ],
        "issues": issues,
    }


def ledger_status(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    audit = audit_ledger(ledger_root, ledger_id=ledger_id)
    if not audit["valid"]:
        status = "invalid"
    elif audit["record_count"] == 0:
        status = "not_activated"
    else:
        status = str(audit["state"]["phase"])
    return {
        "status": status,
        "valid": audit["valid"],
        "ledger_id": audit["ledger_id"],
        "ledger_root": audit["ledger_root"],
        "record_count": audit["record_count"],
        "head_sequence": audit["head_sequence"],
        "head_record_sha256": audit["head_record_sha256"],
        **audit["state"],
        "issues": audit["issues"],
    }


__all__ = [
    "CanonicalJSONError",
    "DEFAULT_LEDGER_ID",
    "DEFAULT_RELEASE",
    "LedgerError",
    "LedgerIntegrityError",
    "LedgerLayout",
    "LedgerStateError",
    "WEIGHT_SCALE_PPM",
    "activate_protocol",
    "append_attestation_receipt",
    "append_correction",
    "append_outcome",
    "audit_ledger",
    "build_decision_plan",
    "canonical_json_bytes",
    "create_only_file",
    "ledger_status",
    "seal_decision",
    "seal_snapshot",
    "sha256_bytes",
    "sha256_file",
    "store_decision_plan",
    "strict_load_canonical",
]
