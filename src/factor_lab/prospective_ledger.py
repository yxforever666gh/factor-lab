"""Create-only, hash-chained evidence for the Factor Lab 5.0 protocol.

The research runner's JSON helpers intentionally optimize for convenient
diagnostic artifacts.  This module is deliberately separate: ledger bytes are
strictly canonical, records never overwrite an existing path, and every state
transition is revalidated while holding a cross-process lock.
"""

from __future__ import annotations

import base64
import binascii
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
LEGACY_PLAN_SCHEMA_VERSION = 1
PLAN_SCHEMA_VERSION = 2
LEGACY_SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_SCHEMA_VERSION = 2
IMPLEMENTATION_UPGRADE_SCHEMA_VERSION = 1
ROUTE_TARGET_PLAN_SCHEMA_VERSION = 1
OUTCOME_SCHEMA_VERSION = 2
DEFAULT_LEDGER_ID = "factor-lab/prospective/5.0"
DEFAULT_RELEASE = "5.0"
FROZEN_REPOSITORY = "yxforever666gh/factor-lab"
FROZEN_REPOSITORY_URL = "https://github.com/yxforever666gh/factor-lab"
SIGSTORE_BUNDLE_MEDIA_TYPE = "application/vnd.dev.sigstore.bundle.v0.3+json"
IN_TOTO_PAYLOAD_TYPE = "application/vnd.in-toto+json"
IN_TOTO_STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
SLSA_PROVENANCE_TYPE = "https://slsa.dev/provenance/v1"
GITHUB_WORKFLOW_BUILD_TYPE = (
    "https://actions.github.io/buildtypes/workflow/v1"
)
WEIGHT_SCALE_PPM = 1_000_000
OFFSET_COUNT = 10
EVALUATION_MILESTONES = (
    ("engineering_closure", 10, 1),
    ("early_stop", 60, 6),
    ("one_year_directional_gate", 250, 25),
)
VERIFICATION_CACHE_SCHEMA_VERSION = 1
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
EVIDENCE_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?Z$"
)
TICKER_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$")
RELEASE_RE = re.compile(r"^(?P<major>[0-9]+)\.(?P<minor>[0-9]+)$")
IMMUTABLE_CAS_PATH_RE = re.compile(
    r"^runtime/prospective/5\.0/source-artifacts/"
    r"sha256=(?P<sha256>[0-9a-f]{64})/artifact$"
)
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


def _optional_available_at_utc(value: datetime | str | None) -> str | None:
    """Normalize an optional evidence cap into JSON-safe UTC text."""

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise LedgerStateError("availability timestamp must be timezone-aware")
        resolved = value.astimezone(timezone.utc)
        if resolved.microsecond:
            return resolved.isoformat(timespec="microseconds").replace("+00:00", "Z")
        return resolved.strftime("%Y-%m-%dT%H:%M:%SZ")
    _parse_evidence_utc(value)
    return value


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


def _parse_evidence_utc(value: Any) -> datetime:
    if not isinstance(value, str) or not EVIDENCE_UTC_RE.fullmatch(value):
        raise LedgerStateError(f"evidence timestamp must be normalized UTC: {value!r}")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").astimezone(timezone.utc)
    except ValueError as exc:
        raise LedgerStateError(f"invalid evidence UTC timestamp: {value!r}") from exc


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
    def inputs(self) -> Path:
        return self.root / "inputs"

    @property
    def executions(self) -> Path:
        return self.root / "executions"

    @property
    def release_runners(self) -> Path:
        return self.root / "release-runners"

    @property
    def dispatch(self) -> Path:
        return self.root / "dispatch"

    @property
    def verification_cache(self) -> Path:
        return self.root / "verification-cache"

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
            self.inputs,
            self.executions,
            self.release_runners,
            self.dispatch,
            self.verification_cache,
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


@contextmanager
def _existing_read_lock(
    layout: LedgerLayout, *, timeout_seconds: float = 15.0
) -> Iterator[None]:
    """Lock an already-created ledger without creating or modifying any path.

    Readiness polling must remain observational even when the ledger is missing
    or incomplete.  The ordinary mutation lock deliberately creates the ledger
    layout and seeds its lock byte; this variant instead requires that exact
    state to exist and only acquires the operating-system lock on it.
    """

    required = (
        layout.root,
        layout.records,
        layout.snapshots,
        layout.bundles,
        layout.release_runners,
    )
    missing = [str(path) for path in required if not path.is_dir()]
    if missing or not layout.lock_path.is_file():
        missing_paths = missing or [str(layout.lock_path)]
        raise LedgerIntegrityError(
            "read-only prospective ledger layout is incomplete: "
            f"{missing_paths}"
        )
    descriptor = os.open(layout.lock_path, os.O_RDWR)
    try:
        if os.fstat(descriptor).st_size < 1:
            raise LedgerIntegrityError("read-only prospective ledger lock is unseeded")
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
                        raise LedgerIntegrityError(
                            "timed out acquiring read-only prospective ledger lock"
                        )
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
                        raise LedgerIntegrityError(
                            "timed out acquiring read-only prospective ledger lock"
                        )
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
    activation_canary_receipt_hash: str | None = None
    active_implementation_hash: str | None = None
    active_implementation: dict[str, Any] | None = None
    latest_implementation_upgrade_hash: str | None = None
    latest_implementation_upgrade: dict[str, Any] | None = None
    pending_previous_implementation_hash: str | None = None
    pending_previous_implementation: dict[str, Any] | None = None
    pending_previous_implementation_canary_receipt_hash: str | None = None
    pending_previous_implementation_tlog_utc: str | None = None
    active_implementation_canary_receipt_hash: str | None = None
    active_implementation_tlog_utc: str | None = None
    # The first successfully attested implementation establishes the
    # prospective epoch.  Corrective implementation releases may replace the
    # active-runtime TLog timestamp, but they must never move the first
    # admissible market session forward.  This field is reconstructed from the
    # immutable record chain and is intentionally excluded from legacy
    # snapshots so existing snapshot hashes remain stable.
    prospective_epoch_tlog_utc: str | None = None
    current_decision_hash: str | None = None
    current_receipt_hash: str | None = None
    pending_decision_plan: dict[str, Any] | None = None
    open_cycles: dict[str, dict[str, Any]] = field(default_factory=dict)
    latest_model_state_sha256: str | None = None
    latest_model_state: dict[str, Any] | None = None
    decision_count: int = 0
    confirmed_observation_count: int = 0
    decision_sessions: set[str] = field(default_factory=set)
    outcome_versions: dict[str, dict[str, Any]] = field(default_factory=dict)
    superseded_versions: set[str] = field(default_factory=set)
    latest_account_states: dict[int, dict[str, Any]] = field(default_factory=dict)
    latest_evaluation_checkpoint_hash: str | None = None
    latest_evaluation: dict[str, Any] | None = None
    completed_evaluation_milestones: set[str] = field(default_factory=set)
    evaluation_due: str | None = None
    direction_rejected: bool = False
    insolvent: bool = False

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

    def decision_generation_ready(self) -> bool:
        return (
            self.active_implementation is not None
            and self.active_implementation_hash is not None
            and self.active_implementation_canary_receipt_hash is not None
            and self.active_implementation_tlog_utc is not None
            and self.phase == "awaiting_decision"
            and self.evaluation_due is None
            and not self.insolvent
        )

    def evaluation_ready(self) -> bool:
        return (
            self.active_implementation is not None
            and self.active_implementation_hash is not None
            and self.active_implementation_canary_receipt_hash is not None
            and self.active_implementation_tlog_utc is not None
            and self.phase in {"awaiting_decision", "awaiting_evaluation"}
            and not self.direction_rejected
        )

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
            "activation_canary_receipt_record_sha256": (
                self.activation_canary_receipt_hash
            ),
            "implementation_upgrade_record_sha256": (
                self.active_implementation_hash
            ),
            "latest_implementation_upgrade_record_sha256": (
                self.latest_implementation_upgrade_hash
            ),
            "implementation_release_tag": (
                (self.active_implementation or {}).get("implementation_release_tag")
            ),
            "implementation_release_tag_object_oid": (
                (self.active_implementation or {}).get(
                    "implementation_release_tag_object_oid"
                )
            ),
            "implementation_commit_oid": (
                (self.active_implementation or {}).get("implementation_commit_oid")
            ),
            "generator_id": (self.active_implementation or {}).get("generator_id"),
            "generator_manifest_sha256": (
                (self.active_implementation or {}).get("generator_manifest_sha256")
            ),
            "generator_test_vector_sha256": (
                (self.active_implementation or {}).get(
                    "generator_test_vector_sha256"
                )
            ),
            "evaluator_id": (self.active_implementation or {}).get("evaluator_id"),
            "evaluation_contract_sha256": (
                (self.active_implementation or {}).get(
                    "evaluation_contract_sha256"
                )
            ),
            "implementation_canary_receipt_record_sha256": (
                self.active_implementation_canary_receipt_hash
            ),
            "implementation_trusted_tlog_timestamp_utc": (
                self.active_implementation_tlog_utc
            ),
            "decision_generation_ready": self.decision_generation_ready(),
            "current_decision_record_sha256": self.current_decision_hash,
            "current_attestation_receipt_record_sha256": self.current_receipt_hash,
            "open_decision_count": len(self.open_cycles),
            "oldest_open_decision_session": min(
                (
                    str(cycle["plan"]["decision_session"])
                    for cycle in self.open_cycles.values()
                ),
                default=None,
            ),
            "latest_model_state_sha256": self.latest_model_state_sha256,
            "latest_account_state_sha256_by_offset": {
                str(offset): str(account["state_sha256"])
                for offset, account in sorted(self.latest_account_states.items())
            },
            "decision_count": self.decision_count,
            "confirmed_observation_count": self.confirmed_observation_count,
            "evaluation_checkpoint_record_sha256": (
                self.latest_evaluation_checkpoint_hash
            ),
            "completed_evaluation_milestones": sorted(
                self.completed_evaluation_milestones
            ),
            "evaluation_due": self.evaluation_due,
            "direction_rejected": self.direction_rejected,
            "insolvent": self.insolvent,
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


def _release_tuple(value: Any, field_name: str) -> tuple[int, int]:
    if not isinstance(value, str):
        raise LedgerStateError(f"{field_name} must be a major.minor release")
    match = RELEASE_RE.fullmatch(value)
    if match is None:
        raise LedgerStateError(f"{field_name} must be a major.minor release")
    return int(match.group("major")), int(match.group("minor"))


IMPLEMENTATION_UPGRADE_KEYS = frozenset(
    {
        "schema_version",
        "activation_record_sha256",
        "supersedes_implementation_upgrade_record_sha256",
        "protocol_id",
        "protocol_sha256",
        "frozen_route",
        "implementation_release_tag",
        "implementation_release_tag_object_oid",
        "implementation_commit_oid",
        "generator_id",
        "generator_entrypoint",
        "generator_manifest_path",
        "generator_manifest_sha256",
        "generator_test_vector_sha256",
        "evaluator_id",
        "evaluation_contract_sha256",
        "decision_plan_schema_version",
    }
)


def _validate_implementation_upgrade(
    payload: Any,
    state: _LedgerState,
) -> Mapping[str, Any]:
    row = _require_exact_keys(
        payload,
        IMPLEMENTATION_UPGRADE_KEYS,
        "implementation upgrade payload",
    )
    if (
        _require_int(row["schema_version"], "schema_version")
        != IMPLEMENTATION_UPGRADE_SCHEMA_VERSION
    ):
        raise LedgerStateError("unsupported implementation upgrade schema")
    if _require_int(
        row["decision_plan_schema_version"], "decision_plan_schema_version"
    ) != PLAN_SCHEMA_VERSION:
        raise LedgerStateError("implementation upgrade does not enable plan schema 2")
    if state.activation is None or state.activation_hash is None:
        raise LedgerStateError("implementation upgrade appears before activation")
    if state.activation_canary_receipt_hash is None:
        raise LedgerStateError("implementation upgrade requires the 5.0 activation canary")
    if state.phase != "awaiting_decision":
        raise LedgerStateError(f"cannot upgrade implementation while phase={state.phase}")
    if state.current_decision_hash is not None or state.open_cycles:
        raise LedgerStateError("implementation upgrade requires no pending or open cycle")
    # A legacy decision has no reconstructible ten-sleeve selection state, and
    # an existing v2 state is bound to the old deployment hash.  Neither may be
    # silently reset to genesis; a future schema must seal an explicit migration.
    if state.decision_count:
        raise LedgerStateError(
            "implementation upgrade after a sealed decision requires an explicit state migration"
        )
    if row["activation_record_sha256"] != state.activation_hash:
        raise LedgerStateError("implementation upgrade binds another activation")
    for field_name in ("protocol_id", "protocol_sha256", "frozen_route"):
        if row[field_name] != state.activation[field_name]:
            raise LedgerStateError(
                f"implementation upgrade {field_name} differs from activation"
            )
    expected_supersedes = state.latest_implementation_upgrade_hash
    if row["supersedes_implementation_upgrade_record_sha256"] != expected_supersedes:
        raise LedgerStateError(
            "implementation upgrade does not supersede the latest attempted binding"
        )
    if expected_supersedes is not None:
        _require_sha256(expected_supersedes, "active implementation upgrade hash")
    for field_name in (
        "implementation_release_tag_object_oid",
        "implementation_commit_oid",
    ):
        _require_oid(row[field_name], field_name)
    for field_name in (
        "generator_manifest_sha256",
        "generator_test_vector_sha256",
        "evaluation_contract_sha256",
    ):
        _require_sha256(row[field_name], field_name)
    for field_name in ("generator_id", "generator_entrypoint", "evaluator_id"):
        if not isinstance(row[field_name], str) or not row[field_name].strip():
            raise LedgerStateError(f"implementation upgrade {field_name} must be non-empty")
    manifest_path = row["generator_manifest_path"]
    if (
        not isinstance(manifest_path, str)
        or not manifest_path
        or "\\" in manifest_path
        or manifest_path.startswith("/")
        or any(part in {"", ".", ".."} for part in manifest_path.split("/"))
    ):
        raise LedgerStateError("generator_manifest_path must be a canonical relative path")
    protocol_release = _release_tuple(
        state.activation["protocol_release"], "activation protocol release"
    )
    implementation_release = _release_tuple(
        row["implementation_release_tag"], "implementation_release_tag"
    )
    prior_release = (
        _release_tuple(
            state.latest_implementation_upgrade["implementation_release_tag"],
            "latest implementation release",
        )
        if state.latest_implementation_upgrade is not None
        else protocol_release
    )
    if implementation_release[0] != protocol_release[0]:
        raise LedgerStateError("implementation upgrade changes the protocol major release")
    if implementation_release <= prior_release:
        raise LedgerStateError("implementation release must increase monotonically")
    if expected_supersedes is None:
        if implementation_release < (5, 2):
            raise LedgerStateError(
                "first implementation_release_tag must be at least '5.2'"
            )
        expected_first_binding = {
            "generator_id": "factor-lab/fixed-core-full-targets/5.2",
            "generator_entrypoint": (
                "factor_lab.prospective_targets:generate_fixed_core_targets"
            ),
            "generator_manifest_path": "protocols/5.2-target-generator.json",
            "evaluator_id": "factor-lab/prospective-evaluation/5.2",
            "evaluation_contract_sha256": (
                "b3aff959751ae317f5783ec0e21fe98b03a2f047e8ec134053252feee8cb3a0c"
            ),
        }
        for field_name, expected in expected_first_binding.items():
            if row[field_name] != expected:
                raise LedgerStateError(
                    f"first implementation upgrade {field_name} must equal {expected!r}"
                )
    return row


LEGACY_PLAN_KEYS = frozenset(
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

PLAN_KEYS = frozenset(
    {
        "schema_version",
        "plan_type",
        "ledger_id",
        "activation_record_sha256",
        "base_head_record_sha256",
        "implementation_upgrade_record_sha256",
        "implementation_attestation_receipt_record_sha256",
        "decision_id",
        "decision_session",
        "route_target_plan_sha256",
        "route_target_plan",
        "source_data_snapshot_sha256",
        "signal_close_utc",
        "input_max_available_at_utc",
        "input_build_checkpoint_utc",
        "information_cutoff_utc",
        "expected_nav_fen",
        "admission_deadline_utc",
        "planned_at_utc",
        "clock_source",
    }
)


TARGET_KEYS = frozenset({"ticker", "target_weight_ppm"})
ROUTE_TARGET_PLAN_KEYS = frozenset(
    {
        "schema_version",
        "route",
        "generator_id",
        "deployment_sha256",
        "input_snapshot_sha256",
        "previous_state_sha256",
        "signal_date",
        "trade_date",
        "calendar_index",
        "due_offset",
        "skipped_sessions",
        "sleeve_plans",
        "aggregate_targets_ppm",
        "aggregate_cash_ppm",
        "next_state",
        "result_sha256",
    }
)
TARGET_STATE_KEYS = frozenset(
    {
        "schema_version",
        "deployment_sha256",
        "activation_record_sha256",
        "implementation_upgrade_record_sha256",
        "last_processed_calendar_index",
        "last_processed_session",
        "sleeves",
        "state_sha256",
    }
)
TARGET_SLEEVE_KEYS = frozenset(
    {
        "offset",
        "capital_fen",
        "initialized",
        "last_signal_date",
        "last_calendar_index",
        "targets_ppm",
        "cash_ppm",
    }
)
TARGET_SLEEVE_PLAN_KEYS = frozenset({"action", *TARGET_SLEEVE_KEYS})


def _validate_targets(
    value: Any,
    *,
    context: str,
    allow_empty: bool,
) -> tuple[list[dict[str, Any]], int]:
    if not isinstance(value, list) or (not allow_empty and not value):
        qualifier = "a list" if allow_empty else "a non-empty list"
        raise LedgerStateError(f"{context} targets must be {qualifier}")
    tickers: list[str] = []
    total = 0
    normalized: list[dict[str, Any]] = []
    for index, target in enumerate(value):
        row = _require_exact_keys(target, TARGET_KEYS, f"{context} target[{index}]")
        ticker = row["ticker"]
        if not isinstance(ticker, str) or not TICKER_RE.fullmatch(ticker):
            raise LedgerStateError(f"invalid canonical ticker at {context} target[{index}]")
        weight = _require_int(
            row["target_weight_ppm"],
            f"{context} target[{index}].target_weight_ppm",
            minimum=1,
            maximum=WEIGHT_SCALE_PPM,
        )
        tickers.append(ticker)
        normalized.append({"ticker": ticker, "target_weight_ppm": weight})
        total += weight
    if tickers != sorted(tickers) or len(tickers) != len(set(tickers)):
        raise LedgerStateError(f"{context} targets must be uniquely sorted by ticker")
    return normalized, total


def route_target_plan_payload_sha256(value: Mapping[str, Any]) -> str:
    """Hash one canonical ``GenerationResult`` mapping."""

    return sha256_bytes(canonical_json_bytes(value))


def _payload_sha256_without(
    value: Mapping[str, Any],
    self_hash_field: str,
    *,
    context: str,
) -> str:
    expected = _require_sha256(value[self_hash_field], self_hash_field)
    payload = dict(value)
    del payload[self_hash_field]
    actual = sha256_bytes(canonical_json_bytes(payload))
    if actual != expected:
        raise LedgerStateError(f"{context} self-hash differs from its canonical payload")
    return actual


def _validate_target_mapping(
    value: Any,
    *,
    context: str,
    allow_empty: bool,
) -> tuple[dict[str, int], int]:
    if not isinstance(value, Mapping) or (not allow_empty and not value):
        qualifier = "a mapping" if allow_empty else "a non-empty mapping"
        raise LedgerStateError(f"{context} targets_ppm must be {qualifier}")
    normalized: dict[str, int] = {}
    total = 0
    for ticker, raw_weight in value.items():
        if not isinstance(ticker, str) or not TICKER_RE.fullmatch(ticker):
            raise LedgerStateError(f"{context} contains an invalid ticker")
        weight = _require_int(
            raw_weight,
            f"{context}.{ticker}",
            minimum=1,
            maximum=WEIGHT_SCALE_PPM,
        )
        normalized[ticker] = weight
        total += weight
    return dict(sorted(normalized.items())), total


def _validate_target_sleeve(
    value: Any,
    *,
    context: str,
    expected_offset: int,
    current_calendar_index: int,
    current_signal_date: date,
) -> dict[str, Any]:
    row = _require_exact_keys(value, TARGET_SLEEVE_KEYS, context)
    offset = _require_int(row["offset"], f"{context}.offset", minimum=0, maximum=9)
    if offset != expected_offset:
        raise LedgerStateError("target sleeves must be sorted offsets 0 through 9")
    capital_fen = _require_int(row["capital_fen"], f"{context}.capital_fen", minimum=1)
    if type(row["initialized"]) is not bool:
        raise LedgerStateError(f"{context}.initialized must be boolean")
    targets, target_total = _validate_target_mapping(
        row["targets_ppm"],
        context=context,
        allow_empty=True,
    )
    cash = _require_int(
        row["cash_ppm"],
        f"{context}.cash_ppm",
        minimum=0,
        maximum=WEIGHT_SCALE_PPM,
    )
    if target_total + cash != WEIGHT_SCALE_PPM:
        raise LedgerStateError(f"{context} target and cash weights do not total one million ppm")
    last_signal_date = row["last_signal_date"]
    last_calendar_index = row["last_calendar_index"]
    if row["initialized"]:
        parsed_signal = _parse_date(last_signal_date)
        parsed_index = _require_int(
            last_calendar_index,
            f"{context}.last_calendar_index",
            minimum=0,
            maximum=current_calendar_index,
        )
        if parsed_signal > current_signal_date:
            raise LedgerStateError(f"{context} last signal date is in the future")
        if parsed_index % OFFSET_COUNT != offset:
            raise LedgerStateError(f"{context} last calendar index differs from its offset")
        if not targets:
            raise LedgerStateError(f"{context} initialized sleeve has no targets")
    elif (
        last_signal_date is not None
        or last_calendar_index is not None
        or targets
        or cash != WEIGHT_SCALE_PPM
    ):
        raise LedgerStateError(
            f"{context} uninitialized sleeve must have null history and remain all cash"
        )
    return {
        "offset": offset,
        "capital_fen": capital_fen,
        "initialized": row["initialized"],
        "last_signal_date": last_signal_date,
        "last_calendar_index": last_calendar_index,
        "targets_ppm": targets,
        "cash_ppm": cash,
    }


def _aggregate_target_state(
    sleeves: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, int], int, int]:
    expected_nav_fen = sum(int(sleeve["capital_fen"]) for sleeve in sleeves)
    if expected_nav_fen <= 0:
        raise LedgerStateError("target state has no positive sleeve capital")
    numerators: dict[str, int] = {}
    cash_numerator = 0
    for sleeve in sleeves:
        capital = int(sleeve["capital_fen"])
        for ticker, weight in sleeve["targets_ppm"].items():
            numerators[ticker] = numerators.get(ticker, 0) + capital * int(weight)
        cash_numerator += capital * int(sleeve["cash_ppm"])
    aggregate: dict[str, int] = {}
    for ticker, numerator in sorted(numerators.items()):
        weight, remainder = divmod(numerator, expected_nav_fen)
        if remainder:
            raise LedgerStateError(
                "sleeve targets cannot be represented exactly in aggregate PPM"
            )
        if weight:
            aggregate[ticker] = weight
    aggregate_cash, cash_remainder = divmod(cash_numerator, expected_nav_fen)
    if cash_remainder:
        raise LedgerStateError("sleeve cash cannot be represented exactly in aggregate PPM")
    return aggregate, aggregate_cash, expected_nav_fen


def _validate_common_plan_identity(
    row: Mapping[str, Any],
    state: _LedgerState,
    *,
    current_head: str,
) -> date:
    if row["plan_type"] != "prospective_decision":
        raise LedgerStateError("unsupported prospective decision plan")
    if row["ledger_id"] != DEFAULT_LEDGER_ID:
        raise LedgerStateError("decision plan ledger_id differs from the frozen ledger")
    if state.phase != "awaiting_decision" or state.activation is None:
        raise LedgerStateError(f"cannot seal a decision while phase={state.phase}")
    if row["activation_record_sha256"] != state.activation_hash:
        raise LedgerStateError("decision plan binds another activation")
    if row["base_head_record_sha256"] != current_head:
        raise LedgerStateError("decision plan is stale relative to the ledger head")
    session = _parse_date(row["decision_session"])
    historical_cutoff = _parse_date(state.activation["historical_data_cutoff"])
    if session <= historical_cutoff:
        raise LedgerStateError("decision session must be strictly after the activation cutoff")
    if row["decision_session"] in state.decision_sessions:
        raise LedgerStateError("decision session was already sealed")
    expected_id = f"{state.activation['protocol_release']}/{row['decision_session']}"
    if row["decision_id"] != expected_id:
        raise LedgerStateError(f"decision_id must equal {expected_id!r}")
    return session


def _validate_legacy_plan(
    plan: Any,
    state: _LedgerState,
    *,
    current_head: str,
) -> Mapping[str, Any]:
    row = _require_exact_keys(plan, LEGACY_PLAN_KEYS, "legacy decision plan")
    if _require_int(row["schema_version"], "schema_version") != LEGACY_PLAN_SCHEMA_VERSION:
        raise LedgerStateError("unsupported legacy decision plan")
    if state.active_implementation is not None:
        raise LedgerStateError("legacy decision plan appears after implementation upgrade")
    session = _validate_common_plan_identity(row, state, current_head=current_head)
    _require_sha256(row["input_snapshot_sha256"], "input_snapshot_sha256")
    _require_sha256(row["model_state_sha256"], "model_state_sha256")
    _require_oid(row["code_commit_oid"], "code_commit_oid")
    if row["code_commit_oid"] != state.activation["release_commit_oid"]:
        raise LedgerStateError("decision plan code commit differs from activation release")
    if row["frozen_route"] != state.activation["frozen_route"]:
        raise LedgerStateError("decision plan frozen route differs from activation")
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
        row["cash_weight_ppm"],
        "cash_weight_ppm",
        minimum=0,
        maximum=WEIGHT_SCALE_PPM,
    )
    _targets, target_total = _validate_targets(
        row["targets"], context="decision", allow_empty=False
    )
    if target_total + cash != WEIGHT_SCALE_PPM:
        raise LedgerStateError("decision target and cash weights must total 1,000,000 ppm")
    if row["clock_source"] != "local_system_clock_untrusted":
        raise LedgerStateError("decision plan clock_source is not diagnostic-only")
    return row


def _validate_route_target_plan(
    value: Any,
    state: _LedgerState,
    *,
    decision_session: str,
) -> Mapping[str, Any]:
    row = _require_exact_keys(value, ROUTE_TARGET_PLAN_KEYS, "route target plan")
    implementation = state.active_implementation
    if implementation is None or state.active_implementation_hash is None:
        raise LedgerStateError("route target plan has no active implementation")
    if _require_int(row["schema_version"], "schema_version") != ROUTE_TARGET_PLAN_SCHEMA_VERSION:
        raise LedgerStateError("unsupported route target plan schema")
    if row["generator_id"] != implementation["generator_id"]:
        raise LedgerStateError("route target plan generator_id differs from active implementation")
    if row["route"] != state.activation["frozen_route"]:
        raise LedgerStateError("route target plan route differs from the frozen activation")
    for field_name in (
        "deployment_sha256",
        "input_snapshot_sha256",
        "previous_state_sha256",
        "result_sha256",
    ):
        _require_sha256(row[field_name], field_name)
    signal_date = _parse_date(row["signal_date"])
    trade_date = _parse_date(row["trade_date"])
    if trade_date != _parse_date(decision_session) or signal_date >= trade_date:
        raise LedgerStateError("route target plan signal/trade dates are inconsistent")
    calendar_index = _require_int(row["calendar_index"], "calendar_index", minimum=0)
    due_offset = _require_int(row["due_offset"], "due_offset", minimum=0, maximum=9)
    if due_offset != calendar_index % OFFSET_COUNT:
        raise LedgerStateError("route target plan due offset differs from calendar index")
    if (
        state.latest_model_state_sha256 is not None
        and row["previous_state_sha256"] != state.latest_model_state_sha256
    ):
        raise LedgerStateError("route target model state is not continuous")

    skipped = row["skipped_sessions"]
    if not isinstance(skipped, list):
        raise LedgerStateError("route target skipped_sessions must be a list")
    normalized_skipped = [_parse_date(value).isoformat() for value in skipped]
    if normalized_skipped != sorted(set(normalized_skipped)):
        raise LedgerStateError("route target skipped_sessions are not unique and increasing")

    next_state = _require_exact_keys(
        row["next_state"], TARGET_STATE_KEYS, "route target next state"
    )
    if _require_int(
        next_state["schema_version"], "next_state.schema_version"
    ) != ROUTE_TARGET_PLAN_SCHEMA_VERSION:
        raise LedgerStateError("unsupported route target state schema")
    for field_name in (
        "deployment_sha256",
        "activation_record_sha256",
        "implementation_upgrade_record_sha256",
        "state_sha256",
    ):
        _require_sha256(next_state[field_name], f"next_state.{field_name}")
    if next_state["deployment_sha256"] != row["deployment_sha256"]:
        raise LedgerStateError("route target result and next state deployment differ")
    if next_state["activation_record_sha256"] != state.activation_hash:
        raise LedgerStateError("route target next state binds another activation")
    if (
        next_state["implementation_upgrade_record_sha256"]
        != state.active_implementation_hash
    ):
        raise LedgerStateError("route target next state binds another implementation upgrade")
    if next_state["last_processed_calendar_index"] != calendar_index:
        raise LedgerStateError("route target next state calendar index differs from result")
    if next_state["last_processed_session"] != row["signal_date"]:
        raise LedgerStateError("route target next state session differs from result")
    _payload_sha256_without(next_state, "state_sha256", context="route target state")

    state_sleeves = next_state["sleeves"]
    sleeve_plans = row["sleeve_plans"]
    if (
        not isinstance(state_sleeves, list)
        or len(state_sleeves) != OFFSET_COUNT
        or not isinstance(sleeve_plans, list)
        or len(sleeve_plans) != OFFSET_COUNT
    ):
        raise LedgerStateError("route target result must contain all ten sleeves")
    normalized_sleeves: list[dict[str, Any]] = []
    for offset, (raw_state_sleeve, raw_plan) in enumerate(
        zip(state_sleeves, sleeve_plans, strict=True)
    ):
        state_sleeve = _validate_target_sleeve(
            raw_state_sleeve,
            context=f"next_state.sleeves[{offset}]",
            expected_offset=offset,
            current_calendar_index=calendar_index,
            current_signal_date=signal_date,
        )
        plan = _require_exact_keys(
            raw_plan, TARGET_SLEEVE_PLAN_KEYS, f"sleeve_plans[{offset}]"
        )
        action = plan["action"]
        if offset == due_offset:
            if action not in {"seed", "rebalance"}:
                raise LedgerStateError("due sleeve action must be seed or rebalance")
        elif action != ("carry" if state_sleeve["initialized"] else "cash"):
            raise LedgerStateError("non-due sleeve action differs from its state")
        plan_state = {key: plan[key] for key in TARGET_SLEEVE_KEYS}
        if canonical_json_bytes(plan_state) != canonical_json_bytes(state_sleeve):
            raise LedgerStateError("route target sleeve plan differs from next state")
        normalized_sleeves.append(state_sleeve)

    aggregate_targets, aggregate_total = _validate_target_mapping(
        row["aggregate_targets_ppm"],
        context="aggregate",
        allow_empty=True,
    )
    aggregate_cash = _require_int(
        row["aggregate_cash_ppm"],
        "aggregate_cash_ppm",
        minimum=0,
        maximum=WEIGHT_SCALE_PPM,
    )
    if aggregate_total + aggregate_cash != WEIGHT_SCALE_PPM:
        raise LedgerStateError("aggregate target weights do not total one million ppm")
    expected_targets, expected_cash, _expected_nav = _aggregate_target_state(
        normalized_sleeves
    )
    if aggregate_targets != expected_targets or aggregate_cash != expected_cash:
        raise LedgerStateError("aggregate targets differ from the ten-sleeve state")
    _payload_sha256_without(row, "result_sha256", context="route target result")
    return row


def _validate_plan(
    plan: Any,
    state: _LedgerState,
    *,
    current_head: str,
    allow_legacy_replay: bool,
) -> Mapping[str, Any]:
    if not isinstance(plan, Mapping):
        raise LedgerStateError("decision plan must be an object")
    schema_version = plan.get("schema_version")
    if schema_version == LEGACY_PLAN_SCHEMA_VERSION:
        if not allow_legacy_replay:
            raise LedgerStateError(
                "legacy manual decision plans are replay-only; implementation upgrade required"
            )
        return _validate_legacy_plan(plan, state, current_head=current_head)
    row = _require_exact_keys(plan, PLAN_KEYS, "decision plan")
    if _require_int(row["schema_version"], "schema_version") != PLAN_SCHEMA_VERSION:
        raise LedgerStateError("unsupported prospective decision plan")
    session = _validate_common_plan_identity(row, state, current_head=current_head)
    if state.evaluation_due is not None:
        raise LedgerStateError(
            f"evaluation checkpoint is due: {state.evaluation_due}"
        )
    if not state.decision_generation_ready():
        raise LedgerStateError("decision generation requires an attested implementation upgrade")
    if row["implementation_upgrade_record_sha256"] != state.active_implementation_hash:
        raise LedgerStateError("decision plan binds another implementation upgrade")
    if (
        row["implementation_attestation_receipt_record_sha256"]
        != state.active_implementation_canary_receipt_hash
    ):
        raise LedgerStateError("decision plan binds another implementation canary")
    _require_sha256(row["route_target_plan_sha256"], "route_target_plan_sha256")
    _require_sha256(row["source_data_snapshot_sha256"], "source_data_snapshot_sha256")
    deadline = _parse_utc(row["admission_deadline_utc"])
    planned = _parse_utc(row["planned_at_utc"])
    expected_deadline = datetime.combine(
        session,
        wall_time(hour=9, minute=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc)
    if deadline != expected_deadline:
        raise LedgerStateError("decision admission deadline is not 09:15 Asia/Shanghai")
    route_plan = _validate_route_target_plan(
        row["route_target_plan"],
        state,
        decision_session=str(row["decision_session"]),
    )
    due_offset = _require_int(
        route_plan["due_offset"], "route target due_offset", minimum=0
    )
    same_offset_open = sum(
        1
        for cycle in state.open_cycles.values()
        if not cycle["legacy_single_slot"]
        and int(cycle["due_offset"]) == due_offset
    )
    if same_offset_open >= 2:
        raise LedgerStateError(
            "two same-offset cycles are already open; an available older outcome "
            "must be sealed before another decision"
        )
    upgrade_tlog = _parse_trusted_utc(state.active_implementation_tlog_utc)
    prospective_epoch_tlog = _parse_trusted_utc(
        state.prospective_epoch_tlog_utc
        or state.active_implementation_tlog_utc
    )
    skipped_sessions = [
        _parse_date(value) for value in route_plan["skipped_sessions"]
    ]
    if state.decision_count:
        if skipped_sessions:
            raise LedgerStateError(
                "sessions cannot be skipped after the first prospective decision"
            )
    else:
        for skipped_session in skipped_sessions:
            session_close = datetime.combine(
                skipped_session,
                wall_time(hour=15),
                tzinfo=ZoneInfo("Asia/Shanghai"),
            ).astimezone(timezone.utc)
            if session_close > prospective_epoch_tlog:
                raise LedgerStateError(
                    "first decision cannot skip a session whose close follows the "
                    "prospective epoch Tlog"
                )
    signal_close = _parse_evidence_utc(row["signal_close_utc"])
    input_maximum = _parse_evidence_utc(row["input_max_available_at_utc"])
    build_checkpoint = _parse_evidence_utc(row["input_build_checkpoint_utc"])
    information_cutoff = _parse_evidence_utc(row["information_cutoff_utc"])
    if not (
        upgrade_tlog
        < signal_close
        < input_maximum
        <= build_checkpoint
        <= information_cutoff
        < deadline
    ):
        raise LedgerStateError(
            "decision timing must satisfy upgrade Tlog < signal close < input availability "
            "<= build checkpoint <= information cutoff < admission deadline"
        )
    if not information_cutoff <= planned < deadline:
        raise LedgerStateError("decision plan creation is not causal before admission")
    expected_nav = _require_int(row["expected_nav_fen"], "expected_nav_fen", minimum=1)
    _targets, _cash, target_nav = _aggregate_target_state(
        route_plan["next_state"]["sleeves"]
    )
    if expected_nav != target_nav:
        raise LedgerStateError("decision expected NAV differs from target sleeve capital")
    if row["route_target_plan_sha256"] != sha256_bytes(
        canonical_json_bytes(route_plan)
    ):
        raise LedgerStateError("decision route target plan hash differs")
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
    if row["purpose"] not in {
        "activation_canary",
        "implementation_upgrade_canary",
        "decision_anchor",
    }:
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
    workflow_name = str(row["workflow_path"]).rsplit("/", 1)[-1]
    expected_request_id = sha256_bytes(
        canonical_json_bytes(
            {
                "release_ref": row["workflow_ref"],
                "repository": FROZEN_REPOSITORY,
                "snapshot_sha256": row["snapshot_sha256"],
                "workflow": workflow_name,
            }
        )
    )
    if row["request_id"] != expected_request_id:
        raise LedgerStateError("receipt request id differs from its dispatch binding")
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
    if row["subject_name"] != f"prospective-snapshot-{row['snapshot_sha256']}.json":
        raise LedgerStateError("attestation subject name differs from the snapshot")
    return row


def _validate_receipt_record_timing(
    receipt: Mapping[str, Any],
    record: Mapping[str, Any],
    *,
    decision_plan: Mapping[str, Any] | None = None,
) -> None:
    created = _parse_utc(receipt["workflow_run_created_at_utc"])
    completed = _parse_utc(receipt["workflow_run_completed_at_utc"])
    recorded = _parse_utc(record["recorded_at_utc"])
    trusted_tlog = _parse_trusted_utc(
        receipt["verified_tlog_timestamp_utc"]
    )
    if not created <= trusted_tlog <= completed <= recorded:
        raise LedgerStateError(
            "receipt timing must satisfy workflow creation <= Tlog <= completion "
            "<= ledger recording"
        )
    if receipt["purpose"] != "decision_anchor":
        return
    if decision_plan is None:
        raise LedgerStateError("decision receipt has no pending decision plan")
    deadline = _parse_utc(decision_plan["admission_deadline_utc"])
    evidence_times = [
        _parse_evidence_utc(decision_plan["input_max_available_at_utc"]),
        _parse_evidence_utc(decision_plan["information_cutoff_utc"]),
    ]
    if decision_plan["schema_version"] == PLAN_SCHEMA_VERSION:
        evidence_times.append(
            _parse_evidence_utc(decision_plan["input_build_checkpoint_utc"])
        )
    if not max(evidence_times) <= trusted_tlog < deadline:
        raise LedgerStateError(
            "decision receipt Tlog is outside the evidence-ready admission window"
        )
    if created >= deadline:
        raise LedgerStateError(
            "attestation workflow was created at or after admission"
        )


def _attestation_bundle_path(
    layout: LedgerLayout, receipt: Mapping[str, Any]
) -> Path:
    snapshot_sha = _require_sha256(
        receipt["snapshot_sha256"], "snapshot_sha256"
    )
    bundle_sha = _require_sha256(
        receipt["attestation_bundle_sha256"], "attestation_bundle_sha256"
    )
    return layout.bundles / f"{snapshot_sha}-{bundle_sha}.jsonl"


def _decode_bundle_payload(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, str) or not value:
        raise LedgerIntegrityError("attestation DSSE payload must be non-empty base64")
    try:
        raw = base64.b64decode(value, validate=True)
        decoded = json.loads(
            raw.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_pairs,
            parse_constant=_reject_constant,
        )
    except (
        binascii.Error,
        UnicodeError,
        json.JSONDecodeError,
        CanonicalJSONError,
    ) as exc:
        raise LedgerIntegrityError(
            "attestation DSSE payload is not strict base64 UTF-8 JSON"
        ) from exc
    if not isinstance(decoded, Mapping):
        raise LedgerIntegrityError("attestation DSSE payload must be a JSON object")
    return decoded


def _bundle_provenance_claims(value: Any) -> tuple[dict[str, Any], list[datetime]]:
    bundle = _require_exact_keys(
        value,
        {"mediaType", "verificationMaterial", "dsseEnvelope"},
        "Sigstore bundle",
    )
    if bundle["mediaType"] != SIGSTORE_BUNDLE_MEDIA_TYPE:
        raise LedgerIntegrityError("attestation bundle media type differs")
    material = bundle["verificationMaterial"]
    if not isinstance(material, Mapping) or set(material) not in (
        {"certificate", "tlogEntries"},
        {"certificate", "tlogEntries", "timestampVerificationData"},
    ):
        raise LedgerIntegrityError(
            "attestation verification material has a non-exact schema"
        )
    certificate = _require_exact_keys(
        material["certificate"], {"rawBytes"}, "attestation certificate"
    )
    try:
        if not base64.b64decode(certificate["rawBytes"], validate=True):
            raise ValueError("empty certificate")
    except (TypeError, ValueError, binascii.Error) as exc:
        raise LedgerIntegrityError(
            "attestation certificate bytes are invalid"
        ) from exc
    tlog_entries = material["tlogEntries"]
    if not isinstance(tlog_entries, list) or not tlog_entries:
        raise LedgerIntegrityError("attestation bundle has no Tlog entries")
    integrated_times: list[datetime] = []
    tlog_keys = {
        "logIndex",
        "logId",
        "kindVersion",
        "integratedTime",
        "inclusionPromise",
        "inclusionProof",
        "canonicalizedBody",
    }
    for index, entry in enumerate(tlog_entries):
        checked = _require_exact_keys(
            entry, tlog_keys, f"attestation Tlog entry {index}"
        )
        integrated = checked["integratedTime"]
        if not isinstance(integrated, str) or not integrated.isdigit():
            raise LedgerIntegrityError(
                f"attestation Tlog entry {index} integratedTime is invalid"
            )
        try:
            integrated_times.append(
                datetime.fromtimestamp(int(integrated), tz=timezone.utc)
            )
        except (OverflowError, OSError, ValueError) as exc:
            raise LedgerIntegrityError(
                f"attestation Tlog entry {index} integratedTime is invalid"
            ) from exc
        for name in ("logId", "kindVersion", "inclusionPromise", "inclusionProof"):
            if not isinstance(checked[name], Mapping):
                raise LedgerIntegrityError(
                    f"attestation Tlog entry {index} {name} must be an object"
                )
        try:
            if not base64.b64decode(checked["canonicalizedBody"], validate=True):
                raise ValueError("empty canonicalized body")
        except (TypeError, ValueError, binascii.Error) as exc:
            raise LedgerIntegrityError(
                f"attestation Tlog entry {index} canonicalizedBody is invalid"
            ) from exc

    envelope = _require_exact_keys(
        bundle["dsseEnvelope"],
        {"payload", "payloadType", "signatures"},
        "attestation DSSE envelope",
    )
    if envelope["payloadType"] != IN_TOTO_PAYLOAD_TYPE:
        raise LedgerIntegrityError("attestation DSSE payload type differs")
    signatures = envelope["signatures"]
    if not isinstance(signatures, list) or not signatures:
        raise LedgerIntegrityError("attestation DSSE envelope has no signature")
    for index, signature in enumerate(signatures):
        checked_signature = _require_exact_keys(
            signature, {"sig"}, f"attestation DSSE signature {index}"
        )
        try:
            if not base64.b64decode(checked_signature["sig"], validate=True):
                raise ValueError("empty signature")
        except (TypeError, ValueError, binascii.Error) as exc:
            raise LedgerIntegrityError(
                f"attestation DSSE signature {index} is invalid"
            ) from exc

    statement = _require_exact_keys(
        _decode_bundle_payload(envelope["payload"]),
        {"_type", "subject", "predicateType", "predicate"},
        "attestation statement",
    )
    if (
        statement["_type"] != IN_TOTO_STATEMENT_TYPE
        or statement["predicateType"] != SLSA_PROVENANCE_TYPE
    ):
        raise LedgerIntegrityError("attestation statement type differs")
    subjects = statement["subject"]
    if not isinstance(subjects, list) or len(subjects) != 1:
        raise LedgerIntegrityError("attestation statement must have one subject")
    subject = _require_exact_keys(
        subjects[0], {"name", "digest"}, "attestation subject"
    )
    digest = _require_exact_keys(
        subject["digest"], {"sha256"}, "attestation subject digest"
    )

    predicate = _require_exact_keys(
        statement["predicate"],
        {"buildDefinition", "runDetails"},
        "attestation predicate",
    )
    build = _require_exact_keys(
        predicate["buildDefinition"],
        {
            "buildType",
            "externalParameters",
            "internalParameters",
            "resolvedDependencies",
        },
        "attestation build definition",
    )
    if build["buildType"] != GITHUB_WORKFLOW_BUILD_TYPE:
        raise LedgerIntegrityError("attestation workflow build type differs")
    external = _require_exact_keys(
        build["externalParameters"],
        {"workflow"},
        "attestation external parameters",
    )
    workflow = dict(
        _require_exact_keys(
            external["workflow"],
            {"ref", "repository", "path"},
            "attestation workflow",
        )
    )
    if not isinstance(build["internalParameters"], Mapping):
        raise LedgerIntegrityError("attestation internal parameters must be an object")
    dependencies = build["resolvedDependencies"]
    if not isinstance(dependencies, list) or len(dependencies) != 1:
        raise LedgerIntegrityError(
            "attestation must bind one resolved workflow dependency"
        )
    dependency = _require_exact_keys(
        dependencies[0], {"uri", "digest"}, "attestation resolved dependency"
    )
    dependency_digest = _require_exact_keys(
        dependency["digest"], {"gitCommit"}, "attestation Git dependency"
    )
    run_details = _require_exact_keys(
        predicate["runDetails"],
        {"builder", "metadata"},
        "attestation run details",
    )
    builder = _require_exact_keys(
        run_details["builder"], {"id"}, "attestation builder"
    )
    metadata = _require_exact_keys(
        run_details["metadata"], {"invocationId"}, "attestation run metadata"
    )
    return (
        {
            "subject_name": subject["name"],
            "subject_sha256": digest["sha256"],
            "workflow_ref": workflow["ref"],
            "workflow_repository": workflow["repository"],
            "workflow_path": workflow["path"],
            "workflow_dependency_uri": dependency["uri"],
            "workflow_source_commit_oid": dependency_digest["gitCommit"],
            "builder_id": builder["id"],
            "run_invocation_uri": metadata["invocationId"],
        },
        integrated_times,
    )


def _verify_attestation_bundle(
    layout: LedgerLayout, receipt: Mapping[str, Any]
) -> Path:
    """Verify stored bytes and cross-check offline provenance claims."""

    path = _attestation_bundle_path(layout, receipt)
    if path.is_symlink() or not path.is_file():
        raise LedgerIntegrityError(f"attestation bundle is missing: {path}")
    raw = path.read_bytes()
    if not raw or sha256_bytes(raw) != receipt["attestation_bundle_sha256"]:
        raise LedgerIntegrityError(f"attestation bundle content hash differs: {path}")
    try:
        text = raw.decode("utf-8", errors="strict")
        lines = text.splitlines()
        if not lines or any(not line.strip() for line in lines):
            raise ValueError("bundle contains an empty JSONL row")
        matching_claims = 0
        for index, line in enumerate(lines):
            value = json.loads(
                line,
                object_pairs_hook=_unique_pairs,
                parse_constant=_reject_constant,
            )
            if not isinstance(value, Mapping):
                raise ValueError(f"bundle row {index} is not a JSON object")
            claims, integrated_times = _bundle_provenance_claims(value)
            expected = {
                "subject_name": receipt["subject_name"],
                "subject_sha256": receipt["subject_sha256"],
                "workflow_ref": receipt["workflow_ref"],
                "workflow_repository": FROZEN_REPOSITORY_URL,
                "workflow_path": receipt["workflow_path"],
                "workflow_dependency_uri": (
                    f"git+{FROZEN_REPOSITORY_URL}@{receipt['workflow_ref']}"
                ),
                "workflow_source_commit_oid": receipt["workflow_source_commit_oid"],
                "builder_id": receipt["certificate_identity"],
                "run_invocation_uri": receipt["run_invocation_uri"],
            }
            if claims == expected:
                receipt_tlog = _parse_trusted_utc(
                    receipt["verified_tlog_timestamp_utc"]
                )
                if min(integrated_times) != receipt_tlog:
                    raise LedgerIntegrityError(
                        "attestation bundle Tlog timestamp differs from receipt"
                    )
                matching_claims += 1
        if matching_claims != 1:
            raise LedgerIntegrityError(
                "attestation bundle must contain exactly one receipt-bound provenance"
            )
    except (UnicodeError, json.JSONDecodeError, CanonicalJSONError, ValueError) as exc:
        raise LedgerIntegrityError(
            f"attestation bundle is not strict UTF-8 JSONL: {path}"
        ) from exc
    return path


def _verify_all_attestation_bundles(
    layout: LedgerLayout, records: Sequence[Mapping[str, Any]]
) -> None:
    for metadata in records:
        record = metadata["record"]
        if record["kind"] == "attestation_receipt":
            _verify_attestation_bundle(layout, record["payload"])


LEGACY_OUTCOME_KEYS = frozenset(
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


OUTCOME_KEYS = frozenset(
    {
        "schema_version",
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "execution_snapshot_sha256",
        "cycle_outcome_sha256",
        "cycle_outcome",
    }
)


def _validate_legacy_outcome(payload: Any) -> Mapping[str, Any]:
    row = _require_exact_keys(payload, LEGACY_OUTCOME_KEYS, "legacy prospective outcome")
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


def _validate_v2_outcome(payload: Any) -> Mapping[str, Any]:
    row = _require_exact_keys(payload, OUTCOME_KEYS, "prospective outcome v2")
    if _require_int(row["schema_version"], "schema_version") != OUTCOME_SCHEMA_VERSION:
        raise LedgerStateError("unsupported prospective outcome schema")
    for field_name in (
        "decision_record_sha256",
        "attestation_receipt_record_sha256",
        "execution_snapshot_sha256",
        "cycle_outcome_sha256",
    ):
        _require_sha256(row[field_name], field_name)
    if not isinstance(row["cycle_outcome"], Mapping):
        raise LedgerStateError("cycle_outcome must be an exact object")
    try:
        from .prospective_execution import CycleOutcome

        cycle = CycleOutcome.from_mapping(row["cycle_outcome"])
    except Exception as exc:
        raise LedgerStateError("embedded cycle outcome is invalid") from exc
    if row["cycle_outcome_sha256"] != cycle.outcome_sha256:
        raise LedgerStateError("cycle outcome hash differs from its embedded payload")
    if row["execution_snapshot_sha256"] != cycle.execution_snapshot_sha256:
        raise LedgerStateError("outcome binds another execution snapshot")
    if cycle.execution_status != "complete" or cycle.not_executed_reason is not None:
        raise LedgerStateError(
            "schema-2 public outcomes must be deterministic complete observations"
        )
    return row


def _is_v2_outcome(payload: Any) -> bool:
    return isinstance(payload, Mapping) and set(payload) == set(OUTCOME_KEYS)


def _evaluation_offset_counts(state: _LedgerState) -> list[int]:
    counts = [0] * OFFSET_COUNT
    for record_hash, payload in state.outcome_versions.items():
        if record_hash in state.superseded_versions or not _is_v2_outcome(payload):
            continue
        cycle = payload["cycle_outcome"]
        offset = _require_int(
            cycle["offset"], "evaluation outcome offset", minimum=0, maximum=9
        )
        counts[offset] += 1
    return counts


def _ready_evaluation_milestones(state: _LedgerState) -> list[str]:
    counts = _evaluation_offset_counts(state)
    total = sum(counts)
    return [
        name
        for name, required_total, minimum_per_offset in EVALUATION_MILESTONES
        if name not in state.completed_evaluation_milestones
        and total >= required_total
        and min(counts) >= minimum_per_offset
    ]


def _update_evaluation_gate_after_outcome(
    state: _LedgerState,
    *,
    ending_nav_fen: int,
) -> None:
    """Apply milestone/terminal gating after one already-validated outcome."""

    if ending_nav_fen == 0:
        state.insolvent = True
        if not state.direction_rejected:
            state.evaluation_due = "terminal_insolvency"
    elif (
        not state.direction_rejected
        and not state.insolvent
        and state.evaluation_due is None
    ):
        ready_milestones = _ready_evaluation_milestones(state)
        state.evaluation_due = ready_milestones[0] if ready_milestones else None
    if state.direction_rejected:
        state.evaluation_due = None
        state.phase = "direction_rejected"
    else:
        state.phase = (
            "awaiting_evaluation"
            if state.evaluation_due is not None
            else "awaiting_decision"
        )


IMPLEMENTATION_ABANDONMENT_KEYS = frozenset(
    {"implementation_upgrade_record_sha256", "reason"}
)
EVALUATION_BINDING_KEYS = frozenset(
    {
        "evaluator_id",
        "evaluation_contract_sha256",
        "ledger_id",
        "ledger_head_record_sha256",
        "implementation_upgrade_record_sha256",
        "outcome_count",
        "outcomes_sha256",
    }
)
EVALUATION_ENVELOPE_KEYS = frozenset(
    {
        "schema_version",
        "binding",
        "evaluation",
        "evaluation_envelope_sha256",
    }
)


def _validate_implementation_abandonment(
    payload: Any, state: _LedgerState
) -> Mapping[str, Any]:
    row = _require_exact_keys(
        payload,
        IMPLEMENTATION_ABANDONMENT_KEYS,
        "implementation upgrade abandonment",
    )
    if (
        state.phase != "awaiting_implementation_attestation"
        or state.active_implementation_hash is None
        or state.active_implementation_canary_receipt_hash is not None
    ):
        raise LedgerStateError(
            "implementation abandonment requires an unattested pending upgrade"
        )
    if state.decision_count or state.current_decision_hash is not None or state.open_cycles:
        raise LedgerStateError(
            "implementation abandonment is allowed only before every decision"
        )
    if row["implementation_upgrade_record_sha256"] != state.active_implementation_hash:
        raise LedgerStateError("implementation abandonment binds another upgrade")
    _require_sha256(
        row["implementation_upgrade_record_sha256"],
        "implementation_upgrade_record_sha256",
    )
    if not isinstance(row["reason"], str) or not row["reason"].strip():
        raise LedgerStateError("implementation abandonment reason must be non-empty")
    return row


def _validate_evaluation_envelope(
    payload: Any,
    state: _LedgerState,
    *,
    ledger_id: str,
    evaluated_head_record_sha256: str,
) -> Mapping[str, Any]:
    envelope = _require_exact_keys(
        payload, EVALUATION_ENVELOPE_KEYS, "evaluation checkpoint"
    )
    if (
        _require_int(envelope["schema_version"], "evaluation schema_version")
        != SCHEMA_VERSION
    ):
        raise LedgerStateError("unsupported evaluation checkpoint schema")
    if (
        not state.evaluation_ready()
        or state.active_implementation is None
        or state.active_implementation_hash is None
        or state.direction_rejected
    ):
        raise LedgerStateError(
            "evaluation checkpoint requires a non-terminal attested implementation"
        )
    binding = _require_exact_keys(
        envelope["binding"], EVALUATION_BINDING_KEYS, "evaluation binding"
    )
    expected = {
        "evaluator_id": state.active_implementation["evaluator_id"],
        "evaluation_contract_sha256": state.active_implementation[
            "evaluation_contract_sha256"
        ],
        "ledger_id": ledger_id,
        "ledger_head_record_sha256": evaluated_head_record_sha256,
        "implementation_upgrade_record_sha256": state.active_implementation_hash,
    }
    for field_name, wanted in expected.items():
        if binding[field_name] != wanted:
            raise LedgerStateError(
                f"evaluation checkpoint {field_name} differs from the ledger"
            )
    outcome_count = _require_int(
        binding["outcome_count"], "evaluation outcome_count", minimum=0
    )
    rich_outcome_count = sum(
        _is_v2_outcome(value) for value in state.outcome_versions.values()
    )
    if outcome_count != rich_outcome_count:
        raise LedgerStateError("evaluation checkpoint outcome count differs")
    _require_sha256(binding["outcomes_sha256"], "evaluation outcomes_sha256")
    evaluation = envelope["evaluation"]
    if not isinstance(evaluation, Mapping):
        raise LedgerStateError("evaluation checkpoint payload must be an object")
    if type(evaluation.get("reject_major_direction")) is not bool:
        raise LedgerStateError("evaluation rejection flag must be boolean")
    if type(evaluation.get("direction_gate_passed")) is not bool:
        raise LedgerStateError("evaluation direction gate flag must be boolean")
    if evaluation["reject_major_direction"] and evaluation["direction_gate_passed"]:
        raise LedgerStateError("evaluation cannot both reject and pass the direction")
    payload_without_hash = {
        key: envelope[key]
        for key in envelope
        if key != "evaluation_envelope_sha256"
    }
    if _require_sha256(
        envelope["evaluation_envelope_sha256"],
        "evaluation_envelope_sha256",
    ) != sha256_bytes(canonical_json_bytes(payload_without_hash)):
        raise LedgerStateError("evaluation checkpoint envelope hash differs")
    return envelope


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
    if kind == "implementation_upgrade":
        upgrade = dict(_validate_implementation_upgrade(payload, state))
        state.pending_previous_implementation_hash = state.active_implementation_hash
        state.pending_previous_implementation = deepcopy(state.active_implementation)
        state.pending_previous_implementation_canary_receipt_hash = (
            state.active_implementation_canary_receipt_hash
        )
        state.pending_previous_implementation_tlog_utc = (
            state.active_implementation_tlog_utc
        )
        state.active_implementation_hash = record_hash
        state.active_implementation = upgrade
        state.latest_implementation_upgrade_hash = record_hash
        state.latest_implementation_upgrade = deepcopy(upgrade)
        state.active_implementation_canary_receipt_hash = None
        state.active_implementation_tlog_utc = None
        state.phase = "awaiting_implementation_attestation"
        return
    if kind == "implementation_upgrade_abandonment":
        _validate_implementation_abandonment(payload, state)
        state.active_implementation_hash = state.pending_previous_implementation_hash
        state.active_implementation = deepcopy(
            state.pending_previous_implementation
        )
        state.active_implementation_canary_receipt_hash = (
            state.pending_previous_implementation_canary_receipt_hash
        )
        state.active_implementation_tlog_utc = (
            state.pending_previous_implementation_tlog_utc
        )
        state.pending_previous_implementation_hash = None
        state.pending_previous_implementation = None
        state.pending_previous_implementation_canary_receipt_hash = None
        state.pending_previous_implementation_tlog_utc = None
        state.phase = "awaiting_decision"
        return
    if kind == "decision":
        row = _require_exact_keys(payload, {"plan_sha256", "plan"}, "decision payload")
        if not isinstance(row["plan"], Mapping):
            raise LedgerStateError("embedded decision plan must be an object")
        plan = dict(row["plan"])
        _require_sha256(row["plan_sha256"], "plan_sha256")
        if row["plan_sha256"] != sha256_bytes(canonical_json_bytes(plan)):
            raise LedgerStateError("decision plan hash differs from embedded plan")
        _validate_plan(
            plan,
            state,
            current_head=str(previous_hash or ""),
            allow_legacy_replay=True,
        )
        sealed = _parse_utc(record["recorded_at_utc"])
        planned = _parse_utc(plan["planned_at_utc"])
        deadline = _parse_utc(plan["admission_deadline_utc"])
        if not planned <= sealed < deadline:
            raise LedgerStateError(
                "decision sealing must be at or after plan creation and before admission"
            )
        state.phase = "awaiting_receipt"
        state.current_decision_hash = record_hash
        state.current_receipt_hash = None
        state.pending_decision_plan = plan
        if plan["schema_version"] == PLAN_SCHEMA_VERSION:
            next_state = plan["route_target_plan"]["next_state"]
            state.latest_model_state_sha256 = str(next_state["state_sha256"])
            state.latest_model_state = deepcopy(next_state)
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
        _validate_receipt_record_timing(
            row,
            record,
            decision_plan=(
                state.pending_decision_plan
                if row["purpose"] == "decision_anchor"
                else None
            ),
        )
        if row["purpose"] == "activation_canary":
            if (
                state.phase != "awaiting_decision"
                or state.decision_count != 0
                or previous_hash != state.activation_hash
                or row["snapshot_head_record_sha256"] != state.activation_hash
                or row["decision_record_sha256"] is not None
                or state.activation_canary_receipt_hash is not None
            ):
                raise LedgerStateError("activation canary does not bind the untouched activation")
            state.activation_canary_receipt_hash = record_hash
            return
        if row["purpose"] == "implementation_upgrade_canary":
            if (
                state.phase != "awaiting_implementation_attestation"
                or state.active_implementation_hash is None
                or previous_hash != state.active_implementation_hash
                or row["snapshot_head_record_sha256"]
                != state.active_implementation_hash
                or row["decision_record_sha256"] is not None
                or state.active_implementation_canary_receipt_hash is not None
            ):
                raise LedgerStateError(
                    "implementation canary does not bind the active upgrade"
                )
            state.active_implementation_canary_receipt_hash = record_hash
            trusted_tlog = str(row["verified_tlog_timestamp_utc"])
            state.active_implementation_tlog_utc = trusted_tlog
            if state.prospective_epoch_tlog_utc is None:
                state.prospective_epoch_tlog_utc = trusted_tlog
            state.pending_previous_implementation_hash = None
            state.pending_previous_implementation = None
            state.pending_previous_implementation_canary_receipt_hash = None
            state.pending_previous_implementation_tlog_utc = None
            state.phase = "awaiting_decision"
            return
        if (
            state.phase != "awaiting_receipt"
            or previous_hash != state.current_decision_hash
            or row["snapshot_head_record_sha256"] != state.current_decision_hash
            or row["decision_record_sha256"] != state.current_decision_hash
            or state.pending_decision_plan is None
        ):
            raise LedgerStateError("decision attestation receipt does not bind the current decision")
        decision_hash = str(state.current_decision_hash)
        plan = state.pending_decision_plan
        if decision_hash in state.open_cycles:
            raise LedgerStateError("decision attestation receipt duplicates an open cycle")
        state.open_cycles[decision_hash] = {
            "receipt_hash": record_hash,
            "plan": plan,
            "legacy_single_slot": plan["schema_version"]
            == LEGACY_PLAN_SCHEMA_VERSION,
        }
        if plan["schema_version"] == PLAN_SCHEMA_VERSION:
            generation_result = plan["route_target_plan"]
            state.open_cycles[decision_hash].update(
                {
                    "calendar_index": generation_result["calendar_index"],
                    "due_offset": generation_result["due_offset"],
                    "holding_start_date": generation_result["trade_date"],
                }
            )
        state.pending_decision_plan = None
        if plan["schema_version"] == LEGACY_PLAN_SCHEMA_VERSION:
            state.phase = "awaiting_outcome"
            state.current_receipt_hash = record_hash
        else:
            state.phase = "awaiting_decision"
            state.current_decision_hash = None
            state.current_receipt_hash = None
        return
    if kind == "outcome":
        rich_v2 = _is_v2_outcome(payload)
        row = dict(
            _validate_v2_outcome(payload)
            if rich_v2
            else _validate_legacy_outcome(payload)
        )
        if state.phase in {"awaiting_receipt", "awaiting_implementation_attestation"}:
            raise LedgerStateError("outcome cannot interrupt a pending attestation")
        decision_hash = str(row["decision_record_sha256"])
        cycle = state.open_cycles.get(decision_hash)
        if cycle is None or row["attestation_receipt_record_sha256"] != cycle["receipt_hash"]:
            raise LedgerStateError("outcome does not close an exact open decision/receipt pair")
        if cycle["legacy_single_slot"] and (
            state.phase != "awaiting_outcome"
            or previous_hash != cycle["receipt_hash"]
        ):
            raise LedgerStateError("legacy outcome is not adjacent to its attestation receipt")
        plan = cycle["plan"]
        if plan["schema_version"] == PLAN_SCHEMA_VERSION:
            cycle = _open_v2_cycle(state, decision_hash)
            if not rich_v2:
                raise LedgerStateError(
                    "schema-2 decisions require a replayable rich outcome; legacy scalar outcomes are replay-only"
                )
            try:
                from .prospective_execution import CycleOutcome, SleeveAccountState

                outcome = CycleOutcome.from_mapping(row["cycle_outcome"])
                generation = plan["route_target_plan"]
                offset = int(generation["due_offset"])
                previous_account = state.latest_account_states.get(offset)
                if previous_account is None:
                    expected_previous = SleeveAccountState.genesis(
                        deployment_sha256=str(generation["deployment_sha256"]),
                        offset=offset,
                    )
                else:
                    expected_previous = SleeveAccountState.from_mapping(previous_account)
            except Exception as exc:
                if isinstance(exc, LedgerStateError):
                    raise
                raise LedgerStateError("outcome accounting state is invalid") from exc
            if outcome.generation_result_sha256 != generation["result_sha256"]:
                raise LedgerStateError("outcome binds another generated target result")
            if outcome.deployment_sha256 != generation["deployment_sha256"]:
                raise LedgerStateError("outcome deployment differs from the decision")
            if outcome.offset != offset:
                raise LedgerStateError("outcome offset differs from the due sleeve")
            if outcome.signal_date != generation["signal_date"]:
                raise LedgerStateError("outcome signal date differs from the decision")
            if outcome.holding_start_date != generation["trade_date"]:
                raise LedgerStateError("outcome holding start differs from the decision trade date")
            if outcome.previous_account_state_sha256 != expected_previous.state_sha256:
                raise LedgerStateError(
                    "same-offset outcome does not continue the latest sealed account state"
                )
            state.latest_account_states[offset] = outcome.next_account_state.to_dict()
        elif rich_v2:
            raise LedgerStateError("rich outcomes cannot be attached to a legacy decision")
        recorded = _parse_utc(record["recorded_at_utc"])
        observation_available = (
            row["cycle_outcome"]["observation_available_at_utc"]
            if rich_v2
            else row["observation_available_at_utc"]
        )
        if recorded < _parse_utc(observation_available):
            raise LedgerStateError("outcome was recorded before its source became available")
        state.outcome_versions[record_hash] = row
        state.confirmed_observation_count += 1
        del state.open_cycles[decision_hash]
        ending_nav_fen = (
            outcome.ending_nav_fen if rich_v2 else row["ending_nav_fen"]
        )
        _update_evaluation_gate_after_outcome(
            state,
            ending_nav_fen=ending_nav_fen,
        )
        if cycle["legacy_single_slot"]:
            state.current_decision_hash = None
            state.current_receipt_hash = None
        return
    if kind == "evaluation_checkpoint":
        envelope = dict(
            _validate_evaluation_envelope(
                payload,
                state,
                ledger_id=str(record["ledger_id"]),
                evaluated_head_record_sha256=str(previous_hash or ""),
            )
        )
        state.latest_evaluation_checkpoint_hash = record_hash
        state.latest_evaluation = envelope
        ready_milestones = _ready_evaluation_milestones(state)
        state.completed_evaluation_milestones.update(ready_milestones)
        state.evaluation_due = None
        if envelope["evaluation"]["reject_major_direction"] is True:
            state.direction_rejected = True
            state.phase = "direction_rejected"
        else:
            if state.insolvent:
                raise LedgerStateError(
                    "insolvent outcomes require terminal direction rejection"
                )
            state.phase = "awaiting_decision"
        return
    if kind == "correction":
        row = _require_exact_keys(
            payload,
            {"supersedes_record_sha256", "reason", "replacement_outcome", "source_snapshot_sha256"},
            "outcome correction",
        )
        if state.phase != "awaiting_decision":
            raise LedgerStateError("correction cannot interrupt a pending attestation")
        supersedes = _require_sha256(row["supersedes_record_sha256"], "supersedes_record_sha256")
        _require_sha256(row["source_snapshot_sha256"], "source_snapshot_sha256")
        if supersedes not in state.outcome_versions:
            raise LedgerStateError("correction does not reference an outcome or prior correction")
        if supersedes in state.superseded_versions:
            raise LedgerStateError("correction creates a fork from an already superseded version")
        if not isinstance(row["reason"], str) or not row["reason"].strip():
            raise LedgerStateError("correction reason must be non-empty")
        previous = state.outcome_versions[supersedes]
        if _is_v2_outcome(previous):
            raise LedgerStateError(
                "schema-2 outcomes cannot be manually corrected; a replayable correction schema is required"
            )
        replacement = dict(_validate_legacy_outcome(row["replacement_outcome"]))
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
        if (
            _require_int(row["schema_version"], "record schema_version")
            != SCHEMA_VERSION
            or row["ledger_id"] != layout.ledger_id
        ):
            raise LedgerIntegrityError(f"record schema or ledger id differs: {path.name}")
        if (
            _require_int(row["sequence"], "record sequence", minimum=1)
            != expected_sequence
            or int(match.group("sequence")) != expected_sequence
        ):
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
    payload = {
        "schema_version": LEGACY_SNAPSHOT_SCHEMA_VERSION,
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
    if state.active_implementation is None:
        return payload
    implementation = state.active_implementation
    return {
        **payload,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        "activation_canary_receipt_record_sha256": (
            state.activation_canary_receipt_hash
        ),
        "implementation_upgrade_record_sha256": state.active_implementation_hash,
        "implementation_release_tag": implementation[
            "implementation_release_tag"
        ],
        "implementation_release_tag_object_oid": implementation[
            "implementation_release_tag_object_oid"
        ],
        "implementation_commit_oid": implementation["implementation_commit_oid"],
        "generator_id": implementation["generator_id"],
        "generator_manifest_sha256": implementation["generator_manifest_sha256"],
        "generator_test_vector_sha256": implementation[
            "generator_test_vector_sha256"
        ],
        "evaluator_id": implementation["evaluator_id"],
        "evaluation_contract_sha256": implementation[
            "evaluation_contract_sha256"
        ],
        "implementation_canary_receipt_record_sha256": (
            state.active_implementation_canary_receipt_hash
        ),
        "implementation_trusted_tlog_timestamp_utc": (
            state.active_implementation_tlog_utc
        ),
        "decision_generation_ready": state.decision_generation_ready(),
        "open_decision_count": len(state.open_cycles),
        "latest_model_state_sha256": state.latest_model_state_sha256,
        "latest_account_state_sha256_by_offset": {
            str(offset): str(account["state_sha256"])
            for offset, account in sorted(state.latest_account_states.items())
        },
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
        records, state, _generated = _load_verified_record_chain(layout)
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
        records, state, _generated = _load_verified_record_chain(layout)
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


def append_implementation_upgrade(
    ledger_root: str | Path,
    upgrade: Mapping[str, Any],
    *,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Append an operational implementation without changing the 5.0 protocol."""

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    normalized = dict(_normalize_json(upgrade))
    with _exclusive_lock(layout):
        # A version transition is launched from the *new* pinned runtime, so
        # it cannot truthfully equal the previously active distribution set.
        # Reject state-incompatible requests from structural state first, then
        # verify every historical capsule byte without executing old research
        # operations.  The newly materialised capsule below must match the
        # running environment exactly before the record can be appended.
        _structural_records, structural_state = _load_record_chain(layout)
        _validate_implementation_upgrade(normalized, structural_state)
        records, state, _generated = _load_verified_record_chain(
            layout,
            require_active_runtime=False,
        )
        _validate_implementation_upgrade(normalized, state)
        _materialize_upgrade_runtime_capsule(layout, normalized)
        _verify_upgrade_runtime_closure(
            layout,
            normalized,
            require_running_environment=True,
        )
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="implementation_upgrade",
            payload=normalized,
            recorded_at_utc=recorded_at_utc,
        )


def abandon_implementation_upgrade(
    ledger_root: str | Path,
    *,
    implementation_upgrade_record_sha256: str,
    reason: str,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Explicitly abandon one unattested upgrade before any decision exists."""

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    payload = {
        "implementation_upgrade_record_sha256": _require_sha256(
            implementation_upgrade_record_sha256,
            "implementation_upgrade_record_sha256",
        ),
        "reason": str(reason),
    }
    with _exclusive_lock(layout):
        records, state, _generated = _load_verified_record_chain(
            layout,
            require_active_runtime=False,
        )
        _validate_implementation_abandonment(payload, state)
        # Abandonment is a create-only control-plane record, not execution of
        # either implementation.  The current environment necessarily matches
        # the failed candidate, not its predecessor.  Both capsules were
        # verified structurally above; operational work remains fail-closed
        # until a runtime matching the restored implementation is used, or a
        # monotonically newer corrective release is bound.
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="implementation_upgrade_abandonment",
            payload=payload,
            recorded_at_utc=recorded_at_utc,
        )


def _implementation_project_root(layout: LedgerLayout) -> Path:
    """Resolve the repository owning the canonical runtime/prospective tree."""

    try:
        return layout.root.parents[2]
    except IndexError as exc:
        raise LedgerIntegrityError(
            "prospective ledger root cannot resolve its implementation repository"
        ) from exc


def _verify_upgrade_runtime_closure(
    layout: LedgerLayout,
    implementation: Mapping[str, Any],
    *,
    require_running_environment: bool = True,
) -> None:
    """Verify a commit capsule without consulting current source bytes."""

    from .prospective_release_runner import verify_release_capsule

    try:
        verify_release_capsule(
            _implementation_project_root(layout),
            layout.release_runners,
            manifest_path=str(implementation["generator_manifest_path"]),
            manifest_sha256=str(implementation["generator_manifest_sha256"]),
            implementation_release_tag=str(
                implementation["implementation_release_tag"]
            ),
            implementation_release_tag_object_oid=str(
                implementation["implementation_release_tag_object_oid"]
            ),
            implementation_commit_oid=str(implementation["implementation_commit_oid"]),
            require_running_environment=require_running_environment,
        )
    except Exception as exc:
        raise LedgerIntegrityError(
            "published prospective release capsule is missing or differs"
        ) from exc


def _materialize_upgrade_runtime_capsule(
    layout: LedgerLayout,
    implementation: Mapping[str, Any],
) -> None:
    """Install published Git blobs only at the explicit upgrade mutation boundary."""

    from .prospective_release_runner import materialize_release_capsule

    try:
        materialize_release_capsule(
            _implementation_project_root(layout),
            layout.release_runners,
            manifest_path=str(implementation["generator_manifest_path"]),
            manifest_sha256=str(implementation["generator_manifest_sha256"]),
            implementation_release_tag=str(
                implementation["implementation_release_tag"]
            ),
            implementation_release_tag_object_oid=str(
                implementation["implementation_release_tag_object_oid"]
            ),
            implementation_commit_oid=str(implementation["implementation_commit_oid"]),
        )
    except Exception as exc:
        raise LedgerIntegrityError(
            "published prospective release capsule cannot be materialized"
        ) from exc


def _active_release_capsule(layout: LedgerLayout, state: _LedgerState) -> Any:
    if state.active_implementation is None:
        raise LedgerStateError("prospective release implementation is not active")
    from .prospective_release_runner import verify_release_capsule

    implementation = state.active_implementation
    try:
        return verify_release_capsule(
            _implementation_project_root(layout),
            layout.release_runners,
            manifest_path=str(implementation["generator_manifest_path"]),
            manifest_sha256=str(implementation["generator_manifest_sha256"]),
            implementation_release_tag=str(
                implementation["implementation_release_tag"]
            ),
            implementation_release_tag_object_oid=str(
                implementation["implementation_release_tag_object_oid"]
            ),
            implementation_commit_oid=str(implementation["implementation_commit_oid"]),
        )
    except Exception as exc:
        raise LedgerIntegrityError("active prospective release capsule differs") from exc


def _run_active_release_operation(
    layout: LedgerLayout,
    state: _LedgerState,
    operation: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    from .prospective_release_runner import run_release_operation

    capsule = _active_release_capsule(layout, state)
    try:
        return run_release_operation(capsule, operation, payload)
    except Exception as exc:
        raise LedgerStateError(
            f"published prospective release operation failed: {operation}"
        ) from exc


def _verify_active_runtime_closure(
    layout: LedgerLayout,
    state: _LedgerState,
) -> None:
    if state.active_implementation is None:
        return
    _verify_upgrade_runtime_closure(
        layout,
        state.active_implementation,
        require_running_environment=True,
    )


def _input_snapshot_plan_metadata(
    value: Any,
    generation_result: Mapping[str, Any],
    *,
    admission_deadline_utc: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise LedgerStateError("route input_snapshot must be an object")
    for field_name in (
        "snapshot_sha256",
        "source_data_snapshot_sha256",
        "signal_date",
        "signal_close_utc",
        "max_available_at_utc",
        "source_build_checkpoint_utc",
        "information_cutoff_utc",
        "admission_deadline_utc",
    ):
        if field_name not in value:
            raise LedgerStateError(f"route input_snapshot is missing {field_name}")
    _payload_sha256_without(value, "snapshot_sha256", context="route input snapshot")
    if value["snapshot_sha256"] != generation_result["input_snapshot_sha256"]:
        raise LedgerStateError("route result binds another input snapshot")
    if value["signal_date"] != generation_result["signal_date"]:
        raise LedgerStateError("route result signal date differs from its input snapshot")
    if value["admission_deadline_utc"] != admission_deadline_utc:
        raise LedgerStateError("route input snapshot binds another admission deadline")
    if "skipped_sessions" in value and value["skipped_sessions"] != generation_result[
        "skipped_sessions"
    ]:
        raise LedgerStateError("route result skipped sessions differ from its input snapshot")
    if "calendar_sessions" in value:
        sessions = value["calendar_sessions"]
        if not isinstance(sessions, list) or not sessions:
            raise LedgerStateError("route input calendar_sessions must be a non-empty list")
        if sessions[-1] != generation_result["trade_date"]:
            raise LedgerStateError("route result trade date differs from its input calendar")
    return {
        "source_data_snapshot_sha256": str(value["source_data_snapshot_sha256"]),
        "signal_close_utc": str(value["signal_close_utc"]),
        "input_max_available_at_utc": str(value["max_available_at_utc"]),
        "input_build_checkpoint_utc": str(value["source_build_checkpoint_utc"]),
        "information_cutoff_utc": str(value["information_cutoff_utc"]),
    }


def _route_target_operation_payload(
    layout: LedgerLayout,
    state: _LedgerState,
    *,
    source_data_snapshot_sha256: str,
    admission_deadline_utc: str | None,
) -> dict[str, Any]:
    if (
        state.activation is None
        or state.activation_hash is None
        or state.active_implementation_hash is None
    ):
        raise LedgerStateError("target regeneration requires an active implementation")
    source_sha = _require_sha256(
        source_data_snapshot_sha256, "source_data_snapshot_sha256"
    )
    return {
        "project_root": str(_implementation_project_root(layout)),
        "source_data_snapshot_sha256": source_sha,
        "deployment_bindings": {
            "activation_record_sha256": str(state.activation_hash),
            "implementation_upgrade_record_sha256": str(
                state.active_implementation_hash
            ),
            "deployment_protocol_sha256": str(
                state.activation["protocol_sha256"]
            ),
        },
        "previous_state": state.latest_model_state,
        "admission_deadline_utc": admission_deadline_utc,
    }


def _regenerate_route_target_plan(
    layout: LedgerLayout,
    state: _LedgerState,
    *,
    source_data_snapshot_sha256: str,
    admission_deadline_utc: str | None,
    release_response: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Replay one target result inside its published commit capsule."""

    operation_payload = _route_target_operation_payload(
        layout,
        state,
        source_data_snapshot_sha256=source_data_snapshot_sha256,
        admission_deadline_utc=admission_deadline_utc,
    )
    response = (
        dict(release_response)
        if release_response is not None
        else _run_active_release_operation(
            layout,
            state,
            "replay_target",
            operation_payload,
        )
    )
    checked = _require_exact_keys(
        response,
        {"generation_result", "deployment", "input_snapshot"},
        "release target response",
    )
    for name in ("generation_result", "deployment", "input_snapshot"):
        if not isinstance(checked[name], Mapping):
            raise LedgerStateError(f"release target response {name} must be an object")
    return (
        dict(checked["generation_result"]),
        dict(checked["deployment"]),
        dict(checked["input_snapshot"]),
    )


def build_membership_evidence(
    ledger_root: str | Path,
    *,
    membership_month: str,
    available_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Build one monthly membership artifact with the active release capsule."""

    try:
        parsed_month = date.fromisoformat(f"{membership_month}-01")
    except (TypeError, ValueError) as exc:
        raise LedgerStateError("membership_month must use canonical YYYY-MM") from exc
    if parsed_month.strftime("%Y-%m") != membership_month:
        raise LedgerStateError("membership_month must use canonical YYYY-MM")
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        _records, state, _generated = _load_verified_record_chain(layout)
        if not state.decision_generation_ready():
            raise LedgerStateError("membership build requires an attested implementation")
        response = _run_active_release_operation(
            layout,
            state,
            "build_membership",
            {
                "project_root": str(_implementation_project_root(layout)),
                "membership_month": membership_month,
                "available_at_utc": _optional_available_at_utc(available_at_utc),
            },
        )
        keys = {
            "membership_month",
            "as_of_date",
            "artifact_sha256",
            "directory",
            "membership_path",
            "manifest_path",
            "source_contract_path",
            "reference_raw_path",
            "completed_at_utc",
        }
        checked = _require_exact_keys(response, keys, "release membership response")
        if checked["membership_month"] != membership_month:
            raise LedgerStateError("release membership response binds another month")
        _require_sha256(checked["artifact_sha256"], "membership artifact sha256")
        return dict(checked)


def build_signal_input_evidence(
    ledger_root: str | Path,
    *,
    signal_date: str,
    available_at_utc: datetime | str | None = None,
    membership_snapshot_path: str | Path | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Build one point-in-time signal artifact with the active release capsule."""

    session = _parse_date(signal_date)
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        _records, state, _generated = _load_verified_record_chain(layout)
        if state.phase != "awaiting_decision" or not state.decision_generation_ready():
            raise LedgerStateError(
                f"cannot build decision input while phase={state.phase}"
            )
        membership = (
            None
            if membership_snapshot_path is None
            else str(Path(membership_snapshot_path).expanduser().resolve())
        )
        response = _run_active_release_operation(
            layout,
            state,
            "build_input",
            {
                "project_root": str(_implementation_project_root(layout)),
                "signal_date": session.isoformat(),
                "available_at_utc": _optional_available_at_utc(available_at_utc),
                "membership_snapshot_path": membership,
            },
        )
        keys = {
            "signal_date",
            "trade_date",
            "source_data_snapshot_sha256",
            "directory",
            "manifest_path",
            "rows_path",
            "build_receipt_path",
            "build_completed_at_utc",
            "inputs_available_at_utc",
        }
        checked = _require_exact_keys(response, keys, "release input response")
        if checked["signal_date"] != session.isoformat():
            raise LedgerStateError("release input response binds another signal date")
        _require_sha256(
            checked["source_data_snapshot_sha256"],
            "source data snapshot sha256",
        )
        return dict(checked)


def _verify_route_target_plan_live(
    layout: LedgerLayout,
    plan: Mapping[str, Any],
    state: _LedgerState,
    *,
    release_response: Mapping[str, Any] | None = None,
) -> None:
    """Regenerate and compare targets at the final append boundary."""

    submitted = plan["route_target_plan"]
    recomputed, deployment, input_snapshot = _regenerate_route_target_plan(
        layout,
        state,
        source_data_snapshot_sha256=str(plan["source_data_snapshot_sha256"]),
        admission_deadline_utc=str(plan["admission_deadline_utc"]),
        release_response=release_response,
    )
    if canonical_json_bytes(recomputed) != canonical_json_bytes(submitted):
        raise LedgerStateError("submitted targets differ from the fixed generator recomputation")
    for field_name in (
        "deployment_sha256",
        "activation_record_sha256",
        "implementation_upgrade_record_sha256",
        "deployment_protocol_sha256",
        "route",
        "generator_id",
    ):
        if field_name not in deployment:
            raise LedgerStateError(f"verified deployment is missing {field_name}")
    _payload_sha256_without(
        deployment,
        "deployment_sha256",
        context="verified target deployment",
    )
    expected_deployment = {
        "deployment_sha256": submitted["deployment_sha256"],
        "activation_record_sha256": state.activation_hash,
        "implementation_upgrade_record_sha256": state.active_implementation_hash,
        "deployment_protocol_sha256": state.activation["protocol_sha256"],
        "route": state.activation["frozen_route"],
        "generator_id": state.active_implementation["generator_id"],
    }
    for field_name, expected in expected_deployment.items():
        if deployment[field_name] != expected:
            raise LedgerStateError(
                f"verified deployment {field_name} differs from the sealed ledger"
            )

    verified_metadata = _input_snapshot_plan_metadata(
        input_snapshot,
        submitted,
        admission_deadline_utc=str(plan["admission_deadline_utc"]),
    )
    for field_name, expected in verified_metadata.items():
        if plan[field_name] != expected:
            raise LedgerStateError(
                f"decision {field_name} differs from the verified input snapshot"
            )


def build_decision_plan(
    ledger_root: str | Path,
    *,
    decision_session: str | None = None,
    source_data_snapshot_sha256: str | None = None,
    route_target_plan: Mapping[str, Any] | None = None,
    input_snapshot: Mapping[str, Any] | None = None,
    information_cutoff_utc: str | None = None,
    input_max_available_at_utc: str | None = None,
    input_snapshot_sha256: str | None = None,
    model_state_sha256: str | None = None,
    code_commit_oid: str | None = None,
    expected_nav_fen: int | None = None,
    targets_ppm: Mapping[str, int] | None = None,
    cash_weight_ppm: int | None = None,
    planned_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    legacy_values = (
        route_target_plan,
        input_snapshot,
        information_cutoff_utc,
        input_max_available_at_utc,
        input_snapshot_sha256,
        model_state_sha256,
        code_commit_oid,
        expected_nav_fen,
        targets_ppm,
        cash_weight_ppm,
    )
    if source_data_snapshot_sha256 is None or any(
        value is not None for value in legacy_values
    ):
        raise LedgerStateError(
            "manual targets/code/input snapshots are disabled; a content-addressed source "
            "snapshot is required"
        )
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    records, state, _generated = _load_verified_record_chain(layout)
    if not records or state.activation is None or state.activation_hash is None:
        raise LedgerStateError("prospective ledger is not activated")
    if state.phase != "awaiting_decision" or not state.decision_generation_ready():
        raise LedgerStateError(
            f"cannot build generated decision while phase={state.phase}"
        )
    normalized_route_plan, _deployment, normalized_input_snapshot = (
        _regenerate_route_target_plan(
            layout,
            state,
            source_data_snapshot_sha256=source_data_snapshot_sha256,
            admission_deadline_utc=None,
        )
    )
    session = _parse_date(normalized_route_plan["trade_date"])
    if decision_session is not None and _parse_date(decision_session) != session:
        raise LedgerStateError(
            "decision_session differs from the released next-trade calendar"
        )
    deadline = datetime.combine(
        session,
        wall_time(hour=9, minute=15),
        tzinfo=ZoneInfo("Asia/Shanghai"),
    ).astimezone(timezone.utc)
    deadline_text = deadline.strftime("%Y-%m-%dT%H:%M:%SZ")
    if normalized_input_snapshot.get("admission_deadline_utc") != deadline_text:
        raise LedgerStateError(
            "released target replay did not derive the canonical admission deadline"
        )
    metadata = _input_snapshot_plan_metadata(
        normalized_input_snapshot,
        normalized_route_plan,
        admission_deadline_utc=deadline_text,
    )
    _targets, _cash, expected_nav_fen = _aggregate_target_state(
        normalized_route_plan["next_state"]["sleeves"]
    )
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan_type": "prospective_decision",
        "ledger_id": layout.ledger_id,
        "activation_record_sha256": state.activation_hash,
        "base_head_record_sha256": records[-1]["record_sha256"],
        "implementation_upgrade_record_sha256": state.active_implementation_hash,
        "implementation_attestation_receipt_record_sha256": (
            state.active_implementation_canary_receipt_hash
        ),
        "decision_id": f"{state.activation['protocol_release']}/{session.isoformat()}",
        "decision_session": session.isoformat(),
        "route_target_plan_sha256": sha256_bytes(
            canonical_json_bytes(normalized_route_plan)
        ),
        "route_target_plan": normalized_route_plan,
        **metadata,
        "expected_nav_fen": expected_nav_fen,
        "admission_deadline_utc": deadline_text,
        "planned_at_utc": _utc_text(planned_at_utc),
        "clock_source": "local_system_clock_untrusted",
    }
    _validate_plan(
        plan,
        state,
        current_head=records[-1]["record_sha256"],
        allow_legacy_replay=False,
    )
    return plan


def store_decision_plan(
    ledger_root: str | Path,
    plan: Mapping[str, Any],
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    records, state, _generated = _load_verified_record_chain(layout)
    if not records:
        raise LedgerStateError("prospective ledger is not activated")
    _validate_plan(
        plan,
        state,
        current_head=records[-1]["record_sha256"],
        allow_legacy_replay=False,
    )
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
        records, state, _generated = _load_verified_record_chain(layout)
        if not records:
            raise LedgerStateError("prospective ledger is not activated")
        _validate_plan(
            resolved_plan,
            state,
            current_head=records[-1]["record_sha256"],
            allow_legacy_replay=False,
        )
        _verify_route_target_plan_live(layout, resolved_plan, state)
        sealed = _parse_utc(sealed_time)
        planned = _parse_utc(resolved_plan["planned_at_utc"])
        deadline = _parse_utc(resolved_plan["admission_deadline_utc"])
        if not planned <= sealed < deadline:
            raise LedgerStateError(
                "decision sealing must be at or after plan creation and before admission"
            )
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
        records, state, _generated = _load_verified_record_chain(layout)
        if not records:
            raise LedgerStateError("prospective ledger is not activated")
        snapshot = _read_snapshot(layout, normalized["snapshot_sha256"])
        if (
            snapshot.get("head_record_sha256") != normalized["snapshot_head_record_sha256"]
            or snapshot.get("head_record_sha256") != records[-1]["record_sha256"]
        ):
            raise LedgerStateError("receipt snapshot is not the current ledger head")
        _verify_attestation_bundle(layout, normalized)
        if normalized["purpose"] == "decision_anchor":
            decision = records[-1]["record"]["payload"]["plan"]
            trusted_tlog = _parse_trusted_utc(
                normalized["verified_tlog_timestamp_utc"]
            )
            deadline = _parse_utc(decision["admission_deadline_utc"])
            if trusted_tlog >= deadline:
                raise LedgerStateError(
                    "trusted transparency-log timestamp is at or after admission"
                )
            if decision["schema_version"] == PLAN_SCHEMA_VERSION:
                evidence_ready = max(
                    _parse_evidence_utc(decision["input_max_available_at_utc"]),
                    _parse_evidence_utc(decision["input_build_checkpoint_utc"]),
                    _parse_evidence_utc(decision["information_cutoff_utc"]),
                )
                if trusted_tlog < evidence_ready:
                    raise LedgerStateError(
                        "trusted transparency-log timestamp predates decision inputs/cutoff"
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


def _regenerate_v2_decision_history(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Replay every schema-2 target decision from its source sidecar."""

    replay_state = _LedgerState()
    generated: dict[str, Any] = {}
    for metadata in records:
        record = metadata["record"]
        if record["kind"] == "decision":
            plan = record["payload"]["plan"]
            if plan["schema_version"] == PLAN_SCHEMA_VERSION:
                _verify_route_target_plan_live(layout, plan, replay_state)
                generated[str(metadata["record_sha256"])] = dict(
                    plan["route_target_plan"]
                )
        _apply_record(
            replay_state,
            record,
            str(metadata["record_sha256"]),
        )
    return generated


def _open_v2_cycle(
    state: _LedgerState,
    decision_record_sha256: str,
) -> Mapping[str, Any]:
    decision_hash = _require_sha256(
        decision_record_sha256, "decision_record_sha256"
    )
    cycle = state.open_cycles.get(decision_hash)
    if cycle is None:
        raise LedgerStateError("outcome does not reference an open decision")
    if cycle["plan"]["schema_version"] != PLAN_SCHEMA_VERSION:
        raise LedgerStateError(
            "legacy decisions accept scalar outcomes only during historical replay"
        )
    offset = int(cycle["due_offset"])
    same_offset = [
        (int(open_cycle["calendar_index"]), open_hash)
        for open_hash, open_cycle in state.open_cycles.items()
        if not open_cycle["legacy_single_slot"]
        and int(open_cycle["due_offset"]) == offset
    ]
    oldest_hash = min(same_offset)[1]
    if decision_hash != oldest_hash:
        raise LedgerStateError(
            "same-offset outcomes must close the oldest open cycle first"
        )
    return cycle


def _previous_account_state(
    state: _LedgerState,
    generation: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return only ledger-sealed state; released code constructs genesis."""

    offset = int(generation["due_offset"])
    sealed = state.latest_account_states.get(offset)
    return None if sealed is None else dict(sealed)


def _outcome_operation_payload(
    layout: LedgerLayout,
    state: _LedgerState,
    *,
    decision_record_sha256: str,
    execution_snapshot_sha256: str,
    generation: Mapping[str, Any],
) -> dict[str, Any]:
    _open_v2_cycle(state, decision_record_sha256)
    execution_sha = _require_sha256(
        execution_snapshot_sha256, "execution_snapshot_sha256"
    )
    return {
        "project_root": str(_implementation_project_root(layout)),
        "generation_result": dict(generation),
        "previous_account_state": _previous_account_state(state, generation),
        "execution_snapshot_sha256": execution_sha,
    }


def _compute_v2_outcome(
    layout: LedgerLayout,
    state: _LedgerState,
    *,
    decision_record_sha256: str,
    execution_snapshot_sha256: str,
    generation: Any,
    release_response: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cycle = _open_v2_cycle(state, decision_record_sha256)
    operation_payload = _outcome_operation_payload(
        layout,
        state,
        decision_record_sha256=decision_record_sha256,
        execution_snapshot_sha256=execution_snapshot_sha256,
        generation=generation,
    )
    execution_sha = str(operation_payload["execution_snapshot_sha256"])
    response = (
        dict(release_response)
        if release_response is not None
        else _run_active_release_operation(
            layout,
            state,
            "replay_outcome",
            operation_payload,
        )
    )
    checked = _require_exact_keys(
        response, {"cycle_outcome"}, "release outcome response"
    )
    if not isinstance(checked["cycle_outcome"], Mapping):
        raise LedgerStateError("release outcome response must contain an object")
    outcome_mapping = dict(checked["cycle_outcome"])
    outcome_sha = _require_sha256(
        outcome_mapping.get("outcome_sha256"), "cycle outcome sha256"
    )
    result = {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "decision_record_sha256": decision_record_sha256,
        "attestation_receipt_record_sha256": str(cycle["receipt_hash"]),
        "execution_snapshot_sha256": execution_sha,
        "cycle_outcome_sha256": outcome_sha,
        "cycle_outcome": outcome_mapping,
    }
    _validate_v2_outcome(result)
    return result


def build_outcome_payload(
    ledger_root: str | Path,
    *,
    decision_record_sha256: str,
    execution_snapshot_sha256: str,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Build a rich outcome from sealed targets and source-backed execution data.

    This is a convenience builder only.  :func:`append_outcome` independently
    repeats the same target and execution replay while holding the ledger lock.
    """

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        records, state, generated = _load_verified_record_chain(layout)
        decision_hash = _require_sha256(
            decision_record_sha256, "decision_record_sha256"
        )
        generation = generated.get(decision_hash)
        if generation is None:
            raise LedgerStateError(
                "outcome decision is not a replayable schema-2 target result"
            )
        return _compute_v2_outcome(
            layout,
            state,
            decision_record_sha256=decision_hash,
            execution_snapshot_sha256=execution_snapshot_sha256,
            generation=generation,
        )


def build_execution_evidence(
    ledger_root: str | Path,
    *,
    decision_record_sha256: str,
    available_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Build the source-backed holding window for one open schema-2 cycle.

    The decision's GenerationResult, source snapshot, and same-offset prior
    account state are always recovered from strict ledger replay.  Callers can
    choose only the observation-availability cap; they cannot supply a roster,
    prices, target result, or accounting state.
    """

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        _records, state, generated = _load_verified_record_chain(layout)
        decision_hash = _require_sha256(
            decision_record_sha256, "decision_record_sha256"
        )
        generation = generated.get(decision_hash)
        if generation is None:
            raise LedgerStateError(
                "execution decision is not a replayable schema-2 target result"
            )
        cycle = _open_v2_cycle(state, decision_hash)
        previous = _previous_account_state(state, generation)
        cap = _optional_available_at_utc(available_at_utc)
        built = _run_active_release_operation(
            layout,
            state,
            "build_execution",
            {
                "project_root": str(_implementation_project_root(layout)),
                "generation_result": dict(generation),
                "source_data_snapshot_sha256": str(
                    cycle["plan"]["source_data_snapshot_sha256"]
                ),
                "previous_account_state": previous,
                "available_at_utc": cap,
            },
        )
        required = {
            "execution_snapshot_sha256",
            "execution_source_sha256",
            "directory",
            "snapshot_path",
            "sources_path",
            "holding_start_date",
            "holding_end_date",
            "observation_available_at_utc",
        }
        if "previous_account_state_sha256" in built:
            required.add("previous_account_state_sha256")
        checked = _require_exact_keys(built, required, "release execution response")
        result = {
            "decision_record_sha256": decision_hash,
            "generation_result_sha256": str(generation["result_sha256"]),
            **dict(checked),
        }
        return result


def _verify_v2_outcome_live(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    state: _LedgerState,
    submitted: Mapping[str, Any],
    *,
    generated: Mapping[str, Any] | None = None,
    release_response: Mapping[str, Any] | None = None,
) -> None:
    row = _validate_v2_outcome(submitted)
    decision_hash = str(row["decision_record_sha256"])
    resolved_generations = (
        dict(generated)
        if generated is not None
        else _regenerate_v2_decision_history(layout, records)
    )
    generation = resolved_generations.get(decision_hash)
    if generation is None:
        raise LedgerStateError(
            "outcome decision is not a replayable schema-2 target result"
        )
    cycle = _open_v2_cycle(state, decision_hash)
    if row["attestation_receipt_record_sha256"] != cycle["receipt_hash"]:
        raise LedgerStateError(
            "outcome does not close an exact open decision/receipt pair"
        )
    recomputed = _compute_v2_outcome(
        layout,
        state,
        decision_record_sha256=decision_hash,
        execution_snapshot_sha256=str(row["execution_snapshot_sha256"]),
        generation=generation,
        release_response=release_response,
    )
    if canonical_json_bytes(recomputed) != canonical_json_bytes(row):
        raise LedgerStateError(
            "submitted rich outcome differs from fixed execution recomputation"
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
    _validate_v2_outcome(normalized)
    with _exclusive_lock(layout):
        records, state, generated = _load_verified_record_chain(layout)
        _verify_v2_outcome_live(
            layout,
            records,
            state,
            normalized,
            generated=generated,
        )
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
        records, state, _generated = _load_verified_record_chain(layout)
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
    expected: dict[int, tuple[str, bytes, Path]] = {}
    replay_state = _LedgerState()
    prefix: list[Mapping[str, Any]] = []
    for row in records:
        _apply_record(
            replay_state,
            row["record"],
            str(row["record_sha256"]),
        )
        prefix.append(row)
        expected_raw = canonical_json_bytes(_snapshot_payload(layout, prefix, replay_state))
        expected_digest = sha256_bytes(expected_raw)
        sequence = int(row["sequence"])
        expected[sequence] = (
            expected_digest,
            expected_raw,
            layout.snapshots / f"{sequence:016d}-{expected_digest}.json",
        )
    if not layout.snapshots.exists():
        return [
            {"code": "missing_snapshot", "path": str(path)}
            for _digest, _raw, path in expected.values()
        ]
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
            expected_row = expected.get(sequence)
            if expected_row is None:
                raise LedgerIntegrityError("snapshot references a missing record sequence")
            expected_digest, expected_raw, expected_path = expected_row
            if path != expected_path or digest != expected_digest or raw != expected_raw:
                raise LedgerIntegrityError(
                    "snapshot bytes differ from deterministic prefix replay"
                )
            strict_load_canonical(raw)
        except (OSError, LedgerError, ValueError) as exc:
            issues.append(
                {"code": "invalid_snapshot", "path": str(path), "detail": str(exc)}
            )
    for sequence, (_digest, _raw, path) in expected.items():
        if sequence not in seen_sequences:
            issues.append({"code": "missing_snapshot", "path": str(path)})
    return issues


def _verification_cache_capsule_identity(
    state: _LedgerState,
) -> dict[str, Any] | None:
    implementation = state.active_implementation
    if implementation is None or state.active_implementation_hash is None:
        return None
    return {
        "implementation_upgrade_record_sha256": state.active_implementation_hash,
        "implementation_release_tag": implementation["implementation_release_tag"],
        "implementation_release_tag_object_oid": implementation[
            "implementation_release_tag_object_oid"
        ],
        "implementation_commit_oid": implementation["implementation_commit_oid"],
        "generator_manifest_sha256": implementation["generator_manifest_sha256"],
        "generator_test_vector_sha256": implementation[
            "generator_test_vector_sha256"
        ],
        "evaluation_contract_sha256": implementation[
            "evaluation_contract_sha256"
        ],
    }


def _record_prefix_sha256(
    records: Sequence[Mapping[str, Any]],
    head_sequence: int,
) -> str:
    return sha256_bytes(
        canonical_json_bytes(
            [
                str(metadata["record_sha256"])
                for metadata in records[:head_sequence]
            ]
        )
    )


def _artifact_tree_sha256(path: Path) -> str:
    if path.is_symlink() or not path.is_dir():
        raise LedgerIntegrityError(f"cached replay artifact is missing: {path}")
    files: list[dict[str, Any]] = []
    for current, directory_names, file_names in os.walk(path, followlinks=False):
        current_path = Path(current)
        for name in directory_names:
            directory = current_path / name
            if directory.is_symlink():
                raise LedgerIntegrityError(
                    f"cached replay artifact contains a symlink: {directory}"
                )
        for name in file_names:
            file_path = current_path / name
            if file_path.is_symlink() or not file_path.is_file():
                raise LedgerIntegrityError(
                    f"cached replay artifact contains a non-file: {file_path}"
                )
            files.append(
                {
                    "path": file_path.relative_to(path).as_posix(),
                    "sha256": sha256_file(file_path),
                }
            )
    files.sort(key=lambda item: str(item["path"]))
    if not files:
        raise LedgerIntegrityError(f"cached replay artifact is empty: {path}")
    return sha256_bytes(canonical_json_bytes(files))


def _json_strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _json_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _json_strings(item)


def _transitive_cas_bindings(
    layout: LedgerLayout,
    contract_path: Path,
) -> list[dict[str, str]]:
    if contract_path.is_symlink() or not contract_path.is_file():
        raise LedgerIntegrityError(
            f"cached replay source contract is missing: {contract_path}"
        )
    contract = strict_load_canonical(contract_path.read_bytes())
    project_root = _implementation_project_root(layout)
    bindings: dict[str, dict[str, str]] = {}
    pending_values = list(_json_strings(contract))
    while pending_values:
        value = pending_values.pop()
        match = IMMUTABLE_CAS_PATH_RE.fullmatch(value)
        if match is None:
            continue
        digest = match.group("sha256")
        if value in bindings:
            continue
        path = (project_root / Path(value)).resolve()
        expected = (
            project_root
            / "runtime"
            / "prospective"
            / "5.0"
            / "source-artifacts"
            / f"sha256={digest}"
            / "artifact"
        ).resolve()
        if (
            path != expected
            or path.is_symlink()
            or not path.is_file()
            or sha256_file(path) != digest
        ):
            raise LedgerIntegrityError(
                f"cached replay transitive CAS bytes differ: {path}"
            )
        bindings[value] = {"path": value, "sha256": digest}
        raw = path.read_bytes()
        try:
            nested_contract = strict_load_canonical(raw)
        except (LedgerError, ValueError):
            continue
        pending_values.extend(_json_strings(nested_contract))
    return [bindings[key] for key in sorted(bindings)]


def _operation_artifact_bindings(
    layout: LedgerLayout,
    item: Mapping[str, Any],
) -> list[dict[str, Any]]:
    operation = str(item["operation"])
    payload = item["payload"]
    if not isinstance(payload, Mapping):
        raise LedgerIntegrityError("cached replay operation payload is not an object")
    if operation == "replay_target":
        artifact_sha = _require_sha256(
            payload.get("source_data_snapshot_sha256"),
            "cached replay input sha256",
        )
        return [
            {
                "kind": "input",
                "artifact_sha256": artifact_sha,
                "tree_sha256": _artifact_tree_sha256(layout.inputs / artifact_sha),
                "transitive_cas": _transitive_cas_bindings(
                    layout,
                    layout.inputs / artifact_sha / "manifest.json",
                ),
            }
        ]
    if operation == "replay_outcome":
        artifact_sha = _require_sha256(
            payload.get("execution_snapshot_sha256"),
            "cached replay execution sha256",
        )
        return [
            {
                "kind": "execution",
                "artifact_sha256": artifact_sha,
                "tree_sha256": _artifact_tree_sha256(
                    layout.executions / artifact_sha
                ),
                "transitive_cas": _transitive_cas_bindings(
                    layout,
                    layout.executions / artifact_sha / "sources.json",
                ),
            }
        ]
    if operation == "evaluate":
        return []
    raise LedgerIntegrityError(f"unsupported cached replay operation: {operation}")


def _load_verification_cache(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    state: _LedgerState,
    pending: Sequence[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    """Load the newest self-consistent capsule result cache for a chain prefix.

    Cache bytes are disposable optimization state, not ledger evidence.  Every
    cached response is still passed through the ordinary record/result
    validators below; malformed, stale, or mismatched files are ignored and
    cause capsule replay instead of ledger repair.
    """

    capsule_identity = _verification_cache_capsule_identity(state)
    if (
        capsule_identity is None
        or not records
        or not pending
        or not layout.verification_cache.is_dir()
    ):
        return {}
    pending_by_hash = {
        str(item["record_sha256"]): item
        for item in pending
    }
    record_sequence_by_hash = {
        str(metadata["record_sha256"]): int(metadata["sequence"])
        for metadata in records
    }
    best_sequence = 0
    best_results: dict[str, Mapping[str, Any]] = {}
    for path in sorted(layout.verification_cache.glob("*.json")):
        try:
            if path.is_symlink() or not path.is_file():
                continue
            value = strict_load_canonical(path.read_bytes())
            row = _require_exact_keys(
                value,
                {
                    "schema_version",
                    "ledger_id",
                    "head_sequence",
                    "head_record_sha256",
                    "record_prefix_sha256",
                    "capsule_identity",
                    "results",
                    "cache_sha256",
                },
                "verification cache",
            )
            payload = {
                key: row[key] for key in row if key != "cache_sha256"
            }
            if (
                _require_int(row["schema_version"], "cache schema_version")
                != VERIFICATION_CACHE_SCHEMA_VERSION
                or row["ledger_id"] != layout.ledger_id
                or _require_sha256(row["cache_sha256"], "cache_sha256")
                != sha256_bytes(canonical_json_bytes(payload))
            ):
                continue
            head_sequence = _require_int(
                row["head_sequence"],
                "cache head_sequence",
                minimum=1,
            )
            if head_sequence > len(records) or head_sequence <= best_sequence:
                continue
            head_hash = _require_sha256(
                row["head_record_sha256"], "cache head_record_sha256"
            )
            if (
                path.name != f"{head_hash}.json"
                or head_hash
                != str(records[head_sequence - 1]["record_sha256"])
                or _require_sha256(
                    row["record_prefix_sha256"], "cache record_prefix_sha256"
                )
                != _record_prefix_sha256(records, head_sequence)
                or row["capsule_identity"] != capsule_identity
                or not isinstance(row["results"], list)
            ):
                continue
            resolved: dict[str, Mapping[str, Any]] = {}
            valid = True
            for index, result in enumerate(row["results"]):
                checked = _require_exact_keys(
                    result,
                    {
                        "record_sha256",
                        "operation_id",
                        "operation",
                        "artifact_bindings",
                        "result",
                    },
                    f"verification cache result {index}",
                )
                record_sha = _require_sha256(
                    checked["record_sha256"],
                    f"verification cache result {index} record_sha256",
                )
                expected = pending_by_hash.get(record_sha)
                if (
                    expected is None
                    or record_sequence_by_hash[record_sha] > head_sequence
                    or record_sha in resolved
                    or checked["operation_id"] != expected["operation_id"]
                    or checked["operation"] != expected["operation"]
                    or checked["artifact_bindings"]
                    != _operation_artifact_bindings(layout, expected)
                    or not isinstance(checked["result"], Mapping)
                ):
                    valid = False
                    break
                resolved[record_sha] = checked["result"]
            expected_prefix_results = {
                record_sha
                for record_sha, item in pending_by_hash.items()
                if record_sequence_by_hash[record_sha] <= head_sequence
            }
            if not valid or set(resolved) != expected_prefix_results:
                continue
            best_sequence = head_sequence
            best_results = resolved
        except (OSError, LedgerError, KeyError, TypeError, ValueError):
            continue
    return best_results


def _write_verification_cache(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    state: _LedgerState,
    pending: Sequence[Mapping[str, Any]],
    replay_results: Mapping[str, Mapping[str, Any]],
) -> None:
    """Atomically refresh the one retained derived cache for the current head."""

    capsule_identity = _verification_cache_capsule_identity(state)
    if capsule_identity is None or not records or not pending:
        return
    head = records[-1]
    results: list[dict[str, Any]] = []
    for item in pending:
        record_sha = str(item["record_sha256"])
        result = replay_results.get(record_sha)
        if result is None:
            return
        try:
            artifact_bindings = _operation_artifact_bindings(layout, item)
        except (OSError, LedgerError, KeyError, TypeError, ValueError):
            return
        results.append(
            {
                "record_sha256": record_sha,
                "operation_id": item["operation_id"],
                "operation": item["operation"],
                "artifact_bindings": artifact_bindings,
                "result": dict(result),
            }
        )
    payload = {
        "schema_version": VERIFICATION_CACHE_SCHEMA_VERSION,
        "ledger_id": layout.ledger_id,
        "head_sequence": int(head["sequence"]),
        "head_record_sha256": str(head["record_sha256"]),
        "record_prefix_sha256": _record_prefix_sha256(records, len(records)),
        "capsule_identity": capsule_identity,
        "results": results,
    }
    value = {
        **payload,
        "cache_sha256": sha256_bytes(canonical_json_bytes(payload)),
    }
    raw = canonical_json_bytes(value)
    target = layout.verification_cache / f"{head['record_sha256']}.json"
    temporary = layout.verification_cache / (
        f".pending-{os.getpid()}-{uuid4().hex}.tmp"
    )
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, target)
        for path in layout.verification_cache.glob("*.json"):
            if path != target and path.is_file() and not path.is_symlink():
                try:
                    path.unlink()
                except OSError:
                    pass
    except OSError:
        try:
            temporary.unlink()
        except OSError:
            pass


def _replay_external_evidence(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    *,
    use_cache: bool = False,
    refresh_cache: bool = False,
    require_active_runtime: bool = True,
) -> tuple[_LedgerState, dict[str, Any]]:
    """Strictly replay the full schema-2 external-evidence prefix."""

    preflight_state = _LedgerState()
    submitted_generations: dict[str, Any] = {}
    submitted_outcomes: list[dict[str, Any]] = []
    pending: list[dict[str, Any]] = []
    implementation_hashes: set[str] = set()
    for metadata in records:
        record = metadata["record"]
        if record["kind"] == "decision":
            plan = record["payload"]["plan"]
            if plan["schema_version"] == PLAN_SCHEMA_VERSION:
                payload = _route_target_operation_payload(
                    layout,
                    preflight_state,
                    source_data_snapshot_sha256=str(
                        plan["source_data_snapshot_sha256"]
                    ),
                    admission_deadline_utc=str(plan["admission_deadline_utc"]),
                )
                pending.append(
                    {
                        "record_sha256": str(metadata["record_sha256"]),
                        "operation": "replay_target",
                        "payload": payload,
                    }
                )
                submitted_generations[str(metadata["record_sha256"])] = dict(
                    plan["route_target_plan"]
                )
                implementation_hashes.add(
                    str(preflight_state.active_implementation_hash)
                )
        elif record["kind"] == "outcome" and _is_v2_outcome(
            record["payload"]
        ):
            decision_hash = str(record["payload"]["decision_record_sha256"])
            generation = submitted_generations.get(decision_hash)
            if generation is None:
                raise LedgerStateError(
                    "outcome decision is not a prior schema-2 target result"
                )
            payload = _outcome_operation_payload(
                layout,
                preflight_state,
                decision_record_sha256=decision_hash,
                execution_snapshot_sha256=str(
                    record["payload"]["execution_snapshot_sha256"]
                ),
                generation=generation,
            )
            pending.append(
                {
                    "record_sha256": str(metadata["record_sha256"]),
                    "operation": "replay_outcome",
                    "payload": payload,
                }
            )
            implementation_hashes.add(
                str(preflight_state.active_implementation_hash)
            )
            submitted_outcomes.append(dict(record["payload"]))
        elif record["kind"] == "evaluation_checkpoint":
            payload = _evaluation_operation_payload(
                layout,
                preflight_state,
                ledger_head_record_sha256=str(record["previous_record_sha256"]),
                outcomes=submitted_outcomes,
            )
            pending.append(
                {
                    "record_sha256": str(metadata["record_sha256"]),
                    "operation": "evaluate",
                    "payload": payload,
                }
            )
            implementation_hashes.add(
                str(preflight_state.active_implementation_hash)
            )
        _apply_record(
            preflight_state,
            record,
            str(metadata["record_sha256"]),
        )
        if record["kind"] == "implementation_upgrade":
            _verify_upgrade_runtime_closure(
                layout,
                record["payload"],
                require_running_environment=False,
            )

    if require_active_runtime:
        _verify_active_runtime_closure(layout, preflight_state)
    elif preflight_state.decision_count or pending:
        raise LedgerIntegrityError(
            "runtime transition verification requires a decision-free ledger"
        )

    if not pending:
        return preflight_state, {}
    active_hash = str(preflight_state.active_implementation_hash)
    if implementation_hashes != {active_hash}:
        raise LedgerIntegrityError(
            "one replay batch cannot mix implementation capsules"
        )
    for item in pending:
        operation_payload = {
            "operation": item["operation"],
            "payload": item["payload"],
        }
        operation_id = sha256_bytes(canonical_json_bytes(operation_payload))
        item["operation_id"] = operation_id
    replay_results: dict[str, Mapping[str, Any]] = (
        _load_verification_cache(layout, records, preflight_state, pending)
        if use_cache
        else {}
    )
    uncached = [
        item
        for item in pending
        if str(item["record_sha256"]) not in replay_results
    ]
    operations: list[dict[str, Any]] = []
    for item in uncached:
        operations.append(
            {
                "operation_id": item["operation_id"],
                "operation": item["operation"],
                "payload": item["payload"],
            }
        )
    if operations:
        batch = _run_active_release_operation(
            layout,
            preflight_state,
            "replay_history",
            {"operations": operations},
        )
        checked_batch = _require_exact_keys(
            batch, {"results"}, "release history replay response"
        )
        results = checked_batch["results"]
        if not isinstance(results, list) or len(results) != len(uncached):
            raise LedgerStateError("release history replay result count differs")
        for index, result in enumerate(results):
            checked = _require_exact_keys(
                result,
                {"operation_id", "operation", "result"},
                f"release history replay result {index}",
            )
            expected = uncached[index]
            if (
                checked["operation_id"] != expected["operation_id"]
                or checked["operation"] != expected["operation"]
                or not isinstance(checked["result"], Mapping)
            ):
                raise LedgerStateError("release history replay result binding differs")
            replay_results[str(expected["record_sha256"])] = checked["result"]

    replay_state = _LedgerState()
    generated: dict[str, Any] = {}
    verified_outcomes: list[dict[str, Any]] = []
    for metadata in records:
        record = metadata["record"]
        record_sha = str(metadata["record_sha256"])
        if record["kind"] == "decision":
            plan = record["payload"]["plan"]
            if plan["schema_version"] == PLAN_SCHEMA_VERSION:
                _verify_route_target_plan_live(
                    layout,
                    plan,
                    replay_state,
                    release_response=replay_results[record_sha],
                )
                generated[record_sha] = dict(plan["route_target_plan"])
        elif record["kind"] == "outcome" and _is_v2_outcome(
            record["payload"]
        ):
            _verify_v2_outcome_live(
                layout,
                records,
                replay_state,
                record["payload"],
                generated=generated,
                release_response=replay_results[record_sha],
            )
            verified_outcomes.append(dict(record["payload"]))
        elif record["kind"] == "evaluation_checkpoint":
            envelope = _validate_release_evaluation_response(
                layout,
                replay_state,
                ledger_head_record_sha256=str(record["previous_record_sha256"]),
                outcomes=verified_outcomes,
                response=replay_results[record_sha],
            )
            if canonical_json_bytes(envelope) != canonical_json_bytes(
                record["payload"]
            ):
                raise LedgerStateError(
                    "evaluation checkpoint differs from capsule recomputation"
                )
        _apply_record(replay_state, record, record_sha)
    if refresh_cache:
        _write_verification_cache(
            layout,
            records,
            replay_state,
            pending,
            replay_results,
        )
    return replay_state, generated


def _load_verified_record_chain(
    layout: LedgerLayout,
    *,
    refresh_cache: bool = True,
    require_active_runtime: bool = True,
) -> tuple[list[dict[str, Any]], _LedgerState, dict[str, Any]]:
    records, _structural_state = _load_record_chain(layout)
    try:
        _verify_all_attestation_bundles(layout, records)
        state, generated = _replay_external_evidence(
            layout,
            records,
            use_cache=True,
            refresh_cache=refresh_cache,
            require_active_runtime=require_active_runtime,
        )
    except (OSError, LedgerError, ValueError) as exc:
        if isinstance(exc, LedgerIntegrityError):
            raise
        raise LedgerIntegrityError(
            "external-evidence prefix failed strict target/outcome replay"
        ) from exc
    return records, state, generated


def _audit_external_evidence(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    """Report a fail-closed external target/execution replay."""

    try:
        _replay_external_evidence(
            layout,
            records,
            use_cache=False,
            refresh_cache=True,
        )
    except (OSError, LedgerError, ValueError) as exc:
        return [
            {
                "code": "invalid_external_evidence",
                "path": str(layout.root),
                "detail": str(exc),
            }
        ]
    return []


def _audit_attestation_bundles(
    layout: LedgerLayout, records: Sequence[Mapping[str, Any]]
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    for metadata in records:
        record = metadata["record"]
        if record["kind"] != "attestation_receipt":
            continue
        try:
            _verify_attestation_bundle(layout, record["payload"])
        except (OSError, LedgerError, ValueError) as exc:
            issues.append(
                {
                    "code": "invalid_attestation_bundle",
                    "path": str(_attestation_bundle_path(layout, record["payload"])),
                    "detail": str(exc),
                }
            )
    return issues


def audit_ledger(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        issues: list[dict[str, str]] = []
        records: list[dict[str, Any]] = []
        state = _LedgerState()
        try:
            records, state = _load_record_chain(layout)
        except (OSError, LedgerError, ValueError) as exc:
            issues.append(
                {
                    "code": "invalid_record_chain",
                    "path": str(layout.records),
                    "detail": str(exc),
                }
            )
        if not issues:
            issues.extend(_audit_snapshots(layout, records))
        if not issues:
            issues.extend(_audit_attestation_bundles(layout, records))
        if not issues:
            issues.extend(_audit_external_evidence(layout, records))
        return {
            "schema_version": 1,
            "ledger_id": ledger_id,
            "ledger_root": str(layout.root),
            "valid": not issues,
            "record_count": len(records),
            "head_sequence": records[-1]["sequence"] if records else None,
            "head_record_sha256": (
                records[-1]["record_sha256"] if records else None
            ),
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


def _evaluation_operation_payload(
    layout: LedgerLayout,
    state: _LedgerState,
    *,
    ledger_head_record_sha256: str,
    outcomes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if (
        state.active_implementation is None
        or state.active_implementation_hash is None
        or not state.evaluation_ready()
    ):
        raise LedgerStateError(
            "prospective evaluation requires an attested active release implementation"
        )
    implementation = state.active_implementation
    return {
        "outcomes": [dict(outcome) for outcome in outcomes],
        "evaluator_id": str(implementation["evaluator_id"]),
        "evaluation_contract_sha256": str(
            implementation["evaluation_contract_sha256"]
        ),
        "ledger_id": layout.ledger_id,
        "ledger_head_record_sha256": _require_sha256(
            ledger_head_record_sha256, "ledger_head_record_sha256"
        ),
        "implementation_upgrade_record_sha256": str(
            state.active_implementation_hash
        ),
    }


def _validate_release_evaluation_response(
    layout: LedgerLayout,
    state: _LedgerState,
    *,
    ledger_head_record_sha256: str,
    outcomes: Sequence[Mapping[str, Any]],
    response: Mapping[str, Any],
) -> dict[str, Any]:
    checked = _require_exact_keys(
        response, {"evaluation_envelope"}, "release evaluation response"
    )
    envelope = dict(
        _validate_evaluation_envelope(
            checked["evaluation_envelope"],
            state,
            ledger_id=layout.ledger_id,
            evaluated_head_record_sha256=ledger_head_record_sha256,
        )
    )
    if envelope["binding"]["outcomes_sha256"] != sha256_bytes(
        canonical_json_bytes([dict(outcome) for outcome in outcomes])
    ):
        raise LedgerStateError("release evaluation outcome digest differs")
    return envelope


def _evaluate_verified_ledger(
    layout: LedgerLayout,
    records: Sequence[Mapping[str, Any]],
    state: _LedgerState,
) -> dict[str, Any]:
    if not records:
        raise LedgerStateError("prospective ledger is not activated")
    outcomes = [
        dict(metadata["record"]["payload"])
        for metadata in records
        if metadata["record"]["kind"] == "outcome"
        and _is_v2_outcome(metadata["record"]["payload"])
    ]
    head = str(records[-1]["record_sha256"])
    payload = _evaluation_operation_payload(
        layout,
        state,
        ledger_head_record_sha256=head,
        outcomes=outcomes,
    )
    response = _run_active_release_operation(
        layout, state, "evaluate", payload
    )
    return _validate_release_evaluation_response(
        layout,
        state,
        ledger_head_record_sha256=head,
        outcomes=outcomes,
        response=response,
    )


def evaluate_ledger(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Evaluate strict replayed outcomes inside the active release capsule."""

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        records, state, _generated = _load_verified_record_chain(layout)
        if state.direction_rejected and state.latest_evaluation is not None:
            return dict(state.latest_evaluation)
        return _evaluate_verified_ledger(layout, records, state)


def checkpoint_evaluation(
    ledger_root: str | Path,
    *,
    recorded_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Evaluate and append the exact capsule-bound result as a state checkpoint."""

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _exclusive_lock(layout):
        records, state, _generated = _load_verified_record_chain(layout)
        if state.direction_rejected:
            raise LedgerStateError(
                "evaluation checkpoints require a non-terminal direction"
            )
        envelope = _evaluate_verified_ledger(layout, records, state)
        return _append_record_unlocked(
            layout,
            records,
            state,
            kind="evaluation_checkpoint",
            payload=envelope,
            recorded_at_utc=recorded_at_utc,
        )


def ledger_status(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    records: list[dict[str, Any]] = []
    state = _LedgerState()
    issues: list[dict[str, str]] = []
    with _exclusive_lock(layout):
        try:
            records, state, _generated = _load_verified_record_chain(layout)
        except (OSError, LedgerError, ValueError) as exc:
            issues.append(
                {
                    "code": "invalid_verified_ledger",
                    "path": str(layout.root),
                    "detail": str(exc),
                }
            )
        if not issues:
            issues.extend(_audit_snapshots(layout, records))
    valid = not issues
    if not valid:
        status = "invalid"
    elif not records:
        status = "not_activated"
    else:
        status = state.public_phase()
    return {
        "status": status,
        "valid": valid,
        "ledger_id": ledger_id,
        "ledger_root": str(layout.root),
        "record_count": len(records),
        "head_sequence": records[-1]["sequence"] if records else None,
        "head_record_sha256": (
            records[-1]["record_sha256"] if records else None
        ),
        **state.public(),
        "issues": issues,
    }


def _latest_decision_coordinates(
    records: Sequence[Mapping[str, Any]],
) -> tuple[str | None, int | None]:
    signal: str | None = None
    calendar_index: int | None = None
    for metadata in records:
        record = metadata["record"]
        if record["kind"] != "decision":
            continue
        payload = record["payload"]
        plan = payload.get("plan") if isinstance(payload, Mapping) else None
        route = (
            plan.get("route_target_plan")
            if isinstance(plan, Mapping)
            else None
        )
        if not isinstance(route, Mapping):
            raise LedgerIntegrityError(
                "readiness requires schema-2 decision coordinates"
            )
        signal = str(route["signal_date"])
        calendar_index = int(route["calendar_index"])
    return signal, calendar_index


def _readiness_issue(
    code: str,
    severity: str,
    component: str,
    message: str,
    *,
    retryable: bool,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "component": component,
        "retryable": retryable,
        "message": message,
        "details": {},
    }


def _override_readiness(
    report: dict[str, Any],
    *,
    status: str,
    reason: str,
    issue: Mapping[str, Any],
) -> dict[str, Any]:
    issues = report.get("issues")
    if not isinstance(issues, list):
        issues = []
    if not any(
        isinstance(item, Mapping) and item.get("code") == issue["code"]
        for item in issues
    ):
        issues.append(dict(issue))
    report["issues"] = sorted(
        issues,
        key=lambda row: (
            {"fatal": 0, "error": 1, "wait": 2}.get(
                str(row.get("severity")), 9
            ),
            str(row.get("code")),
        ),
    )
    report.update(
        status=status,
        reason=reason,
        ready=False,
        next_action="wait" if status == "waiting" else "none",
    )
    report["ready_for"] = {
        "membership_build": False,
        "input_build": False,
        "decision_admission": False,
    }
    return report


def prospective_readiness(
    ledger_root: str | Path,
    *,
    project_root: str | Path | None = None,
    observed_at_utc: datetime | str | None = None,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Strictly bind the zero-write data observer to authoritative ledger state."""

    from .data.prospective_readiness import inspect_prospective_readiness

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    project = (
        _implementation_project_root(layout)
        if project_root is None
        else Path(project_root).expanduser().resolve()
    )
    report: dict[str, Any] | None = None
    records: list[dict[str, Any]] = []
    state = _LedgerState()
    strict_error: Exception | None = None
    try:
        with _existing_read_lock(layout):
            try:
                records, state, _generated = _load_verified_record_chain(
                    layout,
                    refresh_cache=False,
                )
                snapshot_issues = _audit_snapshots(layout, records)
                if snapshot_issues:
                    raise LedgerIntegrityError(
                        f"prospective readiness snapshot audit failed: {snapshot_issues}"
                    )
            except (OSError, LedgerError, ValueError) as exc:
                strict_error = exc
            report = inspect_prospective_readiness(
                project,
                observed_at_utc=observed_at_utc,
                ledger_root=layout.root,
                ledger_id=ledger_id,
            )
    except (OSError, LedgerError, ValueError) as exc:
        strict_error = exc
        report = inspect_prospective_readiness(
            project,
            observed_at_utc=observed_at_utc,
            ledger_root=layout.root,
            ledger_id=ledger_id,
        )
    if not isinstance(report, dict):
        raise LedgerIntegrityError("prospective readiness report is not an object")
    if strict_error is not None:
        return _override_readiness(
            report,
            status="blocked",
            reason="authoritative_ledger_invalid",
            issue=_readiness_issue(
                "AUTHORITATIVE_LEDGER_INVALID",
                "error",
                "ledger",
                str(strict_error),
                retryable=False,
            ),
        )

    last_signal, last_index = _latest_decision_coordinates(records)
    try:
        ledger_relative = layout.root.relative_to(project).as_posix()
    except ValueError as exc:
        return _override_readiness(
            report,
            status="blocked",
            reason="authoritative_ledger_binding_mismatch",
            issue=_readiness_issue(
                "AUTHORITATIVE_LEDGER_BINDING_MISMATCH",
                "error",
                "ledger",
                str(exc),
                retryable=False,
            ),
        )
    snapshot_sha = sha256_bytes(
        canonical_json_bytes(_snapshot_payload(layout, records, state))
    )
    expected_ledger = {
        "root": ledger_relative,
        "ledger_id": ledger_id,
        "head_sequence": records[-1]["sequence"] if records else None,
        "head_record_sha256": (
            records[-1]["record_sha256"] if records else None
        ),
        "snapshot_sha256": snapshot_sha if records else None,
        "phase": state.public_phase(),
        "decision_generation_ready": state.decision_generation_ready(),
        "decision_count": state.decision_count,
        "open_decision_count": len(state.open_cycles),
        "implementation_trusted_tlog_timestamp_utc": (
            state.active_implementation_tlog_utc
        ),
        "prospective_epoch_tlog_timestamp_utc": (
            state.prospective_epoch_tlog_utc
            or state.active_implementation_tlog_utc
        ),
        "last_decision_signal_date": last_signal,
        "last_decision_calendar_index": last_index,
        "observer_validation_scope": (
            "canonical_record_chain_and_snapshot_binding"
        ),
    }
    observed_ledger = report.get("ledger")
    if not isinstance(observed_ledger, Mapping) or dict(observed_ledger) != expected_ledger:
        return _override_readiness(
            report,
            status="blocked",
            reason="authoritative_ledger_binding_mismatch",
            issue=_readiness_issue(
                "AUTHORITATIVE_LEDGER_BINDING_MISMATCH",
                "error",
                "ledger",
                "data observer ledger binding differs from authoritative replay",
                retryable=True,
            ),
        )

    def final_stable_gate() -> dict[str, Any] | None:
        """Re-observe ledger and data under the ledger lock before any action."""

        try:
            with _existing_read_lock(layout):
                latest_records, latest_state, _latest_generated = (
                    _load_verified_record_chain(
                        layout,
                        refresh_cache=False,
                    )
                )
                latest_snapshot_issues = _audit_snapshots(layout, latest_records)
                if latest_snapshot_issues:
                    raise LedgerIntegrityError(
                        "prospective readiness final snapshot audit failed: "
                        f"{latest_snapshot_issues}"
                    )
                recheck_observed_at = (
                    observed_at_utc
                    if observed_at_utc is not None
                    else report.get("observed_at_utc")
                )
                latest_report = inspect_prospective_readiness(
                    project,
                    observed_at_utc=recheck_observed_at,
                    ledger_root=layout.root,
                    ledger_id=ledger_id,
                )
                if observed_at_utc is None and isinstance(
                    report.get("clock_source"), str
                ):
                    # Freeze the first observer's instant while preserving the
                    # truthful source label in the returned report.  Reusing
                    # the instant as a caller-supplied value must not itself
                    # look like evidence drift.
                    latest_report["clock_source"] = report["clock_source"]
                latest_head = (
                    latest_records[-1]["record_sha256"]
                    if latest_records
                    else None
                )
                latest_snapshot_sha = (
                    sha256_bytes(
                        canonical_json_bytes(
                            _snapshot_payload(
                                layout,
                                latest_records,
                                latest_state,
                            )
                        )
                    )
                    if latest_records
                    else None
                )
        except (OSError, LedgerError, ValueError) as exc:
            return _override_readiness(
                report,
                status="blocked",
                reason="authoritative_ledger_recheck_failed",
                issue=_readiness_issue(
                    "AUTHORITATIVE_LEDGER_RECHECK_FAILED",
                    "error",
                    "ledger",
                    str(exc),
                    retryable=True,
                ),
            )
        if (
            latest_head != expected_ledger["head_record_sha256"]
            or latest_snapshot_sha != expected_ledger["snapshot_sha256"]
        ):
            report["stable_view"] = False
            return _override_readiness(
                report,
                status="waiting",
                reason="ledger_changed_during_readiness",
                issue=_readiness_issue(
                    "LEDGER_CHANGED_DURING_READINESS",
                    "wait",
                    "ledger",
                    "ledger advanced before the readiness action gate opened",
                    retryable=True,
                ),
            )
        if latest_report != report:
            report["stable_view"] = False
            return _override_readiness(
                report,
                status="waiting",
                reason="evidence_changed_during_readiness",
                issue=_readiness_issue(
                    "EVIDENCE_CHANGED_DURING_READINESS",
                    "wait",
                    "filesystem",
                    "prospective evidence changed before the action gate opened",
                    retryable=True,
                ),
            )
        return None

    if state.direction_rejected or state.insolvent:
        return _override_readiness(
            report,
            status="terminal",
            reason=(
                "direction_rejected"
                if state.direction_rejected
                else "terminal_insolvency"
            ),
            issue=_readiness_issue(
                "LEDGER_TERMINAL",
                "fatal",
                "ledger",
                "prospective ledger is in a terminal state",
                retryable=False,
            ),
        )
    # Once the derived candidate's immutable pre-trade deadline has passed,
    # missing receipts or evaluations cannot turn that missed admission back
    # into a retryable controller phase.  Preserve the observer's terminal
    # finding before applying ordinary ledger readiness/capacity guards.
    if report.get("status") == "terminal":
        return report
    candidate = report.get("candidate")
    if (
        state.decision_count == 0
        and state.active_implementation_tlog_utc is not None
        and isinstance(candidate, Mapping)
    ):
        signal_close_text = candidate.get("signal_close_utc")
        if isinstance(signal_close_text, str):
            active_tlog = _parse_trusted_utc(
                state.active_implementation_tlog_utc
            )
            signal_close = _parse_evidence_utc(signal_close_text)
            if active_tlog >= signal_close:
                return _override_readiness(
                    report,
                    status="terminal",
                    reason="implementation_canary_missed_first_signal",
                    issue=_readiness_issue(
                        "IMPLEMENTATION_CANARY_MISSED_FIRST_SIGNAL",
                        "fatal",
                        "ledger",
                        "active implementation canary is not earlier than the "
                        "immutable first signal close",
                        retryable=False,
                    ),
                )
    if not state.decision_generation_ready():
        return _override_readiness(
            report,
            status="blocked",
            reason="ledger_not_ready",
            issue=_readiness_issue(
                "AUTHORITATIVE_LEDGER_NOT_READY",
                "error",
                "ledger",
                f"authoritative ledger phase is {state.phase}",
                retryable=state.phase in {
                    "awaiting_receipt",
                    "awaiting_implementation_attestation",
                    "awaiting_evaluation",
                },
            ),
        )

    candidate = report.get("candidate")
    due_offset = (
        candidate.get("due_offset")
        if isinstance(candidate, Mapping)
        else None
    )
    if type(due_offset) is int:
        same_offset_open = sum(
            not bool(cycle["legacy_single_slot"])
            and int(cycle["due_offset"]) == due_offset
            for cycle in state.open_cycles.values()
        )
        if same_offset_open >= 2:
            return _override_readiness(
                report,
                status="waiting",
                reason="same_offset_capacity",
                issue=_readiness_issue(
                    "SAME_OFFSET_CAPACITY_FULL",
                    "wait",
                    "ledger",
                    "two same-offset cycles remain open",
                    retryable=True,
                ),
            )
    if report.get("reason") != "authoritative_target_replay_required":
        if report.get("status") == "ready":
            changed = final_stable_gate()
            if changed is not None:
                return changed
        return report

    candidate = report.get("candidate")
    input_view = report.get("input_snapshot")
    if not isinstance(candidate, Mapping) or not isinstance(input_view, Mapping):
        return _override_readiness(
            report,
            status="blocked",
            reason="authoritative_target_replay_failed",
            issue=_readiness_issue(
                "AUTHORITATIVE_TARGET_REPLAY_FAILED",
                "error",
                "target_replay",
                "readiness report lacks a candidate input binding",
                retryable=False,
            ),
        )
    try:
        source_snapshot_sha = _require_sha256(
            input_view.get("snapshot_sha256"),
            "readiness source data snapshot sha256",
        )
        admission_deadline = str(candidate["admission_deadline_utc"])
        generation, deployment, route_input = _regenerate_route_target_plan(
            layout,
            state,
            source_data_snapshot_sha256=source_snapshot_sha,
            admission_deadline_utc=admission_deadline,
        )
        checked_generation = _validate_route_target_plan(
            generation,
            state,
            decision_session=str(candidate["entry_date"]),
        )
        input_metadata = _input_snapshot_plan_metadata(
            route_input,
            checked_generation,
            admission_deadline_utc=admission_deadline,
        )
        expected_skipped = (
            list(candidate["initial_skipped_sessions"])
            if state.decision_count == 0
            else []
        )
        expected_coordinates = {
            "signal_date": candidate["signal_date"],
            "trade_date": candidate["entry_date"],
            "calendar_index": candidate["calendar_index"],
            "due_offset": candidate["due_offset"],
            "skipped_sessions": expected_skipped,
        }
        if any(
            checked_generation.get(name) != expected
            for name, expected in expected_coordinates.items()
        ):
            raise LedgerIntegrityError(
                "published target replay coordinates differ from readiness candidate"
            )
        if input_metadata["source_data_snapshot_sha256"] != source_snapshot_sha:
            raise LedgerIntegrityError(
                "published target replay binds another source data snapshot"
            )
        _payload_sha256_without(
            deployment,
            "deployment_sha256",
            context="readiness target deployment",
        )
        expected_deployment = {
            "deployment_sha256": checked_generation["deployment_sha256"],
            "activation_record_sha256": state.activation_hash,
            "implementation_upgrade_record_sha256": (
                state.active_implementation_hash
            ),
            "deployment_protocol_sha256": state.activation["protocol_sha256"],
            "route": state.activation["frozen_route"],
            "generator_id": state.active_implementation["generator_id"],
        }
        if any(
            deployment.get(name) != expected
            for name, expected in expected_deployment.items()
        ):
            raise LedgerIntegrityError(
                "published target replay deployment differs from active ledger state"
            )
    except Exception as exc:  # Readiness must convert any replay failure to fail-closed JSON.
        target_replay = report.get("target_replay")
        if isinstance(target_replay, dict):
            target_replay["status"] = "failed"
        return _override_readiness(
            report,
            status="blocked",
            reason="authoritative_target_replay_failed",
            issue=_readiness_issue(
                "AUTHORITATIVE_TARGET_REPLAY_FAILED",
                "error",
                "target_replay",
                f"published target replay failed: {type(exc).__name__}",
                retryable=False,
            ),
        )

    changed = final_stable_gate()
    if changed is not None:
        return changed

    target_replay = report.get("target_replay")
    if not isinstance(target_replay, dict):
        target_replay = {}
        report["target_replay"] = target_replay
    target_replay.update(
        status="complete",
        result_sha256=checked_generation["result_sha256"],
        deployment_sha256=checked_generation["deployment_sha256"],
        generator_id=checked_generation["generator_id"],
    )
    report["ready_for"] = {
        "membership_build": False,
        "input_build": False,
        "decision_admission": True,
    }
    report.update(
        status="ready",
        reason="decision_admission_ready",
        ready=True,
        next_action="build_plan",
    )
    return report


def implementation_transition_status(
    ledger_root: str | Path,
    *,
    ledger_id: str = DEFAULT_LEDGER_ID,
) -> dict[str, Any]:
    """Verify a decision-free ledger while moving between pinned runtimes.

    The new implementation environment cannot also equal the superseded
    environment's exact distribution set.  This narrow observer therefore
    verifies all historical capsule identities without executing them, while
    still checking the canonical chain, attestations, and deterministic
    snapshots.  Mutation APIs independently revalidate the same state under
    their exclusive lock and require the destination runtime before append.
    """

    layout = LedgerLayout.at(ledger_root, ledger_id=ledger_id)
    with _existing_read_lock(layout):
        records, state, _generated = _load_verified_record_chain(
            layout,
            refresh_cache=False,
            require_active_runtime=False,
        )
        issues = _audit_snapshots(layout, records)
        if issues:
            raise LedgerIntegrityError(
                f"prospective transition snapshot audit failed: {issues}"
            )
    return {
        "valid": True,
        "ledger_id": ledger_id,
        "ledger_root": str(layout.root),
        "record_count": len(records),
        "head_sequence": records[-1]["sequence"] if records else None,
        "head_record_sha256": (
            records[-1]["record_sha256"] if records else None
        ),
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
    "abandon_implementation_upgrade",
    "activate_protocol",
    "append_attestation_receipt",
    "append_correction",
    "append_implementation_upgrade",
    "append_outcome",
    "audit_ledger",
    "build_decision_plan",
    "build_membership_evidence",
    "build_execution_evidence",
    "build_outcome_payload",
    "build_signal_input_evidence",
    "canonical_json_bytes",
    "checkpoint_evaluation",
    "create_only_file",
    "ledger_status",
    "evaluate_ledger",
    "implementation_transition_status",
    "prospective_readiness",
    "route_target_plan_payload_sha256",
    "seal_decision",
    "seal_snapshot",
    "sha256_bytes",
    "sha256_file",
    "store_decision_plan",
    "strict_load_canonical",
]
