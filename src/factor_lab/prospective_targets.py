"""Deterministic prospective targets for the frozen ``fixed_core_full`` route.

This module is intentionally a pure computation boundary.  It does not read
files, consult a clock, or contact a service.  Callers must provide the exact
signal-date cross-section, the calendar prefix plus its observed extension,
and the previously sealed ten-sleeve state.

Calendar prefixes have one canonical definition throughout this module::

    {
        "schema_version": 1,
        "anchor": "YYYY-MM-DD",
        "count": N,
        "sessions": ["YYYY-MM-DD", ...],
    }

The SHA-256 is over compact, key-sorted UTF-8 JSON for that object.  It is not
the historical pandas/frame identity hash and it is not a hash of the bare
date array.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any
import unicodedata

import numpy as np
import pandas as pd

from .research.signals import directed_rank_blend, evaluate_expression


SCHEMA_VERSION = 1
GENERATOR_ID = "factor-lab/fixed-core-full-targets/5.2"
FROZEN_ROUTE = "fixed_core_full"
OFFSET_COUNT = 10
POSITION_COUNT = 10
RETENTION_BUFFER = 5
WEIGHT_SCALE_PPM = 1_000_000
SLEEVE_CAPITAL_WEIGHT_PPM = 100_000
POSITION_WEIGHT_PPM = 100_000
SLEEVE_CAPITAL_FEN = 500_000_000
CHALLENGER_WEIGHT_PPM = 700_000

SIGNAL_COLUMNS = (
    "date",
    "ticker",
    "eligible",
    "universe_member",
    "earnings_yield",
    "pb",
    "book_yield",
    "volatility_20",
)
_SIGNAL_COLUMN_SET = frozenset(SIGNAL_COLUMNS)
_NUMERIC_SIGNAL_COLUMNS = (
    "earnings_yield",
    "pb",
    "book_yield",
    "volatility_20",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TICKER_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$")


class TargetGenerationError(ValueError):
    """Raised when supplied evidence cannot deterministically produce targets."""


def _canonical_json_bytes(value: Any) -> bytes:
    """Encode the integer/string target envelope using the ledger JSON shape."""

    def normalize(item: Any, path: str) -> Any:
        if item is None or isinstance(item, bool) or type(item) is int:
            return item
        if isinstance(item, str):
            normalized = unicodedata.normalize("NFC", item)
            if any(0xD800 <= ord(character) <= 0xDFFF for character in normalized):
                raise TargetGenerationError(f"Unicode surrogate is forbidden at {path}")
            return normalized
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for raw_key, raw_value in item.items():
                if not isinstance(raw_key, str):
                    raise TargetGenerationError(f"non-string JSON key at {path}")
                key = normalize(raw_key, f"{path}.<key>")
                if key in result:
                    raise TargetGenerationError(
                        f"duplicate key after Unicode normalization at {path}: {key!r}"
                    )
                result[key] = normalize(raw_value, f"{path}.{key}")
            return result
        if isinstance(item, (list, tuple)):
            return [normalize(child, f"{path}[{index}]") for index, child in enumerate(item)]
        if isinstance(item, (float, np.floating)):
            raise TargetGenerationError(f"floating-point JSON value is forbidden at {path}")
        raise TargetGenerationError(f"unsupported JSON value {type(item).__name__} at {path}")

    return json.dumps(
        normalize(value, "$"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_payload(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _require_sha256(value: str, name: str) -> str:
    result = str(value)
    if not _SHA256_RE.fullmatch(result):
        raise TargetGenerationError(f"{name} must be a lowercase SHA-256")
    return result


def _date_string(value: Any, name: str) -> str:
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise TargetGenerationError(f"{name} must be a valid date") from exc
    if pd.isna(parsed) or parsed.tzinfo is not None or parsed != parsed.normalize():
        raise TargetGenerationError(f"{name} must be a timezone-free calendar date")
    return parsed.date().isoformat()


def _utc_string(value: str, name: str) -> str:
    raw = str(value)
    if not raw.endswith("Z"):
        raise TargetGenerationError(f"{name} must be an ISO-8601 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise TargetGenerationError(f"{name} must be a valid ISO-8601 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise TargetGenerationError(f"{name} must be UTC")
    return raw


def _normalise_sessions(values: Sequence[Any], name: str) -> tuple[str, ...]:
    sessions = tuple(_date_string(value, f"{name}[{index}]") for index, value in enumerate(values))
    if not sessions:
        raise TargetGenerationError(f"{name} must not be empty")
    if any(left >= right for left, right in zip(sessions, sessions[1:])):
        raise TargetGenerationError(f"{name} must be unique and strictly increasing")
    return sessions


def calendar_prefix_payload(sessions: Sequence[Any]) -> dict[str, Any]:
    """Return the sole canonical calendar-prefix payload used by 5.2."""

    normalized = _normalise_sessions(sessions, "calendar prefix sessions")
    return {
        "schema_version": SCHEMA_VERSION,
        "anchor": normalized[0],
        "count": len(normalized),
        "sessions": list(normalized),
    }


def calendar_prefix_sha256(sessions: Sequence[Any]) -> str:
    """Hash :func:`calendar_prefix_payload` as compact canonical JSON."""

    return _sha256_payload(calendar_prefix_payload(sessions))


def _number_from_input(value: Any, name: str) -> float:
    if value is None or value is pd.NA:
        return math.nan
    if isinstance(value, str) and (value.startswith("0x") or value.startswith("-0x")):
        try:
            return float.fromhex(value)
        except ValueError as exc:
            raise TargetGenerationError(f"{name} has an invalid binary64 token") from exc
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise TargetGenerationError(f"{name} must be numeric or null") from exc


def _number_token(value: Any) -> str | None:
    numeric = _number_from_input(value, "signal number")
    if not math.isfinite(numeric):
        return None
    return numeric.hex()


def _eligible_value(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if value is None or value is pd.NA:
        return False
    return str(value).strip().casefold() in {"1", "true", "yes", "y"}


def _records_from_rows(rows: Any) -> list[dict[str, Any]]:
    if isinstance(rows, pd.DataFrame):
        if set(rows.columns) != _SIGNAL_COLUMN_SET or len(rows.columns) != len(SIGNAL_COLUMNS):
            unknown = sorted(set(rows.columns) - _SIGNAL_COLUMN_SET)
            missing = sorted(_SIGNAL_COLUMN_SET - set(rows.columns))
            detail = []
            if missing:
                detail.append(f"missing={missing}")
            if unknown:
                detail.append(f"forbidden_or_unknown={unknown}")
            raise TargetGenerationError("signal snapshot must contain exactly the allowlist; " + ", ".join(detail))
        source = rows.to_dict(orient="records")
    else:
        if isinstance(rows, (str, bytes, Mapping)):
            raise TargetGenerationError("signal rows must be a sequence of row mappings")
        source = list(rows)
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(source):
        if not isinstance(raw, Mapping):
            raise TargetGenerationError(f"signal row {index} is not a mapping")
        keys = set(raw)
        if keys != _SIGNAL_COLUMN_SET or len(raw) != len(SIGNAL_COLUMNS):
            unknown = sorted(keys - _SIGNAL_COLUMN_SET)
            missing = sorted(_SIGNAL_COLUMN_SET - keys)
            detail = []
            if missing:
                detail.append(f"missing={missing}")
            if unknown:
                detail.append(f"forbidden_or_unknown={unknown}")
            raise TargetGenerationError(
                f"signal row {index} must contain exactly the allowlist; " + ", ".join(detail)
            )
        ticker = unicodedata.normalize("NFC", str(raw["ticker"]).strip())
        if not _TICKER_RE.fullmatch(ticker):
            raise TargetGenerationError(f"signal row {index} has an invalid ticker")
        row: dict[str, Any] = {
            "date": _date_string(raw["date"], f"signal row {index}.date"),
            "ticker": ticker,
            "eligible": _eligible_value(raw["eligible"]),
            "universe_member": _eligible_value(raw["universe_member"]),
        }
        for column in _NUMERIC_SIGNAL_COLUMNS:
            row[column] = _number_from_input(raw[column], f"signal row {index}.{column}")
        normalized.append(row)
    normalized.sort(key=lambda row: (row["date"], row["ticker"]))
    if any(
        (left["date"], left["ticker"]) == (right["date"], right["ticker"])
        for left, right in zip(normalized, normalized[1:])
    ):
        raise TargetGenerationError("signal snapshot contains duplicate date/ticker rows")
    if not normalized:
        raise TargetGenerationError("signal snapshot must contain at least one row")
    return normalized


def _row_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "date": row["date"],
        "ticker": row["ticker"],
        "eligible": bool(row["eligible"]),
        "universe_member": bool(row["universe_member"]),
        **{column: _number_token(row[column]) for column in _NUMERIC_SIGNAL_COLUMNS},
    }


@dataclass(frozen=True)
class DeploymentSpec:
    """Frozen bindings and constants required by the target generator."""

    calendar_anchor: str
    calendar_prefix_count: int
    calendar_prefix_last_session: str
    calendar_prefix_sha256: str
    activation_record_sha256: str
    implementation_upgrade_record_sha256: str
    deployment_protocol_sha256: str
    route: str = FROZEN_ROUTE
    generator_id: str = GENERATOR_ID
    schema_version: int = SCHEMA_VERSION
    offset_count: int = OFFSET_COUNT
    position_count: int = POSITION_COUNT
    retention_buffer: int = RETENTION_BUFFER
    target_weight_ppm: int = POSITION_WEIGHT_PPM
    sleeve_capital_weight_ppm: int = SLEEVE_CAPITAL_WEIGHT_PPM
    sleeve_capital_fen: int = SLEEVE_CAPITAL_FEN
    challenger_weight_ppm: int = CHALLENGER_WEIGHT_PPM

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int:
            raise TargetGenerationError("deployment schema_version must be an integer")
        object.__setattr__(self, "calendar_anchor", _date_string(self.calendar_anchor, "calendar_anchor"))
        object.__setattr__(
            self,
            "calendar_prefix_last_session",
            _date_string(self.calendar_prefix_last_session, "calendar_prefix_last_session"),
        )
        for name in (
            "calendar_prefix_sha256",
            "activation_record_sha256",
            "implementation_upgrade_record_sha256",
            "deployment_protocol_sha256",
        ):
            object.__setattr__(self, name, _require_sha256(getattr(self, name), name))
        expected = {
            "schema_version": SCHEMA_VERSION,
            "route": FROZEN_ROUTE,
            "generator_id": GENERATOR_ID,
            "offset_count": OFFSET_COUNT,
            "position_count": POSITION_COUNT,
            "retention_buffer": RETENTION_BUFFER,
            "target_weight_ppm": POSITION_WEIGHT_PPM,
            "sleeve_capital_weight_ppm": SLEEVE_CAPITAL_WEIGHT_PPM,
            "sleeve_capital_fen": SLEEVE_CAPITAL_FEN,
            "challenger_weight_ppm": CHALLENGER_WEIGHT_PPM,
        }
        for name, value in expected.items():
            if getattr(self, name) != value:
                raise TargetGenerationError(f"{name} is frozen at {value!r}")
        if type(self.calendar_prefix_count) is not int or self.calendar_prefix_count <= 0:
            raise TargetGenerationError("calendar_prefix_count must be a positive integer")
        if self.calendar_anchor > self.calendar_prefix_last_session:
            raise TargetGenerationError("calendar prefix last session precedes its anchor")

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "route": self.route,
            "generator_id": self.generator_id,
            "calendar": {
                "anchor": self.calendar_anchor,
                "prefix_count": self.calendar_prefix_count,
                "prefix_last_session": self.calendar_prefix_last_session,
                "prefix_sha256": self.calendar_prefix_sha256,
            },
            "activation_record_sha256": self.activation_record_sha256,
            "implementation_upgrade_record_sha256": self.implementation_upgrade_record_sha256,
            "deployment_protocol_sha256": self.deployment_protocol_sha256,
            "offset_count": self.offset_count,
            "position_count": self.position_count,
            "retention_buffer": self.retention_buffer,
            "target_weight_ppm": self.target_weight_ppm,
            "sleeve_capital_weight_ppm": self.sleeve_capital_weight_ppm,
            "sleeve_capital_fen": self.sleeve_capital_fen,
            "challenger_weight_ppm": self.challenger_weight_ppm,
        }

    @property
    def deployment_sha256(self) -> str:
        return _sha256_payload(self.payload())

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "deployment_sha256": self.deployment_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "DeploymentSpec":
        required = {
            "schema_version",
            "route",
            "generator_id",
            "calendar",
            "activation_record_sha256",
            "implementation_upgrade_record_sha256",
            "deployment_protocol_sha256",
            "offset_count",
            "position_count",
            "retention_buffer",
            "target_weight_ppm",
            "sleeve_capital_weight_ppm",
            "sleeve_capital_fen",
            "challenger_weight_ppm",
            "deployment_sha256",
        }
        if set(value) != required or len(value) != len(required):
            raise TargetGenerationError("deployment mapping does not have the exact schema")
        calendar = value["calendar"]
        if not isinstance(calendar, Mapping) or set(calendar) != {
            "anchor",
            "prefix_count",
            "prefix_last_session",
            "prefix_sha256",
        }:
            raise TargetGenerationError("deployment calendar mapping does not have the exact schema")
        result = cls(
            calendar_anchor=calendar["anchor"],
            calendar_prefix_count=value["calendar"]["prefix_count"],
            calendar_prefix_last_session=calendar["prefix_last_session"],
            calendar_prefix_sha256=calendar["prefix_sha256"],
            activation_record_sha256=value["activation_record_sha256"],
            implementation_upgrade_record_sha256=value[
                "implementation_upgrade_record_sha256"
            ],
            deployment_protocol_sha256=value["deployment_protocol_sha256"],
            route=value["route"],
            generator_id=value["generator_id"],
            schema_version=value["schema_version"],
            offset_count=value["offset_count"],
            position_count=value["position_count"],
            retention_buffer=value["retention_buffer"],
            target_weight_ppm=value["target_weight_ppm"],
            sleeve_capital_weight_ppm=value["sleeve_capital_weight_ppm"],
            sleeve_capital_fen=value["sleeve_capital_fen"],
            challenger_weight_ppm=value["challenger_weight_ppm"],
        )
        if value["deployment_sha256"] != result.deployment_sha256:
            raise TargetGenerationError("deployment_sha256 does not match its canonical payload")
        return result


@dataclass(frozen=True)
class InputSnapshot:
    """One narrow signal-date slice and the calendar known at decision time."""

    signal_date: str
    calendar_sessions: Sequence[Any]
    rows: Any
    source_data_snapshot_sha256: str
    target_rows_sha256: str
    input_sources_sha256: str
    membership_artifact_sha256: str
    source_build_checkpoint_utc: str
    max_available_at_utc: str
    information_cutoff_utc: str
    signal_close_utc: str
    admission_deadline_utc: str
    skipped_sessions: Sequence[Any] = ()
    schema_version: int = SCHEMA_VERSION
    snapshot_sha256: str = ""

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise TargetGenerationError(f"snapshot schema_version is frozen at {SCHEMA_VERSION}")
        signal_date = _date_string(self.signal_date, "signal_date")
        sessions = _normalise_sessions(self.calendar_sessions, "calendar_sessions")
        skipped = tuple(
            _date_string(value, f"skipped_sessions[{index}]")
            for index, value in enumerate(self.skipped_sessions)
        )
        if len(set(skipped)) != len(skipped) or tuple(sorted(skipped)) != skipped:
            raise TargetGenerationError("skipped_sessions must be unique and strictly increasing")
        rows = _records_from_rows(self.rows)
        row_dates = {row["date"] for row in rows}
        if row_dates != {signal_date}:
            raise TargetGenerationError(
                "snapshot rows must be a narrow signal-date slice; past/future rows are forbidden"
            )
        for name in (
            "source_data_snapshot_sha256",
            "target_rows_sha256",
            "input_sources_sha256",
            "membership_artifact_sha256",
        ):
            object.__setattr__(self, name, _require_sha256(getattr(self, name), name))
        checkpoint = _utc_string(self.source_build_checkpoint_utc, "source_build_checkpoint_utc")
        maximum = _utc_string(self.max_available_at_utc, "max_available_at_utc")
        cutoff = _utc_string(self.information_cutoff_utc, "information_cutoff_utc")
        signal_close = _utc_string(self.signal_close_utc, "signal_close_utc")
        admission_deadline = _utc_string(
            self.admission_deadline_utc, "admission_deadline_utc"
        )
        maximum_dt = datetime.fromisoformat(maximum[:-1] + "+00:00")
        cutoff_dt = datetime.fromisoformat(cutoff[:-1] + "+00:00")
        checkpoint_dt = datetime.fromisoformat(checkpoint[:-1] + "+00:00")
        close_dt = datetime.fromisoformat(signal_close[:-1] + "+00:00")
        deadline_dt = datetime.fromisoformat(admission_deadline[:-1] + "+00:00")
        # ``information_cutoff`` is the decision-knowledge cutoff, not the
        # market close.  EOD inputs may legitimately become available after
        # close, but all raw availability and build evidence must precede the
        # admission deadline.
        if not close_dt <= maximum_dt <= checkpoint_dt <= cutoff_dt <= deadline_dt:
            raise TargetGenerationError(
                "timestamps must satisfy signal_close <= input_max_available <= "
                "build_checkpoint <= decision_information_cutoff <= admission_deadline"
            )

        object.__setattr__(self, "signal_date", signal_date)
        object.__setattr__(self, "calendar_sessions", sessions)
        object.__setattr__(self, "skipped_sessions", skipped)
        object.__setattr__(self, "rows", tuple(MappingProxyType(row) for row in rows))
        object.__setattr__(self, "source_build_checkpoint_utc", checkpoint)
        object.__setattr__(self, "max_available_at_utc", maximum)
        object.__setattr__(self, "information_cutoff_utc", cutoff)
        object.__setattr__(self, "signal_close_utc", signal_close)
        object.__setattr__(self, "admission_deadline_utc", admission_deadline)
        expected = _sha256_payload(self.payload())
        if self.snapshot_sha256 and self.snapshot_sha256 != expected:
            raise TargetGenerationError("snapshot_sha256 does not match the canonical snapshot payload")
        object.__setattr__(self, "snapshot_sha256", expected)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "signal_date": self.signal_date,
            "calendar_sessions": list(self.calendar_sessions),
            "skipped_sessions": list(self.skipped_sessions),
            "rows": [_row_payload(row) for row in self.rows],
            "source_data_snapshot_sha256": self.source_data_snapshot_sha256,
            "target_rows_sha256": self.target_rows_sha256,
            "input_sources_sha256": self.input_sources_sha256,
            "membership_artifact_sha256": self.membership_artifact_sha256,
            "source_build_checkpoint_utc": self.source_build_checkpoint_utc,
            "max_available_at_utc": self.max_available_at_utc,
            "information_cutoff_utc": self.information_cutoff_utc,
            "signal_close_utc": self.signal_close_utc,
            "admission_deadline_utc": self.admission_deadline_utc,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "snapshot_sha256": self.snapshot_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "InputSnapshot":
        required = {
            "schema_version",
            "signal_date",
            "calendar_sessions",
            "skipped_sessions",
            "rows",
            "source_data_snapshot_sha256",
            "target_rows_sha256",
            "input_sources_sha256",
            "membership_artifact_sha256",
            "source_build_checkpoint_utc",
            "max_available_at_utc",
            "information_cutoff_utc",
            "signal_close_utc",
            "admission_deadline_utc",
            "snapshot_sha256",
        }
        if set(value) != required or len(value) != len(required):
            raise TargetGenerationError("snapshot mapping does not have the exact schema")
        return cls(**dict(value))


def _normalise_targets(value: Mapping[str, Any]) -> MappingProxyType:
    targets: dict[str, int] = {}
    for raw_ticker, raw_weight in value.items():
        ticker = unicodedata.normalize("NFC", str(raw_ticker).strip())
        if not _TICKER_RE.fullmatch(ticker):
            raise TargetGenerationError(f"invalid target ticker: {ticker!r}")
        if type(raw_weight) is not int or raw_weight <= 0:
            raise TargetGenerationError(f"target weight for {ticker} must be a positive integer PPM")
        if ticker in targets:
            raise TargetGenerationError(f"duplicate target ticker: {ticker}")
        targets[ticker] = raw_weight
    return MappingProxyType(dict(sorted(targets.items())))


@dataclass(frozen=True)
class SleeveState:
    offset: int
    capital_fen: int = SLEEVE_CAPITAL_FEN
    initialized: bool = False
    last_signal_date: str | None = None
    last_calendar_index: int | None = None
    targets_ppm: Mapping[str, int] = field(default_factory=dict)
    cash_ppm: int = WEIGHT_SCALE_PPM

    def __post_init__(self) -> None:
        if type(self.offset) is not int or not 0 <= self.offset < OFFSET_COUNT:
            raise TargetGenerationError("sleeve offset must be an integer in [0, 10)")
        if self.capital_fen != SLEEVE_CAPITAL_FEN:
            raise TargetGenerationError(f"sleeve capital_fen is frozen at {SLEEVE_CAPITAL_FEN}")
        targets = _normalise_targets(self.targets_ppm)
        object.__setattr__(self, "targets_ppm", targets)
        if type(self.cash_ppm) is not int or not 0 <= self.cash_ppm <= WEIGHT_SCALE_PPM:
            raise TargetGenerationError("sleeve cash_ppm must be an integer in [0, 1000000]")
        if sum(targets.values()) + self.cash_ppm != WEIGHT_SCALE_PPM:
            raise TargetGenerationError("sleeve target weights plus cash must equal 1000000 PPM")
        if self.initialized:
            if self.last_signal_date is None or self.last_calendar_index is None:
                raise TargetGenerationError("initialized sleeve requires its last signal date/index")
            normalized_date = _date_string(self.last_signal_date, "sleeve last_signal_date")
            object.__setattr__(self, "last_signal_date", normalized_date)
            if self.last_calendar_index % OFFSET_COUNT != self.offset:
                raise TargetGenerationError("sleeve last index does not match its offset")
            if not targets or len(targets) > POSITION_COUNT:
                raise TargetGenerationError("initialized sleeve must have one to ten targets")
            if any(weight != POSITION_WEIGHT_PPM for weight in targets.values()):
                raise TargetGenerationError("fixed-core sleeve targets must each equal 100000 PPM")
        elif (
            self.last_signal_date is not None
            or self.last_calendar_index is not None
            or targets
            or self.cash_ppm != WEIGHT_SCALE_PPM
        ):
            raise TargetGenerationError("uninitialized sleeve must be all cash with no prior signal")

    def to_dict(self) -> dict[str, Any]:
        return {
            "offset": self.offset,
            "capital_fen": self.capital_fen,
            "initialized": self.initialized,
            "last_signal_date": self.last_signal_date,
            "last_calendar_index": self.last_calendar_index,
            "targets_ppm": dict(self.targets_ppm),
            "cash_ppm": self.cash_ppm,
        }


@dataclass(frozen=True)
class TenSleeveState:
    """Sealed model-target state for ten independent CNY 5m virtual sleeves."""

    deployment_sha256: str
    activation_record_sha256: str
    implementation_upgrade_record_sha256: str
    last_processed_calendar_index: int
    last_processed_session: str
    sleeves: Sequence[SleeveState | Mapping[str, Any]]
    schema_version: int = SCHEMA_VERSION
    state_sha256: str = ""

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise TargetGenerationError(f"state schema_version is frozen at {SCHEMA_VERSION}")
        for name in (
            "deployment_sha256",
            "activation_record_sha256",
            "implementation_upgrade_record_sha256",
        ):
            object.__setattr__(self, name, _require_sha256(getattr(self, name), name))
        if type(self.last_processed_calendar_index) is not int or self.last_processed_calendar_index < 0:
            raise TargetGenerationError("last_processed_calendar_index must be a non-negative integer")
        object.__setattr__(
            self,
            "last_processed_session",
            _date_string(self.last_processed_session, "last_processed_session"),
        )
        normalized: list[SleeveState] = []
        for item in self.sleeves:
            if isinstance(item, SleeveState):
                normalized.append(item)
            elif isinstance(item, Mapping):
                normalized.append(SleeveState(**dict(item)))
            else:
                raise TargetGenerationError("state sleeves must contain SleeveState or mappings")
        normalized.sort(key=lambda sleeve: sleeve.offset)
        if [sleeve.offset for sleeve in normalized] != list(range(OFFSET_COUNT)):
            raise TargetGenerationError("state must contain exactly one sleeve for every offset 0..9")
        if any(
            sleeve.last_calendar_index is not None
            and sleeve.last_calendar_index > self.last_processed_calendar_index
            for sleeve in normalized
        ):
            raise TargetGenerationError("sleeve state is ahead of the global processed index")
        object.__setattr__(self, "sleeves", tuple(normalized))
        expected = _sha256_payload(self.payload())
        if self.state_sha256 and self.state_sha256 != expected:
            raise TargetGenerationError("state_sha256 does not match the canonical state payload")
        object.__setattr__(self, "state_sha256", expected)

    @classmethod
    def genesis(cls, deployment: DeploymentSpec) -> "TenSleeveState":
        return cls(
            deployment_sha256=deployment.deployment_sha256,
            activation_record_sha256=deployment.activation_record_sha256,
            implementation_upgrade_record_sha256=deployment.implementation_upgrade_record_sha256,
            last_processed_calendar_index=deployment.calendar_prefix_count - 1,
            last_processed_session=deployment.calendar_prefix_last_session,
            sleeves=tuple(SleeveState(offset=offset) for offset in range(OFFSET_COUNT)),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "TenSleeveState":
        required = {
            "schema_version",
            "deployment_sha256",
            "activation_record_sha256",
            "implementation_upgrade_record_sha256",
            "last_processed_calendar_index",
            "last_processed_session",
            "sleeves",
            "state_sha256",
        }
        if set(value) != required or len(value) != len(required):
            raise TargetGenerationError("state mapping does not have the exact schema")
        return cls(**dict(value))

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "deployment_sha256": self.deployment_sha256,
            "activation_record_sha256": self.activation_record_sha256,
            "implementation_upgrade_record_sha256": self.implementation_upgrade_record_sha256,
            "last_processed_calendar_index": self.last_processed_calendar_index,
            "last_processed_session": self.last_processed_session,
            "sleeves": [sleeve.to_dict() for sleeve in self.sleeves],
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "state_sha256": self.state_sha256}


@dataclass(frozen=True)
class GenerationResult:
    """A deterministic route-to-target decision and its candidate next state."""

    deployment_sha256: str
    input_snapshot_sha256: str
    previous_state_sha256: str
    signal_date: str
    trade_date: str
    calendar_index: int
    due_offset: int
    skipped_sessions: Sequence[str]
    sleeve_plans: Sequence[Mapping[str, Any]]
    aggregate_targets_ppm: Mapping[str, int]
    aggregate_cash_ppm: int
    next_state: TenSleeveState
    schema_version: int = SCHEMA_VERSION
    route: str = FROZEN_ROUTE
    generator_id: str = GENERATOR_ID
    result_sha256: str = ""

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int:
            raise TargetGenerationError("result schema_version must be an integer")
        expected_constants = {
            "schema_version": SCHEMA_VERSION,
            "route": FROZEN_ROUTE,
            "generator_id": GENERATOR_ID,
        }
        for name, value in expected_constants.items():
            if getattr(self, name) != value:
                raise TargetGenerationError(f"result {name} is frozen at {value!r}")
        for name in ("deployment_sha256", "input_snapshot_sha256", "previous_state_sha256"):
            object.__setattr__(self, name, _require_sha256(getattr(self, name), name))
        object.__setattr__(self, "signal_date", _date_string(self.signal_date, "signal_date"))
        object.__setattr__(self, "trade_date", _date_string(self.trade_date, "trade_date"))
        if type(self.calendar_index) is not int or self.calendar_index < 0:
            raise TargetGenerationError("result calendar_index must be a non-negative integer")
        if type(self.due_offset) is not int or self.due_offset != self.calendar_index % OFFSET_COUNT:
            raise TargetGenerationError("result due_offset must equal calendar_index modulo ten")
        if self.next_state.deployment_sha256 != self.deployment_sha256:
            raise TargetGenerationError("result next state is bound to a different deployment")
        if self.next_state.last_processed_calendar_index != self.calendar_index:
            raise TargetGenerationError("result next state does not end at its calendar index")
        if self.next_state.last_processed_session != self.signal_date:
            raise TargetGenerationError("result next state does not end at its signal date")
        if self.trade_date <= self.signal_date:
            raise TargetGenerationError("result trade_date must be after signal_date")
        skipped = tuple(
            _date_string(value, f"result skipped_sessions[{index}]")
            for index, value in enumerate(self.skipped_sessions)
        )
        if tuple(sorted(set(skipped))) != skipped:
            raise TargetGenerationError("result skipped_sessions must be unique and increasing")
        object.__setattr__(self, "skipped_sessions", skipped)
        expected_plan_keys = {
            "action",
            "offset",
            "capital_fen",
            "initialized",
            "last_signal_date",
            "last_calendar_index",
            "targets_ppm",
            "cash_ppm",
        }
        raw_plans = list(self.sleeve_plans)
        if len(raw_plans) != OFFSET_COUNT:
            raise TargetGenerationError("result must contain exactly ten sleeve plans")
        normalized_plans: list[MappingProxyType] = []
        for offset, (raw_plan, sleeve) in enumerate(
            zip(raw_plans, self.next_state.sleeves, strict=True)
        ):
            if not isinstance(raw_plan, Mapping) or set(raw_plan) != expected_plan_keys:
                raise TargetGenerationError("result sleeve plan does not have the exact schema")
            plan = dict(raw_plan)
            action = plan.pop("action")
            if plan != sleeve.to_dict():
                raise TargetGenerationError("result sleeve plan does not match next state")
            if offset == self.due_offset:
                if action not in {"seed", "rebalance"}:
                    raise TargetGenerationError("due sleeve action must be seed or rebalance")
            elif action != ("carry" if sleeve.initialized else "cash"):
                raise TargetGenerationError("non-due sleeve action must be carry or cash")
            normalized_plans.append(
                MappingProxyType({"action": action, **sleeve.to_dict()})
            )
        plans = tuple(normalized_plans)
        object.__setattr__(self, "sleeve_plans", plans)
        aggregate = _normalise_targets(self.aggregate_targets_ppm)
        object.__setattr__(self, "aggregate_targets_ppm", aggregate)
        if type(self.aggregate_cash_ppm) is not int:
            raise TargetGenerationError("aggregate_cash_ppm must be an integer")
        if sum(aggregate.values()) + self.aggregate_cash_ppm != WEIGHT_SCALE_PPM:
            raise TargetGenerationError("aggregate target weights plus cash must equal 1000000 PPM")
        expected_aggregate, expected_cash = _aggregate_sleeves(self.next_state.sleeves)
        if dict(aggregate) != expected_aggregate or self.aggregate_cash_ppm != expected_cash:
            raise TargetGenerationError("aggregate targets do not match the ten sleeve states")
        expected = _sha256_payload(self.payload())
        if self.result_sha256 and self.result_sha256 != expected:
            raise TargetGenerationError("result_sha256 does not match canonical result payload")
        object.__setattr__(self, "result_sha256", expected)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "route": self.route,
            "generator_id": self.generator_id,
            "deployment_sha256": self.deployment_sha256,
            "input_snapshot_sha256": self.input_snapshot_sha256,
            "previous_state_sha256": self.previous_state_sha256,
            "signal_date": self.signal_date,
            "trade_date": self.trade_date,
            "calendar_index": self.calendar_index,
            "due_offset": self.due_offset,
            "skipped_sessions": list(self.skipped_sessions),
            "sleeve_plans": [dict(plan) for plan in self.sleeve_plans],
            "aggregate_targets_ppm": dict(self.aggregate_targets_ppm),
            "aggregate_cash_ppm": self.aggregate_cash_ppm,
            "next_state": self.next_state.to_dict(),
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "result_sha256": self.result_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "GenerationResult":
        required = {
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
        if set(value) != required or len(value) != len(required):
            raise TargetGenerationError("result mapping does not have the exact schema")
        fields = dict(value)
        next_state = fields.pop("next_state")
        if not isinstance(next_state, Mapping):
            raise TargetGenerationError("result next_state must be a mapping")
        return cls(next_state=TenSleeveState.from_mapping(next_state), **fields)


def _normalise_signal_frame(rows: Any, *, narrow_date: str | None = None) -> pd.DataFrame:
    records = _records_from_rows(rows)
    if narrow_date is not None and {row["date"] for row in records} != {narrow_date}:
        raise TargetGenerationError("signal frame is not the requested narrow date slice")
    frame = pd.DataFrame.from_records(records, columns=SIGNAL_COLUMNS)
    included = frame["eligible"] & frame["universe_member"]
    frame = frame.loc[included].sort_values(["date", "ticker"]).reset_index(drop=True)
    if frame.empty:
        raise TargetGenerationError("signal date has no eligible universe rows")
    return frame


def fixed_core_scores(rows: Any) -> pd.DataFrame:
    """Return fixed-core binary64 scores for one or more allowlisted dates.

    The two expressions and :func:`directed_rank_blend` are the same research
    primitives used by the 5.0 runner.  In particular, the primitive retains
    the original ``(1.0 - weight)`` binary64 operation; replacing it with a
    literal ``0.3`` changes real boundary selections.
    """

    frame = _normalise_signal_frame(rows)
    control = evaluate_expression(frame, "earnings_yield / pb", date_column="date")
    defensive = evaluate_expression(
        frame,
        "rank(book_yield) + rank(earnings_yield) + rank(-volatility_20)",
        date_column="date",
    )
    weight = CHALLENGER_WEIGHT_PPM / WEIGHT_SCALE_PPM
    score = directed_rank_blend(
        frame,
        control,
        defensive,
        control_direction=1,
        challenger_direction=1,
        challenger_weight=weight,
        date_column="date",
    )
    result = frame.loc[:, ["date", "ticker"]].copy()
    result["score"] = score
    return result[result["score"].notna()].reset_index(drop=True)


def rank_fixed_core_tickers(rows: Any, *, signal_date: Any | None = None) -> tuple[str, ...]:
    """Rank eligible tickers by frozen fixed-core score, then ticker ascending."""

    requested = _date_string(signal_date, "signal_date") if signal_date is not None else None
    scores = fixed_core_scores(rows)
    dates = tuple(scores["date"].drop_duplicates())
    if requested is None:
        if len(dates) != 1:
            raise TargetGenerationError("rank_fixed_core_tickers requires exactly one signal date")
        requested = dates[0]
    scores = scores[scores["date"] == requested]
    if scores.empty:
        raise TargetGenerationError("requested signal date has no usable fixed-core scores")
    ranked = scores.sort_values(["score", "ticker"], ascending=[False, True])
    return tuple(ranked["ticker"].astype(str))


def select_with_retention(
    ranked_tickers: Sequence[str],
    previous_targets: Sequence[str] = (),
) -> tuple[str, ...]:
    """Apply Top-15 retention followed by a rank fill to ten equal targets."""

    ranked = tuple(str(ticker) for ticker in ranked_tickers)
    if len(set(ranked)) != len(ranked):
        raise TargetGenerationError("ranked_tickers contains duplicates")
    previous = {str(ticker) for ticker in previous_targets}
    retained = previous & set(ranked[: POSITION_COUNT + RETENTION_BUFFER])
    selected = [ticker for ticker in ranked if ticker in retained]
    for ticker in ranked:
        if len(selected) >= POSITION_COUNT:
            break
        if ticker not in selected:
            selected.append(ticker)
    rank_order = {ticker: index for index, ticker in enumerate(ranked)}
    selected.sort(key=rank_order.__getitem__)
    return tuple(selected)


def _validate_bindings(deployment: DeploymentSpec, state: TenSleeveState) -> None:
    if state.deployment_sha256 != deployment.deployment_sha256:
        raise TargetGenerationError("previous state is bound to a different deployment")
    if state.activation_record_sha256 != deployment.activation_record_sha256:
        raise TargetGenerationError("previous state activation binding does not match deployment")
    if state.implementation_upgrade_record_sha256 != deployment.implementation_upgrade_record_sha256:
        raise TargetGenerationError("previous state upgrade binding does not match deployment")


def _validate_calendar(
    deployment: DeploymentSpec,
    snapshot: InputSnapshot,
    state: TenSleeveState,
) -> tuple[int, str]:
    sessions = snapshot.calendar_sessions
    if len(sessions) < deployment.calendar_prefix_count + 2:
        raise TargetGenerationError("calendar must contain the frozen prefix, signal date, and trade date")
    prefix = sessions[: deployment.calendar_prefix_count]
    if prefix[0] != deployment.calendar_anchor:
        raise TargetGenerationError("calendar anchor does not match deployment")
    if prefix[-1] != deployment.calendar_prefix_last_session:
        raise TargetGenerationError("calendar prefix last session does not match deployment")
    if calendar_prefix_sha256(prefix) != deployment.calendar_prefix_sha256:
        raise TargetGenerationError("calendar prefix SHA-256 does not match deployment")
    try:
        signal_index = sessions.index(snapshot.signal_date)
    except ValueError as exc:
        raise TargetGenerationError("signal_date is not in calendar_sessions") from exc
    if signal_index < deployment.calendar_prefix_count:
        raise TargetGenerationError("prospective signal_date must be strictly after the frozen prefix")
    if signal_index + 1 != len(sessions) - 1:
        raise TargetGenerationError("calendar extension must end at the next-session trade date")
    if state.last_processed_calendar_index >= signal_index:
        raise TargetGenerationError("signal session does not advance the sealed state")
    if sessions[state.last_processed_calendar_index] != state.last_processed_session:
        raise TargetGenerationError("previous state's calendar index/session continuity is broken")
    expected_skipped = sessions[state.last_processed_calendar_index + 1 : signal_index]
    if tuple(snapshot.skipped_sessions) != tuple(expected_skipped):
        raise TargetGenerationError("skipped_sessions must explicitly list every unprocessed calendar session")
    return signal_index, sessions[signal_index + 1]


def _aggregate_sleeves(sleeves: Sequence[SleeveState]) -> tuple[dict[str, int], int]:
    aggregate: defaultdict[str, int] = defaultdict(int)
    cash = 0
    for sleeve in sleeves:
        for ticker, target_ppm in sleeve.targets_ppm.items():
            product = SLEEVE_CAPITAL_WEIGHT_PPM * target_ppm
            if product % WEIGHT_SCALE_PPM:
                raise TargetGenerationError("sleeve target cannot be represented exactly in aggregate PPM")
            aggregate[ticker] += product // WEIGHT_SCALE_PPM
        cash_product = SLEEVE_CAPITAL_WEIGHT_PPM * sleeve.cash_ppm
        if cash_product % WEIGHT_SCALE_PPM:
            raise TargetGenerationError("sleeve cash cannot be represented exactly in aggregate PPM")
        cash += cash_product // WEIGHT_SCALE_PPM
    result = dict(sorted(aggregate.items()))
    if sum(result.values()) + cash != WEIGHT_SCALE_PPM:
        raise TargetGenerationError("ten-sleeve aggregate does not conserve PPM capital")
    return result, cash


def generate_fixed_core_targets(
    *,
    deployment: DeploymentSpec,
    input_snapshot: InputSnapshot,
    previous_state: TenSleeveState,
) -> GenerationResult:
    """Generate the due sleeve target and carry the other nine sleeves.

    A sleeve starts all cash.  Its first encountered offset performs a naked
    Top-10 seed; later visits use that sleeve's own Top-15 retention history.
    Skipped sessions do not fabricate trades or holdings: their sleeves simply
    retain their prior initialized/cash state until their next actual visit.
    """

    _validate_bindings(deployment, previous_state)
    signal_index, trade_date = _validate_calendar(deployment, input_snapshot, previous_state)
    due_offset = signal_index % OFFSET_COUNT
    ranked = rank_fixed_core_tickers(input_snapshot.rows, signal_date=input_snapshot.signal_date)
    prior_due = previous_state.sleeves[due_offset]
    selected = select_with_retention(ranked, prior_due.targets_ppm)
    targets = {ticker: POSITION_WEIGHT_PPM for ticker in selected}
    next_due = SleeveState(
        offset=due_offset,
        initialized=True,
        last_signal_date=input_snapshot.signal_date,
        last_calendar_index=signal_index,
        targets_ppm=targets,
        cash_ppm=WEIGHT_SCALE_PPM - sum(targets.values()),
    )
    sleeves = list(previous_state.sleeves)
    sleeves[due_offset] = next_due
    next_state = TenSleeveState(
        deployment_sha256=deployment.deployment_sha256,
        activation_record_sha256=deployment.activation_record_sha256,
        implementation_upgrade_record_sha256=deployment.implementation_upgrade_record_sha256,
        last_processed_calendar_index=signal_index,
        last_processed_session=input_snapshot.signal_date,
        sleeves=sleeves,
    )
    plans: list[dict[str, Any]] = []
    for old, new in zip(previous_state.sleeves, next_state.sleeves, strict=True):
        if new.offset == due_offset:
            action = "rebalance" if old.initialized else "seed"
        elif new.initialized:
            action = "carry"
        else:
            action = "cash"
        plans.append({"action": action, **new.to_dict()})
    aggregate_targets, aggregate_cash = _aggregate_sleeves(next_state.sleeves)
    return GenerationResult(
        deployment_sha256=deployment.deployment_sha256,
        input_snapshot_sha256=input_snapshot.snapshot_sha256,
        previous_state_sha256=previous_state.state_sha256,
        signal_date=input_snapshot.signal_date,
        trade_date=trade_date,
        calendar_index=signal_index,
        due_offset=due_offset,
        skipped_sessions=input_snapshot.skipped_sessions,
        sleeve_plans=plans,
        aggregate_targets_ppm=aggregate_targets,
        aggregate_cash_ppm=aggregate_cash,
        next_state=next_state,
    )


def replay_fixed_core_cohorts(
    rows: Any,
    calendar_sessions: Sequence[Any],
    *,
    required_future_sessions: int = 0,
) -> tuple[dict[str, Any], ...]:
    """Replay historical independent-offset target cohorts without any I/O.

    This helper exists solely for release parity.  Callers should load only the
    eight allowlisted columns and compare its records with the authoritative
    run's shadow target schedules.  It deliberately does not bootstrap the
    prospective state: every historical offset owns its independent retention
    history, matching the 5.0 long-only evaluator.
    """

    sessions = _normalise_sessions(calendar_sessions, "calendar_sessions")
    if type(required_future_sessions) is not int or required_future_sessions < 0:
        raise TargetGenerationError("required_future_sessions must be a non-negative integer")
    maximum_signal_index = len(sessions) - required_future_sessions - 1
    index_by_date = {session: index for index, session in enumerate(sessions)}
    scores = fixed_core_scores(rows)
    unknown_dates = sorted(set(scores["date"]) - set(index_by_date))
    if unknown_dates:
        raise TargetGenerationError(f"signal dates are absent from calendar: {unknown_dates[:3]}")
    previous: list[tuple[str, ...]] = [tuple() for _ in range(OFFSET_COUNT)]
    records: list[dict[str, Any]] = []
    for signal_date, group in scores.groupby("date", sort=True):
        calendar_index = index_by_date[str(signal_date)]
        if calendar_index > maximum_signal_index:
            continue
        ranked_frame = group.sort_values(["score", "ticker"], ascending=[False, True])
        ranked = tuple(ranked_frame["ticker"].astype(str))
        offset = calendar_index % OFFSET_COUNT
        selected = select_with_retention(ranked, previous[offset])
        previous[offset] = selected
        records.append(
            {
                "signal_date": str(signal_date),
                "calendar_index": calendar_index,
                "offset": offset,
                "targets_ppm": {ticker: POSITION_WEIGHT_PPM for ticker in selected},
            }
        )
    records.sort(key=lambda row: row["calendar_index"])
    return tuple(records)


def compare_cohort_parity(
    generated: Sequence[Mapping[str, Any]],
    expected: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return an integer/string parity envelope suitable for upgrade evidence."""

    def canonical(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for row in records:
            targets = row.get("targets_ppm") or {}
            result.append(
                {
                    "signal_date": _date_string(row["signal_date"], "cohort signal_date"),
                    "calendar_index": int(row["calendar_index"]),
                    "offset": int(row["offset"]),
                    "targets_ppm": dict(sorted((str(key), int(value)) for key, value in targets.items())),
                }
            )
        result.sort(key=lambda row: row["calendar_index"])
        return result

    actual = canonical(generated)
    wanted = canonical(expected)
    mismatch_indices = [
        index
        for index in range(max(len(actual), len(wanted)))
        if index >= len(actual) or index >= len(wanted) or actual[index] != wanted[index]
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_count": len(actual),
        "expected_count": len(wanted),
        "matched_count": len(actual) - len(mismatch_indices) if len(actual) == len(wanted) else 0,
        "mismatch_count": len(mismatch_indices),
        "first_mismatch_index": mismatch_indices[0] if mismatch_indices else None,
        "generated_sha256": _sha256_payload(actual),
        "expected_sha256": _sha256_payload(wanted),
        "passed": not mismatch_indices,
    }


__all__ = [
    "CHALLENGER_WEIGHT_PPM",
    "DeploymentSpec",
    "FROZEN_ROUTE",
    "GENERATOR_ID",
    "GenerationResult",
    "InputSnapshot",
    "OFFSET_COUNT",
    "POSITION_COUNT",
    "POSITION_WEIGHT_PPM",
    "RETENTION_BUFFER",
    "SIGNAL_COLUMNS",
    "SLEEVE_CAPITAL_FEN",
    "SLEEVE_CAPITAL_WEIGHT_PPM",
    "SleeveState",
    "TargetGenerationError",
    "TenSleeveState",
    "calendar_prefix_payload",
    "calendar_prefix_sha256",
    "compare_cohort_parity",
    "fixed_core_scores",
    "generate_fixed_core_targets",
    "rank_fixed_core_tickers",
    "replay_fixed_core_cohorts",
    "select_with_retention",
]
