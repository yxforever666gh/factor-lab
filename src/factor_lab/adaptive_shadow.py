"""Pure contracts for prospective adaptive-shadow research.

This module deliberately has no filesystem, clock, network, CLI, or prospective
ledger dependency.  It provides the deterministic logic needed to register a
small challenger set, produce contemporaneous shadow targets, compare matured
candidate-control pairs, and decide whether a plan was created in its
admissible time window.

Concrete factor formula implementations do not belong here.  A caller supplies
an explicitly versioned score function and this module exposes to it only the
candidate's declared point-in-time fields.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any
import unicodedata


REGISTRY_SCHEMA_VERSION = 1
MAX_CANDIDATES = 3
WEIGHT_SCALE_PPM = 1_000_000

_CANDIDATE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_COMMIT_OID_RE = re.compile(r"^[0-9a-f]{40}$")
_RELEASE_TAG_RE = re.compile(r"^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$")
_RESERVED_ROW_FIELDS = frozenset({"date", "ticker", "eligible"})


class AdaptiveShadowError(ValueError):
    """Raised when an adaptive-shadow contract fails closed."""


def _text(value: Any, *, label: str) -> str:
    if not isinstance(value, str):
        raise AdaptiveShadowError(f"{label} must be a non-empty string")
    normalized = unicodedata.normalize("NFC", value.strip())
    if not normalized or any(0xD800 <= ord(character) <= 0xDFFF for character in normalized):
        raise AdaptiveShadowError(f"{label} must be a non-empty Unicode string")
    return normalized


def _date_text(value: Any, *, label: str) -> str:
    if isinstance(value, datetime):
        raise AdaptiveShadowError(f"{label} must be an ISO date, not a timestamp")
    if isinstance(value, date):
        return value.isoformat()
    text = _text(value, label=label)
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise AdaptiveShadowError(f"{label} must be an ISO date") from exc
    if parsed.isoformat() != text:
        raise AdaptiveShadowError(f"{label} must use canonical ISO date encoding")
    return text


def _utc_text(value: Any, *, label: str) -> str:
    if isinstance(value, str):
        raw = _text(value, label=label)
        try:
            parsed = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
        except ValueError as exc:
            raise AdaptiveShadowError(f"{label} must be an ISO timestamp") from exc
    elif isinstance(value, datetime):
        parsed = value
    else:
        raise AdaptiveShadowError(f"{label} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdaptiveShadowError(f"{label} must be timezone-aware")
    parsed = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if parsed.microsecond else "seconds"
    return parsed.isoformat(timespec=timespec).replace("+00:00", "Z")


def _utc_datetime(value: Any, *, label: str) -> datetime:
    return datetime.fromisoformat(_utc_text(value, label=label).replace("Z", "+00:00"))


def _positive_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AdaptiveShadowError(f"{label} must be a positive integer")
    return int(value)


def _finite_number(value: Any, *, label: str) -> float:
    if isinstance(value, (bool, str, bytes, bytearray)) or value is None:
        raise AdaptiveShadowError(f"{label} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise AdaptiveShadowError(f"{label} must be a finite number") from exc
    if not math.isfinite(result):
        raise AdaptiveShadowError(f"{label} must be a finite number")
    return result


def _canonical_normalize(value: Any, *, path: str = "$") -> Any:
    if value is None or isinstance(value, bool) or type(value) is int:
        return value
    if isinstance(value, str):
        return _text(value, label=path)
    if isinstance(value, Mapping):
        output: dict[str, Any] = {}
        for raw_key, raw_value in value.items():
            key = _text(raw_key, label=f"{path} key")
            if key in output:
                raise AdaptiveShadowError(f"duplicate canonical key at {path}: {key}")
            output[key] = _canonical_normalize(raw_value, path=f"{path}.{key}")
        return {key: output[key] for key in sorted(output)}
    if isinstance(value, (list, tuple)):
        return [
            _canonical_normalize(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    raise AdaptiveShadowError(f"unsupported canonical value at {path}: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Return compact canonical JSON for integer/string contract payloads."""

    return json.dumps(
        _canonical_normalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


@dataclass(frozen=True)
class SelectionSpec:
    """A deterministic equal-weight long-only Top-N selection policy."""

    top_n: int
    retention_n: int
    weighting: str = "equal_weight_long_only"

    def __post_init__(self) -> None:
        top_n = _positive_int(self.top_n, label="selection.top_n")
        retention_n = _positive_int(self.retention_n, label="selection.retention_n")
        if top_n > WEIGHT_SCALE_PPM:
            raise AdaptiveShadowError("selection.top_n exceeds integer PPM precision")
        if retention_n < top_n:
            raise AdaptiveShadowError("selection.retention_n must be at least top_n")
        weighting = _text(self.weighting, label="selection.weighting")
        if weighting != "equal_weight_long_only":
            raise AdaptiveShadowError("only equal_weight_long_only selection is supported")
        object.__setattr__(self, "top_n", top_n)
        object.__setattr__(self, "retention_n", retention_n)
        object.__setattr__(self, "weighting", weighting)

    def to_payload(self) -> dict[str, Any]:
        return {
            "retention_n": self.retention_n,
            "top_n": self.top_n,
            "weighting": self.weighting,
        }


@dataclass(frozen=True)
class CandidateSpec:
    """Tag-bound disclosure for one shadow candidate.

    ``formula`` is an identity disclosure, not an expression evaluated by this
    module.  ``required_fields`` is also an execution allowlist: the score
    callback cannot observe undeclared row columns.
    """

    candidate_id: str
    version: int
    formula: str
    required_fields: tuple[str, ...]
    direction: int
    selection: SelectionSpec
    selection_disclosure: str
    start_after: str

    def __post_init__(self) -> None:
        candidate_id = _text(self.candidate_id, label="candidate_id")
        if not _CANDIDATE_ID_RE.fullmatch(candidate_id):
            raise AdaptiveShadowError("candidate_id must use lowercase safe identifier syntax")
        formula = _text(self.formula, label=f"candidate {candidate_id} formula")
        disclosure = _text(
            self.selection_disclosure,
            label=f"candidate {candidate_id} selection_disclosure",
        )
        if not isinstance(self.selection, SelectionSpec):
            raise AdaptiveShadowError("candidate selection must be SelectionSpec")
        version = _positive_int(self.version, label=f"candidate {candidate_id} version")
        if isinstance(self.direction, bool) or self.direction not in {-1, 1}:
            raise AdaptiveShadowError("candidate direction must be exactly -1 or 1")
        if not isinstance(self.required_fields, (tuple, list)) or not self.required_fields:
            raise AdaptiveShadowError("candidate required_fields must be non-empty")
        fields = tuple(
            _text(value, label=f"candidate {candidate_id} required field")
            for value in self.required_fields
        )
        if len(fields) != len(set(fields)):
            raise AdaptiveShadowError("candidate required_fields contain duplicates")
        if set(fields) & _RESERVED_ROW_FIELDS:
            raise AdaptiveShadowError(
                "candidate required_fields cannot redeclare row identity fields"
            )
        object.__setattr__(self, "candidate_id", candidate_id)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "formula", formula)
        object.__setattr__(self, "required_fields", tuple(sorted(fields)))
        object.__setattr__(self, "direction", int(self.direction))
        object.__setattr__(self, "selection_disclosure", disclosure)
        object.__setattr__(
            self,
            "start_after",
            _date_text(self.start_after, label=f"candidate {candidate_id} start_after"),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "direction": self.direction,
            "formula": self.formula,
            "required_fields": list(self.required_fields),
            "selection": self.selection.to_payload(),
            "selection_disclosure": self.selection_disclosure,
            "start_after": self.start_after,
            "version": self.version,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.to_payload())


@dataclass(frozen=True)
class Registry:
    """An immutable registry of at most three challengers.

    The formal incumbent is an external anchor and is intentionally not a
    member of this registry, so it cannot consume a challenger slot.
    """

    protocol_version: str
    release_tag: str
    commit_oid: str
    released_at_utc: str
    candidates: tuple[CandidateSpec, ...]

    def __post_init__(self) -> None:
        protocol_version = _text(self.protocol_version, label="registry protocol_version")
        release_tag = _text(self.release_tag, label="registry release_tag")
        if release_tag != self.release_tag or not _RELEASE_TAG_RE.fullmatch(release_tag):
            raise AdaptiveShadowError("registry release_tag must be canonical major.minor")
        commit_oid = _text(self.commit_oid, label="registry commit_oid").lower()
        if not _COMMIT_OID_RE.fullmatch(commit_oid):
            raise AdaptiveShadowError("registry commit_oid must be a lowercase 40-hex Git OID")
        if not isinstance(self.candidates, (tuple, list)) or not self.candidates:
            raise AdaptiveShadowError("registry candidates must be non-empty")
        if len(self.candidates) > MAX_CANDIDATES:
            raise AdaptiveShadowError(
                f"registry cannot contain more than {MAX_CANDIDATES} candidates"
            )
        if any(not isinstance(candidate, CandidateSpec) for candidate in self.candidates):
            raise AdaptiveShadowError("registry candidates must be CandidateSpec values")
        identifiers = [candidate.candidate_id for candidate in self.candidates]
        if len(identifiers) != len(set(identifiers)):
            raise AdaptiveShadowError("registry candidate IDs must be unique")
        object.__setattr__(self, "protocol_version", protocol_version)
        object.__setattr__(self, "release_tag", release_tag)
        object.__setattr__(self, "commit_oid", commit_oid)
        object.__setattr__(
            self,
            "released_at_utc",
            _utc_text(self.released_at_utc, label="registry released_at_utc"),
        )
        object.__setattr__(
            self,
            "candidates",
            tuple(sorted(self.candidates, key=lambda candidate: candidate.candidate_id)),
        )

    def candidate(self, candidate_id: str) -> CandidateSpec:
        requested = _text(candidate_id, label="candidate_id")
        for candidate in self.candidates:
            if candidate.candidate_id == requested:
                return candidate
        raise AdaptiveShadowError(f"candidate is not registered: {requested}")

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidates": [candidate.to_payload() for candidate in self.candidates],
            "commit_oid": self.commit_oid,
            "kind": "adaptive_shadow_registry",
            "maximum_challenger_count": MAX_CANDIDATES,
            "protocol_version": self.protocol_version,
            "release_tag": self.release_tag,
            "released_at_utc": self.released_at_utc,
            "schema_version": REGISTRY_SCHEMA_VERSION,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.to_payload())


@dataclass(frozen=True)
class NormalizedSignalRow:
    date: str
    ticker: str
    eligible: bool
    features: tuple[tuple[str, float | None], ...]

    def scorer_view(self) -> Mapping[str, Any]:
        values: dict[str, Any] = {
            "date": self.date,
            "ticker": self.ticker,
            "eligible": self.eligible,
        }
        values.update(dict(self.features))
        return MappingProxyType(values)


def normalize_input_rows(
    rows: Sequence[Mapping[str, Any]],
    required_fields: Sequence[str],
    *,
    signal_date: str | date | None = None,
) -> tuple[NormalizedSignalRow, ...]:
    """Normalize relevant signal rows and reject conflicting identity keys.

    When ``signal_date`` is supplied, rows for other dates are deliberately not
    inspected beyond their date.  Appending genuinely future observations
    therefore cannot change or invalidate a historical target decision.
    """

    if isinstance(rows, (str, bytes, bytearray)) or not isinstance(rows, Sequence):
        raise AdaptiveShadowError("input rows must be a sequence of objects")
    if isinstance(required_fields, (str, bytes, bytearray)):
        raise AdaptiveShadowError("required_fields must be a sequence of field names")
    fields = tuple(_text(value, label="required field") for value in required_fields)
    if not fields or len(fields) != len(set(fields)):
        raise AdaptiveShadowError("required_fields must be unique and non-empty")
    fields = tuple(sorted(fields))
    requested = (
        _date_text(signal_date, label="signal_date") if signal_date is not None else None
    )
    observed: dict[tuple[str, str], NormalizedSignalRow] = {}
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise AdaptiveShadowError(f"input row {index} must be an object")
        row_date = _date_text(raw.get("date"), label=f"input row {index} date")
        if requested is not None and row_date != requested:
            continue
        ticker = _text(raw.get("ticker"), label=f"input row {index} ticker")
        eligible = raw.get("eligible")
        if type(eligible) is not bool:
            raise AdaptiveShadowError(f"input row {index} eligible must be boolean")
        features: list[tuple[str, float | None]] = []
        for field_name in fields:
            if field_name not in raw:
                if eligible:
                    raise AdaptiveShadowError(
                        f"eligible input row {ticker} is missing required field {field_name}"
                    )
                features.append((field_name, None))
                continue
            try:
                feature = _finite_number(
                    raw[field_name], label=f"input row {ticker} field {field_name}"
                )
            except AdaptiveShadowError:
                if eligible:
                    raise
                feature = None
            features.append((field_name, feature))
        normalized = NormalizedSignalRow(
            date=row_date,
            ticker=ticker,
            eligible=eligible,
            features=tuple(features),
        )
        identity = (row_date, ticker)
        previous = observed.get(identity)
        if previous is not None and previous != normalized:
            raise AdaptiveShadowError(f"conflicting duplicate input key: {row_date}/{ticker}")
        observed[identity] = normalized
    return tuple(observed[key] for key in sorted(observed))


ScoreFunction = Callable[[Mapping[str, Any]], Any]


@dataclass(frozen=True)
class TargetPlan:
    registry_sha256: str
    candidate_id: str
    candidate_sha256: str
    signal_date: str
    selected_tickers: tuple[str, ...]
    targets_ppm: tuple[tuple[str, int], ...]
    ranked_tickers: tuple[str, ...]

    def target_mapping(self) -> Mapping[str, int]:
        return MappingProxyType(dict(self.targets_ppm))

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "candidate_sha256": self.candidate_sha256,
            "registry_sha256": self.registry_sha256,
            "selected_tickers": list(self.selected_tickers),
            "signal_date": self.signal_date,
            "targets_ppm": dict(self.targets_ppm),
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.to_payload())


def _signal_is_after_registration(
    registry: Registry, candidate: CandidateSpec, signal_date: str
) -> bool:
    release_date = _utc_datetime(
        registry.released_at_utc, label="registry released_at_utc"
    ).date().isoformat()
    return signal_date > release_date and signal_date > candidate.start_after


def generate_targets(
    *,
    registry: Registry,
    candidate_id: str,
    signal_date: str | date,
    rows: Sequence[Mapping[str, Any]],
    score: ScoreFunction,
    previous_targets: Sequence[str] = (),
) -> TargetPlan:
    """Generate deterministic equal-weight Top-N targets for one candidate."""

    if not isinstance(registry, Registry):
        raise AdaptiveShadowError("registry must be Registry")
    if not callable(score):
        raise AdaptiveShadowError("score must be callable")
    candidate = registry.candidate(candidate_id)
    requested = _date_text(signal_date, label="signal_date")
    if not _signal_is_after_registration(registry, candidate, requested):
        raise AdaptiveShadowError(
            "signal_date is not strictly after registry release and start_after"
        )
    normalized = normalize_input_rows(
        rows,
        candidate.required_fields,
        signal_date=requested,
    )
    ranked_rows: list[tuple[float, str]] = []
    for row in normalized:
        if not row.eligible:
            continue
        raw_score = _finite_number(
            score(row.scorer_view()),
            label=f"candidate {candidate.candidate_id} score for {row.ticker}",
        )
        ranked_rows.append((candidate.direction * raw_score, row.ticker))
    ranked_rows.sort(key=lambda item: (-item[0], item[1]))
    ranked = tuple(ticker for _score, ticker in ranked_rows)
    if len(ranked) < candidate.selection.top_n:
        raise AdaptiveShadowError("eligible finite score count is below candidate Top-N")

    if isinstance(previous_targets, (str, bytes, bytearray)):
        raise AdaptiveShadowError("previous_targets must be a sequence of tickers")
    previous = tuple(_text(value, label="previous target") for value in previous_targets)
    if len(previous) != len(set(previous)):
        raise AdaptiveShadowError("previous_targets contain duplicates")
    if len(previous) > candidate.selection.top_n:
        raise AdaptiveShadowError("previous_targets exceed candidate Top-N")
    retained = set(previous) & set(ranked[: candidate.selection.retention_n])
    selected = [ticker for ticker in ranked if ticker in retained]
    for ticker in ranked:
        if len(selected) >= candidate.selection.top_n:
            break
        if ticker not in selected:
            selected.append(ticker)
    rank_order = {ticker: index for index, ticker in enumerate(ranked)}
    selected = sorted(selected[: candidate.selection.top_n], key=rank_order.__getitem__)
    if len(selected) != candidate.selection.top_n:
        raise AdaptiveShadowError("candidate selection did not produce exactly Top-N targets")
    ordered_targets = tuple(sorted(selected))
    base_weight, remainder = divmod(WEIGHT_SCALE_PPM, len(ordered_targets))
    targets_ppm = tuple(
        (ticker, base_weight + (1 if index < remainder else 0))
        for index, ticker in enumerate(ordered_targets)
    )
    if any(weight <= 0 for _ticker, weight in targets_ppm) or sum(
        weight for _ticker, weight in targets_ppm
    ) != WEIGHT_SCALE_PPM:
        raise AdaptiveShadowError("target PPM weights are not funded long-only weights")
    return TargetPlan(
        registry_sha256=registry.sha256,
        candidate_id=candidate.candidate_id,
        candidate_sha256=candidate.sha256,
        signal_date=requested,
        selected_tickers=tuple(selected),
        targets_ppm=targets_ppm,
        ranked_tickers=ranked,
    )


@dataclass(frozen=True)
class OutcomeObservation:
    candidate_id: str
    signal_date: str
    start_date: str
    end_date: str
    net_return: float

    def __post_init__(self) -> None:
        candidate_id = _text(self.candidate_id, label="outcome candidate_id")
        signal = _date_text(self.signal_date, label="outcome signal_date")
        start = _date_text(self.start_date, label="outcome start_date")
        end = _date_text(self.end_date, label="outcome end_date")
        if not signal < start <= end:
            raise AdaptiveShadowError("outcome dates must satisfy signal < start <= end")
        net_return = _finite_number(self.net_return, label="outcome net_return")
        if net_return <= -1.0:
            raise AdaptiveShadowError("outcome net_return must be greater than -1")
        object.__setattr__(self, "candidate_id", candidate_id)
        object.__setattr__(self, "signal_date", signal)
        object.__setattr__(self, "start_date", start)
        object.__setattr__(self, "end_date", end)
        object.__setattr__(self, "net_return", net_return)

    @property
    def cohort_identity(self) -> tuple[str, str, str]:
        return (self.signal_date, self.start_date, self.end_date)

    def identity(self) -> tuple[str, str, str, str]:
        return (self.candidate_id, *self.cohort_identity)

    def history_payload(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "end_date": self.end_date,
            "net_return_float_hex": self.net_return.hex(),
            "signal_date": self.signal_date,
            "start_date": self.start_date,
        }


def normalize_outcomes(
    outcomes: Sequence[OutcomeObservation | Mapping[str, Any]],
) -> tuple[OutcomeObservation, ...]:
    if isinstance(outcomes, (str, bytes, bytearray)) or not isinstance(outcomes, Sequence):
        raise AdaptiveShadowError("outcomes must be a sequence")
    observed: dict[tuple[str, str, str, str], OutcomeObservation] = {}
    required = {"candidate_id", "signal_date", "start_date", "end_date", "net_return"}
    for index, raw in enumerate(outcomes):
        if isinstance(raw, OutcomeObservation):
            outcome = raw
        elif isinstance(raw, Mapping):
            if set(raw) != required:
                raise AdaptiveShadowError(
                    f"outcome {index} must contain exactly {sorted(required)}"
                )
            outcome = OutcomeObservation(**{key: raw[key] for key in required})
        else:
            raise AdaptiveShadowError(f"outcome {index} must be an object")
        identity = outcome.identity()
        previous = observed.get(identity)
        if previous is not None and previous != outcome:
            raise AdaptiveShadowError(
                "conflicting duplicate outcome key: " + "/".join(identity)
            )
        observed[identity] = outcome
    return tuple(observed[key] for key in sorted(observed))


def _aligned_matured_outcomes(
    *,
    candidate_ids: Sequence[str],
    signal_date: str,
    outcomes: Sequence[OutcomeObservation | Mapping[str, Any]],
) -> tuple[
    tuple[tuple[tuple[str, str, str], tuple[OutcomeObservation, ...]], ...],
    int,
]:
    identifiers = tuple(sorted(_text(value, label="candidate ID") for value in candidate_ids))
    if not identifiers or len(identifiers) != len(set(identifiers)):
        raise AdaptiveShadowError("candidate IDs must be unique and non-empty")
    normalized = normalize_outcomes(outcomes)
    selected = [row for row in normalized if row.candidate_id in identifiers]
    matured = [row for row in selected if row.end_date < signal_date]
    excluded = len(selected) - len(matured)
    grouped: dict[tuple[str, str, str], dict[str, OutcomeObservation]] = {}
    for outcome in matured:
        grouped.setdefault(outcome.cohort_identity, {})[outcome.candidate_id] = outcome
    expected = set(identifiers)
    aligned: list[tuple[tuple[str, str, str], tuple[OutcomeObservation, ...]]] = []
    for cohort in sorted(grouped):
        values = grouped[cohort]
        if set(values) != expected:
            raise AdaptiveShadowError("matured feedback cohort does not cover every candidate")
        aligned.append((cohort, tuple(values[identifier] for identifier in identifiers)))
    return tuple(aligned), excluded


@dataclass(frozen=True)
class PlanTimingDecision:
    admissible: bool
    missed_deadline: bool
    backfill_forbidden: bool
    reason: str


def assess_plan_timing(
    *,
    registry: Registry,
    candidate_id: str,
    signal_date: str | date,
    created_at_utc: str | datetime,
    deadline_at_utc: str | datetime,
) -> PlanTimingDecision:
    """Classify a shadow plan without reading a clock or writing a record."""

    candidate = registry.candidate(candidate_id)
    signal = _date_text(signal_date, label="signal_date")
    created = _utc_datetime(created_at_utc, label="created_at_utc")
    deadline = _utc_datetime(deadline_at_utc, label="deadline_at_utc")
    released = _utc_datetime(registry.released_at_utc, label="registry released_at_utc")
    if deadline.date().isoformat() <= signal:
        raise AdaptiveShadowError("deadline must fall after the signal date")
    if signal <= released.date().isoformat():
        return PlanTimingDecision(False, False, True, "signal_not_after_registry_release")
    if signal <= candidate.start_after:
        return PlanTimingDecision(False, False, True, "signal_not_after_candidate_start")
    if created < released:
        return PlanTimingDecision(False, False, True, "created_before_registry_release")
    if created > deadline:
        return PlanTimingDecision(False, True, True, "missed_deadline")
    return PlanTimingDecision(True, False, False, "admissible")


@dataclass(frozen=True)
class PromotionAssessment:
    evaluation_signal_date: str
    anchor_id: str
    challenger_id: str
    complete_cohort_count: int
    mean_excess_return: float | None
    positive_excess_ratio: float | None
    eligible_for_major_review: bool
    reason: str
    evidence_sha256: str


def assess_pairwise_promotion(
    *,
    registry: Registry,
    evaluation_signal_date: str | date,
    anchor_id: str,
    challenger_id: str,
    outcomes: Sequence[OutcomeObservation | Mapping[str, Any]],
    minimum_complete_cohorts: int,
    minimum_mean_excess: float = 0.0,
    minimum_positive_excess_ratio: float = 0.6,
) -> PromotionAssessment:
    """Return a conservative paired promotion-review signal.

    This is intentionally not a route mutation or investment claim.  Exact
    production thresholds belong in a tag-bound registry extension; arguments
    make this primitive independently testable without embedding a candidate.
    """

    requested = _date_text(evaluation_signal_date, label="evaluation_signal_date")
    anchor = _text(anchor_id, label="anchor_id")
    if not _CANDIDATE_ID_RE.fullmatch(anchor):
        raise AdaptiveShadowError("anchor_id must use lowercase safe identifier syntax")
    if any(candidate.candidate_id == anchor for candidate in registry.candidates):
        raise AdaptiveShadowError("formal anchor must remain outside the challenger registry")
    challenger = registry.candidate(challenger_id)
    if anchor == challenger.candidate_id:
        raise AdaptiveShadowError("challenger must differ from anchor")
    if not _signal_is_after_registration(registry, challenger, requested):
        raise AdaptiveShadowError(
            f"evaluation date is before registered start for {challenger.candidate_id}"
        )
    minimum = _positive_int(
        minimum_complete_cohorts, label="minimum_complete_cohorts"
    )
    excess_floor = _finite_number(minimum_mean_excess, label="minimum_mean_excess")
    positive_floor = _finite_number(
        minimum_positive_excess_ratio,
        label="minimum_positive_excess_ratio",
    )
    if excess_floor < 0.0:
        raise AdaptiveShadowError("minimum_mean_excess cannot be negative")
    if not 0.5 < positive_floor <= 1.0:
        raise AdaptiveShadowError(
            "minimum_positive_excess_ratio must lie within (0.5, 1]"
        )
    aligned, _excluded = _aligned_matured_outcomes(
        candidate_ids=(anchor, challenger.candidate_id),
        signal_date=requested,
        outcomes=outcomes,
    )
    evidence = [
        {
            "cohort": list(cohort),
            "returns_float_hex": {
                row.candidate_id: row.net_return.hex() for row in rows
            },
        }
        for cohort, rows in aligned
    ]
    evidence_sha = canonical_sha256(evidence)
    if len(aligned) < minimum:
        return PromotionAssessment(
            requested,
            anchor,
            challenger.candidate_id,
            len(aligned),
            None,
            None,
            False,
            "insufficient_complete_cohorts",
            evidence_sha,
        )
    differences: list[float] = []
    for _cohort, rows in aligned:
        returns = {row.candidate_id: row.net_return for row in rows}
        differences.append(returns[challenger.candidate_id] - returns[anchor])
    mean_excess = sum(differences) / len(differences)
    positive_ratio = sum(value > 0.0 for value in differences) / len(differences)
    eligible = mean_excess > excess_floor and positive_ratio >= positive_floor
    reason = "strict_pairwise_gate_passed" if eligible else "strict_pairwise_gate_failed"
    return PromotionAssessment(
        requested,
        anchor,
        challenger.candidate_id,
        len(aligned),
        mean_excess,
        positive_ratio,
        eligible,
        reason,
        evidence_sha,
    )


__all__ = [
    "AdaptiveShadowError",
    "CandidateSpec",
    "MAX_CANDIDATES",
    "NormalizedSignalRow",
    "OutcomeObservation",
    "PlanTimingDecision",
    "PromotionAssessment",
    "Registry",
    "SelectionSpec",
    "TargetPlan",
    "WEIGHT_SCALE_PPM",
    "assess_pairwise_promotion",
    "assess_plan_timing",
    "canonical_json_bytes",
    "canonical_sha256",
    "generate_targets",
    "normalize_input_rows",
    "normalize_outcomes",
]
