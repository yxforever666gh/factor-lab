"""Read-only evidence bridge from sealed ledgers to the 5.9 shadow evaluator."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .adaptive_shadow import canonical_sha256
from .adaptive_shadow_execution import ShadowCycleOutcome
from . import adaptive_shadow_store as shadow_store
from . import prospective_ledger as formal_ledger
from .prospective_execution import CycleOutcome


CONTROL_ID = "formal_fixed_core_full"
CHALLENGER_IDS = ("low_turnover_20_v1", "low_volatility_252_v1")
class AdaptiveShadowEvidenceError(ValueError):
    """Raised when audited formal and shadow evidence cannot be cross-bound."""


@dataclass(frozen=True, slots=True)
class AdaptiveShadowEvidence:
    formal_head_record_sha256: str
    shadow_head_record_sha256: str
    evaluation_rows: tuple[dict[str, Any], ...]
def ppb_to_ppm(value: int) -> int:
    """Convert integer PPB to integer PPM using exact round-half-to-even."""

    if type(value) is not int:
        raise AdaptiveShadowEvidenceError("net_return_ppb must be an integer")
    sign = -1 if value < 0 else 1
    quotient, remainder = divmod(abs(value), 1_000)
    if remainder > 500 or (remainder == 500 and quotient % 2 == 1):
        quotient += 1
    return sign * quotient
def canonical_plan_targets_sha256(value: Any) -> str:
    if not isinstance(value, Mapping) or not value:
        raise AdaptiveShadowEvidenceError("plan targets must be a non-empty mapping")
    targets: dict[str, int] = {}
    for ticker, weight in value.items():
        if not isinstance(ticker, str) or not ticker or ticker != ticker.strip():
            raise AdaptiveShadowEvidenceError("plan target ticker is invalid")
        if type(weight) is not int or weight <= 0:
            raise AdaptiveShadowEvidenceError("plan target weight must be a positive integer")
        if ticker in targets:
            raise AdaptiveShadowEvidenceError("plan targets contain a duplicate ticker")
        targets[ticker] = weight
    return canonical_sha256({"targets_ppm": dict(sorted(targets.items()))})
def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AdaptiveShadowEvidenceError(f"{label} must be an object")
    return dict(value)
def _record_kind(metadata: Mapping[str, Any], *, shadow: bool) -> str:
    record = _mapping(metadata.get("record"), "record envelope")
    kind = record.get("kind")
    if not isinstance(kind, str) or (not shadow and metadata.get("kind") != kind):
        raise AdaptiveShadowEvidenceError("record kind binding is invalid")
    return kind
def _record_sha(metadata: Mapping[str, Any]) -> str:
    value = metadata.get("record_sha256")
    if not isinstance(value, str) or len(value) != 64:
        raise AdaptiveShadowEvidenceError("record SHA-256 is invalid")
    try:
        int(value, 16)
    except ValueError as exc:
        raise AdaptiveShadowEvidenceError("record SHA-256 is invalid") from exc
    return value
def _formal_plan(metadata: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(_mapping(metadata["record"], "formal record").get("payload"), "formal payload")
    return _mapping(payload.get("plan"), "formal decision plan")
def _due_targets(plan: Mapping[str, Any]) -> tuple[dict[str, Any], Mapping[str, Any]]:
    route = _mapping(plan.get("route_target_plan"), "formal route target plan")
    due = route.get("due_offset")
    sleeves = route.get("sleeve_plans")
    if type(due) is not int or not 0 <= due < 10 or not isinstance(sleeves, list) or len(sleeves) != 10:
        raise AdaptiveShadowEvidenceError("formal due sleeve structure is invalid")
    sleeve = _mapping(sleeves[due], "formal due sleeve")
    if sleeve.get("offset") != due:
        raise AdaptiveShadowEvidenceError("formal due sleeve offset differs")
    return route, _mapping(sleeve.get("targets_ppm"), "formal due sleeve targets")
def _formal_cycle(value: Any) -> CycleOutcome:
    try:
        return CycleOutcome.from_mapping(_mapping(value, "formal cycle_outcome"))
    except Exception as exc:
        raise AdaptiveShadowEvidenceError("formal cycle_outcome is invalid") from exc
def _shadow_cycle(value: Any) -> ShadowCycleOutcome:
    try:
        return ShadowCycleOutcome.from_mapping(_mapping(value, "shadow cycle_outcome"))
    except Exception as exc:
        raise AdaptiveShadowEvidenceError("shadow cycle_outcome is invalid") from exc


def _daily_path_payload(cycle: Any) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for point in cycle.daily_path:
        if isinstance(point, Mapping):
            output.append(dict(point))
            continue
        to_dict = getattr(point, "to_dict", None)
        if not callable(to_dict):
            raise AdaptiveShadowEvidenceError("cycle daily_path row is not replayable")
        output.append(_mapping(to_dict(), "cycle daily_path row"))
    if not output:
        raise AdaptiveShadowEvidenceError("cycle daily_path is empty")
    return output


def _evaluation_row(candidate: str, decision_sha: str, cycle: Any, targets: Any) -> dict[str, Any]:
    converted = ppb_to_ppm(cycle.net_return_ppb)
    if converted <= -1_000_000:
        raise AdaptiveShadowEvidenceError("a total-loss outcome cannot enter the shadow evaluator")
    return {
        "candidate_id": candidate,
        "formal_decision_record_sha256": decision_sha,
        "signal_date": cycle.signal_date,
        "end_date": cycle.holding_end_date,
        "offset": cycle.offset,
        "net_return_ppm": converted,
        "opening_nav_fen": cycle.opening_nav_fen,
        "ending_nav_fen": cycle.ending_nav_fen,
        "blocked_order_count": cycle.blocked_order_count,
        "daily_path": _daily_path_payload(cycle),
        "plan_targets_sha256": canonical_plan_targets_sha256(targets),
    }
def _assemble_evidence(
    formal_records: Sequence[Mapping[str, Any]],
    shadow_records: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    formal_shas = {_record_sha(row) for row in formal_records}
    decisions = {
        _record_sha(row): _formal_plan(row)
        for row in formal_records
        if _record_kind(row, shadow=False) == "decision"
    }
    activations = [
        _mapping(row.get("payload"), "shadow activation")
        for row in shadow_records
        if _record_kind(row, shadow=True) == "activation"
    ]
    if len(activations) != 1:
        raise AdaptiveShadowEvidenceError("shadow store must contain exactly one activation")
    activation = activations[0]
    if activation.get("formal_head_record_sha256") not in formal_shas:
        raise AdaptiveShadowEvidenceError("shadow activation binds no record in the formal ledger")
    start_after = activation.get("start_after")
    if not isinstance(start_after, str):
        raise AdaptiveShadowEvidenceError("shadow activation start_after is invalid")

    rows: list[dict[str, Any]] = []
    formal_cycles: dict[tuple[str, str, int], Any] = {}
    for metadata in formal_records:
        if _record_kind(metadata, shadow=False) != "outcome":
            continue
        payload = _mapping(_mapping(metadata["record"], "formal record").get("payload"), "formal outcome")
        if "cycle_outcome" not in payload:
            raise AdaptiveShadowEvidenceError("legacy formal outcome cannot enter 5.9 evidence")
        decision_sha = payload.get("decision_record_sha256")
        if decision_sha not in decisions:
            raise AdaptiveShadowEvidenceError("formal outcome binds no formal decision")
        cycle = _formal_cycle(payload["cycle_outcome"])
        plan = decisions[decision_sha]
        route, targets = _due_targets(plan)
        if (
            cycle.generation_result_sha256 != route.get("result_sha256")
            or cycle.signal_date != route.get("signal_date")
            or cycle.holding_start_date != route.get("trade_date")
            or cycle.offset != route.get("due_offset")
        ):
            raise AdaptiveShadowEvidenceError("formal outcome differs from its due sleeve decision")
        if cycle.signal_date <= start_after:
            continue
        key = (str(decision_sha), cycle.signal_date, cycle.offset)
        if key in formal_cycles:
            raise AdaptiveShadowEvidenceError("duplicate formal outcome cohort")
        formal_cycles[key] = cycle
        rows.append(_evaluation_row(CONTROL_ID, str(decision_sha), cycle, targets))

    plans: dict[str, dict[str, Any]] = {}
    for metadata in shadow_records:
        if _record_kind(metadata, shadow=True) == "plan":
            sha = _record_sha(metadata)
            if sha in plans:
                raise AdaptiveShadowEvidenceError("duplicate shadow plan record")
            plans[sha] = _mapping(metadata.get("payload"), "shadow plan")

    seen: set[tuple[str, str, str, int]] = set()
    shadow_calendar: dict[tuple[str, str, int], tuple[str, str]] = {}
    for metadata in shadow_records:
        if _record_kind(metadata, shadow=True) != "outcome":
            continue
        payload = _mapping(metadata.get("payload"), "shadow outcome")
        plan = plans.get(str(payload.get("plan_record_sha256")))
        if plan is None:
            raise AdaptiveShadowEvidenceError("shadow outcome binds no completed plan")
        cycle = _shadow_cycle(payload.get("cycle_outcome"))
        decision_sha = cycle.formal_decision_record_sha256
        formal_plan = decisions.get(decision_sha)
        if formal_plan is None:
            raise AdaptiveShadowEvidenceError("shadow outcome binds no formal decision")
        route, _formal_targets = _due_targets(formal_plan)
        bindings = (
            plan.get("formal_decision_record_sha256") == decision_sha,
            plan.get("formal_route_target_plan_sha256") == formal_plan.get("route_target_plan_sha256"),
            plan.get("formal_input_snapshot_sha256") == route.get("input_snapshot_sha256"),
            plan.get("source_data_snapshot_sha256") == formal_plan.get("source_data_snapshot_sha256"),
            cycle.signal_date == route.get("signal_date") == plan.get("signal_date"),
            cycle.holding_start_date == route.get("trade_date") == plan.get("trade_date"),
            cycle.offset == route.get("due_offset") == plan.get("offset"),
            cycle.candidate_id == plan.get("candidate_id"),
        )
        if not all(bindings) or cycle.candidate_id not in CHALLENGER_IDS:
            raise AdaptiveShadowEvidenceError("shadow outcome/formal decision binding differs")
        cohort = (decision_sha, cycle.signal_date, cycle.offset)
        calendar = (cycle.holding_start_date, cycle.holding_end_date)
        if cohort in shadow_calendar and shadow_calendar[cohort] != calendar:
            raise AdaptiveShadowEvidenceError("challengers disagree on the cohort holding calendar")
        shadow_calendar[cohort] = calendar
        formal_cycle = formal_cycles.get(cohort)
        if formal_cycle is not None and (
            formal_cycle.holding_start_date,
            formal_cycle.holding_end_date,
        ) != calendar:
            raise AdaptiveShadowEvidenceError("formal and shadow outcomes disagree on the cohort calendar")
        identity = (cycle.candidate_id, cycle.signal_date, cycle.holding_end_date, cycle.offset)
        if identity in seen:
            raise AdaptiveShadowEvidenceError("duplicate challenger outcome cohort")
        seen.add(identity)
        rows.append(_evaluation_row(cycle.candidate_id, decision_sha, cycle, plan.get("targets_ppm")))

    visible_cohorts: dict[tuple[str, str, int], str] = {}
    for row in rows:
        key = (row["signal_date"], row["end_date"], row["offset"])
        prior = visible_cohorts.setdefault(key, row["formal_decision_record_sha256"])
        if prior != row["formal_decision_record_sha256"]:
            raise AdaptiveShadowEvidenceError("cohort rows bind different formal decisions")
    order = {CONTROL_ID: 0, **{value: index + 1 for index, value in enumerate(CHALLENGER_IDS)}}
    rows.sort(key=lambda row: (row["signal_date"], row["end_date"], row["offset"], order[row["candidate_id"]]))
    return tuple(rows)


def _require_read_only_layout(paths: Sequence[Path], lock_path: Path, label: str) -> None:
    if any(not path.is_dir() or path.is_symlink() for path in paths):
        raise AdaptiveShadowEvidenceError(f"{label} read-only layout is incomplete")
    if not lock_path.is_file() or lock_path.is_symlink() or lock_path.stat().st_size < 1:
        raise AdaptiveShadowEvidenceError(f"{label} read-only lock is missing or unseeded")


def _audited_formal_records(root: str | Path) -> tuple[list[dict[str, Any]], str]:
    layout = formal_ledger.LedgerLayout.at(root)
    _require_read_only_layout(
        (layout.root, layout.records, layout.snapshots, layout.plans, layout.bundles, layout.inputs,
         layout.executions, layout.release_runners, layout.dispatch, layout.verification_cache),
        layout.lock_path,
        "formal ledger",
    )
    audit = formal_ledger.audit_ledger(
        layout.root,
        refresh_verification_cache=False,
    )
    if audit.get("valid") is not True:
        raise AdaptiveShadowEvidenceError("formal ledger audit is not valid")
    with formal_ledger._existing_read_lock(layout):
        records, _state = formal_ledger._load_record_chain(layout)
    head = records[-1]["record_sha256"] if records else None
    if head is None or head != audit.get("head_record_sha256"):
        raise AdaptiveShadowEvidenceError("formal ledger changed across the audited read")
    return records, str(head)


def _audited_shadow_records(root: str | Path) -> tuple[list[dict[str, Any]], str]:
    layout = shadow_store.ShadowLayout.at(root)
    _require_read_only_layout((layout.root, layout.records, layout.artifacts), layout.lock_path, "shadow store")
    audit = shadow_store.audit_shadow_store(layout)
    if audit.get("integrity_valid") is not True or audit.get("activated") is not True:
        raise AdaptiveShadowEvidenceError("shadow store audit is not valid and activated")
    with shadow_store._lock(layout, create=False):
        records, _state, _orphans = shadow_store._load(layout)
    head = records[-1]["record_sha256"] if records else None
    if head is None or head != audit.get("head_record_sha256"):
        raise AdaptiveShadowEvidenceError("shadow store changed across the audited read")
    return records, str(head)


def load_adaptive_shadow_evidence(
    formal_ledger_root: str | Path,
    shadow_store_root: str | Path,
) -> AdaptiveShadowEvidence:
    """Audit and read both stores without creating or modifying any runtime path."""

    formal_records, formal_head = _audited_formal_records(formal_ledger_root)
    shadow_records, shadow_head = _audited_shadow_records(shadow_store_root)
    rows = _assemble_evidence(formal_records, shadow_records)
    return AdaptiveShadowEvidence(formal_head, shadow_head, rows)


__all__ = [
    "AdaptiveShadowEvidence",
    "AdaptiveShadowEvidenceError",
    "CHALLENGER_IDS",
    "CONTROL_ID",
    "canonical_plan_targets_sha256",
    "load_adaptive_shadow_evidence",
    "ppb_to_ppm",
]
