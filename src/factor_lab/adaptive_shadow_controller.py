"""Single-step controller for the release-5.9 adaptive shadow runtime."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any

from .adaptive_shadow import canonical_json_bytes, canonical_sha256
from .adaptive_shadow_evidence import CHALLENGER_IDS, _audited_formal_records
from .adaptive_shadow_execution import (
    ShadowCycleOutcome, ShadowCyclePlan, evaluate_shadow_cycle, genesis_shadow_account,
)
from .adaptive_shadow_runtime import plan_shadow_runtime
from . import adaptive_shadow_store as _shadow_store
from .data.adaptive_shadow_execution import (
    build_adaptive_shadow_execution_snapshot,
    load_adaptive_shadow_execution_snapshot,
)
from .prospective_execution import CycleOutcome, SleeveAccountState
from .prospective_targets import GenerationResult

CONTROLLER_SCHEMA_VERSION = 1


class AdaptiveShadowControllerError(ValueError):
    """Raised when sealed formal and shadow evidence cannot be cross-bound."""


@dataclass(frozen=True, slots=True)
class _Decision:
    sequence: int
    record_sha256: str
    plan_sha256: str
    plan: dict[str, Any]
    route: dict[str, Any]
    signal_date: str
    trade_date: str
    offset: int


@dataclass(frozen=True, slots=True)
class _OutcomeWork:
    decision: _Decision
    candidate_id: str
    plan_record_sha256: str
    plan_payload: dict[str, Any]
    formal_payload: dict[str, Any]
    formal_cycle: CycleOutcome


def _utc_text(value: str | datetime | None) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value and value == value.strip():
        try:
            parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        except ValueError as exc:
            raise AdaptiveShadowControllerError("observed_at_utc must be an ISO timestamp") from exc
    else:
        raise AdaptiveShadowControllerError("observed_at_utc must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AdaptiveShadowControllerError("observed_at_utc must be timezone-aware")
    normalized = parsed.astimezone(timezone.utc)
    timespec = "microseconds" if normalized.microsecond else "seconds"
    return normalized.isoformat(timespec=timespec).replace("+00:00", "Z")


def _utc_value(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _waiting(reason: str, observed: str, **bindings: Any) -> dict[str, Any]:
    return {"schema_version": CONTROLLER_SCHEMA_VERSION, "status": "waiting",
            "reason": reason, "action": None, "observed_at_utc": observed, **bindings}


def _shadow_snapshot(root: str | Path) -> tuple[list[dict[str, Any]], Any] | None:
    layout = _shadow_store.ShadowLayout.at(root)
    if not layout.root.exists():
        return None
    with _shadow_store._lock(layout, create=False):
        records, state, _orphans = _shadow_store._load(layout)
    return None if state.activation is None else (records, state)


def _record_payload(metadata: Mapping[str, Any]) -> dict[str, Any]:
    record = metadata.get("record")
    if not isinstance(record, Mapping) or not isinstance(record.get("payload"), Mapping):
        raise AdaptiveShadowControllerError("formal record payload is invalid")
    return dict(record["payload"])


def _decision(metadata: Mapping[str, Any]) -> _Decision:
    payload = _record_payload(metadata)
    plan, sequence, record_sha = payload.get("plan"), metadata.get("sequence"), metadata.get("record_sha256")
    if not isinstance(plan, Mapping) or type(sequence) is not int or not isinstance(record_sha, str):
        raise AdaptiveShadowControllerError("formal decision identity is invalid")
    plan = dict(plan)
    route, plan_sha = plan.get("route_target_plan"), payload.get("plan_sha256")
    if not isinstance(route, Mapping) or not isinstance(plan_sha, str):
        raise AdaptiveShadowControllerError("formal decision route plan is invalid")
    route = dict(route)
    offset, signal, trade = route.get("due_offset"), route.get("signal_date"), route.get("trade_date")
    if type(offset) is not int or not 0 <= offset < 10 or not isinstance(signal, str) or not isinstance(trade, str):
        raise AdaptiveShadowControllerError("formal decision coordinates are invalid")
    if canonical_sha256(plan) != plan_sha or plan.get("route_target_plan_sha256") != canonical_sha256(route):
        raise AdaptiveShadowControllerError("formal decision or route plan SHA differs")
    return _Decision(sequence, record_sha, plan_sha, plan, route, signal, trade, offset)


def _formal_view(records: Sequence[Mapping[str, Any]], activation_head: str) -> tuple[
    list[_Decision], dict[str, tuple[dict[str, Any], CycleOutcome]]
]:
    by_sha = {str(row.get("record_sha256")): row for row in records}
    bound = by_sha.get(activation_head)
    if bound is None or type(bound.get("sequence")) is not int:
        raise AdaptiveShadowControllerError("shadow activation head is absent from the formal ledger")
    decisions = [_decision(row) for row in records if row.get("kind") == "decision"
                 and type(row.get("sequence")) is int and row["sequence"] > bound["sequence"]]
    decisions.sort(key=lambda row: row.sequence)
    known, outcomes = {row.record_sha256 for row in decisions}, {}
    for metadata in records:
        if metadata.get("kind") != "outcome":
            continue
        payload = _record_payload(metadata)
        decision_sha = payload.get("decision_record_sha256")
        if decision_sha not in known:
            continue
        if "cycle_outcome" not in payload:
            raise AdaptiveShadowControllerError("a post-activation formal outcome is not replayable")
        if decision_sha in outcomes:
            raise AdaptiveShadowControllerError("duplicate formal outcome for decision")
        try:
            cycle = CycleOutcome.from_mapping(payload["cycle_outcome"])
        except Exception as exc:
            raise AdaptiveShadowControllerError("formal cycle outcome is invalid") from exc
        if (payload.get("execution_snapshot_sha256") != cycle.execution_snapshot_sha256
                or payload.get("cycle_outcome_sha256") != cycle.outcome_sha256):
            raise AdaptiveShadowControllerError("formal outcome envelope differs")
        outcomes[str(decision_sha)] = (payload, cycle)
    return decisions, outcomes


def _candidate_ids(activation: Mapping[str, Any]) -> tuple[str, ...]:
    registry = activation.get("registry")
    candidates = registry.get("candidates") if isinstance(registry, Mapping) else None
    if not isinstance(candidates, list):
        raise AdaptiveShadowControllerError("shadow activation registry is invalid")
    values = tuple(str(row.get("candidate_id")) for row in candidates if isinstance(row, Mapping))
    if len(values) != 2 or set(values) != set(CHALLENGER_IDS):
        raise AdaptiveShadowControllerError("shadow candidate registry differs from 5.9")
    return CHALLENGER_IDS


def _verify_shadow_decision(payload: Mapping[str, Any], decision: _Decision) -> None:
    common = {"formal_decision_record_sha256": decision.record_sha256,
              "signal_date": decision.signal_date, "trade_date": decision.trade_date,
              "offset": decision.offset,
              "admission_deadline_utc": decision.plan["admission_deadline_utc"]}
    if any(payload.get(name) != value for name, value in common.items()):
        raise AdaptiveShadowControllerError("stored shadow decision differs from its formal decision")
    if payload.get("plan_type") == "adaptive_shadow_target":
        expected = {"formal_route_target_plan_sha256": decision.plan["route_target_plan_sha256"],
                    "source_data_snapshot_sha256": decision.plan["source_data_snapshot_sha256"],
                    "formal_input_snapshot_sha256": decision.route["input_snapshot_sha256"]}
        if any(payload.get(name) != value for name, value in expected.items()):
            raise AdaptiveShadowControllerError("stored shadow plan has different formal/source bindings")


def _shadow_metadata(records: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("record_sha256")): dict(row) for row in records
            if isinstance(row.get("record_sha256"), str)}


def _validate_shadow_state(records: Sequence[Mapping[str, Any]], state: Any,
                           decisions: Sequence[_Decision],
                           outcomes: Mapping[str, tuple[dict[str, Any], CycleOutcome]],
                           *,
                           project_root: Path,
                           shadow_root: Path) -> None:
    """Cross-audit already sealed shadow decisions/outcomes against formal evidence."""
    known = {decision.record_sha256: decision for decision in decisions}
    metadata = _shadow_metadata(records)
    for decision_sha, (_record_sha, _payload_sha, intent, _recorded) in (
        state.planning_intents.items()
    ):
        decision = known.get(str(decision_sha))
        if decision is None:
            raise AdaptiveShadowControllerError(
                "stored planning intent binds no formal decision"
            )
        expected = {
            "formal_decision_record_sha256": decision.record_sha256,
            "signal_date": decision.signal_date,
            "trade_date": decision.trade_date,
            "offset": decision.offset,
            "admission_deadline_utc": decision.plan["admission_deadline_utc"],
        }
        if any(intent.get(name) != value for name, value in expected.items()):
            raise AdaptiveShadowControllerError(
                "stored planning intent differs from its formal decision"
            )
        plans = intent.get("ordered_plan_payloads")
        if not isinstance(plans, list) or not plans:
            raise AdaptiveShadowControllerError("stored planning intent has no plans")
        for plan in plans:
            if not isinstance(plan, Mapping):
                raise AdaptiveShadowControllerError(
                    "stored planning intent contains an invalid plan"
                )
            _verify_shadow_decision(plan, decision)
    for _key, (_record_sha, _payload_sha, payload) in state.plans.items():
        decision = known.get(str(payload.get("formal_decision_record_sha256")))
        if decision is None:
            raise AdaptiveShadowControllerError("stored shadow plan binds no formal decision")
        _verify_shadow_decision(payload, decision)
    for _key, record_sha in state.missed.items():
        record = metadata.get(str(record_sha))
        payload = record.get("payload") if record is not None else None
        if not isinstance(payload, Mapping):
            raise AdaptiveShadowControllerError("stored missed record is unavailable")
        decision = known.get(str(payload.get("formal_decision_record_sha256")))
        if decision is None:
            raise AdaptiveShadowControllerError("stored missed record binds no formal decision")
        _verify_shadow_decision(payload, decision)
    replay_accounts: dict[tuple[str, int], SleeveAccountState] = {}
    for record in records:
        envelope, payload = record.get("record"), record.get("payload")
        if not isinstance(envelope, Mapping) or envelope.get("kind") != "outcome":
            continue
        if not isinstance(payload, Mapping):
            raise AdaptiveShadowControllerError("stored shadow outcome is invalid")
        plan = state.plans_by_record.get(str(payload.get("plan_record_sha256")))
        if plan is None:
            raise AdaptiveShadowControllerError("stored shadow outcome binds no plan")
        formal = outcomes.get(str(plan.get("formal_decision_record_sha256")))
        if formal is None:
            raise AdaptiveShadowControllerError("shadow outcome precedes its formal rich outcome")
        try:
            cycle = ShadowCycleOutcome.from_mapping(payload["cycle_outcome"])
        except Exception as exc:
            raise AdaptiveShadowControllerError("stored shadow cycle outcome is invalid") from exc
        formal_cycle = formal[1]
        if (payload.get("formal_execution_snapshot_sha256")
                != formal_cycle.execution_snapshot_sha256
                or cycle.holding_start_date != formal_cycle.holding_start_date
                or cycle.holding_end_date != formal_cycle.holding_end_date
                or cycle.observation_available_at_utc != formal_cycle.observation_available_at_utc):
            raise AdaptiveShadowControllerError("shadow outcome differs from formal execution evidence")
        decision = known[str(plan["formal_decision_record_sha256"])]
        try:
            generation = GenerationResult.from_mapping(decision.route)
        except Exception as exc:
            raise AdaptiveShadowControllerError(
                "stored shadow outcome has an invalid formal generation"
            ) from exc
        execution_plan = _execution_plan(
            plan,
            str(state.execution_plan_sha_by_record.get(str(payload["plan_record_sha256"]), "")),
        )
        account_key = (cycle.candidate_id, cycle.offset)
        previous_shadow = replay_accounts.get(account_key)
        if previous_shadow is None:
            previous_shadow = genesis_shadow_account(execution_plan)
        previous_formal = _formal_previous_state(
            _OutcomeWork(
                decision,
                cycle.candidate_id,
                str(payload["plan_record_sha256"]),
                dict(plan),
                formal[0],
                formal_cycle,
            ),
            decisions,
            outcomes,
            generation,
        )
        bindings = {
            name: plan[name]
            for name in (
                "source_data_snapshot_sha256",
                "shadow_target_rows_sha256",
                "formal_route_target_plan_sha256",
            )
        }
        bindings["plan_record_sha256"] = str(payload["plan_record_sha256"])
        bundle_path = (
            shadow_root
            / "market-windows"
            / str(payload.get("shadow_market_bundle_sha256") or "")
        )
        try:
            loaded = load_adaptive_shadow_execution_snapshot(
                bundle_path,
                generation,
                execution_plan,
                previous_shadow,
                plan_bindings=bindings,
                previous_formal_account_state=previous_formal,
            )
            replayed = evaluate_shadow_cycle(
                execution_plan,
                loaded.snapshot,
                previous_shadow,
            )
        except Exception as exc:
            raise AdaptiveShadowControllerError(
                "shadow market sources or cycle outcome failed independent replay"
            ) from exc
        if (
            loaded.source_contract.get("formal_execution_snapshot_sha256")
            != formal_cycle.execution_snapshot_sha256
            or loaded.bundle_sha256 != payload.get("shadow_market_bundle_sha256")
            or loaded.source_contract_sha256
            != payload.get("shadow_market_source_contract_sha256")
            or canonical_json_bytes(replayed.to_dict())
            != canonical_json_bytes(cycle.to_dict())
        ):
            raise AdaptiveShadowControllerError(
                "stored shadow outcome differs from its independent deep replay"
            )
        replay_accounts[account_key] = replayed.next_account_state


def _decided(state: Any, records_by_sha: Mapping[str, Mapping[str, Any]],
             decision: _Decision, candidate_id: str) -> bool:
    key = (candidate_id, decision.signal_date, decision.offset)
    plan, missed_sha = state.plans.get(key), state.missed.get(key)
    if plan is not None:
        _verify_shadow_decision(plan[2], decision)
    if missed_sha is not None:
        metadata = records_by_sha.get(str(missed_sha))
        if metadata is None or not isinstance(metadata.get("payload"), Mapping):
            raise AdaptiveShadowControllerError("stored missed record is unavailable")
        _verify_shadow_decision(metadata["payload"], decision)
    if plan is not None and missed_sha is not None:
        raise AdaptiveShadowControllerError("shadow identity is both planned and missed")
    return plan is not None or missed_sha is not None


def _sealed_plan_path(root: Path, decision: _Decision) -> Path:
    plan_root = root / "plans"
    if not plan_root.is_dir() or plan_root.is_symlink():
        raise AdaptiveShadowControllerError("formal sealed-plan directory is invalid")
    matches = [path for path in plan_root.iterdir()
               if path.name.endswith(f"-{decision.plan_sha256}.json")
               and path.is_file() and not path.is_symlink()]
    if len(matches) != 1:
        raise AdaptiveShadowControllerError("formal decision must have exactly one sealed plan file")
    raw = matches[0].read_bytes()
    if hashlib.sha256(raw).hexdigest() != decision.plan_sha256 or raw != canonical_json_bytes(decision.plan):
        raise AdaptiveShadowControllerError("sealed formal plan bytes differ")
    return matches[0]


def _plan_stage(project_root: Path, formal_root: Path, shadow_root: Path,
                decision: _Decision, observed: str) -> dict[str, Any]:
    source_sha = decision.plan.get("source_data_snapshot_sha256")
    if not isinstance(source_sha, str):
        raise AdaptiveShadowControllerError("formal source snapshot SHA is invalid")
    input_path = formal_root / "inputs" / source_sha
    if input_path.is_symlink() or not input_path.is_dir():
        raise AdaptiveShadowControllerError("formal input CAS is missing")
    result = plan_shadow_runtime(
        project_root, shadow_root, formal_root,
        formal_plan_path=_sealed_plan_path(formal_root, decision),
        formal_decision_record_sha256=decision.record_sha256,
        input_snapshot_path=input_path, created_at_utc=observed)
    status = str(result.get("status"))
    if status not in {"planned", "missed"}:
        raise AdaptiveShadowControllerError("shadow planner returned an invalid status")
    return {"schema_version": CONTROLLER_SCHEMA_VERSION, "status": status,
            "reason": "shadow_decision_advanced", "action": "missed" if status == "missed" else "plan",
            "observed_at_utc": observed, "formal_decision_record_sha256": decision.record_sha256,
            "result": result}


def _execution_plan(payload: Mapping[str, Any], expected_sha: str) -> ShadowCyclePlan:
    try:
        plan = ShadowCyclePlan(
            registry_sha256=str(payload["registry_sha256"]), candidate_id=str(payload["candidate_id"]),
            candidate_sha256=str(payload["candidate_sha256"]), offset=int(payload["offset"]),
            signal_date=str(payload["signal_date"]), trade_date=str(payload["trade_date"]),
            targets_ppm=payload["targets_ppm"],
            formal_input_snapshot_sha256=str(payload["formal_input_snapshot_sha256"]),
            formal_decision_record_sha256=str(payload["formal_decision_record_sha256"]),
            planned_at_utc=str(payload["created_at_utc"]),
            formal_trade_deadline_utc=str(payload["admission_deadline_utc"]))
    except Exception as exc:
        raise AdaptiveShadowControllerError("stored shadow plan is not executable") from exc
    if plan.plan_sha256 != expected_sha:
        raise AdaptiveShadowControllerError("stored shadow execution-plan SHA differs")
    return plan


def _outcome_work(decisions: Sequence[_Decision],
                  outcomes: Mapping[str, tuple[dict[str, Any], CycleOutcome]], state: Any,
                  candidates: Sequence[str]) -> _OutcomeWork | None:
    work, order = [], {value: index for index, value in enumerate(candidates)}
    for decision in decisions:
        formal = outcomes.get(decision.record_sha256)
        if formal is None:
            continue
        for candidate in candidates:
            stored = state.plans.get((candidate, decision.signal_date, decision.offset))
            if stored is None:
                continue
            _verify_shadow_decision(stored[2], decision)
            record_sha = str(stored[0])
            if record_sha not in state.outcomes:
                work.append(_OutcomeWork(decision, candidate, record_sha, dict(stored[2]), formal[0], formal[1]))
    work.sort(key=lambda row: (row.decision.sequence, order[row.candidate_id]))
    return work[0] if work else None


def _formal_previous_state(work: _OutcomeWork, decisions: Sequence[_Decision],
                           outcomes: Mapping[str, tuple[dict[str, Any], CycleOutcome]],
                           generation: GenerationResult) -> SleeveAccountState:
    prior = [(decision.sequence, outcomes[decision.record_sha256][1]) for decision in decisions
             if decision.sequence < work.decision.sequence and decision.offset == work.decision.offset
             and decision.record_sha256 in outcomes]
    if prior:
        return max(prior, key=lambda row: row[0])[1].next_account_state
    return SleeveAccountState.genesis(deployment_sha256=generation.deployment_sha256,
                                      offset=work.decision.offset)


def _advance_outcome(project_root: Path, formal_root: Path, shadow_root: Path, observed: str,
                     work: _OutcomeWork, decisions: Sequence[_Decision],
                     outcomes: Mapping[str, tuple[dict[str, Any], CycleOutcome]], state: Any) -> dict[str, Any]:
    cycle = work.formal_cycle
    if _utc_value(observed) < _utc_value(cycle.observation_available_at_utc):
        return _waiting("shadow_outcome_not_available", observed,
                        formal_decision_record_sha256=work.decision.record_sha256,
                        candidate_id=work.candidate_id)
    try:
        generation = GenerationResult.from_mapping(work.decision.route)
    except Exception as exc:
        raise AdaptiveShadowControllerError("formal generation result is invalid") from exc
    if (generation.result_sha256 != cycle.generation_result_sha256
            or generation.signal_date != cycle.signal_date
            or generation.trade_date != cycle.holding_start_date
            or generation.due_offset != cycle.offset):
        raise AdaptiveShadowControllerError("formal outcome differs from its decision")
    plan = _execution_plan(work.plan_payload,
                           str(state.execution_plan_sha_by_record.get(work.plan_record_sha256, "")))
    prior_payload = state.latest_account_states.get((work.candidate_id, plan.offset))
    prior_shadow = genesis_shadow_account(plan) if prior_payload is None else SleeveAccountState.from_mapping(prior_payload)
    prior_formal = _formal_previous_state(work, decisions, outcomes, generation)
    if cycle.previous_account_state_sha256 != prior_formal.state_sha256:
        raise AdaptiveShadowControllerError("formal outcome does not continue the prior formal account")
    execution_path = formal_root / "executions" / cycle.execution_snapshot_sha256
    if execution_path.is_symlink() or not execution_path.is_dir():
        raise AdaptiveShadowControllerError("formal execution CAS is missing")
    bindings = {name: work.plan_payload[name] for name in (
        "source_data_snapshot_sha256", "shadow_target_rows_sha256",
        "formal_route_target_plan_sha256")}
    bindings["plan_record_sha256"] = work.plan_record_sha256
    built = build_adaptive_shadow_execution_snapshot(
        project_root, generation, execution_path, plan, prior_shadow,
        plan_bindings=bindings, previous_formal_account_state=prior_formal)
    outcome = evaluate_shadow_cycle(plan, built.snapshot, prior_shadow)
    if (built.source_contract.get("formal_execution_snapshot_sha256")
            != cycle.execution_snapshot_sha256
            or outcome.shadow_execution_snapshot_sha256 != built.snapshot.snapshot_sha256
            or outcome.market_execution_snapshot_sha256
            != built.snapshot.execution_snapshot.snapshot_sha256
            or outcome.holding_end_date != cycle.holding_end_date
            or outcome.observation_available_at_utc != cycle.observation_available_at_utc):
        raise AdaptiveShadowControllerError("shadow outcome differs from the sealed formal market window")
    appended = _shadow_store.append_shadow_outcome(
        shadow_root, {"schema_version": 1, "outcome_type": "adaptive_shadow_outcome",
                      "plan_record_sha256": work.plan_record_sha256,
                      "formal_execution_snapshot_sha256": cycle.execution_snapshot_sha256,
                      "shadow_market_source_contract_sha256": built.source_contract_sha256,
                      "shadow_market_bundle_sha256": built.bundle_sha256,
                      "cycle_outcome": outcome.to_dict()}, recorded_at_utc=observed)
    return {"schema_version": CONTROLLER_SCHEMA_VERSION, "status": "advanced",
            "reason": "shadow_outcome_appended", "action": "outcome", "observed_at_utc": observed,
            "candidate_id": work.candidate_id, "offset": work.decision.offset,
            "signal_date": work.decision.signal_date,
            "formal_decision_record_sha256": work.decision.record_sha256,
            "shadow_market_bundle_sha256": built.bundle_sha256, "outcome": appended}


def advance_adaptive_shadow(project_root: str | Path, formal_ledger_root: str | Path,
                            shadow_root: str | Path,
                            observed_at_utc: str | datetime | None = None) -> dict[str, Any]:
    """Advance at most one planning/missed/outcome stage, using local sealed bytes."""
    observed = _utc_text(observed_at_utc)
    shadow = _shadow_snapshot(shadow_root)
    if shadow is None:
        return _waiting("shadow_not_activated", observed)
    shadow_records, state = shadow
    activation = dict(state.activation)
    formal_records, formal_head = _audited_formal_records(formal_ledger_root)
    decisions, outcomes = _formal_view(formal_records, str(activation["formal_head_record_sha256"]))
    candidates, shadow_by_sha = _candidate_ids(activation), _shadow_metadata(shadow_records)
    roots = (Path(project_root).expanduser().resolve(),
             Path(formal_ledger_root).expanduser().resolve(), Path(shadow_root).expanduser().resolve())
    _validate_shadow_state(
        shadow_records,
        state,
        decisions,
        outcomes,
        project_root=roots[0],
        shadow_root=roots[2],
    )
    for decision in decisions:
        if any(not _decided(state, shadow_by_sha, decision, candidate) for candidate in candidates):
            return _plan_stage(*roots, decision, observed)
    work = _outcome_work(decisions, outcomes, state, candidates)
    if work is not None:
        return _advance_outcome(*roots, observed, work, decisions, outcomes, state)
    return _waiting("no_shadow_action_due", observed,
                    formal_head_record_sha256=formal_head,
                    shadow_head_record_sha256=(shadow_records[-1]["record_sha256"]
                                               if shadow_records else None))


def audit_adaptive_shadow_runtime(
    project_root: str | Path,
    formal_ledger_root: str | Path,
    shadow_root: str | Path,
) -> dict[str, Any]:
    """Structurally audit both chains and deep-replay every shadow market source."""

    roots = (
        Path(project_root).expanduser().resolve(),
        Path(formal_ledger_root).expanduser().resolve(),
        Path(shadow_root).expanduser().resolve(),
    )
    shadow = _shadow_snapshot(roots[2])
    if shadow is None:
        structural = _shadow_store.audit_shadow_store(roots[2])
        return {
            **structural,
            "external_replay_valid": False,
            "valid": False,
            "reason": "shadow_not_activated",
        }
    shadow_records, state = shadow
    activation = dict(state.activation)
    formal_records, formal_head = _audited_formal_records(roots[1])
    decisions, outcomes = _formal_view(
        formal_records,
        str(activation["formal_head_record_sha256"]),
    )
    _candidate_ids(activation)
    _validate_shadow_state(
        shadow_records,
        state,
        decisions,
        outcomes,
        project_root=roots[0],
        shadow_root=roots[2],
    )
    structural = _shadow_store.audit_shadow_store(roots[2])
    _formal_records_after, formal_head_after = _audited_formal_records(roots[1])
    shadow_head = shadow_records[-1]["record_sha256"]
    if (
        formal_head_after != formal_head
        or structural.get("head_record_sha256") != shadow_head
    ):
        raise AdaptiveShadowControllerError(
            "formal or shadow evidence changed during independent audit"
        )
    return {
        **structural,
        "external_replay_valid": True,
        "valid": True,
        "formal_head_record_sha256": formal_head,
        "shadow_head_record_sha256": shadow_head,
        "deep_replayed_outcome_count": len(state.outcomes),
    }


__all__ = [
    "AdaptiveShadowControllerError",
    "CONTROLLER_SCHEMA_VERSION",
    "advance_adaptive_shadow",
    "audit_adaptive_shadow_runtime",
]
