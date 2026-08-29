"""Source-backed, route-neutral market windows for 5.9 adaptive shadow.

The common path wraps an independently verified formal execution snapshot
without changing a row, calendar session, or benchmark member.  The exceptional
path is only for a shadow holding that has left the current formal row universe;
it rebuilds that ticker from the *same* sealed formal sources and references
already-existing raw CAS bytes for causal ADV/volatility history.

Only compact shadow manifests and snapshots are materialised.  Formal artifacts,
raw partitions, and their shared CAS are read-only inputs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

import pandas as pd

from ..adaptive_shadow import canonical_sha256
from ..adaptive_shadow_execution import (
    ShadowCyclePlan,
    ShadowExecutionSnapshot,
)
from ..prospective_execution import ExecutionObservation, ExecutionSnapshot, SleeveAccountState
from ..prospective_targets import GenerationResult
from . import prospective_execution as _formal_data
from .catalog import sha256_file
from .prospective import (
    IMMUTABLE_SOURCE_RELATIVE_ROOT,
    PROSPECTIVE_RELATIVE_ROOT,
)
from .sources import (
    PROVIDER_COMPLETION_DATASETS,
    provider_completion_required,
)


SCHEMA_VERSION = 1
KIND = "adaptive_shadow_market_sources"
SHADOW_MARKET_RELATIVE_ROOT = Path("runtime/adaptive-shadow/1/market-windows")

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_KEYS = frozenset(
    {
        "schema_version",
        "kind",
        "mode",
        "target_plan_sha256",
        "registry_sha256",
        "candidate_id",
        "candidate_sha256",
        "formal_generation_result_sha256",
        "formal_execution_snapshot_sha256",
        "formal_execution_source_sha256",
        "formal_execution_bundle",
        "formal_input_snapshot_sha256",
        "formal_decision_record_sha256",
        "plan_record_sha256",
        "source_data_snapshot_sha256",
        "shadow_target_rows_sha256",
        "formal_route_target_plan_sha256",
        "previous_shadow_account_state_sha256",
        "benchmark_tickers_sha256",
        "required_tickers_sha256",
        "formal_rows_sha256",
        "supplemented_tickers",
        "selected_market_max_date",
        "decision_input",
        "calendar",
        "raw_partitions",
        "fallback_raw_partitions",
        "suspensions",
        "formal_delists",
        "delists",
    }
)
_PLAN_BINDING_KEYS = frozenset(
    {
        "plan_record_sha256",
        "source_data_snapshot_sha256",
        "shadow_target_rows_sha256",
        "formal_route_target_plan_sha256",
    }
)


class AdaptiveShadowExecutionDataError(ValueError):
    """Raised when shadow market evidence cannot be replayed from sealed bytes."""


@dataclass(frozen=True)
class AdaptiveShadowExecutionDataSnapshot:
    snapshot: ShadowExecutionSnapshot
    directory: Path
    snapshot_path: Path
    sources_path: Path
    source_contract: Mapping[str, Any]
    bundle_sha256: str

    @property
    def snapshot_sha256(self) -> str:
        return self.snapshot.snapshot_sha256

    @property
    def source_contract_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.source_contract))


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha(value: Any, label: str) -> str:
    result = str(value or "")
    if not _SHA256_RE.fullmatch(result):
        raise AdaptiveShadowExecutionDataError(f"{label} must be a lowercase SHA-256")
    return result


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise AdaptiveShadowExecutionDataError("shadow source escapes project root") from exc


def _generation(value: GenerationResult | Mapping[str, Any]) -> GenerationResult:
    try:
        return _formal_data._generation(value)
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError("invalid formal generation result") from exc


def _plan(value: ShadowCyclePlan | Mapping[str, Any]) -> ShadowCyclePlan:
    if isinstance(value, ShadowCyclePlan):
        return value
    if isinstance(value, Mapping):
        try:
            return ShadowCyclePlan.from_mapping(value)
        except Exception as exc:
            raise AdaptiveShadowExecutionDataError("invalid shadow cycle plan") from exc
    raise AdaptiveShadowExecutionDataError("shadow_plan must be a strict ShadowCyclePlan")


def _plan_bindings(value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != _PLAN_BINDING_KEYS:
        raise AdaptiveShadowExecutionDataError(
            "plan_bindings must contain the exact stored-plan source identities"
        )
    return {key: _sha(value[key], key) for key in sorted(_PLAN_BINDING_KEYS)}


def _account(value: SleeveAccountState | Mapping[str, Any]) -> SleeveAccountState:
    try:
        return _formal_data._account_state(value)
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError("invalid previous shadow account state") from exc


def _formal_evidence(
    root: Path,
    value: _formal_data.ProspectiveExecutionDataSnapshot
    | ExecutionSnapshot
    | Mapping[str, Any]
    | str
    | Path,
    generation: GenerationResult,
    previous_formal_account_state: SleeveAccountState | Mapping[str, Any] | None,
) -> tuple[
    ExecutionSnapshot,
    Mapping[str, Any],
    Mapping[str, Any],
]:
    if isinstance(value, _formal_data.ProspectiveExecutionDataSnapshot):
        bundle_path: str | Path = value.directory
    elif isinstance(value, (str, Path)):
        bundle_path = value
    else:
        raise AdaptiveShadowExecutionDataError(
            "formal_execution must be a canonical source-backed bundle"
        )
    try:
        loaded = _formal_data.load_prospective_execution_snapshot(
            bundle_path,
            generation,
            previous_account_state=previous_formal_account_state,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "formal execution bundle failed independent source replay"
        ) from exc
    expected = (
        root
        / _formal_data.EXECUTION_RELATIVE_ROOT
        / loaded.snapshot.snapshot_sha256
    ).resolve()
    if loaded.directory != expected:
        raise AdaptiveShadowExecutionDataError(
            "formal execution bundle is outside the requested project root"
        )
    bundle = {
        "path": _relative(loaded.directory, root),
        "snapshot_sha256": loaded.snapshot.snapshot_sha256,
        "source_contract_sha256": loaded.snapshot.execution_source_sha256,
    }
    return loaded.snapshot, loaded.source_contract, bundle


def _formal_rows_sha(snapshot: ExecutionSnapshot) -> str:
    return canonical_sha256([row.to_dict() for row in snapshot.rows])


def _clone_execution_snapshot(
    snapshot: ExecutionSnapshot,
    *,
    execution_source_sha256: str,
    rows: Sequence[ExecutionObservation],
) -> ExecutionSnapshot:
    return ExecutionSnapshot(
        generation_result_sha256=snapshot.generation_result_sha256,
        execution_source_sha256=execution_source_sha256,
        official_calendar_sha256=snapshot.official_calendar_sha256,
        signal_date=snapshot.signal_date,
        holding_start_date=snapshot.holding_start_date,
        holding_end_date=snapshot.holding_end_date,
        calendar_sessions=snapshot.calendar_sessions,
        benchmark_tickers=snapshot.benchmark_tickers,
        benchmark_tickers_sha256=snapshot.benchmark_tickers_sha256,
        rows=tuple(rows),
        calendar_available_at_utc=snapshot.calendar_available_at_utc,
        decision_inputs_available_at_utc=snapshot.decision_inputs_available_at_utc,
        trade_deadline_utc=snapshot.trade_deadline_utc,
        start_open_available_at_utc=snapshot.start_open_available_at_utc,
        end_open_available_at_utc=snapshot.end_open_available_at_utc,
        observation_available_at_utc=snapshot.observation_available_at_utc,
    )


def _bundle_sha(snapshot_sha: str, source_sha: str) -> str:
    return canonical_sha256(
        {
            "shadow_execution_snapshot_sha256": snapshot_sha,
            "shadow_market_source_contract_sha256": source_sha,
        }
    )


def _base_contract(
    generation: GenerationResult,
    formal: ExecutionSnapshot,
    plan: ShadowCyclePlan,
    previous: SleeveAccountState,
    plan_bindings: Mapping[str, str],
    formal_bundle: Mapping[str, Any],
    *,
    mode: str,
    supplemented_tickers: Sequence[str],
) -> dict[str, Any]:
    required = sorted(
        set(dict(plan.targets_ppm)) | {row.ticker for row in previous.positions}
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "mode": mode,
        "target_plan_sha256": plan.plan_sha256,
        "registry_sha256": plan.registry_sha256,
        "candidate_id": plan.candidate_id,
        "candidate_sha256": plan.candidate_sha256,
        "formal_generation_result_sha256": generation.result_sha256,
        "formal_execution_snapshot_sha256": formal.snapshot_sha256,
        "formal_execution_source_sha256": formal.execution_source_sha256,
        "formal_execution_bundle": dict(formal_bundle),
        "formal_input_snapshot_sha256": plan.formal_input_snapshot_sha256,
        "formal_decision_record_sha256": plan.formal_decision_record_sha256,
        "plan_record_sha256": plan_bindings["plan_record_sha256"],
        "source_data_snapshot_sha256": plan_bindings[
            "source_data_snapshot_sha256"
        ],
        "shadow_target_rows_sha256": plan_bindings[
            "shadow_target_rows_sha256"
        ],
        "formal_route_target_plan_sha256": plan_bindings[
            "formal_route_target_plan_sha256"
        ],
        "previous_shadow_account_state_sha256": previous.state_sha256,
        "benchmark_tickers_sha256": formal.benchmark_tickers_sha256,
        "required_tickers_sha256": canonical_sha256(required),
        "formal_rows_sha256": _formal_rows_sha(formal),
        "supplemented_tickers": list(sorted(supplemented_tickers)),
        "selected_market_max_date": formal.holding_end_date,
        "decision_input": None,
        "calendar": None,
        "raw_partitions": [],
        "fallback_raw_partitions": [],
        "suspensions": None,
        "formal_delists": None,
        "delists": None,
    }


def _validate_bindings(
    generation: GenerationResult,
    formal: ExecutionSnapshot,
    plan: ShadowCyclePlan,
    previous: SleeveAccountState,
) -> None:
    if generation.result_sha256 != formal.generation_result_sha256:
        raise AdaptiveShadowExecutionDataError(
            "formal generation and execution snapshot identities differ"
        )
    if generation.input_snapshot_sha256 != plan.formal_input_snapshot_sha256:
        raise AdaptiveShadowExecutionDataError("shadow plan binds another formal input")
    if (
        generation.signal_date != formal.signal_date
        or generation.trade_date != formal.holding_start_date
        or plan.signal_date != generation.signal_date
        or plan.trade_date != generation.trade_date
    ):
        raise AdaptiveShadowExecutionDataError("formal and shadow cycle dates differ")
    if (
        generation.calendar_index >= len(formal.calendar_sessions)
        or formal.calendar_sessions[generation.calendar_index] != generation.signal_date
        or generation.due_offset != generation.calendar_index % 10
        or plan.offset != generation.due_offset
    ):
        raise AdaptiveShadowExecutionDataError("formal/shadow offset or calendar boundary differs")
    if plan.formal_trade_deadline_utc != formal.trade_deadline_utc:
        raise AdaptiveShadowExecutionDataError("shadow plan and formal trade deadlines differ")
    if previous.deployment_sha256 != plan.account_deployment_sha256:
        raise AdaptiveShadowExecutionDataError("previous shadow state uses another candidate")
    if previous.offset != plan.offset:
        raise AdaptiveShadowExecutionDataError("previous shadow state uses another offset")
    if previous.cycle_count > 0 and previous.last_holding_end_date != formal.holding_start_date:
        raise AdaptiveShadowExecutionDataError("previous shadow state is not boundary-continuous")
    targets = set(dict(plan.targets_ppm))
    if not targets.issubset(set(formal.benchmark_tickers)):
        raise AdaptiveShadowExecutionDataError(
            "shadow targets must remain inside the formal decision-time benchmark"
        )


def _materialize_bundle(
    root: Path,
    wrapper: ShadowExecutionSnapshot,
    source_contract: Mapping[str, Any],
    *,
    materialize: bool,
) -> AdaptiveShadowExecutionDataSnapshot:
    source_bytes = _canonical_json_bytes(source_contract)
    source_sha = _sha256_bytes(source_bytes)
    bundle_sha = _bundle_sha(wrapper.snapshot_sha256, source_sha)
    directory = (root / SHADOW_MARKET_RELATIVE_ROOT / bundle_sha).resolve()
    snapshot_path = directory / "snapshot.json"
    sources_path = directory / "sources.json"
    if materialize:
        try:
            _formal_data._write_create_only(sources_path, source_bytes)
            _formal_data._write_create_only(
                snapshot_path,
                _canonical_json_bytes(wrapper.to_dict()),
            )
        except Exception as exc:
            raise AdaptiveShadowExecutionDataError(
                "shadow market bundle failed create-only publication"
            ) from exc
    return AdaptiveShadowExecutionDataSnapshot(
        snapshot=wrapper,
        directory=directory,
        snapshot_path=snapshot_path,
        sources_path=sources_path,
        source_contract=dict(source_contract),
        bundle_sha256=bundle_sha,
    )


def build_adaptive_shadow_execution_snapshot(
    project_root: str | Path,
    formal_generation_result: GenerationResult | Mapping[str, Any],
    formal_execution: _formal_data.ProspectiveExecutionDataSnapshot
    | ExecutionSnapshot
    | Mapping[str, Any]
    | str
    | Path,
    shadow_plan: ShadowCyclePlan | Mapping[str, Any],
    previous_shadow_account_state: SleeveAccountState | Mapping[str, Any],
    *,
    plan_bindings: Mapping[str, Any],
    previous_formal_account_state: SleeveAccountState | Mapping[str, Any] | None = None,
    _materialize: bool = True,
    _sealed_source_contract: Mapping[str, Any] | None = None,
) -> AdaptiveShadowExecutionDataSnapshot:
    """Build a create-only shadow wrapper, supplementing only missing priors."""

    root = Path(project_root).expanduser().resolve()
    generation = _generation(formal_generation_result)
    formal, formal_sources, formal_bundle = _formal_evidence(
        root,
        formal_execution,
        generation,
        previous_formal_account_state,
    )
    plan = _plan(shadow_plan)
    previous = _account(previous_shadow_account_state)
    bindings = _plan_bindings(plan_bindings)
    if (
        formal_sources is not None
        and formal_sources.get("source_data_snapshot_sha256")
        != bindings["source_data_snapshot_sha256"]
    ):
        raise AdaptiveShadowExecutionDataError(
            "stored shadow plan binds another source-data snapshot"
        )
    _validate_bindings(generation, formal, plan, previous)
    required = set(dict(plan.targets_ppm)) | {row.ticker for row in previous.positions}
    formal_row_tickers = {row.ticker for row in formal.rows}
    missing = tuple(sorted(required - formal_row_tickers))

    if not missing:
        source_contract = _base_contract(
            generation,
            formal,
            plan,
            previous,
            bindings,
            formal_bundle,
            mode="formal_snapshot_reuse",
            supplemented_tickers=(),
        )
        if _sealed_source_contract is not None and dict(_sealed_source_contract) != source_contract:
            raise AdaptiveShadowExecutionDataError(
                "sealed fast-path source contract differs from deterministic replay"
            )
        source_sha = _sha256_bytes(_canonical_json_bytes(source_contract))
        try:
            shadow_market = _clone_execution_snapshot(
                formal,
                execution_source_sha256=source_sha,
                rows=formal.rows,
            )
            wrapper = ShadowExecutionSnapshot(
                target_plan_sha256=plan.plan_sha256,
                formal_input_snapshot_sha256=plan.formal_input_snapshot_sha256,
                formal_decision_record_sha256=plan.formal_decision_record_sha256,
                execution_snapshot=shadow_market,
            )
        except Exception as exc:
            raise AdaptiveShadowExecutionDataError(
                "pure shadow execution snapshot rejected formal rows"
            ) from exc
        return _materialize_bundle(
            root,
            wrapper,
            source_contract,
            materialize=_materialize,
        )

    return _build_supplemented_bundle(
        root,
        generation,
        formal,
        formal_sources,
        plan,
        previous,
        bindings,
        formal_bundle,
        missing,
        materialize=_materialize,
        sealed_source_contract=_sealed_source_contract,
    )


def _build_supplemented_bundle(
    root: Path,
    generation: GenerationResult,
    formal: ExecutionSnapshot,
    formal_sources: Mapping[str, Any],
    plan: ShadowCyclePlan,
    previous: SleeveAccountState,
    plan_bindings: Mapping[str, str],
    formal_bundle: Mapping[str, Any],
    missing: Sequence[str],
    *,
    materialize: bool,
    sealed_source_contract: Mapping[str, Any] | None,
) -> AdaptiveShadowExecutionDataSnapshot:
    _validate_formal_source_contract(formal_sources, generation, formal)
    if sealed_source_contract is None:
        decision_input = formal_sources.get("decision_input")
        calendar_contract = formal_sources.get("calendar")
        raw_contracts = formal_sources.get("raw_partitions")
        suspension_contract = formal_sources.get("suspensions")
        formal_delists = formal_sources.get("delists")
        sealed_fallback: Sequence[Mapping[str, Any]] | None = None
        sealed_projected_delists: Mapping[str, Any] | None = None
    else:
        sealed = _strict_source_contract(sealed_source_contract)
        decision_input = sealed["decision_input"]
        calendar_contract = sealed["calendar"]
        raw_contracts = sealed["raw_partitions"]
        suspension_contract = sealed["suspensions"]
        formal_delists = sealed["formal_delists"]
        sealed_fallback = sealed["fallback_raw_partitions"]
        sealed_projected_delists = sealed["delists"]

        if (
            decision_input != formal_sources.get("decision_input")
            or calendar_contract != formal_sources.get("calendar")
            or raw_contracts != formal_sources.get("raw_partitions")
            or suspension_contract != formal_sources.get("suspensions")
            or formal_delists != formal_sources.get("delists")
        ):
            raise AdaptiveShadowExecutionDataError(
                "sealed shadow contract differs from its formal source contract"
            )

    if (
        not isinstance(decision_input, Mapping)
        or not isinstance(calendar_contract, Mapping)
        or not isinstance(raw_contracts, list)
        or not all(isinstance(item, Mapping) for item in raw_contracts)
        or not isinstance(suspension_contract, Mapping)
        or not isinstance(formal_delists, Mapping)
    ):
        raise AdaptiveShadowExecutionDataError(
            "formal execution source components are incomplete"
        )
    source_sha = _sha(
        decision_input.get("snapshot_sha256"),
        "formal decision source snapshot",
    )
    if source_sha != str(formal_sources.get("source_data_snapshot_sha256", source_sha)):
        raise AdaptiveShadowExecutionDataError("formal decision source identity differs")
    source_path = root / PROSPECTIVE_RELATIVE_ROOT / "inputs" / source_sha
    try:
        source = _formal_data.load_prospective_input_snapshot(source_path)
        _formal_data._verify_generation_input_binding(
            generation,
            source,
            deadline=formal.trade_deadline_utc,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "formal decision input failed independent replay"
        ) from exc

    deadline = _formal_data._utc(formal.trade_deadline_utc, label="trade deadline")
    try:
        sessions, window, rebuilt_calendar, _calendar_completed = (
            _formal_data._select_calendar(
                root,
                {},
                generation,
                source.calendar_sessions,
                deadline=deadline,
                sealed_sources=[calendar_contract],
            )
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "formal calendar CAS failed sealed shadow replay"
        ) from exc
    if (
        sessions != formal.calendar_sessions
        or tuple(window)
        != tuple(
            formal.calendar_sessions[
                generation.calendar_index + 1 : generation.calendar_index + 12
            ]
        )
        or rebuilt_calendar != dict(calendar_contract)
    ):
        raise AdaptiveShadowExecutionDataError(
            "formal calendar selection changed during shadow replay"
        )

    formal_raw_by_key = {
        str(item.get("checkpoint_key") or ""): item for item in raw_contracts
    }
    if (
        len(formal_raw_by_key) != len(raw_contracts)
        or "" in formal_raw_by_key
    ):
        raise AdaptiveShadowExecutionDataError(
            "formal raw source keys are invalid or duplicated"
        )
    daily_frames: dict[str, pd.DataFrame] = {}
    basic_frames: dict[str, pd.DataFrame] = {}
    adj_frames: dict[str, pd.DataFrame] = {}
    raw_completed: dict[str, list[pd.Timestamp]] = {session: [] for session in window}
    completion_bundle_by_session: dict[str, str] = {}
    for session in window:
        if provider_completion_required(session) or (
            f"daily_basic/{session}" in formal_raw_by_key
        ):
            required_datasets = PROVIDER_COMPLETION_DATASETS
        else:
            required_datasets = ("daily", "adj_factor")
        for dataset in required_datasets:
            key = f"{dataset}/{session}"
            sealed_raw = formal_raw_by_key.get(key)
            if not isinstance(sealed_raw, Mapping):
                raise AdaptiveShadowExecutionDataError(
                    f"formal source contract lacks holding-window CAS {key}"
                )
            try:
                frame, rebuilt_raw, completed = _formal_data._read_partition(
                    root,
                    {},
                    dataset=dataset,
                    trade_date=session,
                    availability_cap=None,
                    sealed_source=sealed_raw,
                )
            except Exception as exc:
                raise AdaptiveShadowExecutionDataError(
                    f"formal holding-window CAS failed replay for {key}"
                ) from exc
            if rebuilt_raw != dict(sealed_raw):
                raise AdaptiveShadowExecutionDataError(
                    f"formal holding-window source changed for {key}"
                )
            raw_completed[session].append(completed)
            if provider_completion_required(session):
                evidence = rebuilt_raw.get("provider_completion")
                if not isinstance(evidence, Mapping):
                    raise AdaptiveShadowExecutionDataError(
                        f"holding-window source lacks provider proof for {key}"
                    )
                bundle = str(evidence.get("evidence_sha256") or "")
                prior_bundle = completion_bundle_by_session.get(session)
                if prior_bundle is not None and prior_bundle != bundle:
                    raise AdaptiveShadowExecutionDataError(
                        f"holding-window provider proofs differ for {session}"
                    )
                completion_bundle_by_session[session] = bundle
            if dataset == "daily":
                daily_frames[session] = frame
            elif dataset == "daily_basic":
                basic_frames[session] = frame
            else:
                adj_frames[session] = frame
        if provider_completion_required(session):
            daily_tickers = set(daily_frames[session]["ts_code"].astype(str))
            basic_tickers = set(basic_frames[session]["ts_code"].astype(str))
            adj_tickers = set(adj_frames[session]["ts_code"].astype(str))
            if daily_tickers != basic_tickers or not daily_tickers.issubset(adj_tickers):
                raise AdaptiveShadowExecutionDataError(
                    f"holding-window provider universe differs for {session}"
                )

    decision_frame = _formal_data._source_rows(source)
    decision_tickers = set(decision_frame["ticker"].astype(str))
    fallback_tickers = set(missing) - decision_tickers
    fallback_inputs, fallback_sources = _fallback_inputs_from_existing_cas(
        root,
        calendar_sessions=sessions,
        signal_index=generation.calendar_index,
        tickers=fallback_tickers,
        deadline=deadline,
        formal_raw_sources=formal_raw_by_key,
        sealed_fallback_sources=sealed_fallback,
    )
    fallback_sources = tuple(
        sorted(fallback_sources, key=lambda row: (row["trade_date"], row["dataset"]))
    )

    end_completed = max(raw_completed[window[-1]])
    try:
        suspension_frame, rebuilt_suspensions, _suspension_completed = (
            _formal_data._load_suspensions(
                root,
                start=window[0],
                end=window[-1],
                availability_cap=_formal_data._utc(
                    formal.observation_available_at_utc,
                    label="formal observation availability",
                ),
                minimum_completed_at=end_completed,
                artifact_sha256=str(suspension_contract.get("artifact_sha256") or ""),
            )
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "formal suspension CAS failed shadow replay"
        ) from exc
    if rebuilt_suspensions != dict(suspension_contract):
        raise AdaptiveShadowExecutionDataError("formal suspension source changed")

    projection_tickers = (
        decision_tickers
        | set(formal.benchmark_tickers)
        | set(dict(plan.targets_ppm))
        | {row.ticker for row in previous.positions}
    )
    try:
        delists, projected_delists, _delist_completed = _formal_data._delist_dates(
            root,
            source,
            projection_tickers,
            sealed_source=formal_delists,
            allow_sealed_projection=True,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "formal delist CAS failed route-neutral projection"
        ) from exc
    if (
        sealed_projected_delists is not None
        and projected_delists != dict(sealed_projected_delists)
    ):
        raise AdaptiveShadowExecutionDataError(
            "sealed shadow delist projection changed"
        )

    try:
        benchmark, rebuilt_rows = _formal_data._build_route_neutral_observations(
            source,
            signal_date=plan.signal_date,
            benchmark_tickers=formal.benchmark_tickers,
            target_tickers=set(dict(plan.targets_ppm)),
            window=window,
            daily_frames=daily_frames,
            adj_frames=adj_frames,
            suspension_frame=suspension_frame,
            delists=delists,
            previous=previous,
            fallback_execution_inputs=fallback_inputs,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "route-neutral observation rebuild failed"
        ) from exc
    if benchmark != formal.benchmark_tickers:
        raise AdaptiveShadowExecutionDataError("shadow rebuild changed the formal benchmark")
    formal_by_key = {(row.date, row.ticker): row for row in formal.rows}
    rebuilt_by_key = {(row.date, row.ticker): row for row in rebuilt_rows}
    for key in sorted(set(formal_by_key) & set(rebuilt_by_key)):
        if formal_by_key[key].to_dict() != rebuilt_by_key[key].to_dict():
            raise AdaptiveShadowExecutionDataError(
                f"shadow rebuild changed a formal observation: {key[0]}/{key[1]}"
            )
    supplement = tuple(
        rebuilt_by_key[(session, ticker)]
        for session in window
        for ticker in sorted(missing)
    )
    if len(supplement) != len(window) * len(missing):
        raise AdaptiveShadowExecutionDataError(
            "supplemented prior rows do not form a complete holding rectangle"
        )
    combined = tuple(
        sorted((*formal.rows, *supplement), key=lambda row: (row.date, row.ticker))
    )

    source_contract = _base_contract(
        generation,
        formal,
        plan,
        previous,
        plan_bindings,
        formal_bundle,
        mode="supplemented_prior_holdings",
        supplemented_tickers=missing,
    )
    source_contract.update(
        {
            "decision_input": dict(decision_input),
            "calendar": dict(calendar_contract),
            "raw_partitions": [dict(item) for item in raw_contracts],
            "fallback_raw_partitions": [dict(item) for item in fallback_sources],
            "suspensions": dict(suspension_contract),
            "formal_delists": dict(formal_delists),
            "delists": projected_delists,
        }
    )
    if sealed_source_contract is not None and source_contract != dict(sealed_source_contract):
        raise AdaptiveShadowExecutionDataError(
            "sealed supplemented source contract differs from deterministic replay"
        )
    execution_source_sha = _sha256_bytes(_canonical_json_bytes(source_contract))
    try:
        shadow_market = ExecutionSnapshot(
            generation_result_sha256=generation.result_sha256,
            execution_source_sha256=execution_source_sha,
            official_calendar_sha256=formal.official_calendar_sha256,
            signal_date=formal.signal_date,
            holding_start_date=formal.holding_start_date,
            holding_end_date=formal.holding_end_date,
            calendar_sessions=formal.calendar_sessions,
            benchmark_tickers=formal.benchmark_tickers,
            benchmark_tickers_sha256=formal.benchmark_tickers_sha256,
            rows=combined,
            calendar_available_at_utc=formal.calendar_available_at_utc,
            decision_inputs_available_at_utc=formal.decision_inputs_available_at_utc,
            trade_deadline_utc=formal.trade_deadline_utc,
            start_open_available_at_utc=formal.start_open_available_at_utc,
            end_open_available_at_utc=formal.end_open_available_at_utc,
            observation_available_at_utc=formal.observation_available_at_utc,
        )
        wrapper = ShadowExecutionSnapshot(
            target_plan_sha256=plan.plan_sha256,
            formal_input_snapshot_sha256=plan.formal_input_snapshot_sha256,
            formal_decision_record_sha256=plan.formal_decision_record_sha256,
            execution_snapshot=shadow_market,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "pure shadow execution snapshot rejected source-backed rows"
        ) from exc
    return _materialize_bundle(
        root,
        wrapper,
        source_contract,
        materialize=materialize,
    )


def _strict_source_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    if (
        set(result) != _SOURCE_KEYS
        or type(result.get("schema_version")) is not int
        or result.get("schema_version") != SCHEMA_VERSION
        or result.get("kind") != KIND
    ):
        raise AdaptiveShadowExecutionDataError(
            "shadow market source contract has a non-exact schema"
        )
    for key in (
        "target_plan_sha256",
        "registry_sha256",
        "candidate_sha256",
        "formal_generation_result_sha256",
        "formal_execution_snapshot_sha256",
        "formal_execution_source_sha256",
        "formal_input_snapshot_sha256",
        "formal_decision_record_sha256",
        "plan_record_sha256",
        "source_data_snapshot_sha256",
        "shadow_target_rows_sha256",
        "formal_route_target_plan_sha256",
        "previous_shadow_account_state_sha256",
        "benchmark_tickers_sha256",
        "required_tickers_sha256",
        "formal_rows_sha256",
    ):
        _sha(result[key], key)
    if not isinstance(result.get("candidate_id"), str) or not result["candidate_id"]:
        raise AdaptiveShadowExecutionDataError("source candidate_id is invalid")
    formal_bundle = result.get("formal_execution_bundle")
    expected_formal_path = (
        _formal_data.EXECUTION_RELATIVE_ROOT
        / result["formal_execution_snapshot_sha256"]
    ).as_posix()
    if (
        not isinstance(formal_bundle, Mapping)
        or set(formal_bundle)
        != {"path", "snapshot_sha256", "source_contract_sha256"}
        or formal_bundle.get("path") != expected_formal_path
        or formal_bundle.get("snapshot_sha256")
        != result["formal_execution_snapshot_sha256"]
        or formal_bundle.get("source_contract_sha256")
        != result["formal_execution_source_sha256"]
    ):
        raise AdaptiveShadowExecutionDataError(
            "formal execution bundle identity is invalid"
        )
    supplemented = result.get("supplemented_tickers")
    if (
        not isinstance(supplemented, list)
        or supplemented != sorted(supplemented)
        or len(supplemented) != len(set(supplemented))
        or not all(isinstance(value, str) and value for value in supplemented)
    ):
        raise AdaptiveShadowExecutionDataError(
            "source supplemented_tickers are invalid"
        )
    mode = result.get("mode")
    if mode == "formal_snapshot_reuse":
        if (
            supplemented
            or result.get("decision_input") is not None
            or result.get("calendar") is not None
            or result.get("raw_partitions") != []
            or result.get("fallback_raw_partitions") != []
            or result.get("suspensions") is not None
            or result.get("formal_delists") is not None
            or result.get("delists") is not None
        ):
            raise AdaptiveShadowExecutionDataError(
                "fast-path source contract contains supplemental evidence"
            )
    elif mode == "supplemented_prior_holdings":
        decision = result.get("decision_input")
        if (
            not supplemented
            or not isinstance(decision, Mapping)
            or decision.get("snapshot_sha256")
            != result["source_data_snapshot_sha256"]
            or not isinstance(result.get("calendar"), Mapping)
            or not isinstance(result.get("raw_partitions"), list)
            or not all(
                isinstance(item, Mapping) for item in result["raw_partitions"]
            )
            or not isinstance(result.get("fallback_raw_partitions"), list)
            or not all(
                isinstance(item, Mapping)
                for item in result["fallback_raw_partitions"]
            )
            or not isinstance(result.get("suspensions"), Mapping)
            or not isinstance(result.get("formal_delists"), Mapping)
            or not isinstance(result.get("delists"), Mapping)
        ):
            raise AdaptiveShadowExecutionDataError(
                "supplemented source contract has invalid nested evidence"
            )
    else:
        raise AdaptiveShadowExecutionDataError("unknown shadow market source mode")
    return result


def _validate_formal_source_contract(
    value: Mapping[str, Any],
    generation: GenerationResult,
    formal: ExecutionSnapshot,
) -> None:
    if set(value) != _formal_data._EXECUTION_SOURCE_KEYS:
        raise AdaptiveShadowExecutionDataError(
            "formal execution source contract has a non-exact schema"
        )
    if _sha256_bytes(_canonical_json_bytes(value)) != formal.execution_source_sha256:
        raise AdaptiveShadowExecutionDataError(
            "formal execution source contract hash differs from its snapshot"
        )
    if (
        value.get("generation_result_sha256") != generation.result_sha256
        or value.get("target_input_snapshot_sha256")
        != generation.input_snapshot_sha256
        or value.get("benchmark_tickers_sha256")
        != formal.benchmark_tickers_sha256
        or value.get("selected_market_max_date") != formal.holding_end_date
    ):
        raise AdaptiveShadowExecutionDataError(
            "formal execution source identities differ"
        )


def _checkpoint_source_from_existing_cas(
    root: Path,
    checkpoint: Mapping[str, Any],
    *,
    dataset: str,
    trade_date: str,
    deadline: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    key = f"{dataset}/{trade_date}"
    raw = checkpoint.get("partitions", {}).get(key)
    if not isinstance(raw, Mapping):
        raise AdaptiveShadowExecutionDataError(
            f"fallback raw checkpoint lacks {key}"
        )
    if (
        raw.get("status") != "complete"
        or raw.get("dataset") != dataset
        or raw.get("trade_date") != trade_date
    ):
        raise AdaptiveShadowExecutionDataError(
            f"fallback raw checkpoint identity differs for {key}"
        )
    completed = _formal_data._utc(
        raw.get("completed_at_utc"),
        label=f"{key}.completed_at_utc",
    )
    if completed > deadline:
        raise AdaptiveShadowExecutionDataError(
            f"fallback partition {key} was unavailable by the formal deadline"
        )
    digest = _sha(raw.get("sha256"), f"{key}.sha256")
    size = raw.get("size_bytes")
    rows = raw.get("row_count")
    if type(size) is not int or size < 0 or type(rows) is not int or rows <= 0:
        raise AdaptiveShadowExecutionDataError(
            f"fallback checkpoint metadata is invalid for {key}"
        )
    expected_origin = (
        root
        / "runtime/data/raw"
        / dataset
        / f"trade_date={trade_date}"
        / "part-000.parquet"
    )
    if raw.get("path") != _relative(expected_origin, root):
        raw_path = Path(str(raw.get("path") or ""))
        if not raw_path.is_absolute() or raw_path.resolve() != expected_origin.resolve():
            raise AdaptiveShadowExecutionDataError(
                f"fallback checkpoint origin path differs for {key}"
            )
    immutable_relative = (
        IMMUTABLE_SOURCE_RELATIVE_ROOT / f"sha256={digest}" / "artifact"
    )
    immutable_path = (root / immutable_relative).resolve()
    if (
        immutable_path.is_symlink()
        or not immutable_path.is_file()
        or immutable_path.stat().st_size != size
        or sha256_file(immutable_path) != digest
    ):
        raise AdaptiveShadowExecutionDataError(
            f"fallback partition CAS is missing or differs for {key}"
        )
    source: dict[str, Any] = {
        "role": "raw_partition",
        "checkpoint_key": key,
        "dataset": dataset,
        "trade_date": trade_date,
        "path": _relative(expected_origin, root),
        "sha256": digest,
        "immutable_path": immutable_relative.as_posix(),
        "size_bytes": size,
        "media_type": "application/vnd.apache.parquet",
        "row_count": rows,
        "completed_at_utc": _formal_data._utc_text(
            completed,
            label=f"{key}.completion",
        ),
    }
    if provider_completion_required(trade_date):
        evidence = raw.get("provider_completion")
        if not isinstance(evidence, Mapping):
            raise AdaptiveShadowExecutionDataError(
                f"fallback partition lacks provider proof for {key}"
            )
        source["provider_completion"] = dict(evidence)
    try:
        frame, rebuilt, _completed = _formal_data._read_partition(
            root,
            {},
            dataset=dataset,
            trade_date=trade_date,
            availability_cap=deadline,
            sealed_source=source,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            f"fallback partition CAS failed deep validation for {key}"
        ) from exc
    if rebuilt != source:
        raise AdaptiveShadowExecutionDataError(
            f"fallback partition source changed for {key}"
        )
    return frame, source


def _fallback_inputs_from_existing_cas(
    root: Path,
    *,
    calendar_sessions: Sequence[str],
    signal_index: int,
    tickers: set[str],
    deadline: pd.Timestamp,
    formal_raw_sources: Mapping[str, Mapping[str, Any]],
    sealed_fallback_sources: Sequence[Mapping[str, Any]] | None,
) -> tuple[dict[str, tuple[float, float, str]], tuple[dict[str, Any], ...]]:
    if not tickers:
        if sealed_fallback_sources not in (None, (), []):
            raise AdaptiveShadowExecutionDataError(
                "sealed fallback sources exist without a fallback ticker"
            )
        return {}, ()
    if sealed_fallback_sources is None:
        _checkpoint_path, checkpoint = _formal_data._checkpoint(root)
        sealed_by_key: dict[str, Mapping[str, Any]] = {}
    else:
        checkpoint = {}
        sealed_by_key = {
            str(item.get("checkpoint_key") or ""): item
            for item in sealed_fallback_sources
        }
        if (
            len(sealed_by_key) != len(sealed_fallback_sources)
            or "" in sealed_by_key
        ):
            raise AdaptiveShadowExecutionDataError(
                "sealed fallback source keys are invalid or duplicated"
            )
    history: dict[str, list[tuple[str, float, float]]] = {
        ticker: [] for ticker in tickers
    }
    used_extra: dict[str, dict[str, Any]] = {}
    for session in reversed(tuple(calendar_sessions[: signal_index + 1])):
        frames: dict[str, pd.DataFrame] = {}
        session_sources: dict[str, dict[str, Any]] = {}
        datasets = (
            PROVIDER_COMPLETION_DATASETS
            if provider_completion_required(session)
            else ("daily", "adj_factor")
        )
        for dataset in datasets:
            key = f"{dataset}/{session}"
            existing = formal_raw_sources.get(key)
            if existing is not None:
                try:
                    frame, source, _completed = _formal_data._read_partition(
                        root,
                        {},
                        dataset=dataset,
                        trade_date=session,
                        availability_cap=deadline,
                        sealed_source=existing,
                    )
                except Exception as exc:
                    raise AdaptiveShadowExecutionDataError(
                        f"formal fallback CAS failed replay for {key}"
                    ) from exc
            elif sealed_fallback_sources is not None:
                sealed = sealed_by_key.get(key)
                if not isinstance(sealed, Mapping):
                    raise AdaptiveShadowExecutionDataError(
                        f"sealed fallback source is missing {key}"
                    )
                try:
                    frame, source, _completed = _formal_data._read_partition(
                        root,
                        {},
                        dataset=dataset,
                        trade_date=session,
                        availability_cap=deadline,
                        sealed_source=sealed,
                    )
                except Exception as exc:
                    raise AdaptiveShadowExecutionDataError(
                        f"sealed fallback CAS failed replay for {key}"
                    ) from exc
                used_extra[key] = source
            else:
                frame, source = _checkpoint_source_from_existing_cas(
                    root,
                    checkpoint,
                    dataset=dataset,
                    trade_date=session,
                    deadline=deadline,
                )
                used_extra[key] = source
            frames[dataset] = frame
            session_sources[dataset] = source
        daily = frames["daily"]
        adj = frames["adj_factor"]
        if provider_completion_required(session):
            evidence_shas = {
                str(item.get("provider_completion", {}).get("evidence_sha256") or "")
                for item in session_sources.values()
                if isinstance(item.get("provider_completion"), Mapping)
            }
            daily_tickers = set(daily["ts_code"].astype(str))
            basic_tickers = set(frames["daily_basic"]["ts_code"].astype(str))
            adj_tickers = set(adj["ts_code"].astype(str))
            if (
                len(evidence_shas) != 1
                or "" in evidence_shas
                or daily_tickers != basic_tickers
                or not daily_tickers.issubset(adj_tickers)
            ):
                raise AdaptiveShadowExecutionDataError(
                    f"fallback provider-completion bundle differs for {session}"
                )
        factors = adj.set_index("ts_code")["adj_factor"]
        for row in daily.loc[daily["ts_code"].isin(tickers)].itertuples(index=False):
            ticker = str(row.ts_code)
            if len(history[ticker]) >= 21 or ticker not in factors.index:
                continue
            close = float(row.close) * float(factors.loc[ticker])
            amount_rmb = float(row.amount) * 1_000.0
            if not (
                math.isfinite(close)
                and close > 0
                and math.isfinite(amount_rmb)
                and amount_rmb > 0
            ):
                raise AdaptiveShadowExecutionDataError(
                    f"fallback execution input is invalid for {ticker}/{session}"
                )
            history[ticker].append((session, close, amount_rmb))
        if all(len(values) >= 21 for values in history.values()):
            break
    result: dict[str, tuple[float, float, str]] = {}
    for ticker, reverse_rows in history.items():
        rows = sorted(reverse_rows)
        if len(rows) < 21:
            raise AdaptiveShadowExecutionDataError(
                f"fewer than 21 sealed observations for prior holding {ticker}"
            )
        rows = rows[-21:]
        closes = pd.Series([row[1] for row in rows], dtype="float64")
        returns = closes.pct_change(fill_method=None).iloc[-20:]
        amounts = pd.Series([row[2] for row in rows[-20:]], dtype="float64")
        volatility = float(returns.std(ddof=1))
        adv = float(amounts.mean())
        if not (
            math.isfinite(volatility)
            and volatility >= 0
            and math.isfinite(adv)
            and adv > 0
        ):
            raise AdaptiveShadowExecutionDataError(
                f"fallback ADV/volatility is invalid for {ticker}"
            )
        result[ticker] = (adv, volatility, rows[-1][0])
    return result, tuple(used_extra[key] for key in sorted(used_extra))


def _formal_snapshot_from_loaded(
    wrapper: ShadowExecutionSnapshot,
    source_contract: Mapping[str, Any],
) -> ExecutionSnapshot:
    market = wrapper.execution_snapshot
    mode = source_contract.get("mode")
    if mode == "formal_snapshot_reuse":
        supplemented: list[str] = []
    elif mode == "supplemented_prior_holdings":
        supplemented = source_contract.get("supplemented_tickers")
        if (
            not isinstance(supplemented, list)
            or not supplemented
            or len(set(supplemented)) != len(supplemented)
            or not all(isinstance(value, str) for value in supplemented)
        ):
            raise AdaptiveShadowExecutionDataError(
                "supplemented ticker identity is invalid"
            )
    else:
        raise AdaptiveShadowExecutionDataError("unknown shadow market source mode")
    rows = tuple(row for row in market.rows if row.ticker not in set(supplemented))
    try:
        formal = _clone_execution_snapshot(
            market,
            execution_source_sha256=str(
                source_contract["formal_execution_source_sha256"]
            ),
            rows=rows,
        )
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "formal snapshot could not be recovered from shadow rows"
        ) from exc
    if (
        formal.snapshot_sha256
        != source_contract.get("formal_execution_snapshot_sha256")
        or formal.execution_source_sha256
        != source_contract.get("formal_execution_source_sha256")
        or _formal_rows_sha(formal) != source_contract.get("formal_rows_sha256")
    ):
        raise AdaptiveShadowExecutionDataError(
            "recovered formal snapshot differs from the stored identity"
        )
    return formal


def _load_bundle_files(path: str | Path) -> AdaptiveShadowExecutionDataSnapshot:
    directory = Path(path).expanduser().resolve()
    snapshot_path = directory / "snapshot.json"
    sources_path = directory / "sources.json"
    if (
        snapshot_path.is_symlink()
        or sources_path.is_symlink()
        or not snapshot_path.is_file()
        or not sources_path.is_file()
    ):
        raise AdaptiveShadowExecutionDataError(
            "shadow market bundle is incomplete or uses a symlink"
        )
    snapshot_raw = snapshot_path.read_bytes()
    sources_raw = sources_path.read_bytes()
    try:
        snapshot_value = json.loads(snapshot_raw.decode("utf-8"))
        sources_value = json.loads(sources_raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AdaptiveShadowExecutionDataError(
            "shadow market bundle contains unreadable JSON"
        ) from exc
    if (
        not isinstance(snapshot_value, Mapping)
        or not isinstance(sources_value, Mapping)
        or snapshot_raw != _canonical_json_bytes(snapshot_value)
        or sources_raw != _canonical_json_bytes(sources_value)
    ):
        raise AdaptiveShadowExecutionDataError(
            "shadow market bundle JSON is not canonical"
        )
    contract = _strict_source_contract(sources_value)
    try:
        wrapper = ShadowExecutionSnapshot.from_mapping(snapshot_value)
    except Exception as exc:
        raise AdaptiveShadowExecutionDataError(
            "shadow execution snapshot contract is invalid"
        ) from exc
    source_sha = _sha256_bytes(sources_raw)
    bundle_sha = _bundle_sha(wrapper.snapshot_sha256, source_sha)
    if directory.name != bundle_sha or not _SHA256_RE.fullmatch(directory.name):
        raise AdaptiveShadowExecutionDataError(
            "shadow market directory does not match its content address"
        )
    if (
        contract["target_plan_sha256"] != wrapper.target_plan_sha256
        or contract["formal_input_snapshot_sha256"]
        != wrapper.formal_input_snapshot_sha256
        or contract["formal_decision_record_sha256"]
        != wrapper.formal_decision_record_sha256
        or contract["benchmark_tickers_sha256"]
        != wrapper.execution_snapshot.benchmark_tickers_sha256
        or contract["selected_market_max_date"]
        != wrapper.execution_snapshot.holding_end_date
    ):
        raise AdaptiveShadowExecutionDataError(
            "shadow snapshot and market source bindings differ"
        )
    if wrapper.execution_snapshot.execution_source_sha256 != source_sha:
        raise AdaptiveShadowExecutionDataError(
            "shadow market snapshot does not bind its source contract"
        )
    if contract["mode"] not in {
        "formal_snapshot_reuse",
        "supplemented_prior_holdings",
    }:
        raise AdaptiveShadowExecutionDataError("unknown shadow market source mode")
    _formal_snapshot_from_loaded(wrapper, contract)
    return AdaptiveShadowExecutionDataSnapshot(
        snapshot=wrapper,
        directory=directory,
        snapshot_path=snapshot_path,
        sources_path=sources_path,
        source_contract=contract,
        bundle_sha256=bundle_sha,
    )


def load_adaptive_shadow_execution_snapshot(
    path: str | Path,
    formal_generation_result: GenerationResult | Mapping[str, Any],
    shadow_plan: ShadowCyclePlan | Mapping[str, Any],
    previous_shadow_account_state: SleeveAccountState | Mapping[str, Any],
    *,
    plan_bindings: Mapping[str, Any],
    previous_formal_account_state: SleeveAccountState | Mapping[str, Any] | None = None,
) -> AdaptiveShadowExecutionDataSnapshot:
    """Load and independently replay a sealed adaptive-shadow market bundle."""

    loaded = _load_bundle_files(path)
    generation = _generation(formal_generation_result)
    plan = _plan(shadow_plan)
    previous = _account(previous_shadow_account_state)
    bindings = _plan_bindings(plan_bindings)
    try:
        root = loaded.directory.parents[4]
    except IndexError as exc:
        raise AdaptiveShadowExecutionDataError(
            "shadow market bundle is outside the canonical layout"
        ) from exc
    expected = (root / SHADOW_MARKET_RELATIVE_ROOT / loaded.bundle_sha256).resolve()
    if loaded.directory != expected:
        raise AdaptiveShadowExecutionDataError(
            "shadow market bundle is outside the canonical store"
        )
    contract = loaded.source_contract
    if (
        contract["formal_generation_result_sha256"] != generation.result_sha256
        or contract["target_plan_sha256"] != plan.plan_sha256
        or contract["registry_sha256"] != plan.registry_sha256
        or contract["candidate_id"] != plan.candidate_id
        or contract["candidate_sha256"] != plan.candidate_sha256
        or contract["previous_shadow_account_state_sha256"] != previous.state_sha256
        or any(contract[key] != value for key, value in bindings.items())
    ):
        raise AdaptiveShadowExecutionDataError(
            "loader received different generation, plan, or account bindings"
        )
    _formal_snapshot_from_loaded(loaded.snapshot, contract)
    formal_bundle = contract["formal_execution_bundle"]
    formal_path = (root / str(formal_bundle["path"])).resolve()
    expected_formal_path = (
        root
        / _formal_data.EXECUTION_RELATIVE_ROOT
        / contract["formal_execution_snapshot_sha256"]
    ).resolve()
    if formal_path != expected_formal_path:
        raise AdaptiveShadowExecutionDataError(
            "formal execution bundle path differs from its canonical identity"
        )
    rebuilt = build_adaptive_shadow_execution_snapshot(
        root,
        generation,
        formal_path,
        plan,
        previous,
        plan_bindings=bindings,
        previous_formal_account_state=previous_formal_account_state,
        _materialize=False,
        _sealed_source_contract=contract,
    )
    if (
        rebuilt.bundle_sha256 != loaded.bundle_sha256
        or _canonical_json_bytes(rebuilt.source_contract)
        != loaded.sources_path.read_bytes()
        or _canonical_json_bytes(rebuilt.snapshot.to_dict())
        != loaded.snapshot_path.read_bytes()
    ):
        raise AdaptiveShadowExecutionDataError(
            "shadow market bundle differs from independent sealed replay"
        )
    return loaded


__all__ = [
    "AdaptiveShadowExecutionDataError",
    "AdaptiveShadowExecutionDataSnapshot",
    "KIND",
    "SCHEMA_VERSION",
    "SHADOW_MARKET_RELATIVE_ROOT",
    "build_adaptive_shadow_execution_snapshot",
    "load_adaptive_shadow_execution_snapshot",
]
