"""Pure 5.9 adaptive-shadow execution and accounting.

The shadow route shares the released prospective execution kernel, calendar
semantics, holding period, costs, and benchmark rules.  It does *not* create a
``GenerationResult`` or mutate the formal route.  A separate wrapper binds the
same content-addressed market evidence to an independently hashed shadow target
plan before any outcome is calculated.

This module has no filesystem, network, clock, broker, or ledger access.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any

from .adaptive_shadow import canonical_sha256
from . import prospective_execution as _formal
from .prospective_execution import (
    AccountPosition,
    CycleDailyObservation,
    ExecutionSnapshot,
    FROZEN_EXECUTION_CONTRACT,
    HOLDING_DAYS,
    SleeveAccountState,
)
from .prospective_targets import (
    OFFSET_COUNT,
    POSITION_WEIGHT_PPM,
    SLEEVE_CAPITAL_FEN,
    WEIGHT_SCALE_PPM,
)


SCHEMA_VERSION = 1
ENGINE_ID = "factor-lab/adaptive-shadow-execution/5.9"

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class AdaptiveShadowExecutionError(ValueError):
    """Raised when shadow execution evidence fails its sealed contract."""


def _fail(message: str) -> None:
    raise AdaptiveShadowExecutionError(message)


def _sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        _fail(f"{label} must be a lowercase SHA-256")
    return value


def _candidate_id(value: Any) -> str:
    if not isinstance(value, str) or not _CANDIDATE_ID_RE.fullmatch(value):
        _fail("candidate_id must use lowercase safe identifier syntax")
    return value


def _date(value: Any, label: str) -> str:
    try:
        return _formal._date(value, label)
    except _formal.ProspectiveExecutionError as exc:
        raise AdaptiveShadowExecutionError(str(exc)) from exc


def _utc(value: Any, label: str) -> str:
    try:
        return _formal._utc(value, label)
    except _formal.ProspectiveExecutionError as exc:
        raise AdaptiveShadowExecutionError(str(exc)) from exc


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        _fail(f"{label} must be an integer >= {minimum}")
    return value


def _exact_mapping(value: Any, keys: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys or len(value) != len(keys):
        _fail(f"{label} does not have the exact schema")
    return value


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value[:-1] + "+00:00")


def _account_state(value: SleeveAccountState | Mapping[str, Any]) -> SleeveAccountState:
    try:
        if isinstance(value, SleeveAccountState):
            return value
        if isinstance(value, Mapping):
            return SleeveAccountState.from_mapping(value)
    except _formal.ProspectiveExecutionError as exc:
        raise AdaptiveShadowExecutionError(str(exc)) from exc
    _fail("previous_account_state must be a strict SleeveAccountState")


def _normalize_targets(value: Any) -> tuple[tuple[str, int], ...]:
    if isinstance(value, Mapping):
        raw = list(value.items())
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        raw = list(value)
    else:
        _fail("targets_ppm must be a mapping or sequence of pairs")
    normalized: list[tuple[str, int]] = []
    for index, item in enumerate(raw):
        if not isinstance(item, Sequence) or isinstance(item, (str, bytes, bytearray)):
            _fail(f"targets_ppm[{index}] must be a ticker/weight pair")
        pair = list(item)
        if len(pair) != 2:
            _fail(f"targets_ppm[{index}] must be a ticker/weight pair")
        try:
            ticker = _formal._ticker(pair[0], f"targets_ppm[{index}].ticker")
        except _formal.ProspectiveExecutionError as exc:
            raise AdaptiveShadowExecutionError(str(exc)) from exc
        weight = _integer(pair[1], f"targets_ppm[{ticker}]", minimum=1)
        normalized.append((ticker, weight))
    normalized.sort()
    if len(normalized) != 10 or len({ticker for ticker, _weight in normalized}) != 10:
        _fail("shadow target plan must contain exactly ten unique tickers")
    if any(weight != POSITION_WEIGHT_PPM for _ticker, weight in normalized):
        _fail("shadow targets must use the frozen equal Top10 integer PPM weight")
    if sum(weight for _ticker, weight in normalized) != WEIGHT_SCALE_PPM:
        _fail("shadow target PPM weights must sum to one million")
    return tuple(normalized)


@dataclass(frozen=True)
class ShadowCyclePlan:
    """One independently sealed candidate/offset target plan."""

    registry_sha256: str
    candidate_id: str
    candidate_sha256: str
    offset: int
    signal_date: str
    trade_date: str
    targets_ppm: Sequence[Sequence[Any]] | Mapping[str, Any]
    formal_input_snapshot_sha256: str
    formal_decision_record_sha256: str
    planned_at_utc: str
    formal_trade_deadline_utc: str
    schema_version: int = SCHEMA_VERSION
    engine_id: str = ENGINE_ID
    plan_sha256: str = ""

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != SCHEMA_VERSION
            or self.engine_id != ENGINE_ID
        ):
            _fail("unsupported shadow cycle plan version/engine")
        object.__setattr__(self, "registry_sha256", _sha(self.registry_sha256, "registry_sha256"))
        object.__setattr__(self, "candidate_id", _candidate_id(self.candidate_id))
        object.__setattr__(
            self,
            "candidate_sha256",
            _sha(self.candidate_sha256, "candidate_sha256"),
        )
        object.__setattr__(
            self,
            "formal_input_snapshot_sha256",
            _sha(self.formal_input_snapshot_sha256, "formal_input_snapshot_sha256"),
        )
        object.__setattr__(
            self,
            "formal_decision_record_sha256",
            _sha(self.formal_decision_record_sha256, "formal_decision_record_sha256"),
        )
        if type(self.offset) is not int or not 0 <= self.offset < OFFSET_COUNT:
            _fail("shadow plan offset must be an integer in [0, 10)")
        signal = _date(self.signal_date, "signal_date")
        trade = _date(self.trade_date, "trade_date")
        if signal >= trade:
            _fail("shadow plan trade_date must follow signal_date")
        object.__setattr__(self, "signal_date", signal)
        object.__setattr__(self, "trade_date", trade)
        object.__setattr__(self, "targets_ppm", _normalize_targets(self.targets_ppm))
        planned = _utc(self.planned_at_utc, "planned_at_utc")
        deadline = _utc(self.formal_trade_deadline_utc, "formal_trade_deadline_utc")
        if _timestamp(planned) > _timestamp(deadline):
            _fail("shadow plan was created after the formal trade deadline")
        object.__setattr__(self, "planned_at_utc", planned)
        object.__setattr__(self, "formal_trade_deadline_utc", deadline)
        expected = canonical_sha256(self.payload())
        if self.plan_sha256 and self.plan_sha256 != expected:
            _fail("plan_sha256 does not match the shadow target plan")
        object.__setattr__(self, "plan_sha256", expected)

    @property
    def account_deployment_sha256(self) -> str:
        """Stable, candidate-isolated identity shared by that candidate's ten accounts."""

        return canonical_sha256(
            {
                "candidate_id": self.candidate_id,
                "candidate_sha256": self.candidate_sha256,
                "kind": "adaptive_shadow_candidate_accounts_v1",
                "registry_sha256": self.registry_sha256,
            }
        )

    def target_mapping(self) -> Mapping[str, int]:
        return dict(self.targets_ppm)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "registry_sha256": self.registry_sha256,
            "candidate_id": self.candidate_id,
            "candidate_sha256": self.candidate_sha256,
            "offset": self.offset,
            "signal_date": self.signal_date,
            "trade_date": self.trade_date,
            "targets_ppm": dict(self.targets_ppm),
            "formal_input_snapshot_sha256": self.formal_input_snapshot_sha256,
            "formal_decision_record_sha256": self.formal_decision_record_sha256,
            "planned_at_utc": self.planned_at_utc,
            "formal_trade_deadline_utc": self.formal_trade_deadline_utc,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "plan_sha256": self.plan_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ShadowCyclePlan":
        required = {
            "schema_version",
            "engine_id",
            "registry_sha256",
            "candidate_id",
            "candidate_sha256",
            "offset",
            "signal_date",
            "trade_date",
            "targets_ppm",
            "formal_input_snapshot_sha256",
            "formal_decision_record_sha256",
            "planned_at_utc",
            "formal_trade_deadline_utc",
            "plan_sha256",
        }
        raw = _exact_mapping(value, required, "shadow cycle plan")
        _sha(raw["plan_sha256"], "plan_sha256")
        return cls(**dict(raw))


@dataclass(frozen=True)
class ShadowExecutionSnapshot:
    """Formal market evidence re-sealed against an independent shadow plan."""

    target_plan_sha256: str
    formal_input_snapshot_sha256: str
    formal_decision_record_sha256: str
    execution_snapshot: ExecutionSnapshot | Mapping[str, Any]
    schema_version: int = SCHEMA_VERSION
    engine_id: str = ENGINE_ID
    snapshot_sha256: str = ""

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != SCHEMA_VERSION
            or self.engine_id != ENGINE_ID
        ):
            _fail("unsupported shadow execution snapshot version/engine")
        for name in (
            "target_plan_sha256",
            "formal_input_snapshot_sha256",
            "formal_decision_record_sha256",
        ):
            object.__setattr__(self, name, _sha(getattr(self, name), name))
        inner = self.execution_snapshot
        try:
            if isinstance(inner, Mapping):
                inner = ExecutionSnapshot.from_mapping(inner)
        except _formal.ProspectiveExecutionError as exc:
            raise AdaptiveShadowExecutionError(str(exc)) from exc
        if not isinstance(inner, ExecutionSnapshot):
            _fail("execution_snapshot must be a strict formal ExecutionSnapshot")
        object.__setattr__(self, "execution_snapshot", inner)
        expected = canonical_sha256(self.payload())
        if self.snapshot_sha256 and self.snapshot_sha256 != expected:
            _fail("snapshot_sha256 does not match shadow execution evidence")
        object.__setattr__(self, "snapshot_sha256", expected)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "target_plan_sha256": self.target_plan_sha256,
            "formal_input_snapshot_sha256": self.formal_input_snapshot_sha256,
            "formal_decision_record_sha256": self.formal_decision_record_sha256,
            "execution_snapshot": self.execution_snapshot.to_dict(),
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "snapshot_sha256": self.snapshot_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ShadowExecutionSnapshot":
        required = {
            "schema_version",
            "engine_id",
            "target_plan_sha256",
            "formal_input_snapshot_sha256",
            "formal_decision_record_sha256",
            "execution_snapshot",
            "snapshot_sha256",
        }
        raw = _exact_mapping(value, required, "shadow execution snapshot")
        _sha(raw["snapshot_sha256"], "snapshot_sha256")
        return cls(**dict(raw))


@dataclass(frozen=True)
class ShadowCycleOutcome:
    """Integer-valued replay result with complete shadow identity bindings."""

    contract_sha256: str
    registry_sha256: str
    candidate_id: str
    candidate_sha256: str
    account_deployment_sha256: str
    target_plan_sha256: str
    shadow_execution_snapshot_sha256: str
    market_execution_snapshot_sha256: str
    formal_input_snapshot_sha256: str
    formal_decision_record_sha256: str
    previous_account_state_sha256: str
    offset: int
    signal_date: str
    holding_start_date: str
    holding_end_date: str
    observation_available_at_utc: str
    opening_nav_fen: int
    pretrade_nav_fen: int
    ending_nav_fen: int
    gross_return_ppb: int
    net_return_ppb: int
    benchmark_return_ppb: int
    turnover_ppm: int
    fees_fen: int
    executed_order_count: int
    blocked_order_count: int
    benchmark_expected_count: int
    benchmark_complete_count: int
    daily_path: Sequence[CycleDailyObservation | Mapping[str, Any]]
    next_account_state: SleeveAccountState | Mapping[str, Any]
    schema_version: int = SCHEMA_VERSION
    engine_id: str = ENGINE_ID
    outcome_sha256: str = ""

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != SCHEMA_VERSION
            or self.engine_id != ENGINE_ID
        ):
            _fail("unsupported shadow cycle outcome version/engine")
        for name in (
            "contract_sha256",
            "registry_sha256",
            "candidate_sha256",
            "account_deployment_sha256",
            "target_plan_sha256",
            "shadow_execution_snapshot_sha256",
            "market_execution_snapshot_sha256",
            "formal_input_snapshot_sha256",
            "formal_decision_record_sha256",
            "previous_account_state_sha256",
        ):
            object.__setattr__(self, name, _sha(getattr(self, name), name))
        object.__setattr__(self, "candidate_id", _candidate_id(self.candidate_id))
        if self.contract_sha256 != FROZEN_EXECUTION_CONTRACT.contract_sha256:
            _fail("shadow outcome is bound to a different execution contract")
        if type(self.offset) is not int or not 0 <= self.offset < OFFSET_COUNT:
            _fail("shadow outcome offset is invalid")
        for name in ("signal_date", "holding_start_date", "holding_end_date"):
            object.__setattr__(self, name, _date(getattr(self, name), name))
        object.__setattr__(
            self,
            "observation_available_at_utc",
            _utc(self.observation_available_at_utc, "observation_available_at_utc"),
        )
        if not self.signal_date < self.holding_start_date <= self.holding_end_date:
            _fail("shadow outcome dates are not ordered")
        for name in (
            "opening_nav_fen",
            "pretrade_nav_fen",
            "ending_nav_fen",
            "turnover_ppm",
            "fees_fen",
            "executed_order_count",
            "blocked_order_count",
            "benchmark_expected_count",
            "benchmark_complete_count",
        ):
            _integer(getattr(self, name), name)
        if self.opening_nav_fen <= 0 or self.pretrade_nav_fen <= 0:
            _fail("shadow cycle opening/pretrade NAV must be positive")
        for name in ("gross_return_ppb", "net_return_ppb", "benchmark_return_ppb"):
            value = getattr(self, name)
            if type(value) is not int or value < -_formal.RETURN_SCALE_PPB:
                _fail(f"{name} must be an integer no less than -100%")
        if (
            self.benchmark_expected_count <= 0
            or self.benchmark_complete_count != self.benchmark_expected_count
        ):
            _fail("shadow benchmark must account for every frozen roster member")
        path: list[CycleDailyObservation] = []
        for item in self.daily_path:
            try:
                path.append(
                    item
                    if isinstance(item, CycleDailyObservation)
                    else CycleDailyObservation.from_mapping(item)
                )
            except (_formal.ProspectiveExecutionError, TypeError) as exc:
                raise AdaptiveShadowExecutionError("daily_path contains an invalid row") from exc
        if len(path) != HOLDING_DAYS + 1 or any(
            left.date >= right.date for left, right in zip(path, path[1:])
        ):
            _fail("daily_path must contain eleven strictly ordered holding sessions")
        if (
            path[0].date != self.holding_start_date
            or path[-1].date != self.holding_end_date
            or path[-1].account_nav_fen != self.ending_nav_fen
            or path[0].benchmark_index_ppb != _formal.RETURN_SCALE_PPB
            or path[-1].benchmark_index_ppb - _formal.RETURN_SCALE_PPB
            != self.benchmark_return_ppb
        ):
            _fail("daily_path does not reconcile to the shadow cycle")
        object.__setattr__(self, "daily_path", tuple(path))
        state = _account_state(self.next_account_state)
        object.__setattr__(self, "next_account_state", state)
        if (
            state.offset != self.offset
            or state.deployment_sha256 != self.account_deployment_sha256
            or state.last_holding_end_date != self.holding_end_date
            or state.last_generation_result_sha256 != self.target_plan_sha256
            or state.last_execution_snapshot_sha256 != self.shadow_execution_snapshot_sha256
            or state.nav_fen != self.ending_nav_fen
        ):
            _fail("shadow outcome does not reconcile to its next account state")
        expected = canonical_sha256(self.payload())
        if self.outcome_sha256 and self.outcome_sha256 != expected:
            _fail("outcome_sha256 does not match the shadow cycle outcome")
        object.__setattr__(self, "outcome_sha256", expected)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "contract_sha256": self.contract_sha256,
            "registry_sha256": self.registry_sha256,
            "candidate_id": self.candidate_id,
            "candidate_sha256": self.candidate_sha256,
            "account_deployment_sha256": self.account_deployment_sha256,
            "target_plan_sha256": self.target_plan_sha256,
            "shadow_execution_snapshot_sha256": self.shadow_execution_snapshot_sha256,
            "market_execution_snapshot_sha256": self.market_execution_snapshot_sha256,
            "formal_input_snapshot_sha256": self.formal_input_snapshot_sha256,
            "formal_decision_record_sha256": self.formal_decision_record_sha256,
            "previous_account_state_sha256": self.previous_account_state_sha256,
            "offset": self.offset,
            "signal_date": self.signal_date,
            "holding_start_date": self.holding_start_date,
            "holding_end_date": self.holding_end_date,
            "observation_available_at_utc": self.observation_available_at_utc,
            "opening_nav_fen": self.opening_nav_fen,
            "pretrade_nav_fen": self.pretrade_nav_fen,
            "ending_nav_fen": self.ending_nav_fen,
            "gross_return_ppb": self.gross_return_ppb,
            "net_return_ppb": self.net_return_ppb,
            "benchmark_return_ppb": self.benchmark_return_ppb,
            "turnover_ppm": self.turnover_ppm,
            "fees_fen": self.fees_fen,
            "executed_order_count": self.executed_order_count,
            "blocked_order_count": self.blocked_order_count,
            "benchmark_expected_count": self.benchmark_expected_count,
            "benchmark_complete_count": self.benchmark_complete_count,
            "daily_path": [row.to_dict() for row in self.daily_path],
            "next_account_state": self.next_account_state.to_dict(),
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "outcome_sha256": self.outcome_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ShadowCycleOutcome":
        required = {
            "schema_version",
            "engine_id",
            "contract_sha256",
            "registry_sha256",
            "candidate_id",
            "candidate_sha256",
            "account_deployment_sha256",
            "target_plan_sha256",
            "shadow_execution_snapshot_sha256",
            "market_execution_snapshot_sha256",
            "formal_input_snapshot_sha256",
            "formal_decision_record_sha256",
            "previous_account_state_sha256",
            "offset",
            "signal_date",
            "holding_start_date",
            "holding_end_date",
            "observation_available_at_utc",
            "opening_nav_fen",
            "pretrade_nav_fen",
            "ending_nav_fen",
            "gross_return_ppb",
            "net_return_ppb",
            "benchmark_return_ppb",
            "turnover_ppm",
            "fees_fen",
            "executed_order_count",
            "blocked_order_count",
            "benchmark_expected_count",
            "benchmark_complete_count",
            "daily_path",
            "next_account_state",
            "outcome_sha256",
        }
        raw = _exact_mapping(value, required, "shadow cycle outcome")
        _sha(raw["outcome_sha256"], "outcome_sha256")
        return cls(**dict(raw))


def genesis_shadow_account(plan: ShadowCyclePlan | Mapping[str, Any]) -> SleeveAccountState:
    """Create the frozen CNY 5m genesis account for one candidate/offset."""

    normalized = _plan(plan)
    try:
        return SleeveAccountState.genesis(
            deployment_sha256=normalized.account_deployment_sha256,
            offset=normalized.offset,
        )
    except _formal.ProspectiveExecutionError as exc:
        raise AdaptiveShadowExecutionError(str(exc)) from exc


def _plan(value: ShadowCyclePlan | Mapping[str, Any]) -> ShadowCyclePlan:
    if isinstance(value, ShadowCyclePlan):
        return value
    if isinstance(value, Mapping):
        return ShadowCyclePlan.from_mapping(value)
    _fail("plan must be a strict ShadowCyclePlan")


def _shadow_snapshot(
    value: ShadowExecutionSnapshot | Mapping[str, Any] | ExecutionSnapshot,
) -> ShadowExecutionSnapshot:
    if isinstance(value, ShadowExecutionSnapshot):
        return value
    if isinstance(value, Mapping):
        return ShadowExecutionSnapshot.from_mapping(value)
    if isinstance(value, ExecutionSnapshot):
        _fail("raw ExecutionSnapshot lacks an independent target_plan_sha256 binding")
    _fail("execution_snapshot must be a strict ShadowExecutionSnapshot")


def _state_from_account(
    account: Any,
    *,
    previous: SleeveAccountState,
    plan: ShadowCyclePlan,
    snapshot: ShadowExecutionSnapshot,
) -> SleeveAccountState:
    positions = tuple(
        AccountPosition(
            ticker=ticker,
            quantity_hex=position.quantity.hex(),
            last_price_hex=position.last_price.hex(),
            average_cost_hex=position.average_cost.hex(),
            last_observation_date=position.last_observation_date,
            stale_since_date=position.stale_since_date,
        )
        for ticker, position in sorted(account.positions.items())
        if position.quantity > 0.0 and position.market_value > 1e-8
    )
    try:
        return SleeveAccountState(
            deployment_sha256=previous.deployment_sha256,
            offset=previous.offset,
            cycle_count=previous.cycle_count + 1,
            last_holding_end_date=snapshot.execution_snapshot.holding_end_date,
            # SleeveAccountState is the released state carrier.  Its legacy
            # field stores the shadow target-plan SHA; no GenerationResult is
            # constructed or represented by this value.
            last_generation_result_sha256=plan.plan_sha256,
            last_execution_snapshot_sha256=snapshot.snapshot_sha256,
            cash_hex=max(account.cash, 0.0).hex(),
            positions=positions,
            nav_fen=_formal._fen(account.nav()),
        )
    except _formal.ProspectiveExecutionError as exc:
        raise AdaptiveShadowExecutionError(str(exc)) from exc


def evaluate_shadow_cycle(
    plan: ShadowCyclePlan | Mapping[str, Any],
    execution_snapshot: ShadowExecutionSnapshot | Mapping[str, Any] | ExecutionSnapshot,
    previous_account_state: SleeveAccountState | Mapping[str, Any],
) -> ShadowCycleOutcome:
    """Execute one sealed shadow Top10 cycle under the formal frozen kernel."""

    selected = _plan(plan)
    wrapper = _shadow_snapshot(execution_snapshot)
    previous = _account_state(previous_account_state)
    snapshot = wrapper.execution_snapshot

    if wrapper.target_plan_sha256 != selected.plan_sha256:
        _fail("shadow execution snapshot is bound to another target plan")
    if wrapper.formal_input_snapshot_sha256 != selected.formal_input_snapshot_sha256:
        _fail("shadow execution snapshot is bound to another formal input")
    if wrapper.formal_decision_record_sha256 != selected.formal_decision_record_sha256:
        _fail("shadow execution snapshot is bound to another formal decision")
    if snapshot.trade_deadline_utc != selected.formal_trade_deadline_utc:
        _fail("shadow plan deadline differs from the sealed formal market evidence")
    if (
        selected.signal_date != snapshot.signal_date
        or selected.trade_date != snapshot.holding_start_date
    ):
        _fail("shadow plan and execution dates differ")
    signal_index = snapshot.calendar_sessions.index(snapshot.signal_date)
    if selected.offset != signal_index % OFFSET_COUNT:
        _fail("shadow offset differs from the official calendar index modulo ten")
    if _timestamp(selected.planned_at_utc) < max(
        _timestamp(snapshot.calendar_available_at_utc),
        _timestamp(snapshot.decision_inputs_available_at_utc),
    ):
        _fail("shadow plan predates its sealed formal inputs")
    if previous.deployment_sha256 != selected.account_deployment_sha256:
        _fail("shadow candidate and accounting deployments differ")
    if previous.offset != selected.offset:
        _fail("shadow plan offset differs from accounting sleeve")
    if previous.last_generation_result_sha256 == selected.plan_sha256:
        _fail("shadow target plan was already applied to this account")
    if previous.cycle_count > 0 and previous.last_holding_end_date != snapshot.holding_start_date:
        _fail("same-offset shadow NAV is not continuous at the boundary")

    target_weights = {
        ticker: weight / WEIGHT_SCALE_PPM for ticker, weight in selected.targets_ppm
    }
    rows_by_key = {(row.date, row.ticker): row for row in snapshot.rows}
    row_tickers = {row.ticker for row in snapshot.rows}
    required_tickers = set(target_weights) | {row.ticker for row in previous.positions}
    if not required_tickers.issubset(row_tickers):
        _fail("execution snapshot omits target or prior-held securities")
    start_rows = {
        ticker: rows_by_key[(snapshot.holding_start_date, ticker)] for ticker in row_tickers
    }
    for ticker in sorted(required_tickers):
        row = start_rows[ticker]
        if (
            row.execution_input_date is None
            or row.adv_20_asof_hex is None
            or row.volatility_20_asof_hex is None
        ):
            _fail(f"complete shadow execution lacks causal ADV/volatility for {ticker}")
        if row.execution_input_date > selected.signal_date:
            _fail(f"shadow execution input is from the future for {ticker}")

    if previous.cycle_count > 0:
        for position in previous.positions:
            row = start_rows[position.ticker]
            open_value = _formal._decode(row.open_adj_hex)
            if open_value is not None and not row.is_suspended and not row.is_delisted:
                if (
                    position.last_observation_date != snapshot.holding_start_date
                    or position.last_price_hex != row.open_adj_hex
                ):
                    _fail(f"shared-boundary mark changed for {position.ticker}")

    try:
        account = _formal._account_from_state(previous)
        accounting_start_nav = account.nav()
        if accounting_start_nav <= 0.0:
            _fail("a zero-NAV shadow sleeve cannot begin another cycle")
        policy = FROZEN_EXECUTION_CONTRACT.execution_policy()
        start_market = {
            ticker: _formal._market_payload(row) for ticker, row in start_rows.items()
        }
        execution = _formal.execute_rebalance(
            account,
            target_weights,
            start_market,
            trade_date=snapshot.holding_start_date,
            policy=policy,
            columns=_formal._EXECUTION_COLUMNS,
            ticker_column="ticker",
            process_corporate_actions=False,
            process_events=previous.cycle_count == 0,
        )
        accounting_start_nav = execution.accounting_start_nav
        pretrade_nav = execution.pretrade_nav
        fees_yuan = float(execution.costs["total"])
        traded_notional = execution.traded_notional
        executed_order_count = sum(order.status == "executed" for order in execution.orders)
        blocked_order_count = sum(order.status == "blocked" for order in execution.orders)

        window_sessions = snapshot.calendar_sessions[
            signal_index + 1 : signal_index + HOLDING_DAYS + 2
        ]
        strategy_navs = [account.nav()]
        ending_nav = strategy_navs[0]
        for observation_date in window_sessions[1:]:
            market = {
                ticker: _formal._market_payload(rows_by_key[(observation_date, ticker)])
                for ticker in tuple(account.positions)
            }
            observed = _formal.process_account_observation(
                account,
                market,
                observation_date=observation_date,
                policy=policy,
                columns=_formal._EXECUTION_COLUMNS,
                ticker_column="ticker",
                mark_at_open=True,
                process_corporate_actions=False,
                process_events=True,
            )
            ending_nav = observed.nav
            strategy_navs.append(ending_nav)
    except AdaptiveShadowExecutionError:
        raise
    except (ValueError, RuntimeError, _formal.ProspectiveExecutionError) as exc:
        raise AdaptiveShadowExecutionError(f"frozen execution kernel failed closed: {exc}") from exc

    benchmark_count = len(snapshot.benchmark_tickers)
    allocation = 1.0 / benchmark_count
    benchmark_cash = 0.0
    benchmark_units: dict[str, float] = {}
    benchmark_last_prices: dict[str, float] = {}
    benchmark_delisted: set[str] = set()
    for ticker in snapshot.benchmark_tickers:
        row = rows_by_key[(snapshot.holding_start_date, ticker)]
        price = _formal._benchmark_endpoint(row)
        if price is None:
            benchmark_cash += allocation
        else:
            benchmark_units[ticker] = allocation / price
            benchmark_last_prices[ticker] = price
    benchmark_indices = [1.0]
    for observation_date in window_sessions[1:]:
        for ticker in tuple(benchmark_units):
            if ticker in benchmark_delisted:
                continue
            row = rows_by_key[(observation_date, ticker)]
            if row.is_delisted:
                benchmark_delisted.add(ticker)
                benchmark_last_prices[ticker] = 0.0
                continue
            price = _formal._decode(row.open_adj_hex)
            if price is not None and not row.is_suspended:
                benchmark_last_prices[ticker] = price
            elif not row.is_suspended:
                _fail(f"benchmark observation lacks an explained open for {ticker}")
        benchmark_indices.append(
            benchmark_cash
            + sum(
                benchmark_units[ticker] * benchmark_last_prices[ticker]
                for ticker in benchmark_units
            )
        )
    benchmark_index_ppb = [
        _formal._scaled_return(value - 1.0) + _formal.RETURN_SCALE_PPB
        for value in benchmark_indices
    ]
    benchmark_return = benchmark_indices[-1] - 1.0
    net_return = ending_nav / accounting_start_nav - 1.0
    gross_return = (ending_nav + fees_yuan) / accounting_start_nav - 1.0
    turnover = traded_notional / pretrade_nav if pretrade_nav > 0.0 else 0.0
    next_state = _state_from_account(
        account,
        previous=previous,
        plan=selected,
        snapshot=wrapper,
    )
    return ShadowCycleOutcome(
        contract_sha256=FROZEN_EXECUTION_CONTRACT.contract_sha256,
        registry_sha256=selected.registry_sha256,
        candidate_id=selected.candidate_id,
        candidate_sha256=selected.candidate_sha256,
        account_deployment_sha256=selected.account_deployment_sha256,
        target_plan_sha256=selected.plan_sha256,
        shadow_execution_snapshot_sha256=wrapper.snapshot_sha256,
        market_execution_snapshot_sha256=snapshot.snapshot_sha256,
        formal_input_snapshot_sha256=selected.formal_input_snapshot_sha256,
        formal_decision_record_sha256=selected.formal_decision_record_sha256,
        previous_account_state_sha256=previous.state_sha256,
        offset=selected.offset,
        signal_date=selected.signal_date,
        holding_start_date=snapshot.holding_start_date,
        holding_end_date=snapshot.holding_end_date,
        observation_available_at_utc=snapshot.observation_available_at_utc,
        opening_nav_fen=_formal._fen(accounting_start_nav),
        pretrade_nav_fen=_formal._fen(pretrade_nav),
        ending_nav_fen=_formal._fen(ending_nav),
        gross_return_ppb=_formal._scaled_return(gross_return),
        net_return_ppb=_formal._scaled_return(net_return),
        benchmark_return_ppb=_formal._scaled_return(benchmark_return),
        turnover_ppm=int(round(turnover * WEIGHT_SCALE_PPM)),
        fees_fen=_formal._fen(fees_yuan),
        executed_order_count=executed_order_count,
        blocked_order_count=blocked_order_count,
        benchmark_expected_count=benchmark_count,
        benchmark_complete_count=benchmark_count,
        daily_path=tuple(
            CycleDailyObservation(
                date=observation_date,
                account_nav_fen=_formal._fen(strategy_nav),
                benchmark_index_ppb=benchmark_index,
            )
            for observation_date, strategy_nav, benchmark_index in zip(
                window_sessions, strategy_navs, benchmark_index_ppb, strict=True
            )
        ),
        next_account_state=next_state,
    )


__all__ = [
    "AdaptiveShadowExecutionError",
    "ENGINE_ID",
    "SCHEMA_VERSION",
    "ShadowCycleOutcome",
    "ShadowCyclePlan",
    "ShadowExecutionSnapshot",
    "evaluate_shadow_cycle",
    "genesis_shadow_account",
]
