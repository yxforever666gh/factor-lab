"""Pure prospective execution and outcome accounting for the 5.2 route.

The target generator owns *selection* state.  This module deliberately owns a
different, per-offset accounting state.  It reuses the frozen long-only
execution kernel and accepts only caller-supplied, content-addressed evidence;
there is no filesystem, network, broker, or clock access here.

For a signal at calendar index ``i`` the due sleeve trades at ``i + 1`` and is
observed through the open at ``i + 11``.  Ten offsets therefore form ten
independent CNY 5m virtual accounts whose accounting boundaries touch but do
not share cash or holdings.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import hashlib
import json
from math import isfinite
import re
from types import MappingProxyType
from typing import Any
import unicodedata

import numpy as np

from .portfolio.execution import (
    AShareCostPolicy,
    ExecutionAccount,
    ExecutionColumns,
    ExecutionPolicy,
    ExecutionPosition,
    execute_rebalance,
    process_account_observation,
)
from .prospective_targets import (
    GenerationResult,
    OFFSET_COUNT,
    POSITION_WEIGHT_PPM,
    SLEEVE_CAPITAL_FEN,
    WEIGHT_SCALE_PPM,
    calendar_prefix_sha256,
)


SCHEMA_VERSION = 1
ENGINE_ID = "factor-lab/prospective-execution/5.2"
HOLDING_DAYS = 10
RETURN_SCALE_PPB = 1_000_000_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TICKER_RE = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._-]{0,31}$")
_COST_NAMES = (
    "commission",
    "slippage",
    "stamp_duty",
    "exchange_handling",
    "transfer_fee",
    "impact",
    "total",
)


class ProspectiveExecutionError(ValueError):
    """Raised when prospective execution evidence is incomplete or inconsistent."""


def _canonical_json_bytes(value: Any) -> bytes:
    """Canonical JSON for envelopes that never contain JSON floating point."""

    def normalize(item: Any, path: str) -> Any:
        if item is None or isinstance(item, bool) or type(item) is int:
            return item
        if isinstance(item, str):
            result = unicodedata.normalize("NFC", item)
            if any(0xD800 <= ord(character) <= 0xDFFF for character in result):
                raise ProspectiveExecutionError(f"Unicode surrogate is forbidden at {path}")
            return result
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for raw_key, raw_value in item.items():
                if not isinstance(raw_key, str):
                    raise ProspectiveExecutionError(f"non-string JSON key at {path}")
                key = normalize(raw_key, f"{path}.<key>")
                if key in result:
                    raise ProspectiveExecutionError(
                        f"duplicate key after Unicode normalization at {path}: {key!r}"
                    )
                result[key] = normalize(raw_value, f"{path}.{key}")
            return result
        if isinstance(item, (list, tuple)):
            return [normalize(child, f"{path}[{index}]") for index, child in enumerate(item)]
        if isinstance(item, (float, np.floating)):
            raise ProspectiveExecutionError(f"floating-point JSON value is forbidden at {path}")
        raise ProspectiveExecutionError(
            f"unsupported JSON value {type(item).__name__} at {path}"
        )

    return json.dumps(
        normalize(value, "$"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _sha256_payload(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _sha256(value: Any, name: str) -> str:
    result = str(value)
    if not _SHA256_RE.fullmatch(result):
        raise ProspectiveExecutionError(f"{name} must be a lowercase SHA-256")
    return result


def _ticker(value: Any, name: str = "ticker") -> str:
    result = unicodedata.normalize("NFC", str(value).strip())
    if not _TICKER_RE.fullmatch(result):
        raise ProspectiveExecutionError(f"{name} is invalid: {result!r}")
    return result


def _date(value: Any, name: str) -> str:
    raw = str(value)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise ProspectiveExecutionError(f"{name} must be an ISO calendar date") from exc
    if parsed.isoformat() != raw:
        raise ProspectiveExecutionError(f"{name} must be a canonical ISO calendar date")
    return raw


def _utc(value: Any, name: str) -> str:
    raw = str(value)
    if not raw.endswith("Z"):
        raise ProspectiveExecutionError(f"{name} must end in Z")
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise ProspectiveExecutionError(f"{name} must be an ISO-8601 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ProspectiveExecutionError(f"{name} must be UTC")
    canonical = parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    if canonical != raw:
        raise ProspectiveExecutionError(f"{name} must use canonical UTC whole seconds")
    return raw


def _utc_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value[:-1] + "+00:00")


def _int(value: Any, name: str, *, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise ProspectiveExecutionError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise ProspectiveExecutionError(f"{name} must be at least {minimum}")
    return value


def _bool(value: Any, name: str) -> bool:
    if type(value) is not bool:
        raise ProspectiveExecutionError(f"{name} must be a boolean")
    return value


def _float_token(
    value: Any,
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> str:
    if not isinstance(value, str):
        raise ProspectiveExecutionError(f"{name} must be a canonical binary64 hex string")
    try:
        number = float.fromhex(value)
    except ValueError as exc:
        raise ProspectiveExecutionError(f"{name} is not a binary64 hex string") from exc
    if not isfinite(number) or number.hex() != value:
        raise ProspectiveExecutionError(f"{name} must be a canonical finite binary64 token")
    if positive and number <= 0.0:
        raise ProspectiveExecutionError(f"{name} must be positive")
    if nonnegative and (number < 0.0 or (number == 0.0 and value.startswith("-"))):
        raise ProspectiveExecutionError(f"{name} must be non-negative")
    return value


def _optional_float_token(
    value: Any,
    name: str,
    *,
    positive: bool = False,
    nonnegative: bool = False,
) -> str | None:
    if value is None:
        return None
    return _float_token(value, name, positive=positive, nonnegative=nonnegative)


def _decode(value: str | None) -> float | None:
    return None if value is None else float.fromhex(value)


def _fen(value_yuan: float) -> int:
    if not isfinite(value_yuan) or value_yuan < -1e-8:
        raise ProspectiveExecutionError("accounting value must be finite and non-negative")
    return int(round(max(value_yuan, 0.0) * 100.0))


def _scaled_return(value: float) -> int:
    if not isfinite(value) or value < -1.0 - 1e-12:
        raise ProspectiveExecutionError("return must be finite and no less than -100%")
    return int(round(max(value, -1.0) * RETURN_SCALE_PPB))


def _exact_keys(value: Any, required: set[str], name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != required or len(value) != len(required):
        raise ProspectiveExecutionError(f"{name} does not have the exact schema")
    return value


@dataclass(frozen=True)
class ExecutionContract:
    """Integer encoding of the frozen 4.1/5.0 execution contract."""

    max_adv_participation_ppm: int = 50_000
    max_position_weight_ppm: int = POSITION_WEIGHT_PPM
    lot_size: int = 0
    max_stale_position_age_days: int = 21
    commission_rate_ppb: int = 300_000
    slippage_rate_per_side_ppb: int = 500_000
    stamp_duty_before_2023_08_28_ppb: int = 1_000_000
    stamp_duty_from_2023_08_28_ppb: int = 500_000
    exchange_handling_rate_ppb: int = 34_100
    transfer_fee_rate_ppb: int = 10_000
    impact_coefficient_ppm: int = 500_000
    price_basis: str = "adjusted_total_return"
    execution_price_column: str = "open_adj"
    execution_input_policy: str = "previous_valid_ticker_observation"
    corporate_action_mode: str = "embedded_in_adjusted_prices"
    outcome_status_policy: str = "deterministic_complete_only"
    outcome_daily_path: str = "every_holding_session_posttrade_start_through_end"
    benchmark_accounting: str = (
        "frozen_decision_roster_equal_weight_start_missing_as_cash_"
        "suspension_carry_delist_zero_no_endpoint_reweight"
    )
    holding_days: int = HOLDING_DAYS
    schema_version: int = SCHEMA_VERSION
    engine_id: str = ENGINE_ID
    contract_sha256: str = ""

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int:
            raise ProspectiveExecutionError("execution contract schema_version must be an integer")
        expected = {
            "max_adv_participation_ppm": 50_000,
            "max_position_weight_ppm": POSITION_WEIGHT_PPM,
            "lot_size": 0,
            "max_stale_position_age_days": 21,
            "commission_rate_ppb": 300_000,
            "slippage_rate_per_side_ppb": 500_000,
            "stamp_duty_before_2023_08_28_ppb": 1_000_000,
            "stamp_duty_from_2023_08_28_ppb": 500_000,
            "exchange_handling_rate_ppb": 34_100,
            "transfer_fee_rate_ppb": 10_000,
            "impact_coefficient_ppm": 500_000,
            "price_basis": "adjusted_total_return",
            "execution_price_column": "open_adj",
            "execution_input_policy": "previous_valid_ticker_observation",
            "corporate_action_mode": "embedded_in_adjusted_prices",
            "outcome_status_policy": "deterministic_complete_only",
            "outcome_daily_path": "every_holding_session_posttrade_start_through_end",
            "benchmark_accounting": (
                "frozen_decision_roster_equal_weight_start_missing_as_cash_"
                "suspension_carry_delist_zero_no_endpoint_reweight"
            ),
            "holding_days": HOLDING_DAYS,
            "schema_version": SCHEMA_VERSION,
            "engine_id": ENGINE_ID,
        }
        for name, wanted in expected.items():
            if getattr(self, name) != wanted:
                raise ProspectiveExecutionError(f"execution contract {name} is frozen at {wanted!r}")
        expected_sha = _sha256_payload(self.payload())
        if self.contract_sha256 and self.contract_sha256 != expected_sha:
            raise ProspectiveExecutionError("contract_sha256 does not match its canonical payload")
        object.__setattr__(self, "contract_sha256", expected_sha)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "holding_days": self.holding_days,
            "max_adv_participation_ppm": self.max_adv_participation_ppm,
            "max_position_weight_ppm": self.max_position_weight_ppm,
            "lot_size": self.lot_size,
            "max_stale_position_age_days": self.max_stale_position_age_days,
            "commission_rate_ppb": self.commission_rate_ppb,
            "slippage_rate_per_side_ppb": self.slippage_rate_per_side_ppb,
            "stamp_duty_before_2023_08_28_ppb": self.stamp_duty_before_2023_08_28_ppb,
            "stamp_duty_from_2023_08_28_ppb": self.stamp_duty_from_2023_08_28_ppb,
            "exchange_handling_rate_ppb": self.exchange_handling_rate_ppb,
            "transfer_fee_rate_ppb": self.transfer_fee_rate_ppb,
            "impact_coefficient_ppm": self.impact_coefficient_ppm,
            "price_basis": self.price_basis,
            "execution_price_column": self.execution_price_column,
            "execution_input_policy": self.execution_input_policy,
            "corporate_action_mode": self.corporate_action_mode,
            "outcome_status_policy": self.outcome_status_policy,
            "outcome_daily_path": self.outcome_daily_path,
            "benchmark_accounting": self.benchmark_accounting,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "contract_sha256": self.contract_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ExecutionContract":
        required = set(cls().to_dict())
        _exact_keys(value, required, "execution contract")
        _sha256(value["contract_sha256"], "contract_sha256")
        return cls(**dict(value))

    def execution_policy(self) -> ExecutionPolicy:
        ppb = float(RETURN_SCALE_PPB)
        return ExecutionPolicy(
            max_adv_participation=self.max_adv_participation_ppm / WEIGHT_SCALE_PPM,
            max_position_weight=self.max_position_weight_ppm / WEIGHT_SCALE_PPM,
            lot_size=self.lot_size,
            max_stale_position_age_days=self.max_stale_position_age_days,
            costs=AShareCostPolicy(
                commission_rate=self.commission_rate_ppb / ppb,
                slippage_bps_per_side=(self.slippage_rate_per_side_ppb / ppb) * 10_000.0,
                stamp_duty_before_2023_08_28=(
                    self.stamp_duty_before_2023_08_28_ppb / ppb
                ),
                stamp_duty_from_2023_08_28=(
                    self.stamp_duty_from_2023_08_28_ppb / ppb
                ),
                exchange_handling_rate=self.exchange_handling_rate_ppb / ppb,
                transfer_fee_rate=self.transfer_fee_rate_ppb / ppb,
                impact_coefficient=self.impact_coefficient_ppm / WEIGHT_SCALE_PPM,
            ),
        )


FROZEN_EXECUTION_CONTRACT = ExecutionContract()


@dataclass(frozen=True)
class AccountPosition:
    ticker: str
    quantity_hex: str
    last_price_hex: str
    average_cost_hex: str
    last_observation_date: str | None
    stale_since_date: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "ticker", _ticker(self.ticker))
        object.__setattr__(
            self, "quantity_hex", _float_token(self.quantity_hex, "quantity_hex", positive=True)
        )
        object.__setattr__(
            self, "last_price_hex", _float_token(self.last_price_hex, "last_price_hex", positive=True)
        )
        object.__setattr__(
            self,
            "average_cost_hex",
            _float_token(self.average_cost_hex, "average_cost_hex", positive=True),
        )
        if self.last_observation_date is not None:
            object.__setattr__(
                self,
                "last_observation_date",
                _date(self.last_observation_date, "last_observation_date"),
            )
        if self.stale_since_date is not None:
            object.__setattr__(
                self, "stale_since_date", _date(self.stale_since_date, "stale_since_date")
            )
        if (
            self.last_observation_date is not None
            and self.stale_since_date is not None
            and self.stale_since_date < self.last_observation_date
        ):
            raise ProspectiveExecutionError("stale_since_date precedes last_observation_date")

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "quantity_hex": self.quantity_hex,
            "last_price_hex": self.last_price_hex,
            "average_cost_hex": self.average_cost_hex,
            "last_observation_date": self.last_observation_date,
            "stale_since_date": self.stale_since_date,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AccountPosition":
        required = {
            "ticker",
            "quantity_hex",
            "last_price_hex",
            "average_cost_hex",
            "last_observation_date",
            "stale_since_date",
        }
        _exact_keys(value, required, "account position")
        return cls(**dict(value))


@dataclass(frozen=True)
class SleeveAccountState:
    """Accounting state for one offset; never used as target-retention state."""

    deployment_sha256: str
    offset: int
    cycle_count: int
    cash_hex: str
    positions: Sequence[AccountPosition | Mapping[str, Any]]
    nav_fen: int
    last_holding_end_date: str | None = None
    last_generation_result_sha256: str | None = None
    last_execution_snapshot_sha256: str | None = None
    initial_capital_fen: int = SLEEVE_CAPITAL_FEN
    schema_version: int = SCHEMA_VERSION
    engine_id: str = ENGINE_ID
    state_sha256: str = ""

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != SCHEMA_VERSION
            or self.engine_id != ENGINE_ID
        ):
            raise ProspectiveExecutionError("unsupported accounting state version/engine")
        object.__setattr__(
            self, "deployment_sha256", _sha256(self.deployment_sha256, "deployment_sha256")
        )
        if type(self.offset) is not int or not 0 <= self.offset < OFFSET_COUNT:
            raise ProspectiveExecutionError("account offset must be an integer in [0, 10)")
        _int(self.cycle_count, "cycle_count", minimum=0)
        if self.initial_capital_fen != SLEEVE_CAPITAL_FEN:
            raise ProspectiveExecutionError(
                f"initial_capital_fen is frozen at {SLEEVE_CAPITAL_FEN}"
            )
        object.__setattr__(
            self, "cash_hex", _float_token(self.cash_hex, "cash_hex", nonnegative=True)
        )
        normalized: list[AccountPosition] = []
        for item in self.positions:
            if isinstance(item, AccountPosition):
                normalized.append(item)
            elif isinstance(item, Mapping):
                normalized.append(AccountPosition.from_mapping(item))
            else:
                raise ProspectiveExecutionError("positions must contain strict position mappings")
        normalized.sort(key=lambda row: row.ticker)
        if len({row.ticker for row in normalized}) != len(normalized):
            raise ProspectiveExecutionError("account contains duplicate positions")
        object.__setattr__(self, "positions", tuple(normalized))
        computed_nav_fen = _fen(
            float.fromhex(self.cash_hex)
            + sum(
                float.fromhex(row.quantity_hex) * float.fromhex(row.last_price_hex)
                for row in normalized
            )
        )
        _int(self.nav_fen, "nav_fen", minimum=0)
        if self.nav_fen != computed_nav_fen:
            raise ProspectiveExecutionError("nav_fen does not reconcile to cash and positions")
        history = (
            self.last_holding_end_date,
            self.last_generation_result_sha256,
            self.last_execution_snapshot_sha256,
        )
        if self.cycle_count == 0:
            if any(value is not None for value in history) or normalized:
                raise ProspectiveExecutionError("genesis accounting state cannot contain history")
            if float.fromhex(self.cash_hex) != self.initial_capital_fen / 100.0:
                raise ProspectiveExecutionError("genesis accounting state must be all CNY 5m cash")
        else:
            if any(value is None for value in history):
                raise ProspectiveExecutionError("non-genesis accounting state requires cycle bindings")
            object.__setattr__(
                self,
                "last_holding_end_date",
                _date(self.last_holding_end_date, "last_holding_end_date"),
            )
            object.__setattr__(
                self,
                "last_generation_result_sha256",
                _sha256(
                    self.last_generation_result_sha256,
                    "last_generation_result_sha256",
                ),
            )
            object.__setattr__(
                self,
                "last_execution_snapshot_sha256",
                _sha256(
                    self.last_execution_snapshot_sha256,
                    "last_execution_snapshot_sha256",
                ),
            )
        expected = _sha256_payload(self.payload())
        if self.state_sha256 and self.state_sha256 != expected:
            raise ProspectiveExecutionError("state_sha256 does not match accounting state")
        object.__setattr__(self, "state_sha256", expected)

    @classmethod
    def genesis(cls, *, deployment_sha256: str, offset: int) -> "SleeveAccountState":
        cash = (SLEEVE_CAPITAL_FEN / 100.0).hex()
        return cls(
            deployment_sha256=deployment_sha256,
            offset=offset,
            cycle_count=0,
            cash_hex=cash,
            positions=(),
            nav_fen=SLEEVE_CAPITAL_FEN,
        )

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "deployment_sha256": self.deployment_sha256,
            "offset": self.offset,
            "initial_capital_fen": self.initial_capital_fen,
            "cycle_count": self.cycle_count,
            "last_holding_end_date": self.last_holding_end_date,
            "last_generation_result_sha256": self.last_generation_result_sha256,
            "last_execution_snapshot_sha256": self.last_execution_snapshot_sha256,
            "cash_hex": self.cash_hex,
            "positions": [row.to_dict() for row in self.positions],
            "nav_fen": self.nav_fen,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "state_sha256": self.state_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SleeveAccountState":
        required = {
            "schema_version",
            "engine_id",
            "deployment_sha256",
            "offset",
            "initial_capital_fen",
            "cycle_count",
            "last_holding_end_date",
            "last_generation_result_sha256",
            "last_execution_snapshot_sha256",
            "cash_hex",
            "positions",
            "nav_fen",
            "state_sha256",
        }
        _exact_keys(value, required, "sleeve accounting state")
        _sha256(value["state_sha256"], "state_sha256")
        return cls(**dict(value))


@dataclass(frozen=True)
class ExecutionObservation:
    """One allowlisted adjusted-open observation in an outcome snapshot."""

    date: str
    ticker: str
    open_adj_hex: str | None
    adv_20_asof_hex: str | None
    volatility_20_asof_hex: str | None
    execution_input_date: str | None
    is_one_price_limit_up: bool = False
    is_one_price_limit_down: bool = False
    is_suspended: bool = False
    is_delisted: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "date", _date(self.date, "observation.date"))
        object.__setattr__(self, "ticker", _ticker(self.ticker, "observation.ticker"))
        object.__setattr__(
            self,
            "open_adj_hex",
            _optional_float_token(self.open_adj_hex, "open_adj_hex", positive=True),
        )
        object.__setattr__(
            self,
            "adv_20_asof_hex",
            _optional_float_token(self.adv_20_asof_hex, "adv_20_asof_hex", positive=True),
        )
        object.__setattr__(
            self,
            "volatility_20_asof_hex",
            _optional_float_token(
                self.volatility_20_asof_hex,
                "volatility_20_asof_hex",
                nonnegative=True,
            ),
        )
        if self.execution_input_date is not None:
            object.__setattr__(
                self,
                "execution_input_date",
                _date(self.execution_input_date, "execution_input_date"),
            )
        for name in (
            "is_one_price_limit_up",
            "is_one_price_limit_down",
            "is_suspended",
            "is_delisted",
        ):
            _bool(getattr(self, name), name)
        if self.is_suspended and self.open_adj_hex is not None:
            raise ProspectiveExecutionError("a suspended observation cannot publish an open")
        if self.is_delisted and self.open_adj_hex is not None:
            raise ProspectiveExecutionError("a delisted observation cannot publish an open")

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "ticker": self.ticker,
            "open_adj_hex": self.open_adj_hex,
            "adv_20_asof_hex": self.adv_20_asof_hex,
            "volatility_20_asof_hex": self.volatility_20_asof_hex,
            "execution_input_date": self.execution_input_date,
            "is_one_price_limit_up": self.is_one_price_limit_up,
            "is_one_price_limit_down": self.is_one_price_limit_down,
            "is_suspended": self.is_suspended,
            "is_delisted": self.is_delisted,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ExecutionObservation":
        required = {
            "date",
            "ticker",
            "open_adj_hex",
            "adv_20_asof_hex",
            "volatility_20_asof_hex",
            "execution_input_date",
            "is_one_price_limit_up",
            "is_one_price_limit_down",
            "is_suspended",
            "is_delisted",
        }
        _exact_keys(value, required, "execution observation")
        return cls(**dict(value))


@dataclass(frozen=True)
class ExecutionSnapshot:
    """Content-addressed market/calendar evidence for exactly one cycle."""

    generation_result_sha256: str
    execution_source_sha256: str
    official_calendar_sha256: str
    signal_date: str
    holding_start_date: str
    holding_end_date: str
    calendar_sessions: Sequence[Any]
    benchmark_tickers: Sequence[Any]
    rows: Sequence[ExecutionObservation | Mapping[str, Any]]
    calendar_available_at_utc: str
    decision_inputs_available_at_utc: str
    trade_deadline_utc: str
    start_open_available_at_utc: str
    end_open_available_at_utc: str
    observation_available_at_utc: str
    contract_sha256: str = FROZEN_EXECUTION_CONTRACT.contract_sha256
    holding_days: int = HOLDING_DAYS
    schema_version: int = SCHEMA_VERSION
    engine_id: str = ENGINE_ID
    benchmark_tickers_sha256: str = ""
    snapshot_sha256: str = ""

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != SCHEMA_VERSION
            or self.engine_id != ENGINE_ID
            or self.holding_days != HOLDING_DAYS
        ):
            raise ProspectiveExecutionError("unsupported execution snapshot version/engine/holding period")
        for name in (
            "generation_result_sha256",
            "execution_source_sha256",
            "official_calendar_sha256",
            "contract_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        if self.contract_sha256 != FROZEN_EXECUTION_CONTRACT.contract_sha256:
            raise ProspectiveExecutionError("execution snapshot is bound to a different contract")
        signal = _date(self.signal_date, "signal_date")
        start = _date(self.holding_start_date, "holding_start_date")
        end = _date(self.holding_end_date, "holding_end_date")
        object.__setattr__(self, "signal_date", signal)
        object.__setattr__(self, "holding_start_date", start)
        object.__setattr__(self, "holding_end_date", end)
        sessions = tuple(
            _date(value, f"calendar_sessions[{index}]")
            for index, value in enumerate(self.calendar_sessions)
        )
        if not sessions or any(left >= right for left, right in zip(sessions, sessions[1:])):
            raise ProspectiveExecutionError("calendar_sessions must be unique and strictly increasing")
        try:
            signal_index = sessions.index(signal)
        except ValueError as exc:
            raise ProspectiveExecutionError("signal_date is absent from official calendar") from exc
        if signal_index + HOLDING_DAYS + 1 >= len(sessions):
            raise ProspectiveExecutionError("official calendar does not reach the holding end")
        if sessions[signal_index + 1] != start:
            raise ProspectiveExecutionError("holding start must be the next official session")
        if sessions[signal_index + HOLDING_DAYS + 1] != end:
            raise ProspectiveExecutionError("holding end must be signal index plus eleven")
        if sessions[-1] != end:
            raise ProspectiveExecutionError("outcome calendar must end exactly at holding_end_date")
        if calendar_prefix_sha256(sessions) != self.official_calendar_sha256:
            raise ProspectiveExecutionError("official calendar SHA-256 does not match its sessions")
        object.__setattr__(self, "calendar_sessions", sessions)

        tickers = tuple(sorted(_ticker(value, "benchmark ticker") for value in self.benchmark_tickers))
        if not tickers or len(set(tickers)) != len(tickers):
            raise ProspectiveExecutionError("benchmark_tickers must be non-empty and unique")
        object.__setattr__(self, "benchmark_tickers", tickers)
        tickers_sha = _sha256_payload(list(tickers))
        if self.benchmark_tickers_sha256 and self.benchmark_tickers_sha256 != tickers_sha:
            raise ProspectiveExecutionError("benchmark_tickers_sha256 does not match its roster")
        object.__setattr__(self, "benchmark_tickers_sha256", tickers_sha)

        normalized_rows: list[ExecutionObservation] = []
        for item in self.rows:
            if isinstance(item, ExecutionObservation):
                normalized_rows.append(item)
            elif isinstance(item, Mapping):
                normalized_rows.append(ExecutionObservation.from_mapping(item))
            else:
                raise ProspectiveExecutionError("execution rows must contain strict mappings")
        normalized_rows.sort(key=lambda row: (row.date, row.ticker))
        keys = [(row.date, row.ticker) for row in normalized_rows]
        if len(set(keys)) != len(keys):
            raise ProspectiveExecutionError("execution snapshot contains duplicate date/ticker rows")
        window_sessions = sessions[signal_index + 1 : signal_index + HOLDING_DAYS + 2]
        if not normalized_rows or any(row.date not in window_sessions for row in normalized_rows):
            raise ProspectiveExecutionError("execution rows must be restricted to start-through-end sessions")
        row_tickers = tuple(sorted({row.ticker for row in normalized_rows}))
        if not set(tickers).issubset(row_tickers):
            raise ProspectiveExecutionError("execution rows omit benchmark securities")
        expected_keys = {
            (session, ticker) for session in window_sessions for ticker in row_tickers
        }
        if set(keys) != expected_keys:
            raise ProspectiveExecutionError(
                "execution rows must form a complete session/security rectangle"
            )
        for row in normalized_rows:
            input_values = (
                row.adv_20_asof_hex,
                row.volatility_20_asof_hex,
                row.execution_input_date,
            )
            if row.date != start and any(value is not None for value in input_values):
                raise ProspectiveExecutionError(
                    "only holding-start rows may contain causal execution inputs"
                )
            if row.date == start:
                present_count = sum(value is not None for value in input_values)
                if present_count not in {0, 3}:
                    raise ProspectiveExecutionError(
                        "holding-start execution inputs must be all present or all absent"
                    )
                if row.execution_input_date is not None and row.execution_input_date > signal:
                    raise ProspectiveExecutionError(
                        "execution input date is later than the signal close session"
                    )
        object.__setattr__(self, "rows", tuple(normalized_rows))

        timestamp_names = (
            "calendar_available_at_utc",
            "decision_inputs_available_at_utc",
            "trade_deadline_utc",
            "start_open_available_at_utc",
            "end_open_available_at_utc",
            "observation_available_at_utc",
        )
        for name in timestamp_names:
            object.__setattr__(self, name, _utc(getattr(self, name), name))
        calendar_available = _utc_datetime(self.calendar_available_at_utc)
        inputs_available = _utc_datetime(self.decision_inputs_available_at_utc)
        deadline = _utc_datetime(self.trade_deadline_utc)
        start_available = _utc_datetime(self.start_open_available_at_utc)
        end_available = _utc_datetime(self.end_open_available_at_utc)
        observation_available = _utc_datetime(self.observation_available_at_utc)
        if not (
            max(calendar_available, inputs_available)
            <= deadline
            < start_available
            <= end_available
            <= observation_available
        ):
            raise ProspectiveExecutionError(
                "timestamps must prove calendar/inputs before deadline and opens before outcome"
            )
        expected = _sha256_payload(self.payload())
        if self.snapshot_sha256 and self.snapshot_sha256 != expected:
            raise ProspectiveExecutionError("snapshot_sha256 does not match execution evidence")
        object.__setattr__(self, "snapshot_sha256", expected)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "contract_sha256": self.contract_sha256,
            "holding_days": self.holding_days,
            "generation_result_sha256": self.generation_result_sha256,
            "execution_source_sha256": self.execution_source_sha256,
            "official_calendar_sha256": self.official_calendar_sha256,
            "signal_date": self.signal_date,
            "holding_start_date": self.holding_start_date,
            "holding_end_date": self.holding_end_date,
            "calendar_sessions": list(self.calendar_sessions),
            "benchmark_tickers": list(self.benchmark_tickers),
            "benchmark_tickers_sha256": self.benchmark_tickers_sha256,
            "rows": [row.to_dict() for row in self.rows],
            "calendar_available_at_utc": self.calendar_available_at_utc,
            "decision_inputs_available_at_utc": self.decision_inputs_available_at_utc,
            "trade_deadline_utc": self.trade_deadline_utc,
            "start_open_available_at_utc": self.start_open_available_at_utc,
            "end_open_available_at_utc": self.end_open_available_at_utc,
            "observation_available_at_utc": self.observation_available_at_utc,
        }

    def to_dict(self) -> dict[str, Any]:
        return {**self.payload(), "snapshot_sha256": self.snapshot_sha256}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ExecutionSnapshot":
        required = {
            "schema_version",
            "engine_id",
            "contract_sha256",
            "holding_days",
            "generation_result_sha256",
            "execution_source_sha256",
            "official_calendar_sha256",
            "signal_date",
            "holding_start_date",
            "holding_end_date",
            "calendar_sessions",
            "benchmark_tickers",
            "benchmark_tickers_sha256",
            "rows",
            "calendar_available_at_utc",
            "decision_inputs_available_at_utc",
            "trade_deadline_utc",
            "start_open_available_at_utc",
            "end_open_available_at_utc",
            "observation_available_at_utc",
            "snapshot_sha256",
        }
        _exact_keys(value, required, "execution snapshot")
        _sha256(value["benchmark_tickers_sha256"], "benchmark_tickers_sha256")
        _sha256(value["snapshot_sha256"], "snapshot_sha256")
        return cls(**dict(value))


@dataclass(frozen=True)
class CycleDailyObservation:
    """One immutable holding-session strategy NAV and benchmark index point."""

    date: str
    account_nav_fen: int
    benchmark_index_ppb: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "date", _date(self.date, "cycle daily observation date"))
        _int(self.account_nav_fen, "account_nav_fen", minimum=0)
        _int(self.benchmark_index_ppb, "benchmark_index_ppb", minimum=0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "account_nav_fen": self.account_nav_fen,
            "benchmark_index_ppb": self.benchmark_index_ppb,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "CycleDailyObservation":
        _exact_keys(
            value,
            {"date", "account_nav_fen", "benchmark_index_ppb"},
            "cycle daily observation",
        )
        return cls(**dict(value))


@dataclass(frozen=True)
class CycleOutcome:
    """Replayable result plus an exact adapter for ledger-v2 outcome fields."""

    contract_sha256: str
    deployment_sha256: str
    generation_result_sha256: str
    execution_snapshot_sha256: str
    previous_account_state_sha256: str
    offset: int
    signal_date: str
    holding_start_date: str
    holding_end_date: str
    observation_available_at_utc: str
    execution_status: str
    not_executed_reason: str | None
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
            raise ProspectiveExecutionError("unsupported cycle outcome version/engine")
        for name in (
            "contract_sha256",
            "deployment_sha256",
            "generation_result_sha256",
            "execution_snapshot_sha256",
            "previous_account_state_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        if self.contract_sha256 != FROZEN_EXECUTION_CONTRACT.contract_sha256:
            raise ProspectiveExecutionError("cycle outcome is bound to a different contract")
        if type(self.offset) is not int or not 0 <= self.offset < OFFSET_COUNT:
            raise ProspectiveExecutionError("cycle outcome offset is invalid")
        for name in ("signal_date", "holding_start_date", "holding_end_date"):
            object.__setattr__(self, name, _date(getattr(self, name), name))
        object.__setattr__(
            self,
            "observation_available_at_utc",
            _utc(self.observation_available_at_utc, "observation_available_at_utc"),
        )
        if not self.signal_date < self.holding_start_date <= self.holding_end_date:
            raise ProspectiveExecutionError("cycle outcome dates are not ordered")
        if self.execution_status != "complete" or self.not_executed_reason is not None:
            raise ProspectiveExecutionError(
                "5.2 cycle outcomes must be deterministic complete executions"
            )
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
            _int(getattr(self, name), name, minimum=0)
        if self.opening_nav_fen <= 0 or self.pretrade_nav_fen <= 0:
            raise ProspectiveExecutionError("cycle opening/pretrade NAV must be positive")
        for name in ("gross_return_ppb", "net_return_ppb", "benchmark_return_ppb"):
            _int(getattr(self, name), name, minimum=-RETURN_SCALE_PPB)
        if (
            self.benchmark_expected_count <= 0
            or self.benchmark_complete_count != self.benchmark_expected_count
        ):
            raise ProspectiveExecutionError(
                "stateful benchmark must account for every frozen roster member"
            )
        daily_path: list[CycleDailyObservation] = []
        for item in self.daily_path:
            if isinstance(item, CycleDailyObservation):
                daily_path.append(item)
            elif isinstance(item, Mapping):
                daily_path.append(CycleDailyObservation.from_mapping(item))
            else:
                raise ProspectiveExecutionError(
                    "daily_path must contain strict daily observation mappings"
                )
        if len(daily_path) != HOLDING_DAYS + 1:
            raise ProspectiveExecutionError(
                "daily_path must contain every holding session from start through end"
            )
        if any(left.date >= right.date for left, right in zip(daily_path, daily_path[1:])):
            raise ProspectiveExecutionError("daily_path dates must be strictly increasing")
        if (
            daily_path[0].date != self.holding_start_date
            or daily_path[-1].date != self.holding_end_date
            or daily_path[-1].account_nav_fen != self.ending_nav_fen
            or daily_path[0].benchmark_index_ppb != RETURN_SCALE_PPB
            or daily_path[-1].benchmark_index_ppb - RETURN_SCALE_PPB
            != self.benchmark_return_ppb
        ):
            raise ProspectiveExecutionError(
                "daily_path does not reconcile to cycle dates, ending NAV, or benchmark return"
            )
        object.__setattr__(self, "daily_path", tuple(daily_path))
        next_state = self.next_account_state
        if isinstance(next_state, Mapping):
            next_state = SleeveAccountState.from_mapping(next_state)
        if not isinstance(next_state, SleeveAccountState):
            raise ProspectiveExecutionError("next_account_state has an invalid type")
        object.__setattr__(self, "next_account_state", next_state)
        if (
            next_state.offset != self.offset
            or next_state.deployment_sha256 != self.deployment_sha256
            or next_state.last_holding_end_date != self.holding_end_date
            or next_state.last_generation_result_sha256 != self.generation_result_sha256
            or next_state.last_execution_snapshot_sha256 != self.execution_snapshot_sha256
            or next_state.nav_fen != self.ending_nav_fen
        ):
            raise ProspectiveExecutionError("cycle outcome does not reconcile to next account state")
        expected = _sha256_payload(self.payload())
        if self.outcome_sha256 and self.outcome_sha256 != expected:
            raise ProspectiveExecutionError("outcome_sha256 does not match cycle outcome")
        object.__setattr__(self, "outcome_sha256", expected)

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "engine_id": self.engine_id,
            "contract_sha256": self.contract_sha256,
            "deployment_sha256": self.deployment_sha256,
            "generation_result_sha256": self.generation_result_sha256,
            "execution_snapshot_sha256": self.execution_snapshot_sha256,
            "previous_account_state_sha256": self.previous_account_state_sha256,
            "offset": self.offset,
            "signal_date": self.signal_date,
            "holding_start_date": self.holding_start_date,
            "holding_end_date": self.holding_end_date,
            "observation_available_at_utc": self.observation_available_at_utc,
            "execution_status": self.execution_status,
            "not_executed_reason": self.not_executed_reason,
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
    def from_mapping(cls, value: Mapping[str, Any]) -> "CycleOutcome":
        required = {
            "schema_version",
            "engine_id",
            "contract_sha256",
            "deployment_sha256",
            "generation_result_sha256",
            "execution_snapshot_sha256",
            "previous_account_state_sha256",
            "offset",
            "signal_date",
            "holding_start_date",
            "holding_end_date",
            "observation_available_at_utc",
            "execution_status",
            "not_executed_reason",
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
        _exact_keys(value, required, "cycle outcome")
        _sha256(value["outcome_sha256"], "outcome_sha256")
        return cls(**dict(value))

    def to_ledger_v2_outcome(
        self,
        *,
        decision_record_sha256: str,
        attestation_receipt_record_sha256: str,
    ) -> dict[str, Any]:
        """Return the exact rich ledger-v2 outcome envelope.

        The embedded, self-hashed :class:`CycleOutcome` is authoritative.
        Ledger append and audit can therefore recover the generation,
        execution and accounting-state chain instead of trusting duplicated
        hand-filled scalar fields.
        """

        return {
            "schema_version": 2,
            "decision_record_sha256": _sha256(
                decision_record_sha256, "decision_record_sha256"
            ),
            "attestation_receipt_record_sha256": _sha256(
                attestation_receipt_record_sha256,
                "attestation_receipt_record_sha256",
            ),
            "execution_snapshot_sha256": self.execution_snapshot_sha256,
            "cycle_outcome_sha256": self.outcome_sha256,
            "cycle_outcome": self.to_dict(),
        }


_EXECUTION_COLUMNS = ExecutionColumns(
    open="open_adj",
    mark="open_adj",
    adv="adv_20_asof",
    volatility="volatility_20_asof",
    limit_up="is_one_price_limit_up",
    limit_down="is_one_price_limit_down",
    suspended="is_suspended",
    delisted="is_delisted",
    split_ratio=None,
    cash_dividend=None,
)


def _market_payload(row: ExecutionObservation) -> dict[str, Any]:
    return {
        "ticker": row.ticker,
        "open_adj": _decode(row.open_adj_hex),
        "adv_20_asof": _decode(row.adv_20_asof_hex),
        "volatility_20_asof": _decode(row.volatility_20_asof_hex),
        "is_one_price_limit_up": row.is_one_price_limit_up,
        "is_one_price_limit_down": row.is_one_price_limit_down,
        "is_suspended": row.is_suspended,
        "is_delisted": row.is_delisted,
    }


def _account_from_state(state: SleeveAccountState) -> ExecutionAccount:
    return ExecutionAccount(
        cash=float.fromhex(state.cash_hex),
        positions={
            row.ticker: ExecutionPosition(
                ticker=row.ticker,
                quantity=float.fromhex(row.quantity_hex),
                last_price=float.fromhex(row.last_price_hex),
                average_cost=float.fromhex(row.average_cost_hex),
                last_observation_date=row.last_observation_date,
                stale_since_date=row.stale_since_date,
            )
            for row in state.positions
        },
    )


def _state_from_account(
    account: ExecutionAccount,
    *,
    previous: SleeveAccountState,
    generation: GenerationResult,
    snapshot: ExecutionSnapshot,
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
    return SleeveAccountState(
        deployment_sha256=previous.deployment_sha256,
        offset=previous.offset,
        cycle_count=previous.cycle_count + 1,
        last_holding_end_date=snapshot.holding_end_date,
        last_generation_result_sha256=generation.result_sha256,
        last_execution_snapshot_sha256=snapshot.snapshot_sha256,
        cash_hex=max(account.cash, 0.0).hex(),
        positions=positions,
        nav_fen=_fen(account.nav()),
    )


def _benchmark_endpoint(row: ExecutionObservation) -> float | None:
    if row.is_suspended or row.is_delisted:
        return None
    return _decode(row.open_adj_hex)


def _generation(value: GenerationResult | Mapping[str, Any]) -> GenerationResult:
    if isinstance(value, GenerationResult):
        return value
    if isinstance(value, Mapping):
        return GenerationResult.from_mapping(value)
    raise ProspectiveExecutionError("generation_result must be GenerationResult or a strict mapping")


def _snapshot(value: ExecutionSnapshot | Mapping[str, Any]) -> ExecutionSnapshot:
    if isinstance(value, ExecutionSnapshot):
        return value
    if isinstance(value, Mapping):
        return ExecutionSnapshot.from_mapping(value)
    raise ProspectiveExecutionError("execution_snapshot must be ExecutionSnapshot or a strict mapping")


def _account_state(value: SleeveAccountState | Mapping[str, Any]) -> SleeveAccountState:
    if isinstance(value, SleeveAccountState):
        return value
    if isinstance(value, Mapping):
        return SleeveAccountState.from_mapping(value)
    raise ProspectiveExecutionError("previous_account_state must be a strict accounting state")


def evaluate_due_sleeve_cycle(
    *,
    generation_result: GenerationResult | Mapping[str, Any],
    execution_snapshot: ExecutionSnapshot | Mapping[str, Any],
    previous_account_state: SleeveAccountState | Mapping[str, Any],
) -> CycleOutcome:
    """Execute and observe one due-offset cycle with no external side effects.

    The 5.2 route has one deterministic production action: execute the sealed
    target under the frozen kernel.  Incomplete evidence always raises instead
    of exposing an outcome-time ``not_executed`` choice.
    """

    generation = _generation(generation_result)
    snapshot = _snapshot(execution_snapshot)
    previous = _account_state(previous_account_state)
    if generation.result_sha256 != snapshot.generation_result_sha256:
        raise ProspectiveExecutionError("execution snapshot is bound to another target result")
    if generation.deployment_sha256 != previous.deployment_sha256:
        raise ProspectiveExecutionError("selection and accounting deployments differ")
    if generation.due_offset != previous.offset:
        raise ProspectiveExecutionError("generation due offset differs from accounting sleeve")
    if generation.signal_date != snapshot.signal_date or generation.trade_date != snapshot.holding_start_date:
        raise ProspectiveExecutionError("generation and execution dates differ")
    if (
        generation.calendar_index >= len(snapshot.calendar_sessions)
        or snapshot.calendar_sessions[generation.calendar_index] != generation.signal_date
    ):
        raise ProspectiveExecutionError(
            "official outcome calendar changed the generation's absolute index"
        )
    if previous.cycle_count > 0 and previous.last_holding_end_date != snapshot.holding_start_date:
        raise ProspectiveExecutionError("same-offset accounting NAV is not continuous at the boundary")
    if previous.last_generation_result_sha256 == generation.result_sha256:
        raise ProspectiveExecutionError("generation result was already applied to this account")

    due_plan = generation.sleeve_plans[generation.due_offset]
    expected_action = "seed" if previous.cycle_count == 0 else "rebalance"
    if due_plan["action"] != expected_action:
        raise ProspectiveExecutionError(
            f"accounting cycle requires due sleeve action {expected_action!r}"
        )
    if int(due_plan["capital_fen"]) != SLEEVE_CAPITAL_FEN:
        raise ProspectiveExecutionError("due sleeve selection capital contract changed")
    targets_ppm = dict(due_plan["targets_ppm"])
    if not targets_ppm or any(weight != POSITION_WEIGHT_PPM for weight in targets_ppm.values()):
        raise ProspectiveExecutionError("due sleeve targets are not frozen equal-weight targets")
    target_weights = {
        ticker: weight / WEIGHT_SCALE_PPM for ticker, weight in sorted(targets_ppm.items())
    }

    rows_by_key = {(row.date, row.ticker): row for row in snapshot.rows}
    row_tickers = {row.ticker for row in snapshot.rows}
    required_tickers = set(target_weights) | {row.ticker for row in previous.positions}
    if not required_tickers.issubset(row_tickers):
        raise ProspectiveExecutionError("execution snapshot omits target or prior-held securities")
    start_rows = {
        ticker: rows_by_key[(snapshot.holding_start_date, ticker)]
        for ticker in row_tickers
    }
    for ticker in sorted(required_tickers):
        row = start_rows[ticker]
        if (
            row.execution_input_date is None
            or row.adv_20_asof_hex is None
            or row.volatility_20_asof_hex is None
        ):
            raise ProspectiveExecutionError(
                f"complete execution lacks causal ADV/volatility for {ticker}"
            )

    # A shared offset boundary is the same opening print in two adjacent
    # cycles.  Refuse a silent revision of the already sealed prior end mark.
    if previous.cycle_count > 0:
        previous_positions = {row.ticker: row for row in previous.positions}
        for ticker, position in previous_positions.items():
            row = start_rows[ticker]
            open_value = _decode(row.open_adj_hex)
            if open_value is not None and not row.is_suspended and not row.is_delisted:
                if (
                    position.last_observation_date != snapshot.holding_start_date
                    or position.last_price_hex != row.open_adj_hex
                ):
                    raise ProspectiveExecutionError(
                        f"shared-boundary mark changed for {ticker}"
                    )

    account = _account_from_state(previous)
    accounting_start_nav = account.nav()
    if accounting_start_nav <= 0.0:
        raise ProspectiveExecutionError("a zero-NAV sleeve cannot begin another cycle")
    policy = FROZEN_EXECUTION_CONTRACT.execution_policy()
    start_market = {
        ticker: _market_payload(row) for ticker, row in start_rows.items()
    }
    same_boundary_already_processed = previous.cycle_count > 0
    try:
        execution = execute_rebalance(
            account,
            target_weights,
            start_market,
            trade_date=snapshot.holding_start_date,
            policy=policy,
            columns=_EXECUTION_COLUMNS,
            ticker_column="ticker",
            process_corporate_actions=False,
            process_events=not same_boundary_already_processed,
        )
        accounting_start_nav = execution.accounting_start_nav
        pretrade_nav = execution.pretrade_nav
        fees_yuan = float(execution.costs["total"])
        traded_notional = execution.traded_notional
        executed_order_count = sum(order.status == "executed" for order in execution.orders)
        blocked_order_count = sum(order.status == "blocked" for order in execution.orders)

        sessions = snapshot.calendar_sessions
        signal_index = sessions.index(snapshot.signal_date)
        window_sessions = sessions[signal_index + 1 : signal_index + HOLDING_DAYS + 2]
        strategy_navs = [account.nav()]
        ending_nav = strategy_navs[0]
        for observation_date in window_sessions[1:]:
            held = tuple(account.positions)
            market = {
                ticker: _market_payload(rows_by_key[(observation_date, ticker)])
                for ticker in held
            }
            observed = process_account_observation(
                account,
                market,
                observation_date=observation_date,
                policy=policy,
                columns=_EXECUTION_COLUMNS,
                ticker_column="ticker",
                mark_at_open=True,
                process_corporate_actions=False,
                process_events=True,
            )
            ending_nav = observed.nav
            strategy_navs.append(ending_nav)
    except (ValueError, RuntimeError) as exc:
        raise ProspectiveExecutionError(f"frozen execution kernel failed closed: {exc}") from exc

    # Freeze equal decision-time weights.  A constituent without a tradable
    # start open keeps its allocation in cash for the whole cycle.  A funded
    # constituent is marked at later valid opens, carried through suspension,
    # and written to zero at the first delisted observation.  No outcome-time
    # endpoint filtering or survivor reweighting is permitted.
    benchmark_count = len(snapshot.benchmark_tickers)
    allocation = 1.0 / benchmark_count
    benchmark_cash = 0.0
    benchmark_units: dict[str, float] = {}
    benchmark_last_prices: dict[str, float] = {}
    benchmark_delisted: set[str] = set()
    for ticker in snapshot.benchmark_tickers:
        row = rows_by_key[(snapshot.holding_start_date, ticker)]
        price = _benchmark_endpoint(row)
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
            price = _decode(row.open_adj_hex)
            if price is not None and not row.is_suspended:
                benchmark_last_prices[ticker] = price
            elif not row.is_suspended:
                raise ProspectiveExecutionError(
                    f"benchmark observation lacks an explained open for {ticker}"
                )
        benchmark_indices.append(
            benchmark_cash
            + sum(
                benchmark_units[ticker] * benchmark_last_prices[ticker]
                for ticker in benchmark_units
            )
        )
    benchmark_index_ppb = [
        _scaled_return(value - 1.0) + RETURN_SCALE_PPB
        for value in benchmark_indices
    ]
    benchmark_return = benchmark_indices[-1] - 1.0
    net_return = ending_nav / accounting_start_nav - 1.0
    gross_return = (ending_nav + fees_yuan) / accounting_start_nav - 1.0
    turnover = traded_notional / pretrade_nav if pretrade_nav > 0.0 else 0.0
    next_state = _state_from_account(
        account,
        previous=previous,
        generation=generation,
        snapshot=snapshot,
    )
    return CycleOutcome(
        contract_sha256=FROZEN_EXECUTION_CONTRACT.contract_sha256,
        deployment_sha256=generation.deployment_sha256,
        generation_result_sha256=generation.result_sha256,
        execution_snapshot_sha256=snapshot.snapshot_sha256,
        previous_account_state_sha256=previous.state_sha256,
        offset=generation.due_offset,
        signal_date=generation.signal_date,
        holding_start_date=snapshot.holding_start_date,
        holding_end_date=snapshot.holding_end_date,
        observation_available_at_utc=snapshot.observation_available_at_utc,
        execution_status="complete",
        not_executed_reason=None,
        opening_nav_fen=_fen(accounting_start_nav),
        pretrade_nav_fen=_fen(pretrade_nav),
        ending_nav_fen=_fen(ending_nav),
        gross_return_ppb=_scaled_return(gross_return),
        net_return_ppb=_scaled_return(net_return),
        benchmark_return_ppb=_scaled_return(benchmark_return),
        turnover_ppm=int(round(turnover * WEIGHT_SCALE_PPM)),
        fees_fen=_fen(fees_yuan),
        executed_order_count=executed_order_count,
        blocked_order_count=blocked_order_count,
        benchmark_expected_count=benchmark_count,
        benchmark_complete_count=benchmark_count,
        daily_path=tuple(
            CycleDailyObservation(
                date=observation_date,
                account_nav_fen=_fen(strategy_nav),
                benchmark_index_ppb=benchmark_index,
            )
            for observation_date, strategy_nav, benchmark_index in zip(
                window_sessions, strategy_navs, benchmark_index_ppb, strict=True
            )
        ),
        next_account_state=next_state,
    )


__all__ = [
    "AccountPosition",
    "CycleOutcome",
    "CycleDailyObservation",
    "ENGINE_ID",
    "ExecutionContract",
    "ExecutionObservation",
    "ExecutionSnapshot",
    "FROZEN_EXECUTION_CONTRACT",
    "HOLDING_DAYS",
    "ProspectiveExecutionError",
    "SCHEMA_VERSION",
    "SleeveAccountState",
    "evaluate_due_sleeve_cycle",
]
