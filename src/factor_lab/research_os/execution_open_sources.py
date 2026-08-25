"""Typed live-opening source adapters for shadow-account execution evidence.

The ordinary data-plane adapters intentionally accept historical responses.
That is the wrong trust boundary for a forward shadow fill: an event stamped
09:30 can be downloaded hours later.  This module therefore keeps the live
collector contract small and explicit.  A formal observation is accepted only
when the official real-time endpoint returns the current session's 09:30
``1MIN`` row during the bounded collection window.  Availability is the local
collector receive time, never the provider's event timestamp.

Tushare documents ``rt_min`` as the multi-symbol real-time minute endpoint
(document 374; the entitlement table limits one request to 300 companies) and
``rt_min_daily`` as the current-day minute-history endpoint (document 457).
Both are supported, but neither is treated as permissioned merely because it
appears in configuration: the live call and exact response contract are the
capability probe.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
import re
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from .data_sources import (
    DatasetContract,
    FetchRequest,
    FieldContract,
    ProbeResult,
    SourceAdapter,
    SourceBatch,
    SourceContractError,
    SourceHealth,
    harden_tushare_client_transport,
    tushare_client_uses_direct_transport,
    validate_production_diemeng_base_url,
)
from .field_safety import is_forward_derived_field
from .fingerprint import content_fingerprint


TUSHARE_RT_MIN = "rt_min"
TUSHARE_RT_MIN_DAILY = "rt_min_daily"
TUSHARE_REALTIME_ENDPOINTS = frozenset({TUSHARE_RT_MIN, TUSHARE_RT_MIN_DAILY})
TUSHARE_RT_MIN_MAX_SYMBOLS_PER_REQUEST = 300
NORMALIZED_OPEN_DATASET = "minute_history"
TUSHARE_REALTIME_DOC_IDS = {
    TUSHARE_RT_MIN: 374,
    TUSHARE_RT_MIN_DAILY: 457,
}
_SHANGHAI = ZoneInfo("Asia/Shanghai")

_COMMON_RESPONSE_FIELDS = (
    "ts_code",
    "time",
    "open",
    "close",
    "high",
    "low",
    "vol",
    "amount",
)
_DAILY_RESPONSE_FIELDS = (
    "ts_code",
    "freq",
    "time",
    "open",
    "close",
    "high",
    "low",
    "vol",
    "amount",
)


def diemeng_opening_session_request_template() -> dict[str, Any]:
    """Return the one reviewed bounded request for Diemeng opening evidence.

    Keeping this template in the provider contract module prevents the
    engineering canary and the formal execution authority from drifting onto
    different minute levels, pages, or time windows.
    """

    return {
        "stock_code": "${ticker}",
        "level": "1min",
        "start_time": "${partition_key} 09:30:00",
        "end_time": "${partition_key} 15:00:00",
        "page": 0,
        "page_size": 10000,
    }


def diemeng_engineering_canary_execution_mapping(
    base_url: str = "https://data.diemeng.chat/api",
) -> dict[str, Any]:
    """Return the closed retrospective execution source used only by canary.

    Formal shadow collection deliberately uses Tushare's current-session
    realtime endpoint. A 20-session engineering replay cannot use that
    endpoint as historical evidence, so the canary has a separate, explicitly
    non-formal Diemeng minute-history contract. Keeping the whole mapping
    code-owned prevents configuration from widening the time range, paging,
    endpoint, response shape, or credential binding.
    """

    reviewed_url = validate_production_diemeng_base_url(base_url)
    return {
        "source": "diemeng",
        "profile_name": "primary-diemeng",
        "credential_ref": "secret://diemeng_api_key",
        "base_url": reviewed_url,
        "dataset": "minute_history",
        "endpoint": "/stock/history",
        "method": "POST",
        "response_path": "data.list",
        "request": diemeng_opening_session_request_template(),
        "contract": {
            "key_fields": ["stock_code", "trade_time"],
            "event_time_field": "trade_time",
            "fields": [
                "stock_code",
                "trade_time",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            ],
            "execution_observation": {
                "required_local_time": "09:30:00",
                "timezone": "Asia/Shanghai",
                "event_time_source": "trade_time",
                "available_at_source": "trade_time",
                "price_field": "open",
            },
        },
        "availability": {
            "mode": "event_timestamp",
            "event_time_field": "trade_time",
            "available_at_field": "trade_time",
        },
        "formal_capability": {
            "status": "insufficient",
            "reason": (
                "minute history is retrospective engineering evidence and "
                "cannot prove live opening execution"
            ),
            "formal_shadow_projection": "blocked",
            "engineering_canary": True,
        },
        "end_of_day_mark": {
            "source": "accepted_gold_close_snapshot",
            "event_time": "15:00:00",
            "available_at": "accepted_snapshot_time",
        },
    }


def validate_diemeng_engineering_canary_execution_mapping(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate and canonicalize the exact non-formal canary source mapping."""

    if not isinstance(value, Mapping):
        raise ValueError("engineering canary execution source must be an object")
    try:
        candidate = dict(value)
        candidate["base_url"] = validate_production_diemeng_base_url(
            str(candidate.get("base_url") or "")
        )
    except ValueError:
        raise ValueError(
            "engineering canary Diemeng source requires the reviewed HTTPS origin"
        ) from None
    expected = diemeng_engineering_canary_execution_mapping(candidate["base_url"])
    if candidate != expected:
        raise ValueError(
            "engineering canary execution source must match the closed retrospective contract"
        )
    return expected


def engineering_canary_execution_contract_hash(
    canary: Mapping[str, Any],
) -> str:
    """Hash the exact retrospective source contract, excluding no fields."""

    if not isinstance(canary, Mapping):
        raise ValueError("engineering canary must be an object")
    expected_fields = {"evidence_scope", "execution_market_data"}
    if set(canary) != expected_fields:
        raise ValueError(
            "engineering canary must contain exactly evidence_scope and "
            "execution_market_data"
        )
    if canary.get("evidence_scope") != "retrospective_non_forward":
        raise ValueError(
            "engineering canary must declare retrospective_non_forward evidence"
        )
    execution = validate_diemeng_engineering_canary_execution_mapping(
        canary.get("execution_market_data")
    )
    return content_fingerprint(
        {
            "evidence_scope": "retrospective_non_forward",
            "execution_market_data": execution,
        },
        domain="factor-lab/research-os/v1/engineering-canary-execution-contract",
    )


def diemeng_engineering_canary_opening_contract_hash(
    canary: Mapping[str, Any],
) -> str:
    """Return the exact DatasetContract hash persisted by the canary probe."""

    engineering_canary_execution_contract_hash(canary)
    execution = validate_diemeng_engineering_canary_execution_mapping(
        canary.get("execution_market_data")
    )
    contract = execution["contract"]
    fields = tuple(map(str, contract["fields"]))
    dtypes = {"stock_code": "string", "trade_time": "datetime"}
    opening_contract = DatasetContract(
        dataset="opening_execution",
        key_fields=tuple(map(str, contract["key_fields"])),
        fields=tuple(
            FieldContract(
                name=name,
                dtype=dtypes.get(name, "float64"),
                nullable=False,
            )
            for name in fields
        ),
        event_time_field=str(contract["event_time_field"]),
        release_timing="provider event timestamp at the observed minute",
        allows_empty=False,
    )
    return content_fingerprint(
        opening_contract,
        domain="factor-lab/research-os/v1/physical-canary-source-contract",
    )


def normalized_open_contract() -> DatasetContract:
    """Return the provider-neutral shape consumed by execution authority."""

    return DatasetContract(
        dataset=NORMALIZED_OPEN_DATASET,
        key_fields=("stock_code", "trade_time"),
        fields=(
            FieldContract("stock_code", "string", nullable=False),
            FieldContract("trade_time", "datetime", nullable=False),
            FieldContract("open", "float64", nullable=False, unit="CNY"),
            FieldContract("high", "float64", nullable=False, unit="CNY"),
            FieldContract("low", "float64", nullable=False, unit="CNY"),
            FieldContract("close", "float64", nullable=False, unit="CNY"),
            FieldContract("vol", "float64", nullable=False, unit="shares"),
            FieldContract("amount", "float64", nullable=False, unit="CNY"),
        ),
        event_time_field="trade_time",
        release_timing=(
            "official realtime endpoint response received during the current "
            "session opening collection window"
        ),
        allows_empty=True,
    )


def _aware_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise SourceContractError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _event_time(value: Any) -> pd.Timestamp:
    if isinstance(value, str) and not re.match(
        r"^\d{4}(?:-?\d{2}){2}[ T]", value.strip()
    ):
        raise SourceContractError(
            "Tushare realtime response time must include an explicit calendar date"
        )
    parsed = pd.Timestamp(value)
    if pd.isna(parsed):
        raise SourceContractError("Tushare realtime response has an invalid time")
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize(_SHANGHAI)
    else:
        parsed = parsed.tz_convert(_SHANGHAI)
    return parsed


def _call_tushare(client: Any, endpoint: str, parameters: Mapping[str, Any]) -> Any:
    target = getattr(client, endpoint, None)
    try:
        if callable(target):
            return target(**dict(parameters))
        query = getattr(client, "query", None)
        if callable(query):
            return query(endpoint, **dict(parameters))
    except Exception as exc:
        # Do not copy a vendor exception into persisted lineage: SDK/network
        # errors have historically included request material.  The type is
        # sufficient to distinguish entitlement/network failures operationally.
        raise SourceContractError(
            f"Tushare realtime endpoint unavailable or unauthorized ({type(exc).__name__})"
        ) from exc
    raise SourceContractError("Tushare client has no supported realtime endpoint")


def _real_tushare_data_api(client: Any) -> bool:
    return tushare_client_uses_direct_transport(client)


@dataclass(frozen=True)
class RealtimeOpenCollection:
    """One exact current-session provider response and its receive authority."""

    batch: SourceBatch
    requested_tickers: tuple[str, ...]
    received_at: datetime
    endpoint: str
    doc_id: int


@dataclass(frozen=True)
class _OfficialResponse:
    frame: pd.DataFrame
    requested_tickers: tuple[str, ...]
    received_at: datetime
    request_hash: str


class TushareRealtimeOpenAdapter(SourceAdapter):
    """Strict adapter for official Tushare real-time minute endpoints.

    ``fetch_open_batch`` is deliberately the only formal collection method.
    It performs the actual permission probe, rejects response-schema drift and
    binds the response to the current session plus collector receive time.
    The inherited generic ``fetch`` method remains useful for deterministic
    tests but does not itself confer formal capability.
    """

    def __init__(
        self,
        client: Any,
        *,
        endpoint: str = TUSHARE_RT_MIN,
        priority: int = 10,
        receive_clock: Callable[[], datetime] | None = None,
        collection_window_minutes: int = 5,
        max_universe_size: int = 500,
        max_symbols_per_request: int = TUSHARE_RT_MIN_MAX_SYMBOLS_PER_REQUEST,
        lineage: Mapping[str, Any] | None = None,
    ) -> None:
        selected = str(endpoint).strip()
        if selected not in TUSHARE_REALTIME_ENDPOINTS:
            raise ValueError("unsupported Tushare realtime endpoint")
        if collection_window_minutes <= 0 or max_universe_size <= 0:
            raise ValueError("collection window and universe size must be positive")
        if not 1 <= int(max_symbols_per_request) <= TUSHARE_RT_MIN_MAX_SYMBOLS_PER_REQUEST:
            raise ValueError(
                "Tushare rt_min request capacity must be between 1 and 300"
            )
        self.client = harden_tushare_client_transport(client)
        self.endpoint = selected
        self.collection_window_minutes = int(collection_window_minutes)
        self.max_universe_size = int(max_universe_size)
        self.max_symbols_per_request = int(max_symbols_per_request)
        self._receive_clock = receive_clock or (lambda: datetime.now(timezone.utc))
        self._clock_is_injected = receive_clock is not None
        super().__init__(
            source_id="tushare",
            priority=priority,
            contracts=(normalized_open_contract(),),
            lineage={
                **dict(lineage or {}),
                "endpoint": selected,
                "official_document_id": TUSHARE_REALTIME_DOC_IDS[selected],
                "availability_authority": "collector_received_at",
                "provider_event_time_is_not_availability": True,
                "max_symbols_per_request": self.max_symbols_per_request,
            },
        )

    @property
    def production_attested(self) -> bool:
        """Whether construction used the unmodified SDK client and real clock."""

        return bool(
            _real_tushare_data_api(self.client)
            and not self._clock_is_injected
            and self.endpoint in TUSHARE_REALTIME_ENDPOINTS
        )

    @property
    def real_time_open_capable(self) -> bool:
        return True

    def probe(self) -> ProbeResult:
        # A ticker-free/static probe cannot establish entitlement to these
        # paid endpoints.  The session-bound collection is the only honest
        # capability probe.
        return ProbeResult(
            source_id=self.source_id,
            health=SourceHealth.DEGRADED,
            checked_at=_aware_utc(self._receive_clock(), label="receive_clock"),
            latency_ms=0.0,
            datasets=(NORMALIZED_OPEN_DATASET,),
            message="session-bound realtime fetch required",
        )

    @staticmethod
    def _request_hash(
        *, endpoint: str, session: date, tickers: Sequence[str]
    ) -> str:
        material = json.dumps(
            {
                "endpoint": endpoint,
                "frequency": "1MIN",
                "session": session.isoformat(),
                "tickers": list(tickers),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(material.encode("utf-8")).hexdigest()

    @staticmethod
    def _ticker_hash(ticker: str) -> str:
        return hashlib.sha256(
            ("factor-lab/tushare-realtime-missing/v1\0" + ticker).encode("utf-8")
        ).hexdigest()

    def _validated_official_frame(self, value: Any) -> pd.DataFrame:
        if not isinstance(value, pd.DataFrame):
            raise SourceContractError(
                f"Tushare {self.endpoint} did not return a DataFrame"
            )
        frame = value.copy(deep=True)
        expected = (
            _COMMON_RESPONSE_FIELDS
            if self.endpoint == TUSHARE_RT_MIN
            else _DAILY_RESPONSE_FIELDS
        )
        actual = tuple(map(str, frame.columns))
        if len(actual) != len(set(actual)) or set(actual) != set(expected):
            missing = sorted(set(expected) - set(actual))
            extra = sorted(set(actual) - set(expected))
            raise SourceContractError(
                "Tushare realtime response schema must match exactly; "
                f"missing={missing}, extra={extra}"
            )
        forbidden = sorted(name for name in actual if is_forward_derived_field(name))
        if forbidden:
            raise SourceContractError(
                f"Tushare realtime response contains forward fields: {forbidden}"
            )
        if self.endpoint == TUSHARE_RT_MIN_DAILY and not frame.empty:
            if not frame["freq"].astype(str).str.upper().eq("1MIN").all():
                raise SourceContractError("Tushare rt_min_daily returned another frequency")
        return frame

    def _official_frames(
        self, tickers: Sequence[str], session: date
    ) -> tuple[_OfficialResponse, ...]:
        responses: list[_OfficialResponse] = []
        if self.endpoint == TUSHARE_RT_MIN:
            request_groups = tuple(
                tuple(tickers[offset : offset + self.max_symbols_per_request])
                for offset in range(0, len(tickers), self.max_symbols_per_request)
            )
        else:
            request_groups = tuple((ticker,) for ticker in tickers)

        for group in request_groups:
            value = _call_tushare(
                self.client,
                self.endpoint,
                {"ts_code": ",".join(group), "freq": "1MIN"},
            )
            # Capture availability immediately after each successful response.
            # One clock read after a multi-request merge would let a late batch
            # borrow an earlier batch's admissible receipt time.
            received_at = _aware_utc(self._receive_clock(), label="receive_clock")
            responses.append(
                _OfficialResponse(
                    frame=self._validated_official_frame(value),
                    requested_tickers=group,
                    received_at=received_at,
                    request_hash=self._request_hash(
                        endpoint=self.endpoint,
                        session=session,
                        tickers=group,
                    ),
                )
            )
        return tuple(responses)

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        raw_tickers = request.parameters.get("tickers")
        if not isinstance(raw_tickers, (tuple, list)):
            raise SourceContractError("typed Tushare fetch requires a ticker sequence")
        collection = self.fetch_open_batch(
            tuple(map(str, raw_tickers)),
            date.fromisoformat(str(request.parameters.get("trade_date") or "")),
        )
        return collection.batch.frame.copy(deep=True)

    def fetch_open_batch(
        self,
        tickers: Sequence[str],
        session: date,
    ) -> RealtimeOpenCollection:
        requested = tuple(sorted(map(str, tickers)))
        if (
            not requested
            or len(requested) > self.max_universe_size
            or len(requested) != len(set(requested))
            or any(not item.strip() for item in requested)
        ):
            raise SourceContractError("Tushare realtime ticker universe is invalid")

        start = datetime.combine(session, time(9, 30), tzinfo=_SHANGHAI)
        deadline = start + timedelta(minutes=self.collection_window_minutes)
        official = self._official_frames(requested, session)
        exact_frames: list[pd.DataFrame] = []
        seen: set[str] = set()
        missing_total: set[str] = set()
        batch_lineage: list[dict[str, Any]] = []
        for index, response in enumerate(official):
            local_received = response.received_at.astimezone(_SHANGHAI)
            if (
                local_received.date() != session
                or not start <= local_received <= deadline
            ):
                raise SourceContractError(
                    "Tushare realtime response was not received in the live opening window"
                )
            raw = response.frame
            if raw.empty:
                exact = raw.copy()
            else:
                events = raw["time"].map(_event_time)
                exact_mask = events.map(
                    lambda value: value.date() == session
                    and value.time().replace(tzinfo=None) == time(9, 30)
                )
                if self.endpoint == TUSHARE_RT_MIN and not exact_mask.all():
                    raise SourceContractError(
                        "Tushare rt_min did not return only current-session 09:30 rows"
                    )
                exact = raw.loc[exact_mask].copy()
                exact["time"] = events.loc[exact_mask].map(
                    lambda value: value.tz_convert(timezone.utc).to_pydatetime()
                )
            if exact["ts_code"].astype(str).duplicated().any():
                raise SourceContractError("Tushare realtime response has duplicate tickers")
            observed_in_batch = set(map(str, exact["ts_code"]))
            cross_batch = sorted(observed_in_batch & seen)
            if cross_batch:
                raise SourceContractError(
                    f"Tushare realtime response has cross-batch duplicate tickers: {cross_batch}"
                )
            expected_in_batch = set(response.requested_tickers)
            unexpected = sorted(observed_in_batch - expected_in_batch)
            if unexpected:
                raise SourceContractError(
                    f"Tushare realtime response contains unrequested tickers: {unexpected}"
                )
            missing = sorted(expected_in_batch - observed_in_batch)
            missing_total.update(missing)
            seen.update(observed_in_batch)
            exact_frames.append(exact)
            batch_lineage.append(
                {
                    "batch_index": index,
                    "ticker_count": len(response.requested_tickers),
                    "request_hash": response.request_hash,
                    "received_at": response.received_at.isoformat(),
                    "missing_ticker_count": len(missing),
                    "missing_ticker_hashes": tuple(
                        self._ticker_hash(item) for item in missing
                    ),
                }
            )

        if seen | missing_total != set(requested) or seen & missing_total:
            raise SourceContractError(
                "Tushare realtime response coverage accounting is inconsistent"
            )
        non_empty_exact = [item for item in exact_frames if not item.empty]
        exact = (
            pd.concat(non_empty_exact, ignore_index=True)
            if non_empty_exact
            else pd.DataFrame(columns=(
                _COMMON_RESPONSE_FIELDS
                if self.endpoint == TUSHARE_RT_MIN
                else _DAILY_RESPONSE_FIELDS
            ))
        )
        received_at = max(response.received_at for response in official)
        observed = seen
        for field in ("open", "high", "low", "close", "vol", "amount"):
            values = pd.to_numeric(exact[field], errors="coerce")
            if values.isna().any() or (~values.map(math.isfinite)).any():
                raise SourceContractError(
                    f"Tushare realtime response has invalid {field} values"
                )
            if field in {"open", "high", "low", "close"} and values.le(0).any():
                raise SourceContractError(
                    f"Tushare realtime response has non-positive {field} values"
                )

        normalized = exact.rename(
            columns={"ts_code": "stock_code", "time": "trade_time"}
        )[[item.name for item in normalized_open_contract().fields]].copy()
        normalized = normalized.sort_values("stock_code", kind="mergesort").reset_index(
            drop=True
        )
        revision_material = json.dumps(
            {
                "endpoint": self.endpoint,
                "session": session.isoformat(),
                "requested_tickers": list(requested),
                "frame": normalized.to_json(
                    orient="split", date_format="iso", date_unit="ns"
                ),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        request = FetchRequest(
            dataset=NORMALIZED_OPEN_DATASET,
            parameters={
                "trade_date": session.isoformat(),
                "ticker_count": len(requested),
                "frequency": "1MIN",
            },
            fields=tuple(item.name for item in normalized_open_contract().fields),
        )
        batch = SourceBatch(
            source_id=self.source_id,
            source_priority=self.priority,
            dataset=NORMALIZED_OPEN_DATASET,
            frame=normalized,
            ingested_at=received_at,
            vendor_revision=hashlib.sha256(
                revision_material.encode("utf-8")
            ).hexdigest(),
            contract=normalized_open_contract(),
            request=request,
            lineage={
                **self.lineage,
                "permission_probe": "session_bound_successful_fetch",
                "requested_ticker_count": len(requested),
                "observed_ticker_count": len(observed),
                "coverage_status": (
                    "complete" if not missing_total else "provisional_missing"
                ),
                "missing_ticker_count": len(missing_total),
                "missing_ticker_hashes": tuple(
                    self._ticker_hash(item) for item in sorted(missing_total)
                ),
                "request_batch_count": len(batch_lineage),
                "request_hashes": tuple(
                    item["request_hash"] for item in batch_lineage
                ),
                "request_batches": tuple(batch_lineage),
            },
        )
        return RealtimeOpenCollection(
            batch=batch,
            requested_tickers=requested,
            received_at=received_at,
            endpoint=self.endpoint,
            doc_id=TUSHARE_REALTIME_DOC_IDS[self.endpoint],
        )


__all__ = [
    "NORMALIZED_OPEN_DATASET",
    "RealtimeOpenCollection",
    "TUSHARE_REALTIME_DOC_IDS",
    "TUSHARE_REALTIME_ENDPOINTS",
    "TUSHARE_RT_MIN",
    "TUSHARE_RT_MIN_DAILY",
    "TUSHARE_RT_MIN_MAX_SYMBOLS_PER_REQUEST",
    "TushareRealtimeOpenAdapter",
    "diemeng_engineering_canary_execution_mapping",
    "diemeng_engineering_canary_opening_contract_hash",
    "diemeng_opening_session_request_template",
    "engineering_canary_execution_contract_hash",
    "normalized_open_contract",
    "validate_diemeng_engineering_canary_execution_mapping",
]
