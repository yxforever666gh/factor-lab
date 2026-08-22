"""Provider-neutral source contracts for the Factor Lab data plane.

Adapters in this module deliberately stop at the Bronze boundary: they prove
that a provider can be reached, fetch an explicitly contracted dataset, and
return the raw response with lineage.  Point-in-time normalization and source
reconciliation live in sibling modules so a provider can never silently win a
conflict merely because it was fetched last.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import importlib
import json
import math
from pathlib import Path
import re
from threading import RLock
import time
from types import MappingProxyType, MethodType
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlparse, urlunparse

import pandas as pd
import requests


class SourceHealth(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class SourceContractError(ValueError):
    """Raised when a provider response violates its declared Bronze contract."""


_CREDENTIAL_KEY_PUBLIC_METADATA = frozenset(
    {
        # These keys describe credential governance or aggregate model usage;
        # they are not slots into which credential material can be supplied.
        "credentialpolicy",
        "credentialrotation",
        "secretrotation",
        "tokenbudget",
        "tokencount",
        "tokenlimit",
        "tokenquota",
        "tokenusage",
    }
)
_CREDENTIAL_KEY_STRONG_MARKERS = frozenset(
    {
        "apikey",
        "authorization",
        "accesstoken",
        "refreshtoken",
        "authtoken",
        "bearertoken",
        "clientsecret",
        "password",
        "passwd",
        "secretkey",
        "accesskey",
        "privatekey",
    }
)


def _normalized_credential_key(value: Any) -> str:
    return "".join(
        character for character in str(value).casefold() if character.isalnum()
    )


def is_credential_shaped_key(value: Any) -> bool:
    """Return whether a mapping key can carry authentication material.

    Separators and casing are deliberately ignored, so ``apiKey``,
    ``api-key`` and ``API_KEY`` are the same slot.  A very small public
    metadata allow-list avoids treating governance state and token *counts*
    as secrets while still rejecting vendor-specific names such as
    ``tushare_token`` and nested headers such as ``X-API-Key``.
    """

    normalized = _normalized_credential_key(value)
    if not normalized or normalized in _CREDENTIAL_KEY_PUBLIC_METADATA:
        return False
    if any(marker in normalized for marker in _CREDENTIAL_KEY_STRONG_MARKERS):
        return True
    if "credential" in normalized:
        return True
    return normalized in {"auth", "authentication", "secret", "token"} or (
        normalized.endswith("auth")
        or normalized.endswith("secret")
        or normalized.endswith("token")
    )


def assert_credential_free_request_parameters(
    value: Any,
    *,
    context: str = "request.parameters",
) -> None:
    """Reject credential-shaped request fields without reflecting values.

    Bronze request parameters are persisted in revision hashes and lineage.
    Credential references belong in the adapter binding, never in this
    serialised request map.  This check is intentionally provider-independent
    and recursive so nested ``headers`` or list entries cannot bypass it.
    """

    def walk(item: Any) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                if is_credential_shaped_key(key):
                    raise SourceContractError(
                        f"{context} contains a forbidden credential-shaped field"
                    )
                walk(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                walk(child)

    walk(value)


class _ProviderResponseStatusError(SourceContractError):
    """A persistable provider rejection containing only reviewed numeric data.

    Provider response prose is untrusted: vendors have echoed credentials and
    request material in ``msg``/error fields. Keeping the numeric status in a
    typed exception lets operators distinguish HTTP, redirect and business
    failures without copying provider-controlled prose into logs, probe
    evidence or CLI output.
    """

    _CLASSIFICATIONS = frozenset({"redirect_status", "http_status", "api_code"})

    def __init__(
        self,
        *,
        provider: str,
        classification: str,
        numeric_code: int,
    ) -> None:
        if classification not in self._CLASSIFICATIONS:
            raise ValueError("provider response classification is invalid")
        self.provider = str(provider)
        self.classification = classification
        self.numeric_code = int(numeric_code)
        if classification == "redirect_status":
            message = (
                f"{self.provider} HTTP redirect {self.numeric_code} is forbidden"
            )
        elif classification == "http_status":
            message = (
                f"{self.provider} HTTP rejected request "
                f"(status={self.numeric_code})"
            )
        else:
            message = (
                f"{self.provider} API rejected request "
                f"(code={self.numeric_code})"
            )
        super().__init__(message)


def _safe_probe_failure_message(provider: str, exc: Exception) -> str:
    """Return a fixed, provider-data-free probe failure classification."""

    if isinstance(exc, _ProviderResponseStatusError):
        return (
            f"{provider} probe failed "
            f"({exc.classification}={exc.numeric_code})"
        )
    if isinstance(exc, SourceContractError):
        classification = "contract_error"
    elif isinstance(exc, requests.RequestException):
        classification = "transport_error"
    else:
        classification = "provider_error"
    return f"{provider} probe failed ({classification})"


_TUSHARE_DATA_API_MODULE = "tushare.pro.client"
_TUSHARE_DATA_API_NAME = "DataApi"
_TUSHARE_DIRECT_SESSION_ATTRIBUTE = "_factor_lab_direct_http_session"
_TUSHARE_DIRECT_TRANSPORT_ATTRIBUTE = "_factor_lab_direct_transport_seal"
_TUSHARE_DIRECT_TRANSPORT_SEAL = object()
_TUSHARE_API_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_TUSHARE_API_HOSTS = frozenset({"api.waditu.com", "api.tushare.pro"})


def _requests_session_is_direct(session: Any) -> bool:
    """Return whether an exact Requests session ignores ambient routing/trust.

    ``trust_env=False`` disables proxy, CA-bundle and ``.netrc`` discovery.
    The remaining checks ensure a caller did not replace those ambient
    overrides with an equally unreviewed session-level proxy/certificate.
    """

    return bool(
        type(session) is requests.Session
        and session.trust_env is False
        and not session.proxies
        and session.verify is True
        and session.cert is None
        and session.auth is None
    )


def _tushare_sdk_client(value: Any) -> bool:
    cls = type(value)
    return (
        cls.__module__ == _TUSHARE_DATA_API_MODULE
        and cls.__name__ == _TUSHARE_DATA_API_NAME
    )


def _tushare_base_url(client: Any) -> str:
    raw = str(getattr(client, "_DataApi__http_url", "") or "").strip().rstrip("/")
    return _validated_tushare_https_origin(raw)


def _validated_tushare_https_origin(raw: str) -> str:
    raw = str(raw or "").strip().rstrip("/")
    parsed = urlparse(raw)
    try:
        port = parsed.port
    except ValueError as exc:
        raise SourceContractError("Tushare SDK HTTPS origin is invalid") from exc
    if not (
        parsed.scheme == "https"
        and parsed.hostname is not None
        and parsed.hostname.casefold() in _TUSHARE_API_HOSTS
        and parsed.username is None
        and parsed.password is None
        and port in {None, 443}
        and parsed.path.rstrip("/") == "/dataapi"
        and not parsed.params
        and not parsed.query
        and not parsed.fragment
    ):
        # The pinned SDK currently declares a plaintext HTTP endpoint.  Do not
        # silently rewrite it to a guessed HTTPS URL: production remains
        # blocked until the vendor/SDK itself supplies a confirmed HTTPS
        # origin, and the already exposed credential must be rotated.
        raise SourceContractError(
            "Tushare SDK HTTPS transport is unavailable or unverified"
        )
    return raw


def require_tushare_sdk_https_transport() -> str:
    """Preflight the installed SDK without constructing a client or token.

    Production factories call this before resolving ``credential_ref``.  The
    check reads only the SDK class's public transport constant and performs no
    network operation, so an HTTP-only SDK cannot cause credential access.
    """

    try:
        module = importlib.import_module(_TUSHARE_DATA_API_MODULE)
        client_class = getattr(module, _TUSHARE_DATA_API_NAME)
        raw = getattr(client_class, "_DataApi__http_url")
    except (ImportError, AttributeError) as exc:
        raise SourceContractError(
            "Tushare SDK HTTPS transport cannot be verified"
        ) from exc
    return _validated_tushare_https_origin(str(raw or ""))


def _tushare_query_with_direct_session(
    client: Any,
    api_name: str,
    fields: str = "",
    **parameters: Any,
) -> pd.DataFrame:
    """Execute the documented SDK wire contract through a sealed Session.

    The upstream ``DataApi.query`` calls ``requests.post`` directly, which
    constructs a new environment-trusting session for every request.  Binding
    this instance method preserves the SDK request/response contract while
    making the transport explicit and fail-closed.
    """

    if not _TUSHARE_API_NAME.fullmatch(str(api_name or "")):
        raise SourceContractError("Tushare API name is invalid")
    client_state = vars(client)
    session = client_state.get(_TUSHARE_DIRECT_SESSION_ATTRIBUTE)
    if not (
        client_state.get(_TUSHARE_DIRECT_TRANSPORT_ATTRIBUTE)
        is _TUSHARE_DIRECT_TRANSPORT_SEAL
        and _requests_session_is_direct(session)
    ):
        raise SourceContractError("Tushare direct HTTPS transport is not sealed")
    base_url = _tushare_base_url(client)
    token = str(getattr(client, "_DataApi__token", "") or "")
    timeout = getattr(client, "_DataApi__timeout", 30)
    if not token:
        raise SourceContractError("Tushare SDK credential is unavailable")
    try:
        timeout_value = float(timeout)
    except (TypeError, ValueError) as exc:
        raise SourceContractError("Tushare SDK timeout is invalid") from exc
    if not math.isfinite(timeout_value) or timeout_value <= 0:
        raise SourceContractError("Tushare SDK timeout is invalid")

    request_parameters = dict(parameters)
    request_parameters.setdefault("ts_type_name", base_url)
    request_failed = False
    try:
        response = session.post(
            f"{base_url}/{api_name}",
            json={
                "api_name": str(api_name),
                "token": token,
                "params": request_parameters,
                "fields": str(fields or ""),
            },
            timeout=timeout_value,
            allow_redirects=False,
        )
        status = int(response.status_code)
        if 300 <= status < 400:
            raise _ProviderResponseStatusError(
                provider="Tushare",
                classification="redirect_status",
                numeric_code=status,
            )
        if status < 200 or status >= 300:
            raise _ProviderResponseStatusError(
                provider="Tushare",
                classification="http_status",
                numeric_code=status,
            )
        payload = response.json()
    except SourceContractError:
        raise
    except Exception:
        # SDK/network exceptions can include request bodies. Do not retain the
        # exception as a cause/context: structured loggers may traverse the
        # exception graph even when ``raise ... from None`` hides it in a
        # normal traceback.
        request_failed = True
    if request_failed:
        raise SourceContractError(
            "Tushare direct request failed (transport_or_response_error)"
        )
    if not isinstance(payload, Mapping):
        raise SourceContractError("Tushare response must be an object")
    invalid_code = False
    try:
        code = int(payload.get("code", -1))
    except (TypeError, ValueError):
        invalid_code = True
    if invalid_code:
        raise SourceContractError("Tushare response code is invalid")
    if code != 0:
        raise _ProviderResponseStatusError(
            provider="Tushare",
            classification="api_code",
            numeric_code=code,
        )
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise SourceContractError("Tushare response data is invalid")
    columns = data.get("fields")
    items = data.get("items")
    if not isinstance(columns, list) or not isinstance(items, list):
        raise SourceContractError("Tushare response table is invalid")
    malformed_table = False
    try:
        return pd.DataFrame(items, columns=list(map(str, columns)))
    except (TypeError, ValueError):
        malformed_table = True
    if malformed_table:  # pragma: no branch - the except path cannot return
        raise SourceContractError("Tushare response table is malformed")
    raise AssertionError("unreachable Tushare response table state")  # pragma: no cover


def harden_tushare_client_transport(client: Any) -> Any:
    """Bind a real Tushare SDK client to a non-environment HTTPS session.

    Test/provider fixtures are deliberately left untouched.  A genuine SDK
    ``DataApi`` is never allowed to retain its module-level ``requests.post``
    implementation, because that path implicitly consumes proxy/CA variables.
    """

    if not _tushare_sdk_client(client):
        return client
    if tushare_client_uses_direct_transport(client):
        return client
    # Validate the immutable public destination before constructing any
    # network client or reading the private token.
    _tushare_base_url(client)
    session = requests.Session()
    session.trust_env = False
    if not _requests_session_is_direct(session):  # pragma: no cover - requests invariant
        session.close()
        raise SourceContractError("could not construct direct Tushare HTTPS session")
    setattr(client, _TUSHARE_DIRECT_SESSION_ATTRIBUTE, session)
    setattr(
        client,
        _TUSHARE_DIRECT_TRANSPORT_ATTRIBUTE,
        _TUSHARE_DIRECT_TRANSPORT_SEAL,
    )
    setattr(client, "query", MethodType(_tushare_query_with_direct_session, client))
    if not tushare_client_uses_direct_transport(client):  # pragma: no cover
        session.close()
        raise SourceContractError("could not seal direct Tushare HTTPS transport")
    return client


def tushare_client_uses_direct_transport(client: Any) -> bool:
    """Return whether a real SDK client is bound to our sealed direct session."""

    state = vars(client)
    query = state.get("query")
    return bool(
        _tushare_sdk_client(client)
        and state.get(_TUSHARE_DIRECT_TRANSPORT_ATTRIBUTE)
        is _TUSHARE_DIRECT_TRANSPORT_SEAL
        and _requests_session_is_direct(
            state.get(_TUSHARE_DIRECT_SESSION_ATTRIBUTE)
        )
        and getattr(query, "__func__", None) is _tushare_query_with_direct_session
        and getattr(query, "__self__", None) is client
    )


@dataclass(frozen=True)
class RateLimit:
    requests: int
    per_seconds: float
    burst: int = 1

    def __post_init__(self) -> None:
        if self.requests <= 0 or self.per_seconds <= 0 or self.burst <= 0:
            raise ValueError("rate-limit values must be positive")


@dataclass
class _TokenBucketState:
    limit: RateLimit
    tokens: float
    updated_at: float


class TokenBucketRateLimiter:
    """Thread-safe, reserving token bucket shared by adapter instances.

    Reservations are recorded before sleeping.  Concurrent adapters therefore
    queue behind the same ``(source_id, dataset)`` bucket instead of waking at
    the same instant and exceeding the provider contract.  The clock and sleep
    functions are injectable so tests can advance virtual time without waiting.
    """

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._clock = monotonic_clock
        self._sleeper = sleeper
        self._lock = RLock()
        self._buckets: dict[tuple[str, str], _TokenBucketState] = {}

    def acquire(
        self,
        source_id: str,
        dataset: str,
        limit: RateLimit,
        *,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> float:
        """Reserve one request token and return the imposed wait in seconds."""

        clock = monotonic_clock or self._clock
        sleep = sleeper or self._sleeper
        key = (str(source_id), str(dataset))
        now = float(clock())
        refill_per_second = float(limit.requests) / float(limit.per_seconds)
        with self._lock:
            state = self._buckets.get(key)
            if state is None:
                state = _TokenBucketState(
                    limit=limit,
                    tokens=float(limit.burst),
                    updated_at=now,
                )
                self._buckets[key] = state
            elif state.limit != limit:
                raise ValueError(
                    "conflicting rate limits for "
                    f"source_id={source_id!r}, dataset={dataset!r}: "
                    f"existing={state.limit!r}, requested={limit!r}"
                )

            if now >= state.updated_at:
                elapsed = now - state.updated_at
                state.tokens = min(
                    float(limit.burst),
                    state.tokens + elapsed * refill_per_second,
                )
                state.updated_at = now

            if state.tokens >= 1.0:
                state.tokens -= 1.0
                wait_seconds = 0.0
            else:
                # ``updated_at`` can be in the future when another thread has
                # already reserved the next token.  Queue this reservation
                # after it rather than entering a wake/retry race.
                queue_delay = max(0.0, state.updated_at - now)
                token_delay = (1.0 - state.tokens) / refill_per_second
                wait_seconds = queue_delay + token_delay
                state.tokens = 0.0
                state.updated_at = now + wait_seconds

        if wait_seconds > 0.0:
            sleep(wait_seconds)
        return wait_seconds


_PROCESS_RATE_LIMITER = TokenBucketRateLimiter()


@dataclass(frozen=True)
class FieldContract:
    name: str
    dtype: str
    nullable: bool = True
    unit: str | None = None
    adjustment: str | None = None
    release_timing: str | None = None

    def __post_init__(self) -> None:
        if not self.name.strip() or not self.dtype.strip():
            raise ValueError("field name and dtype are required")


@dataclass(frozen=True)
class DatasetContract:
    dataset: str
    key_fields: tuple[str, ...]
    fields: tuple[FieldContract, ...]
    event_time_field: str
    release_timing: str
    allows_empty: bool = False

    def __post_init__(self) -> None:
        names = [item.name for item in self.fields]
        if not self.dataset.strip():
            raise ValueError("dataset is required")
        if not self.key_fields or len(set(self.key_fields)) != len(self.key_fields):
            raise ValueError("key_fields must be non-empty and unique")
        if len(names) != len(set(names)):
            raise ValueError("field contracts must have unique names")
        required = {*self.key_fields, self.event_time_field}
        missing = sorted(required - set(names))
        if missing:
            raise ValueError(f"contract omits key/event fields: {missing}")
        if not self.release_timing.strip():
            raise ValueError("release_timing must state when data becomes knowable")

    @property
    def field_map(self) -> dict[str, FieldContract]:
        return {item.name: item for item in self.fields}


@dataclass(frozen=True)
class FetchRequest:
    dataset: str
    parameters: Mapping[str, Any] = field(default_factory=dict)
    fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProbeResult:
    source_id: str
    health: SourceHealth
    checked_at: datetime
    latency_ms: float
    datasets: tuple[str, ...]
    message: str


@dataclass(frozen=True)
class SourceBatch:
    source_id: str
    source_priority: int
    dataset: str
    frame: pd.DataFrame
    ingested_at: datetime
    vendor_revision: str
    contract: DatasetContract
    request: FetchRequest
    lineage: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.source_priority < 0:
            raise ValueError("source_priority must be non-negative; lower values win")
        if self.dataset != self.contract.dataset or self.request.dataset != self.dataset:
            raise ValueError("batch, request, and contract datasets must match")
        if self.ingested_at.tzinfo is None:
            raise ValueError("ingested_at must be timezone-aware")
        object.__setattr__(self, "frame", self.frame.copy())


def validate_source_frame(frame: pd.DataFrame, contract: DatasetContract) -> None:
    """Fail closed on structural provider-contract violations."""

    declared = set(contract.field_map)
    missing = sorted(declared - set(frame.columns))
    if missing:
        raise SourceContractError(f"{contract.dataset} missing contracted fields: {missing}")
    if frame.empty and not contract.allows_empty:
        raise SourceContractError(f"{contract.dataset} returned no rows")
    null_key_fields = [name for name in contract.key_fields if frame[name].isna().any()]
    if null_key_fields:
        raise SourceContractError(
            f"{contract.dataset} has null key fields: {sorted(null_key_fields)}"
        )
    if not frame.empty and frame.duplicated(list(contract.key_fields)).any():
        raise SourceContractError(f"{contract.dataset} has duplicate keys")
    for item in contract.fields:
        if not item.nullable and frame[item.name].isna().any():
            raise SourceContractError(
                f"{contract.dataset}.{item.name} is non-nullable but contains nulls"
            )


class SourceAdapter(ABC):
    """Abstract Bronze adapter with explicit priority and schema contracts."""

    def __init__(
        self,
        *,
        source_id: str,
        priority: int,
        contracts: Sequence[DatasetContract],
        lineage: Mapping[str, Any] | None = None,
        rate_limits: Mapping[str, RateLimit] | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        if not source_id.strip():
            raise ValueError("source_id is required")
        if priority < 0:
            raise ValueError("priority must be non-negative; lower values win")
        by_name = {item.dataset: item for item in contracts}
        if not by_name or len(by_name) != len(contracts):
            raise ValueError("contracts must be non-empty and dataset names unique")
        self.source_id = source_id
        self.priority = int(priority)
        self.contracts = by_name
        self.lineage = dict(lineage or {})
        self.rate_limits = dict(rate_limits or {})
        self.rate_limiter = rate_limiter or _PROCESS_RATE_LIMITER
        self._monotonic_clock = monotonic_clock
        self._sleeper = sleeper

    def _acquire_rate_limit(self, dataset: str) -> float:
        limit = self.rate_limits.get(dataset)
        if limit is None:
            return 0.0
        return self.rate_limiter.acquire(
            self.source_id,
            dataset,
            limit,
            monotonic_clock=self._monotonic_clock,
            sleeper=self._sleeper,
        )

    def contract_for(self, dataset: str) -> DatasetContract:
        try:
            return self.contracts[dataset]
        except KeyError as exc:
            raise SourceContractError(
                f"source {self.source_id!r} does not provide {dataset!r}"
            ) from exc

    def public_contract_identity(self) -> dict[str, Any]:
        """Return a deterministic, credential-free adapter contract identity.

        The identity is safe to persist in canary fingerprints and evidence.
        It describes the public routing/parsing contract while deliberately
        ignoring provider clients, credential values and credential locations.
        Custom controlled adapters receive a conservative class-and-schema
        fallback rather than unsafe attribute introspection.
        """

        return source_adapter_public_contract_identity(self)

    @abstractmethod
    def probe(self) -> ProbeResult:
        """Perform a real, bounded provider operation rather than checking config only."""

    @abstractmethod
    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        """Fetch a raw frame from the provider."""

    def fetch(self, request: FetchRequest) -> SourceBatch:
        # Request parameters become part of the immutable vendor revision and
        # later Bronze lineage.  Reject credential-shaped fields before a
        # provider call, even when a caller bypassed production-config
        # validation and constructed ``FetchRequest`` directly.
        assert_credential_free_request_parameters(request.parameters)
        contract = self.contract_for(request.dataset)
        self._acquire_rate_limit(request.dataset)
        frame = self._fetch_frame(request)
        if not isinstance(frame, pd.DataFrame):
            raise SourceContractError(
                f"source {self.source_id!r} returned {type(frame).__name__}, expected DataFrame"
            )
        if request.fields:
            omitted = sorted(set(request.fields) - set(frame.columns))
            if omitted:
                raise SourceContractError(f"requested fields missing from response: {omitted}")
        validate_source_frame(frame, contract)
        ingested_at = datetime.now(timezone.utc)
        # A vendor revision identifies the response bytes/semantics, not the
        # wall-clock time of a retry.  This makes Bronze ingestion restart-safe
        # while preserving ``ingested_at`` as an independent bitemporal field.
        revision_material = json.dumps(
            {
                "source_id": self.source_id,
                "dataset": request.dataset,
                "parameters": dict(request.parameters),
                "fields": list(request.fields),
                "columns": list(map(str, frame.columns)),
                "dtypes": [str(dtype) for dtype in frame.dtypes],
                "frame": frame.to_json(
                    orient="split",
                    date_format="iso",
                    date_unit="ns",
                    force_ascii=False,
                ),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return SourceBatch(
            source_id=self.source_id,
            source_priority=self.priority,
            dataset=request.dataset,
            frame=frame,
            ingested_at=ingested_at,
            vendor_revision=hashlib.sha256(revision_material.encode("utf-8")).hexdigest(),
            contract=contract,
            request=request,
            lineage={"adapter": type(self).__name__, **self.lineage},
        )


def _call_provider(client: Any, endpoint: str, parameters: Mapping[str, Any]) -> Any:
    target = getattr(client, endpoint, None)
    if callable(target):
        return target(**dict(parameters))
    query = getattr(client, "query", None)
    if callable(query):
        return query(endpoint, **dict(parameters))
    pro = getattr(client, "pro", None)
    if pro is not None:
        target = getattr(pro, endpoint, None)
        if callable(target):
            return target(**dict(parameters))
        query = getattr(pro, "query", None)
        if callable(query):
            return query(endpoint, **dict(parameters))
    raise TypeError(f"provider client has no callable endpoint {endpoint!r}")


class TushareSourceAdapter(SourceAdapter):
    def __init__(
        self,
        client: Any,
        *,
        contracts: Sequence[DatasetContract],
        endpoint_map: Mapping[str, str] | None = None,
        priority: int = 10,
        lineage: Mapping[str, Any] | None = None,
        rate_limits: Mapping[str, RateLimit] | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        super().__init__(
            source_id="tushare",
            priority=priority,
            contracts=contracts,
            lineage={
                **dict(lineage or {}),
                "endpoint_map": {
                    str(key): str(value)
                    for key, value in (endpoint_map or {}).items()
                },
            },
            rate_limits=rate_limits,
            rate_limiter=rate_limiter,
            monotonic_clock=monotonic_clock,
            sleeper=sleeper,
        )
        self.client = harden_tushare_client_transport(client)
        if _tushare_sdk_client(self.client):
            self._base_url: str | None = _tushare_base_url(self.client)
            self._transport_policy = MappingProxyType({
                "schema_version": "research-os/tushare-sealed-https/v1",
                "https_only": True,
                "redirects_allowed": False,
                "trust_environment": False,
            })
        else:
            # Unit/provider fixtures have no network authority.  Their public
            # identity remains deterministic and visibly non-production.
            self._base_url = None
            self._transport_policy = MappingProxyType({
                "schema_version": "research-os/tushare-injected-fixture/v1",
                "formal_production_transport": False,
            })
        self.endpoint_map = dict(endpoint_map or {})

    @property
    def base_url(self) -> str | None:
        return self._base_url

    @property
    def transport_policy(self) -> Mapping[str, Any]:
        return self._transport_policy

    def probe(self) -> ProbeResult:
        started = time.perf_counter()
        try:
            self._acquire_rate_limit("trade_cal")
            value = _call_provider(
                self.client,
                "trade_cal",
                {
                    "exchange": "SSE",
                    "start_date": "20240102",
                    "end_date": "20240102",
                    "fields": "exchange,cal_date,is_open",
                },
            )
            if not isinstance(value, pd.DataFrame):
                raise TypeError("trade_cal probe did not return a DataFrame")
            health = SourceHealth.HEALTHY
            message = "trade_cal endpoint reachable"
        except Exception as exc:
            health = SourceHealth.UNAVAILABLE
            message = _safe_probe_failure_message("Tushare", exc)
        return ProbeResult(
            source_id=self.source_id,
            health=health,
            checked_at=datetime.now(timezone.utc),
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            datasets=tuple(sorted(self.contracts)),
            message=message,
        )

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        endpoint = self.endpoint_map.get(request.dataset, request.dataset)
        parameters = dict(request.parameters)
        if request.fields:
            parameters["fields"] = ",".join(request.fields)
        provider_failed = False
        try:
            value = _call_provider(self.client, endpoint, parameters)
        except _ProviderResponseStatusError:
            raise
        except Exception:
            # SDK/provider exceptions can embed response bodies, request
            # parameters or the token. Never expose that exception object as
            # a public adapter error or traceback cause.
            provider_failed = True
        if provider_failed:
            raise SourceContractError(
                "Tushare provider request failed (provider_error)"
            )
        return value.copy() if isinstance(value, pd.DataFrame) else value


class AkShareSourceAdapter(SourceAdapter):
    def __init__(
        self,
        client: Any,
        *,
        contracts: Sequence[DatasetContract],
        endpoint_map: Mapping[str, str],
        probe_endpoint: str | None = None,
        probe_parameters: Mapping[str, Any] | None = None,
        column_mapping: Mapping[str, str] | None = None,
        constant_fields: Mapping[str, Any] | None = None,
        priority: int = 20,
        lineage: Mapping[str, Any] | None = None,
        rate_limits: Mapping[str, RateLimit] | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        super().__init__(
            source_id="akshare",
            priority=priority,
            contracts=contracts,
            lineage={
                **dict(lineage or {}),
                "endpoint_map": {
                    str(key): str(value) for key, value in endpoint_map.items()
                },
                "response_field_mapping": dict(column_mapping or {}),
                "constant_fields": sorted(map(str, (constant_fields or {}).keys())),
            },
            rate_limits=rate_limits,
            rate_limiter=rate_limiter,
            monotonic_clock=monotonic_clock,
            sleeper=sleeper,
        )
        self.client = client
        self.endpoint_map = dict(endpoint_map)
        if not self.endpoint_map:
            raise ValueError("AkShare endpoint_map must be non-empty")
        self.probe_endpoint = str(
            probe_endpoint or next(iter(self.endpoint_map.values()))
        )
        self.probe_parameters = dict(
            probe_parameters
            if probe_parameters is not None
            else {
                "symbol": "000001",
                "period": "daily",
                "start_date": "20240102",
                "end_date": "20240103",
                "adjust": "",
            }
            if self.probe_endpoint == "stock_zh_a_hist"
            else {}
        )
        self.column_mapping = {
            str(source): str(target)
            for source, target in (column_mapping or {}).items()
        }
        self.constant_fields = dict(constant_fields or {})
        targets = list(self.column_mapping.values())
        if len(targets) != len(set(targets)):
            raise ValueError("AkShare column_mapping targets must be unique")

    def probe(self) -> ProbeResult:
        started = time.perf_counter()
        try:
            self._acquire_rate_limit(self.probe_endpoint)
            value = _call_provider(
                self.client, self.probe_endpoint, self.probe_parameters
            )
            if not isinstance(value, pd.DataFrame) or value.empty:
                raise SourceContractError("AkShare probe returned no rows")
            health = SourceHealth.HEALTHY
            message = f"{self.probe_endpoint} endpoint reachable"
        except Exception as exc:
            health = SourceHealth.UNAVAILABLE
            message = f"{type(exc).__name__}: {exc}"
        return ProbeResult(
            source_id=self.source_id,
            health=health,
            checked_at=datetime.now(timezone.utc),
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            datasets=tuple(sorted(self.contracts)),
            message=message,
        )

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        try:
            endpoint = self.endpoint_map[request.dataset]
        except KeyError as exc:
            raise SourceContractError(
                f"no AkShare endpoint configured for {request.dataset!r}"
            ) from exc
        value = _call_provider(self.client, endpoint, request.parameters)
        if not isinstance(value, pd.DataFrame):
            return value
        # Preserve the provider's raw columns and add only aliases explicitly
        # declared in configuration.  This keeps Bronze evidence inspectable
        # while preventing an implicit Chinese/English field rename from
        # changing the research contract.
        frame = value.copy()
        missing_sources = sorted(set(self.column_mapping) - set(frame.columns))
        if missing_sources:
            raise SourceContractError(
                f"AkShare response_field_mapping sources missing: {missing_sources}"
            )
        for source, target in self.column_mapping.items():
            if target in frame.columns and not frame[target].equals(frame[source]):
                raise SourceContractError(
                    f"AkShare mapped field {target!r} collides with provider response"
                )
            frame[target] = frame[source]
        for target, value in self.constant_fields.items():
            if target in frame.columns and not frame[target].eq(value).all():
                raise SourceContractError(
                    f"AkShare constant field {target!r} collides with provider response"
                )
            frame[target] = value
        return frame


_DIEMENG_CANONICAL_HOST = "data.diemeng.chat"
_DIEMENG_DIRECT_HOSTS = frozenset({"diemeng.chat", "mg.diemeng.chat"})


def normalize_diemeng_base_url(value: str) -> str:
    """Return the stable direct API origin used by the existing go-stock client.

    The public landing/API hosts have historically redirected direct clients to
    ``data.diemeng.chat``.  Normalising once keeps provenance stable and avoids
    TLS host surprises while still preserving custom/private deployments.
    """

    raw = str(value or "").strip().rstrip("/")
    if not raw:
        raise ValueError("Diemeng base_url is required")
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Diemeng base_url must be an http(s) URL")
    if parsed.username is not None or parsed.password is not None:
        # URL userinfo is both an SSRF ambiguity and an easy way for a raw
        # credential to enter lineage/error output.  It is never a supported
        # authentication mechanism; the adapter uses the reviewed apiKey
        # header exclusively.
        raise ValueError("Diemeng base_url must not contain URL userinfo")
    host = parsed.hostname.casefold()
    if host in _DIEMENG_DIRECT_HOSTS:
        port = f":{parsed.port}" if parsed.port is not None else ""
        parsed = parsed._replace(netloc=f"{_DIEMENG_CANONICAL_HOST}{port}")
    path = parsed.path.rstrip("/")
    if not path:
        path = "/api"
    return urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


def validate_production_diemeng_base_url(value: str) -> str:
    """Return the sole reviewed Diemeng production API URL.

    Historical public aliases are canonicalised first, but the effective
    destination must be HTTPS on ``data.diemeng.chat`` with no custom port.
    Validation is deliberately side-effect free so callers can reject unsafe
    routing before reading a credential or creating a network client.
    """

    normalized = normalize_diemeng_base_url(value)
    parsed = urlparse(normalized)
    if parsed.scheme != "https":
        raise ValueError("production Diemeng base_url must use HTTPS")
    if parsed.hostname is None or parsed.hostname.casefold() != _DIEMENG_CANONICAL_HOST:
        raise ValueError(
            "production Diemeng base_url host must be data.diemeng.chat"
        )
    if parsed.port not in {None, 443}:
        raise ValueError("production Diemeng base_url must not use a custom port")
    # Collapse an explicit default port so the public identity has one
    # canonical spelling regardless of operator input.
    if parsed.port == 443:
        parsed = parsed._replace(netloc=_DIEMENG_CANONICAL_HOST)
    return urlunparse(parsed)


def _mapping_path(value: Any, path: str) -> Any:
    current = value
    for part in (item for item in str(path).split(".") if item):
        if not isinstance(current, Mapping) or part not in current:
            raise SourceContractError(f"Diemeng response path {path!r} is missing")
        current = current[part]
    return current


def _safe_diemeng_lineage(lineage: Mapping[str, Any] | None) -> dict[str, Any]:
    """Keep reviewed public lineage while excluding credential material/URLs."""

    def cleaned(item: Any) -> Any:
        if isinstance(item, Mapping):
            result: dict[str, Any] = {}
            for raw_key, nested in item.items():
                key = str(raw_key)
                normalized = "".join(ch for ch in key.casefold() if ch.isalnum())
                if normalized != "credentialbinding" and any(
                    marker in normalized
                    for marker in (
                        "apikey",
                        "authorization",
                        "client",
                        "credential",
                        "password",
                        "secret",
                        "token",
                    )
                ):
                    continue
                result[key] = cleaned(nested)
            return result
        if isinstance(item, tuple):
            return tuple(cleaned(value) for value in item)
        if isinstance(item, list):
            return [cleaned(value) for value in item]
        if isinstance(item, str) and "://" in item:
            parsed = urlparse(item)
            if parsed.username is not None or parsed.password is not None:
                raise ValueError("Diemeng lineage must not contain URL userinfo")
        return item

    return cleaned(dict(lineage or {}))


class DiemengSourceAdapter(SourceAdapter):
    """Bounded REST adapter for the Diemeng capabilities verified by go-stock.

    No undocumented dataset is inferred.  Production configuration must map
    every dataset to an endpoint/method and may optionally declare an explicit
    response path.  Authentication is supplied by the caller and is never
    copied into lineage or error messages.
    """

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        contracts: Sequence[DatasetContract],
        endpoint_map: Mapping[str, str],
        method_map: Mapping[str, str] | None = None,
        response_paths: Mapping[str, str] | None = None,
        column_mapping: Mapping[str, str] | None = None,
        constant_fields: Mapping[str, Any] | None = None,
        priority: int = 20,
        timeout_seconds: float = 60.0,
        max_attempts: int = 3,
        probe_dataset: str | None = None,
        probe_parameters: Mapping[str, Any] | None = None,
        lineage: Mapping[str, Any] | None = None,
        rate_limits: Mapping[str, RateLimit] | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
        session: requests.Session | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        normalized_url = normalize_diemeng_base_url(base_url)
        key = str(api_key or "").strip()
        if not key:
            raise ValueError("Diemeng api_key is required")
        endpoints = {str(k): str(v) for k, v in endpoint_map.items()}
        if not endpoints:
            raise ValueError("Diemeng endpoint_map must be non-empty")
        methods = {
            str(k): str(v).strip().upper()
            for k, v in (method_map or {}).items()
        }
        invalid_methods = sorted(
            {value for value in methods.values() if value not in {"GET", "POST"}}
        )
        if invalid_methods:
            raise ValueError(f"unsupported Diemeng methods: {invalid_methods}")
        if timeout_seconds <= 0 or max_attempts <= 0:
            raise ValueError("Diemeng timeout/max_attempts must be positive")
        safe_lineage = _safe_diemeng_lineage(lineage)
        super().__init__(
            source_id="diemeng",
            priority=priority,
            contracts=contracts,
            lineage={
                **safe_lineage,
                "base_url": normalized_url,
                "endpoint_map": endpoints,
                "method_map": methods,
            },
            rate_limits=rate_limits,
            rate_limiter=rate_limiter,
            sleeper=sleeper,
        )
        self.base_url = normalized_url
        self._api_key = key
        self.endpoint_map = endpoints
        self.method_map = methods
        self.response_paths = {
            str(k): str(v) for k, v in (response_paths or {}).items()
        }
        self.column_mapping = {
            str(k): str(v) for k, v in (column_mapping or {}).items()
        }
        self.constant_fields = dict(constant_fields or {})
        self.timeout_seconds = float(timeout_seconds)
        self.max_attempts = int(max_attempts)
        self.probe_dataset = str(probe_dataset or next(iter(self.contracts)))
        if self.probe_dataset not in self.contracts:
            raise ValueError("Diemeng probe_dataset must be one of the declared contracts")
        self.probe_parameters = dict(probe_parameters or {})
        selected_session = session or requests.Session()
        try:
            # Requests otherwise consumes proxy, CA-bundle and netrc settings
            # from the worker environment.  Production constructs an exact
            # pristine Session; controlled fixtures merely need to expose the
            # same explicit ``trust_env`` boundary.
            selected_session.trust_env = False
        except Exception as exc:
            raise ValueError(
                "Diemeng HTTP session cannot disable ambient environment"
            ) from exc
        if getattr(selected_session, "trust_env", None) is not False:
            raise ValueError("Diemeng HTTP session still trusts ambient environment")
        if type(selected_session) is requests.Session and not _requests_session_is_direct(
            selected_session
        ):
            raise ValueError("Diemeng HTTP session contains unreviewed transport state")
        self.session = selected_session
        self._retry_sleep = sleeper or time.sleep

    def _request_json(
        self, dataset: str, parameters: Mapping[str, Any]
    ) -> Mapping[str, Any]:
        try:
            endpoint = self.endpoint_map[dataset]
        except KeyError as exc:
            raise SourceContractError(
                f"no Diemeng endpoint configured for {dataset!r}"
            ) from exc
        if not endpoint.startswith("/") or ".." in endpoint:
            raise SourceContractError("Diemeng endpoint must be an absolute API path")
        method = self.method_map.get(dataset, "GET")
        url = f"{self.base_url}{endpoint}"
        last_status_error: tuple[str, int] | None = None
        had_unclassified_failure = False
        for attempt in range(1, self.max_attempts + 1):
            try:
                kwargs: dict[str, Any] = {
                    "headers": {"apiKey": self._api_key},
                    "timeout": self.timeout_seconds,
                    # Never let requests carry the custom apiKey header to a
                    # redirect target.  Provider routing is part of the
                    # reviewed source contract and redirects fail closed.
                    "allow_redirects": False,
                }
                if method == "POST":
                    kwargs["json"] = dict(parameters)
                else:
                    kwargs["params"] = dict(parameters)
                response = self.session.request(method, url, **kwargs)
                status = int(response.status_code)
                if 300 <= status < 400:
                    raise _ProviderResponseStatusError(
                        provider="Diemeng",
                        classification="redirect_status",
                        numeric_code=status,
                    )
                if status == 429 or status >= 500:
                    last_status_error = ("http_status", status)
                    had_unclassified_failure = False
                    if attempt >= self.max_attempts:
                        break
                    self._retry_sleep(min(8.0, 0.5 * (2 ** (attempt - 1))))
                    continue
                if status >= 400:
                    raise _ProviderResponseStatusError(
                        provider="Diemeng",
                        classification="http_status",
                        numeric_code=status,
                    )
                payload = response.json()
                if not isinstance(payload, Mapping):
                    raise SourceContractError("Diemeng response must be an object")
                invalid_code = False
                try:
                    code = int(payload.get("code", 0))
                except (TypeError, ValueError):
                    invalid_code = True
                if invalid_code:
                    raise SourceContractError("Diemeng response code is invalid")
                if code != 200:
                    raise _ProviderResponseStatusError(
                        provider="Diemeng",
                        classification="api_code",
                        numeric_code=code,
                    )
                return payload
            except SourceContractError:
                raise
            except Exception:  # network/JSON failures; never retain raw error
                last_status_error = None
                had_unclassified_failure = True
                if attempt >= self.max_attempts:
                    break
                self._retry_sleep(min(8.0, 0.5 * (2 ** (attempt - 1))))
        if last_status_error is not None:
            classification, numeric_code = last_status_error
            raise _ProviderResponseStatusError(
                provider="Diemeng",
                classification=classification,
                numeric_code=numeric_code,
            )
        assert had_unclassified_failure
        raise SourceContractError(
            "Diemeng request failed "
            f"(transport_or_response_error; attempts={self.max_attempts})"
        )

    def _rows(self, dataset: str, payload: Mapping[str, Any]) -> Any:
        explicit = self.response_paths.get(dataset)
        if explicit:
            return _mapping_path(payload, explicit)
        data = payload.get("data")
        if isinstance(data, Mapping):
            for key in ("items", "list"):
                if key in data:
                    return data[key]
        return data

    def _frame(self, dataset: str, payload: Mapping[str, Any]) -> pd.DataFrame:
        rows = self._rows(dataset, payload)
        if isinstance(rows, list):
            frame = pd.DataFrame(rows)
        elif isinstance(rows, Mapping):
            frame = pd.DataFrame([dict(rows)])
        else:
            raise SourceContractError(
                f"Diemeng {dataset!r} response contains no tabular data"
            )
        missing_sources = sorted(set(self.column_mapping) - set(frame.columns))
        if missing_sources:
            raise SourceContractError(
                f"Diemeng response_field_mapping sources missing: {missing_sources}"
            )
        for source, target in self.column_mapping.items():
            if target in frame.columns and not frame[target].equals(frame[source]):
                raise SourceContractError(
                    f"Diemeng mapped field {target!r} collides with response"
                )
            frame[target] = frame[source]
        for target, value in self.constant_fields.items():
            if target in frame.columns and not frame[target].eq(value).all():
                raise SourceContractError(
                    f"Diemeng constant field {target!r} collides with response"
                )
            frame[target] = value
        return frame

    def probe(self) -> ProbeResult:
        return self.probe_with_parameters(self.probe_parameters)

    def probe_with_parameters(
        self,
        parameters: Mapping[str, Any],
        *,
        dataset: str | None = None,
    ) -> ProbeResult:
        """Run one bounded probe without mutating the adapter contract.

        Production canaries need a recent trading-session request, while the
        adapter's configured probe parameters are part of its public source
        generation.  Passing the bounded request explicitly prevents a probe
        from silently changing that generation for later partitions.
        """

        started = time.perf_counter()
        probe_dataset = str(dataset or self.probe_dataset)
        if probe_dataset not in self.contracts:
            raise ValueError("Diemeng probe dataset must be one of the declared contracts")
        request_parameters: dict[str, Any] = dict(parameters)
        if not request_parameters and probe_dataset == "trade_calendar":
            request_parameters = {
                "start_time": "2024-01-02",
                "end_time": "2024-01-03",
            }
        try:
            self._acquire_rate_limit(probe_dataset)
            frame = self._frame(
                probe_dataset,
                self._request_json(probe_dataset, request_parameters),
            )
            if frame.empty and not self.contracts[probe_dataset].allows_empty:
                raise SourceContractError("Diemeng probe returned no rows")
            health = SourceHealth.HEALTHY
            message = f"{probe_dataset} endpoint reachable"
        except Exception as exc:
            health = SourceHealth.UNAVAILABLE
            message = _safe_probe_failure_message("Diemeng", exc)
        return ProbeResult(
            source_id=self.source_id,
            health=health,
            checked_at=datetime.now(timezone.utc),
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            datasets=tuple(sorted(self.contracts)),
            message=message,
        )

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        return self._frame(
            request.dataset,
            self._request_json(request.dataset, request.parameters),
        )


class LocalFileSourceAdapter(SourceAdapter):
    """Read explicitly mapped local files without allowing paths to escape the root."""

    def __init__(
        self,
        root: str | Path,
        *,
        contracts: Sequence[DatasetContract],
        path_templates: Mapping[str, str],
        priority: int = 30,
        lineage: Mapping[str, Any] | None = None,
        readers: Mapping[str, Callable[[Path], pd.DataFrame]] | None = None,
    ) -> None:
        resolved_root = Path(root).resolve()
        templates = {
            str(key): str(value) for key, value in path_templates.items()
        }
        super().__init__(
            source_id="local_file",
            priority=priority,
            contracts=contracts,
            lineage={
                **dict(lineage or {}),
                "local_root": str(resolved_root),
                "path_templates": templates,
            },
        )
        self.root = resolved_root
        self.path_templates = templates
        self.readers = dict(readers or {})

    def _path_for(self, request: FetchRequest) -> Path:
        try:
            relative = self.path_templates[request.dataset].format(**dict(request.parameters))
        except KeyError as exc:
            raise SourceContractError(
                f"local path template or parameter missing for {request.dataset!r}: {exc}"
            ) from exc
        path = (self.root / relative).resolve()
        try:
            path.relative_to(self.root)
        except ValueError as exc:
            raise SourceContractError("local dataset path escapes configured root") from exc
        return path

    def probe(self) -> ProbeResult:
        started = time.perf_counter()
        missing = []
        unreadable = []
        if not self.root.is_dir():
            missing.append(str(self.root))
        else:
            for dataset, template in self.path_templates.items():
                if "{" in template:
                    continue
                path = (self.root / template).resolve()
                if not path.is_file():
                    missing.append(dataset)
                    continue
                try:
                    with path.open("rb") as handle:
                        handle.read(1)
                except OSError:
                    unreadable.append(dataset)
        if missing or unreadable:
            health = SourceHealth.UNAVAILABLE
            message = f"missing={missing}; unreadable={unreadable}"
        else:
            health = SourceHealth.HEALTHY
            message = "configured local files are readable"
        return ProbeResult(
            source_id=self.source_id,
            health=health,
            checked_at=datetime.now(timezone.utc),
            latency_ms=round((time.perf_counter() - started) * 1000.0, 3),
            datasets=tuple(sorted(self.contracts)),
            message=message,
        )

    def _fetch_frame(self, request: FetchRequest) -> pd.DataFrame:
        path = self._path_for(request)
        if not path.is_file():
            raise FileNotFoundError(path)
        suffix = path.suffix.lower()
        if suffix in self.readers:
            frame = self.readers[suffix](path)
        elif suffix in {".parquet", ".pq"}:
            frame = pd.read_parquet(path, columns=list(request.fields) or None)
        elif suffix == ".csv":
            frame = pd.read_csv(path, usecols=list(request.fields) or None)
        elif suffix in {".json", ".jsonl", ".ndjson"}:
            frame = pd.read_json(path, lines=suffix in {".jsonl", ".ndjson"})
            if request.fields:
                frame = frame.loc[:, list(request.fields)]
        else:
            raise SourceContractError(f"unsupported local data format: {suffix}")
        return frame


_SOURCE_ADAPTER_PUBLIC_IDENTITY_SCHEMA = (
    "research-os/source-adapter-public-contract/v1"
)
_SENSITIVE_PUBLIC_IDENTITY_KEYS = frozenset(
    {
        "apikey",
        "authorization",
        "client",
        "credential",
        "credentialref",
        "password",
        "secret",
        "token",
    }
)


def _is_sensitive_public_identity_key(value: Any) -> bool:
    normalized = "".join(
        character for character in str(value).casefold() if character.isalnum()
    )
    return any(
        marker in normalized
        for marker in _SENSITIVE_PUBLIC_IDENTITY_KEYS
    )


def _public_contract_value(value: Any) -> Any:
    """Convert reviewed contract values to canonical JSON-safe data.

    Arbitrary objects are represented only by their type.  This is important
    for controlled test adapters: a client object or its repr must never enter
    a persisted identity.  Credential-looking mapping entries are represented
    by a fixed marker so rotating a credential neither leaks nor changes the
    public contract hash.
    """

    if isinstance(value, str) and "://" in value:
        parsed = urlparse(value)
        if parsed.username is not None or parsed.password is not None:
            return {"redacted": "url_userinfo"}
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            return {"non_finite_float": "nan"}
        return {"non_finite_float": "positive_inf" if value > 0 else "negative_inf"}
    if isinstance(value, Enum):
        return _public_contract_value(value.value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Mapping):
        public: dict[str, Any] = {}
        for key in sorted(value, key=lambda item: str(item)):
            name = str(key)
            public[name] = (
                {"redacted": "credential_material"}
                if _is_sensitive_public_identity_key(name)
                else _public_contract_value(value[key])
            )
        return public
    if isinstance(value, (tuple, list)):
        return [_public_contract_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        items = [_public_contract_value(item) for item in value]
        return sorted(items, key=_canonical_public_contract_json)
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    return {
        "opaque_type": (
            f"{type(value).__module__}.{type(value).__qualname__}"
        )
    }


def _canonical_public_contract_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _public_dataset_contract(contract: DatasetContract) -> dict[str, Any]:
    return {
        "dataset": contract.dataset,
        "key_fields": list(contract.key_fields),
        "event_time_field": contract.event_time_field,
        "release_timing": contract.release_timing,
        "allows_empty": bool(contract.allows_empty),
        "fields": [
            {
                "name": item.name,
                "dtype": item.dtype,
                "nullable": bool(item.nullable),
                "unit": item.unit,
                "adjustment": item.adjustment,
                "release_timing": item.release_timing,
            }
            for item in contract.fields
        ],
    }


def _public_profile_lineage(lineage: Mapping[str, Any]) -> dict[str, str] | None:
    public = {
        key: str(lineage[key]).strip()
        for key in ("profile_name", "profile_source_type")
        if str(lineage.get(key) or "").strip()
    }
    if not public:
        return None
    public_hash = hashlib.sha256(
        _canonical_public_contract_json(public).encode("utf-8")
    ).hexdigest()
    return {**public, "public_hash": public_hash}


def _public_url_without_userinfo(value: str) -> str:
    """Preserve a public origin/path while dropping URL authentication."""

    parsed = urlparse(str(value))
    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    port = f":{parsed.port}" if parsed.port is not None else ""
    return urlunparse(
        parsed._replace(
            netloc=f"{hostname}{port}",
            params="",
            query="",
            fragment="",
        )
    )


def _adapter_public_routing(adapter: SourceAdapter) -> dict[str, Any]:
    datasets = tuple(sorted(adapter.contracts))
    if isinstance(adapter, DiemengSourceAdapter):
        return {
            "base_url": _public_url_without_userinfo(adapter.base_url),
            "datasets": {
                dataset: {
                    "endpoint": adapter.endpoint_map.get(dataset),
                    "method": adapter.method_map.get(dataset, "GET"),
                    "response_path": adapter.response_paths.get(dataset),
                }
                for dataset in datasets
            },
            "response_field_mapping": _public_contract_value(
                adapter.column_mapping
            ),
            "constant_fields": _public_contract_value(adapter.constant_fields),
            "probe": {
                "dataset": adapter.probe_dataset,
                "parameters": _public_contract_value(adapter.probe_parameters),
            },
        }
    if isinstance(adapter, TushareSourceAdapter):
        return {
            "base_url": adapter.base_url,
            "transport_policy": _public_contract_value(
                adapter.transport_policy
            ),
            "datasets": {
                dataset: {
                    "endpoint": adapter.endpoint_map.get(dataset, dataset),
                }
                for dataset in datasets
            },
            "probe": {"endpoint": "trade_cal"},
        }
    if isinstance(adapter, AkShareSourceAdapter):
        return {
            "datasets": {
                dataset: {"endpoint": adapter.endpoint_map.get(dataset)}
                for dataset in datasets
            },
            "response_field_mapping": _public_contract_value(
                adapter.column_mapping
            ),
            "constant_fields": _public_contract_value(adapter.constant_fields),
            "probe": {
                "endpoint": adapter.probe_endpoint,
                "parameters": _public_contract_value(adapter.probe_parameters),
            },
        }
    if isinstance(adapter, LocalFileSourceAdapter):
        return {
            "datasets": {
                dataset: {
                    # A path template affects the contract, but local paths are
                    # not public evidence and therefore enter only as a digest.
                    "path_template_sha256": hashlib.sha256(
                        str(adapter.path_templates.get(dataset, "")).encode("utf-8")
                    ).hexdigest()
                }
                for dataset in datasets
            }
        }
    return {
        "datasets": {
            dataset: {"endpoint": "adapter_defined"}
            for dataset in datasets
        },
        "fallback": "declared_schema_and_adapter_class_only",
    }


def source_adapter_public_contract_identity(
    adapter: SourceAdapter,
) -> dict[str, Any]:
    """Build a stable public identity for one concrete ``SourceAdapter``.

    The returned mapping contains its own ``public_contract_hash``.  The hash
    covers adapter class/source/priority, every declared dataset contract,
    built-in route and parser settings, and a credential-free profile lineage.
    It never inspects a provider client and never includes credential values or
    credential reference paths.
    """

    if not isinstance(adapter, SourceAdapter):
        raise TypeError("adapter must be a SourceAdapter")
    payload: dict[str, Any] = {
        "schema_version": _SOURCE_ADAPTER_PUBLIC_IDENTITY_SCHEMA,
        "adapter_class": {
            "module": type(adapter).__module__,
            "qualname": type(adapter).__qualname__,
        },
        "source_id": adapter.source_id,
        "priority": int(adapter.priority),
        "profile_lineage": _public_profile_lineage(adapter.lineage),
        "dataset_contracts": {
            dataset: _public_dataset_contract(adapter.contracts[dataset])
            for dataset in sorted(adapter.contracts)
        },
        "routing_contract": _adapter_public_routing(adapter),
    }
    public_contract_hash = hashlib.sha256(
        _canonical_public_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return {**payload, "public_contract_hash": public_contract_hash}


__all__ = [
    "AkShareSourceAdapter",
    "DatasetContract",
    "DiemengSourceAdapter",
    "FetchRequest",
    "FieldContract",
    "LocalFileSourceAdapter",
    "ProbeResult",
    "RateLimit",
    "SourceAdapter",
    "SourceBatch",
    "SourceContractError",
    "SourceHealth",
    "TushareSourceAdapter",
    "TokenBucketRateLimiter",
    "assert_credential_free_request_parameters",
    "harden_tushare_client_transport",
    "is_credential_shaped_key",
    "normalize_diemeng_base_url",
    "require_tushare_sdk_https_transport",
    "validate_production_diemeng_base_url",
    "source_adapter_public_contract_identity",
    "tushare_client_uses_direct_transport",
    "validate_source_frame",
]
