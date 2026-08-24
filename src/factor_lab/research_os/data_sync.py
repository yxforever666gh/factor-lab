"""Configuration-driven Bronze ingestion used by CLI and Dagster assets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import importlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Mapping

import pandas as pd

from .data_quality import sha256_path
from .data_sources import (
    AkShareSourceAdapter,
    DatasetContract,
    DiemengSourceAdapter,
    FetchRequest,
    FieldContract,
    LocalFileSourceAdapter,
    RateLimit,
    SourceAdapter,
    SourceHealth,
    SourceObservationError,
    TushareSourceAdapter,
    assert_credential_free_request_parameters,
    require_tushare_sdk_https_transport,
    validate_production_diemeng_base_url,
)
from .object_store import S3ImmutableArchive


DATA_SOURCE_PROFILES_ENV = "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON"
DATA_SOURCE_ORDER_ENV = "FACTOR_LAB_DATA_SOURCE_ORDER"
SUPPORTED_PROFILE_TYPES = frozenset(
    {"tushare", "akshare", "diemeng", "local_file"}
)
SECRETS_DIR_ENV = "FACTOR_LAB_SECRETS_DIR"
_SECRET_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")


class BronzeObservationError(RuntimeError):
    """A provider probe/fetch or returned-data contract failure.

    This narrow type lets an explicitly reviewed non-blocking sample degrade
    without also swallowing lake, object-store, manifest, catalog or ledger
    persistence failures.  The original message is deliberately not copied:
    provider exceptions may contain request details or credential material.
    """

    def __init__(self, failure_type: str) -> None:
        self.failure_type = str(failure_type or "provider_error")
        super().__init__("source provider observation failed")


@dataclass(frozen=True)
class BronzeSyncResult:
    source_id: str
    dataset: str
    rows: int
    data_path: str
    metadata_path: str
    sha256: str
    vendor_revision: str
    ingested_at: str
    probe_latency_ms: float
    data_object_uri: str | None = None
    metadata_object_uri: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def dataset_contract_from_mapping(payload: Mapping[str, Any]) -> DatasetContract:
    return DatasetContract(
        dataset=str(payload["dataset"]),
        key_fields=tuple(map(str, payload["key_fields"])),
        fields=tuple(
            FieldContract(
                name=str(row["name"]),
                dtype=str(row["dtype"]),
                nullable=bool(row.get("nullable", True)),
                unit=None if row.get("unit") is None else str(row["unit"]),
                adjustment=(
                    None if row.get("adjustment") is None else str(row["adjustment"])
                ),
                release_timing=(
                    None
                    if row.get("release_timing") is None
                    else str(row["release_timing"])
                ),
            )
            for row in payload["fields"]
        ),
        event_time_field=str(payload["event_time_field"]),
        release_timing=str(payload["release_timing"]),
        allows_empty=bool(payload.get("allows_empty", False)),
    )


def _rate_limits(payload: Mapping[str, Any]) -> dict[str, RateLimit]:
    return {
        str(dataset): RateLimit(
            requests=int(row["requests"]),
            per_seconds=float(row["per_seconds"]),
            burst=int(row.get("burst", 1)),
        )
        for dataset, row in (payload.get("rate_limits") or {}).items()
    }


def _boolish(value: Any, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on", "enabled"}:
        return True
    if text in {"0", "false", "no", "off", "disabled"}:
        return False
    raise ValueError(f"invalid data-source enabled value: {value!r}")


def _read_credential_file(path: Path, *, root: Path | None = None) -> str:
    resolved = path.resolve(strict=True)
    if root is not None:
        resolved_root = root.resolve(strict=True)
        try:
            resolved.relative_to(resolved_root)
        except ValueError as exc:
            raise ValueError("credential file escapes configured secrets root") from exc
    if not resolved.is_file():
        raise ValueError("credential reference is not a file")
    value = resolved.read_text(encoding="utf-8").strip()
    if not value or value == "replace-me":
        raise ValueError("credential reference is empty or a placeholder")
    return value


def resolve_credential(
    *,
    credential_ref: str | None,
    env_name: str,
    env: Mapping[str, str],
    legacy_inline: str | None = None,
) -> str:
    """Resolve a secret without copying it into config lineage.

    Production uses ``secret://name`` under ``FACTOR_LAB_SECRETS_DIR`` (the
    Compose default is ``/run/secrets``). ``env://NAME`` and ``NAME_FILE`` are
    supported for local operators. Inline values remain migration-compatible
    but production readiness rejects them.
    """

    reference = str(credential_ref or "").strip()
    if reference:
        if reference.startswith("secret://"):
            name = reference.removeprefix("secret://")
            if not _SECRET_NAME.fullmatch(name):
                raise ValueError("invalid secret credential reference")
            root = Path(str(env.get(SECRETS_DIR_ENV) or "/run/secrets"))
            return _read_credential_file(root / name, root=root)
        if reference.startswith("env://"):
            name = reference.removeprefix("env://")
            if not _SECRET_NAME.fullmatch(name):
                raise ValueError("invalid environment credential reference")
            value = str(env.get(name) or "").strip()
            if not value or value == "replace-me":
                raise ValueError(f"credential environment variable {name!r} is not configured")
            return value
        raise ValueError("credential_ref must use secret:// or env://")

    file_value = str(env.get(f"{env_name}_FILE") or "").strip()
    if file_value:
        return _read_credential_file(Path(file_value))
    inline = str(legacy_inline or "").strip()
    if inline and inline != "replace-me":
        return inline
    value = str(env.get(env_name) or "").strip()
    if not value or value == "replace-me":
        raise ValueError(f"credential {env_name!r} is not configured")
    return value


def configured_source_profiles(env: Mapping[str, str]) -> tuple[dict[str, Any], ...]:
    """Load the WebUI profile ledger in its deterministic enabled order.

    Invalid or ambiguous profile configuration is a data-plane error.  It must
    never be treated as an instruction to fall back to an arbitrary provider.
    Secrets remain in memory and are not copied into Bronze metadata.
    """

    raw = str(env.get(DATA_SOURCE_PROFILES_ENV) or "").strip()
    if not raw:
        return ()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{DATA_SOURCE_PROFILES_ENV} is not valid JSON") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"{DATA_SOURCE_PROFILES_ENV} must contain a JSON list")
    profiles: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, item in enumerate(parsed):
        if not isinstance(item, Mapping):
            raise ValueError(f"data-source profile {index} must be an object")
        name = str(item.get("name") or "").strip()
        source_type = str(item.get("source_type") or "").strip().lower()
        if not name:
            raise ValueError(f"data-source profile {index} has no name")
        if name in names:
            raise ValueError(f"duplicate data-source profile name: {name!r}")
        if source_type not in SUPPORTED_PROFILE_TYPES:
            raise ValueError(
                f"data-source profile {name!r} has unsupported type {source_type!r}"
            )
        names.add(name)
        if not _boolish(item.get("enabled"), default=True):
            continue
        profiles.append(
            {
                "name": name,
                "source_type": source_type,
                "api_key": str(item.get("api_key") or ""),
                "credential_ref": str(
                    item.get("credential_ref")
                    or (
                        item.get("extra", {}).get("credential_ref")
                        if isinstance(item.get("extra"), Mapping)
                        else ""
                    )
                    or ""
                ),
                "extra": dict(item.get("extra") or {})
                if isinstance(item.get("extra"), Mapping)
                else {},
            }
        )
    order = [
        item.strip()
        for item in str(env.get(DATA_SOURCE_ORDER_ENV) or "").split(",")
        if item.strip()
    ]
    if len(order) != len(set(order)):
        raise ValueError(f"{DATA_SOURCE_ORDER_ENV} contains duplicate profile names")
    if order:
        enabled_names = {str(item["name"]) for item in profiles}
        unknown = sorted(set(order) - enabled_names)
        if unknown:
            raise ValueError(
                f"{DATA_SOURCE_ORDER_ENV} references absent or disabled profiles: {unknown}"
            )
        rank = {name: index for index, name in enumerate(order)}
        profiles.sort(key=lambda row: (rank.get(str(row["name"]), len(rank)), str(row["name"])))
    return tuple(profiles)


def resolve_source_profile(
    payload: Mapping[str, Any], env: Mapping[str, str]
) -> dict[str, Any] | None:
    """Resolve one adapter to an enabled WebUI profile without fallback.

    When the profile ledger is absent, checked-in configuration and legacy
    credential environment variables remain compatible.  Once the ledger is
    configured, a source opts in with ``profile_name`` and is then bound to
    that exact enabled profile.  Inline canary/test specs without a binding are
    not affected by ambient WebUI state.
    """

    profiles = configured_source_profiles(env)
    requested_name = str(payload.get("profile_name") or "").strip()
    source_type = str(payload.get("source") or "").strip().lower()
    if not profiles:
        # Backward-compatible deployments may use only reviewed config plus
        # token_env.  As soon as a WebUI ledger exists, profile_name becomes a
        # strict binding rather than a hint.
        if requested_name and str(env.get(DATA_SOURCE_PROFILES_ENV) or "").strip():
            raise ValueError(f"source profile {requested_name!r} is absent or disabled")
        return None
    if not requested_name:
        # Inline/canary specs are intentionally self-contained and must not be
        # changed by an unrelated WebUI profile ledger in the worker process.
        # Production config opts in with an exact profile_name.
        return None
    candidates = [row for row in profiles if row["name"] == requested_name]
    if not candidates:
        raise ValueError(f"source profile {requested_name!r} is absent or disabled")
    selected = candidates[0]
    if selected["source_type"] != source_type:
        raise ValueError(
            f"source profile {requested_name!r} is {selected['source_type']!r}, "
            f"not {source_type!r}"
        )
    return selected


def _reviewed_diemeng_base_url(
    payload: Mapping[str, Any],
    profile_extra: Mapping[str, Any],
    env: Mapping[str, str],
) -> str:
    """Resolve a fail-closed, secret-independent Diemeng destination.

    A WebUI profile may repeat the reviewed URL but may not replace it.  This
    function intentionally runs before ``resolve_credential`` so an unsafe
    scheme, host or userinfo component cannot trigger secret-file reads or any
    provider setup.
    """

    configured = str(
        payload.get("base_url")
        or env.get("DIEMENG_BASE_URL")
        or "https://data.diemeng.chat/api"
    ).strip()
    reviewed = validate_production_diemeng_base_url(configured)
    profile_value = str(profile_extra.get("base_url") or "").strip()
    if profile_value:
        if not str(payload.get("base_url") or "").strip():
            raise ValueError(
                "Diemeng profile extra.base_url cannot replace reviewed configuration"
            )
        profile_url = validate_production_diemeng_base_url(profile_value)
        if profile_url != reviewed:
            raise ValueError(
                "Diemeng profile extra.base_url must match reviewed configuration"
            )
    return reviewed


def source_adapter_from_mapping(
    payload: Mapping[str, Any],
    *,
    env: Mapping[str, str] | None = None,
) -> SourceAdapter:
    """Build one explicit adapter without silently falling back to another vendor."""

    values = os.environ if env is None else env
    source = str(payload["source"]).lower()
    profile = resolve_source_profile(payload, values)
    profile_extra = dict((profile or {}).get("extra") or {})
    safe_lineage = (
        {}
        if profile is None
        else {
            "profile_name": str(profile["name"]),
            "profile_source_type": str(profile["source_type"]),
            "credential_binding": (
                str(profile.get("credential_ref") or "") or "legacy"
            ),
        }
    )
    contract = dataset_contract_from_mapping(payload["contract"])
    priority = int(payload.get("priority", 10))
    rate_limits = _rate_limits(payload)
    if source == "local_file":
        configured_root = profile_extra.get("root") or payload.get("root")
        if not configured_root:
            raise ValueError("local_file source requires an explicit root")
        return LocalFileSourceAdapter(
            root=Path(str(configured_root)),
            contracts=(contract,),
            path_templates={
                str(key): str(value)
                for key, value in (payload.get("path_templates") or {}).items()
            },
            priority=priority,
            lineage=safe_lineage,
        )
    if source == "tushare":
        # The installed SDK's transport contract must be HTTPS before a token
        # file/reference is touched.  The currently pinned HTTP-only SDK is an
        # explicit production blocker, not a reason to reuse the exposed key.
        sdk_origin = require_tushare_sdk_https_transport()
        reviewed_origin = str(payload.get("api_origin") or "").strip().rstrip("/")
        if not reviewed_origin or reviewed_origin != sdk_origin:
            raise ValueError(
                "Tushare SDK HTTPS origin must exactly match reviewed configuration"
            )
        token_env = str(payload.get("token_env") or "TUSHARE_TOKEN")
        token = resolve_credential(
            credential_ref=str(
                (profile or {}).get("credential_ref")
                or profile_extra.get("credential_ref")
                or payload.get("credential_ref")
                or ""
            ),
            env_name=token_env,
            env=values,
            legacy_inline=str((profile or {}).get("api_key") or ""),
        )
        tushare = importlib.import_module("tushare")
        client = tushare.pro_api(token)
        return TushareSourceAdapter(
            client,
            contracts=(contract,),
            endpoint_map={
                str(key): str(value)
                for key, value in (payload.get("endpoint_map") or {}).items()
            },
            priority=priority,
            lineage=safe_lineage,
            rate_limits=rate_limits,
        )
    if source == "diemeng":
        base_url = _reviewed_diemeng_base_url(payload, profile_extra, values)
        key_env = str(payload.get("token_env") or "DIEMENG_API_KEY")
        api_key = resolve_credential(
            credential_ref=str(
                (profile or {}).get("credential_ref")
                or profile_extra.get("credential_ref")
                or payload.get("credential_ref")
                or ""
            ),
            env_name=key_env,
            env=values,
            legacy_inline=str((profile or {}).get("api_key") or ""),
        )
        return DiemengSourceAdapter(
            base_url=base_url,
            api_key=api_key,
            contracts=(contract,),
            endpoint_map={
                str(key): str(value)
                for key, value in (payload.get("endpoint_map") or {}).items()
            },
            method_map={
                str(key): str(value)
                for key, value in (payload.get("method_map") or {}).items()
            },
            response_paths={
                str(key): str(value)
                for key, value in (payload.get("response_paths") or {}).items()
            },
            column_mapping={
                str(key): str(value)
                for key, value in (payload.get("response_field_mapping") or {}).items()
            },
            constant_fields=dict(payload.get("constant_fields") or {}),
            priority=priority,
            timeout_seconds=float(payload.get("timeout_seconds", 60.0)),
            max_attempts=int(payload.get("max_attempts", 3)),
            probe_dataset=(
                None
                if payload.get("probe_dataset") is None
                else str(payload["probe_dataset"])
            ),
            probe_parameters=dict(payload.get("probe_parameters") or {}),
            lineage=safe_lineage,
            rate_limits=rate_limits,
        )
    if source == "akshare":
        akshare = importlib.import_module("akshare")
        return AkShareSourceAdapter(
            akshare,
            contracts=(contract,),
            endpoint_map={
                str(key): str(value)
                for key, value in (payload.get("endpoint_map") or {}).items()
            },
            probe_endpoint=(
                None
                if payload.get("probe_endpoint") is None
                else str(payload["probe_endpoint"])
            ),
            probe_parameters=dict(payload.get("probe_parameters") or {}),
            column_mapping={
                str(key): str(value)
                for key, value in (payload.get("response_field_mapping") or {}).items()
            },
            constant_fields=dict(payload.get("constant_fields") or {}),
            priority=priority,
            lineage=safe_lineage,
            rate_limits=rate_limits,
        )
    raise ValueError(f"unsupported source adapter: {source!r}")


def sync_bronze(
    payload: Mapping[str, Any],
    *,
    lake_root: str | Path,
    env: Mapping[str, str] | None = None,
    now: datetime | None = None,
    object_store_archive: S3ImmutableArchive | None = None,
    classify_observation_failures: bool = False,
) -> BronzeSyncResult:
    request_payload = payload.get("request") or {}
    if not isinstance(request_payload, Mapping):
        raise ValueError("Bronze request must be an object")
    request_parameters = dict(request_payload.get("parameters") or {})
    # Defense in depth: this path is also used by tests and legacy callers
    # that do not pass through production-config validation.  Fail before
    # adapter construction/probing so a forbidden value cannot reach a
    # provider, revision hash, error object or lineage file.
    assert_credential_free_request_parameters(
        request_parameters,
        context="Bronze request.parameters",
    )
    # Adapter/configuration construction is deployment authority, not a
    # provider observation.  Missing dependencies, credentials or malformed
    # reviewed configuration therefore remain fail-closed even for a
    # non-blocking sample.
    adapter = source_adapter_from_mapping(payload, env=env)
    request = FetchRequest(
        dataset=str(request_payload.get("dataset") or payload["contract"]["dataset"]),
        parameters=request_parameters,
        fields=tuple(map(str, request_payload.get("fields") or ())),
    )

    # Calling ``probe`` may itself expose a rate-limiter, configuration or
    # programming defect.  Such raised exceptions are infrastructure failures
    # and must remain fail-closed.  Only the adapter's explicit, sanitized
    # non-healthy result is an optional-source observation failure.
    probe = adapter.probe()
    if probe.health is not SourceHealth.HEALTHY:
        if classify_observation_failures:
            raise BronzeObservationError(
                f"probe_{probe.health.value}"
            ) from None
        raise RuntimeError(
            f"source {adapter.source_id} probe failed closed: {probe.message}"
        )

    if classify_observation_failures:
        try:
            batch = adapter.fetch(request)
        except SourceObservationError as exc:
            # The narrow adapter type proves this arose at the reviewed
            # provider observation/response boundary.  Drop the original
            # traceback because SDK exceptions can retain request objects or
            # credential material in their cause chain.
            raise BronzeObservationError(exc.failure_kind) from None
    else:
        batch = adapter.fetch(request)

    # These guards intentionally sit outside the optional observation catch.
    # A mutated request or malformed clock/batch is a local correctness defect,
    # not evidence that a secondary provider is merely unavailable.
    assert_credential_free_request_parameters(
        request.parameters,
        context="Bronze request.parameters",
    )
    timestamp = now or batch.ingested_at
    if timestamp.tzinfo is None:
        raise ValueError("sync timestamp must be timezone-aware")
    partition = (
        Path(lake_root)
        / "bronze"
        / batch.source_id
        / batch.dataset
        / f"ingest_date={timestamp.astimezone(timezone.utc).date().isoformat()}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    target = partition / f"{batch.vendor_revision}.parquet"
    with tempfile.NamedTemporaryFile(
        prefix=f".{target.stem}.", suffix=".parquet", dir=partition, delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        batch.frame.to_parquet(temporary, index=False)
        if target.exists():
            if sha256_path(target) != sha256_path(temporary):
                raise FileExistsError(
                    f"Bronze vendor revision already has different bytes: {target}"
                )
        else:
            try:
                os.link(temporary, target)
            except FileExistsError:
                if sha256_path(target) != sha256_path(temporary):
                    raise FileExistsError(
                        f"concurrent Bronze publication differed: {target}"
                    )
    finally:
        if temporary.exists():
            temporary.unlink()
    digest = sha256_path(target)
    # Keep this immediately adjacent to persistence as an independent guard
    # against a future adapter or request-wrapper mutation.
    assert_credential_free_request_parameters(
        request.parameters,
        context="Bronze request.parameters",
    )
    metadata = {
        "schema_version": "research-os/bronze-lineage/v1",
        "source_id": batch.source_id,
        "source_priority": batch.source_priority,
        "dataset": batch.dataset,
        "rows": len(batch.frame),
        "columns": list(map(str, batch.frame.columns)),
        "ingested_at": batch.ingested_at.isoformat(),
        "vendor_revision": batch.vendor_revision,
        "request": {
            "dataset": request.dataset,
            "parameters": dict(request.parameters),
            "fields": list(request.fields),
        },
        "contract": dict(payload["contract"]),
        "lineage": dict(batch.lineage),
        "data_sha256": digest,
        "probe": {
            "health": probe.health.value,
            "latency_ms": probe.latency_ms,
            "message": probe.message,
            "checked_at": probe.checked_at.isoformat(),
        },
    }
    metadata_path = target.with_suffix(".metadata.json")
    stable_fields = (
        "schema_version",
        "source_id",
        "source_priority",
        "dataset",
        "rows",
        "columns",
        "vendor_revision",
        "request",
        "contract",
        "lineage",
        "data_sha256",
    )
    if metadata_path.exists():
        existing = json.loads(metadata_path.read_text(encoding="utf-8"))
        mismatched = [name for name in stable_fields if existing.get(name) != metadata.get(name)]
        if mismatched:
            raise FileExistsError(
                f"Bronze lineage is immutable and differs in {mismatched}: {metadata_path}"
            )
        metadata = existing
        encoded_bytes = metadata_path.read_bytes()
    else:
        encoded_bytes = json.dumps(
            metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        try:
            descriptor = os.open(
                metadata_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444
            )
        except FileExistsError:
            existing = json.loads(metadata_path.read_text(encoding="utf-8"))
            mismatched = [
                name for name in stable_fields if existing.get(name) != metadata.get(name)
            ]
            if mismatched:
                raise FileExistsError(
                    f"concurrent Bronze lineage differed in {mismatched}: {metadata_path}"
                )
            metadata = existing
            encoded_bytes = metadata_path.read_bytes()
        else:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(encoded_bytes)
                handle.flush()
                os.fsync(handle.fileno())

    data_object_uri: str | None = None
    metadata_object_uri: str | None = None
    if object_store_archive is not None:
        logical = (
            Path("bronze")
            / batch.source_id
            / batch.dataset
            / f"ingest_date={timestamp.astimezone(timezone.utc).date().isoformat()}"
        ).as_posix()
        data_object_uri = object_store_archive.archive_file(
            target, logical_path=logical
        ).uri
        metadata_object_uri = object_store_archive.archive_file(
            metadata_path, logical_path=logical
        ).uri
    return BronzeSyncResult(
        source_id=batch.source_id,
        dataset=batch.dataset,
        rows=len(batch.frame),
        data_path=str(target.resolve()),
        metadata_path=str(metadata_path.resolve()),
        sha256=digest,
        vendor_revision=batch.vendor_revision,
        ingested_at=str(metadata["ingested_at"]),
        probe_latency_ms=float(metadata["probe"]["latency_ms"]),
        data_object_uri=data_object_uri,
        metadata_object_uri=metadata_object_uri,
    )


def read_frame(path: str | Path) -> pd.DataFrame:
    source = Path(path).resolve()
    suffix = source.suffix.lower()
    if suffix in {".parquet", ".pq"}:
        frame = pd.read_parquet(source)
    elif suffix == ".csv":
        frame = pd.read_csv(source)
    elif suffix in {".jsonl", ".ndjson"}:
        frame = pd.read_json(source, lines=True)
    else:
        raise ValueError(f"unsupported research data format: {suffix}")
    # HistoricalResearchCycle verifies these hints against the immutable Gold
    # manifest and re-reads the source before accepting the frame.  The hints
    # are deliberately not treated as proof on their own.
    frame.attrs["research_os_source_path"] = str(source)
    frame.attrs["research_os_source_sha256"] = sha256_path(source)
    return frame


__all__ = [
    "BronzeObservationError",
    "BronzeSyncResult",
    "configured_source_profiles",
    "dataset_contract_from_mapping",
    "read_frame",
    "resolve_credential",
    "resolve_source_profile",
    "source_adapter_from_mapping",
    "sync_bronze",
]
