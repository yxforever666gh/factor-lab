"""Production bootstrap validation for the local Research OS deployment.

The checks here intentionally run before Dagster or Alembic.  A source-only
image is not considered production-ready unless it uses the immutable
production configuration, fixed container mount targets and secret files.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
from typing import Any, Callable, Mapping

from .build_provenance import capture_epoch_provenance
from .credentials import CredentialResolutionError, resolve_credential_ref
from .data_sources import (
    SourceContractError,
    TUSHARE_ACCOUNT_RATE_LIMIT_KEY,
    TUSHARE_PRODUCTION_ACCOUNT_RATE_LIMIT,
    is_credential_shaped_key,
    validate_tushare_https_origin,
    validate_production_diemeng_base_url,
)
from .execution_open_sources import (
    engineering_canary_execution_contract_hash,
    validate_diemeng_engineering_canary_execution_mapping,
)


PRODUCTION_CONFIG_NAME = "research_os_orchestration.production.json"
PRODUCTION_REPOSITORY = PurePosixPath("/opt/factor-lab")
PRODUCTION_RUNTIME_ROOT = PurePosixPath("/opt/factor-lab/runtime")
PRODUCTION_DATA_ROOT = PRODUCTION_RUNTIME_ROOT / "data"
PRODUCTION_ARTIFACT_ROOT = PRODUCTION_RUNTIME_ROOT / "artifacts"
PRODUCTION_SECRETS_EDITOR_ROOT = PRODUCTION_RUNTIME_ROOT / "secrets-editor"
RUNTIME_DATA_ROOT_ENV = "FACTOR_LAB_RUNTIME_DATA_ROOT"
RUNTIME_ARTIFACT_ROOT_ENV = "FACTOR_LAB_RUNTIME_ARTIFACT_ROOT"
ORCHESTRATION_CONFIG_ENV = "FACTOR_LAB_ORCHESTRATION_CONFIG"

_WINDOWS_PATH = re.compile(r"^[A-Za-z]:[\\/]")
_PLACEHOLDER = re.compile(
    r"(?:change[-_ ]?me|replace[-_ ]?me|placeholder|"
    r"exp_catalog_authoritative_|shadow_forward_)",
    re.IGNORECASE,
)
_RAW_SECRET_ENV = {
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "DIEMENG_API_KEY",
    "FACTOR_LAB_LLM_API_KEY",
    "FACTOR_LAB_OBJECT_STORE_ACCESS_KEY",
    "FACTOR_LAB_OBJECT_STORE_SECRET_KEY",
    "MINIO_ROOT_PASSWORD",
    "PGPASSWORD",
    "RESEARCH_OS_MINIO_ROOT_PASSWORD",
    "RESEARCH_OS_POSTGRES_PASSWORD",
    "TUSHARE_TOKEN",
}
_FORBIDDEN_PRODUCTION_PATH_KEYS = {
    "input_path",
    "market_bars_path",
    "sleeve_returns_path",
    "state_history_path",
    "trading_partitions_path",
}
_CANONICAL_SOURCE_PROFILES = {
    "akshare": "secondary-akshare",
    "diemeng": "primary-diemeng",
    "tushare": "primary-tushare",
}


def _credential_looking_profile_extra(
    value: Mapping[str, Any], *, prefix: str = "extra"
) -> tuple[str, ...]:
    """Find plaintext credential-shaped fields in a source profile extra map."""

    found: list[str] = []
    for raw_key, item in value.items():
        key = str(raw_key)
        path = f"{prefix}.{key}"
        if isinstance(item, Mapping):
            found.extend(_credential_looking_profile_extra(item, prefix=path))
            continue
        if not str(item or "").strip():
            continue
        if key == "credential_ref":
            if not str(item).strip().startswith("secret://"):
                found.append(path)
            continue
        if is_credential_shaped_key(key):
            found.append(path)
    return tuple(found)


_PUBLIC_CREDENTIAL_ROTATION_SUBJECTS = frozenset(
    {
        "$.security.credential_rotation.tushare_token",
        "$.security.credential_rotation.diemeng_api_key",
    }
)
_PUBLIC_CREDENTIAL_ROTATION_FIELDS = frozenset(
    {
        "accepted_at",
        "credential_ref",
        "reason",
        "required_before",
        "status",
        "vendor_confirmation",
    }
)
_PUBLIC_CREDENTIAL_ROTATION_OPERATIONS = frozenset(
    {
        "authoritative_calendar_bootstrap",
        "historical_backfill",
        "formal_forward_epoch_activation",
    }
)
_OPERATOR_RETENTION_STATUS = "retained_unrotated_operator_accepted"
_OPERATOR_RETENTION_CONFIRMATION = "not_rotated"
_OPERATOR_RETENTION_REASON = (
    "operator_declined_rotation_for_local_research_only_runtime"
)
_OPERATOR_RETENTION_REFS = {
    "tushare_token": "secret://tushare_token",
    "diemeng_api_key": "secret://diemeng_api_key",
}
_OPERATOR_RETENTION_FIELDS = frozenset(
    {
        "accepted_at",
        "credential_ref",
        "reason",
        "status",
        "vendor_confirmation",
    }
)
_TUSHARE_REVIEWED_HTTPS_ORIGIN = "https://api.tushare.pro/dataapi"
_DIEMENG_REVIEWED_HTTPS_ORIGIN = "https://data.diemeng.chat/api"
_TUSHARE_ACCOUNT_RATE_LIMIT_FIELDS = frozenset(
    {"requests", "per_seconds", "burst"}
)


def _is_public_credential_rotation_subject(item_path: str, value: Any) -> bool:
    """Allow only the two reviewed vendor rotation status objects.

    Their credential-shaped *names* identify governance records, not material.
    A string or an unreviewed alias at the same location remains forbidden and
    descendants are still traversed normally.
    """

    if item_path not in _PUBLIC_CREDENTIAL_ROTATION_SUBJECTS or not isinstance(
        value, Mapping
    ):
        return False
    fields = set(map(str, value))
    if not fields.issubset(_PUBLIC_CREDENTIAL_ROTATION_FIELDS):
        return False
    status = value.get("status")
    confirmation = value.get("vendor_confirmation")
    if status == _OPERATOR_RETENTION_STATUS:
        return fields == _OPERATOR_RETENTION_FIELDS and confirmation in {
            "recorded",
            _OPERATOR_RETENTION_CONFIRMATION,
        }
    if status not in {
        "pending_vendor_rotation",
        "verified_post_exposure",
    } or confirmation not in {"pending", "recorded"}:
        return False
    if fields.difference({"status", "vendor_confirmation", "required_before"}):
        return False
    if "required_before" not in value:
        return True
    required_before = value.get("required_before")
    return isinstance(required_before, list) and set(
        map(str, required_before)
    ).issubset(_PUBLIC_CREDENTIAL_ROTATION_OPERATIONS)


def _operator_retention_waiver(
    value: Any,
    *,
    credential: str,
    credential_refs: set[str],
    https_transport_verified: bool,
) -> bool:
    """Validate an explicit decision to retain, not rotate, one credential.

    The waiver is deliberately a small closed schema.  It cannot claim vendor
    rotation, cannot carry arbitrary operator prose or secret material, and is
    useful only while the exact reviewed ``secret://`` binding and HTTPS
    transport remain active.  The acceptance timestamp is public provenance,
    not a credential or a replacement for vendor evidence.
    """

    if not isinstance(value, Mapping) or value.get("status") != (
        _OPERATOR_RETENTION_STATUS
    ):
        return False
    expected_ref = _OPERATOR_RETENTION_REFS[credential]
    if set(map(str, value)) != _OPERATOR_RETENTION_FIELDS:
        raise ProductionConfigurationError(
            f"{credential} operator retention waiver has an invalid field set"
        )
    if value.get("vendor_confirmation") != _OPERATOR_RETENTION_CONFIRMATION:
        raise ProductionConfigurationError(
            f"{credential} retention waiver must state that it was not rotated"
        )
    if (
        value.get("credential_ref") != expected_ref
        or expected_ref not in credential_refs
    ):
        raise ProductionConfigurationError(
            f"{credential} retention waiver must bind the exact reviewed secret reference"
        )
    if value.get("reason") != _OPERATOR_RETENTION_REASON:
        raise ProductionConfigurationError(
            f"{credential} retention waiver reason is not the reviewed local-only reason"
        )
    raw_accepted_at = value.get("accepted_at")
    try:
        accepted_at = datetime.fromisoformat(str(raw_accepted_at))
    except ValueError:
        accepted_at = None
    if (
        accepted_at is None
        or accepted_at.tzinfo is None
        or accepted_at.utcoffset() is None
        or accepted_at.astimezone(timezone.utc) > datetime.now(timezone.utc)
    ):
        raise ProductionConfigurationError(
            f"{credential} retention waiver accepted_at must be an aware, non-future time"
        )
    return bool(https_transport_verified)


class ProductionConfigurationError(RuntimeError):
    """Raised when a production process would start with an unsafe contract."""


def _validated_tushare_account_rate_limit(
    source: Mapping[str, Any], *, path: str
) -> tuple[int, float, int]:
    """Validate one closed, public account-level Tushare rate contract."""

    rate_limits = source.get("rate_limits")
    account = (
        rate_limits.get(TUSHARE_ACCOUNT_RATE_LIMIT_KEY)
        if isinstance(rate_limits, Mapping)
        else None
    )
    account_path = f"{path}.rate_limits.{TUSHARE_ACCOUNT_RATE_LIMIT_KEY}"
    if not isinstance(account, Mapping):
        raise ProductionConfigurationError(
            f"{account_path} is required for every production Tushare source"
        )
    if set(map(str, account)) != _TUSHARE_ACCOUNT_RATE_LIMIT_FIELDS:
        raise ProductionConfigurationError(
            f"{account_path} must contain exactly requests, per_seconds, burst"
        )
    requests_value = account.get("requests")
    per_seconds_value = account.get("per_seconds")
    burst_value = account.get("burst")
    if (
        isinstance(requests_value, bool)
        or not isinstance(requests_value, int)
        or requests_value <= 0
        or isinstance(per_seconds_value, bool)
        or not isinstance(per_seconds_value, (int, float))
        or not math.isfinite(float(per_seconds_value))
        or float(per_seconds_value) <= 0
        or isinstance(burst_value, bool)
        or not isinstance(burst_value, int)
        or burst_value <= 0
    ):
        raise ProductionConfigurationError(
            f"{account_path} values must be finite positive numbers with integer requests/burst"
        )
    return int(requests_value), float(per_seconds_value), int(burst_value)


class ProductionOperation(str, Enum):
    ENGINEERING_CANARY = "engineering_canary"
    CALENDAR_CAPABILITY_PROBE = "calendar_capability_probe"
    AUTHORITATIVE_HISTORICAL_BACKFILL = "authoritative_historical_backfill"
    FORMAL_FORWARD_ACTIVATION = "formal_forward_activation"


@dataclass(frozen=True)
class ProductionOperationAdmission:
    operation: ProductionOperation
    allowed: bool
    blockers: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProductionConfigEvidence:
    path: Path
    runtime_data_root: Path
    runtime_artifact_root: Path
    credential_refs: tuple[str, ...]
    provenance: Any
    formal_execution_capable: bool
    historical_backfill_allowed: bool
    formal_forward_evidence: bool
    readiness_blockers: tuple[str, ...]
    credential_rotation_blockers: tuple[str, ...]
    source_transport_blockers: tuple[str, ...] = ()
    credential_retention_waivers: tuple[str, ...] = ()
    engineering_canary_execution_contract_hash: str = ""

    @property
    def status(self) -> str:
        # A validated JSON document is necessary but never sufficient proof
        # that a production canary, restore drill, Gold matrix, or soak has
        # actually completed.  Only a later PostgreSQL-backed readiness audit
        # may issue a formal-epoch-ready verdict.
        return "config_valid_canary_pending"


def admit_production_operation(
    evidence: ProductionConfigEvidence,
    operation: ProductionOperation | str,
) -> ProductionOperationAdmission:
    """Return a side-effect-free fail-closed production operation decision."""

    selected = ProductionOperation(operation)
    blockers: list[str] = []
    rotation_blockers = tuple(
        getattr(evidence, "credential_rotation_blockers", ()) or ()
    )
    transport_blockers = tuple(
        getattr(evidence, "source_transport_blockers", ()) or ()
    )
    if (
        not rotation_blockers
        and not transport_blockers
        and not evidence.historical_backfill_allowed
    ):
        # Compatibility for callers/tests constructing the earlier evidence
        # shape. Validated production evidence always carries the exact vendor
        # blocker set.
        rotation_blockers = ("tushare_token_post_exposure_rotation_pending",)
    if selected in {
        ProductionOperation.ENGINEERING_CANARY,
        ProductionOperation.CALENDAR_CAPABILITY_PROBE,
        ProductionOperation.AUTHORITATIVE_HISTORICAL_BACKFILL,
    }:
        blockers.extend(rotation_blockers)
        blockers.extend(transport_blockers)
    elif selected is ProductionOperation.FORMAL_FORWARD_ACTIVATION:
        if not bool(getattr(evidence.provenance, "formal_epoch_eligible", False)):
            blockers.append("daemon_inspected_oci_provenance_missing")
        if not evidence.formal_execution_capable:
            blockers.append("formal_execution_adapter_insufficient")
        blockers.extend(rotation_blockers)
        blockers.extend(transport_blockers)
        if not bool(getattr(evidence, "formal_forward_evidence", False)):
            persisted_blockers = tuple(
                getattr(evidence, "readiness_blockers", ()) or ()
            )
            blockers.extend(persisted_blockers)
            if not persisted_blockers:
                blockers.append("persisted_production_readiness_audit_missing")
    return ProductionOperationAdmission(
        operation=selected,
        allowed=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
    )


def _walk(value: Any, *, path: str = "$"):
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            yield child_path, str(key), child
            yield from _walk(child, path=child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            child_path = f"{path}[{index}]"
            yield child_path, str(index), child
            yield from _walk(child, path=child_path)


def _is_mount(path: Path) -> bool:
    if os.path.ismount(path):
        return True
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.is_file():
        return False
    resolved = str(path.resolve()).replace(" ", "\\040")
    try:
        return any(
            len(parts := line.split()) > 4 and parts[4] == resolved
            for line in mountinfo.read_text(encoding="utf-8").splitlines()
        )
    except OSError:
        return False


def _safe_container_path(value: str, *, field: str) -> PurePosixPath:
    if _WINDOWS_PATH.match(value) or "\\" in value:
        raise ProductionConfigurationError(
            f"{field} contains a Windows host path; production config must use container paths"
        )
    path = PurePosixPath(value)
    if any(part == ".." for part in path.parts):
        raise ProductionConfigurationError(f"{field} contains parent traversal")
    return path


def load_production_config(path: str | Path) -> Mapping[str, Any]:
    config_path = Path(path)
    if config_path.name != PRODUCTION_CONFIG_NAME or "example" in config_path.name.lower():
        raise ProductionConfigurationError(
            f"production requires {PRODUCTION_CONFIG_NAME}, never an example config"
        )
    if config_path.is_symlink() or not config_path.is_file():
        raise ProductionConfigurationError(
            f"production orchestration config is missing or is a symlink: {config_path}"
        )
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProductionConfigurationError("production config is not valid UTF-8 JSON") from exc
    if not isinstance(payload, Mapping):
        raise ProductionConfigurationError("production config must be a JSON object")
    return payload


def validate_production_environment(
    env: Mapping[str, str],
    *,
    config_path: Path,
    require_mounts: bool = True,
    mount_checker: Callable[[Path], bool] | None = None,
) -> tuple[Path, Path]:
    if str(env.get("FACTOR_LAB_ENVIRONMENT") or "").strip().lower() != "production":
        raise ProductionConfigurationError(
            "FACTOR_LAB_ENVIRONMENT must be production"
        )
    raw = sorted(name for name in _RAW_SECRET_ENV if str(env.get(name) or ""))
    if raw:
        raise ProductionConfigurationError(
            "raw credentials are forbidden in production environment: " + ", ".join(raw)
        )
    selected = str(env.get(ORCHESTRATION_CONFIG_ENV) or "").strip()
    if selected and Path(selected).resolve() != config_path.resolve():
        raise ProductionConfigurationError(
            f"{ORCHESTRATION_CONFIG_ENV} does not select the validated production config"
        )
    data_root = Path(str(env.get(RUNTIME_DATA_ROOT_ENV) or ""))
    artifact_root = Path(str(env.get(RUNTIME_ARTIFACT_ROOT_ENV) or ""))
    if data_root.as_posix() != str(PRODUCTION_DATA_ROOT):
        raise ProductionConfigurationError(
            f"{RUNTIME_DATA_ROOT_ENV} must be {PRODUCTION_DATA_ROOT}"
        )
    if artifact_root.as_posix() != str(PRODUCTION_ARTIFACT_ROOT):
        raise ProductionConfigurationError(
            f"{RUNTIME_ARTIFACT_ROOT_ENV} must be {PRODUCTION_ARTIFACT_ROOT}"
        )
    if require_mounts:
        checker = mount_checker or _is_mount
        for label, root in (("data", data_root), ("artifacts", artifact_root)):
            if not root.is_dir() or not checker(root):
                raise ProductionConfigurationError(
                    f"production {label} root is not a mounted directory: {root}"
                )
    return data_root, artifact_root


def validate_production_config(
    path: str | Path,
    *,
    env: Mapping[str, str] | None = None,
    require_mounts: bool = True,
    mount_checker: Callable[[Path], bool] | None = None,
    image_reference: str | None = None,
) -> ProductionConfigEvidence:
    """Validate configuration, mounts, credentials and immutable source proof."""

    values = os.environ if env is None else env
    config_path = Path(path).resolve()
    payload = load_production_config(config_path)
    data_root, artifact_root = validate_production_environment(
        values,
        config_path=config_path,
        require_mounts=require_mounts,
        mount_checker=mount_checker,
    )
    repository = _safe_container_path(
        str(payload.get("repository") or ""), field="$.repository"
    )
    path_base = _safe_container_path(
        str(payload.get("path_base") or ""), field="$.path_base"
    )
    if repository != PRODUCTION_REPOSITORY:
        raise ProductionConfigurationError(
            f"production repository must be {PRODUCTION_REPOSITORY}"
        )
    if path_base != PRODUCTION_RUNTIME_ROOT:
        raise ProductionConfigurationError(
            f"production path_base must be {PRODUCTION_RUNTIME_ROOT}"
        )

    credential_refs: set[str] = set()
    for item_path, key, value in _walk(payload):
        raw_key = str(key)
        lowered = key.lower()
        if raw_key == "credential_ref":
            if not isinstance(value, str) or not value.startswith("secret://"):
                raise ProductionConfigurationError(
                    f"{item_path} must use secret:// in production"
                )
            credential_refs.add(value)
        elif is_credential_shaped_key(raw_key) and not (
            _is_public_credential_rotation_subject(item_path, value)
        ):
            # Reject the key regardless of whether its current value is empty;
            # otherwise a later templating/substitution step could populate a
            # credential slot that escaped review.  Never reflect the value.
            raise ProductionConfigurationError(
                "production config embeds a raw credential field; "
                "use credential_ref=secret://NAME"
            )
        if isinstance(value, str):
            _safe_container_path(value, field=item_path)
            if _PLACEHOLDER.search(value):
                raise ProductionConfigurationError(
                    f"{item_path} contains a forbidden placeholder"
                )
        if lowered in {"token_env", "password_env", "api_key_env"}:
            raise ProductionConfigurationError(
                f"{item_path} is forbidden; use credential_ref=secret://NAME"
            )
        if lowered in _FORBIDDEN_PRODUCTION_PATH_KEYS:
            raise ProductionConfigurationError(
                f"{item_path} is a legacy file-driven production input"
            )

    daily = payload.get("daily")
    if not isinstance(daily, Mapping):
        raise ProductionConfigurationError("$.daily must be configured")
    sources = daily.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ProductionConfigurationError("$.daily.sources must be non-empty")
    if any(
        isinstance(source, Mapping) and source.get("source") == "local_file"
        for source in sources
    ):
        raise ProductionConfigurationError(
            "production data evidence cannot depend on local_file sources"
        )
    gold = daily.get("gold")
    research_panel = gold.get("research_panel") if isinstance(gold, Mapping) else None
    required_gold_datasets = set(
        map(
            str,
            (
                research_panel.get("required_datasets") or ()
                if isinstance(research_panel, Mapping)
                else ()
            ),
        )
    )
    reviewed_diemeng_urls: dict[str, str] = {}
    tushare_account_rate_limit: tuple[int, float, int] | None = None
    for index, source in enumerate(sources):
        if not isinstance(source, Mapping):
            raise ProductionConfigurationError(f"$.daily.sources[{index}] must be an object")
        request = source.get("request")
        dataset = request.get("dataset") if isinstance(request, Mapping) else None
        raw_non_blocking = source.get("non_blocking")
        if "non_blocking" in source and type(raw_non_blocking) is not bool:
            raise ProductionConfigurationError(
                f"$.daily.sources[{index}].non_blocking must be a JSON boolean"
            )
        raw_evidence_role = source.get("evidence_role")
        if (
            "evidence_role" in source
            and raw_evidence_role != "non_blocking_sample"
        ):
            raise ProductionConfigurationError(
                f"$.daily.sources[{index}].evidence_role is unsupported"
            )
        declared_non_blocking = raw_non_blocking is True
        non_blocking_role = raw_evidence_role == "non_blocking_sample"
        if declared_non_blocking != non_blocking_role:
            raise ProductionConfigurationError(
                f"$.daily.sources[{index}] must declare both "
                "non_blocking=true and evidence_role=non_blocking_sample"
            )
        if declared_non_blocking and str(dataset or "") in required_gold_datasets:
            raise ProductionConfigurationError(
                f"required Gold dataset {dataset!r} cannot be non-blocking"
            )
        if declared_non_blocking:
            coverage_scope = source.get("coverage_scope")
            if not isinstance(coverage_scope, Mapping) or (
                coverage_scope.get("eligible_for_reconciliation") is not False
            ):
                raise ProductionConfigurationError(
                    f"$.daily.sources[{index}] non-blocking sample must declare "
                    "coverage_scope.eligible_for_reconciliation=false"
                )
        cadence = source.get("partition_cadence")
        if not isinstance(cadence, Mapping):
            raise ProductionConfigurationError(
                f"$.daily.sources[{index}].partition_cadence is required"
            )
        cadence_kind = str(cadence.get("kind") or "")
        if cadence_kind not in {"trading_session", "static_snapshot", "event_date"}:
            raise ProductionConfigurationError(
                f"$.daily.sources[{index}].partition_cadence.kind is unsupported"
            )
        if str(dataset or "").startswith("stock_basic_"):
            if (
                cadence_kind != "static_snapshot"
                or not str(cadence.get("ledger_identity") or "").strip()
                or cadence.get("refresh_policy") != "bootstrap_or_explicit"
            ):
                raise ProductionConfigurationError(
                    f"{dataset} must be a ledger-bound bootstrap/explicit static snapshot"
                )
        elif cadence_kind == "static_snapshot":
            raise ProductionConfigurationError(
                f"{dataset} is not an approved static snapshot dataset"
            )
        if cadence_kind == "event_date" and not str(
            cadence.get("partition_parameter") or ""
        ).strip():
            raise ProductionConfigurationError(
                f"{dataset} event_date cadence requires partition_parameter"
            )
        source_type = str(source.get("source") or "").strip().lower()
        expected_profile = _CANONICAL_SOURCE_PROFILES.get(source_type)
        if expected_profile and source.get("profile_name") != expected_profile:
            raise ProductionConfigurationError(
                f"$.daily.sources[{index}].profile_name must be {expected_profile}"
            )
        if source_type == "tushare":
            account_limit = _validated_tushare_account_rate_limit(
                source, path=f"$.daily.sources[{index}]"
            )
            if tushare_account_rate_limit is None:
                tushare_account_rate_limit = account_limit
            elif account_limit != tushare_account_rate_limit:
                raise ProductionConfigurationError(
                    "all production Tushare sources must use the same "
                    f"{TUSHARE_ACCOUNT_RATE_LIMIT_KEY} rate limit"
                )
        if source_type == "diemeng":
            try:
                reviewed_url = validate_production_diemeng_base_url(
                    str(source.get("base_url") or "")
                )
            except ValueError as exc:
                raise ProductionConfigurationError(
                    f"$.daily.sources[{index}].base_url: {exc}"
                ) from exc
            profile_name = str(source.get("profile_name") or "")
            prior = reviewed_diemeng_urls.setdefault(profile_name, reviewed_url)
            if prior != reviewed_url:
                raise ProductionConfigurationError(
                    f"Diemeng profile {profile_name!r} has inconsistent reviewed base_url values"
                )
        if source.get("source") == "akshare" and (
            dataset == "daily" or not declared_non_blocking
        ):
            raise ProductionConfigurationError(
                "AkShare single-symbol data must be a non-blocking sample dataset"
            )
    reviewed_tushare_account_limit = (
        int(TUSHARE_PRODUCTION_ACCOUNT_RATE_LIMIT.requests),
        float(TUSHARE_PRODUCTION_ACCOUNT_RATE_LIMIT.per_seconds),
        int(TUSHARE_PRODUCTION_ACCOUNT_RATE_LIMIT.burst),
    )
    if (
        tushare_account_rate_limit is not None
        and tushare_account_rate_limit != reviewed_tushare_account_limit
    ):
        raise ProductionConfigurationError(
            "all production Tushare sources must use the reviewed account rate "
            "limit: 60 requests per 60 seconds with burst 1"
        )
    bootstrap = daily.get("bootstrap")
    if not isinstance(bootstrap, Mapping) or (
        bootstrap.get("source_start") != "2016-06-01"
        or int(bootstrap.get("minimum_prewarm_trading_sessions") or 0) < 120
        or not bool(bootstrap.get("resume_from_partition_ledger"))
    ):
        raise ProductionConfigurationError(
            "production requires resumable 2016-06-01 backfill and at least 120 prewarm sessions"
        )
    legacy_seed = bootstrap.get("legacy_bronze_seed")
    expected_seed_root = PRODUCTION_DATA_ROOT / "legacy" / "expanded_long_only"
    if not isinstance(legacy_seed, Mapping) or (
        legacy_seed.get("mode") != "hash_verified_checkpoint"
        or _safe_container_path(
            str(legacy_seed.get("root") or ""),
            field="$.daily.bootstrap.legacy_bronze_seed.root",
        )
        != expected_seed_root
        or legacy_seed.get("checkpoint") != "download_checkpoint.json"
        or tuple(map(str, legacy_seed.get("datasets") or ()))
        != ("daily", "daily_basic", "adj_factor")
        or legacy_seed.get("promotion_policy") != "bronze_only_fail_closed"
    ):
        raise ProductionConfigurationError(
            "production legacy seed must use the fixed runtime-data checkpoint and remain Bronze-only"
        )
    sensors = payload.get("sensors")
    if not isinstance(sensors, Mapping) or sensors.get("partition_source") != (
        "postgresql_accepted_gold_calendar"
    ):
        raise ProductionConfigurationError(
            "production partitions must come from the PostgreSQL accepted Gold calendar"
        )

    shadow = daily.get("shadow")
    execution_market_data = (
        shadow.get("execution_market_data") if isinstance(shadow, Mapping) else None
    )
    if isinstance(execution_market_data, Mapping):
        source_type = str(execution_market_data.get("source") or "").strip().lower()
        expected_profile = _CANONICAL_SOURCE_PROFILES.get(source_type)
        if expected_profile and execution_market_data.get("profile_name") != expected_profile:
            raise ProductionConfigurationError(
                f"$.daily.shadow.execution_market_data.profile_name must be {expected_profile}"
            )
        if source_type == "diemeng":
            try:
                reviewed_url = validate_production_diemeng_base_url(
                    str(execution_market_data.get("base_url") or "")
                )
            except ValueError as exc:
                raise ProductionConfigurationError(
                    f"$.daily.shadow.execution_market_data.base_url: {exc}"
                ) from exc
            profile_name = str(execution_market_data.get("profile_name") or "")
            prior = reviewed_diemeng_urls.setdefault(profile_name, reviewed_url)
            if prior != reviewed_url:
                raise ProductionConfigurationError(
                    f"Diemeng profile {profile_name!r} has inconsistent reviewed base_url values"
                )
        execution_dataset = str(
            execution_market_data.get("dataset") or ""
        ).strip()
        execution_pair = (source_type, execution_dataset)
        allowed_execution_pairs = {
            ("diemeng", "minute_history"),
            ("tushare", "rt_min"),
        }
        if execution_pair not in allowed_execution_pairs:
            raise ProductionConfigurationError(
                "shadow execution source/dataset is not a reviewed closed-set adapter"
            )
        if execution_pair == ("diemeng", "minute_history"):
            contract = execution_market_data.get("contract")
            observation = (
                contract.get("execution_observation")
                if isinstance(contract, Mapping)
                else None
            )
            availability = execution_market_data.get("availability")
            capability = execution_market_data.get("formal_capability")
            mark = execution_market_data.get("end_of_day_mark")
            if not isinstance(observation, Mapping) or (
                observation.get("required_local_time") != "09:30:00"
                or observation.get("event_time_source") != "trade_time"
                or observation.get("available_at_source") != "trade_time"
            ):
                raise ProductionConfigurationError(
                    "minute_history execution must bind the 09:30 trade_time observation"
                )
            if not isinstance(availability, Mapping) or (
                availability.get("mode") != "event_timestamp"
                or availability.get("event_time_field") != "trade_time"
                or availability.get("available_at_field") != "trade_time"
            ):
                raise ProductionConfigurationError(
                    "minute_history cannot use delayed EOD availability for open execution"
                )
            if not isinstance(capability, Mapping) or (
                capability.get("status") != "insufficient"
                or capability.get("formal_shadow_projection") != "blocked"
            ):
                raise ProductionConfigurationError(
                    "Diemeng minute history cannot self-approve formal opening-auction evidence"
                )
            if not isinstance(mark, Mapping) or mark.get("source") != (
                "accepted_gold_close_snapshot"
            ):
                raise ProductionConfigurationError(
                    "shadow EOD marks must come from an accepted Gold close snapshot"
                )
        elif execution_pair == ("tushare", "rt_min"):
            endpoint = str(execution_market_data.get("endpoint") or "")
            dataset = str(execution_market_data.get("dataset") or "")
            contract = execution_market_data.get("contract")
            availability = execution_market_data.get("availability")
            capability = execution_market_data.get("formal_capability")
            mark = execution_market_data.get("end_of_day_mark")
            batching = execution_market_data.get("batching")
            execution_rate_limits = execution_market_data.get("rate_limits")
            if not isinstance(execution_rate_limits, Mapping) or set(
                map(str, execution_rate_limits)
            ) != {TUSHARE_ACCOUNT_RATE_LIMIT_KEY}:
                raise ProductionConfigurationError(
                    "Tushare realtime execution rate_limits must contain only "
                    f"{TUSHARE_ACCOUNT_RATE_LIMIT_KEY}"
                )
            execution_account_rate_limit = _validated_tushare_account_rate_limit(
                execution_market_data,
                path="$.daily.shadow.execution_market_data",
            )
            if (
                tushare_account_rate_limit is None
                or execution_account_rate_limit != tushare_account_rate_limit
            ):
                raise ProductionConfigurationError(
                    "Tushare realtime execution must use the same "
                    f"{TUSHARE_ACCOUNT_RATE_LIMIT_KEY} rate limit as every daily source"
                )
            expected_fields = [
                "ts_code",
                "time",
                "open",
                "close",
                "high",
                "low",
                "vol",
                "amount",
            ]
            if not (
                source_type == "tushare"
                and endpoint == dataset
                and execution_market_data.get("method") == "SDK"
                and execution_market_data.get("credential_ref")
                == "secret://tushare_token"
            ):
                raise ProductionConfigurationError(
                    "realtime execution must bind the selected official Tushare SDK endpoint"
                )
            expected_batching = {
                "mode": "sorted_deterministic_chunks",
                "maximum_symbols_per_request": 300,
            }
            if not isinstance(batching, Mapping) or batching != expected_batching:
                raise ProductionConfigurationError(
                    "Tushare realtime request batching exceeds the official capacity"
                )
            if not isinstance(contract, Mapping) or (
                list(map(str, contract.get("key_fields") or ()))
                != ["ts_code", "time"]
                or list(map(str, contract.get("fields") or ())) != expected_fields
                or contract.get("event_time_field") != "time"
            ):
                raise ProductionConfigurationError(
                    "Tushare realtime execution contract does not match the official fields"
                )
            if not isinstance(availability, Mapping) or availability != {
                "mode": "collector_ingested_at",
                "event_time_field": "time",
                "available_at_field": "ingested_at",
                "maximum_delay_minutes": 5,
            }:
                raise ProductionConfigurationError(
                    "Tushare realtime availability must use collector ingested_at"
                )
            if not isinstance(capability, Mapping) or (
                capability.get("status") != "runtime_probe_required"
                or capability.get("formal_shadow_projection")
                != "runtime_probe_gated"
            ):
                raise ProductionConfigurationError(
                    "Tushare realtime execution must remain runtime-probe gated"
                )
            if not isinstance(mark, Mapping) or mark.get("source") != (
                "accepted_gold_close_snapshot"
            ):
                raise ProductionConfigurationError(
                    "shadow EOD marks must come from an accepted Gold close snapshot"
                )

    engineering_canary = daily.get("engineering_canary")
    canary_execution_market_data: Mapping[str, Any] | None = None
    canary_execution_contract_hash = ""
    if not isinstance(engineering_canary, Mapping):
        raise ProductionConfigurationError(
            "$.daily.engineering_canary must explicitly declare retrospective_non_forward evidence"
        )
    try:
        canary_execution_market_data = (
            validate_diemeng_engineering_canary_execution_mapping(
                engineering_canary.get("execution_market_data")
            )
        )
        canary_execution_contract_hash = engineering_canary_execution_contract_hash(
            engineering_canary
        )
    except ValueError as exc:
        raise ProductionConfigurationError(str(exc)) from None
    profile_name = str(canary_execution_market_data["profile_name"])
    reviewed_url = str(canary_execution_market_data["base_url"])
    prior = reviewed_diemeng_urls.setdefault(profile_name, reviewed_url)
    if prior != reviewed_url:
        raise ProductionConfigurationError(
            f"Diemeng profile {profile_name!r} has inconsistent reviewed base_url values"
        )

    formal_capability = (
        execution_market_data.get("formal_capability")
        if isinstance(execution_market_data, Mapping)
        else None
    )
    # Static JSON can establish only that one code-reviewed adapter is
    # structurally capable of a future runtime probe.  It can never self-report
    # an arbitrary accepted/allowed capability.  The actual formal admission
    # remains derived from the PostgreSQL capability ledger and typed runtime
    # evidence.
    formal_execution_capable = bool(
        isinstance(execution_market_data, Mapping)
        and str(execution_market_data.get("source") or "").lower() == "tushare"
        and execution_market_data.get("dataset") == "rt_min"
        and isinstance(formal_capability, Mapping)
        and formal_capability.get("status") == "runtime_probe_required"
        and formal_capability.get("formal_shadow_projection")
        == "runtime_probe_gated"
    )
    security = payload.get("security")
    credential_rotation = (
        security.get("credential_rotation") if isinstance(security, Mapping) else None
    )
    tushare_rotation = (
        credential_rotation.get("tushare_token")
        if isinstance(credential_rotation, Mapping)
        else None
    )
    tushare_rotation_verified = bool(
        isinstance(tushare_rotation, Mapping)
        and tushare_rotation.get("status") == "verified_post_exposure"
        and tushare_rotation.get("vendor_confirmation") == "recorded"
    )
    diemeng_rotation = (
        credential_rotation.get("diemeng_api_key")
        if isinstance(credential_rotation, Mapping)
        else None
    )
    diemeng_rotation_verified = bool(
        isinstance(diemeng_rotation, Mapping)
        and diemeng_rotation.get("status") == "verified_post_exposure"
        and diemeng_rotation.get("vendor_confirmation") == "recorded"
    )
    source_transport = (
        security.get("source_transport") if isinstance(security, Mapping) else None
    )
    tushare_transport = (
        source_transport.get("tushare")
        if isinstance(source_transport, Mapping)
        else None
    )
    configured_tushare_origin = str(
        tushare_transport.get("api_origin")
        if isinstance(tushare_transport, Mapping)
        else ""
    ).strip().rstrip("/")
    try:
        reviewed_tushare_origin = validate_tushare_https_origin(
            configured_tushare_origin
        )
    except (SourceContractError, ValueError):
        reviewed_tushare_origin = ""
    tushare_transport_verified = bool(
        isinstance(tushare_transport, Mapping)
        and tushare_transport.get("status") == "verified_vendor_https"
        and tushare_transport.get("vendor_confirmation") == "recorded"
        and configured_tushare_origin == _TUSHARE_REVIEWED_HTTPS_ORIGIN
        and reviewed_tushare_origin == _TUSHARE_REVIEWED_HTTPS_ORIGIN
    )
    diemeng_transport = (
        source_transport.get("diemeng")
        if isinstance(source_transport, Mapping)
        else None
    )
    configured_diemeng_origin = str(
        diemeng_transport.get("api_origin")
        if isinstance(diemeng_transport, Mapping)
        else ""
    ).strip().rstrip("/")
    try:
        reviewed_diemeng_origin = validate_production_diemeng_base_url(
            configured_diemeng_origin
        )
    except ValueError:
        reviewed_diemeng_origin = ""
    operational_credential_refs = {
        str(source.get("credential_ref") or "")
        for source in sources
        if isinstance(source, Mapping) and source.get("credential_ref")
    }
    if isinstance(execution_market_data, Mapping) and execution_market_data.get(
        "credential_ref"
    ):
        operational_credential_refs.add(
            str(execution_market_data.get("credential_ref"))
        )
    if canary_execution_market_data is not None:
        operational_credential_refs.add(
            str(canary_execution_market_data["credential_ref"])
        )
    reviewed_diemeng_origins = set(reviewed_diemeng_urls.values())
    diemeng_transport_verified = bool(
        isinstance(diemeng_transport, Mapping)
        and diemeng_transport.get("status") == "verified_vendor_https"
        and diemeng_transport.get("vendor_confirmation") == "recorded"
        and configured_diemeng_origin == _DIEMENG_REVIEWED_HTTPS_ORIGIN
        and reviewed_diemeng_origin == _DIEMENG_REVIEWED_HTTPS_ORIGIN
        and reviewed_diemeng_origins == {_DIEMENG_REVIEWED_HTTPS_ORIGIN}
        and _OPERATOR_RETENTION_REFS["diemeng_api_key"]
        in operational_credential_refs
    )
    tushare_retention_waived = _operator_retention_waiver(
        tushare_rotation,
        credential="tushare_token",
        credential_refs=operational_credential_refs,
        https_transport_verified=tushare_transport_verified,
    )
    diemeng_retention_waived = _operator_retention_waiver(
        diemeng_rotation,
        credential="diemeng_api_key",
        credential_refs=operational_credential_refs,
        https_transport_verified=diemeng_transport_verified,
    )

    # When the WebUI profile ledger is mounted into a worker it becomes an
    # exact binding, not a hint.  It may contain only secret references and
    # must provide every canonical profile named by this production config.
    raw_profiles = str(values.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or "").strip()
    if raw_profiles:
        try:
            profiles = json.loads(raw_profiles)
        except json.JSONDecodeError as exc:
            raise ProductionConfigurationError(
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON is not valid JSON"
            ) from exc
        if not isinstance(profiles, list):
            raise ProductionConfigurationError(
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON must contain a list"
            )
        by_name: dict[str, Mapping[str, Any]] = {}
        for index, profile in enumerate(profiles):
            if not isinstance(profile, Mapping):
                raise ProductionConfigurationError(
                    f"data source profile {index} must be an object"
                )
            name = str(profile.get("name") or "").strip()
            if not name or name in by_name:
                raise ProductionConfigurationError(
                    "data source profile names must be non-empty and unique"
                )
            if str(profile.get("api_key") or ""):
                raise ProductionConfigurationError(
                    f"data source profile {name!r} embeds a raw credential"
                )
            extra = profile.get("extra")
            if extra is not None and not isinstance(extra, Mapping):
                raise ProductionConfigurationError(
                    f"data source profile {name!r} extra must be an object"
                )
            unsafe_extra = _credential_looking_profile_extra(
                extra or {}, prefix=f"data source profile {name!r} extra"
            )
            if unsafe_extra:
                raise ProductionConfigurationError(
                    f"data source profile {name!r} extra embeds a raw credential: "
                    + ", ".join(unsafe_extra)
                )
            reference = str(
                profile.get("credential_ref")
                or (
                    profile.get("extra", {}).get("credential_ref")
                    if isinstance(profile.get("extra"), Mapping)
                    else ""
                )
                or ""
            ).strip()
            if reference:
                if not reference.startswith("secret://"):
                    raise ProductionConfigurationError(
                        f"data source profile {name!r} must use secret://"
                    )
                credential_refs.add(reference)
            by_name[name] = profile
        required_profiles = {
            str(source.get("profile_name"))
            for source in sources
            if source.get("profile_name")
        }
        if isinstance(execution_market_data, Mapping) and execution_market_data.get(
            "profile_name"
        ):
            required_profiles.add(str(execution_market_data["profile_name"]))
        if canary_execution_market_data is not None:
            required_profiles.add(
                str(canary_execution_market_data["profile_name"])
            )
        missing = sorted(
            name
            for name in required_profiles
            if name not in by_name or not bool(by_name[name].get("enabled", True))
        )
        if missing:
            raise ProductionConfigurationError(
                "production source profiles are absent or disabled: " + ", ".join(missing)
            )
        for source_type, expected_name in _CANONICAL_SOURCE_PROFILES.items():
            profile = by_name.get(expected_name)
            if profile is not None and str(profile.get("source_type") or "").lower() != source_type:
                raise ProductionConfigurationError(
                    f"data source profile {expected_name!r} must have source_type={source_type}"
                )
        for profile_name, reviewed_url in reviewed_diemeng_urls.items():
            profile = by_name.get(profile_name)
            if profile is None:
                continue
            extra = profile.get("extra")
            profile_url = str(
                extra.get("base_url")
                if isinstance(extra, Mapping) and extra.get("base_url") is not None
                else ""
            ).strip()
            if not profile_url:
                continue
            try:
                normalized_profile_url = validate_production_diemeng_base_url(
                    profile_url
                )
            except ValueError as exc:
                raise ProductionConfigurationError(
                    f"data source profile {profile_name!r} extra.base_url: {exc}"
                ) from exc
            if normalized_profile_url != reviewed_url:
                raise ProductionConfigurationError(
                    f"data source profile {profile_name!r} extra.base_url must match reviewed configuration"
                )

    llm_reference = str(values.get("FACTOR_LAB_LLM_API_KEY_REF") or "").strip()
    if llm_reference:
        if not llm_reference.startswith("secret://"):
            raise ProductionConfigurationError(
                "FACTOR_LAB_LLM_API_KEY_REF must use secret://"
            )
        credential_refs.add(llm_reference)
    raw_llm_profiles = str(values.get("FACTOR_LAB_LLM_PROFILES_JSON") or "").strip()
    if raw_llm_profiles:
        try:
            llm_profiles = json.loads(raw_llm_profiles)
        except json.JSONDecodeError as exc:
            raise ProductionConfigurationError(
                "FACTOR_LAB_LLM_PROFILES_JSON is not valid JSON"
            ) from exc
        if not isinstance(llm_profiles, list):
            raise ProductionConfigurationError(
                "FACTOR_LAB_LLM_PROFILES_JSON must contain a list"
            )
        for index, profile in enumerate(llm_profiles):
            if not isinstance(profile, Mapping):
                raise ProductionConfigurationError(
                    f"LLM profile {index} must be an object"
                )
            if str(profile.get("api_key") or ""):
                raise ProductionConfigurationError(
                    f"LLM profile {index} embeds a raw credential"
                )
            reference = str(profile.get("credential_ref") or "").strip()
            if reference:
                if not reference.startswith("secret://"):
                    raise ProductionConfigurationError(
                        f"LLM profile {index} must use secret://"
                    )
                credential_refs.add(reference)

    configured_secrets_dir = str(values.get("FACTOR_LAB_SECRETS_DIR") or "").strip()
    legacy_secrets_root = str(values.get("FACTOR_LAB_SECRETS_ROOT") or "").strip()
    role = str(values.get("FACTOR_LAB_PRODUCTION_ROLE") or "worker").strip().lower()
    expected_secrets_dir = (
        str(PRODUCTION_SECRETS_EDITOR_ROOT) if role == "webui" else "/run/secrets"
    )
    if role not in {"worker", "webui"}:
        raise ProductionConfigurationError(
            "FACTOR_LAB_PRODUCTION_ROLE must be worker or webui"
        )
    if configured_secrets_dir != expected_secrets_dir:
        raise ProductionConfigurationError(
            f"FACTOR_LAB_SECRETS_DIR must be {expected_secrets_dir} for {role}"
        )
    if legacy_secrets_root != "/run/secrets":
        raise ProductionConfigurationError(
            "FACTOR_LAB_SECRETS_ROOT must be /run/secrets"
        )
    if require_mounts:
        checker = mount_checker or _is_mount
        reader = Path("/run/secrets")
        if not reader.is_dir() or not checker(reader):
            raise ProductionConfigurationError(
                "production secrets reader is not a mounted directory: /run/secrets"
            )
    if role == "webui" and require_mounts:
        checker = mount_checker or _is_mount
        editor = Path(PRODUCTION_SECRETS_EDITOR_ROOT)
        if not editor.is_dir() or not checker(editor):
            raise ProductionConfigurationError(
                f"WebUI secrets editor is not a mounted directory: {editor}"
            )
    secret_root = legacy_secrets_root
    for reference in sorted(credential_refs):
        # The pinned SDK currently declares plaintext HTTP.  Do not even open
        # the already-exposed Tushare token file until both the vendor/SDK HTTPS
        # origin and the reviewed configuration agree.  Static validation can
        # still report all independent readiness blockers without touching the
        # unsafe credential.
        if reference == "secret://tushare_token" and not tushare_transport_verified:
            continue
        try:
            resolve_credential_ref(
                reference,
                env=values,
                secrets_root=secret_root,
                allow_plain_env=False,
            )
        except CredentialResolutionError as exc:
            raise ProductionConfigurationError(str(exc)) from exc

    manifest = str(values.get("FACTOR_LAB_SOURCE_BUNDLE_MANIFEST") or "").strip()
    if not manifest:
        raise ProductionConfigurationError(
            "production requires FACTOR_LAB_SOURCE_BUNDLE_MANIFEST"
        )
    provenance = capture_epoch_provenance(
        configuration_path=config_path,
        repository=Path(PRODUCTION_REPOSITORY),
        manifest_path=manifest,
        image_reference=image_reference,
    )
    readiness_blockers: list[str] = []
    if not bool(getattr(provenance, "formal_epoch_eligible", False)):
        readiness_blockers.append("daemon_inspected_oci_provenance_missing")
    if not formal_execution_capable:
        readiness_blockers.append("formal_execution_adapter_insufficient")
    credential_rotation_blockers: list[str] = []
    credential_retention_waivers: list[str] = []
    if tushare_retention_waived:
        credential_retention_waivers.append("tushare_token")
    if diemeng_retention_waived:
        credential_retention_waivers.append("diemeng_api_key")
    if not (tushare_rotation_verified or tushare_retention_waived):
        credential_rotation_blockers.append(
            "tushare_token_post_exposure_rotation_pending"
        )
    if not (diemeng_rotation_verified or diemeng_retention_waived):
        credential_rotation_blockers.append(
            "diemeng_api_key_post_exposure_rotation_pending"
        )
    source_transport_blockers: list[str] = []
    if not tushare_transport_verified:
        source_transport_blockers.append("tushare_https_transport_unverified")
    if (
        _OPERATOR_RETENTION_REFS["diemeng_api_key"]
        in operational_credential_refs
        and not diemeng_transport_verified
    ):
        source_transport_blockers.append("diemeng_https_transport_unverified")
    readiness_blockers.extend(credential_rotation_blockers)
    readiness_blockers.extend(source_transport_blockers)
    # This validator proves only that the static production contract and image
    # inputs are coherent.  Formal forward activation additionally requires a
    # persisted runtime readiness audit (capability probe, accepted Gold/full
    # matrix, restore drill, and soak), which is intentionally outside JSON.
    readiness_blockers.append("persisted_production_readiness_audit_missing")
    formal_forward_evidence = False
    return ProductionConfigEvidence(
        path=config_path,
        runtime_data_root=data_root,
        runtime_artifact_root=artifact_root,
        credential_refs=tuple(sorted(credential_refs)),
        provenance=provenance,
        formal_execution_capable=formal_execution_capable,
        historical_backfill_allowed=(
            (tushare_rotation_verified or tushare_retention_waived)
            and (diemeng_rotation_verified or diemeng_retention_waived)
            and tushare_transport_verified
            and diemeng_transport_verified
        ),
        formal_forward_evidence=formal_forward_evidence,
        readiness_blockers=tuple(readiness_blockers),
        credential_rotation_blockers=tuple(credential_rotation_blockers),
        source_transport_blockers=tuple(source_transport_blockers),
        credential_retention_waivers=tuple(credential_retention_waivers),
        engineering_canary_execution_contract_hash=(
            canary_execution_contract_hash
        ),
    )


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate Research OS production bootstrap")
    parser.add_argument("--config", required=True)
    parser.add_argument("--no-mount-check", action="store_true")
    args = parser.parse_args(argv)
    evidence = validate_production_config(
        args.config,
        require_mounts=not args.no_mount_check,
    )
    print(
        json.dumps(
            {
                "status": evidence.status,
                "formal_execution_capable": evidence.formal_execution_capable,
                "historical_backfill_allowed": evidence.historical_backfill_allowed,
                "formal_forward_evidence": evidence.formal_forward_evidence,
                "readiness_blockers": list(evidence.readiness_blockers),
                "config": str(evidence.path),
                "runtime_data_root": str(evidence.runtime_data_root),
                "runtime_artifact_root": str(evidence.runtime_artifact_root),
                "credential_refs": list(evidence.credential_refs),
                "credential_retention_waivers": list(
                    evidence.credential_retention_waivers
                ),
                "provenance": evidence.provenance.public_dict(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by container startup
    raise SystemExit(_main())


__all__ = [
    "ORCHESTRATION_CONFIG_ENV",
    "PRODUCTION_ARTIFACT_ROOT",
    "PRODUCTION_CONFIG_NAME",
    "PRODUCTION_DATA_ROOT",
    "PRODUCTION_REPOSITORY",
    "PRODUCTION_RUNTIME_ROOT",
    "ProductionConfigEvidence",
    "ProductionConfigurationError",
    "ProductionOperation",
    "ProductionOperationAdmission",
    "admit_production_operation",
    "load_production_config",
    "validate_production_config",
    "validate_production_environment",
]
