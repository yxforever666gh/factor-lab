"""Configuration-driven application services behind Dagster operations.

This is the concrete bridge between the orchestration allow-list and Research
OS domain services.  It intentionally owns no broker or live-order capability.
Each operation either records an authoritative artifact, reports a legitimate
absence of research/shadow input, or fails closed; it never fabricates a green
result merely to keep a schedule moving.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields, is_dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
import hashlib
import json
import os
from pathlib import Path
import platform as runtime_platform
import re
import tempfile
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from .bitemporal import CanonicalizationSpec, canonicalize_batch
from .build_provenance import (
    SOURCE_BUNDLE_MANIFEST_ENV,
    SourceBundleProvenanceError,
    capture_source_bundle_environment,
)
from .catalog import LifecycleEvent, ResearchCatalog, RunRecord
from .champion import ChampionChallengePolicy
from .champion_control import (
    AuthoritativeChampionControl,
    ChampionAllocationProjection,
    ChampionControlError,
    ChampionStockTarget,
    ChampionStockTargetUnavailable,
)
from .challenger_planner import (
    AuthoritativeChallengerPlanner,
    ChallengerPlannerError,
    ChallengerStockTarget,
)
from .contracts import (
    DataQualityStatus,
    DataSnapshotRef,
    EnvironmentRef,
    ExperimentSpec,
    LabelSpec,
    LifecycleState,
    RecoveryCaseStatus,
    SnapshotTier,
    UniverseSpec,
    ValidationProtocol,
)
from .cycle import HistoricalResearchCycle, field_specs_from_mapping
from .data_quality import DataQualityGate, QualityReport, QualitySeverity, sha256_path
from .data_sources import FetchRequest, SourceBatch
from .data_sync import (
    BronzeObservationError,
    BronzeSyncResult,
    bind_production_source_transport,
    dataset_contract_from_mapping,
    read_frame,
    source_adapter_from_mapping,
    sync_bronze,
)
from .evaluator import CANONICAL_EVALUATOR_VERSION
from .execution import AShareCostPolicy
from .fingerprint import canonical_json, capture_environment, content_fingerprint
from .governance import (
    HISTORICAL_HOLDOUT_ID,
    EvidenceClass,
    TrialKind,
    TrialRegistration,
)
from .gold_panel import (
    DEFAULT_REQUIRED_DATASETS,
    DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP,
    GoldPanelError,
    RESEARCH_SILVER_PARTITION_LABEL,
    ResearchGoldPanelService,
    load_gold_research_panel,
)
from .iceberg_service import GoldSnapshotPublisher, PyIcebergGoldPublisher
from .lifecycle import (
    LifecycleTransition,
    SleeveHealthObservation,
    SleeveLifecycleRecord,
    SleeveState,
    advance_lifecycle,
)
from .legacy_bronze_seed import (
    SnapshotPromotionBlocked,
    assert_snapshot_promotion_allowed,
)
from .monitor import LifecycleMonitor
from .monthly_research import MonthlyResearchCoordinator
from .negative_controls import NegativeControlMetric
from .object_store import ArchivedObject, S3ImmutableArchive
from .orchestration import (
    CycleName,
    OperationName,
    OperationRequest,
    OperationResult,
    OrchestrationFailure,
    ResearchOSServices,
    ServiceNotConfigured,
    Trigger,
    TriggerPoll,
)
from .reconciliation import ComparisonPolicy, reconcile_observations
from .proposals import persist_proposal_decision, review_llm_proposal
from .proposal_ports import ProposalPortError, proposal_port_from_config
from .production_daily import DailyDataOutcome, DailyDataStatus, ProductionDailyControl
from .production_config import (
    ProductionConfigEvidence,
    ProductionOperation,
    admit_production_operation,
    validate_production_config,
)
from .production_ledger import (
    CapabilityRecord,
    CapabilityStatus,
    IncidentRecord,
    IncidentStage,
    IncidentStatus,
    PartitionIdentity,
    PartitionLease,
    PartitionStatus,
    ProductionLedger,
    ProductionLedgerError,
    TypedEffectFenceConflict,
    load_runtime_authority_marker,
    sanitize_operational_text,
)
from .incident_control_outbox import IncidentControlActionStatus
from .readiness_audit import (
    ProductionReadinessAudit,
    ProductionReadinessAuditor,
)
from .data_incidents import (
    CashTargetIntent,
    DataIncident,
    DataIncidentCoordinator,
    DataPipelineStage,
    DataRevalidation,
    DataRevalidationConflict,
)
from .recovery import RecoveryCoordinator, RecoveryWorkflowError
from .runtime import ResearchOSSettings
from .shadow import ShadowExecutionConfig
from .shadow_authority import ShadowEvidenceAuthority, ShadowRole
from .shadow_catalog import ShadowStepAlreadyApplied, ShadowStepService
from .sleeve_registry import load_sleeve_roster, persist_sleeve_roster
from .sleeve_lifecycle import DailyShadowPlan
from .sleeves import fit_state_conditioned_overlay
from .soak_monitor import (
    DagsterCodeLocationSoakMonitor,
    DagsterSoakError,
    DagsterSoakIncomplete,
    bind_current_code_server_to_host_attestation,
)
from .snapshot_service import environment_hashes
from .snapshots import (
    ImmutableSnapshotManifest,
    build_immutable_snapshot_manifest,
    publish_snapshot_manifest,
    verify_immutable_snapshot_manifest,
)


class NonDataPipelineFailure(OrchestrationFailure):
    """A Dagster run failed after every durable data stage had succeeded."""


class RetryableShadowRepairEvidence(OrchestrationFailure):
    """A completed Shadow attempt lost its exact fleet/tail validation window."""


APPLICATION_SERVICES_SCHEMA_VERSION = "research-os/application-services/v1"
ORCHESTRATION_CONFIG_ENV = "FACTOR_LAB_ORCHESTRATION_CONFIG"
WEBUI_ENV_FILE_ENV = "FACTOR_LAB_ENV_FILE"
_SAFE_COMPONENT = re.compile(r"[^A-Za-z0-9._-]+")
_ZERO_HASH = hashlib.sha256(b"").hexdigest()
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_ACTIVE_RECOVERY_CASE_STATUSES = (
    RecoveryCaseStatus.OPEN,
    RecoveryCaseStatus.DIAGNOSING,
    RecoveryCaseStatus.OBSERVING,
)

_WEBUI_DATA_SOURCE_ENV_KEYS = frozenset(
    {
        "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON",
        "FACTOR_LAB_DATA_SOURCE_ORDER",
        "FACTOR_LAB_PRIMARY_DATA_SOURCE",
    }
)
_WEBUI_LLM_ENV_KEYS = frozenset(
    {
        "FACTOR_LAB_DECISION_PROVIDER",
        "FACTOR_LAB_LIVE_DECISION_PROVIDER",
        "FACTOR_LAB_OBSERVATION_DECISION_PROVIDER",
        "FACTOR_LAB_LLM_BASE_URL",
        "FACTOR_LAB_LLM_MODEL",
        "FACTOR_LAB_LLM_API_KEY_REF",
        "FACTOR_LAB_LLM_API_FORMAT",
        "FACTOR_LAB_LLM_PROFILES_JSON",
        "FACTOR_LAB_LLM_FALLBACK_ORDER",
    }
)
_WEBUI_RUNTIME_ENV_KEYS = _WEBUI_DATA_SOURCE_ENV_KEYS | _WEBUI_LLM_ENV_KEYS
_RAW_WEBUI_SECRET_KEYS = frozenset(
    {
        "TUSHARE_TOKEN",
        "DIEMENG_API_KEY",
        "DATA_SOURCE_API_KEY",
        "FACTOR_LAB_LLM_API_KEY",
    }
)


def _active_recovery_cases(
    catalog: ResearchCatalog,
    *,
    sleeve_id: str | None = None,
) -> tuple[Any, ...]:
    cases = tuple(
        catalog.iter_recovery_cases(
            statuses=_ACTIVE_RECOVERY_CASE_STATUSES,
            sleeve_id=sleeve_id,
            batch_size=1_000,
        )
    )
    seen_sleeves: set[str] = set()
    duplicates: set[str] = set()
    for case in cases:
        if case.sleeve_id in seen_sleeves:
            duplicates.add(case.sleeve_id)
        seen_sleeves.add(case.sleeve_id)
    if duplicates:
        raise OrchestrationFailure(
            "multiple active recovery cases exist for Sleeve(s): "
            + ", ".join(sorted(duplicates))
        )
    return cases


def _database_connect_args(settings: Any) -> dict[str, Any] | None:
    resolver = getattr(settings, "database_connect_args", None)
    if not callable(resolver):
        return None
    return resolver() or None


def _effective_production_authority(settings: Any) -> bool:
    """Resolve production authority from both process intent and the database.

    PostgreSQL's Alembic-owned marker is deliberately stronger than a mutable
    process environment variable.  A production database therefore remains
    production even if a service is accidentally launched with
    ``FACTOR_LAB_ENVIRONMENT=local``.  Conversely, merely setting the variable
    cannot turn an unmarked PostgreSQL database into production authority.
    """

    declared = (
        str(getattr(settings, "environment", "local")).strip().lower()
        == "production"
    )
    if not bool(getattr(settings, "uses_postgresql", False)):
        return declared
    try:
        connect_args = _database_connect_args(settings)
        marker = (
            load_runtime_authority_marker(str(settings.database_url))
            if connect_args is None
            else load_runtime_authority_marker(
                str(settings.database_url),
                connect_args=connect_args,
            )
        )
    except Exception as exc:
        raise ServiceNotConfigured(
            "cannot verify the PostgreSQL runtime-authority marker"
        ) from exc
    database_is_production = bool(marker is not None and marker.is_production)
    if declared and not database_is_production:
        raise ServiceNotConfigured(
            "production PostgreSQL requires its Alembic-owned authority marker"
        )
    return database_is_production


_REQUIRED_DATASET_ALIASES = {
    "suspend_d": "trade_status",
    "suspend": "trade_status",
    "suspension": "trade_status",
    "suspension_status": "trade_status",
    "stock_limit": "trade_status",
    "stk_limit": "trade_status",
    "namechange": "historical_st",
    "stock_st": "historical_st",
    "industry": "industry_classification",
    "stock_industry": "industry_classification",
    "dividend": "company_action",
    "corporate_action": "company_action",
    "trade_cal": "trade_calendar",
    "stock_basic_l": "stock_basic",
    "stock_basic_p": "stock_basic",
    "stock_basic_d": "stock_basic",
    "stock_basic_g": "stock_basic",
}

_PRODUCTION_STAGE_DATASETS = {
    OperationName.SOURCE_SYNC: "stage_source",
    OperationName.SOURCE_RECONCILIATION: "stage_silver",
    OperationName.DATA_QUALITY_GATE: "stage_data_quality",
    OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH: "stage_gold",
    OperationName.SHADOW_NAV_STEP: "stage_shadow",
}
_SEMANTIC_OPERATION_METADATA_KEYS = frozenset(
    {
        "static_refresh_id",
        "submission_id",
        "repair_scope_key",
        "repair_incident_id",
        "repair_fingerprint",
        "repair_generation",
        "repair_parent_partition_run_id",
        "repair_validation_trade_date",
    }
)
_REPAIR_METADATA_KEYS = frozenset(
    {
        "repair_scope_key",
        "repair_incident_id",
        "repair_fingerprint",
        "repair_generation",
        "repair_parent_partition_run_id",
        "repair_authority_id",
        "repair_cohort_id",
        "repair_stage_generations",
        "repair_validation_trade_date",
    }
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, np.generic):
        return value.item()
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump(mode="json"))
    if isinstance(value, float) and not np.isfinite(value):
        return None
    return value


def _encoded_json(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        _jsonable(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _write_bytes_once(path: Path, payload: bytes) -> Path:
    """Publish one immutable artifact; identical retries are idempotent."""

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o444)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise OrchestrationFailure(
                f"immutable orchestration artifact already differs: {path}"
            )
        return path
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    return path


def _write_json_once(path: Path, payload: Mapping[str, Any]) -> Path:
    return _write_bytes_once(path, _encoded_json(payload))


def _write_parquet_once(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pd.read_parquet(path)
        if list(existing.columns) == list(frame.columns) and existing.equals(frame):
            return path
        raise OrchestrationFailure(
            f"immutable orchestration Parquet already differs: {path}"
        )
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.stem}.", suffix=".parquet", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
    try:
        frame.to_parquet(temporary, index=False)
        # A hard link gives both Windows and Linux an exclusive target create;
        # unlike os.replace it cannot silently overwrite another worker.
        try:
            os.link(temporary, path)
        except FileExistsError:
            existing = pd.read_parquet(path)
            if list(existing.columns) != list(frame.columns) or not existing.equals(frame):
                raise OrchestrationFailure(
                    f"concurrent immutable Parquet publication differed: {path}"
                )
    finally:
        if temporary.exists():
            temporary.unlink()
    return path


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON document must be an object: {path}")
    return value


def _validate_runtime_profiles(values: Mapping[str, str]) -> None:
    raw_data_profiles = str(
        values.get("FACTOR_LAB_DATA_SOURCE_PROFILES_JSON") or ""
    ).strip()
    if raw_data_profiles:
        try:
            profiles = json.loads(raw_data_profiles)
        except json.JSONDecodeError as exc:
            raise ServiceNotConfigured(
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON is invalid JSON"
            ) from exc
        if not isinstance(profiles, list):
            raise ServiceNotConfigured(
                "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON must contain a list"
            )
        for index, profile in enumerate(profiles):
            if not isinstance(profile, Mapping):
                raise ServiceNotConfigured(
                    f"data-source profile {index} must be an object"
                )
            if str(profile.get("api_key") or "").strip():
                raise ServiceNotConfigured(
                    f"data-source profile {index} contains forbidden api_key; "
                    "use credential_ref"
                )
            if (
                str(profile.get("source_type") or "").strip().lower()
                in {"tushare", "diemeng"}
                and bool(profile.get("enabled", True))
                and not str(profile.get("credential_ref") or "").strip()
            ):
                raise ServiceNotConfigured(
                    f"data-source profile {index} requires credential_ref"
                )

    llm_reference = str(values.get("FACTOR_LAB_LLM_API_KEY_REF") or "").strip()
    if llm_reference and not llm_reference.startswith("secret://"):
        raise ServiceNotConfigured(
            "FACTOR_LAB_LLM_API_KEY_REF must use secret://"
        )
    raw_llm_profiles = str(
        values.get("FACTOR_LAB_LLM_PROFILES_JSON") or ""
    ).strip()
    if not raw_llm_profiles:
        return
    try:
        llm_profiles = json.loads(raw_llm_profiles)
    except json.JSONDecodeError as exc:
        raise ServiceNotConfigured(
            "FACTOR_LAB_LLM_PROFILES_JSON is invalid JSON"
        ) from exc
    if not isinstance(llm_profiles, list):
        raise ServiceNotConfigured(
            "FACTOR_LAB_LLM_PROFILES_JSON must contain a list"
        )
    for index, profile in enumerate(llm_profiles):
        if not isinstance(profile, Mapping):
            raise ServiceNotConfigured(f"LLM profile {index} must be an object")
        if str(profile.get("api_key") or "").strip():
            raise ServiceNotConfigured(
                f"LLM profile {index} contains forbidden api_key; use credential_ref"
            )
        reference = str(profile.get("credential_ref") or "").strip()
        if reference and not reference.startswith("secret://"):
            raise ServiceNotConfigured(
                f"LLM profile {index} credential_ref must use secret://"
            )


def _read_runtime_env_file(path: str | Path) -> dict[str, str]:
    """Read only the non-secret WebUI runtime-settings ledger.

    The file is intentionally not a general dotenv loader.  In particular,
    model credentials and legacy inline market-data tokens never enter the
    Dagster process through this seam.  Safe model/source profiles carry only
    ``credential_ref`` values.  Process environment values are merged by
    :func:`_runtime_environment` afterwards and therefore take precedence.
    """

    env_path = Path(path).resolve()
    if not env_path.is_file():
        raise ServiceNotConfigured(
            f"{WEBUI_ENV_FILE_ENV} does not reference a readable file: {env_path}"
        )
    selected: dict[str, str] = {}
    for line_number, line in enumerate(
        env_path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if raw.startswith("export "):
            raw = raw[7:].lstrip()
        if "=" not in raw:
            raise ServiceNotConfigured(
                f"invalid {WEBUI_ENV_FILE_ENV} assignment on line {line_number}"
            )
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key in _RAW_WEBUI_SECRET_KEYS and value:
            raise ServiceNotConfigured(
                f"{WEBUI_ENV_FILE_ENV} contains forbidden inline secret {key}; "
                "store credentials behind credential_ref/*_FILE"
            )
        if key in _WEBUI_RUNTIME_ENV_KEYS:
            selected[key] = value
    _validate_runtime_profiles(selected)
    return selected


def _runtime_environment(process_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Merge the current WebUI settings snapshot without mutating ``os.environ``."""

    process = dict(os.environ if process_env is None else process_env)
    raw_path = str(process.get(WEBUI_ENV_FILE_ENV) or "").strip()
    file_values = _read_runtime_env_file(raw_path) if raw_path else {}
    merged = {**file_values, **process}
    _validate_runtime_profiles(merged)
    return merged


def _parse_aware(value: Any, *, name: str) -> datetime:
    parsed = pd.Timestamp(value)
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must include a timezone")
    return parsed.to_pydatetime()


def _parse_date(value: Any, *, name: str) -> date:
    try:
        return pd.Timestamp(value).date()
    except Exception as exc:
        raise ValueError(f"invalid {name}: {value!r}") from exc


def _safe_name(value: str) -> str:
    rendered = _SAFE_COMPONENT.sub("_", str(value)).strip("._")
    return rendered[:96] or "unnamed"


def _render(value: Any, tokens: Mapping[str, Any]) -> Any:
    if isinstance(value, str):
        result = value
        for key, replacement in tokens.items():
            result = result.replace("${" + str(key) + "}", str(replacement))
        unresolved = re.findall(r"\$\{[^}]+\}", result)
        if unresolved:
            raise ValueError(f"unresolved orchestration tokens: {sorted(set(unresolved))}")
        return result
    if isinstance(value, Mapping):
        return {str(key): _render(item, tokens) for key, item in value.items()}
    if isinstance(value, list):
        return [_render(item, tokens) for item in value]
    if isinstance(value, tuple):
        return tuple(_render(item, tokens) for item in value)
    return value


def _result_from_dict(payload: Mapping[str, Any]) -> OperationResult:
    return OperationResult(
        operation=OperationName(str(payload["operation"])),
        status=str(payload["status"]),
        summary=str(payload["summary"]),
        outputs=dict(payload.get("outputs") or {}),
    )


@dataclass(frozen=True)
class _StagePaths:
    root: Path
    artifacts: Path


class ApplicationServices(ResearchOSServices):
    """Concrete, restart-safe implementation of every orchestration operation."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        settings: ResearchOSSettings,
        catalog: ResearchCatalog,
        iceberg_publisher: GoldSnapshotPublisher,
        object_store_archive: S3ImmutableArchive | None = None,
        env: Mapping[str, str] | None = None,
        config_base: str | Path | None = None,
        environment_hashes_override: Mapping[str, str] | None = None,
        source_bundle_manifest: str | Path | None = None,
        configuration_path: str | Path | None = None,
        production_config_evidence: ProductionConfigEvidence | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = dict(config)
        if self.config.get("schema_version") != APPLICATION_SERVICES_SCHEMA_VERSION:
            raise ServiceNotConfigured(
                f"orchestration config schema_version must be {APPLICATION_SERVICES_SCHEMA_VERSION!r}"
            )
        self.settings = settings
        self.catalog = catalog
        self.iceberg_publisher = iceberg_publisher
        self.object_store_archive = object_store_archive
        self.production_config_evidence = production_config_evidence
        self._configuration_path = (
            None if configuration_path is None else Path(configuration_path).resolve()
        )
        if self.settings.uses_postgresql and self.object_store_archive is None:
            raise ServiceNotConfigured(
                "PostgreSQL Research OS requires a configured Bronze/Silver object-store archive"
            )
        self._production_authority = _effective_production_authority(self.settings)
        if (
            self._production_authority
            and self.production_config_evidence is None
        ):
            raise ServiceNotConfigured(
                "production ApplicationServices requires validated configuration evidence"
            )
        self.env = dict(os.environ if env is None else env)
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._config_base = Path(config_base or Path.cwd()).resolve()
        repository = self._resolve_path(self.config.get("repository") or ".")
        self._path_base = self._resolve_from(
            repository, self.config.get("path_base") or "."
        )
        dependency_lock = self._resolve_from(
            repository, self.config.get("dependency_lock") or "uv.lock"
        )
        if environment_hashes_override is None:
            source_bundle_provenance = None
            try:
                if source_bundle_manifest is not None:
                    if configuration_path is None:
                        raise SourceBundleProvenanceError(
                            "configuration_path is required for source-bundle provenance"
                        )
                    source_bundle_capture = capture_source_bundle_environment(
                        source_bundle_manifest,
                        bundle_root=repository,
                        dependency_lock=dependency_lock,
                        configuration_path=configuration_path,
                        evaluator_build=CANONICAL_EVALUATOR_VERSION,
                    )
                    source_bundle_provenance = source_bundle_capture.provenance
                    environment = source_bundle_capture.environment
                else:
                    environment = capture_environment(
                        repository,
                        dependency_lock=dependency_lock,
                        configuration=self.config,
                        evaluator_build=CANONICAL_EVALUATOR_VERSION,
                    )
            except (RuntimeError, OSError) as exc:
                raise ServiceNotConfigured(
                    f"cannot establish exact build provenance: {exc}"
                ) from exc
            self._environment_hashes = environment_hashes(environment)
            if source_bundle_provenance is not None:
                self._environment_hashes["build_provenance_hash"] = (
                    source_bundle_provenance.manifest_hash
                )
        else:
            self._environment_hashes = {
                str(key): str(value)
                for key, value in environment_hashes_override.items()
            }
        required_hashes = {
            "code_hash",
            "dependency_lock_hash",
            "config_hash",
            "dirty_patch_hash",
        }
        invalid = sorted(
            key
            for key in required_hashes
            if not re.fullmatch(r"[0-9a-f]{64}", self._environment_hashes.get(key, ""))
        )
        invalid.extend(
            sorted(
                key
                for key, value in self._environment_hashes.items()
                if key not in required_hashes
                and not re.fullmatch(r"[0-9a-f]{64}", value)
            )
        )
        if invalid:
            raise ServiceNotConfigured(f"invalid environment hash inputs: {invalid}")
        self._configuration_hash = content_fingerprint(
            self.config, domain="factor-lab/research-os/v1/orchestration-config"
        )
        state_config = self.config.get("state") or {}
        if not isinstance(state_config, Mapping):
            raise ServiceNotConfigured("state configuration must be an object")
        self._state_root = self._resolve_from(
            self.settings.lake_root,
            state_config.get("root") or "_orchestration",
        )
        self.settings.lake_root.mkdir(parents=True, exist_ok=True)
        self.settings.snapshot_root.mkdir(parents=True, exist_ok=True)
        self._state_root.mkdir(parents=True, exist_ok=True)
        self.catalog.initialize_schema()
        self.production_ledger = (
            ProductionLedger(self.settings.database_url)
            if self.settings.uses_postgresql
            else None
        )
        self.shadow_authority = (
            ShadowEvidenceAuthority(self.settings.database_url)
            if self.settings.uses_postgresql
            else None
        )
        self.sleeve_roster = None
        roster_value = self.config.get("sleeve_roster_path")
        if roster_value:
            roster_path = self._resolve_from(repository, roster_value)
            self.sleeve_roster = load_sleeve_roster(roster_path)
            persist_sleeve_roster(self.catalog, self.sleeve_roster)
        self.monthly_research = MonthlyResearchCoordinator(
            self.catalog,
            lake_root=self.settings.lake_root,
            environment=EnvironmentRef(
                code_hash=self._environment_hashes["code_hash"],
                dependency_lock_hash=self._environment_hashes[
                    "dependency_lock_hash"
                ],
                configuration_hash=self._environment_hashes["config_hash"],
                dirty_patch_hash=self._environment_hashes.get("dirty_patch_hash"),
                python_version=runtime_platform.python_version(),
                platform=runtime_platform.platform(),
                evaluator_build=CANONICAL_EVALUATOR_VERSION,
            ),
            shadow_authority=self.shadow_authority,
        )
        if self.sleeve_roster is not None:
            self.monthly_research.register_families(
                tuple(entry.sleeve for entry in self.sleeve_roster.entries)
            )
        elif self.settings.uses_postgresql:
            raise ServiceNotConfigured(
                "production monthly research requires a fixed sleeve_roster_path"
            )
        self._handlers = {
            OperationName.SOURCE_SYNC: self._source_sync,
            OperationName.SOURCE_RECONCILIATION: self._source_reconciliation,
            OperationName.DATA_QUALITY_GATE: self._data_quality_gate,
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH: self._gold_publish,
            OperationName.SHADOW_NAV_STEP: self._shadow_nav_step,
            OperationName.SLEEVE_HEALTH_CHECK: self._sleeve_health_check,
            OperationName.DRIFT_DETECTION: self._drift_detection,
            OperationName.LIFECYCLE_TRANSITION: self._lifecycle_transition,
            OperationName.RECOVERY_SLA_CHECK: self._recovery_sla_check,
            OperationName.CONFIRMATORY_BUDGET_GATE: self._confirmatory_budget_gate,
            OperationName.LIMITED_DISCOVERY: self._limited_discovery,
            OperationName.WEIGHT_REESTIMATION: self._weight_reestimation,
            OperationName.CHALLENGER_GENERATION: self._challenger_generation,
            OperationName.VALIDATION_PROTOCOL_AUDIT: self._validation_protocol_audit,
            OperationName.RESEARCH_BUDGET_AUDIT: self._research_budget_audit,
        }

    def _is_production_runtime(self) -> bool:
        """Return the constructor-sealed authority, with a test-double fallback."""

        if hasattr(self, "_production_authority"):
            return bool(self._production_authority)
        return _effective_production_authority(self.settings)

    def _bind_source_transport_authority(
        self,
        source: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not self._is_production_runtime():
            return dict(source)
        return bind_production_source_transport(
            source,
            production_config=self.config,
        )

    def _production_readiness_auditor(self) -> ProductionReadinessAuditor:
        if self.production_ledger is None or self.production_config_evidence is None:
            raise OrchestrationFailure(
                "production readiness audit requires PostgreSQL and validated config"
            )
        return ProductionReadinessAuditor(
            self.catalog,
            self.production_ledger,
            config=self.config,
            config_evidence=self.production_config_evidence,
        )

    def audit_production_readiness(self) -> ProductionReadinessAudit:
        """Persist a PG-derived audit; no caller-supplied facts are accepted."""

        return self._production_readiness_auditor().audit()

    def run_physical_engineering_canary(
        self,
        *,
        as_of: date | None = None,
    ) -> Any:
        """Run the code-selected 50×20 physical, explicitly non-forward canary."""

        if not self._is_production_runtime():
            raise OrchestrationFailure("physical canary is production-only")
        if (
            self._configuration_path is None
            or self.production_ledger is None
            or self.shadow_authority is None
            or self.object_store_archive is None
        ):
            raise OrchestrationFailure(
                "physical canary requires production config, PG, MinIO, and shadow authority"
            )
        from .physical_canary import PhysicalEngineeringCanaryService

        service = PhysicalEngineeringCanaryService.from_production_config(
            self._configuration_path,
            env=self.env,
            catalog=self.catalog,
            production_ledger=self.production_ledger,
            shadow_authority=self.shadow_authority,
            object_store_archive=self.object_store_archive,
            now=self._now,
        )
        return service.run(as_of=as_of)

    def run_physical_minio_restore_drill(self) -> Any:
        """Restore a code-selected canary object twice; accept no object inputs."""

        if not self._is_production_runtime():
            raise OrchestrationFailure("physical MinIO restore drill is production-only")
        if self._configuration_path is None or self.object_store_archive is None:
            raise OrchestrationFailure(
                "physical MinIO restore drill requires production config, PG, and MinIO"
            )
        from .restore_drill import PhysicalMinioRestoreDrillService

        service = PhysicalMinioRestoreDrillService.from_production_config(
            self._configuration_path,
            env=self.env,
            catalog=self.catalog,
            object_store_archive=self.object_store_archive,
        )
        return service.run()

    def _production_execution_snapshot_authority(self) -> Any:
        """Construct the sole typed execution producer from sealed infrastructure.

        The factory deliberately accepts no frame, snapshot identifier,
        capability declaration, or adapter payload from a caller/configured
        operation.  Reconstructing it per call also revalidates the production
        configuration and fixed secret references before touching evidence.
        """

        if (
            not self._is_production_runtime()
            or self._configuration_path is None
            or self.production_ledger is None
            or self.object_store_archive is None
            or self.production_config_evidence is None
        ):
            raise OrchestrationFailure(
                "typed execution authority requires production config, PostgreSQL, and MinIO"
            )
        from .execution_snapshot_authority import ExecutionSnapshotAuthority

        return ExecutionSnapshotAuthority.from_production_config(
            config_path=self._configuration_path,
            env=self.env,
            catalog=self.catalog,
            ledger=self.production_ledger,
            archive=self.object_store_archive,
            cache_root=(
                self.production_config_evidence.runtime_artifact_root
                / "execution-cache"
            ),
        )

    def accepted_execution_open_partition(
        self, scheduled_for: datetime
    ) -> str | None:
        """Select today's live 09:30 partition from the accepted PG calendar.

        Dagster's weekday cron is only a wake-up.  It cannot manufacture a
        session, replay a missed historical tick, or choose a future date.
        """

        if scheduled_for.tzinfo is None or scheduled_for.utcoffset() is None:
            raise ValueError("scheduled_for must include a timezone")
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "opening schedule requires the PostgreSQL production ledger"
            )
        scheduled = scheduled_for.astimezone(_SHANGHAI)
        if scheduled.time().replace(tzinfo=None) != time(9, 30):
            raise OrchestrationFailure(
                "opening schedule must represent the code-defined 09:30 tick"
            )
        observed = self.catalog.database_now().astimezone(_SHANGHAI)
        if observed.date() != scheduled.date() or observed.time() < time(9, 30):
            return None
        partition_key = scheduled.date().isoformat()
        accepted = set(self.production_ledger.accepted_calendar_partitions())
        return partition_key if partition_key in accepted else None

    def observe_execution_open(self, trade_date: date | str) -> Any:
        """Collect one code-selected opening observation for an accepted session."""

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "opening observation requires the PostgreSQL production ledger"
            )
        session = _parse_date(trade_date, name="execution trade_date")
        accepted = set(self.production_ledger.accepted_calendar_partitions())
        if session.isoformat() not in accepted:
            raise OrchestrationFailure(
                "opening observation requires an accepted PostgreSQL trading session"
            )
        return self._production_execution_snapshot_authority().observe_open(session)

    def build_execution_session(self, trade_date: date | str) -> Any:
        """Build one immutable execution/mark bundle from persisted authorities."""

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "typed execution session requires the PostgreSQL production ledger"
            )
        session = _parse_date(trade_date, name="execution trade_date")
        accepted = set(self.production_ledger.accepted_calendar_partitions())
        if session.isoformat() not in accepted:
            raise OrchestrationFailure(
                "typed execution session requires an accepted PostgreSQL trading session"
            )
        return self._production_execution_snapshot_authority().build_session(session)

    def formal_shadow_projection_allowed(self, trade_date: date | str) -> bool:
        """Return epoch admission only; typed capability may be built earlier."""

        session = _parse_date(trade_date, name="shadow projection trade_date")
        epoch = self.catalog.get_evidence_epoch()
        return bool(
            epoch is not None
            and epoch.first_forward_session is not None
            and epoch.forward_holdout_id is not None
            and session >= epoch.first_forward_session
        )

    def _latest_complete_accepted_session(self) -> date:
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "formal shadow projection requires the PostgreSQL partition ledger"
            )
        observed = self.catalog.database_now().astimezone(_SHANGHAI)
        completed = tuple(
            session
            for raw in self.production_ledger.accepted_calendar_partitions()
            for session in (date.fromisoformat(str(raw)),)
            if datetime.combine(session, time(15, 0), tzinfo=_SHANGHAI)
            <= observed
        )
        if not completed:
            raise OrchestrationFailure(
                "no accepted trading session has completed its closing mark"
            )
        return max(completed)

    def _require_formal_shadow_session(self, trade_date: date) -> None:
        """Prevent historical replay from being recorded as forward evidence."""

        epoch = self.catalog.get_evidence_epoch()
        if (
            epoch is None
            or epoch.first_forward_session is None
            or epoch.forward_holdout_id is None
        ):
            raise OrchestrationFailure(
                "formal shadow projection requires an activated evidence epoch"
            )
        latest = self._latest_complete_accepted_session()
        if trade_date != latest:
            raise OrchestrationFailure(
                "formal shadow projection may process only the latest complete "
                "accepted trading session"
            )
        if trade_date < epoch.first_forward_session:
            raise OrchestrationFailure(
                "pre-epoch sessions cannot enter formal forward evidence"
            )

    def latest_production_readiness_audit(
        self,
    ) -> ProductionReadinessAudit | None:
        """Return the latest hash-verified readiness audit for doctor/UI consumers."""

        return self._production_readiness_auditor().latest()

    @staticmethod
    def _audit_check(
        audit: ProductionReadinessAudit,
        code: str,
    ) -> Any | None:
        return next((item for item in audit.checks if item.code == code), None)

    def _runtime_readiness_blockers(self) -> tuple[str, ...]:
        """Revalidate the persisted release verdict before formal work.

        The service does not trust static JSON readiness fields.  It binds the
        latest hash-verified PG audit to the running immutable source bundle,
        the daemon-inspected image ID injected by Compose, current credential
        rotation configuration, and the live data-incident ledger.
        """

        blockers: list[str] = []
        try:
            audit = self.latest_production_readiness_audit()
        except Exception:
            audit = None
        if audit is None:
            return ("persisted_production_readiness_audit_missing",)
        if not audit.ready:
            blockers.extend(audit.blockers or ("production_readiness_not_ready",))
        observed_now = self.catalog.database_now().astimezone(timezone.utc)
        age = observed_now - audit.audited_at.astimezone(timezone.utc)
        if age.total_seconds() < 0 or age > timedelta(minutes=5):
            blockers.append("production_readiness_audit_stale")

        oci_check = self._audit_check(audit, "daemon_inspected_oci_provenance")
        inspected = dict(oci_check.evidence) if oci_check is not None else {}
        current = self.production_config_evidence.provenance
        inspected_epoch = inspected.get("epoch_fields")
        release_fields = {
            "architecture_version": current.architecture_version,
            "code_hash": current.code_hash,
            "configuration_hash": current.configuration_hash,
            "dependency_lock_hash": current.dependency_lock_hash,
        }
        if (
            oci_check is None
            or not oci_check.passed
            or not isinstance(inspected_epoch, Mapping)
            or any(
                inspected_epoch.get(key) != value
                for key, value in release_fields.items()
            )
        ):
            blockers.append("production_readiness_release_mismatch")
        try:
            attested_at = _parse_aware(
                inspected["attested_at"], name="host attestation time"
            )
        except (KeyError, TypeError, ValueError):
            blockers.append("production_readiness_host_attestation_missing")
        else:
            attestation_age = observed_now - attested_at
            if (
                attestation_age.total_seconds() < 0
                or attestation_age > timedelta(minutes=10)
            ):
                blockers.append("production_readiness_host_attestation_stale")
        if not (
            re.fullmatch(
                r"sha256:[0-9a-f]{64}",
                str(inspected.get("oci_image_id") or ""),
            )
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(inspected.get("host_attestation_hash") or ""),
            )
            and re.fullmatch(
                r"[0-9a-f]{64}", str(inspected.get("container_id") or "")
            )
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(inspected.get("deployment_identity_hash") or ""),
            )
            and str(inspected.get("compose_config_hash") or "").strip()
        ):
            blockers.append("production_readiness_deployment_identity_invalid")
        soak_check = self._audit_check(audit, "dagster_code_location_24h_soak")
        soak_evidence = dict(soak_check.evidence) if soak_check is not None else {}
        try:
            from .soak_monitor import local_code_server_process_identity

            current_process_identity = local_code_server_process_identity()
        except Exception:
            current_process_identity = None
        if (
            soak_check is None
            or not soak_check.passed
            or current_process_identity is None
            or soak_evidence.get("process_identity") != current_process_identity
        ):
            blockers.append("production_readiness_code_server_process_mismatch")
        if not self.production_config_evidence.historical_backfill_allowed:
            blockers.extend(self.production_config_evidence.credential_rotation_blockers)
        if self.production_ledger is not None and self.production_ledger.list_incidents(
            status=IncidentStatus.OPEN,
            limit=1,
        ):
            blockers.append("open_data_incident")
        return tuple(dict.fromkeys(blockers))

    def record_dagster_code_location_health(self) -> Mapping[str, Any]:
        """Append one safe heartbeat sample from a Dagster sensor round trip."""

        if self.production_ledger is None or self.production_config_evidence is None:
            raise OrchestrationFailure(
                "Dagster code-location soak requires PostgreSQL and validated config"
            )
        try:
            # A host attestation's ten-minute freshness window protects the
            # admission of *new formal work*.  The soak instead proves that
            # the same already-attested immutable container and PID-1 remain
            # alive for 24 hours.  Requiring a new host signature every sensor
            # tick made that continuity proof impossible after minute ten.
            #
            # The latest-attempt selector remains fail-closed: a later failed,
            # running or ambiguous host inspection stops sampling.  The local
            # binder then re-hashes the full persisted proof and matches the
            # executing hostname, init start/root, image and source bundle.
            auditor = self._production_readiness_auditor()
            host_run, attempt_blockers, _ = auditor._latest_host_docker_attempt()
            if host_run is None or attempt_blockers:
                raise DagsterSoakError(
                    "latest host Docker attestation attempt is unavailable"
                )
            deployment = bind_current_code_server_to_host_attestation(
                host_run,
                provenance=self.production_config_evidence.provenance,
            )
        except Exception:
            return {
                "status": "skipped",
                "reason": "matching daemon-inspected OCI readiness attestation is unavailable",
            }
        monitor = DagsterCodeLocationSoakMonitor(
            self.catalog,
            self.production_ledger,
            build_identity_hash=deployment.build_identity_hash,
            oci_image_id=deployment.oci_image_id,
            process_identity=deployment.process_identity,
            deployment_identity_hash=deployment.deployment_identity_hash,
            host_attestation_hash=deployment.host_attestation_hash,
            container_id=deployment.container_id,
            compose_config_hash=deployment.compose_config_hash,
        )
        run = monitor.record_sample()
        result = {
            "status": "recorded",
            "run_id": run.run_id,
            "sampled_at": run.metadata["sampled_at"],
            "sample_evidence_hash": run.metadata["sample_evidence_hash"],
            "process_identity": run.metadata["process_identity"],
        }
        try:
            soak = monitor.finalize(
                provenance=SimpleNamespace(
                    formal_epoch_eligible=True,
                    build_identity_hash=deployment.build_identity_hash,
                    oci_image_id=deployment.oci_image_id,
                )
            )
        except DagsterSoakIncomplete:
            result["soak_status"] = "accumulating"
        else:
            result.update(
                soak_status="completed",
                soak_run_id=soak.run_id,
                soak_evidence_hash=soak.metadata["soak_evidence_hash"],
            )
        return result

    def _resolve_path(self, value: Any) -> Path:
        path = Path(str(value))
        return path.resolve() if path.is_absolute() else (self._config_base / path).resolve()

    @staticmethod
    def _resolve_from(base: Path, value: Any) -> Path:
        path = Path(str(value))
        return path.resolve() if path.is_absolute() else (base / path).resolve()

    def _input_path(self, value: Any) -> Path:
        return self._resolve_from(self._path_base, value)

    def _latest_gold_snapshot(self, *, accepted_only: bool = True) -> Any | None:
        rows = self.catalog.list_snapshots(
            limit=1_000,
            quality_status=(
                DataQualityStatus.ACCEPTED if accepted_only else None
            ),
            tier=SnapshotTier.GOLD,
        )
        if len(rows) >= 1_000:
            raise OrchestrationFailure(
                "Gold snapshot listing reached the non-paginated safety limit"
            )
        return rows[0] if rows else None

    def _tokens(self, request: OperationRequest) -> dict[str, Any]:
        metadata_tokens = {
            str(key): value
            for key, value in request.metadata.items()
            if isinstance(value, (str, int, float, bool))
        }
        partition_compact = request.partition_key.replace("-", "")
        return {
            **metadata_tokens,
            "partition_key": request.partition_key,
            "partition_compact": partition_compact,
            "partition_yyyymmdd": partition_compact,
            "run_id": request.run_id,
            "cycle": request.cycle.value,
        }

    def _section(self, name: str, request: OperationRequest) -> dict[str, Any]:
        value = self.config.get(name)
        if not isinstance(value, Mapping):
            raise ServiceNotConfigured(f"{name} orchestration section is not configured")
        rendered = _render(dict(value), self._tokens(request))
        assert isinstance(rendered, dict)
        return rendered

    def _stage_paths(self, request: OperationRequest) -> _StagePaths:
        identity = content_fingerprint(
            {
                "config": self._configuration_hash,
                "cycle": request.cycle.value,
                "partition_key": request.partition_key,
                "semantic_metadata": self._semantic_operation_metadata(
                    request, request.operation
                ),
            },
            domain="factor-lab/research-os/v1/orchestration-stage",
        )
        root = (
            self._state_root
            / request.cycle.value
            / _safe_name(request.partition_key)
            / identity
        )
        return _StagePaths(
            root=root,
            artifacts=root / request.operation.value,
        )

    @staticmethod
    def _semantic_operation_metadata(
        request: OperationRequest,
        operation: OperationName | None = None,
    ) -> dict[str, Any]:
        """Keep only inputs that may change deterministic operation semantics.

        Dagster tags, run UUIDs, CLI authority labels and failure wake-up
        envelopes are orchestration provenance, not operation inputs.  Binding
        them into the fingerprint would make a new Dagster retry collide with
        the immutable partition created by the previous attempt.
        """

        selected_operation = request.operation if operation is None else operation
        metadata_keys = set(_SEMANTIC_OPERATION_METADATA_KEYS)
        # A generic retry regenerates only its failed logical slot; its
        # dependencies remain the immutable base authority.  Incident repair,
        # in contrast, intentionally carries one scope across all five stages.
        if (
            request.metadata.get("repair_scope_key")
            and not request.metadata.get("repair_incident_id")
            and selected_operation is not request.operation
        ):
            metadata_keys -= _REPAIR_METADATA_KEYS
        elif request.metadata.get("repair_incident_id"):
            metadata_keys -= {
                "repair_generation",
                "repair_parent_partition_run_id",
            }
        selected = {
            key: _jsonable(request.metadata[key])
            for key in sorted(metadata_keys)
            if key in request.metadata
        }
        if request.metadata.get("repair_incident_id"):
            generations = request.metadata.get("repair_stage_generations")
            if isinstance(generations, Mapping):
                generation = str(
                    generations.get(selected_operation.value) or ""
                ).strip()
                if generation:
                    selected["repair_generation"] = generation
        return selected

    def _operation_identity(
        self, request: OperationRequest, operation: OperationName
    ) -> tuple[str, str, str]:
        payload = {
            "configuration_hash": self._configuration_hash,
            "cycle": request.cycle.value,
            "partition_key": request.partition_key,
            "operation": operation.value,
            "semantic_metadata": self._semantic_operation_metadata(
                request, operation
            ),
        }
        fingerprint = content_fingerprint(
            payload, domain="factor-lab/research-os/v1/dagster-operation"
        )
        return (
            f"rosop_{fingerprint[:48]}",
            f"dagster:{request.cycle.value}:{operation.value}",
            fingerprint,
        )

    def _operation_run_id(self, request: OperationRequest) -> str:
        """Return the authoritative application run bound to partition writes.

        Dagster's run id is orchestration metadata: one Dagster run may execute
        several Research OS operations.  Each operation is claimed separately
        in ``ros_runs`` by :meth:`_execute_admitted`, so production partitions
        must reference that deterministic ``rosop_*`` row rather than the raw
        external Dagster UUID.
        """

        return self._operation_identity(request, request.operation)[0]

    def _read_stage(
        self, request: OperationRequest, operation: OperationName
    ) -> OperationResult | None:
        run_id, _, _ = self._operation_identity(request, operation)
        run = self.catalog.get_run(run_id)
        if run is None or run.status == "running":
            return None
        payload = run.metadata.get("operation_result")
        if not isinstance(payload, Mapping):
            raise OrchestrationFailure(
                f"catalog run {run.run_id!r} has no authoritative operation result"
            )
        result = _result_from_dict(payload)
        if result.operation is not operation:
            raise OrchestrationFailure(
                f"catalog run {run.run_id!r} operation result mismatches {operation.value}"
            )
        if result.status != run.status:
            raise OrchestrationFailure(
                f"catalog run {run.run_id!r} status disagrees with its result summary"
            )
        return result

    def _authoritative_dependency_request(
        self,
        request: OperationRequest,
        operation: OperationName,
    ) -> OperationRequest:
        """Select the immutable generic-retry leaf for an upstream stage."""

        if (
            self.production_ledger is None
            or request.metadata.get("repair_incident_id")
        ):
            return request
        dataset = _PRODUCTION_STAGE_DATASETS.get(operation)
        if dataset is None:
            return request
        base_identity = PartitionIdentity(
            "research_os", dataset, request.partition_key
        )
        selector = getattr(self.production_ledger, "get_retry_authority", None)
        if not callable(selector):
            return request
        authority = selector(base_identity)
        if authority is None:
            return request
        metadata = {
            key: value
            for key, value in dict(request.metadata).items()
            if key not in _REPAIR_METADATA_KEYS
        }
        metadata.update(
            {
                "repair_scope_key": authority.scope_key,
                "repair_fingerprint": authority.repair_fingerprint,
                "repair_generation": authority.identity.generation,
                "repair_parent_partition_run_id": (
                    authority.parent_partition_run_id
                ),
                "repair_authority_id": authority.authority_id,
            }
        )
        # Make the selected upstream the request's own operation so generic
        # repair metadata is retained by semantic identity construction.
        return replace(request, operation=operation, metadata=metadata)

    def _dependency(
        self, request: OperationRequest, operation: OperationName, *, allow_skipped: bool = False
    ) -> OperationResult:
        dependency_request = self._authoritative_dependency_request(
            request, operation
        )
        result = self._read_stage(dependency_request, operation)
        if result is None:
            raise OrchestrationFailure(
                f"{request.operation.value} requires persisted {operation.value} state"
            )
        allowed = {"completed", *(('skipped',) if allow_skipped else ())}
        if result.status not in allowed:
            raise OrchestrationFailure(
                f"upstream {operation.value} is {result.status}: {result.summary}"
            )
        if operation is OperationName.SOURCE_SYNC:
            self._hydrate_cached_bronze_files(result)
        elif operation is OperationName.SOURCE_RECONCILIATION:
            self._hydrate_cached_silver_files(result)
        return result

    def _restore_cached_manifest_file(
        self,
        *,
        snapshot_id: str,
        tier: SnapshotTier,
        raw_target: Any,
        archived_evidence: Any,
    ) -> None:
        if self.object_store_archive is None:
            raise OrchestrationFailure(
                f"{tier.value.title()} local cache is missing and no immutable object-store archive is configured"
            )
        target_value = str(raw_target or "").strip()
        if isinstance(archived_evidence, Mapping):
            object_uri = str(archived_evidence.get("uri") or "").strip()
        else:
            object_uri = str(archived_evidence or "").strip()
        if not target_value or not object_uri:
            raise OrchestrationFailure(
                f"{tier.value.title()} cache recovery evidence is incomplete"
            )
        snapshot_record = self.catalog.get_snapshot(snapshot_id)
        if snapshot_record is None:
            raise OrchestrationFailure(
                f"missing registered {tier.value.title()} snapshot for cache recovery: {snapshot_id}"
            )
        reference = snapshot_record.reference
        if reference.tier is not tier:
            raise OrchestrationFailure(
                f"cache recovery snapshot is not {tier.value.title()}: {snapshot_id}"
            )
        root = self.settings.lake_root.resolve()
        target = Path(target_value).resolve()
        try:
            relative = target.relative_to(root).as_posix()
        except ValueError as exc:
            raise OrchestrationFailure(
                f"{tier.value.title()} cache target escapes the configured lake root: {target}"
            ) from exc
        matches = [
            entry
            for entry in tuple(reference.manifest.get("files") or ())
            if isinstance(entry, Mapping)
            and str(entry.get("path") or "") == relative
        ]
        if len(matches) != 1:
            raise OrchestrationFailure(
                f"{tier.value.title()} cache file is not uniquely declared by snapshot {snapshot_id}: {relative}"
            )
        entry = matches[0]
        expected_sha256 = str(entry.get("sha256") or "")
        expected_size = int(entry.get("size_bytes", -1))
        if isinstance(archived_evidence, Mapping):
            declared_sha = str(archived_evidence.get("sha256") or "")
            declared_size = archived_evidence.get("size_bytes")
            if declared_sha and declared_sha != expected_sha256:
                raise OrchestrationFailure(
                    f"archived {tier.value.title()} digest disagrees with its snapshot"
                )
            if declared_size is not None and int(declared_size) != expected_size:
                raise OrchestrationFailure(
                    f"archived {tier.value.title()} size disagrees with its snapshot"
                )
        try:
            restored = self.object_store_archive.restore_file(
                object_uri,
                target,
                expected_sha256=expected_sha256,
                expected_size_bytes=expected_size,
            )
        except Exception as exc:
            raise OrchestrationFailure(
                f"immutable {tier.value.title()} cache recovery failed for {relative}: {type(exc).__name__}"
            ) from exc
        if restored.sha256 != expected_sha256:
            raise OrchestrationFailure(
                f"restored {tier.value.title()} digest disagrees with snapshot {snapshot_id}"
            )

    def _hydrate_cached_silver_files(self, result: OperationResult) -> None:
        snapshot_id = str(result.outputs.get("silver_snapshot_id") or "").strip()
        for path_field, object_field in (
            ("silver_path", "silver_object"),
            ("audit_path", "silver_audit_object"),
        ):
            raw_path = str(result.outputs.get(path_field) or "").strip()
            if raw_path and Path(raw_path).is_file():
                continue
            self._restore_cached_manifest_file(
                snapshot_id=snapshot_id,
                tier=SnapshotTier.SILVER,
                raw_target=raw_path,
                archived_evidence=result.outputs.get(object_field),
            )

    def _hydrate_historical_silver_cache(self) -> None:
        """Hydrate every succeeded Silver partition needed by full-history Gold.

        Gold construction scans the accepted parent closure, not just today's
        upstream result.  A machine or disk migration may therefore leave an
        old Silver cache path absent even though its PostgreSQL and MinIO facts
        are intact.  Restore those exact files from the partition ledger before
        catalog discovery; never refetch or silently omit the parent.
        """

        if self.production_ledger is None:
            return
        after_partition_key: str | None = None
        while True:
            records = self.production_ledger.list_partitions(
                statuses=(PartitionStatus.SUCCEEDED,),
                source_id="research_os",
                dataset="stage_silver",
                after_partition_key=after_partition_key,
                limit=10_000,
            )
            if not records:
                return
            for record in records:
                payload = record.details.get("operation_result")
                if not isinstance(payload, Mapping):
                    raise OrchestrationFailure(
                        "succeeded Silver partition lacks an authoritative operation result"
                    )
                outputs = payload.get("outputs")
                if (
                    str(payload.get("operation") or "")
                    != OperationName.SOURCE_RECONCILIATION.value
                    or str(payload.get("status") or "") != "completed"
                    or not isinstance(outputs, Mapping)
                ):
                    raise OrchestrationFailure(
                        "succeeded Silver partition carries invalid terminal evidence"
                    )
                self._hydrate_cached_silver_files(
                    OperationResult(
                        operation=OperationName.SOURCE_RECONCILIATION,
                        status="completed",
                        summary=str(payload.get("summary") or "persisted Silver"),
                        outputs=dict(outputs),
                    )
                )
            latest = records[-1].identity.partition_key
            if len(records) < 10_000:
                return
            if latest == after_partition_key:
                raise OrchestrationFailure(
                    "Silver cache recovery pagination did not advance"
                )
            after_partition_key = latest

    def _hydrate_cached_bronze_files(self, result: OperationResult) -> None:
        """Restore missing Bronze cache files from immutable MinIO evidence.

        The PostgreSQL run/partition ledger is the authority for the archived
        object URI, while the registered immutable Bronze manifest supplies
        the expected hash and byte count.  Neither value is accepted from a
        runtime caller.  This keeps a deleted local cache recoverable without
        repeating an already successful vendor request.
        """

        raw_sources = result.outputs.get("sources")
        if not isinstance(raw_sources, (list, tuple)):
            raise OrchestrationFailure(
                "persisted source result lacks authoritative source outputs"
            )
        root = self.settings.lake_root.resolve()
        for index, raw_source in enumerate(raw_sources):
            if not isinstance(raw_source, Mapping):
                raise OrchestrationFailure(
                    f"persisted source output {index} is not an object"
                )
            missing_fields = [
                field_name
                for field_name in ("data_path", "metadata_path")
                if not Path(str(raw_source.get(field_name) or "")).is_file()
            ]
            if not missing_fields:
                continue
            if self.object_store_archive is None:
                raise OrchestrationFailure(
                    "Bronze local cache is missing and no immutable object-store archive is configured"
                )
            snapshot_id = str(raw_source.get("bronze_snapshot_id") or "").strip()
            snapshot_record = self.catalog.get_snapshot(snapshot_id)
            if snapshot_record is None:
                raise OrchestrationFailure(
                    f"missing registered Bronze snapshot for cache recovery: {snapshot_id}"
                )
            reference = snapshot_record.reference
            if reference.tier is not SnapshotTier.BRONZE:
                raise OrchestrationFailure(
                    f"cache recovery snapshot is not Bronze: {snapshot_id}"
                )
            manifest_files = tuple(reference.manifest.get("files") or ())
            for path_field in missing_fields:
                uri_field = (
                    "data_object_uri"
                    if path_field == "data_path"
                    else "metadata_object_uri"
                )
                raw_target = str(raw_source.get(path_field) or "").strip()
                object_uri = str(raw_source.get(uri_field) or "").strip()
                if not raw_target or not object_uri:
                    raise OrchestrationFailure(
                        f"Bronze cache recovery evidence is incomplete: {path_field}/{uri_field}"
                    )
                target = Path(raw_target).resolve()
                try:
                    relative = target.relative_to(root).as_posix()
                except ValueError as exc:
                    raise OrchestrationFailure(
                        f"Bronze cache target escapes the configured lake root: {target}"
                    ) from exc
                matches = [
                    entry
                    for entry in manifest_files
                    if isinstance(entry, Mapping)
                    and str(entry.get("path") or "") == relative
                ]
                if len(matches) != 1:
                    raise OrchestrationFailure(
                        f"Bronze cache file is not uniquely declared by snapshot {snapshot_id}: {relative}"
                    )
                entry = matches[0]
                try:
                    restored = self.object_store_archive.restore_file(
                        object_uri,
                        target,
                        expected_sha256=str(entry["sha256"]),
                        expected_size_bytes=int(entry["size_bytes"]),
                    )
                except Exception as exc:
                    raise OrchestrationFailure(
                        f"immutable Bronze cache recovery failed for {relative}: {type(exc).__name__}"
                    ) from exc
                if restored.sha256 != str(entry["sha256"]):
                    raise OrchestrationFailure(
                        f"restored Bronze digest disagrees with snapshot {snapshot_id}"
                    )

    def _production_stage_identity(
        self, request: OperationRequest
    ) -> PartitionIdentity | None:
        dataset = _PRODUCTION_STAGE_DATASETS.get(request.operation)
        if self.production_ledger is None or dataset is None:
            return None
        generation = str(
            request.metadata.get("repair_generation") or "base"
        ).strip()
        return PartitionIdentity(
            source_id="research_os",
            dataset=dataset,
            partition_key=request.partition_key,
            generation=generation,
        )

    def _server_repair_fingerprint(
        self,
        request: OperationRequest,
        *,
        incident: IncidentRecord | None = None,
    ) -> str:
        return content_fingerprint(
            {
                "configuration_hash": self._configuration_hash,
                "environment_hashes": _jsonable(
                    getattr(self, "_environment_hashes", {})
                ),
                "cycle": request.cycle.value,
                "partition_key": request.partition_key,
                "incident_id": None if incident is None else incident.incident_id,
                "incident_hash": None if incident is None else incident.incident_hash,
            },
            domain="factor-lab/research-os/v1/server-repair-fingerprint",
        )

    @staticmethod
    def _has_repair_metadata(request: OperationRequest) -> bool:
        return any(key in request.metadata for key in _REPAIR_METADATA_KEYS)

    @staticmethod
    def _reject_external_repair_metadata(request: OperationRequest) -> None:
        if ApplicationServices._has_repair_metadata(request):
            raise OrchestrationFailure(
                "repair authority metadata is server-owned"
            )

    def _prepare_generic_retry_request(
        self, request: OperationRequest, *, now: datetime
    ) -> OperationRequest:
        if self.production_ledger is None or self._has_repair_metadata(request):
            return request
        dataset = _PRODUCTION_STAGE_DATASETS.get(request.operation)
        if dataset is None:
            return request
        base_identity = PartitionIdentity(
            "research_os", dataset, request.partition_key
        )
        base = self.production_ledger.get_partition(base_identity)
        if base is None or base.status not in {
            PartitionStatus.FAILED,
            PartitionStatus.DISPUTED,
            PartitionStatus.QUARANTINED,
        }:
            return request
        repair_fingerprint = self._server_repair_fingerprint(request)
        authority = self.production_ledger.reserve_retry_successor(
            base_identity,
            repair_fingerprint=repair_fingerprint,
            created_at=now,
            details={
                "cycle": request.cycle.value,
                "operation": request.operation.value,
                "dagster_run_id": request.run_id,
                "authority_kind": "generic_terminal_retry",
            },
        )
        return replace(
            request,
            metadata={
                **dict(request.metadata),
                "repair_scope_key": authority.scope_key,
                "repair_fingerprint": authority.repair_fingerprint,
                "repair_generation": authority.identity.generation,
                "repair_parent_partition_run_id": (
                    authority.parent_partition_run_id
                ),
                "repair_authority_id": authority.authority_id,
            },
        )

    def _claim_production_stage(
        self,
        request: OperationRequest,
        *,
        input_hash: str,
        now: datetime,
    ) -> tuple[PartitionLease | None, OperationResult | None]:
        identity = self._production_stage_identity(request)
        if identity is None:
            return None, None
        assert self.production_ledger is not None
        record = self.production_ledger.ensure_partition(
            identity,
            created_at=now,
            input_hash=input_hash,
            details={
                "cycle": request.cycle.value,
                "operation": request.operation.value,
                "dagster_run_id": request.run_id,
                **{
                    key: request.metadata[key]
                    for key in sorted(_REPAIR_METADATA_KEYS)
                    if key in request.metadata
                },
            },
        )
        if record.status in {
            PartitionStatus.SUCCEEDED,
            PartitionStatus.DISPUTED,
            PartitionStatus.QUARANTINED,
        }:
            payload = record.details.get("operation_result")
            if not isinstance(payload, Mapping):
                raise OrchestrationFailure(
                    "terminal production partition lacks its operation result"
                )
            result = _result_from_dict(payload)
            expected_hash = content_fingerprint(
                result.to_dict(),
                domain="factor-lab/research-os/v1/production-operation-result",
            )
            if record.output_hash != expected_hash:
                raise OrchestrationFailure(
                    "production partition result differs from its immutable output hash"
                )
            return None, result
        lease = self.production_ledger.claim(
            identity=identity,
            owner=f"appsvc-{_safe_name(request.run_id)}",
            now=now,
            lease_for=timedelta(hours=1),
        )
        if lease is None:
            refreshed = self.production_ledger.get_partition(identity)
            state = "missing" if refreshed is None else refreshed.status.value
            raise OrchestrationFailure(
                f"production partition stage is not claimable (status={state})"
            )
        return lease, None

    def _finish_production_stage(
        self,
        request: OperationRequest,
        lease: PartitionLease | None,
        result: OperationResult,
    ) -> None:
        if lease is None:
            return
        assert self.production_ledger is not None
        if result.status in {"completed", "skipped"}:
            status = PartitionStatus.SUCCEEDED
        elif result.status == "blocked":
            status = PartitionStatus.QUARANTINED
        else:
            status = PartitionStatus.FAILED
        result_payload = result.to_dict()
        output_hash = content_fingerprint(
            result_payload,
            domain="factor-lab/research-os/v1/production-operation-result",
        )
        snapshot_id = next(
            (
                str(result.outputs[key])
                for key in (
                    "snapshot_id",
                    "silver_snapshot_id",
                    "gold_snapshot_id",
                )
                if result.outputs.get(key)
            ),
            None,
        )
        self.production_ledger.finish(
            lease,
            status=status,
            completed_at=self._now(),
            run_id=self._operation_run_id(request),
            output_snapshot_id=snapshot_id,
            output_hash=(output_hash if status is PartitionStatus.SUCCEEDED else None),
            details={
                "cycle": request.cycle.value,
                "operation": request.operation.value,
                "dagster_run_id": request.run_id,
                "operation_result": result_payload,
                **{
                    key: request.metadata[key]
                    for key in sorted(_REPAIR_METADATA_KEYS)
                    if key in request.metadata
                },
            },
            error_code=(
                None
                if status is PartitionStatus.SUCCEEDED
                else f"{request.operation.value}_{result.status}"
            ),
            error=(None if status is PartitionStatus.SUCCEEDED else result.summary),
        )

    def _require_production_operation(
        self, operation: ProductionOperation
    ) -> None:
        if not self._is_production_runtime():
            return
        evidence = self.production_config_evidence
        if evidence is None:
            raise OrchestrationFailure(
                "production operation lacks validated configuration evidence"
            )
        if operation is ProductionOperation.FORMAL_FORWARD_ACTIVATION:
            blockers = self._runtime_readiness_blockers()
            if blockers:
                raise OrchestrationFailure(
                    f"production operation {operation.value} is blocked: "
                    + ",".join(blockers)
                )
            return
        admission = admit_production_operation(evidence, operation)
        if not admission.allowed:
            raise OrchestrationFailure(
                f"production operation {operation.value} is blocked: "
                + ",".join(admission.blockers)
            )

    def execute(self, request: OperationRequest) -> OperationResult:
        self._reject_external_repair_metadata(request)
        if request.operation in {
            OperationName.SOURCE_SYNC,
            OperationName.SOURCE_RECONCILIATION,
            OperationName.DATA_QUALITY_GATE,
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
            OperationName.SHADOW_NAV_STEP,
        }:
            # Ordinary schedules are formal-forward work.  Request metadata is
            # intentionally ignored, so an operator cannot relabel a blocked
            # run as a canary or historical evidence class.
            self._require_production_operation(
                ProductionOperation.FORMAL_FORWARD_ACTIVATION
            )
        return self._execute_admitted(request)

    def execute_daily_failure_settlement(
        self, request: OperationRequest
    ) -> OperationResult:
        """Run only the fail-closed shadow settlement without a green gate.

        A readiness failure must not prevent the system from reducing risk.
        The handler accepts no alternate operation and revalidates the typed
        envelope against the durable failed PG stage before freezing anything.
        """

        self._reject_external_repair_metadata(request)
        if (
            request.cycle is not CycleName.DAILY
            or request.operation is not OperationName.SHADOW_NAV_STEP
            or not isinstance(
                request.metadata.get("daily_data_outcome"), Mapping
            )
        ):
            raise OrchestrationFailure(
                "daily failure settlement is limited to a typed shadow outcome"
            )
        return self._execute_admitted(request)

    def execute_authoritative_backfill(
        self, request: OperationRequest
    ) -> OperationResult:
        self._reject_external_repair_metadata(request)
        if request.operation not in {
            OperationName.SOURCE_SYNC,
            OperationName.SOURCE_RECONCILIATION,
            OperationName.DATA_QUALITY_GATE,
            OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
        }:
            raise OrchestrationFailure(
                "authoritative historical backfill is limited to the data chain"
            )
        self._require_production_operation(
            ProductionOperation.AUTHORITATIVE_HISTORICAL_BACKFILL
        )
        return self._execute_admitted(request)

    @staticmethod
    def _repair_cohort_id(
        incident: IncidentRecord, repair_fingerprint: str
    ) -> str:
        return "repaircohort_" + content_fingerprint(
            {
                "incident_id": incident.incident_id,
                "incident_hash": incident.incident_hash,
                "repair_fingerprint": repair_fingerprint,
            },
            domain="factor-lab/research-os/v1/data-incident-repair-cohort",
        )[:64]

    def _incident_repair_fingerprint(
        self,
        request: OperationRequest,
        *,
        incident: IncidentRecord,
    ) -> str:
        """Bind an OPEN repair cohort to its originating runtime provenance.

        Repair authorities are immutable.  Resuming one under a different
        production configuration/environment would otherwise combine Source
        evidence from runtime A with later stages produced by runtime B.  The
        safe recovery policy is to stop before reserving or executing another
        stage; the original authority remains untouched and can be resumed by
        the exact originating runtime.
        """

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "data incident repair provenance requires the production ledger"
            )
        expected = self._server_repair_fingerprint(
            request, incident=incident
        )
        source_authority = self.production_ledger.get_repair_authority(
            incident.incident_id, "stage_source"
        )
        if source_authority is None:
            return expected
        if source_authority.repair_fingerprint != expected:
            raise OrchestrationFailure(
                "OPEN data incident repair configuration drift: the immutable "
                "cohort must be resumed under its originating production "
                "configuration and environment"
            )
        return source_authority.repair_fingerprint

    def pending_data_incident_repairs(self) -> tuple[Mapping[str, Any], ...]:
        """Return server-derived OPEN repair candidates for the Dagster sensor."""

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "data incident repair coordination requires the production ledger"
            )
        candidates: list[Mapping[str, Any]] = []
        incidents = self.production_ledger.iter_incidents(
            status=IncidentStatus.OPEN
        )
        try:
            for incident in incidents:
                domain_incident_id = str(
                    incident.payload.get("domain_incident_id") or ""
                ).strip()
                if not re.fullmatch(r"dinc_[0-9a-f]{64}", domain_incident_id):
                    continue
                controls = self.production_ledger.incident_controls.get(
                    incident.incident_id
                )
                if (
                    controls is None
                    or controls.status
                    is not IncidentControlActionStatus.SUCCEEDED
                ):
                    continue
                seed = OperationRequest(
                    operation=OperationName.SOURCE_SYNC,
                    cycle=CycleName.DAILY,
                    partition_key=incident.partition_key,
                    run_id="repair-coordinator",
                )
                repair_fingerprint = self._incident_repair_fingerprint(
                    seed, incident=incident
                )
                candidates.append(
                    {
                        "incident_id": incident.incident_id,
                        "incident_hash": incident.incident_hash,
                        "partition_key": incident.partition_key,
                        "occurred_at": incident.occurred_at.isoformat(),
                        "repair_fingerprint": repair_fingerprint,
                        "repair_cohort_id": self._repair_cohort_id(
                            incident, repair_fingerprint
                        ),
                    }
                )
        finally:
            close = getattr(incidents, "close", None)
            if close is not None:
                close()
        return tuple(
            sorted(
                candidates,
                key=lambda item: (
                    str(item["occurred_at"]),
                    str(item["incident_id"]),
                ),
            )
        )

    def execute_data_incident_repair(
        self,
        request: OperationRequest,
        *,
        incident_id: str,
    ) -> OperationResult:
        """Execute one server-selected stage of an OPEN incident repair chain."""

        self._reject_external_repair_metadata(request)
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "data incident repair requires the production ledger"
            )
        dataset = _PRODUCTION_STAGE_DATASETS.get(request.operation)
        if (
            request.cycle is not CycleName.DAILY
            or dataset is None
            or request.operation
            not in {
                OperationName.SOURCE_SYNC,
                OperationName.SOURCE_RECONCILIATION,
                OperationName.DATA_QUALITY_GATE,
                OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
                OperationName.SHADOW_NAV_STEP,
            }
        ):
            raise OrchestrationFailure(
                "data incident repair is limited to the five-stage daily chain"
            )
        incident = self.production_ledger.get_incident(incident_id)
        if incident is None:
            raise OrchestrationFailure("data incident repair authority was not found")
        if (
            incident.status is not IncidentStatus.OPEN
            or incident.partition_key != request.partition_key
        ):
            raise OrchestrationFailure(
                "data incident repair requires its exact OPEN partition authority"
            )
        repair_fingerprint = self._incident_repair_fingerprint(
            request, incident=incident
        )
        # ``repair_fingerprint`` is the durable repair cohort.  Dagster run IDs
        # are individual execution attempts: after a worker/process crash a
        # new Dagster run must be able to resume the same immutable successor
        # chain instead of being permanently rejected by the first attempt ID.
        repair_cohort_id = self._repair_cohort_id(
            incident, repair_fingerprint
        )
        repair_validation_trade_date: str | None = None
        if request.operation is OperationName.SHADOW_NAV_STEP:
            existing_shadow = self.production_ledger.get_repair_authority(
                incident.incident_id, "stage_shadow"
            )
            existing_shadow_record = (
                None
                if existing_shadow is None
                else self.production_ledger.get_partition(
                    existing_shadow.identity
                )
            )
            if (
                existing_shadow_record is not None
                and existing_shadow_record.status
                in {
                    PartitionStatus.PENDING,
                    PartitionStatus.RUNNING,
                    PartitionStatus.SUCCEEDED,
                }
            ):
                repair_validation_trade_date = str(
                    existing_shadow_record.details.get(
                        "repair_validation_trade_date"
                    )
                    or ""
                ).strip()
                try:
                    repair_validation_trade_date = date.fromisoformat(
                        repair_validation_trade_date
                    ).isoformat()
                except ValueError as exc:
                    raise OrchestrationFailure(
                        "active Shadow repair lacks its server-selected validation session"
                    ) from exc
            else:
                validation_session = self._latest_complete_accepted_session()
                previous_validation_session: date | None = None
                if existing_shadow_record is not None:
                    raw_previous_validation = str(
                        existing_shadow_record.details.get(
                            "repair_validation_trade_date"
                        )
                        or ""
                    ).strip()
                    if raw_previous_validation:
                        try:
                            previous_validation_session = date.fromisoformat(
                                raw_previous_validation
                            )
                        except ValueError as exc:
                            raise OrchestrationFailure(
                                "failed Shadow repair has a malformed validation session"
                            ) from exc
                if validation_session < date.fromisoformat(
                    incident.partition_key
                ):
                    raise OrchestrationFailure(
                        "no post-incident accepted session is ready for Shadow validation"
                    )
                if (
                    previous_validation_session is not None
                    and validation_session <= previous_validation_session
                ):
                    raise OrchestrationFailure(
                        "no newer accepted session is ready after the failed Shadow validation"
                    )
                repair_validation_trade_date = validation_session.isoformat()
        authority = self.production_ledger.reserve_repair_successor(
            incident_id=incident.incident_id,
            dataset=dataset,
            repair_fingerprint=repair_fingerprint,
            created_at=self._now(),
            details={
                "cycle": request.cycle.value,
                "operation": request.operation.value,
                "dagster_run_id": request.run_id,
                "repair_cohort_id": repair_cohort_id,
                **(
                    {}
                    if repair_validation_trade_date is None
                    else {
                        "repair_validation_trade_date": (
                            repair_validation_trade_date
                        )
                    }
                ),
                "authority_kind": "data_incident_repair",
            },
        )
        stage_generations: dict[str, str] = {}
        for stage_operation, stage_dataset in _PRODUCTION_STAGE_DATASETS.items():
            selected = self.production_ledger.get_repair_authority(
                incident.incident_id, stage_dataset
            )
            if selected is not None:
                stage_generations[stage_operation.value] = (
                    selected.identity.generation
                )
        repaired_request = replace(
            request,
            metadata={
                **dict(request.metadata),
                "repair_scope_key": authority.scope_key,
                "repair_incident_id": incident.incident_id,
                "repair_fingerprint": authority.repair_fingerprint,
                "repair_generation": authority.identity.generation,
                "repair_parent_partition_run_id": (
                    authority.parent_partition_run_id
                ),
                "repair_authority_id": authority.authority_id,
                "repair_cohort_id": repair_cohort_id,
                "repair_stage_generations": stage_generations,
                **(
                    {}
                    if repair_validation_trade_date is None
                    else {
                        "repair_validation_trade_date": (
                            repair_validation_trade_date
                        )
                    }
                ),
            },
        )
        return self._execute_admitted(repaired_request)

    def execute_engineering_canary(
        self, request: OperationRequest
    ) -> OperationResult:
        self._reject_external_repair_metadata(request)
        if request.operation is not OperationName.SOURCE_SYNC:
            raise OrchestrationFailure(
                "engineering canary authority is limited to source synchronization"
            )
        self._require_production_operation(ProductionOperation.ENGINEERING_CANARY)
        return self._execute_admitted(request)

    def _execute_admitted(self, request: OperationRequest) -> OperationResult:
        started_at = self._now()
        request = self._prepare_generic_retry_request(
            request, now=started_at
        )
        run_id, run_type, input_fingerprint = self._operation_identity(
            request, request.operation
        )
        stage_lease, ledger_cached = self._claim_production_stage(
            request, input_hash=input_fingerprint, now=started_at
        )
        catalog_cached = self._read_stage(request, request.operation)
        if ledger_cached is not None:
            if catalog_cached is not None and catalog_cached != ledger_cached:
                raise OrchestrationFailure(
                    "catalog and production partition ledgers disagree"
                )
            if catalog_cached is None:
                self.catalog.save_run(
                    RunRecord(
                        run_id=run_id,
                        run_type=run_type,
                        status=ledger_cached.status,
                        input_fingerprint=input_fingerprint,
                        started_at=started_at,
                        completed_at=started_at,
                        metadata={
                            "schema_version": APPLICATION_SERVICES_SCHEMA_VERSION,
                            "dagster_run_id": request.run_id,
                            "cycle": request.cycle.value,
                            "partition_key": request.partition_key,
                            "operation": request.operation.value,
                            "configuration_hash": self._configuration_hash,
                            "summary": ledger_cached.summary,
                            "outputs": _jsonable(ledger_cached.outputs),
                            "operation_result": ledger_cached.to_dict(),
                            "recovered_from_partition_ledger": True,
                        },
                        error=(
                            ledger_cached.summary
                            if ledger_cached.status == "failed"
                            else None
                        ),
                    )
                )
            return ledger_cached
        if catalog_cached is not None:
            # Crash recovery: the catalog mutation was durable but the new
            # partition ledger terminal write was not.  Bind the exact cached
            # result instead of repeating domain side effects.
            self._finish_production_stage(request, stage_lease, catalog_cached)
            return catalog_cached
        initial_metadata = {
            "schema_version": APPLICATION_SERVICES_SCHEMA_VERSION,
            "dagster_run_id": request.run_id,
            "cycle": request.cycle.value,
            "partition_key": request.partition_key,
            "operation": request.operation.value,
            "configuration_hash": self._configuration_hash,
            "request_metadata": _jsonable(dict(request.metadata)),
            "summary": "operation claimed by application services",
        }
        claimed_run, claimed = self.catalog.claim_run(
            RunRecord(
                run_id=run_id,
                run_type=run_type,
                status="running",
                input_fingerprint=input_fingerprint,
                started_at=started_at,
                metadata=initial_metadata,
            )
        )
        if not claimed:
            cached = self._read_stage(request, request.operation)
            if cached is not None:
                return cached
            raise OrchestrationFailure(
                f"catalog run {claimed_run.run_id!r} is already running; "
                "refusing duplicate side effects"
            )
        handler = self._handlers[request.operation]
        try:
            result = handler(request)
            if not isinstance(result, OperationResult):
                raise TypeError("operation handler did not return OperationResult")
        except Exception as exc:
            result = OperationResult(
                operation=request.operation,
                status="failed",
                summary=f"{type(exc).__name__}: {exc}",
                outputs={"error_type": type(exc).__name__},
            )
        try:
            self._finish_production_stage(request, stage_lease, result)
        except Exception as exc:
            result = OperationResult(
                operation=request.operation,
                status="failed",
                summary=f"production partition terminalization failed: {type(exc).__name__}: {exc}",
                outputs={"error_type": type(exc).__name__},
            )
        self.catalog.save_run(
            RunRecord(
                run_id=run_id,
                run_type=run_type,
                status=result.status,
                input_fingerprint=input_fingerprint,
                started_at=started_at,
                completed_at=self._now(),
                metadata={
                    **initial_metadata,
                    "summary": result.summary,
                    "outputs": _jsonable(result.outputs),
                    "operation_result": result.to_dict(),
                },
                error=(result.summary if result.status == "failed" else None),
            )
        )
        return result

    def _manifest(
        self,
        *,
        paths: Iterable[str | Path],
        tier: str,
        as_of: Any,
        parent_snapshot_ids: Sequence[str],
        quality_report: QualityReport | Mapping[str, Any],
        trust_labels: Sequence[str],
        trading_calendar: Mapping[str, Any] | None = None,
    ) -> tuple[ImmutableSnapshotManifest, Path, ArchivedObject | None]:
        manifest = build_immutable_snapshot_manifest(
            paths,
            base_dir=self.settings.lake_root,
            tier=tier,
            as_of=as_of,
            parent_snapshot_ids=parent_snapshot_ids,
            environment_hashes=self._environment_hashes,
            quality_report=quality_report,
            trust_labels=trust_labels,
            trading_calendar=trading_calendar,
        )
        manifest_path = publish_snapshot_manifest(
            self.settings.snapshot_root,
            manifest,
            base_dir=self.settings.lake_root,
        )
        archived = (
            None
            if self.object_store_archive is None
            else self.object_store_archive.archive_file(
                manifest_path,
                logical_path=f"manifests/{tier}/{manifest.snapshot_id}",
            )
        )
        return manifest, manifest_path, archived

    def _source_sync(self, request: OperationRequest) -> OperationResult:
        daily = self._section("daily", request)
        sources = daily.get("sources")
        if not isinstance(sources, list) or not sources:
            raise ServiceNotConfigured("daily.sources must contain at least one real adapter")
        outputs: list[dict[str, Any]] = []
        snapshot_ids: list[str] = []
        non_blocking_samples: list[dict[str, Any]] = []
        degraded_sources: list[dict[str, Any]] = []
        for index, raw in enumerate(sources):
            if not isinstance(raw, Mapping):
                raise ValueError(f"daily.sources[{index}] must be an object")
            source = self._bind_source_transport_authority(raw)
            non_blocking_sample = bool(
                source.get("non_blocking") is True
                and source.get("evidence_role") == "non_blocking_sample"
            )
            if source.get("source") == "local_file":
                if not source.get("root"):
                    raise ServiceNotConfigured(f"daily.sources[{index}].root is required")
                source["root"] = str(self._input_path(source["root"]))
            source_lease: PartitionLease | None = None
            source_identity: PartitionIdentity | None = None
            if self.production_ledger is not None:
                request_payload = source.get("request") or {}
                if not isinstance(request_payload, Mapping):
                    raise ServiceNotConfigured(
                        f"daily.sources[{index}].request must be an object"
                    )
                cadence = source.get("partition_cadence")
                if not isinstance(cadence, Mapping):
                    raise ServiceNotConfigured(
                        f"daily.sources[{index}] requires partition_cadence in production"
                    )
                cadence_kind = str(cadence.get("kind") or "").strip()
                if cadence_kind not in {
                    "trading_session",
                    "static_snapshot",
                    "event_date",
                }:
                    raise ServiceNotConfigured(
                        f"daily.sources[{index}] has unsupported partition cadence {cadence_kind!r}"
                    )
                source_name = str(
                    cadence.get("ledger_identity")
                    or source.get("profile_name")
                    or source.get("source")
                    or f"source_{index}"
                )
                ledger_partition_key = request.partition_key
                if cadence_kind == "static_snapshot":
                    if cadence.get("refresh_policy") != "bootstrap_or_explicit":
                        raise ServiceNotConfigured(
                            "static source cadence requires refresh_policy=bootstrap_or_explicit"
                        )
                    explicit_refresh = str(
                        request.metadata.get("static_refresh_id") or ""
                    ).strip()
                    ledger_partition_key = (
                        f"refresh:{explicit_refresh}" if explicit_refresh else "bootstrap"
                    )
                source_identity = PartitionIdentity(
                    source_id=_safe_name(source_name),
                    dataset=str(request_payload.get("dataset") or ""),
                    partition_key=ledger_partition_key,
                )
                source_input_hash = content_fingerprint(
                    source,
                    domain="factor-lab/research-os/v1/source-partition-input",
                )
                base_source_identity = source_identity
                source_record = self.production_ledger.get_partition(
                    base_source_identity
                )
                if source_record is None:
                    source_record = self.production_ledger.ensure_partition(
                        base_source_identity,
                        created_at=self._now(),
                        input_hash=source_input_hash,
                        details={
                            "source_config_index": index,
                            "dagster_run_id": request.run_id,
                        },
                    )
                repair_scope = str(
                    request.metadata.get("repair_scope_key") or ""
                ).strip()
                repair_incident_id = str(
                    request.metadata.get("repair_incident_id") or ""
                ).strip()
                if repair_scope and (
                    repair_incident_id
                    or source_record.status
                    in {
                        PartitionStatus.FAILED,
                        PartitionStatus.DISPUTED,
                        PartitionStatus.QUARANTINED,
                    }
                ):
                    child_authority = self.production_ledger.reserve_retry_successor(
                        base_source_identity,
                        repair_fingerprint=str(
                            request.metadata["repair_fingerprint"]
                        ),
                        created_at=self._now(),
                        input_hash=source_input_hash,
                        details={
                            "source_config_index": index,
                            "dagster_run_id": request.run_id,
                            "parent_stage_authority_id": request.metadata.get(
                                "repair_authority_id"
                            ),
                        },
                        scope_key=repair_scope,
                        incident_id=(repair_incident_id or None),
                        allow_succeeded_base=bool(repair_incident_id),
                    )
                    source_identity = child_authority.identity
                    source_record = self.production_ledger.ensure_partition(
                        source_identity,
                        created_at=self._now(),
                        input_hash=source_input_hash,
                    )
                elif source_record.input_hash not in {None, source_input_hash}:
                    raise OrchestrationFailure(
                        "source partition input changed without repair authority"
                    )
                elif source_record.input_hash is None:
                    source_record = self.production_ledger.ensure_partition(
                        source_identity,
                        created_at=self._now(),
                        input_hash=source_input_hash,
                    )
                if source_record.status is PartitionStatus.SUCCEEDED:
                    cached_output = source_record.details.get("source_output")
                    if not isinstance(cached_output, Mapping):
                        raise OrchestrationFailure(
                            "successful source partition lacks immutable source_output"
                        )
                    row = dict(cached_output)
                    if non_blocking_sample:
                        non_blocking_samples.append(row)
                    else:
                        outputs.append(row)
                        snapshot_ids.append(str(row["bronze_snapshot_id"]))
                    continue
                if source_record.status in {
                    PartitionStatus.DISPUTED,
                    PartitionStatus.QUARANTINED,
                }:
                    raise OrchestrationFailure(
                        f"source partition is terminal {source_record.status.value}"
                    )
                source_lease = self.production_ledger.claim(
                    identity=source_identity,
                    owner=f"source-{_safe_name(request.run_id)}-{index}",
                    now=self._now(),
                    lease_for=timedelta(hours=1),
                )
                if source_lease is None:
                    raise OrchestrationFailure(
                        f"source partition {source_identity.partition_run_id} is already leased"
                    )
            try:
                result = sync_bronze(
                    source,
                    lake_root=self.settings.lake_root,
                    env=self.env,
                    object_store_archive=self.object_store_archive,
                    classify_observation_failures=non_blocking_sample,
                )
                manifest, manifest_path, manifest_object = self._manifest(
                    paths=(result.data_path, result.metadata_path),
                    tier="bronze",
                    as_of=result.ingested_at,
                    parent_snapshot_ids=(),
                    quality_report={"status": "pass"},
                    trust_labels=(
                        "raw_vendor_response",
                        f"source:{result.source_id}",
                        *(
                            ("gold_promotion_forbidden", "non_blocking_sample")
                            if non_blocking_sample
                            else ()
                        ),
                    ),
                )
                self.catalog.register_snapshot(
                    manifest.to_snapshot_ref(
                        uri=(
                            manifest_object.uri
                            if manifest_object is not None
                            else manifest_path.resolve().as_uri()
                        )
                    )
                )
                output = {
                    **result.to_dict(),
                    "source_config_index": index,
                    # A non-blocking sample may be retained as accepted Bronze
                    # evidence when it succeeds, but it is never a formal
                    # reconciliation parent.  Its contract is observational,
                    # not an alternate authority for production fields.
                    "reconciliation_eligible": not non_blocking_sample,
                    "bronze_snapshot_id": manifest.snapshot_id,
                    "bronze_manifest_path": str(manifest_path.resolve()),
                    "bronze_manifest_object": (
                        None if manifest_object is None else manifest_object.to_dict()
                    ),
                }
                if non_blocking_sample and self.production_ledger is not None:
                    if source_identity is None:
                        raise OrchestrationFailure(
                            "non-blocking source acceptance requires a durable partition identity"
                        )
                    contract_payload = source.get("contract")
                    if not isinstance(contract_payload, Mapping):
                        raise OrchestrationFailure(
                            "non-blocking source acceptance lacks its reviewed contract"
                        )
                    contract = dataset_contract_from_mapping(contract_payload)
                    contract_hash = content_fingerprint(
                        contract_payload,
                        domain=(
                            "factor-lab/research-os/v1/"
                            "non-blocking-source-contract"
                        ),
                    )
                    capability_evidence = {
                        "schema_version": (
                            "research-os/non-blocking-source-capability/v1"
                        ),
                        "decision": "accepted_sample",
                        "blocking": False,
                        "evidence_role": "non_blocking_sample",
                        "source_id": source_identity.source_id,
                        "provider_source": str(source.get("source") or ""),
                        "dataset": source_identity.dataset,
                        "partition_key": request.partition_key,
                        "partition_run_id": source_identity.partition_run_id,
                        "bronze_snapshot_id": manifest.snapshot_id,
                        "accepted_bronze_published": True,
                        "reconciliation_eligible": False,
                        "contract_hash": contract_hash,
                    }
                    probe_hash = content_fingerprint(
                        capability_evidence,
                        domain=(
                            "factor-lab/research-os/v1/"
                            "non-blocking-source-capability-probe"
                        ),
                    )
                    capability = self.production_ledger.upsert_capability(
                        CapabilityRecord(
                            source_id=source_identity.source_id,
                            dataset=source_identity.dataset,
                            status=CapabilityStatus.ACCEPTED,
                            contract_hash=contract_hash,
                            probe_hash=probe_hash,
                            fields=tuple(contract.field_map),
                            detail=canonical_json(capability_evidence),
                            probed_at=self._now(),
                        )
                    )
                    output.update(
                        capability_status=capability.status.value,
                        capability_probe_hash=capability.probe_hash,
                    )
                if source_lease is not None:
                    assert self.production_ledger is not None
                    self.production_ledger.finish(
                        source_lease,
                        status=PartitionStatus.SUCCEEDED,
                        completed_at=self._now(),
                        run_id=self._operation_run_id(request),
                        output_snapshot_id=manifest.snapshot_id,
                        output_hash=content_fingerprint(
                            output,
                            domain="factor-lab/research-os/v1/source-partition-output",
                        ),
                        vendor_revision=result.vendor_revision,
                        details={
                            "source_config_index": index,
                            "dagster_run_id": request.run_id,
                            "source_output": output,
                            **{
                                key: request.metadata[key]
                                for key in sorted(_REPAIR_METADATA_KEYS)
                                if key in request.metadata
                            },
                        },
                    )
                if non_blocking_sample:
                    non_blocking_samples.append(output)
                else:
                    outputs.append(output)
                    snapshot_ids.append(manifest.snapshot_id)
            except Exception as exc:
                accepted_non_blocking_degradation = bool(
                    non_blocking_sample
                    and isinstance(exc, BronzeObservationError)
                )
                failure_persisted = False
                if source_lease is not None:
                    assert self.production_ledger is not None
                    try:
                        self.production_ledger.finish(
                            source_lease,
                            status=PartitionStatus.FAILED,
                            completed_at=self._now(),
                            run_id=self._operation_run_id(request),
                            error_code="source_sync_failed",
                            error=(
                                "BronzeObservationError: optional source "
                                "probe/fetch/contract failed"
                                if accepted_non_blocking_degradation
                                else f"{type(exc).__name__}: {exc}"
                            ),
                            details={
                                "source_config_index": index,
                                "dagster_run_id": request.run_id,
                                **{
                                    key: request.metadata[key]
                                    for key in sorted(_REPAIR_METADATA_KEYS)
                                    if key in request.metadata
                                },
                            },
                        )
                        failure_persisted = True
                    except ProductionLedgerError:
                        if non_blocking_sample:
                            raise OrchestrationFailure(
                                "non-blocking source failure could not be persisted"
                            ) from None
                if accepted_non_blocking_degradation:
                    if (
                        self.production_ledger is None
                        or source_identity is None
                        or not failure_persisted
                    ):
                        raise OrchestrationFailure(
                            "non-blocking source degradation requires a durable partition ledger"
                        ) from None
                    dataset = source_identity.dataset
                    source_id = source_identity.source_id
                    contract_payload = source.get("contract")
                    if not isinstance(contract_payload, Mapping):
                        raise OrchestrationFailure(
                            "non-blocking source degradation lacks its reviewed contract"
                        ) from None
                    contract = dataset_contract_from_mapping(contract_payload)
                    contract_hash = content_fingerprint(
                        contract_payload,
                        domain=(
                            "factor-lab/research-os/v1/"
                            "non-blocking-source-contract"
                        ),
                    )
                    occurred_at = self._now()
                    evidence = {
                        "schema_version": (
                            "research-os/non-blocking-source-degradation/v1"
                        ),
                        "decision": "degraded",
                        "blocking": False,
                        "evidence_role": "non_blocking_sample",
                        "source_id": source_id,
                        "provider_source": str(source.get("source") or ""),
                        "dataset": dataset,
                        "partition_key": request.partition_key,
                        "partition_run_id": source_identity.partition_run_id,
                        "failure_type": exc.failure_type,
                        "accepted_bronze_published": False,
                        "reconciliation_eligible": False,
                        "contract_hash": contract_hash,
                    }
                    probe_hash = content_fingerprint(
                        evidence,
                        domain=(
                            "factor-lab/research-os/v1/"
                            "non-blocking-source-degradation-probe"
                        ),
                    )
                    capability = self.production_ledger.upsert_capability(
                        CapabilityRecord(
                            source_id=source_id,
                            dataset=dataset,
                            status=CapabilityStatus.DEGRADED,
                            contract_hash=contract_hash,
                            probe_hash=probe_hash,
                            fields=tuple(contract.field_map),
                            detail=canonical_json(evidence),
                            probed_at=occurred_at,
                        )
                    )
                    incident = self.production_ledger.record_incident(
                        partition_key=request.partition_key,
                        stage=IncidentStage.SOURCE,
                        error_code="non_blocking_source_degraded",
                        message=(
                            f"optional source {source_id}/{dataset} degraded; "
                            "formal source evidence was not published"
                        ),
                        occurred_at=occurred_at,
                        partition_run_id=source_identity.partition_run_id,
                        source_ids=(source_id,),
                        evidence_hashes=(contract_hash, probe_hash),
                        payload=evidence,
                    )
                    resolved = self.production_ledger.resolve_incident(
                        incident.incident_id,
                        resolved_at=occurred_at,
                        evidence={
                            "classification": "accepted_non_blocking_degradation",
                            "blocking": False,
                            "capability_probe_hash": capability.probe_hash,
                            "accepted_bronze_published": False,
                            "reconciliation_eligible": False,
                        },
                    )
                    degraded_sources.append(
                        {
                            **evidence,
                            "capability_status": capability.status.value,
                            "capability_probe_hash": capability.probe_hash,
                            "incident_id": resolved.incident_id,
                            "incident_status": resolved.status.value,
                        }
                    )
                    continue
                raise
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=(
                f"persisted {len(outputs)} immutable Bronze source responses"
                + (
                    f"; recorded {len(degraded_sources)} non-blocking degradation(s)"
                    if degraded_sources
                    else ""
                )
            ),
            outputs={
                "sources": outputs,
                "bronze_snapshot_ids": snapshot_ids,
                "non_blocking_samples": non_blocking_samples,
                "degraded_sources": degraded_sources,
            },
        )

    @staticmethod
    def _batch_from_sync(row: Mapping[str, Any]) -> SourceBatch:
        metadata = _read_json(Path(str(row["metadata_path"])))
        data_path = Path(str(row["data_path"]))
        if sha256_path(data_path) != str(metadata.get("data_sha256") or ""):
            raise OrchestrationFailure(f"Bronze hash mismatch: {data_path}")
        contract = dataset_contract_from_mapping(metadata["contract"])
        request_payload = metadata.get("request") or {}
        return SourceBatch(
            source_id=str(metadata["source_id"]),
            source_priority=int(metadata["source_priority"]),
            dataset=str(metadata["dataset"]),
            frame=pd.read_parquet(data_path),
            ingested_at=_parse_aware(metadata["ingested_at"], name="ingested_at"),
            vendor_revision=str(metadata["vendor_revision"]),
            contract=contract,
            request=FetchRequest(
                dataset=str(request_payload["dataset"]),
                parameters=dict(request_payload.get("parameters") or {}),
                fields=tuple(map(str, request_payload.get("fields") or ())),
            ),
            lineage=dict(metadata.get("lineage") or {}),
        )

    @staticmethod
    def _canonicalization(
        payload: Mapping[str, Any], batch: SourceBatch
    ) -> tuple[CanonicalizationSpec, Callable[[pd.Series], Any] | None]:
        canonical = payload.get("canonicalization")
        if not isinstance(canonical, Mapping):
            raise ServiceNotConfigured(
                f"source {batch.source_id}/{batch.dataset} requires canonicalization"
            )
        value_columns = tuple(map(str, canonical.get("value_columns") or ()))
        if not value_columns:
            raise ServiceNotConfigured(
                f"source {batch.source_id}/{batch.dataset} requires explicit value_columns"
            )
        available_column = canonical.get("available_at_column")
        resolver: Callable[[pd.Series], Any] | None = None
        if available_column is None:
            availability = canonical.get("availability")
            if not isinstance(availability, Mapping):
                raise ServiceNotConfigured(
                    "canonicalization requires available_at_column or availability policy"
                )
            if availability.get("mode") != "session_release_time":
                raise ServiceNotConfigured(
                    "only explicit session_release_time availability is supported"
                )
            release_time = time.fromisoformat(str(availability["time"]))
            zone = ZoneInfo(str(availability.get("timezone") or "Asia/Shanghai"))
            lag_days = int(availability.get("lag_days", 0))
            event_column = str(canonical["event_time_column"])

            def resolve(row: pd.Series) -> datetime:
                event_date = pd.Timestamp(row[event_column]).date() + timedelta(days=lag_days)
                return datetime.combine(event_date, release_time, tzinfo=zone).astimezone(
                    timezone.utc
                )

            resolver = resolve
        spec = CanonicalizationSpec(
            entity_columns=tuple(map(str, canonical["entity_columns"])),
            event_time_column=str(canonical["event_time_column"]),
            available_at_column=None if available_column is None else str(available_column),
            vendor_revision_column=(
                None
                if canonical.get("vendor_revision_column") is None
                else str(canonical["vendor_revision_column"])
            ),
            value_columns=value_columns,
        )
        return spec, resolver

    @staticmethod
    def _accepted_wide(accepted: pd.DataFrame) -> pd.DataFrame:
        if accepted.empty:
            raise OrchestrationFailure("reconciliation accepted no observations")
        keys = ["dataset", "entity_key", "event_time"]
        wide = accepted.pivot(index=keys, columns="field", values="value").reset_index()
        wide.columns.name = None
        decoded = accepted[keys].drop_duplicates().copy()
        entity_rows = [json.loads(str(value)) for value in decoded["entity_key"]]
        entity_names = sorted({str(key) for row in entity_rows for key in row})
        reserved = set(wide.columns)
        for name in entity_names:
            target = name if name not in reserved else f"entity_{name}"
            decoded[target] = [row.get(name) for row in entity_rows]
        lineage = (
            accepted.groupby(keys, sort=True)
            .agg(
                available_at=("available_at", "max"),
                ingested_at=("ingested_at", "max"),
                source_evidence_count=("evidence_count", "sum"),
            )
            .reset_index()
        )
        wide = decoded.merge(wide, on=keys, validate="one_to_one").merge(
            lineage, on=keys, validate="one_to_one"
        )
        wide["event_time"] = pd.to_datetime(wide["event_time"], utc=True)
        wide["available_at"] = pd.to_datetime(wide["available_at"], utc=True)
        wide["ingested_at"] = pd.to_datetime(wide["ingested_at"], utc=True)
        return wide.sort_values(keys, kind="stable").reset_index(drop=True).convert_dtypes()

    def _source_reconciliation(self, request: OperationRequest) -> OperationResult:
        sync = self._dependency(request, OperationName.SOURCE_SYNC)
        daily = self._section("daily", request)
        configured_sources = daily["sources"]
        raw_reconciliation_sources = sync.outputs.get("sources")
        if not isinstance(raw_reconciliation_sources, list):
            raise OrchestrationFailure(
                "source synchronization formal sources are malformed"
            )
        reconciliation_sources: list[Mapping[str, Any]] = []
        for row in raw_reconciliation_sources:
            if not isinstance(row, Mapping):
                raise OrchestrationFailure(
                    "source synchronization formal source row is malformed"
                )
            raw_index = row.get("source_config_index")
            if (
                type(raw_index) is not int
                or raw_index < 0
                or raw_index >= len(configured_sources)
                or not isinstance(configured_sources[raw_index], Mapping)
            ):
                raise OrchestrationFailure(
                    "formal source row has no reviewed source configuration"
                )
            source_config = configured_sources[raw_index]
            configured_non_blocking = bool(
                source_config.get("non_blocking") is True
                and source_config.get("evidence_role")
                == "non_blocking_sample"
            )
            if configured_non_blocking:
                # Code upgrades must not reinterpret an older cached SOURCE_SYNC
                # result that pre-dates the non-blocking output split.  Expose
                # the stale cache instead of silently laundering that Bronze
                # sample into an authoritative Silver parent.
                raise OrchestrationFailure(
                    "non-blocking source evidence appeared in formal source outputs"
                )
            if (
                "reconciliation_eligible" in row
                and row.get("reconciliation_eligible") is not True
            ):
                raise OrchestrationFailure(
                    "formal source explicitly forbids reconciliation"
                )
            reconciliation_sources.append(row)
        if not reconciliation_sources:
            raise OrchestrationFailure(
                "source synchronization produced no reconciliation-eligible evidence"
            )
        bronze_parent_ids = tuple(
            str(row["bronze_snapshot_id"])
            for row in reconciliation_sources
        )
        declared_parent_ids = tuple(
            map(str, sync.outputs.get("bronze_snapshot_ids") or ())
        )
        if bronze_parent_ids != declared_parent_ids:
            raise OrchestrationFailure(
                "formal Bronze source outputs and parent closure disagree"
            )
        try:
            assert_snapshot_promotion_allowed(self.catalog, bronze_parent_ids)
        except SnapshotPromotionBlocked as exc:
            raise OrchestrationFailure(
                f"Bronze trust labels block Silver promotion: {exc}"
            ) from exc
        canonical_parts: list[pd.DataFrame] = []
        for row in reconciliation_sources:
            index = int(row["source_config_index"])
            source_config = configured_sources[index]
            batch = self._batch_from_sync(row)
            spec, resolver = self._canonicalization(source_config, batch)
            canonical_parts.append(
                canonicalize_batch(batch, spec, availability_resolver=resolver)
            )
        policies = {
            str(field_name): ComparisonPolicy(
                absolute_tolerance=float(values.get("absolute_tolerance", 0.0)),
                relative_tolerance=float(values.get("relative_tolerance", 0.0)),
                case_sensitive=bool(values.get("case_sensitive", True)),
            )
            for field_name, values in (daily.get("comparison_policies") or {}).items()
        }
        default_values = daily.get("default_comparison_policy") or {}
        reconciled = reconcile_observations(
            pd.concat(canonical_parts, ignore_index=True),
            policies=policies,
            default_policy=ComparisonPolicy(
                absolute_tolerance=float(default_values.get("absolute_tolerance", 0.0)),
                relative_tolerance=float(default_values.get("relative_tolerance", 0.0)),
                case_sensitive=bool(default_values.get("case_sensitive", True)),
            ),
        )
        paths = self._stage_paths(request).artifacts
        audit_path = _write_json_once(paths / "reconciliation_audit.json", reconciled.audit)
        if not reconciled.promotion_allowed:
            disputed_path = paths / "disputed.jsonl"
            quarantined_path = paths / "quarantined.jsonl"
            _write_bytes_once(
                disputed_path,
                reconciled.disputed.to_json(
                    orient="records", lines=True, date_format="iso", force_ascii=False
                ).encode("utf-8"),
            )
            _write_bytes_once(
                quarantined_path,
                reconciled.quarantined.to_json(
                    orient="records", lines=True, date_format="iso", force_ascii=False
                ).encode("utf-8"),
            )
            return OperationResult(
                operation=request.operation,
                status="blocked",
                summary="source disputes or quarantined observations block Silver",
                outputs={
                    "audit": reconciled.audit,
                    "audit_path": str(audit_path),
                    "disputed_path": str(disputed_path),
                    "quarantined_path": str(quarantined_path),
                },
            )
        wide = self._accepted_wide(reconciled.accepted)
        silver_path = _write_parquet_once(paths / "accepted_silver.parquet", wide)
        silver_object = (
            None
            if self.object_store_archive is None
            else self.object_store_archive.archive_file(
                silver_path,
                logical_path=f"silver/{request.partition_key}/accepted",
            )
        )
        audit_object = (
            None
            if self.object_store_archive is None
            else self.object_store_archive.archive_file(
                audit_path,
                logical_path=f"silver/{request.partition_key}/audit",
            )
        )
        parent_ids = bronze_parent_ids
        as_of = max(
            _parse_aware(row["ingested_at"], name="ingested_at")
            for row in reconciliation_sources
        )
        manifest, manifest_path, manifest_object = self._manifest(
            paths=(silver_path, audit_path),
            tier="silver",
            as_of=as_of,
            parent_snapshot_ids=parent_ids,
            quality_report={"status": "pass"},
            trust_labels=(
                "point_in_time",
                "field_reconciled",
                RESEARCH_SILVER_PARTITION_LABEL,
            ),
        )
        self.catalog.register_snapshot(
            manifest.to_snapshot_ref(
                uri=(
                    manifest_object.uri
                    if manifest_object is not None
                    else manifest_path.resolve().as_uri()
                )
            )
        )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"accepted {len(wide)} PIT Silver rows after source reconciliation",
            outputs={
                "silver_path": str(silver_path),
                "silver_snapshot_id": manifest.snapshot_id,
                "silver_manifest_path": str(manifest_path),
                "silver_object": (
                    None if silver_object is None else silver_object.to_dict()
                ),
                "silver_audit_object": (
                    None if audit_object is None else audit_object.to_dict()
                ),
                "silver_manifest_object": (
                    None if manifest_object is None else manifest_object.to_dict()
                ),
                "audit": reconciled.audit,
                "audit_path": str(audit_path),
                "as_of": as_of.isoformat(),
            },
        )

    def _historical_st_check(
        self,
        gate: DataQualityGate,
        quality: Mapping[str, Any],
        *,
        silver_frame: pd.DataFrame,
        partition_key: str,
    ) -> None:
        if self.production_ledger is not None:
            if "dataset" not in silver_frame.columns:
                gate.check_historical_st(
                    pd.DataFrame(),
                    available=False,
                    degraded=False,
                    reason="accepted Silver omits dataset lineage",
                )
                return
            dataset = silver_frame["dataset"].astype(str).str.lower()
            calendar = silver_frame.loc[
                dataset.isin({"trade_calendar", "trade_cal"})
            ].copy()
            if calendar.empty or "is_open" not in calendar.columns:
                gate.check_historical_st(
                    pd.DataFrame(),
                    available=False,
                    degraded=False,
                    reason="accepted Silver has no authoritative open-session calendar row",
                )
                return
            calendar_dates = pd.to_datetime(
                calendar.get("event_time"), errors="coerce", utc=True
            ).dt.date
            requested_date = _parse_date(partition_key, name="partition_key")
            calendar = calendar.loc[calendar_dates.eq(requested_date)]
            open_session = bool(
                pd.to_numeric(calendar.get("is_open"), errors="coerce")
                .fillna(0)
                .eq(1)
                .any()
            )
            if not open_session:
                gate.check_historical_st(
                    pd.DataFrame(),
                    available=False,
                    degraded=False,
                    reason="partition is not proven to be an accepted open session",
                )
                return
            records = silver_frame.loc[
                dataset.isin({"historical_st", "stock_st", "namechange"})
            ].copy()
            if records.empty:
                gate.check_historical_st(
                    pd.DataFrame(),
                    available=True,
                    degraded=False,
                    reason="historical_st is empty on an accepted open session",
                )
                return
            ticker_column = next(
                (name for name in ("ts_code", "ticker", "symbol") if name in records),
                None,
            )
            if ticker_column is None:
                gate.check_historical_st(
                    pd.DataFrame(),
                    available=True,
                    degraded=False,
                    reason="historical_st daily membership omits a security identifier",
                )
                return
            event_dates = pd.to_datetime(
                records.get("event_time"), errors="coerce", utc=True
            ).dt.date
            records = records.loc[event_dates.eq(requested_date)]
            normalized = pd.DataFrame(
                {
                    "ts_code": records[ticker_column].astype(str),
                    "start_date": requested_date.isoformat(),
                    "end_date": requested_date.isoformat(),
                }
            )
            gate.check_historical_st(
                normalized,
                available=not normalized.empty,
                degraded=False,
                reason=(
                    None
                    if not normalized.empty
                    else "historical_st lacks membership rows for the accepted open session"
                ),
            )
            return

        # Explicit SQLite test/legacy compatibility path.  Production never
        # reads this file-backed evidence.
        st = quality.get("historical_st")
        if not isinstance(st, Mapping):
            gate.check_historical_st(
                pd.DataFrame(),
                available=False,
                degraded=True,
                reason="historical_st configuration is missing",
            )
            return
        raw_path = st.get("path")
        path = self._input_path(raw_path) if raw_path else None
        available = bool(st.get("available", path is not None and path.is_file()))
        degraded = bool(st.get("degraded", False))
        if path is None or not path.is_file():
            records = pd.DataFrame()
            available = False
            reason = str(st.get("reason") or "historical ST file is missing")
        else:
            records = read_frame(path)
            reason = None if st.get("reason") is None else str(st["reason"])
        gate.check_historical_st(
            records,
            available=available,
            degraded=degraded,
            reason=reason,
        )

    def _data_quality_gate(self, request: OperationRequest) -> OperationResult:
        sync = self._dependency(request, OperationName.SOURCE_SYNC)
        silver = self._dependency(request, OperationName.SOURCE_RECONCILIATION)
        daily = self._section("daily", request)
        quality = daily.get("data_quality")
        if not isinstance(quality, Mapping):
            raise ServiceNotConfigured("daily.data_quality must be configured")
        gate = DataQualityGate()
        text_config = quality.get("chinese_text_columns") or {}
        for row in sync.outputs["sources"]:
            batch = self._batch_from_sync(row)
            columns: Sequence[str]
            if isinstance(text_config, Mapping):
                columns = tuple(map(str, text_config.get(batch.dataset) or ()))
            else:
                columns = tuple(map(str, text_config))
            gate.check_dataframe(batch.frame, batch.contract, chinese_text_columns=columns)
            metadata = _read_json(Path(str(row["metadata_path"])))
            actual = sha256_path(row["data_path"])
            expected = str(metadata.get("data_sha256") or "")
            if expected != actual:
                gate.add_issue(
                    "bronze_hash_mismatch",
                    "Bronze file differs from its immutable lineage metadata",
                    details={"source_id": batch.source_id, "dataset": batch.dataset},
                )
        audit = dict(silver.outputs.get("audit") or {})
        if audit.get("status") != "pass":
            gate.add_issue(
                "source_reconciliation_not_accepted",
                "source reconciliation audit did not pass",
                details=audit,
            )
        silver_path = Path(str(silver.outputs["silver_path"]))
        frame = pd.read_parquet(silver_path)
        self._historical_st_check(
            gate,
            quality,
            silver_frame=frame,
            partition_key=request.partition_key,
        )
        minimum_rows = int(quality.get("minimum_rows", 1))
        if len(frame) < minimum_rows:
            gate.add_issue(
                "insufficient_accepted_rows",
                "accepted Silver partition is below its configured minimum",
                details={"rows": len(frame), "minimum_rows": minimum_rows},
            )
        required = set(map(str, quality.get("required_gold_columns") or ()))
        missing = sorted(required - set(frame.columns))
        if missing:
            gate.add_issue(
                "gold_columns_missing",
                "accepted Silver partition omits required Gold columns",
                details={"columns": missing},
            )
        coverage_threshold = float(quality.get("minimum_core_coverage", 0.95))
        if not 0 <= coverage_threshold <= 1:
            raise ValueError("minimum_core_coverage must be in [0, 1]")
        for column in map(str, quality.get("core_coverage_columns") or ()):
            if column not in frame.columns:
                continue
            coverage = float(frame[column].notna().mean()) if len(frame) else 0.0
            if coverage < coverage_threshold:
                gate.add_issue(
                    "core_field_coverage_low",
                    f"Gold core field {column!r} coverage is too low",
                    details={
                        "column": column,
                        "coverage": coverage,
                        "minimum": coverage_threshold,
                    },
                )
        report = gate.report()
        report_path = _write_json_once(
            self._stage_paths(request).artifacts / "data_quality_report.json",
            report.to_dict(),
        )
        if report.promotion_allowed and self.production_ledger is not None:
            base_calendar_identity = PartitionIdentity(
                source_id="research_os",
                dataset="accepted_trade_calendar",
                partition_key=request.partition_key,
            )
            calendar_identity = base_calendar_identity
            calendar_output = {
                "partition_key": request.partition_key,
                "silver_snapshot_id": silver.outputs["silver_snapshot_id"],
                "quality_report_hash": content_fingerprint(
                    report.to_dict(),
                    domain="factor-lab/research-os/v1/calendar-quality-report",
                ),
            }
            calendar_input_hash = content_fingerprint(
                calendar_output,
                domain="factor-lab/research-os/v1/accepted-calendar-input",
            )
            calendar_record = self.production_ledger.get_partition(
                base_calendar_identity
            )
            if calendar_record is None:
                calendar_record = self.production_ledger.ensure_partition(
                    base_calendar_identity,
                    created_at=self._now(),
                    input_hash=calendar_input_hash,
                    details={"dagster_run_id": request.run_id},
                )
            repair_scope = str(
                request.metadata.get("repair_scope_key") or ""
            ).strip()
            repair_incident_id = str(
                request.metadata.get("repair_incident_id") or ""
            ).strip()
            if repair_scope and (
                repair_incident_id
                or calendar_record.status
                in {
                    PartitionStatus.FAILED,
                    PartitionStatus.DISPUTED,
                    PartitionStatus.QUARANTINED,
                }
            ):
                child_authority = self.production_ledger.reserve_retry_successor(
                    base_calendar_identity,
                    repair_fingerprint=str(
                        request.metadata["repair_fingerprint"]
                    ),
                    created_at=self._now(),
                    input_hash=calendar_input_hash,
                    details={
                        "dagster_run_id": request.run_id,
                        "parent_stage_authority_id": request.metadata.get(
                            "repair_authority_id"
                        ),
                    },
                    scope_key=repair_scope,
                    incident_id=(repair_incident_id or None),
                    allow_succeeded_base=bool(repair_incident_id),
                )
                calendar_identity = child_authority.identity
                calendar_record = self.production_ledger.ensure_partition(
                    calendar_identity,
                    created_at=self._now(),
                    input_hash=calendar_input_hash,
                )
            elif calendar_record.input_hash not in {None, calendar_input_hash}:
                raise OrchestrationFailure(
                    "accepted calendar input changed without repair authority"
                )
            elif calendar_record.input_hash is None:
                calendar_record = self.production_ledger.ensure_partition(
                    calendar_identity,
                    created_at=self._now(),
                    input_hash=calendar_input_hash,
                )
            if calendar_record.status is not PartitionStatus.SUCCEEDED:
                if calendar_record.status in {
                    PartitionStatus.FAILED,
                    PartitionStatus.DISPUTED,
                    PartitionStatus.QUARANTINED,
                }:
                    raise OrchestrationFailure(
                        "accepted calendar identity is already terminal non-accepted"
                    )
                calendar_lease = self.production_ledger.claim(
                    identity=calendar_identity,
                    owner=f"calendar-{_safe_name(request.run_id)}",
                    now=self._now(),
                    lease_for=timedelta(minutes=15),
                )
                if calendar_lease is None:
                    raise OrchestrationFailure(
                        "accepted calendar partition is already leased"
                    )
                self.production_ledger.finish(
                    calendar_lease,
                    status=PartitionStatus.SUCCEEDED,
                    completed_at=self._now(),
                    run_id=self._operation_run_id(request),
                    output_snapshot_id=str(silver.outputs["silver_snapshot_id"]),
                    output_hash=content_fingerprint(
                        calendar_output,
                        domain="factor-lab/research-os/v1/accepted-calendar-output",
                    ),
                    details={
                        "dagster_run_id": request.run_id,
                        "accepted_calendar": calendar_output,
                        **{
                            key: request.metadata[key]
                            for key in sorted(_REPAIR_METADATA_KEYS)
                            if key in request.metadata
                        },
                    },
                )
        status = "completed" if report.promotion_allowed else "blocked"
        return OperationResult(
            operation=request.operation,
            status=status,
            summary=(
                "data quality passed"
                if report.promotion_allowed
                else "data quality blocked Gold publication"
            ),
            outputs={
                "quality_report": report.to_dict(),
                "quality_report_path": str(report_path),
                "silver_path": str(silver_path),
                "silver_snapshot_id": silver.outputs["silver_snapshot_id"],
                "as_of": silver.outputs["as_of"],
            },
        )

    def _research_panel_gold_publish(
        self,
        request: OperationRequest,
        *,
        quality: OperationResult,
        gold: Mapping[str, Any],
        panel_config: Mapping[str, Any],
    ) -> OperationResult:
        """Assemble and publish the formal 2017-present PIT research panel."""

        if panel_config.get("mode") != "research_ready_panel_v1":
            raise ServiceNotConfigured(
                "daily.gold.research_panel.mode must be 'research_ready_panel_v1'"
            )
        required_keys = {
            "analysis_start",
            "analysis_end",
            "required_datasets",
            "universe",
            "label",
            "amount_unit_multiplier",
            "market_cap_unit_multiplier",
        }
        missing_keys = sorted(required_keys - set(panel_config))
        if missing_keys:
            raise ServiceNotConfigured(
                f"daily.gold.research_panel is incomplete: {missing_keys}"
            )
        required_datasets = tuple(map(str, panel_config["required_datasets"]))
        canonical_required = {
            _REQUIRED_DATASET_ALIASES.get(name, name) for name in required_datasets
        }
        omitted = sorted(set(DEFAULT_REQUIRED_DATASETS) - canonical_required)
        if omitted:
            raise ServiceNotConfigured(
                f"formal Gold panel cannot omit required datasets: {omitted}"
            )
        universe = UniverseSpec.model_validate(panel_config["universe"])
        label = LabelSpec.model_validate(panel_config["label"])
        if universe != UniverseSpec():
            # The service permits a smaller target only in the pure builder's
            # test fixtures.  The orchestrated research fact source is frozen.
            raise ServiceNotConfigured(
                "formal Gold panel requires the frozen default UniverseSpec"
            )
        if label != LabelSpec():
            raise ServiceNotConfigured(
                "formal Gold panel requires the frozen default LabelSpec"
            )
        analysis_start = str(panel_config["analysis_start"])
        if pd.Timestamp(analysis_start).normalize() != pd.Timestamp("2017-01-01"):
            raise ServiceNotConfigured("formal Gold panel must start at 2017-01-01")
        as_of = _parse_aware(quality.outputs["as_of"], name="Gold as_of")
        panel_service = ResearchGoldPanelService(
            self.catalog, lake_root=self.settings.lake_root
        )
        self._hydrate_historical_silver_cache()
        try:
            artifacts = panel_service.build(
                output_dir=self._stage_paths(request).artifacts / "research_panel",
                analysis_start=analysis_start,
                analysis_end=str(panel_config["analysis_end"]),
                as_of=as_of,
                universe=universe,
                label=label,
                required_datasets=required_datasets,
                amount_unit_multiplier=float(panel_config["amount_unit_multiplier"]),
                market_cap_unit_multiplier=float(
                    panel_config["market_cap_unit_multiplier"]
                ),
                snapshot_limit=int(
                    panel_config.get(
                        "snapshot_limit", DEFAULT_SILVER_SNAPSHOT_DISCOVERY_CAP
                    )
                ),
            )
        except GoldPanelError as exc:
            raise OrchestrationFailure(f"research-ready Gold panel blocked: {exc}") from exc
        try:
            assert_snapshot_promotion_allowed(
                self.catalog, artifacts.parent_snapshot_ids
            )
        except SnapshotPromotionBlocked as exc:
            raise OrchestrationFailure(
                f"Silver trust labels block research Gold promotion: {exc}"
            ) from exc
        current_silver_id = str(quality.outputs["silver_snapshot_id"])
        if current_silver_id not in artifacts.parent_snapshot_ids:
            raise OrchestrationFailure(
                "research Gold parent closure omits the DQ-accepted current Silver snapshot"
            )
        table_identifier = str(gold.get("table_identifier") or "").strip()
        if not table_identifier:
            raise ServiceNotConfigured("daily.gold.table_identifier is required")
        report_path = Path(str(quality.outputs["quality_report_path"]))
        manifest = build_immutable_snapshot_manifest(
            (*artifacts.manifest_paths, report_path),
            base_dir=self.settings.lake_root,
            tier="gold",
            as_of=as_of,
            parent_snapshot_ids=artifacts.parent_snapshot_ids,
            environment_hashes=self._environment_hashes,
            quality_report=artifacts.audit,
            trust_labels=(
                "point_in_time",
                "research_ready_panel",
                "historical_st_verified",
                "field_reconciled",
                "quality_accepted",
            ),
            trading_calendar=artifacts.audit["trading_calendar"],
        )
        verification = verify_immutable_snapshot_manifest(
            manifest, base_dir=self.settings.lake_root
        )
        if not verification["valid"]:
            raise OrchestrationFailure(
                f"research Gold manifest verification failed: {verification['errors']}"
            )
        tag_prefix = str(gold.get("tag_prefix") or "ros_")
        tag = f"{tag_prefix}{manifest.snapshot_id}"
        panel = pd.read_parquet(artifacts.panel_path)
        publish_research_panel = getattr(
            self.iceberg_publisher, "publish_research_panel", None
        )
        if not callable(publish_research_panel):
            raise ServiceNotConfigured(
                "configured Iceberg publisher lacks full-history research overwrite support"
            )
        commit = publish_research_panel(
            panel,
            table_identifier=table_identifier,
            tag=tag,
            snapshot_key=manifest.snapshot_id,
            partition_key=request.partition_key,
        )
        if commit.tag != tag or commit.table_identifier != table_identifier:
            raise OrchestrationFailure("Iceberg publisher returned a mismatched commit")
        manifest_path = publish_snapshot_manifest(
            self.settings.snapshot_root,
            manifest,
            base_dir=self.settings.lake_root,
        )
        iceberg_config = self.config.get("iceberg") or {}
        catalog_name = str(iceberg_config.get("catalog_name") or "factorlab")
        uri = f"iceberg://{catalog_name}/{table_identifier}#{tag}"
        self.catalog.register_snapshot(manifest.to_snapshot_ref(uri=uri))
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=(
                "verified existing research-ready PIT Gold panel"
                if commit.reused
                else "committed research-ready PIT Gold panel"
            ),
            outputs={
                "iceberg_table": commit.table_identifier,
                "iceberg_snapshot_id": commit.snapshot_id,
                "iceberg_tag": commit.tag,
                "snapshot_id": manifest.snapshot_id,
                "manifest_path": str(manifest_path.resolve()),
                "uri": uri,
                "row_count": commit.row_count,
                "reused": commit.reused,
                "gold_panel_path": str(artifacts.panel_path.resolve()),
                "membership_path": str(artifacts.membership_path.resolve()),
                "parent_snapshot_ids": list(artifacts.parent_snapshot_ids),
                "analysis_start": artifacts.audit["analysis_start"],
                "analysis_end": artifacts.audit["analysis_end"],
                "research_ready": True,
            },
        )

    def _gold_publish(self, request: OperationRequest) -> OperationResult:
        quality = self._dependency(request, OperationName.DATA_QUALITY_GATE)
        daily = self._section("daily", request)
        gold = daily.get("gold")
        if not isinstance(gold, Mapping):
            raise ServiceNotConfigured("daily.gold must be configured")
        panel_config = gold.get("research_panel")
        if panel_config is not None:
            if not isinstance(panel_config, Mapping):
                raise ServiceNotConfigured(
                    "daily.gold.research_panel must be an object"
                )
            return self._research_panel_gold_publish(
                request,
                quality=quality,
                gold=gold,
                panel_config=panel_config,
            )
        table_identifier = str(gold.get("table_identifier") or "").strip()
        if not table_identifier:
            raise ServiceNotConfigured("daily.gold.table_identifier is required")
        quality_report = dict(quality.outputs["quality_report"])
        if quality_report.get("status") != "pass":
            raise OrchestrationFailure("Gold publication requires an accepted DQ report")
        try:
            assert_snapshot_promotion_allowed(
                self.catalog, (str(quality.outputs["silver_snapshot_id"]),)
            )
        except SnapshotPromotionBlocked as exc:
            raise OrchestrationFailure(
                f"Silver trust labels block Gold promotion: {exc}"
            ) from exc
        silver_path = Path(str(quality.outputs["silver_path"]))
        report_path = Path(str(quality.outputs["quality_report_path"]))
        manifest = build_immutable_snapshot_manifest(
            (silver_path, report_path),
            base_dir=self.settings.lake_root,
            tier="gold",
            as_of=quality.outputs["as_of"],
            parent_snapshot_ids=(str(quality.outputs["silver_snapshot_id"]),),
            environment_hashes=self._environment_hashes,
            quality_report=quality_report,
            trust_labels=("point_in_time", "field_reconciled", "quality_accepted"),
        )
        verification = verify_immutable_snapshot_manifest(
            manifest, base_dir=self.settings.lake_root
        )
        if not verification["valid"]:
            raise OrchestrationFailure(
                f"Gold manifest verification failed: {verification['errors']}"
            )
        tag_prefix = str(gold.get("tag_prefix") or "ros_")
        tag = f"{tag_prefix}{manifest.snapshot_id}"
        frame = pd.read_parquet(silver_path)
        commit = self.iceberg_publisher.publish(
            frame,
            table_identifier=table_identifier,
            tag=tag,
            snapshot_key=manifest.snapshot_id,
            partition_key=request.partition_key,
        )
        if commit.tag != tag or commit.table_identifier != table_identifier:
            raise OrchestrationFailure("Iceberg publisher returned a mismatched commit")
        manifest_path = publish_snapshot_manifest(
            self.settings.snapshot_root,
            manifest,
            base_dir=self.settings.lake_root,
        )
        iceberg_config = self.config.get("iceberg") or {}
        catalog_name = str(iceberg_config.get("catalog_name") or "factorlab")
        uri = f"iceberg://{catalog_name}/{table_identifier}#{tag}"
        self.catalog.register_snapshot(manifest.to_snapshot_ref(uri=uri))
        # A reused physical snapshot is still reported as completed: all three
        # required outputs prove that the exact immutable key and tag exist.
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=(
                "verified existing tagged Iceberg Gold snapshot"
                if commit.reused
                else "committed and tagged Iceberg Gold snapshot"
            ),
            outputs={
                "iceberg_table": commit.table_identifier,
                "iceberg_snapshot_id": commit.snapshot_id,
                "iceberg_tag": commit.tag,
                "snapshot_id": manifest.snapshot_id,
                "manifest_path": str(manifest_path.resolve()),
                "uri": uri,
                "row_count": commit.row_count,
                "reused": commit.reused,
            },
        )

    @staticmethod
    def _shadow_execution_config(payload: Mapping[str, Any]) -> ShadowExecutionConfig:
        values = dict(payload)
        cost_values = values.pop("costs", None)
        allowed = {item.name for item in fields(ShadowExecutionConfig)} - {"costs"}
        unknown = sorted(set(values) - allowed)
        if unknown:
            raise ValueError(f"unknown shadow execution settings: {unknown}")
        costs = AShareCostPolicy(**dict(cost_values or {}))
        return ShadowExecutionConfig(**values, costs=costs)

    @staticmethod
    def _blocked_daily_outcome_from_request(
        request: OperationRequest,
    ) -> Mapping[str, str] | None:
        raw = request.metadata.get("daily_data_outcome")
        if raw is None:
            return None
        if not isinstance(raw, Mapping):
            raise OrchestrationFailure("daily_data_outcome must be a typed object")
        stage = str(raw.get("failure_stage") or "").strip()
        error_code = str(raw.get("error_code") or "").strip()
        message = str(raw.get("message") or "").strip()
        occurred_at = str(raw.get("occurred_at") or "").strip()
        if (
            str(raw.get("partition_key") or "") != request.partition_key
            or str(raw.get("status") or "") != "blocked"
            or stage not in {item.value for item in DataPipelineStage}
            or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", error_code
            )
            or not message
        ):
            raise OrchestrationFailure("daily_data_outcome envelope is invalid")
        try:
            _parse_aware(occurred_at, name="daily_data_outcome.occurred_at")
        except (TypeError, ValueError) as exc:
            raise OrchestrationFailure(
                "daily_data_outcome timestamp is invalid"
            ) from exc
        return {
            "partition_key": request.partition_key,
            "status": "blocked",
            "failure_stage": stage,
            "error_code": error_code,
            "message": sanitize_operational_text(message),
            "occurred_at": occurred_at,
        }

    def _durable_stage_failure_time(
        self, partition_key: str, dataset: str
    ) -> datetime:
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "production stage failure time requires the partition ledger"
            )
        record = self.production_ledger.get_partition(
            PartitionIdentity("research_os", dataset, partition_key)
        )
        if record is None:
            raise OrchestrationFailure(
                "production stage failure has no durable partition identity"
            )
        return (
            record.started_at
            or record.updated_at
            or record.created_at
        ).astimezone(timezone.utc)

    def _shadow_nav_step(self, request: OperationRequest) -> OperationResult:
        blocked = self._blocked_daily_outcome_from_request(request)
        if blocked is not None:
            incident = self.report_unexpected_data_failure(
                request.partition_key,
                message=blocked["message"],
                occurred_at=_parse_aware(
                    blocked["occurred_at"],
                    name="daily_data_outcome.occurred_at",
                ),
                dagster_run_id=request.run_id,
                failed_step_key="shadow_account_nav:daily_data_outcome",
                error_code=blocked["error_code"],
                expected_failure_stage=blocked["failure_stage"],
            )
            return OperationResult(
                operation=request.operation,
                status="completed",
                summary=(
                    "persisted data incident, frozen_data lifecycle, and "
                    "all-cash target intent"
                ),
                outputs={
                    "daily_data_outcome": dict(blocked),
                    "incident": dict(incident),
                    "risk_guard": "cash_target_intent",
                },
            )
        daily = self._section("daily", request)
        shadow = daily.get("shadow")
        if not isinstance(shadow, Mapping):
            raise ServiceNotConfigured("daily.shadow must be configured")
        input_mode = str(shadow.get("input_mode") or "authoritative_pg").strip()
        if input_mode in {"legacy_import", "test"}:
            return self._legacy_shadow_nav_step(request)
        if input_mode != "authoritative_pg":
            raise ServiceNotConfigured(
                "daily.shadow.input_mode must be authoritative_pg, legacy_import, or test"
            )
        try:
            return self._authoritative_shadow_nav_step(request, shadow=shadow)
        except Exception as exc:
            if request.metadata.get("repair_incident_id"):
                # A repair attempt already belongs to one durable incident
                # cohort.  Let its selected successor fail and be retried;
                # minting a nested incident would fork the recovery authority.
                raise
            if not self._is_production_runtime():
                raise
            incident = self.report_unexpected_data_failure(
                request.partition_key,
                message=(
                    "authoritative typed shadow closure failed: "
                    f"{type(exc).__name__}"
                ),
                occurred_at=self._durable_stage_failure_time(
                    request.partition_key, "stage_shadow"
                ),
                dagster_run_id=request.run_id,
                failed_step_key="shadow_account_nav:typed_execution_closure",
                error_code="shadow_typed_execution_failed",
            )
            return OperationResult(
                operation=request.operation,
                status="failed",
                summary=(
                    "authoritative typed shadow closure failed and the fleet "
                    f"was frozen: {type(exc).__name__}"
                ),
                outputs={"incident": dict(incident), "risk_guard": "frozen_data"},
            )

    def _legacy_shadow_nav_step(self, request: OperationRequest) -> OperationResult:
        """Explicit compatibility path; never selected by the production default."""

        self._dependency(request, OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH)
        daily = self._section("daily", request)
        shadow = daily.get("shadow")
        if not isinstance(shadow, Mapping):
            raise ServiceNotConfigured("daily.shadow must be configured")
        input_value = shadow.get("champion_input_path")
        if not input_value:
            raise ServiceNotConfigured("daily.shadow.champion_input_path is required")
        input_path = self._input_path(input_value)
        if not input_path.is_file():
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no Champion target exists for this partition",
                outputs={"champion_input_path": str(input_path)},
            )
        payload = _read_json(input_path)
        required = {
            "account_id",
            "account_name",
            "initial_capital",
            "opened_at",
            "decision_date",
            "trade_date",
            "target_weights",
            "market_bars_path",
            "snapshot_id",
            "model_version",
        }
        missing = sorted(required - set(payload))
        if missing:
            raise OrchestrationFailure(f"Champion input fields missing: {missing}")
        market_path = self._input_path(payload["market_bars_path"])
        if not market_path.is_file():
            raise OrchestrationFailure(f"shadow market bars are missing: {market_path}")
        snapshot = self.catalog.get_snapshot(str(payload["snapshot_id"]))
        if snapshot is None:
            raise OrchestrationFailure("Champion references an unregistered decision snapshot")
        # Never substitute today's newly published Gold snapshot.  The input
        # must carry the actual snapshot visible at the prior decision close.
        account_id = str(payload["account_id"])
        if self.catalog.get_shadow_account(account_id) is None:
            self.catalog.create_shadow_account(
                account_id=account_id,
                name=str(payload["account_name"]),
                initial_capital=float(payload["initial_capital"]),
                opened_at=_parse_aware(payload["opened_at"], name="opened_at"),
                currency=str(payload.get("currency") or "CNY"),
            )
        execution = self._shadow_execution_config(
            dict(shadow.get("execution") or {})
        )
        service = ShadowStepService(self.catalog, execution)
        try:
            result = service.step(
                account_id=account_id,
                decision_date=payload["decision_date"],
                trade_date=payload["trade_date"],
                target_weights={
                    str(key): float(value)
                    for key, value in payload["target_weights"].items()
                },
                market_bars=read_frame(market_path),
                snapshot_id=str(payload["snapshot_id"]),
                model_version=str(payload["model_version"]),
                benchmark_return=(
                    None
                    if payload.get("benchmark_return") is None
                    else float(payload["benchmark_return"])
                ),
                expected_next_session=payload.get("expected_next_session"),
            )
        except ShadowStepAlreadyApplied:
            # The operation state may have been lost after the atomic catalog
            # commit.  Repeating the domain mutation is forbidden, and without
            # a persisted matching result we cannot claim a completed step.
            raise OrchestrationFailure(
                "shadow step is already authoritative but its orchestration result is absent"
            )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"advanced shadow account {account_id} through {payload['trade_date']}",
            outputs=_jsonable(asdict(result)),
        )

    @staticmethod
    def _authoritative_shadow_account(
        shadow: Mapping[str, Any],
    ) -> dict[str, Any]:
        raw = shadow.get("account")
        if not isinstance(raw, Mapping):
            raise ServiceNotConfigured(
                "authoritative daily.shadow.account must be configured"
            )
        required = {"account_id", "name", "initial_capital", "opened_at"}
        missing = sorted(required - set(raw))
        if missing:
            raise ServiceNotConfigured(
                f"daily.shadow.account fields missing: {missing}"
            )
        return dict(raw)

    def _current_target_generation(
        self,
        request: OperationRequest,
        *,
        control: AuthoritativeChampionControl,
    ) -> tuple[ChampionStockTarget | None, str]:
        """Create the next-session target only from settled catalog state."""

        decision_date = _parse_date(request.partition_key, name="daily partition")
        existing = control.latest_stock_target(decision_date=decision_date)

        generated_at = self._now()
        quality = self._read_stage(request, OperationName.DATA_QUALITY_GATE)
        gold = self._read_stage(
            request, OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH
        )
        if quality is None or (quality.status == "completed" and gold is None):
            return (
                (existing, "already_persisted")
                if existing is not None
                else (None, "daily_data_outcome_not_settled")
            )

        operational_data_failure = (
            quality.status != "completed"
            or gold is None
            or gold.status != "completed"
            or not bool(gold.outputs.get("research_ready"))
        )
        if operational_data_failure:
            if existing is not None and existing.cash_weight >= 1.0 - 1e-12:
                return existing, "current_gold_data_failure_all_cash"
            if existing is not None:
                generated_at = max(
                    generated_at, existing.generated_at + timedelta(microseconds=1)
                )
            projection = control.latest_allocation(through=generated_at)
            if projection is None:
                return None, "no_authoritative_champion_projection"
            snapshot_record = self.catalog.get_snapshot(projection.data_snapshot_id)
            if snapshot_record is None:
                raise OrchestrationFailure(
                    "Champion allocation references a missing Gold snapshot"
                )
            try:
                target = control.build_stock_target(
                    projection,
                    gold_snapshot_id=snapshot_record.reference.snapshot_id,
                    gold_frame=pd.DataFrame(),
                    decision_date=decision_date,
                    generated_at=generated_at,
                    force_data_failure=True,
                    supersedes=existing,
                )
                control.persist_stock_target(target)
            except (ChampionControlError, ValueError) as exc:
                raise OrchestrationFailure(
                    f"failed to persist mandatory all-cash target: {exc}"
                ) from exc
            return target, "current_gold_data_failure_all_cash"

        assert gold is not None
        if existing is not None:
            return existing, "already_persisted"
        snapshot_id = str(gold.outputs.get("snapshot_id") or "").strip()
        snapshot_record = self.catalog.get_snapshot(snapshot_id)
        if snapshot_record is None:
            raise OrchestrationFailure(
                "completed research Gold stage has no registered snapshot"
            )
        projection = control.latest_allocation(through=generated_at)
        if projection is None:
            return None, "no_authoritative_champion_projection"

        previous = control.latest_stock_target()
        if (
            previous is not None
            and previous.allocation_projection_id == projection.projection_id
        ):
            calendar = snapshot_record.reference.manifest.get("trading_calendar")
            if not isinstance(calendar, Mapping):
                raise OrchestrationFailure(
                    "research Gold snapshot has no trusted trading calendar"
                )
            try:
                sessions = tuple(
                    date.fromisoformat(str(item)) for item in calendar["sessions"]
                )
                elapsed = sessions.index(decision_date) - sessions.index(
                    previous.decision_date
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise OrchestrationFailure(
                    "research Gold trading calendar cannot prove rebalance cadence"
                ) from exc
            if elapsed < 5:
                return None, "weekly_rebalance_not_due"

        try:
            gold_frame = load_gold_research_panel(
                snapshot_record.reference,
                lake_root=self.settings.lake_root,
            )
            target = control.build_stock_target(
                projection,
                gold_snapshot_id=snapshot_id,
                gold_frame=gold_frame,
                decision_date=decision_date,
                generated_at=generated_at,
            )
            control.persist_stock_target(target)
        except (ChampionControlError, GoldPanelError, ValueError) as exc:
            # A missing/invalid current target is a closed outcome, never a
            # fallback to a hand-authored weight file.
            raise OrchestrationFailure(
                f"authoritative Champion stock target unavailable: {exc}"
            ) from exc
        return target, "authoritative_gold_dsl_target"

    def _current_challenger_target_generation(
        self,
        request: OperationRequest,
        *,
        planner: AuthoritativeChallengerPlanner,
    ) -> tuple[tuple[ChallengerStockTarget, ...], str]:
        """Persist weekly Challenger intents before any execution capability gate."""

        if not planner.active_authorities(projectable_only=True):
            return (), "no_projectable_promoted_challenger"
        quality = self._read_stage(request, OperationName.DATA_QUALITY_GATE)
        gold = self._read_stage(
            request, OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH
        )
        if quality is None or gold is None:
            return (), "daily_data_outcome_not_settled"
        if (
            quality.status != "completed"
            or gold.status != "completed"
            or not bool(gold.outputs.get("research_ready"))
        ):
            return (), "current_gold_data_failure"
        snapshot_id = str(gold.outputs.get("snapshot_id") or "").strip()
        snapshot = self.catalog.get_snapshot(snapshot_id)
        if snapshot is None:
            raise OrchestrationFailure(
                "completed Gold stage has no registered Challenger decision snapshot"
            )
        try:
            frame = load_gold_research_panel(
                snapshot.reference, lake_root=self.settings.lake_root
            )
            targets = planner.generate_due_targets(
                gold_snapshot_id=snapshot_id,
                gold_frame=frame,
                decision_date=request.partition_key,
            )
        except (ChallengerPlannerError, GoldPanelError, ValueError) as exc:
            raise OrchestrationFailure(
                f"authoritative Challenger target unavailable: {exc}"
            ) from exc
        return targets, (
            "authoritative_gold_dsl_targets"
            if targets
            else "weekly_rebalance_not_due"
        )

    def _pending_cash_intent_for_account(
        self,
        account_id: str,
        *,
        superseding_target_generated_at: datetime | None,
    ) -> Mapping[str, Any] | None:
        """Return an unresolved cash intent while actual positions remain."""

        account = self.catalog.get_shadow_account(account_id)
        if account is None or self.production_ledger is None:
            return None
        open_domain_incidents = {
            str(item.payload.get("domain_incident_id") or "")
            for item in self.production_ledger.iter_incidents(
                status=IncidentStatus.OPEN
            )
        }
        if not open_domain_incidents:
            return None
        pending = tuple(
            event
            for event in self.catalog.iter_shadow_events_by_type(
                account_id=account_id,
                event_type="cash_target_intent",
                since=None,
                through=None,
            )
            if str(event.payload.get("incident_id") or "") in open_domain_incidents
        )
        if not pending:
            return None
        latest = max(
            pending, key=lambda item: (item.occurred_at, item.sequence_number)
        )
        if (
            superseding_target_generated_at is not None
            and superseding_target_generated_at > latest.occurred_at
        ):
            return None
        if not self.catalog.list_shadow_positions(account_id):
            return None
        return {**dict(latest.payload), "occurred_at": latest.occurred_at}

    def _validate_fresh_shadow_repair_projection(
        self,
        *,
        incident_id: str,
        trade_date: date,
        projections: Sequence[Any],
    ) -> None:
        """Reject cached pre-repair account/session evidence before stage success."""

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "Shadow incident repair requires the production partition ledger"
            )
        gold_stage = self.production_ledger.get_repair_partition(
            incident_id, "stage_gold"
        )
        if not (
            gold_stage is not None
            and gold_stage.status is PartitionStatus.SUCCEEDED
            and gold_stage.completed_at is not None
        ):
            raise OrchestrationFailure(
                "Shadow incident repair has no completed repaired Gold authority"
            )
        if self.shadow_authority is None:
            raise OrchestrationFailure(
                "Shadow incident repair lacks formal role authority"
            )
        try:
            bindings = tuple(self.shadow_authority.active_fleet_bindings())
        except Exception as exc:
            raise OrchestrationFailure(
                "Shadow incident repair cannot read active fleet authority"
            ) from exc
        binding_by_account = {
            str(binding.account_id): binding for binding in bindings
        }
        if not (
            bindings
            and len(binding_by_account) == len(bindings)
            and set(binding_by_account)
            == {str(item.account_id) for item in projections}
        ):
            raise OrchestrationFailure(
                "Shadow incident repair projection fleet differs from active bindings"
            )

        formal_projection_reader = getattr(
            self.shadow_authority, "session_projection", None
        )
        fleet_closure_reader = getattr(
            self.shadow_authority, "fleet_closure", None
        )
        if getattr(self, "_production_authority", False) and not (
            callable(formal_projection_reader) and callable(fleet_closure_reader)
        ):
            raise OrchestrationFailure(
                "production Shadow repair lacks formal session/closure readers"
            )

        formal_sessions: dict[str, Any] = {}
        for projection in projections:
            account_id = str(projection.account_id)
            account = self.catalog.get_shadow_account(account_id)
            if account is None or account.status != "active":
                raise OrchestrationFailure(
                    "Shadow incident repair account is not active"
                )
            reported_tail = (
                int(projection.last_event_sequence),
                str(projection.last_event_hash),
            )
            if (
                account.last_event_sequence,
                account.last_event_hash,
            ) != reported_tail:
                raise OrchestrationFailure(
                    "Shadow incident repair did not finish at its projection tail"
                )
            event = self.catalog.get_shadow_event(
                account_id=account_id,
                event_hash=reported_tail[1],
            )
            if event is None or event.sequence_number != reported_tail[0]:
                raise OrchestrationFailure(
                    "Shadow incident repair projection event is not uniquely durable"
                )
            if not (
                event.event_type == "account_projected"
                and event.occurred_at.astimezone(_SHANGHAI).date() == trade_date
                and event.occurred_at >= gold_stage.completed_at
                and isinstance(
                    event.payload.get("research_os_shadow_step"), Mapping
                )
                and event.payload["research_os_shadow_step"].get("kind")
                == "account_projection"
                and str(
                    event.payload["research_os_shadow_step"].get("step_id") or ""
                )
                == str(projection.step_id)
            ):
                raise OrchestrationFailure(
                    "Shadow incident repair recovered a pre-Gold or detached projection"
                )

            if getattr(self, "_production_authority", False):
                try:
                    formal = formal_projection_reader(
                        account_id=account_id, trade_date=trade_date
                    )
                except Exception as exc:
                    raise OrchestrationFailure(
                        "Shadow incident repair formal session is invalid"
                    ) from exc
                binding = binding_by_account[account_id]
                if not (
                    formal is not None
                    and formal.trade_date == trade_date
                    and formal.role_binding_id == str(binding.binding_id)
                    and formal.account_event_sequence == reported_tail[0]
                    and formal.account_event_hash == reported_tail[1]
                    and formal.decision_snapshot_id
                    == projection.decision_snapshot_id
                    and formal.execution_snapshot_id
                    == projection.execution_snapshot_id
                    and formal.mark_snapshot_id == projection.mark_snapshot_id
                    and formal.rebalanced == bool(projection.rebalanced)
                    and np.isclose(formal.cash, projection.cash, atol=0.01)
                    and np.isclose(formal.nav, projection.nav, atol=0.01)
                    and np.isclose(
                        formal.benchmark_nav,
                        projection.benchmark_nav,
                        atol=0.01,
                    )
                    and formal.position_count == projection.position_count
                    and formal.created_at >= event.occurred_at
                ):
                    raise OrchestrationFailure(
                        "Shadow incident repair formal session differs from its account event"
                    )
                formal_sessions[account_id] = formal

        if formal_sessions:
            try:
                closure = fleet_closure_reader(trade_date)
            except Exception as exc:
                raise OrchestrationFailure(
                    "Shadow incident repair formal fleet closure is invalid"
                ) from exc
            expected_members = tuple(
                sorted(
                    (
                        {
                            "binding_id": str(binding_by_account[account_id].binding_id),
                            "binding_hash": str(
                                binding_by_account[account_id].binding_hash
                            ),
                            "role": (
                                binding_by_account[account_id].role.value
                                if isinstance(
                                    binding_by_account[account_id].role, ShadowRole
                                )
                                else str(binding_by_account[account_id].role)
                            ),
                            "role_key": str(
                                binding_by_account[account_id].role_key
                            ),
                            "account_id": account_id,
                            "session_hash": formal_sessions[account_id].session_hash,
                            "account_event_hash": formal_sessions[
                                account_id
                            ].account_event_hash,
                        }
                        for account_id in formal_sessions
                    ),
                    key=lambda member: (
                        member["role"],
                        member["role_key"],
                        member["binding_id"],
                    ),
                )
            )
            if not (
                closure is not None
                and closure.trade_date == trade_date
                and closure.members == expected_members
                and closure.member_count == len(expected_members)
                and closure.closed_at
                >= max(item.created_at for item in formal_sessions.values())
            ):
                raise OrchestrationFailure(
                    "Shadow incident repair lacks its exact immutable fleet closure"
                )

    def _authoritative_shadow_nav_step(
        self,
        request: OperationRequest,
        *,
        shadow: Mapping[str, Any],
    ) -> OperationResult:
        incident_repair = bool(request.metadata.get("repair_incident_id"))
        validation_partition = str(
            request.metadata.get("repair_validation_trade_date")
            or request.partition_key
        )
        trade_date = _parse_date(
            validation_partition,
            name=(
                "repair validation trade date"
                if incident_repair
                else "daily partition"
            ),
        )
        execution_request = (
            replace(request, partition_key=trade_date.isoformat())
            if trade_date.isoformat() != request.partition_key
            else request
        )
        control = AuthoritativeChampionControl(
            self.catalog, shadow_authority=self.shadow_authority
        )
        challenger_planner = (
            None
            if self.shadow_authority is None
            else AuthoritativeChallengerPlanner(
                self.catalog, shadow_authority=self.shadow_authority
            )
        )
        challenger_generated: tuple[ChallengerStockTarget, ...] = ()
        challenger_generation_reason = "formal_shadow_authority_unavailable"
        challenger_plans: tuple[DailyShadowPlan, ...] = ()
        challenger_authorities: dict[str, Any] = {}
        if challenger_planner is not None:
            if not incident_repair:
                challenger_generated, challenger_generation_reason = (
                    self._current_challenger_target_generation(
                        execution_request, planner=challenger_planner
                    )
                )
            else:
                challenger_generation_reason = "incident_repair_validation_only"
            try:
                authorities = challenger_planner.active_authorities()
                challenger_authorities = {
                    item.experiment_id: item for item in authorities
                }
                challenger_plans = challenger_planner.plans_for_trade_date(trade_date)
            except ChallengerPlannerError as exc:
                raise OrchestrationFailure(
                    f"authoritative Challenger plan unavailable: {exc}"
                ) from exc
        try:
            executable = control.stock_target_for_trade_date(trade_date)
        except ChampionControlError as exc:
            raise OrchestrationFailure(
                f"authoritative Champion target lookup failed: {exc}"
            ) from exc

        account = self._authoritative_shadow_account(shadow)
        account_id = str(account["account_id"])
        pending_cash_intent = self._pending_cash_intent_for_account(
            account_id,
            superseding_target_generated_at=(
                None if executable is None else executable.generated_at
            ),
        )

        effective_challenger_plans: list[DailyShadowPlan] = []
        if challenger_planner is not None:
            for plan in challenger_plans:
                assert plan.role_key is not None
                target = challenger_planner.target_for_trade_date(
                    plan.role_key, trade_date
                )
                cash_intent = self._pending_cash_intent_for_account(
                    plan.account_id,
                    superseding_target_generated_at=(
                        None if target is None else target.generated_at
                    ),
                )
                if cash_intent is None:
                    effective_challenger_plans.append(plan)
                    continue
                authority = challenger_authorities.get(plan.role_key)
                if authority is None:
                    raise OrchestrationFailure(
                        "Challenger cash intent lacks promoted experiment authority"
                    )
                effective_challenger_plans.append(
                    DailyShadowPlan(
                        account_id=plan.account_id,
                        role="challenger",
                        role_key=plan.role_key,
                        target_weights={},
                        decision_snapshot_id=(
                            target.gold_snapshot_id
                            if target is not None
                            else authority.experiment.spec.snapshot.snapshot_id
                        ),
                        model_version=str(cash_intent.get("intent_id") or ""),
                    )
                )
        challenger_plans = tuple(effective_challenger_plans)

        executed: Mapping[str, Any] | None = None
        formal_daily_projection = bool(
            self.shadow_authority is not None and self.settings.uses_postgresql
        )
        if (
            formal_daily_projection
            or executable is not None
            or pending_cash_intent is not None
            or challenger_plans
        ):
            if shadow.get("market_bars_path"):
                raise OrchestrationFailure(
                    "production shadow execution rejects market_bars_path; "
                    "use a capability-audited execution adapter"
                )
            self._require_formal_shadow_session(trade_date)
            try:
                typed_session = self.build_execution_session(trade_date)
            except Exception as exc:
                # Persisted error details are written by the typed authority;
                # the orchestration surface emits only the error class so no
                # vendor payload or credential-bearing exception can leak.
                raise OrchestrationFailure(
                    "typed execution session is unavailable: "
                    f"{type(exc).__name__}"
                ) from exc
            if not typed_session.capability.accepted:
                raise OrchestrationFailure(
                    "typed execution capability is non-forward: "
                    + ",".join(typed_session.capability.reasons)
                )
            market_bars = typed_session.bars.copy(deep=True)
            execution_snapshot_id = (
                typed_session.execution_snapshot.snapshot_id
            )
            mark_snapshot_id = typed_session.mark_snapshot.snapshot_id
            benchmark_return = float(typed_session.benchmark_return)
            if self.catalog.get_shadow_account(account_id) is None:
                self.catalog.create_shadow_account(
                    account_id=account_id,
                    name=str(account["name"]),
                    initial_capital=float(account["initial_capital"]),
                    opened_at=_parse_aware(account["opened_at"], name="opened_at"),
                    currency=str(account.get("currency") or "CNY"),
                )
            if self.shadow_authority is None:
                raise OrchestrationFailure(
                    "formal production shadow projection requires the 0007 authority"
                )
            if not np.isfinite(benchmark_return):
                raise OrchestrationFailure("formal benchmark_return is not finite")
            if pending_cash_intent is not None:
                intent_time = _parse_aware(
                    pending_cash_intent["occurred_at"],
                    name="cash_intent.occurred_at",
                )
                target_weights: Mapping[str, float] | None = {}
                projection = control.latest_allocation(through=intent_time)
                if projection is None:
                    raise OrchestrationFailure(
                        "cash intent has no prior accepted Champion snapshot"
                    )
                decision_snapshot_id: str | None = projection.data_snapshot_id
                model_version: str | None = str(
                    pending_cash_intent.get("intent_id") or ""
                )
            elif executable is not None:
                target_weights = dict(executable.target_weights)
                decision_snapshot_id = executable.gold_snapshot_id
                model_version = executable.target_id
            else:
                # A Challenger session still requires the static Champion to
                # be marked on the exact same execution/mark evidence even
                # when no weekly Champion rebalance is due.
                target_weights = None
                decision_snapshot_id = None
                model_version = None
            role_key = str(
                shadow.get("role_key")
                or ((self.config.get("monthly") or {}).get("challenger") or {}).get(
                    "champion_role"
                )
                or "static_champion"
            ).strip()
            binding = self.shadow_authority.active_binding(
                role=ShadowRole.CHAMPION, role_key=role_key
            )
            if binding is None:
                raise OrchestrationFailure(
                    "Champion role was not atomically bound before the evidence epoch"
                )
            if binding.account_id != account_id:
                raise OrchestrationFailure(
                    "configured Champion account differs from its active role binding"
                )
            lifecycle_records, shadow_accounts = self._production_lifecycle_fleet()
            daily_plans = (
                DailyShadowPlan(
                    account_id=account_id,
                    role="champion",
                    role_key=role_key,
                    target_weights=target_weights,
                    decision_snapshot_id=decision_snapshot_id,
                    model_version=model_version,
                ),
                *challenger_plans,
            )
            daily_result = ProductionDailyControl(
                self.catalog, shadow_authority=self.shadow_authority
            ).run(
                outcome=DailyDataOutcome(
                    partition_key=trade_date.isoformat(),
                    status=DailyDataStatus.ACCEPTED,
                    occurred_at=self._now(),
                    execution_snapshot_id=execution_snapshot_id,
                    mark_snapshot_id=mark_snapshot_id,
                ),
                lifecycle_records=lifecycle_records,
                shadow_accounts=shadow_accounts,
                plans=daily_plans,
                market_bars=market_bars,
                benchmark_return=benchmark_return,
            )
            if len(daily_result.projections) != len(daily_plans):
                raise OrchestrationFailure(
                    "formal daily control returned an unexpected projection count"
                )
            if incident_repair:
                self._validate_fresh_shadow_repair_projection(
                    incident_id=str(request.metadata["repair_incident_id"]),
                    trade_date=trade_date,
                    projections=daily_result.projections,
                )
            executed = {
                "projections": [
                    _jsonable(asdict(item)) for item in daily_result.projections
                ],
                "champion_account_id": account_id,
                "challenger_account_ids": [
                    plan.account_id for plan in challenger_plans
                ],
            }

        if incident_repair:
            generated = None
            generation_reason = "incident_repair_validation_only"
        else:
            generated, generation_reason = self._current_target_generation(
                execution_request, control=control
            )
        if executed is None and generated is None and not challenger_generated:
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no executable or newly generated authoritative Champion target",
                outputs={
                    "input_mode": "authoritative_pg",
                    **(
                        {
                            "incident_partition_key": request.partition_key,
                            "validation_trade_date": trade_date.isoformat(),
                        }
                        if incident_repair
                        else {}
                    ),
                    "generation_reason": generation_reason,
                    "challenger_generation_reason": challenger_generation_reason,
                },
            )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=(
                "executed PostgreSQL-authoritative Champion target"
                if executed is not None
                else "persisted PostgreSQL-authoritative target for next session"
            ),
            outputs={
                "input_mode": "authoritative_pg",
                **(
                    {
                        "incident_partition_key": request.partition_key,
                        "validation_trade_date": trade_date.isoformat(),
                    }
                    if incident_repair
                    else {}
                ),
                "executed": executed,
                "generated_target": (
                    None if generated is None else generated.to_dict()
                ),
                "generation_reason": generation_reason,
                "generated_challenger_targets": [
                    target.to_dict() for target in challenger_generated
                ],
                "challenger_generation_reason": challenger_generation_reason,
            },
        )

    def _optional_cycle_input(
        self, section_name: str, request: OperationRequest
    ) -> tuple[dict[str, Any], Path, dict[str, Any] | None]:
        section = self._section(section_name, request)
        raw_path = section.get("input_path")
        if not raw_path:
            raise ServiceNotConfigured(f"{section_name}.input_path is required")
        path = self._input_path(raw_path)
        return section, path, (_read_json(path) if path.is_file() else None)

    @staticmethod
    def _lifecycle_record(payload: Mapping[str, Any]) -> SleeveLifecycleRecord:
        transitions = tuple(
            LifecycleTransition(
                from_state=SleeveState(str(row["from_state"])),
                to_state=SleeveState(str(row["to_state"])),
                as_of_date=_parse_date(row["as_of_date"], name="transition.as_of_date"),
                reasons=tuple(map(str, row.get("reasons") or ())),
            )
            for row in payload.get("transitions") or ()
        )
        return SleeveLifecycleRecord(
            sleeve_id=str(payload["sleeve_id"]),
            state=SleeveState(str(payload.get("state") or SleeveState.PROPOSED.value)),
            target_weight=float(payload.get("target_weight", 0.0)),
            effective_weight=float(payload.get("effective_weight", 0.0)),
            consecutive_multi_alarm_checks=int(
                payload.get("consecutive_multi_alarm_checks", 0)
            ),
            reduced_weeks=int(payload.get("reduced_weeks", 0)),
            probation_weeks=int(payload.get("probation_weeks", 0)),
            dormant_since=(
                None
                if payload.get("dormant_since") is None
                else _parse_date(payload["dormant_since"], name="dormant_since")
            ),
            transitions=transitions,
        )

    @staticmethod
    def _health_observation(
        payload: Mapping[str, Any], *, catalog_projection: bool = False
    ) -> SleeveHealthObservation:
        values = dict(payload)
        values["as_of_date"] = _parse_date(values["as_of_date"], name="as_of_date")
        # This counter is a catalog/shadow-ledger projection, never an external
        # measurement.  Silently accepting it would let a JSON file self-award
        # the 60-session recovery gate.
        if not catalog_projection:
            values.pop("new_sessions_since_dormant", None)
        return SleeveHealthObservation(**values)

    def _record_from_catalog(
        self, sleeve_id: str, bootstrap: Mapping[str, Any]
    ) -> SleeveLifecycleRecord:
        record: SleeveLifecycleRecord | None = None
        lifecycle_events = self.catalog.iter_lifecycle_events(sleeve_id=sleeve_id)
        try:
            for event in lifecycle_events:
                persisted = event.evidence.get("record")
                if isinstance(persisted, Mapping):
                    record = self._lifecycle_record(persisted)
                    break
        finally:
            close = getattr(lifecycle_events, "close", None)
            if close is not None:
                close()

        # Compatibility bootstrap for sleeves not yet imported into the new
        # catalog.  Only durable identity/state/weights are admitted.  Runtime
        # counters and dormant dates from the old JSON are deliberately ignored.
        if record is None:
            record = SleeveLifecycleRecord(
                sleeve_id=sleeve_id,
                state=SleeveState(
                    str(bootstrap.get("state") or SleeveState.PROPOSED.value)
                ),
                target_weight=float(bootstrap.get("target_weight", 0.0)),
                effective_weight=float(bootstrap.get("effective_weight", 0.0)),
            )
        if self._has_open_data_incident():
            return replace(
                record,
                state=SleeveState.FROZEN_DATA,
                effective_weight=0.0,
            )
        return record

    def _shadow_sessions_since(
        self, account_id: str | None, since: date | None, as_of: date
    ) -> int:
        if not account_id or since is None:
            return 0
        account = self.catalog.get_shadow_account(account_id)
        if account is None:
            return 0
        return self.catalog.count_shadow_sessions(
            account_id=account_id, since=since, through=as_of
        )

    def _catalog_sessions_since(self, since: date, as_of: date) -> int:
        return len(
            {
                snapshot.reference.as_of.date()
                for snapshot in self.catalog.list_snapshots(
                    limit=1000,
                    quality_status=DataQualityStatus.ACCEPTED,
                    tier=SnapshotTier.GOLD,
                )
                if since < snapshot.reference.as_of.date() <= as_of
            }
        )

    def _persist_health_measurement(
        self,
        *,
        record: SleeveLifecycleRecord,
        observation: SleeveHealthObservation,
        snapshot_id: str,
        shadow_account_id: str | None,
    ) -> LifecycleEvent:
        # Health measurements share the append-only catalog but occur one minute
        # before the deterministic monitor event, so they cannot shadow its
        # authoritative end-of-tick state in latest-state projections.
        occurred_at = datetime.combine(
            observation.as_of_date, time(14, 59), tzinfo=timezone.utc
        )
        return self.catalog.append_lifecycle_event(
            LifecycleEvent(
                idempotency_key=(
                    f"health:{record.sleeve_id}:{observation.as_of_date.isoformat()}:"
                    f"{snapshot_id}"
                ),
                sleeve_id=record.sleeve_id,
                from_state=LifecycleState(record.state.value),
                to_state=LifecycleState(record.state.value),
                cause="health_measurement_recorded",
                occurred_at=occurred_at,
                evidence={
                    "snapshot_id": snapshot_id,
                    "shadow_account_id": shadow_account_id,
                    "measurement": {
                        key: value
                        for key, value in asdict(observation).items()
                        if key != "new_sessions_since_dormant"
                    },
                    "measurement_kind": "raw_point_in_time",
                },
            )
        )

    def _weekly_rows(
        self, request: OperationRequest
    ) -> tuple[dict[str, Any], Path, dict[str, Any] | None, list[Mapping[str, Any]]]:
        section = self._section("weekly", request)
        input_mode = str(section.get("input_mode") or "").strip()
        if input_mode == "authoritative_pg_event_chain":
            forbidden = sorted(
                {"input_path", "monitor_inputs", "research_inputs"} & set(section)
            )
            if forbidden:
                raise OrchestrationFailure(
                    "production weekly monitoring rejects caller/file inputs: "
                    + ", ".join(forbidden)
                )
            # Event-chain health is advanced by ProductionDailyControl after a
            # trusted daily projection.  The weekly orchestration operations
            # remain a compatibility no-op and never load a side-channel file.
            return section, self._state_root / "authoritative_pg_event_chain", None, []
        section, path, payload = self._optional_cycle_input("weekly", request)
        if payload is None:
            return section, path, None, []
        rows = payload.get("sleeves")
        if not isinstance(rows, list):
            raise ValueError("weekly input sleeves must be an array")
        return section, path, payload, rows

    def _sleeve_health_check(self, request: OperationRequest) -> OperationResult:
        _, path, payload, rows = self._weekly_rows(request)
        if payload is None or not rows:
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no configured Sleeve health input",
                outputs={"input_path": str(path)},
            )
        snapshot_id = str(payload.get("snapshot_id") or "")
        snapshot = self.catalog.get_snapshot(snapshot_id) if snapshot_id else None
        if (
            snapshot is None
            or snapshot.reference.tier is not SnapshotTier.GOLD
            or snapshot.reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise OrchestrationFailure(
                "weekly health measurements require a registered accepted Gold snapshot"
            )
        evaluations: list[dict[str, Any]] = []
        for row in rows:
            bootstrap = row.get("record") or {}
            if not isinstance(bootstrap, Mapping):
                raise ValueError("weekly record compatibility seed must be an object")
            sleeve_id = str(bootstrap.get("sleeve_id") or row.get("sleeve_id") or "")
            if not sleeve_id:
                raise ValueError("weekly Sleeve measurement requires sleeve_id")
            record = self._record_from_catalog(sleeve_id, bootstrap)
            observation = self._health_observation(row["observation"])
            shadow_account_id = (
                str(row["shadow_account_id"])
                if row.get("shadow_account_id")
                else None
            )
            observation = SleeveHealthObservation(
                **{
                    **asdict(observation),
                    "new_sessions_since_dormant": self._shadow_sessions_since(
                        shadow_account_id,
                        record.dormant_since,
                        observation.as_of_date,
                    ),
                }
            )
            measurement_event = self._persist_health_measurement(
                record=record,
                observation=observation,
                snapshot_id=snapshot_id,
                shadow_account_id=shadow_account_id,
            )
            decision = advance_lifecycle(record, observation)
            evaluations.append(
                {
                    "sleeve_id": record.sleeve_id,
                    "as_of_date": observation.as_of_date.isoformat(),
                    "alarm_reasons": list(decision.alarm_reasons),
                    "recommended_action": decision.recommended_action,
                    "from_state": record.state.value,
                    "proposed_state": decision.record.state.value,
                    "transition_required": decision.transition is not None,
                    "record": _jsonable(asdict(record)),
                    "observation": _jsonable(asdict(observation)),
                    "measurement_event_id": measurement_event.event_id,
                    "shadow_account_id": shadow_account_id,
                }
            )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"evaluated point-in-time health for {len(evaluations)} Sleeves",
            outputs={
                "evaluations": evaluations,
                "input_path": str(path),
                "snapshot_id": snapshot_id,
            },
        )

    def _drift_detection(self, request: OperationRequest) -> OperationResult:
        health = self._dependency(
            request, OperationName.SLEEVE_HEALTH_CHECK, allow_skipped=True
        )
        if health.status == "skipped":
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no Sleeve observations available for drift detection",
                outputs={"drift_events": []},
            )
        events = [
            {
                "sleeve_id": row["sleeve_id"],
                "as_of_date": row["as_of_date"],
                "alarm_reasons": list(row["alarm_reasons"]),
                "alarm_count": len(row["alarm_reasons"]),
                "confirmed_multi_alarm": len(row["alarm_reasons"]) >= 2,
            }
            for row in health.outputs["evaluations"]
            if row["alarm_reasons"]
        ]
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"detected {len(events)} point-in-time Sleeve drift events",
            outputs={"drift_events": events},
        )

    def _lifecycle_transition(self, request: OperationRequest) -> OperationResult:
        health = self._dependency(
            request, OperationName.SLEEVE_HEALTH_CHECK, allow_skipped=True
        )
        self._dependency(request, OperationName.DRIFT_DETECTION, allow_skipped=True)
        if health.status == "skipped":
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no Sleeve observation can advance lifecycle",
                outputs={"ticks": []},
            )
        rows = health.outputs.get("evaluations")
        if not isinstance(rows, list):
            raise OrchestrationFailure(
                "authoritative health run has no persisted evaluations"
            )
        snapshot_id = str(health.outputs.get("snapshot_id") or "")
        snapshot = self.catalog.get_snapshot(snapshot_id) if snapshot_id else None
        if (
            snapshot is None
            or snapshot.reference.tier is not SnapshotTier.GOLD
            or snapshot.reference.quality_status is not DataQualityStatus.ACCEPTED
        ):
            raise OrchestrationFailure(
                "weekly lifecycle input requires a registered accepted Gold snapshot"
        )
        monitor = LifecycleMonitor(self.catalog)
        outputs: list[dict[str, Any]] = []
        active_by_sleeve = {
            case.sleeve_id: case
            for case in _active_recovery_cases(self.catalog)
        }
        for row in rows:
            record = self._lifecycle_record(row["record"])
            observation = self._health_observation(
                row["observation"], catalog_projection=True
            )
            recovery = active_by_sleeve.get(record.sleeve_id)
            result = monitor.tick(
                record,
                observation,
                snapshot_id=snapshot_id,
                active_recovery_case=recovery,
                shadow_account_id=(
                    str(row["shadow_account_id"])
                    if row.get("shadow_account_id")
                    else None
                ),
                allow_projected_deadlines=True,
            )
            outputs.append(result.to_dict())
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"persisted {len(outputs)} idempotent lifecycle monitor ticks",
            outputs={"ticks": outputs, "snapshot_id": snapshot_id},
        )

    def _recovery_sla_check(self, request: OperationRequest) -> OperationResult:
        self._dependency(
            request, OperationName.LIFECYCLE_TRANSITION, allow_skipped=True
        )
        try:
            as_of_date = _parse_date(request.partition_key, name="weekly partition")
            as_of = datetime.combine(as_of_date, time(15, 0), tzinfo=timezone.utc)
        except ValueError:
            as_of = self._now()
        active = _active_recovery_cases(self.catalog)
        if not active:
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no active recovery case requires an SLA check",
                outputs={"cases": []},
            )
        accepted_gold = [
            row
            for row in self.catalog.list_snapshots(
                limit=1_000,
                quality_status=DataQualityStatus.ACCEPTED,
                tier=SnapshotTier.GOLD,
            )
            if row.reference.as_of <= as_of
        ]
        if len(accepted_gold) >= 1_000:
            raise OrchestrationFailure(
                "recovery Gold snapshot scan reached its safety limit"
            )
        coordinator = RecoveryCoordinator(self.catalog)
        rows = []
        for case in active:
            current_case = case
            diagnosis_projection: Mapping[str, Any] | None = None
            observation_projection: Mapping[str, Any] | None = None
            if current_case.status is RecoveryCaseStatus.OPEN and accepted_gold:
                lifecycle_evidence = self.catalog.list_lifecycle_events(
                    sleeve_id=current_case.sleeve_id, limit=1_000
                )
                health_event = next(
                    (
                        event
                        for event in lifecycle_evidence
                        if event.occurred_at <= as_of
                        and event.cause
                        in {"health_measurement_recorded", "weekly_health_tick"}
                    ),
                    None,
                )
                trigger = dict(current_case.trigger_evidence)
                findings = {
                    "source": "persisted_trigger_health_and_accepted_gold",
                    "trigger_hash": content_fingerprint(
                        trigger,
                        domain="factor-lab/research-os/v1/recovery-trigger-diagnosis",
                    ),
                    "trigger_alarms": list(trigger.get("alarms") or ()),
                    "trigger_observation": _jsonable(trigger.get("observation") or {}),
                    "latest_health_event": (
                        None
                        if health_event is None
                        else {
                            "idempotency_key": health_event.idempotency_key,
                            "cause": health_event.cause,
                            "occurred_at": health_event.occurred_at.isoformat(),
                            "evidence": _jsonable(dict(health_event.evidence)),
                        }
                    ),
                }
                branches = tuple(
                    map(str, trigger.get("diagnostic_branches") or ())
                )[:2]
                try:
                    current_case = coordinator.complete_diagnosis(
                        current_case.recovery_case_id,
                        diagnosed_at=max(as_of, current_case.triggered_at),
                        snapshot_id=accepted_gold[0].reference.snapshot_id,
                        findings=findings,
                        diagnostic_branches=branches,
                    )
                    diagnosis_projection = current_case.model_dump(mode="json")
                except (RecoveryWorkflowError, ValueError) as exc:
                    raise OrchestrationFailure(
                        f"deterministic recovery diagnosis failed: {exc}"
                    ) from exc

            if current_case.status is RecoveryCaseStatus.OBSERVING:
                try:
                    observation = coordinator.evaluate_observation(
                        current_case.recovery_case_id,
                        as_of=as_of,
                    )
                    observation_projection = observation.to_dict()
                    refreshed = self.catalog.get_recovery_case(
                        current_case.recovery_case_id
                    )
                    if refreshed is not None:
                        current_case = refreshed
                except (RecoveryWorkflowError, ValueError) as exc:
                    raise OrchestrationFailure(
                        f"recovery observation evaluation failed: {exc}"
                    ) from exc

            account_id = current_case.trigger_evidence.get("shadow_account_id")
            account_id = str(account_id) if account_id else None
            shadow_session_age = self._shadow_sessions_since(
                account_id, current_case.triggered_at.date(), as_of.date()
            )
            catalog_session_age = self._catalog_sessions_since(
                current_case.triggered_at.date(), as_of.date()
            )
            session_age = max(shadow_session_age, catalog_session_age)
            lifecycle_events = self.catalog.list_lifecycle_events(
                sleeve_id=current_case.sleeve_id, limit=1000
            )
            drift_event = next(
                (
                    event
                    for event in lifecycle_events
                    if event.occurred_at >= current_case.triggered_at
                    and event.cause == "weekly_health_tick"
                    and event.evidence.get("recovery_case_id")
                    == current_case.recovery_case_id
                ),
                None,
            )
            diagnosis_event = next(
                (
                    event
                    for event in lifecycle_events
                    if event.occurred_at >= current_case.triggered_at
                    and event.cause == "recovery_diagnosis_completed"
                    and event.evidence.get("recovery_case_id")
                    == current_case.recovery_case_id
                ),
                None,
            )
            checkpoints = {
                "drift_registered": drift_event is not None,
                "diagnosis_completed": diagnosis_event is not None,
                "challenger_registered": bool(current_case.challenger_ids),
                "recovery_observation_complete": bool(
                    observation_projection
                    and observation_projection.get("observation_complete")
                ),
            }
            overdue = {
                "drift_5_sessions": session_age > 5
                and not checkpoints["drift_registered"],
                "diagnosis_20_sessions": session_age > 20
                and not checkpoints["diagnosis_completed"],
                "challenger_20_sessions": session_age > 20
                and not checkpoints["challenger_registered"],
            }
            rows.append(
                {
                    "recovery_case_id": current_case.recovery_case_id,
                    "sleeve_id": current_case.sleeve_id,
                    "status": current_case.status.value,
                    "persisted_session_age": session_age,
                    "persisted_shadow_sessions": shadow_session_age,
                    "persisted_gold_sessions": catalog_session_age,
                    "checkpoints": checkpoints,
                    "overdue": overdue,
                    "has_overdue_checkpoint": any(overdue.values()),
                    "diagnosis_projection": diagnosis_projection,
                    "observation_projection": observation_projection,
                }
            )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"audited recovery SLA for {len(rows)} active cases",
            outputs={"as_of": as_of.isoformat(), "cases": rows},
        )

    def _monthly_payload(
        self, request: OperationRequest
    ) -> tuple[dict[str, Any], Path, dict[str, Any] | None]:
        section = self._section("monthly", request)
        input_mode = str(section.get("input_mode") or "").strip()
        if input_mode == "authoritative_pg":
            forbidden = sorted(
                {
                    "input_path",
                    "research_inputs",
                    "frame_path",
                    "exposure_frame_path",
                    "returns_history_path",
                    "benchmark_weights_path",
                    "negative_controls",
                    "within_family_p_values",
                    "data_audit_blockers",
                    "statistics",
                }
                & set(section)
            )
            if forbidden:
                raise OrchestrationFailure(
                    "production monthly research rejects caller/file evidence: "
                    + ", ".join(forbidden)
                )
            return section, self._state_root / "authoritative_pg", None
        if input_mode not in {"test", "legacy_import", ""}:
            raise ServiceNotConfigured(
                "monthly.input_mode must be authoritative_pg, test, or legacy_import"
            )
        return self._optional_cycle_input("monthly", request)

    def _authoritative_monthly_submission(
        self, request: OperationRequest
    ) -> Any | None:
        explicit = str(request.metadata.get("submission_id") or "").strip()
        if explicit:
            return self.monthly_research.status(explicit)
        candidates: dict[str, Any] = {}
        for status in ("reviewed", "reserved", "running"):
            for row in self.catalog.list_research_submissions(
                limit=1_000, status=status
            ):
                candidates[row.submission_id] = row
        if not candidates:
            return None
        return sorted(
            candidates.values(), key=lambda row: (row.created_at, row.submission_id)
        )[0]

    def _authoritative_monthly_submissions(
        self, request: OperationRequest, *, maximum: int = 3
    ) -> tuple[Any, ...]:
        explicit = str(request.metadata.get("submission_id") or "").strip()
        if explicit:
            return (self.monthly_research.status(explicit),)
        candidates: dict[str, Any] = {}
        for status in ("reviewed", "reserved", "running"):
            for row in self.catalog.list_research_submissions(
                limit=1_000, status=status
            ):
                candidates[row.submission_id] = row
        selected: list[Any] = []
        selected_families: set[str] = set()
        for row in sorted(
            candidates.values(), key=lambda item: (item.created_at, item.submission_id)
        ):
            if row.family_id in selected_families:
                continue
            selected.append(row)
            selected_families.add(row.family_id)
            if len(selected) >= maximum:
                break
        return tuple(selected)

    def _automatic_monthly_proposals(
        self,
        request: OperationRequest,
        *,
        section: Mapping[str, Any],
        maximum: int = 3,
    ) -> tuple[Mapping[str, Any], ...]:
        """Invoke at most one model proposal per fixed Family and three monthly."""

        if maximum < 1 or maximum > 3:
            raise ValueError("automatic monthly proposal maximum must be 1..3")
        proposal_config = section.get("proposal") or {}
        if not isinstance(proposal_config, Mapping):
            raise ServiceNotConfigured("monthly.proposal must be an object")
        configured_maximum = int(
            proposal_config.get("max_proposals_per_month", maximum)
        )
        configured_family_maximum = int(
            proposal_config.get("max_proposals_per_family", 1)
        )
        if configured_maximum < 1 or configured_maximum > 3:
            raise ServiceNotConfigured(
                "monthly.proposal.max_proposals_per_month must be 1..3"
            )
        if configured_family_maximum != 1:
            raise ServiceNotConfigured(
                "monthly.proposal.max_proposals_per_family must equal 1"
            )
        maximum = min(maximum, configured_maximum)
        database_now = self.catalog.database_now()
        month_key = database_now.strftime("%Y-%m")
        try:
            port = proposal_port_from_config(
                proposal_config,
                env=self.env,
                production=True,
            )
        except ProposalPortError as exc:
            # A missing/invalid model profile is an auditable monthly blocker,
            # not a silent "nothing to do".  Only allow-listed public model
            # selection and credential-reference material participates in the
            # identity; secret contents are neither persisted nor hashed here.
            public_env = {
                key: str(self.env.get(key) or "")
                for key in (
                    "FACTOR_LAB_LLM_PROVIDER",
                    "FACTOR_LAB_LLM_BASE_URL",
                    "FACTOR_LAB_LLM_MODEL",
                    "FACTOR_LAB_LLM_API_KEY_REF",
                    "FACTOR_LAB_LLM_API_FORMAT",
                    "FACTOR_LAB_LLM_PROFILES_JSON",
                    "FACTOR_LAB_LLM_FALLBACK_ORDER",
                )
            }
            blocker_identity = content_fingerprint(
                {
                    "month_key": month_key,
                    "proposal_config": dict(proposal_config),
                    "public_runtime_selection": public_env,
                },
                domain="factor-lab/research-os/v1/monthly-model-proposal-blocker",
            )
            run_id = f"monthly_model_proposal_blocked_{blocker_identity[:32]}"
            message = sanitize_operational_text(
                f"automatic monthly direct-model proposal is unavailable: {exc}"
            )
            metadata = {
                "month_key": month_key,
                "family_id": None,
                "provider": {
                    "provider": str(
                        proposal_config.get("provider") or "direct_model"
                    )
                },
                "accepted": False,
                "submission_id": None,
                "configuration_available": False,
                "authority": "proposal_only_no_budget_or_execution_authority",
                "error_type": type(exc).__name__,
                "status": "blocked",
            }
            blocked = RunRecord(
                run_id=run_id,
                run_type="monthly_model_proposal",
                status="blocked",
                input_fingerprint=blocker_identity,
                started_at=database_now,
                completed_at=database_now,
                metadata=metadata,
                error=message,
            )
            stored, won = self.catalog.claim_run(blocked)
            if not won and (
                stored.input_fingerprint != blocked.input_fingerprint
                or canonical_json(stored.metadata) != canonical_json(blocked.metadata)
                or stored.status != blocked.status
                or stored.error != blocked.error
            ):
                raise OrchestrationFailure(
                    "monthly model proposal blocker identity collision"
                )
            return ({**metadata, "run_id": stored.run_id, "error": stored.error},)
        public_identity = dict(getattr(port, "public_identity", {}))
        invocation_rows = self.catalog.list_runs(
            limit=1_000, run_type="monthly_model_proposal"
        )
        if len(invocation_rows) >= 1_000:
            raise OrchestrationFailure(
                "monthly model proposal ledger reached the safety limit"
            )
        attempted_families = {
            str(row.metadata.get("family_id") or "")
            for row in invocation_rows
            if str(row.metadata.get("month_key") or "") == month_key
        }
        submissions = self.catalog.list_research_submissions(limit=1_000)
        if len(submissions) >= 1_000:
            raise OrchestrationFailure(
                "monthly submission ledger reached the safety limit"
            )
        attempted_families.update(
            row.family_id
            for row in submissions
            if row.created_at.strftime("%Y-%m") == month_key
        )
        remaining = max(0, maximum - len(attempted_families))
        if remaining == 0:
            return ()

        active_cases = _active_recovery_cases(self.catalog)
        cases_by_family: dict[str, Any] = {}
        for case in sorted(
            active_cases,
            key=lambda item: (item.triggered_at, item.recovery_case_id),
            reverse=True,
        ):
            cases_by_family.setdefault(case.sleeve_id, case)

        outcomes: list[Mapping[str, Any]] = []
        families = sorted(
            self.catalog.list_research_families(active_only=True),
            key=lambda item: item.family_id,
        )
        for family in families:
            if remaining <= 0:
                break
            if family.family_id in attempted_families:
                continue
            recovery_case = cases_by_family.get(family.family_id)
            invocation_identity = content_fingerprint(
                {
                    "month_key": month_key,
                    "family_id": family.family_id,
                    "field_registry_hash": family.registry_hash,
                    "provider": public_identity,
                },
                domain="factor-lab/research-os/v1/monthly-model-proposal",
            )
            run_id = f"monthly_model_proposal_{invocation_identity[:32]}"
            existing = self.catalog.get_run(run_id)
            if existing is not None:
                attempted_families.add(family.family_id)
                remaining -= 1
                outcomes.append(dict(existing.metadata))
                continue
            started_at = self.catalog.database_now()
            try:
                admission = self.monthly_research.propose(
                    port,
                    family_id=family.family_id,
                    recovery_case_id=(
                        None
                        if recovery_case is None
                        else recovery_case.recovery_case_id
                    ),
                )
                metadata = {
                    "month_key": month_key,
                    "family_id": family.family_id,
                    "provider": public_identity,
                    "proposal_decision_id": admission.decision.decision_id,
                    "raw_proposal_hash": admission.decision.raw_proposal_hash,
                    "accepted": admission.accepted,
                    "violations": list(admission.violations),
                    "submission_id": (
                        None
                        if admission.submission is None
                        else admission.submission.submission_id
                    ),
                    "recovery_case_id": (
                        None
                        if recovery_case is None
                        else recovery_case.recovery_case_id
                    ),
                    "authority": "proposal_only_no_budget_or_execution_authority",
                }
                run = RunRecord(
                    run_id=run_id,
                    run_type="monthly_model_proposal",
                    status="completed",
                    input_fingerprint=invocation_identity,
                    started_at=started_at,
                    completed_at=self.catalog.database_now(),
                    metadata=metadata,
                )
            except Exception as exc:
                metadata = {
                    "month_key": month_key,
                    "family_id": family.family_id,
                    "provider": public_identity,
                    "accepted": False,
                    "submission_id": None,
                    "authority": "proposal_only_no_budget_or_execution_authority",
                    "error_type": type(exc).__name__,
                }
                run = RunRecord(
                    run_id=run_id,
                    run_type="monthly_model_proposal",
                    status="failed",
                    input_fingerprint=invocation_identity,
                    started_at=started_at,
                    completed_at=self.catalog.database_now(),
                    metadata=metadata,
                    error=sanitize_operational_text(str(exc)),
                )
            stored, won = self.catalog.claim_run(run)
            if not won and (
                stored.input_fingerprint != run.input_fingerprint
                or canonical_json(stored.metadata) != canonical_json(run.metadata)
                or stored.status != run.status
            ):
                raise OrchestrationFailure(
                    "monthly model proposal invocation identity collision"
                )
            outcomes.append(metadata)
            attempted_families.add(family.family_id)
            remaining -= 1
        return tuple(outcomes)

    def _confirmatory_budget_gate(self, request: OperationRequest) -> OperationResult:
        section, path, payload = self._monthly_payload(request)
        if str(section.get("input_mode") or "").strip() == "authoritative_pg":
            submissions = self._authoritative_monthly_submissions(request)
            proposal_outcomes: tuple[Mapping[str, Any], ...] = ()
            if not submissions and not str(
                request.metadata.get("submission_id") or ""
            ).strip():
                proposal_outcomes = self._automatic_monthly_proposals(
                    request, section=section, maximum=3
                )
                submissions = self._authoritative_monthly_submissions(request)
            if not submissions:
                proposal_failures = [
                    row for row in proposal_outcomes if not bool(row.get("accepted"))
                ]
                return OperationResult(
                    operation=request.operation,
                    status="blocked" if proposal_failures else "skipped",
                    summary=(
                        "automatic model proposals produced no admissible submission"
                        if proposal_failures
                        else "no reviewed/reserved authoritative monthly submission"
                    ),
                    outputs={
                        "source": "postgresql_research_submissions",
                        "allowed": False,
                        "automatic_proposals": list(proposal_outcomes),
                    },
                )
            reserved_rows = tuple(
                self.monthly_research.reserve(submission.submission_id)
                for submission in submissions
            )
            admitted = tuple(
                row
                for row in reserved_rows
                if row.status in {"reserved", "running", "completed"}
            )
            allowed = bool(admitted)
            first = admitted[0] if admitted else reserved_rows[0]
            return OperationResult(
                operation=request.operation,
                status="completed" if allowed else "blocked",
                summary=(
                    f"{len(admitted)} authoritative monthly submissions reserved statistical budget"
                    if allowed
                    else "authoritative monthly submissions were rejected by statistical budget"
                ),
                outputs={
                    "source": "postgresql_research_submissions",
                    "allowed": allowed,
                    "submission_id": first.submission_id,
                    "submission_status": first.status,
                    "trial_id": first.trial_id,
                    "research_equivalence_hash": first.research_equivalence_hash,
                    "experiment_fingerprint": first.experiment_fingerprint,
                    "experiment_spec": first.spec.model_dump(mode="json"),
                    "recovery_case_id": first.recovery_case_id,
                    "error": first.error,
                    "submissions": [
                        {
                            "submission_id": row.submission_id,
                            "submission_status": row.status,
                            "family_id": row.family_id,
                            "trial_id": row.trial_id,
                            "research_equivalence_hash": row.research_equivalence_hash,
                            "experiment_fingerprint": row.experiment_fingerprint,
                            "recovery_case_id": row.recovery_case_id,
                            "allowed": row in admitted,
                            "error": row.error,
                        }
                        for row in reserved_rows
                    ],
                    "automatic_proposals": list(proposal_outcomes),
                },
            )
        if payload is None:
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no preregistered monthly Challenger input",
                outputs={"input_path": str(path)},
            )
        forbidden_authority = sorted(
            {"registered_at", "evidence_class", "holdout_id"} & set(payload)
        )
        if forbidden_authority:
            raise OrchestrationFailure(
                "monthly input cannot self-assert evidence authority: "
                + ", ".join(forbidden_authority)
            )
        has_proposal = "proposal" in payload
        has_experiment = "experiment" in payload
        if has_proposal == has_experiment:
            raise ServiceNotConfigured(
                "monthly input requires exactly one of proposal or experiment"
            )
        proposal_decision: dict[str, Any] | None = None
        if has_proposal:
            proposal_payload = payload.get("proposal")
            template_payload = payload.get("experiment_template")
            field_registry_payload = payload.get("field_specs")
            if not isinstance(proposal_payload, Mapping):
                raise ServiceNotConfigured("monthly proposal must be an object")
            if not isinstance(template_payload, Mapping):
                raise ServiceNotConfigured(
                    "monthly proposal requires a frozen experiment_template"
                )
            if not isinstance(field_registry_payload, Sequence) or isinstance(
                field_registry_payload, (str, bytes)
            ):
                raise ServiceNotConfigured(
                    "monthly proposal requires a trusted field_specs registry"
                )
            decision = review_llm_proposal(
                proposal_payload,
                experiment_template=template_payload,
                field_specs=field_specs_from_mapping(field_registry_payload),
            )
            persist_proposal_decision(self.catalog, decision)
            proposal_decision = decision.to_dict()
            if not decision.accepted or decision.experiment_spec is None:
                return OperationResult(
                    operation=request.operation,
                    status="blocked",
                    summary="LLM proposal failed deterministic preregistration review",
                    outputs={
                        "allowed": False,
                        "proposal_decision": proposal_decision,
                        "reasons": list(decision.violations),
                    },
                )
            spec = decision.experiment_spec
        else:
            spec_payload = payload.get("experiment")
            if not isinstance(spec_payload, Mapping):
                raise ServiceNotConfigured("monthly input requires experiment")
            spec = ExperimentSpec.model_validate(spec_payload)
        fingerprint = spec.fingerprint()
        existing_experiment = self.catalog.get_experiment_by_fingerprint(fingerprint)
        existing_authoritative_result = bool(
            existing_experiment is not None
            and self.catalog.get_authoritative_result(existing_experiment.experiment_id)
            is not None
        )
        registration = TrialRegistration(
            trial_id=f"trial_{fingerprint[:32]}",
            experiment_fingerprint=fingerprint,
            hypothesis_id=spec.preregistration.hypothesis_id,
            family=spec.family,
            kind=TrialKind.CONFIRMATORY,
            # Admission time comes from the authoritative database clock.  A
            # replayed partition or backfilled request therefore consumes the
            # current month's budget and cannot masquerade as an old trial.
            registered_at=self.catalog.database_now(),
            holdout_id=HISTORICAL_HOLDOUT_ID,
            requested_evidence_class=EvidenceClass.PSEUDO_OOS,
        )
        budget = spec.validation.statistical_budget
        reservation = self.catalog.reserve_trial(
            registration,
            candidate_id=spec.candidate_id,
            experiment_id=(
                None if existing_experiment is None else existing_experiment.experiment_id
            ),
            maximum_monthly_confirmatory_trials=budget.maximum_confirmatory_challengers_per_month,
            maximum_monthly_confirmatory_trials_per_family=budget.maximum_confirmatory_challengers_per_family_per_month,
            maximum_diagnostic_branches=budget.maximum_diagnostic_branches,
        )
        admission = reservation.admission
        return OperationResult(
            operation=request.operation,
            status="completed" if admission.allowed else "blocked",
            summary=(
                "confirmatory trial fits the lifetime and monthly budget"
                if admission.allowed
                else "confirmatory trial exceeds or duplicates its statistical budget"
            ),
            outputs={
                "allowed": admission.allowed,
                "evidence_class": admission.evidence_class.value,
                "family_trial_index": admission.family_trial_index,
                "reasons": list(admission.reasons),
                "fingerprint": fingerprint,
                "trial_id": registration.trial_id,
                "reservation_created": reservation.created,
                "existing_authoritative_result": existing_authoritative_result,
                "experiment_spec": spec.model_dump(mode="json"),
                "proposal_decision": proposal_decision,
            },
        )

    def _limited_discovery(self, request: OperationRequest) -> OperationResult:
        gate = self._dependency(
            request, OperationName.CONFIRMATORY_BUDGET_GATE, allow_skipped=True
        )
        if gate.status == "skipped":
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no admitted Challenger experiment",
                outputs={},
            )
        if not bool(gate.outputs.get("allowed")):
            raise OrchestrationFailure("confirmatory budget gate did not admit experiment")
        section, _, payload = self._monthly_payload(request)
        if str(section.get("input_mode") or "").strip() == "authoritative_pg":
            raw_submissions = gate.outputs.get("submissions")
            if isinstance(raw_submissions, Sequence) and not isinstance(
                raw_submissions, (str, bytes)
            ):
                submission_ids = tuple(
                    str(row.get("submission_id") or "").strip()
                    for row in raw_submissions
                    if isinstance(row, Mapping) and bool(row.get("allowed"))
                )
            else:
                submission_ids = (
                    str(gate.outputs.get("submission_id") or "").strip(),
                )
            submission_ids = tuple(item for item in submission_ids if item)
            if not submission_ids:
                raise OrchestrationFailure(
                    "authoritative monthly budget gate omitted admitted submission_ids"
                )
            executions = tuple(
                self.monthly_research.run(
                    submission_id,
                    worker_id=(
                        f"dagster-{_safe_name(request.run_id)}-"
                        f"{_safe_name(submission_id)[:24]}"
                    ),
                )
                for submission_id in submission_ids
            )
            execution_rows = [
                {
                    "submission": _jsonable(asdict(execution.submission)),
                    "claimed": execution.claimed,
                    "result": (
                        None
                        if execution.result is None
                        else execution.result.to_dict()
                    ),
                    "shadow_account_id": execution.shadow_account_id,
                }
                for execution in executions
            ]
            terminals = [execution.submission.status for execution in executions]
            first_execution = executions[0]
            outputs = {
                "source": "postgresql_gold_catalog_coordinator",
                "submission": _jsonable(asdict(first_execution.submission)),
                "claimed": first_execution.claimed,
                "result": (
                    None
                    if first_execution.result is None
                    else first_execution.result.to_dict()
                ),
                "shadow_account_id": first_execution.shadow_account_id,
                "executions": execution_rows,
            }
            if all(
                not execution.claimed and execution.submission.status == "running"
                for execution in executions
            ):
                return OperationResult(
                    operation=request.operation,
                    status="skipped",
                    summary="authoritative monthly submissions have active worker leases",
                    outputs=outputs,
                )
            status = (
                "failed"
                if "failed" in terminals
                else "blocked"
                if "missing_data" in terminals
                else "completed"
                if all(item == "completed" for item in terminals)
                else "skipped"
            )
            return OperationResult(
                operation=request.operation,
                status=status,
                summary=(
                    "authoritative monthly submissions finished as "
                    + ",".join(terminals)
                ),
                outputs=outputs,
            )
        assert payload is not None
        spec_payload = gate.outputs.get("experiment_spec")
        if not isinstance(spec_payload, Mapping):
            raise OrchestrationFailure(
                "confirmatory budget gate did not persist its admitted experiment contract"
            )
        spec = ExperimentSpec.model_validate(spec_payload)
        frame_value = payload.get("frame_path")
        if not frame_value:
            raise ServiceNotConfigured("monthly input frame_path is required")
        frame_path = self._input_path(frame_value)
        if not frame_path.is_file():
            raise OrchestrationFailure(f"historical research frame is missing: {frame_path}")
        controls = tuple(
            NegativeControlMetric(
                control_name=str(row["control_name"]),
                metric=float(row["metric"]),
                passed_promotion_gate=bool(row.get("passed_promotion_gate", False)),
            )
            for row in payload.get("negative_controls") or ()
        )
        exposure_frame = (
            None
            if not payload.get("exposure_frame_path")
            else read_frame(self._input_path(payload["exposure_frame_path"]))
        )
        returns_history = (
            None
            if not payload.get("returns_history_path")
            else read_frame(self._input_path(payload["returns_history_path"]))
        )
        benchmark_weights = (
            None
            if not payload.get("benchmark_weights_path")
            else read_frame(self._input_path(payload["benchmark_weights_path"]))
        )
        result = HistoricalResearchCycle(self.catalog).run(
            spec,
            read_frame(frame_path),
            field_specs=field_specs_from_mapping(payload.get("field_specs") or ()),
            sleeve_signal=payload.get("sleeve_signal"),
            negative_controls=controls,
            within_family_p_values=tuple(
                float(value) for value in payload.get("within_family_p_values") or ()
            ),
            data_audit_blockers=tuple(
                map(str, payload.get("data_audit_blockers") or ())
            ),
            bootstrap_resamples=int(payload.get("bootstrap_resamples", 2_000)),
            seed=int(payload.get("seed", 0)),
            exposure_frame=exposure_frame,
            returns_history=returns_history,
            benchmark_weights=benchmark_weights,
            optimization_policy=payload.get("optimization_policy"),
        )
        recovery_case_id = str(payload.get("recovery_case_id") or "").strip()
        recovery_case: Mapping[str, Any] | None = None
        if recovery_case_id:
            try:
                registered = RecoveryCoordinator(self.catalog).register_challengers(
                    recovery_case_id,
                    (result.experiment_id,),
                    registered_at=self._now(),
                )
                recovery_case = registered.model_dump(mode="json")
            except (RecoveryWorkflowError, ValueError) as exc:
                raise OrchestrationFailure(
                    f"research result could not register as recovery Challenger: {exc}"
                ) from exc
        outputs = result.to_dict()
        if recovery_case is not None:
            outputs["recovery_case"] = dict(recovery_case)
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=(
                f"historical research completed with verdict {result.promotion_verdict}"
            ),
            outputs=outputs,
        )

    def _weight_reestimation(self, request: OperationRequest) -> OperationResult:
        self._dependency(request, OperationName.LIMITED_DISCOVERY, allow_skipped=True)
        section, _, payload = self._monthly_payload(request)
        weights = section.get("weights")
        if not isinstance(weights, Mapping):
            raise ServiceNotConfigured("monthly.weights must be configured")
        input_mode = str(weights.get("input_mode") or "authoritative_pg")
        if input_mode not in {"authoritative_pg", "test", "legacy_import"}:
            raise ServiceNotConfigured(
                "monthly.weights.input_mode must be authoritative_pg, test, or legacy_import"
            )
        state_path_value = weights.get("state_history_path")
        returns_path_value = weights.get("sleeve_returns_path")
        if input_mode in {"test", "legacy_import"} and bool(state_path_value) != bool(
            returns_path_value
        ):
            raise ServiceNotConfigured(
                "monthly.weights requires both state_history_path and sleeve_returns_path"
            )
        overlay: dict[str, Any] = {
            "status": (
                "authoritative_pg_static_only"
                if input_mode == "authoritative_pg"
                else "not_configured"
            ),
            "weights": {},
            "predictions": {},
        }
        if (
            input_mode in {"test", "legacy_import"}
            and state_path_value
            and returns_path_value
        ):
            state_path = self._input_path(state_path_value)
            returns_path = self._input_path(returns_path_value)
            if not state_path.is_file() or not returns_path.is_file():
                raise OrchestrationFailure("state-conditioned weight inputs are missing")
            state = read_frame(state_path)
            sleeve_returns = read_frame(returns_path)
            date_column = str(weights.get("date_column") or "date")
            if date_column not in state or date_column not in sleeve_returns:
                raise ValueError("weight input frames require their configured date column")
            state = state.set_index(pd.to_datetime(state.pop(date_column)))
            sleeve_returns = sleeve_returns.set_index(
                pd.to_datetime(sleeve_returns.pop(date_column))
            )
            overlay = fit_state_conditioned_overlay(
                state,
                sleeve_returns,
                ridge_alpha=float(weights.get("ridge_alpha", 100.0)),
                min_observations=int(weights.get("minimum_observations", 60)),
            )
            overlay["input_mode"] = input_mode
        configured_snapshot_id = str(weights.get("data_snapshot_id") or "").strip()
        if configured_snapshot_id:
            snapshot_record = self.catalog.get_snapshot(configured_snapshot_id)
            if snapshot_record is None:
                raise OrchestrationFailure(
                    "monthly.weights.data_snapshot_id is not cataloged"
                )
        else:
            snapshot_record = self._latest_gold_snapshot()
        if snapshot_record is None:
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no cataloged Gold snapshot exists for Champion projection",
                outputs={"source": "postgresql_authoritative_catalog"},
            )
        control = AuthoritativeChampionControl(
            self.catalog, shadow_authority=self.shadow_authority
        )
        generated_at = self._now()
        previous = control.latest_allocation(through=generated_at)
        try:
            projection = control.build_allocation(
                data_snapshot_id=snapshot_record.reference.snapshot_id,
                generated_at=generated_at,
                # State-model output is only a proposal here.  It may not
                # enter an allocation until the later Challenger gate proves
                # stitched outer-OOS superiority and >=60 new shadow sessions.
                adaptive_scores={},
                previous=previous,
                adaptive_fraction=float(weights.get("adaptive_fraction", 0.25)),
                max_monthly_change=float(weights.get("max_monthly_change", 0.05)),
            )
            projection_run = control.persist_allocation(projection)
        except (ChampionControlError, ValueError) as exc:
            raise OrchestrationFailure(
                f"authoritative Champion projection blocked: {exc}"
            ) from exc
        artifact = {
            "schema_version": projection.schema_version,
            "partition_key": request.partition_key,
            "source": "postgresql_authoritative_promoted_sleeves",
            "state_input_mode": input_mode,
            "projection": projection.to_dict(),
            # Stable compatibility names for the read model.  They now point
            # to catalog-derived facts, never configuration-supplied Sleeves.
            "static_champion": dict(projection.static_allocation),
            "state_overlay": dict(projection.state_overlay),
            "proposed_state_overlay": _jsonable(overlay),
            "proposed_adaptive_scores": dict(overlay.get("weights") or {}),
            "challenger_allocation": dict(projection.effective_allocation),
            "projection_run_id": projection_run.run_id,
        }
        path = _write_json_once(
            self._stage_paths(request).artifacts / "champion_challenger_weights.json",
            artifact,
        )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=(
                "persisted catalog-authoritative Champion allocation; "
                f"{len(projection.candidates)} promoted Sleeve(s) considered"
            ),
            outputs={**artifact, "artifact_path": str(path)},
        )

    def _challenger_generation(self, request: OperationRequest) -> OperationResult:
        weights = self._dependency(request, OperationName.WEIGHT_REESTIMATION, allow_skipped=True)
        if weights.status == "skipped":
            return OperationResult(
                operation=request.operation,
                status="skipped",
                summary="no Champion allocation exists for Challenger comparison",
                outputs={},
            )
        section, _, _ = self._monthly_payload(request)
        config = section.get("challenger")
        if not isinstance(config, Mapping):
            raise ServiceNotConfigured("monthly.challenger must be configured")
        forbidden_files = {
            "historical_challenger_path",
            "historical_champion_path",
            "shadow_challenger_path",
            "shadow_champion_path",
        }
        asserted_files = sorted(forbidden_files & set(config))
        if asserted_files:
            raise OrchestrationFailure(
                "production Challenger evidence cannot come from caller return files: "
                + ", ".join(asserted_files)
            )
        if "historical_champion_experiment_id" in config:
            raise OrchestrationFailure(
                "historical Champion identity is derived from the active immutable role/roster; "
                "callers cannot select a completed result"
            )
        authority_mode = str(config.get("authority_mode") or "explicit_ids").strip()
        required = {
            "historical_challenger_experiment_id",
            "shadow_challenger_account_id",
            "shadow_champion_account_id",
        }
        identifiers: dict[str, str] = {}
        role_blocker: str | None = None
        if authority_mode == "catalog_roles":
            if self.shadow_authority is None:
                role_blocker = "formal PostgreSQL shadow authority is unavailable"
            else:
                event = max(
                    self.catalog.iter_lifecycle_events(
                        cause="challenger_shadow_account_bound"
                    ),
                    key=lambda item: (item.occurred_at, item.idempotency_key),
                    default=None,
                )
                if event is None:
                    role_blocker = "no promoted Challenger role binding exists"
                else:
                    promotion = event.evidence.get("promotion")
                    challenger_experiment_id = (
                        str(promotion.get("experiment_id") or "").strip()
                        if isinstance(promotion, Mapping)
                        else ""
                    )
                    challenger_account_id = str(
                        event.evidence.get("shadow_account_id") or ""
                    ).strip()
                    challenger_binding = (
                        None
                        if not challenger_experiment_id
                        else self.shadow_authority.active_binding(
                            role=ShadowRole.CHALLENGER,
                            role_key=challenger_experiment_id,
                        )
                    )
                    champion_role_key = str(
                        config.get("champion_role") or "static_champion"
                    ).strip()
                    champion_binding = self.shadow_authority.active_binding(
                        role=ShadowRole.CHAMPION,
                        role_key=champion_role_key,
                    )
                    if (
                        challenger_binding is None
                        or challenger_binding.account_id != challenger_account_id
                    ):
                        role_blocker = "latest Challenger lifecycle binding is not active"
                    elif champion_binding is None:
                        role_blocker = "static Champion role binding is absent"
                    elif not champion_binding.experiment_id:
                        role_blocker = (
                            "static Champion role has no historical experiment binding"
                        )
                    else:
                        identifiers = {
                            "historical_challenger_experiment_id": challenger_experiment_id,
                            "shadow_challenger_account_id": challenger_binding.account_id,
                            "shadow_champion_account_id": champion_binding.account_id,
                        }
        elif authority_mode in {"explicit_ids", "test", "legacy_import"}:
            missing = sorted(required - set(config))
            if missing:
                raise ServiceNotConfigured(
                    f"monthly.challenger fields missing: {missing}"
                )
            identifiers = {key: str(config[key]) for key in required}
        else:
            raise ServiceNotConfigured(
                "monthly.challenger.authority_mode must be catalog_roles or an explicit test/legacy mode"
            )
        policy = ChampionChallengePolicy(
            **dict(config.get("policy") or {})
        )
        control = AuthoritativeChampionControl(
            self.catalog, shadow_authority=self.shadow_authority
        )
        authority: Mapping[str, Any] | None = None
        authority_blocker: str | None = None
        if role_blocker is not None:
            authority_blocker = role_blocker
            decision_payload = {
                "decision": "retain_champion",
                "checks": {"authoritative_evidence": False},
                "metrics": {"shadow_sessions": 0},
                "fallback": "static_champion",
            }
        else:
            try:
                decision, authority = control.evaluate_authoritative_challenger(
                    historical_challenger_experiment_id=str(
                        identifiers["historical_challenger_experiment_id"]
                    ),
                    shadow_challenger_account_id=str(
                        identifiers["shadow_challenger_account_id"]
                    ),
                    shadow_champion_account_id=str(
                        identifiers["shadow_champion_account_id"]
                    ),
                    policy=policy,
                )
                decision_payload = decision.to_dict()
            except ChampionControlError as exc:
                # Missing epoch, a 59-session window, a changed result hash, or a
                # broken shadow chain all deterministically retain the static
                # Champion.  None can be papered over with a local return file.
                authority_blocker = str(exc)
                decision_payload = {
                    "decision": "retain_champion",
                    "checks": {"authoritative_evidence": False},
                    "metrics": {"shadow_sessions": 0},
                    "fallback": "static_champion",
                }
        adaptive_projection: ChampionAllocationProjection | None = None
        adaptive_approval_run_id: str | None = None
        proposed_scores = {
            str(key): float(value)
            for key, value in dict(
                weights.outputs.get("proposed_adaptive_scores") or {}
            ).items()
        }
        if (
            decision_payload["decision"] == "challenger_research_recovered"
            and proposed_scores
            and authority is not None
        ):
            try:
                approval = control.persist_adaptive_approval(
                    decision_payload,
                    proposed_scores,
                    authority=authority,
                    generated_at=self._now(),
                    source_partition=request.partition_key,
                )
                base = ChampionAllocationProjection.from_dict(
                    dict(weights.outputs["projection"])
                )
                adaptive_projection = control.build_allocation(
                    data_snapshot_id=base.data_snapshot_id,
                    generated_at=max(
                        self._now(), base.generated_at + timedelta(microseconds=1)
                    ),
                    adaptive_scores=proposed_scores,
                    previous=base,
                    adaptive_fraction=float(
                        (section.get("weights") or {}).get("adaptive_fraction", 0.25)
                    ),
                    max_monthly_change=float(
                        (section.get("weights") or {}).get("max_monthly_change", 0.05)
                    ),
                    adaptive_approval_run_id=approval.run_id,
                )
                control.persist_allocation(adaptive_projection)
                adaptive_approval_run_id = approval.run_id
            except (ChampionControlError, KeyError, TypeError, ValueError) as exc:
                raise OrchestrationFailure(
                    f"approved Challenger overlay could not be persisted: {exc}"
                ) from exc
        artifact = {
            "schema_version": "research-os/challenger-decision/v1",
            "partition_key": request.partition_key,
            **decision_payload,
            "authority": None if authority is None else dict(authority),
            "authority_blocker": authority_blocker,
            "adaptive_approval_run_id": adaptive_approval_run_id,
            "adaptive_projection": (
                None
                if adaptive_projection is None
                else adaptive_projection.to_dict()
            ),
            "fallback_projection_id": weights.outputs.get("projection_run_id"),
        }
        path = _write_json_once(
            self._stage_paths(request).artifacts / "challenger_decision.json", artifact
        )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"Challenger decision: {decision_payload['decision']}",
            outputs={**artifact, "artifact_path": str(path)},
        )

    def _validation_protocol_audit(self, request: OperationRequest) -> OperationResult:
        quarterly = self._section("quarterly", request)
        raw = quarterly.get("validation_protocol")
        if not isinstance(raw, Mapping):
            raise ServiceNotConfigured("quarterly.validation_protocol must be configured")
        protocol = ValidationProtocol.model_validate(raw)
        payload = protocol.model_dump(mode="json")
        digest = content_fingerprint(
            payload, domain="factor-lab/research-os/v1/validation-protocol-audit"
        )
        path = _write_json_once(
            self._stage_paths(request).artifacts / "validation_protocol_audit.json",
            {
                "schema_version": "research-os/validation-protocol-audit/v1",
                "partition_key": request.partition_key,
                "protocol_hash": digest,
                "protocol": payload,
                "thresholds_modified": False,
            },
        )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary="validated and froze the quarterly validation protocol",
            outputs={"protocol_hash": digest, "artifact_path": str(path)},
        )

    def _research_budget_audit(self, request: OperationRequest) -> OperationResult:
        protocol_result = self._dependency(
            request, OperationName.VALIDATION_PROTOCOL_AUDIT
        )
        trials = self.catalog.list_trials()
        families: dict[str, dict[str, Any]] = {}
        for trial in trials:
            row = families.setdefault(
                trial.family, {"trial_count": 0, "alpha_spent": 0.0, "outcomes": {}}
            )
            row["trial_count"] += 1
            row["alpha_spent"] += float(trial.alpha_spent)
            outcome = trial.outcome.value
            row["outcomes"][outcome] = row["outcomes"].get(outcome, 0) + 1
        payload = {
            "schema_version": "research-os/research-budget-audit/v1",
            "partition_key": request.partition_key,
            "validation_protocol_hash": protocol_result.outputs["protocol_hash"],
            "lifetime_trial_count": len(trials),
            "families": families,
            "thresholds_modified": False,
        }
        path = _write_json_once(
            self._stage_paths(request).artifacts / "research_budget_audit.json", payload
        )
        return OperationResult(
            operation=request.operation,
            status="completed",
            summary=f"audited {len(trials)} lifetime trials without changing thresholds",
            outputs={**payload, "artifact_path": str(path)},
        )

    def poll(self, sensor_name: str, cursor: str | None) -> TriggerPoll:
        if sensor_name == "new_trading_partition":
            return self._poll_trading_partitions(cursor)
        if sensor_name == "recovery_sla_due":
            return self._poll_recovery_sla(cursor)
        return TriggerPoll(cursor=cursor, message=f"unknown sensor {sensor_name!r}")

    def _has_open_data_incident(self) -> bool:
        ledger = getattr(self, "production_ledger", None)
        iter_incidents = getattr(ledger, "iter_incidents", None)
        if not callable(iter_incidents):
            return False
        incidents = iter_incidents(status=IncidentStatus.OPEN)
        try:
            return next(incidents, None) is not None
        finally:
            close = getattr(incidents, "close", None)
            if close is not None:
                close()

    def _production_lifecycle_fleet(
        self,
        *,
        overlay_open_incidents: bool = True,
    ) -> tuple[tuple[SleeveLifecycleRecord, ...], dict[str, tuple[str, ...]]]:
        if self.sleeve_roster is None:
            raise OrchestrationFailure(
                "production incident handling requires the fixed Sleeve roster"
            )
        records: list[SleeveLifecycleRecord] = []
        for entry in self.sleeve_roster.entries:
            sleeve_id = entry.sleeve.sleeve_id
            state = self.catalog.latest_lifecycle_state(sleeve_id)
            records.append(
                SleeveLifecycleRecord(
                    sleeve_id=sleeve_id,
                    state=SleeveState(
                        (state or LifecycleState.PROPOSED).value
                    ),
                )
            )
        if not records:
            raise OrchestrationFailure(
                "production incident handling found no registered lifecycle records"
            )
        if overlay_open_incidents and self._has_open_data_incident():
            records = [
                replace(
                    record,
                    state=SleeveState.FROZEN_DATA,
                    effective_weight=0.0,
                )
                for record in records
            ]
        # A data failure freezes the entire research fleet.  Every open shadow
        # account therefore receives the same cash intent; actual positions
        # remain unchanged until a trusted priced execution consumes it.
        account_ids = tuple(
            sorted(
                account.account_id
                for account in self.catalog.iter_shadow_accounts(status="active")
            )
        )
        return tuple(records), {
            record.sleeve_id: account_ids for record in records
        }

    def _infer_failed_data_stage(
        self, partition_key: str, *, require_incomplete: bool = False
    ) -> tuple[IncidentStage, DataPipelineStage, str | None, tuple[str, ...]]:
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "unexpected production data failures require the PostgreSQL partition ledger"
            )
        order = (
            ("stage_source", IncidentStage.SOURCE, DataPipelineStage.SOURCE),
            ("stage_silver", IncidentStage.SILVER, DataPipelineStage.SILVER),
            (
                "stage_data_quality",
                IncidentStage.DATA_QUALITY,
                DataPipelineStage.DATA_QUALITY,
            ),
            ("stage_gold", IncidentStage.GOLD, DataPipelineStage.GOLD),
            (
                "stage_shadow",
                IncidentStage.SHADOW_EXECUTION,
                DataPipelineStage.SHADOW_EXECUTION,
            ),
        )
        completed_hashes: list[str] = []
        for dataset, incident_stage, pipeline_stage in order:
            identity = PartitionIdentity(
                source_id="research_os",
                dataset=dataset,
                partition_key=partition_key,
            )
            record = self.production_ledger.get_partition(identity)
            retry_selector = getattr(
                self.production_ledger, "get_retry_partition", None
            )
            retry_record = (
                retry_selector(identity) if callable(retry_selector) else None
            )
            if retry_record is not None:
                record = retry_record
            if record is None or record.status is not PartitionStatus.SUCCEEDED:
                return (
                    incident_stage,
                    pipeline_stage,
                    (
                        identity.partition_run_id
                        if record is None
                        else record.identity.partition_run_id
                    ),
                    tuple(completed_hashes),
                )
            if record.output_hash:
                completed_hashes.append(record.output_hash)
        raise NonDataPipelineFailure(
            "daily data outcome claimed a failure but every durable stage succeeded"
        )

    def resume_pending_incident_controls(
        self,
        *,
        worker_id: str,
        limit: int = 100,
        lease_for: timedelta = timedelta(minutes=5),
    ) -> Mapping[str, Any]:
        """Replay crash-left incident controls from their durable origin only."""

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "incident control resume requires the PostgreSQL ledger"
            )

        def materialize(authority: IncidentRecord) -> Mapping[str, Any]:
            payload = dict(authority.payload)
            payload.pop("resolution", None)
            dagster_run_id = str(payload.get("dagster_run_id") or "").strip()
            failed_step_key = str(payload.get("failed_step_key") or "").strip()
            domain_incident_id = str(
                payload.get("domain_incident_id") or ""
            ).strip()
            if not (dagster_run_id and failed_step_key and domain_incident_id):
                raise OrchestrationFailure(
                    "incident control action lacks a formal durable origin"
                )
            pipeline_stage = DataPipelineStage(authority.stage.value)
            return self.report_unexpected_data_failure(
                authority.partition_key,
                message=authority.message,
                occurred_at=authority.occurred_at,
                dagster_run_id=dagster_run_id,
                failed_step_key=failed_step_key,
                error_code=authority.error_code,
                expected_failure_stage=pipeline_stage,
                _locked_authority=authority,
                _failure_context=(
                    authority.stage,
                    pipeline_stage,
                    authority.partition_run_id,
                    authority.evidence_hashes,
                    (
                        None
                        if payload.get("failed_partition_input_hash") is None
                        else str(payload["failed_partition_input_hash"])
                    ),
                ),
                _was_preexisting=True,
            )

        completed = self.production_ledger.resume_incident_controls(
            owner=worker_id,
            apply_effects=materialize,
            limit=limit,
            lease_for=lease_for,
        )
        return {
            "status": "completed",
            "worker_id": worker_id,
            "completed_count": len(completed),
            "action_ids": [item.action_id for item in completed],
            "incident_ids": [item.incident_id for item in completed],
        }

    def report_unexpected_data_failure(
        self,
        partition_key: str,
        *,
        message: str,
        occurred_at: datetime,
        dagster_run_id: str,
        failed_step_key: str,
        error_code: str = "dagster_run_failure",
        expected_failure_stage: DataPipelineStage | str | None = None,
        _locked_authority: IncidentRecord | None = None,
        _failure_context: tuple[
            IncidentStage,
            DataPipelineStage,
            str | None,
            tuple[str, ...],
            str | None,
        ]
        | None = None,
        _was_preexisting: bool = False,
    ) -> Mapping[str, Any]:
        """Freeze the fleet from durable PG stage facts after an unexpected run failure."""

        if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        normalized_partition_key = str(partition_key or "").strip()
        normalized_dagster_run_id = dagster_run_id.strip()
        normalized_failed_step_key = failed_step_key.strip()
        if not normalized_dagster_run_id or not normalized_failed_step_key:
            raise ValueError("dagster_run_id and failed_step_key are required")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,159}", error_code):
            raise ValueError("error_code contains unsupported characters or length")
        expected_stage = (
            None
            if expected_failure_stage is None
            else DataPipelineStage(expected_failure_stage)
        )
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "production data failures require the PostgreSQL incident ledger"
            )
        if _failure_context is None:
            incident_stage, pipeline_stage, partition_run_id, evidence = (
                self._infer_failed_data_stage(
                    normalized_partition_key,
                    require_incomplete=expected_stage is not None,
                )
            )
            failed_stage_record = self.production_ledger.get_partition_by_run_id(
                partition_run_id
            )
            failed_input_hash = (
                None
                if failed_stage_record is None
                else failed_stage_record.input_hash
            )
        else:
            (
                incident_stage,
                pipeline_stage,
                partition_run_id,
                evidence,
                failed_input_hash,
            ) = _failure_context
        if expected_stage is not None and pipeline_stage is not expected_stage:
            raise OrchestrationFailure(
                "daily data outcome stage differs from the durable PostgreSQL failure"
            )
        safe_message = sanitize_operational_text(message)
        normalized_occurred_at = occurred_at.astimezone(timezone.utc)
        if normalized_occurred_at > self.catalog.database_now():
            raise OrchestrationFailure(
                "data incident timestamp is after the database clock"
            )
        outcome = DailyDataOutcome(
            partition_key=normalized_partition_key,
            status=DailyDataStatus.BLOCKED,
            occurred_at=normalized_occurred_at,
            failure_stage=pipeline_stage,
            error_code=error_code,
            message=safe_message,
            evidence_hashes=evidence,
        )
        domain_incident = outcome.to_incident()
        normalized_evidence = domain_incident.evidence_hashes
        incident_payload = {
            "dagster_run_id": normalized_dagster_run_id,
            "failed_step_key": normalized_failed_step_key,
            "domain_incident_id": domain_incident.incident_id,
            "failed_partition_input_hash": failed_input_hash,
        }
        expected_incident_hash = content_fingerprint(
            {
                "partition_key": normalized_partition_key,
                "stage": incident_stage.value,
                "error_code": error_code,
                "occurred_at": normalized_occurred_at,
                "partition_run_id": partition_run_id,
                "source_ids": (),
                "evidence_hashes": normalized_evidence,
                "payload": incident_payload,
            },
            domain="factor-lab/research-os/v1/data-incident",
        )
        expected_incident_id = f"incident_{expected_incident_hash[:64]}"
        controls_locked = _locked_authority is not None
        preexisting = None
        if not controls_locked:
            incidents = self.production_ledger.iter_incidents()
            try:
                observed_incidents = tuple(incidents)
            finally:
                close = getattr(incidents, "close", None)
                if close is not None:
                    close()
            preexisting = next(
                (
                    item
                    for item in observed_incidents
                    if item.incident_id == expected_incident_id
                ),
                None,
            )
            same_run_incidents = tuple(
                item
                for item in observed_incidents
                if item.partition_key == normalized_partition_key
                and str(item.payload.get("dagster_run_id") or "").strip()
                == normalized_dagster_run_id
            )
            if any(
                item.incident_id != expected_incident_id
                for item in same_run_incidents
            ):
                # The daily de-risk path can intentionally persist an incident
                # and then make daily_integrity fail the Dagster run.  Its
                # failure sensor is secondary notification for the same run,
                # not authority to mint a second incident with a new step key.
                raise NonDataPipelineFailure(
                    "Dagster run already has a durable data incident for this partition"
                )

            ledger_incident = self.production_ledger.record_incident(
                partition_key=normalized_partition_key,
                stage=incident_stage,
                error_code=error_code,
                message=safe_message,
                occurred_at=normalized_occurred_at,
                partition_run_id=partition_run_id,
                evidence_hashes=normalized_evidence,
                payload=incident_payload,
            )
        else:
            assert _locked_authority is not None
            ledger_incident = _locked_authority

        def origin_is_intact(item: Any) -> bool:
            origin_payload = dict(item.payload)
            origin_payload.pop("resolution", None)
            return bool(
                item.incident_id == expected_incident_id
                and item.incident_hash == expected_incident_hash
                and item.partition_key == normalized_partition_key
                and item.stage is incident_stage
                and item.error_code == error_code
                and item.message == safe_message
                and item.occurred_at == normalized_occurred_at
                and item.partition_run_id == partition_run_id
                and item.source_ids == ()
                and item.evidence_hashes == normalized_evidence
                and origin_payload == incident_payload
            )

        if not origin_is_intact(ledger_incident):
            raise OrchestrationFailure(
                "production data incident identity differs from its durable origin"
            )
        if ledger_incident.status is not IncidentStatus.OPEN:
            raise OrchestrationFailure(
                "exact production data incident is already terminal"
            )

        # A Dagster process can die after claiming a production stage but
        # before its handler reaches the normal terminal CAS.  Bind the exact
        # OPEN incident's durable run/step origin to that abandoned lease now;
        # this fences the old token and gives repair generation selection an
        # immutable FAILED parent.  Existing handler-written terminal rows are
        # returned unchanged.
        self.production_ledger.terminalize_incident_partition(
            ledger_incident.incident_id
        )

        if not controls_locked:
            failure_context = (
                incident_stage,
                pipeline_stage,
                partition_run_id,
                evidence,
                failed_input_hash,
            )
            with self.production_ledger.incident_control_guard(
                expected_incident_id
            ) as locked_authority:
                return self.report_unexpected_data_failure(
                    partition_key,
                    message=message,
                    occurred_at=occurred_at,
                    dagster_run_id=dagster_run_id,
                    failed_step_key=failed_step_key,
                    error_code=error_code,
                    expected_failure_stage=expected_failure_stage,
                    _locked_authority=locked_authority,
                    _failure_context=failure_context,
                    _was_preexisting=preexisting is not None,
                )

        lifecycle_records, shadow_accounts = self._production_lifecycle_fleet(
            overlay_open_incidents=False
        )
        cash_intent = CashTargetIntent.for_incident(domain_incident)
        expected_lifecycle_evidence = {
            "data_incident": domain_incident.to_dict(),
            "cash_target_intent": cash_intent.to_dict(),
        }

        def exact_lifecycle_event(sleeve_id: str) -> LifecycleEvent | None:
            expected_key = (
                f"data-incident:{domain_incident.incident_id}:{sleeve_id}"
            )
            events = self.catalog.iter_lifecycle_events(
                sleeve_id=sleeve_id,
                cause="data_integrity_failure",
            )
            try:
                event = next(
                    (
                        item
                        for item in events
                        if item.idempotency_key == expected_key
                    ),
                    None,
                )
            finally:
                close = getattr(events, "close", None)
                if close is not None:
                    close()
            if event is None:
                return None
            if not (
                event.sleeve_id == sleeve_id
                and event.to_state is LifecycleState.FROZEN_DATA
                and event.cause == "data_integrity_failure"
                and event.occurred_at >= normalized_occurred_at
                and canonical_json(event.evidence)
                == canonical_json(expected_lifecycle_evidence)
            ):
                raise OrchestrationFailure(
                    "production data incident lifecycle evidence is inconsistent"
                )
            return event

        replay_records: list[SleeveLifecycleRecord] = []
        for record in lifecycle_records:
            prior_event = exact_lifecycle_event(record.sleeve_id)
            replay_records.append(
                record
                if prior_event is None or prior_event.from_state is None
                else replace(
                    record,
                    state=SleeveState(prior_event.from_state.value),
                )
            )

        control_result = ProductionDailyControl(
            self.catalog, shadow_authority=self.shadow_authority
        ).run(
            outcome=outcome,
            lifecycle_records=tuple(replay_records),
            shadow_accounts=shadow_accounts,
        )
        if control_result.incident is None:
            raise OrchestrationFailure(
                "blocked production data outcome did not create incident controls"
            )

        for record in lifecycle_records:
            if exact_lifecycle_event(record.sleeve_id) is None:
                raise OrchestrationFailure(
                    "production data incident lifecycle evidence is missing"
                )
            state = self.catalog.latest_lifecycle_state(record.sleeve_id)
            if state is not LifecycleState.FROZEN_DATA:
                raise OrchestrationFailure(
                    "open production data incident does not freeze the current fleet"
                )
        account_ids = tuple(
            sorted(
                {
                    str(account_id)
                    for accounts in shadow_accounts.values()
                    for account_id in accounts
                }
            )
        )
        missing_cash_intents: list[str] = []
        for account_id in account_ids:
            cash_events = self.catalog.iter_shadow_events_by_type(
                account_id=account_id,
                event_type="cash_target_intent",
                since=None,
                through=None,
            )
            try:
                has_matching_intent = any(
                    str(event.payload.get("incident_id") or "")
                    == domain_incident.incident_id
                    and str(event.payload.get("intent_id") or "")
                    == cash_intent.intent_id
                    and canonical_json(event.payload)
                    == canonical_json(cash_intent.to_dict())
                    for event in cash_events
                )
            finally:
                close = getattr(cash_events, "close", None)
                if close is not None:
                    close()
            if not has_matching_intent:
                missing_cash_intents.append(account_id)
        if missing_cash_intents:
            raise OrchestrationFailure(
                "open production data incident lacks current fleet cash intents"
            )

        final_incidents = self.production_ledger.iter_incidents()
        try:
            final_authority = next(
                (
                    item
                    for item in final_incidents
                    if item.incident_id == expected_incident_id
                ),
                None,
            )
        finally:
            close = getattr(final_incidents, "close", None)
            if close is not None:
                close()
        if final_authority is None or not origin_is_intact(final_authority):
            raise OrchestrationFailure(
                "production data incident lost its durable origin authority"
            )
        if final_authority.status is not IncidentStatus.OPEN:
            raise OrchestrationFailure(
                "production data incident was terminalized during control recovery"
            )

        return {
            "incident_id": final_authority.incident_id,
            "domain_incident_id": domain_incident.incident_id,
            "stage": incident_stage.value,
            "partition_key": normalized_partition_key,
            "cash_intent_accounts": list(account_ids),
            "reused": _was_preexisting,
        }

    @staticmethod
    def _calendar_bootstrap_child_payload(
        *,
        partition_key: str,
        silver_snapshot_id: str,
        bootstrap_input_hash: str,
    ) -> dict[str, str]:
        return {
            "partition_key": partition_key,
            "silver_snapshot_id": silver_snapshot_id,
            "bootstrap_input_hash": bootstrap_input_hash,
        }

    @classmethod
    def _calendar_bootstrap_child_input_hash(
        cls,
        *,
        partition_key: str,
        silver_snapshot_id: str,
        bootstrap_input_hash: str,
    ) -> str:
        return content_fingerprint(
            cls._calendar_bootstrap_child_payload(
                partition_key=partition_key,
                silver_snapshot_id=silver_snapshot_id,
                bootstrap_input_hash=bootstrap_input_hash,
            ),
            domain="factor-lab/research-os/v1/bootstrap-calendar-partition",
        )

    def _validate_calendar_recovery_manifest_binding(
        self,
        reference: DataSnapshotRef,
        *,
        expected_tier: SnapshotTier,
        context: str,
    ) -> dict[str, Any]:
        manifest = dict(reference.manifest or {})
        verification = verify_immutable_snapshot_manifest(
            manifest,
            base_dir=self.settings.lake_root,
        )
        if not verification["valid"]:
            raise OrchestrationFailure(
                f"{context} manifest or file hash is invalid"
            )
        quality_by_manifest = {
            "pass": DataQualityStatus.ACCEPTED,
            "warning": DataQualityStatus.DISPUTED,
            "blocked": DataQualityStatus.QUARANTINED,
        }
        try:
            manifest_as_of = _parse_aware(
                manifest.get("as_of"), name=f"{context}.manifest.as_of"
            )
        except (TypeError, ValueError) as exc:
            raise OrchestrationFailure(
                f"{context} manifest/reference binding is invalid"
            ) from exc
        if (
            str(manifest.get("snapshot_id") or "") != reference.snapshot_id
            or reference.content_hash != reference.snapshot_id
            or str(manifest.get("tier") or "") != expected_tier.value
            or reference.tier is not expected_tier
            or manifest_as_of
            != _parse_aware(reference.as_of, name=f"{context}.reference.as_of")
            or tuple(manifest.get("parent_snapshot_ids") or ())
            != tuple(reference.parent_snapshot_ids)
            or tuple(manifest.get("trust_labels") or ())
            != tuple(reference.trust_labels)
            or quality_by_manifest.get(str(manifest.get("quality_status") or ""))
            is not reference.quality_status
        ):
            raise OrchestrationFailure(
                f"{context} manifest/reference binding is invalid"
            )
        return manifest

    def _validate_recoverable_calendar_snapshot(
        self,
        reference: DataSnapshotRef,
        *,
        exchange: str,
        source_start: date,
        through: date,
    ) -> tuple[str, ...]:
        """Validate one already-published Silver before crash recovery.

        Recovery trusts neither a run-detail snapshot id nor a surviving local
        filename.  The catalog reference, immutable manifest, file bytes,
        parent closure and manifest-bound calendar must all agree.
        """

        if (
            reference.tier is not SnapshotTier.SILVER
            or reference.quality_status is not DataQualityStatus.ACCEPTED
            or reference.content_hash != reference.snapshot_id
        ):
            raise OrchestrationFailure(
                "bound calendar recovery snapshot is not accepted Silver"
            )
        required_labels = {
            "point_in_time",
            "field_reconciled",
            "accepted_trade_calendar",
        }
        if not required_labels.issubset(set(reference.trust_labels)):
            raise OrchestrationFailure(
                "bound calendar recovery snapshot lacks required trust labels"
            )
        manifest = self._validate_calendar_recovery_manifest_binding(
            reference,
            expected_tier=SnapshotTier.SILVER,
            context="bound calendar recovery snapshot",
        )
        calendar = manifest.get("trading_calendar")
        if not isinstance(calendar, Mapping):
            raise OrchestrationFailure(
                "bound calendar recovery snapshot lacks trading-calendar evidence"
            )
        if (
            str(calendar.get("source") or "")
            != f"dual_source_reconciled:{exchange}"
            or str(calendar.get("quality_status") or "") != "accepted"
        ):
            raise OrchestrationFailure(
                "bound calendar recovery snapshot has incompatible source or quality"
            )
        raw_sessions = tuple(map(str, calendar.get("sessions") or ()))
        try:
            parsed_sessions = tuple(date.fromisoformat(item) for item in raw_sessions)
        except ValueError as exc:
            raise OrchestrationFailure(
                "bound calendar recovery snapshot has invalid sessions"
            ) from exc
        if (
            not parsed_sessions
            or raw_sessions != tuple(sorted(set(raw_sessions)))
            or any(item < source_start or item > through for item in parsed_sessions)
        ):
            raise OrchestrationFailure(
                "bound calendar recovery sessions are empty, unordered or out of range"
            )
        expected_calendar_hash = hashlib.sha256(
            "\n".join(raw_sessions).encode("ascii")
        ).hexdigest()
        if str(calendar.get("content_hash") or "") != expected_calendar_hash:
            raise OrchestrationFailure(
                "bound calendar recovery session hash is invalid"
            )

        parent_ids = tuple(reference.parent_snapshot_ids)
        if not parent_ids:
            raise OrchestrationFailure(
                "bound calendar recovery snapshot has no Bronze parents"
            )
        observed_sources: set[str] = set()
        for parent_id in parent_ids:
            parent = self.catalog.get_snapshot(parent_id)
            if parent is None:
                raise OrchestrationFailure(
                    "bound calendar recovery snapshot has a missing parent"
                )
            parent_reference = parent.reference
            parent_labels = set(parent_reference.trust_labels)
            if (
                parent_reference.tier is not SnapshotTier.BRONZE
                or parent_reference.quality_status is not DataQualityStatus.ACCEPTED
                or not {"raw_vendor_response", "calendar_bootstrap"}.issubset(
                    parent_labels
                )
            ):
                raise OrchestrationFailure(
                    "bound calendar recovery parent is not accepted raw calendar evidence"
                )
            self._validate_calendar_recovery_manifest_binding(
                parent_reference,
                expected_tier=SnapshotTier.BRONZE,
                context="bound calendar recovery parent",
            )
            observed_sources.update(
                label.removeprefix("source:")
                for label in parent_labels
                if label.startswith("source:")
            )
        if not {"tushare", "diemeng"}.issubset(observed_sources):
            raise OrchestrationFailure(
                "bound calendar recovery parent closure is not dual-source"
            )

        file_entries = tuple(manifest.get("files") or ())
        silver_entries = [
            entry
            for entry in file_entries
            if isinstance(entry, Mapping)
            and Path(str(entry.get("path") or "")).name
            == "accepted_calendar_silver.parquet"
        ]
        if len(silver_entries) != 1:
            raise OrchestrationFailure(
                "bound calendar recovery manifest lacks one Silver calendar file"
            )
        silver_path = (
            self.settings.lake_root
            / Path(*str(silver_entries[0]["path"]).split("/"))
        ).resolve()
        frame = pd.read_parquet(silver_path)
        required_columns = {"dataset", "event_time", "exchange", "is_open"}
        if frame.empty or not required_columns.issubset(frame.columns):
            raise OrchestrationFailure(
                "bound calendar recovery Silver is empty or malformed"
            )
        if set(frame["dataset"].dropna().astype(str)) != {"trade_calendar"}:
            raise OrchestrationFailure(
                "bound calendar recovery Silver has an incompatible dataset"
            )
        if set(frame["exchange"].dropna().astype(str).str.upper()) != {exchange}:
            raise OrchestrationFailure(
                "bound calendar recovery Silver has an incompatible exchange"
            )
        event_dates = pd.to_datetime(
            frame["event_time"], errors="raise", utc=True
        ).dt.date
        if any(item < source_start or item > through for item in event_dates):
            raise OrchestrationFailure(
                "bound calendar recovery Silver contains out-of-range dates"
            )
        file_sessions = tuple(
            sorted(
                {
                    item.isoformat()
                    for item in event_dates.loc[
                        pd.to_numeric(frame["is_open"], errors="coerce")
                        .fillna(0)
                        .eq(1)
                    ]
                }
            )
        )
        if file_sessions != raw_sessions:
            raise OrchestrationFailure(
                "bound calendar recovery Silver disagrees with its session manifest"
            )
        return raw_sessions

    def _recover_bound_calendar_snapshot(
        self,
        *,
        exchange: str,
        source_start: date,
        through: date,
        bootstrap_input_hash: str,
    ) -> tuple[DataSnapshotRef, tuple[str, ...]] | None:
        """Resolve a crash-bound Silver solely from child input hashes."""

        assert self.production_ledger is not None
        children = []
        for row in self.production_ledger.list_partitions(
            source_id="research_os",
            dataset="accepted_trade_calendar",
            limit=100_000,
        ):
            try:
                session = date.fromisoformat(row.identity.partition_key)
            except ValueError as exc:
                raise OrchestrationFailure(
                    "accepted calendar ledger contains an invalid partition key"
                ) from exc
            if source_start <= session <= through:
                children.append(row)
        if not children:
            return None

        candidates: list[tuple[DataSnapshotRef, tuple[str, ...]]] = []
        inconsistent_binding = False
        cursor = None
        while True:
            page = self.catalog.list_snapshot_page(
                limit=1_000,
                quality_status=DataQualityStatus.ACCEPTED,
                tier=SnapshotTier.SILVER,
                after=cursor,
            )
            for record in page.records:
                reference = record.reference
                expected = {
                    row.identity.partition_key: self._calendar_bootstrap_child_input_hash(
                        partition_key=row.identity.partition_key,
                        silver_snapshot_id=reference.snapshot_id,
                        bootstrap_input_hash=bootstrap_input_hash,
                    )
                    for row in children
                }
                if not any(
                    row.input_hash == expected[row.identity.partition_key]
                    for row in children
                ):
                    continue
                sessions = self._validate_recoverable_calendar_snapshot(
                    reference,
                    exchange=exchange,
                    source_start=source_start,
                    through=through,
                )
                session_set = set(sessions)
                if any(
                    row.identity.partition_key not in session_set
                    or row.input_hash != expected[row.identity.partition_key]
                    for row in children
                ):
                    inconsistent_binding = True
                    continue
                candidates.append((reference, sessions))
            if page.next_cursor is None:
                break
            cursor = page.next_cursor
        if inconsistent_binding or len(candidates) != 1:
            raise OrchestrationFailure(
                "calendar child hashes do not bind one unique verified Silver snapshot"
            )
        return candidates[0]

    @staticmethod
    def _calendar_bootstrap_attempt_hash(
        *,
        bootstrap_input_hash: str,
        parent_snapshot_ids: Sequence[str],
        wide: pd.DataFrame,
        reconciliation_audit: Mapping[str, Any],
    ) -> str:
        ordered = wide.reindex(sorted(wide.columns), axis=1)
        sort_columns = [
            name
            for name in ("dataset", "entity_key", "event_time")
            if name in ordered.columns
        ]
        if sort_columns:
            ordered = ordered.sort_values(sort_columns, kind="mergesort")
        accepted_frame_json = ordered.reset_index(drop=True).to_json(
            orient="records",
            date_format="iso",
            date_unit="us",
        )
        return content_fingerprint(
            {
                "bootstrap_input_hash": bootstrap_input_hash,
                "parent_snapshot_ids": sorted(parent_snapshot_ids),
                "accepted_frame_json": accepted_frame_json,
                "reconciliation_audit": _jsonable(reconciliation_audit),
            },
            domain="factor-lab/research-os/v1/calendar-bootstrap-attempt",
        )

    def bootstrap_accepted_calendar(
        self,
        *,
        exchange: str,
        source_start: str | date,
        through: str | date,
        dagster_run_id: str,
    ) -> TriggerPoll:
        """Bootstrap dynamic partitions from reconciled vendor calendars only."""

        self._require_production_operation(
            ProductionOperation.CALENDAR_CAPABILITY_PROBE
        )
        if self.production_ledger is None:
            raise OrchestrationFailure(
                "calendar bootstrap requires the PostgreSQL production ledger"
            )
        start_date = _parse_date(source_start, name="source_start")
        through_date = _parse_date(through, name="through")
        if start_date > through_date:
            raise ValueError("source_start cannot follow through")
        exchange_code = str(exchange or "").strip().upper()
        if not re.fullmatch(r"[A-Z]{2,12}", exchange_code):
            raise ValueError("exchange must be a short uppercase exchange code")
        if not str(dagster_run_id or "").strip():
            raise ValueError("dagster_run_id is required")
        daily = self.config.get("daily") or {}
        raw_sources = daily.get("sources") if isinstance(daily, Mapping) else None
        if not isinstance(raw_sources, list):
            raise ServiceNotConfigured("daily.sources is required for calendar bootstrap")
        selected = [
            dict(row)
            for row in raw_sources
            if isinstance(row, Mapping)
            and str((row.get("request") or {}).get("dataset") or "")
            in {"trade_calendar", "trade_cal"}
        ]
        selected = [self._bind_source_transport_authority(row) for row in selected]
        source_types = {str(row.get("source") or "").lower() for row in selected}
        if not {"tushare", "diemeng"}.issubset(source_types):
            raise ServiceNotConfigured(
                "calendar bootstrap requires configured Tushare and Diemeng sources"
            )
        bootstrap_input_hash = content_fingerprint(
            {
                "exchange": exchange_code,
                "source_start": start_date,
                "through": through_date,
                "sources": selected,
            },
            domain="factor-lab/research-os/v1/calendar-bootstrap-input",
        )
        bootstrap_identity = PartitionIdentity(
            source_id="research_os",
            dataset="bootstrap_trade_calendar",
            partition_key=through_date.isoformat(),
        )
        bootstrap_record = self.production_ledger.ensure_partition(
            bootstrap_identity,
            created_at=self._now(),
            input_hash=bootstrap_input_hash,
            details={"dagster_run_id": dagster_run_id},
        )
        if bootstrap_record.status is PartitionStatus.SUCCEEDED:
            cached = bootstrap_record.details.get("bootstrap_result")
            if not isinstance(cached, Mapping):
                raise OrchestrationFailure(
                    "successful calendar bootstrap lacks its immutable result"
                )
            if not bootstrap_record.run_id:
                raise OrchestrationFailure(
                    "successful calendar bootstrap lacks its attempt run"
                )
            bridge_run = self.catalog.get_run(bootstrap_record.run_id)
            if (
                bridge_run is None
                or bridge_run.run_type != "dagster_calendar_bootstrap"
                or bridge_run.input_fingerprint != bootstrap_input_hash
                or bridge_run.status != "succeeded"
            ):
                raise OrchestrationFailure(
                    "successful calendar bootstrap attempt authority is invalid"
                )
            sessions = tuple(map(str, cached.get("sessions") or ()))
            return TriggerPoll(
                triggers=tuple(
                    Trigger(
                        partition_key=value,
                        run_key=f"daily:{value}",
                        metadata={
                            "sensor": "bootstrap_accepted_calendar",
                            "calendar_snapshot_id": cached.get("silver_snapshot_id"),
                        },
                    )
                    for value in sessions
                ),
                cursor=(sessions[-1] if sessions else None),
                message=f"{len(sessions)} accepted bootstrap trading sessions",
            )
        bootstrap_lease = self.production_ledger.claim(
            identity=bootstrap_identity,
            owner=f"calendar-bootstrap-{_safe_name(dagster_run_id)}",
            now=self._now(),
            lease_for=timedelta(hours=1),
        )
        if bootstrap_lease is None:
            raise OrchestrationFailure("calendar bootstrap is already leased")
        attempt_generation = int(bootstrap_lease.record.attempts)
        attempt_identity_hash = content_fingerprint(
            {
                "bootstrap_input_hash": bootstrap_input_hash,
                "attempt_generation": attempt_generation,
            },
            domain="factor-lab/research-os/v1/calendar-bootstrap-run-attempt",
        )
        bridge_run_id = f"roscal_{attempt_identity_hash[:48]}"
        bridge_metadata = {
            "dagster_run_id": dagster_run_id,
            "dagster_run_ids": [dagster_run_id],
            "operation": "accepted_calendar_bootstrap",
            "exchange": exchange_code,
            "source_start": start_date.isoformat(),
            "through": through_date.isoformat(),
            "attempt_generation": attempt_generation,
            "bootstrap_partition_run_id": bootstrap_identity.partition_run_id,
        }
        bridge_run, bridge_claimed = self.catalog.claim_run(
            RunRecord(
                run_id=bridge_run_id,
                run_type="dagster_calendar_bootstrap",
                status="running",
                input_fingerprint=bootstrap_input_hash,
                started_at=bootstrap_lease.record.started_at or self._now(),
                metadata=bridge_metadata,
            )
        )
        if not bridge_claimed or bridge_run.status != "running":
            raise OrchestrationFailure(
                "calendar bootstrap attempt run identity is already occupied"
            )

        def finish_bridge(
            *,
            status: str,
            completed_at: datetime,
            metadata: Mapping[str, Any],
            error: str | None = None,
        ) -> None:
            self.catalog.save_run(
                replace(
                    bridge_run,
                    status=status,
                    metadata=dict(metadata),
                    completed_at=completed_at,
                    error=error,
                )
            )

        def complete_calendar_snapshot(
            reference: DataSnapshotRef,
            sessions: tuple[str, ...],
            *,
            reused_failed_attempt: bool,
            artifact_attempt_hash: str | None = None,
        ) -> TriggerPoll:
            for session in sessions:
                identity = PartitionIdentity(
                    source_id="research_os",
                    dataset="accepted_trade_calendar",
                    partition_key=session,
                )
                item_payload = self._calendar_bootstrap_child_payload(
                    partition_key=session,
                    silver_snapshot_id=reference.snapshot_id,
                    bootstrap_input_hash=bootstrap_input_hash,
                )
                item_input_hash = self._calendar_bootstrap_child_input_hash(
                    partition_key=session,
                    silver_snapshot_id=reference.snapshot_id,
                    bootstrap_input_hash=bootstrap_input_hash,
                )
                item = self.production_ledger.ensure_partition(
                    identity,
                    created_at=self._now(),
                    input_hash=item_input_hash,
                )
                if item.status is PartitionStatus.SUCCEEDED:
                    if (
                        item.input_hash != item_input_hash
                        or item.output_snapshot_id != reference.snapshot_id
                    ):
                        raise OrchestrationFailure(
                            f"accepted calendar session {session} has incompatible terminal evidence"
                        )
                    producer = (
                        None
                        if not item.run_id
                        else self.catalog.get_run(item.run_id)
                    )
                    try:
                        producer_generation = int(
                            (producer.metadata if producer is not None else {}).get(
                                "attempt_generation"
                            )
                        )
                    except (TypeError, ValueError):
                        producer_generation = 0
                    if (
                        producer is None
                        or producer.run_type != "dagster_calendar_bootstrap"
                        or producer.input_fingerprint != bootstrap_input_hash
                        or producer.status not in {"running", "failed"}
                        or producer.metadata.get("bootstrap_partition_run_id")
                        != bootstrap_identity.partition_run_id
                        or not 1 <= producer_generation <= attempt_generation
                    ):
                        raise OrchestrationFailure(
                            f"accepted calendar session {session} has incompatible attempt authority"
                        )
                    continue
                if item.status in {
                    PartitionStatus.DISPUTED,
                    PartitionStatus.QUARANTINED,
                }:
                    raise OrchestrationFailure(
                        f"accepted calendar session {session} is terminal {item.status.value}"
                    )
                item_lease = self.production_ledger.claim(
                    identity=identity,
                    owner=f"calendar-bootstrap-{_safe_name(dagster_run_id)}",
                    now=self._now(),
                    lease_for=timedelta(minutes=15),
                )
                if item_lease is None:
                    raise OrchestrationFailure(
                        f"accepted calendar session {session} is already leased"
                    )
                self.production_ledger.finish(
                    item_lease,
                    status=PartitionStatus.SUCCEEDED,
                    completed_at=self._now(),
                    run_id=bridge_run.run_id,
                    output_snapshot_id=reference.snapshot_id,
                    output_hash=content_fingerprint(
                        item_payload,
                        domain="factor-lab/research-os/v1/bootstrap-calendar-output",
                    ),
                    details={"accepted_calendar": item_payload},
                )
            bootstrap_result = {
                "exchange": exchange_code,
                "source_start": start_date.isoformat(),
                "through": through_date.isoformat(),
                "sessions": list(sessions),
                "silver_snapshot_id": reference.snapshot_id,
            }
            if artifact_attempt_hash is not None:
                bootstrap_result["artifact_attempt_hash"] = artifact_attempt_hash
            self.production_ledger.finish(
                bootstrap_lease,
                status=PartitionStatus.SUCCEEDED,
                completed_at=self._now(),
                run_id=bridge_run.run_id,
                output_snapshot_id=reference.snapshot_id,
                output_hash=content_fingerprint(
                    bootstrap_result,
                    domain="factor-lab/research-os/v1/calendar-bootstrap-output",
                ),
                details={"bootstrap_result": bootstrap_result},
            )
            finish_bridge(
                status="succeeded",
                completed_at=self._now(),
                metadata={
                    **bridge_metadata,
                    "reused_terminal_partition": False,
                    "reused_failed_attempt": reused_failed_attempt,
                    "accepted_session_count": len(sessions),
                    "silver_snapshot_id": reference.snapshot_id,
                    **(
                        {}
                        if artifact_attempt_hash is None
                        else {"artifact_attempt_hash": artifact_attempt_hash}
                    ),
                },
            )
            return TriggerPoll(
                triggers=tuple(
                    Trigger(
                        partition_key=value,
                        run_key=f"daily:{value}",
                        metadata={
                            "sensor": "bootstrap_accepted_calendar",
                            "calendar_snapshot_id": reference.snapshot_id,
                        },
                    )
                    for value in sessions
                ),
                cursor=sessions[-1],
                message=f"{len(sessions)} accepted bootstrap trading sessions",
            )

        try:
            recovered = self._recover_bound_calendar_snapshot(
                exchange=exchange_code,
                source_start=start_date,
                through=through_date,
                bootstrap_input_hash=bootstrap_input_hash,
            )
            if recovered is not None:
                recovered_reference, recovered_sessions = recovered
                return complete_calendar_snapshot(
                    recovered_reference,
                    recovered_sessions,
                    reused_failed_attempt=True,
                )
            parent_ids: list[str] = []
            canonical_parts: list[pd.DataFrame] = []
            ingested_times: list[datetime] = []
            source_date_sets: dict[str, set[date]] = {}
            tokens = {
                "partition_key": through_date.isoformat(),
                "partition_compact": through_date.strftime("%Y%m%d"),
                "partition_yyyymmdd": through_date.strftime("%Y%m%d"),
                "source_start": start_date.isoformat(),
                "source_start_compact": start_date.strftime("%Y%m%d"),
                "through": through_date.isoformat(),
                "through_compact": through_date.strftime("%Y%m%d"),
                "run_id": dagster_run_id,
            }
            for index, raw_source in enumerate(selected):
                source = _render(raw_source, tokens)
                assert isinstance(source, dict)
                request_payload = dict(source.get("request") or {})
                parameters = dict(request_payload.get("parameters") or {})
                for key in ("start_date", "start_time"):
                    if key in parameters:
                        parameters[key] = (
                            start_date.strftime("%Y%m%d")
                            if key.endswith("date")
                            else start_date.isoformat()
                        )
                for key in ("end_date", "end_time"):
                    if key in parameters:
                        parameters[key] = (
                            through_date.strftime("%Y%m%d")
                            if key.endswith("date")
                            else through_date.isoformat()
                        )
                if "exchange" in parameters:
                    parameters["exchange"] = exchange_code
                request_payload["parameters"] = parameters
                source["request"] = request_payload
                result = sync_bronze(
                    source,
                    lake_root=self.settings.lake_root,
                    env=self.env,
                    object_store_archive=self.object_store_archive,
                )
                manifest, manifest_path, manifest_object = self._manifest(
                    paths=(result.data_path, result.metadata_path),
                    tier="bronze",
                    as_of=result.ingested_at,
                    parent_snapshot_ids=(),
                    quality_report={"status": "pass"},
                    trust_labels=(
                        "raw_vendor_response",
                        "calendar_bootstrap",
                        f"source:{result.source_id}",
                    ),
                )
                self.catalog.register_snapshot(
                    manifest.to_snapshot_ref(
                        uri=(
                            manifest_object.uri
                            if manifest_object is not None
                            else manifest_path.resolve().as_uri()
                        )
                    )
                )
                row = {
                    **result.to_dict(),
                    "source_config_index": index,
                    "bronze_snapshot_id": manifest.snapshot_id,
                }
                batch = self._batch_from_sync(row)
                spec, resolver = self._canonicalization(source, batch)
                # Exchange calendars describe future events that are already
                # knowable. Bind that knowledge conservatively to the actual
                # vendor ingestion time; do not pretend the future session has
                # already completed or use a caller-reported release time.
                spec = replace(spec, allows_pre_event_availability=True)
                resolver = lambda _row, observed_at=result.ingested_at: observed_at
                canonical = canonicalize_batch(
                    batch, spec, availability_resolver=resolver
                )
                canonical_parts.append(canonical)
                observed = {
                    stamp.date()
                    for stamp in pd.to_datetime(
                        canonical["event_time"], errors="raise", utc=True
                    )
                    if start_date <= stamp.date() <= through_date
                }
                source_date_sets[result.source_id] = observed
                parent_ids.append(manifest.snapshot_id)
                ingested_times.append(
                    _parse_aware(result.ingested_at, name="ingested_at")
                )
            date_sets = list(source_date_sets.values())
            if not date_sets or any(values != date_sets[0] for values in date_sets[1:]):
                raise OrchestrationFailure(
                    "calendar vendors do not cover the same date set"
                )
            reconciled = reconcile_observations(
                pd.concat(canonical_parts, ignore_index=True),
                policies={
                    "is_open": ComparisonPolicy(),
                    "pretrade_date": ComparisonPolicy(),
                },
                default_policy=ComparisonPolicy(),
                allows_pre_event_availability=True,
            )
            if not reconciled.promotion_allowed:
                raise OrchestrationFailure(
                    "calendar vendor conflict/dispute blocks bootstrap"
                )
            wide = self._accepted_wide(reconciled.accepted)
            event_dates = pd.to_datetime(
                wide["event_time"], errors="raise", utc=True
            ).dt.date
            wide = wide.loc[
                event_dates.map(lambda value: start_date <= value <= through_date)
            ].copy()
            if wide.empty or "is_open" not in wide:
                raise OrchestrationFailure("reconciled calendar is empty or lacks is_open")
            sessions = tuple(
                sorted(
                    {
                        stamp.date().isoformat()
                        for stamp in pd.to_datetime(
                            wide.loc[
                                pd.to_numeric(wide["is_open"], errors="coerce")
                                .fillna(0)
                                .eq(1),
                                "event_time",
                            ],
                            errors="raise",
                            utc=True,
                        )
                    }
                )
            )
            if not sessions:
                raise OrchestrationFailure(
                    "calendar bootstrap found no mutually accepted open sessions"
                )
            try:
                assert_snapshot_promotion_allowed(self.catalog, tuple(parent_ids))
            except SnapshotPromotionBlocked as exc:
                raise OrchestrationFailure(
                    f"Bronze trust labels block calendar Silver promotion: {exc}"
                ) from exc
            artifact_attempt_hash = self._calendar_bootstrap_attempt_hash(
                bootstrap_input_hash=bootstrap_input_hash,
                parent_snapshot_ids=parent_ids,
                wide=wide,
                reconciliation_audit=reconciled.audit,
            )
            artifact_root = (
                self._state_root
                / "calendar_bootstrap"
                / bootstrap_input_hash
                / artifact_attempt_hash
            )
            silver_path = _write_parquet_once(
                artifact_root / "accepted_calendar_silver.parquet", wide
            )
            audit_path = _write_json_once(
                artifact_root / "calendar_reconciliation_audit.json",
                reconciled.audit,
            )
            silver_manifest, silver_manifest_path, silver_object = self._manifest(
                paths=(silver_path, audit_path),
                tier="silver",
                as_of=max(ingested_times),
                parent_snapshot_ids=tuple(parent_ids),
                quality_report={
                    "status": "pass",
                    "calendar_sources": {
                        key: [item.isoformat() for item in sorted(values)]
                        for key, values in sorted(source_date_sets.items())
                    },
                },
                trust_labels=(
                    "point_in_time",
                    "field_reconciled",
                    "accepted_trade_calendar",
                ),
                trading_calendar={
                    "source": f"dual_source_reconciled:{exchange_code}",
                    "quality_status": "accepted",
                    "sessions": sessions,
                    "content_hash": hashlib.sha256(
                        "\n".join(sessions).encode("ascii")
                    ).hexdigest(),
                },
            )
            self.catalog.register_snapshot(
                silver_manifest.to_snapshot_ref(
                    uri=(
                        silver_object.uri
                        if silver_object is not None
                        else silver_manifest_path.resolve().as_uri()
                    )
                )
            )
            return complete_calendar_snapshot(
                silver_manifest.to_snapshot_ref(
                    uri=(
                        silver_object.uri
                        if silver_object is not None
                        else silver_manifest_path.resolve().as_uri()
                    )
                ),
                sessions,
                reused_failed_attempt=False,
                artifact_attempt_hash=artifact_attempt_hash,
            )
        except Exception as exc:
            failed_at = self._now()
            try:
                self.production_ledger.finish(
                    bootstrap_lease,
                    status=PartitionStatus.FAILED,
                    completed_at=failed_at,
                    run_id=bridge_run.run_id,
                    error_code="calendar_bootstrap_failed",
                    error=f"{type(exc).__name__}: {exc}",
                )
            except ProductionLedgerError:
                pass
            finally:
                finish_bridge(
                    status="failed",
                    completed_at=failed_at,
                    metadata={
                        **bridge_metadata,
                        "failure_type": type(exc).__name__,
                    },
                    error=f"calendar bootstrap failed closed ({type(exc).__name__})",
                )
            raise

    def revalidate_ready_data_incident(
        self, *, incident_id: str
    ) -> Mapping[str, Any]:
        """Resume or start revalidation from durable repair evidence only.

        The operator supplies only the production incident identity.  Gold,
        the database clock, and any partially committed revalidation request
        are reconstructed from PostgreSQL/catalog authority so a crash retry
        cannot accidentally mint a second request timestamp.
        """

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "data incident revalidation requires the PostgreSQL ledger"
            )
        incidents = self.production_ledger.iter_incidents()
        try:
            incident = next(
                (item for item in incidents if item.incident_id == incident_id),
                None,
            )
        finally:
            close = getattr(incidents, "close", None)
            if close is not None:
                close()
        if incident is None:
            raise OrchestrationFailure("data incident was not found")
        if incident.status is not IncidentStatus.OPEN:
            stored_resolution = incident.payload.get("resolution")
            stored_snapshot_id = (
                ""
                if not isinstance(stored_resolution, Mapping)
                else str(stored_resolution.get("snapshot_id") or "").strip()
            )
            if incident.resolved_at is None or not stored_snapshot_id:
                raise OrchestrationFailure(
                    "terminal data incident lacks immutable revalidation evidence"
                )
            return self.revalidate_data_incident(
                incident_id=incident.incident_id,
                snapshot_id=stored_snapshot_id,
                occurred_at=incident.resolved_at,
            )

        domain_incident_id = str(
            incident.payload.get("domain_incident_id") or ""
        ).strip()
        shadow_partition = self.production_ledger.get_repair_partition(
            incident.incident_id, "stage_shadow"
        )
        if not (
            shadow_partition is not None
            and shadow_partition.status is PartitionStatus.SUCCEEDED
            and shadow_partition.completed_at is not None
        ):
            raise OrchestrationFailure(
                "data incident is waiting for a completed fresh Shadow session"
            )
        shadow_completed_at = shadow_partition.completed_at
        partial_evidence: list[Mapping[str, Any]] = []

        def admit_partial_evidence(value: Mapping[str, Any]) -> None:
            evidence = dict(value)
            if str(evidence.get("incident_id") or "") != domain_incident_id:
                return
            try:
                occurred = _parse_aware(
                    evidence.get("occurred_at"),
                    name="partial revalidation occurred_at",
                ).astimezone(timezone.utc)
            except (TypeError, ValueError):
                return
            # A typed Shadow rejection supersedes every effect from the stale
            # attempt.  Only evidence created after the *current* succeeded
            # Shadow leaf may pin this attempt's immutable request time.
            if occurred >= shadow_completed_at:
                partial_evidence.append(evidence)

        events = self.catalog.iter_lifecycle_events(
            cause="data_revalidation_passed"
        )
        try:
            for event in events:
                admit_partial_evidence(dict(event.evidence or {}))
        finally:
            close = getattr(events, "close", None)
            if close is not None:
                close()

        # Account effects are intentionally persisted before lifecycle effects.
        # A crash between those phases must still recover the exact request
        # rather than minting a second timestamp on the same Shadow session.
        if self.shadow_authority is not None:
            try:
                recovery_bindings = self.shadow_authority.active_fleet_bindings()
            except Exception as exc:
                raise OrchestrationFailure(
                    "data incident cannot read the revalidation recovery fleet"
                ) from exc
            for binding in recovery_bindings:
                shadow_events = self.catalog.iter_shadow_events_by_type(
                    account_id=str(binding.account_id),
                    event_type="data_revalidated",
                    since=shadow_completed_at,
                    through=None,
                    batch_size=500,
                )
                try:
                    for event in shadow_events:
                        admit_partial_evidence(dict(event.payload or {}))
                finally:
                    close = getattr(shadow_events, "close", None)
                    if close is not None:
                        close()

        if partial_evidence:
            canonical = {
                canonical_json(evidence) for evidence in partial_evidence
            }
            if len(canonical) != 1:
                raise OrchestrationFailure(
                    "partial data revalidation evidence is inconsistent"
                )
            durable_request = partial_evidence[0]
            snapshot_id = str(durable_request.get("snapshot_id") or "").strip()
            revalidated_at = _parse_aware(
                durable_request.get("occurred_at"),
                name="partial revalidation occurred_at",
            ).astimezone(timezone.utc)
        else:
            gold_partition = self.production_ledger.get_repair_partition(
                incident.incident_id,
                "stage_gold",
            )
            if (
                gold_partition is None
                or gold_partition.status is not PartitionStatus.SUCCEEDED
                or not gold_partition.output_snapshot_id
            ):
                raise OrchestrationFailure(
                    "data incident has no completed repaired Gold partition"
                )
            snapshot_id = str(gold_partition.output_snapshot_id)
            revalidated_at = self.catalog.database_now()

        try:
            return self.revalidate_data_incident(
                incident_id=incident.incident_id,
                snapshot_id=snapshot_id,
                occurred_at=revalidated_at,
            )
        except RetryableShadowRepairEvidence as exc:
            gold_partition = self.production_ledger.get_repair_partition(
                incident.incident_id, "stage_gold"
            )
            if not (
                shadow_partition is not None
                and shadow_partition.status is PartitionStatus.SUCCEEDED
                and isinstance(shadow_partition.output_hash, str)
                and gold_partition is not None
                and gold_partition.status is PartitionStatus.SUCCEEDED
                and isinstance(gold_partition.output_hash, str)
            ):
                raise OrchestrationFailure(
                    "retryable Shadow rejection lost its succeeded repair authority"
                ) from exc
            bindings = (
                ()
                if self.shadow_authority is None
                else tuple(self.shadow_authority.active_fleet_bindings())
            )
            fleet_tails = []
            for binding in bindings:
                account = self.catalog.get_shadow_account(str(binding.account_id))
                if account is None:
                    raise OrchestrationFailure(
                        "retryable Shadow rejection lost an active account"
                    ) from exc
                fleet_tails.append(
                    {
                        "binding_id": str(binding.binding_id),
                        "binding_hash": str(binding.binding_hash),
                        "account_id": str(binding.account_id),
                        "last_event_sequence": account.last_event_sequence,
                        "last_event_hash": account.last_event_hash,
                    }
                )
            rejection_evidence_hash = content_fingerprint(
                {
                    "incident_id": incident.incident_id,
                    "incident_hash": incident.incident_hash,
                    "gold_partition_run_id": (
                        gold_partition.identity.partition_run_id
                    ),
                    "gold_output_hash": gold_partition.output_hash,
                    "shadow_partition_run_id": (
                        shadow_partition.identity.partition_run_id
                    ),
                    "shadow_output_hash": shadow_partition.output_hash,
                    "repair_validation_trade_date": shadow_partition.details.get(
                        "repair_validation_trade_date"
                    ),
                    "fleet_tails": sorted(
                        fleet_tails,
                        key=lambda item: (
                            item["binding_id"], item["account_id"]
                        ),
                    ),
                },
                domain="factor-lab/research-os/v1/shadow-revalidation-failure-evidence",
            )
            try:
                self.production_ledger.record_shadow_revalidation_rejection(
                    incident_id=incident.incident_id,
                    rejected_partition_run_id=(
                        shadow_partition.identity.partition_run_id
                    ),
                    rejection_evidence_hash=rejection_evidence_hash,
                )
            except ProductionLedgerError as ledger_exc:
                raise OrchestrationFailure(
                    "retryable Shadow rejection could not persist its immutable successor"
                ) from ledger_exc
            raise OrchestrationFailure(
                "Shadow repair evidence advanced; recorded a typed rejection and "
                "waiting for a newer accepted session"
            ) from exc

    def revalidate_data_incident(
        self,
        *,
        incident_id: str,
        snapshot_id: str,
        occurred_at: datetime,
    ) -> Mapping[str, Any]:
        """Close one data incident through its exact post-failure Gold lineage.

        Incident terminalization is serialized in PostgreSQL.  Catalog writes
        remain independently committed, so every lifecycle and shadow effect
        is content-addressed and replayable: a process may die after those
        writes and the next invocation will validate and fill the missing
        suffix before the incident can become terminal.
        """

        if self.production_ledger is None:
            raise OrchestrationFailure(
                "data incident revalidation requires the PostgreSQL ledger"
            )
        if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        revalidated_at = occurred_at.astimezone(timezone.utc)
        if (
            getattr(self.catalog, "_backend", None).__class__.__name__
            == "_SQLAlchemyCatalog"
            and revalidated_at > self.catalog.database_now()
        ):
            raise OrchestrationFailure(
                "data incident revalidation timestamp is after the database clock"
            )

        incidents = self.production_ledger.iter_incidents()
        try:
            incident = next(
                (item for item in incidents if item.incident_id == incident_id),
                None,
            )
        finally:
            close = getattr(incidents, "close", None)
            if close is not None:
                close()
        if incident is None:
            raise OrchestrationFailure("data incident was not found")
        if revalidated_at < incident.occurred_at:
            raise ValueError("revalidation cannot predate the data incident")
        # A terminal retry is an idempotent replay of immutable authority.  Fail
        # a caller-supplied timestamp mismatch before inspecting mutable fleet
        # state: legitimate account events may have advanced after resolution,
        # but they must not obscure the authoritative terminal timestamp.
        if (
            incident.status is not IncidentStatus.OPEN
            and incident.resolved_at != revalidated_at
        ):
            raise OrchestrationFailure(
                "terminal revalidation retry timestamp differs from authority"
            )
        if incident.status is IncidentStatus.OPEN:
            control_action = self.production_ledger.incident_controls.get(
                incident.incident_id
            )
            if (
                control_action is None
                or control_action.status
                is not IncidentControlActionStatus.SUCCEEDED
            ):
                raise OrchestrationFailure(
                    "data incident controls are not durably materialized"
                )

        def validated_domain_incident(
            item: IncidentRecord, *, current: bool
        ) -> DataIncident:
            origin_payload = dict(item.payload)
            resolution = origin_payload.pop("resolution", None)
            expected_hash = content_fingerprint(
                {
                    "partition_key": item.partition_key,
                    "stage": item.stage.value,
                    "error_code": item.error_code,
                    "occurred_at": item.occurred_at,
                    "partition_run_id": item.partition_run_id,
                    "source_ids": item.source_ids,
                    "evidence_hashes": item.evidence_hashes,
                    "payload": origin_payload,
                },
                domain="factor-lab/research-os/v1/data-incident",
            )
            terminal_envelope_valid = (
                item.status is IncidentStatus.OPEN
                and item.resolved_at is None
                and item.resolution_hash is None
                and resolution is None
            ) or (
                item.status is not IncidentStatus.OPEN
                and item.resolved_at is not None
                and item.resolution_hash is not None
                and isinstance(resolution, Mapping)
            )
            if not (
                item.incident_hash == expected_hash
                and item.incident_id == f"incident_{expected_hash[:64]}"
                and terminal_envelope_valid
            ):
                raise OrchestrationFailure(
                    "data incident origin or terminal envelope is inconsistent"
                )
            try:
                domain = DataIncident(
                    stage=DataPipelineStage(item.stage.value),
                    partition_key=item.partition_key,
                    error_code=item.error_code,
                    message=item.message,
                    occurred_at=item.occurred_at,
                    source_ids=item.source_ids,
                    evidence_hashes=item.evidence_hashes,
                )
            except (TypeError, ValueError) as exc:
                raise OrchestrationFailure(
                    "data incident cannot be reconstructed as a typed domain failure"
                ) from exc
            domain_id = str(origin_payload.get("domain_incident_id") or "").strip()
            if domain_id != domain.incident_id:
                qualifier = "requested" if current else "other open"
                raise OrchestrationFailure(
                    f"{qualifier} incident lacks its canonical domain identity"
                )
            if not all(
                str(origin_payload.get(name) or "").strip()
                for name in ("dagster_run_id", "failed_step_key")
            ):
                raise OrchestrationFailure(
                    "domain incident lacks its production failure lineage"
                )
            return domain

        domain_incident = validated_domain_incident(incident, current=True)

        # A terminal retry replays the immutable resolution, not today's
        # mutable production fleet.  Champion/Challenger bindings and account
        # tails may legitimately advance after an incident is closed.  The
        # terminal row hash plus the append-only effects created by this exact
        # revalidation remain the replay authority.
        if incident.status is not IncidentStatus.OPEN:
            stored_resolution = incident.payload.get("resolution")
            if not isinstance(stored_resolution, Mapping):
                raise OrchestrationFailure(
                    "terminal data incident lacks immutable revalidation evidence"
                )
            if incident.status is not IncidentStatus.RESOLVED:
                raise OrchestrationFailure(
                    "terminal data incident is not a resolved revalidation"
                )
            stored_snapshot_id = str(
                stored_resolution.get("snapshot_id") or ""
            ).strip()
            stored_content_hash = str(
                stored_resolution.get("snapshot_content_hash") or ""
            ).strip()
            if not (
                stored_snapshot_id == snapshot_id
                and re.fullmatch(r"[0-9a-f]{64}", stored_snapshot_id)
                and stored_content_hash == stored_snapshot_id
            ):
                raise OrchestrationFailure(
                    "terminal revalidation retry differs from authority evidence"
                )
            replay_evidence = DataRevalidation(
                incident_id=domain_incident.incident_id,
                snapshot_id=stored_snapshot_id,
                snapshot_content_hash=stored_content_hash,
                occurred_at=revalidated_at,
            )
            if str(stored_resolution.get("revalidation_id") or "") != (
                replay_evidence.revalidation_id
            ):
                raise OrchestrationFailure(
                    "terminal revalidation identity differs from authority evidence"
                )

            def terminal_ids(name: str) -> tuple[str, ...]:
                raw = stored_resolution.get(name)
                if not (
                    isinstance(raw, Sequence)
                    and not isinstance(raw, (str, bytes))
                ):
                    raise OrchestrationFailure(
                        f"terminal revalidation {name} is malformed"
                    )
                values = tuple(str(item or "").strip() for item in raw)
                if any(not item for item in values) or len(values) != len(
                    set(values)
                ):
                    raise OrchestrationFailure(
                        f"terminal revalidation {name} is ambiguous"
                    )
                return values

            blocking_ids = terminal_ids("blocking_incident_ids")
            restored_sleeves = terminal_ids("restored_sleeves")
            revalidated_accounts = terminal_ids("revalidated_accounts")
            action = str(stored_resolution.get("fleet_action") or "")
            if action == "restored_to_dormant":
                if blocking_ids or not restored_sleeves or not revalidated_accounts:
                    raise OrchestrationFailure(
                        "terminal revalidation carries an invalid restored fleet"
                    )
                expected_effect = replay_evidence.to_dict()
                for sleeve_id in restored_sleeves:
                    key = (
                        f"data-revalidation:{replay_evidence.revalidation_id}:"
                        f"{sleeve_id}"
                    )
                    events = self.catalog.iter_lifecycle_events(
                        sleeve_id=sleeve_id,
                        cause="data_revalidation_passed",
                    )
                    try:
                        matches = tuple(
                            item
                            for item in events
                            if item.idempotency_key == key
                        )
                    finally:
                        close = getattr(events, "close", None)
                        if close is not None:
                            close()
                    if not (
                        len(matches) == 1
                        and matches[0].from_state
                        is LifecycleState.FROZEN_DATA
                        and matches[0].to_state is LifecycleState.DORMANT
                        and matches[0].occurred_at >= revalidated_at
                        and canonical_json(matches[0].evidence)
                        == canonical_json(expected_effect)
                    ):
                        raise OrchestrationFailure(
                            "terminal revalidation lacks immutable lifecycle evidence"
                        )
                for account_id in revalidated_accounts:
                    events = self.catalog.iter_shadow_events_by_type(
                        account_id=account_id,
                        event_type="data_revalidated",
                        since=revalidated_at,
                        through=None,
                    )
                    try:
                        matches = tuple(
                            item
                            for item in events
                            if str(
                                item.payload.get("revalidation_id") or ""
                            )
                            == replay_evidence.revalidation_id
                        )
                    finally:
                        close = getattr(events, "close", None)
                        if close is not None:
                            close()
                    if not (
                        len(matches) == 1
                        and matches[0].occurred_at >= revalidated_at
                        and canonical_json(matches[0].payload)
                        == canonical_json(expected_effect)
                    ):
                        raise OrchestrationFailure(
                            "terminal revalidation lacks immutable Shadow evidence"
                        )
            elif not (
                action == "remained_frozen"
                and blocking_ids
                and not restored_sleeves
                and not revalidated_accounts
            ):
                raise OrchestrationFailure(
                    "terminal revalidation carries an invalid fleet action"
                )

            def terminal_effects_must_not_run(*_args: Any) -> None:
                raise OrchestrationFailure(
                    "terminal revalidation unexpectedly attempted new effects"
                )

            try:
                resolved, _, effects_applied = (
                    self.production_ledger._resolve_typed_data_incident_with_effects(
                        incident_id,
                        resolved_at=revalidated_at,
                        evidence=dict(stored_resolution),
                        apply_effects=terminal_effects_must_not_run,
                    )
                )
            except ProductionLedgerError as exc:
                raise OrchestrationFailure(
                    "terminal revalidation authority is inconsistent"
                ) from exc
            if effects_applied:
                raise OrchestrationFailure(
                    "terminal revalidation replay applied duplicate effects"
                )
            return {
                "incident_id": resolved.incident_id,
                "status": resolved.status.value,
                "revalidation_id": replay_evidence.revalidation_id,
                "snapshot_id": stored_snapshot_id,
                "fleet_action": action,
                "blocking_incident_ids": list(blocking_ids),
                "restored_sleeves": list(restored_sleeves),
                "revalidated_accounts": list(revalidated_accounts),
                "effects_applied": False,
            }

        candidate_snapshot = self.catalog.get_snapshot(snapshot_id)
        if candidate_snapshot is None:
            raise OrchestrationFailure("revalidation Gold snapshot is not cataloged")
        candidate_reference = candidate_snapshot.reference
        candidate_labels = {
            str(label).strip().lower() for label in candidate_reference.trust_labels
        }
        if not (
            candidate_reference.tier is SnapshotTier.GOLD
            and candidate_reference.quality_status is DataQualityStatus.ACCEPTED
            and {"point_in_time", "quality_accepted"}.issubset(candidate_labels)
            and candidate_reference.uri.startswith("iceberg://")
        ):
            raise OrchestrationFailure(
                "revalidation requires formal forward-eligible accepted Gold evidence"
            )
        stage_contract = (
            ("stage_source", IncidentStage.SOURCE, OperationName.SOURCE_SYNC),
            (
                "stage_silver",
                IncidentStage.SILVER,
                OperationName.SOURCE_RECONCILIATION,
            ),
            (
                "stage_data_quality",
                IncidentStage.DATA_QUALITY,
                OperationName.DATA_QUALITY_GATE,
            ),
            (
                "stage_gold",
                IncidentStage.GOLD,
                OperationName.GOLD_ICEBERG_SNAPSHOT_PUBLISH,
            ),
            (
                "stage_shadow",
                IncidentStage.SHADOW_EXECUTION,
                OperationName.SHADOW_NAV_STEP,
            ),
        )
        failed_index = next(
            index
            for index, (_, stage, _) in enumerate(stage_contract)
            if stage is incident.stage
        )
        failed_partition = (
            None
            if not incident.partition_run_id
            else self.production_ledger.get_partition_by_run_id(
                incident.partition_run_id
            )
        )
        if not (
            failed_partition is not None
            and failed_partition.identity.source_id == "research_os"
            and failed_partition.identity.dataset == stage_contract[failed_index][0]
            and failed_partition.identity.partition_key == incident.partition_key
            and failed_partition.status
            in {
                PartitionStatus.FAILED,
                PartitionStatus.DISPUTED,
                PartitionStatus.QUARANTINED,
            }
        ):
            raise OrchestrationFailure(
                "data incident is not bound to its exact failed terminal partition"
            )

        # Data repair is complete only after the rebuilt Gold has produced a
        # real daily projection for the exact production fleet.  The incident
        # stage identifies the origin of the failure; it never shortens the
        # recovery contract to a data-only chain.
        required_stage_contract = stage_contract

        preceding_authorities: list[Any] = []
        base_preceding_hashes: list[str] = []
        for dataset, _, _ in stage_contract[:failed_index]:
            base_identity = PartitionIdentity(
                source_id="research_os",
                dataset=dataset,
                partition_key=incident.partition_key,
            )
            base_record = self.production_ledger.get_partition(base_identity)
            retry_record = self.production_ledger.get_retry_partition(base_identity)
            if retry_record is not None:
                base_record = retry_record
            if not (
                base_record is not None
                and base_record.status is PartitionStatus.SUCCEEDED
                and isinstance(base_record.output_hash, str)
                and re.fullmatch(r"[0-9a-f]{64}", base_record.output_hash)
            ):
                raise OrchestrationFailure(
                    "data incident preceding base evidence is incomplete"
                )
            preceding_authorities.append(base_record)
            base_preceding_hashes.append(base_record.output_hash)
        if tuple(sorted(set(base_preceding_hashes))) != incident.evidence_hashes:
            raise OrchestrationFailure(
                "data incident upstream evidence differs from its authoritative chain"
            )
        failed_input_hash = incident.payload.get("failed_partition_input_hash")
        if (
            failed_input_hash is not None
            and failed_input_hash != failed_partition.input_hash
        ):
            raise OrchestrationFailure(
                "data incident failed-stage input differs from its exact failed partition"
            )
        try:
            repair_predecessor = (
                self.production_ledger.get_incident_repair_predecessor(
                    incident.incident_id
                )
            )
        except ProductionLedgerError as exc:
            raise OrchestrationFailure(
                "data incident repair is detached from the global partition lineage"
            ) from exc
        if repair_predecessor is None:
            raise OrchestrationFailure(
                "revalidation Gold has no complete durable repaired partition chain"
            )
        if incident.stage is IncidentStage.SOURCE:
            if repair_predecessor.identity != failed_partition.identity:
                raise OrchestrationFailure(
                    "Source incident repair does not descend from its exact failed partition"
                )
        elif repair_predecessor.repair_incident_id is None:
            # The first later-stage repair cohort starts from the authoritative
            # Source base (or its generic retry leaf) that also supplied the
            # incident's upstream evidence.
            if (
                not preceding_authorities
                or repair_predecessor.identity
                != preceding_authorities[0].identity
                or repair_predecessor.identity.dataset != "stage_source"
                or repair_predecessor.status is not PartitionStatus.SUCCEEDED
            ):
                raise OrchestrationFailure(
                    "later-stage incident repair has an invalid Source predecessor"
                )
        elif not (
            repair_predecessor.repair_incident_id != incident.incident_id
            and repair_predecessor.identity.dataset == "stage_shadow"
            and repair_predecessor.identity.partition_key
            == incident.partition_key
            and repair_predecessor.status is PartitionStatus.SUCCEEDED
        ):
            raise OrchestrationFailure(
                "cross-incident repair predecessor is not a completed Shadow cohort"
            )

        repaired_chain: list[
            tuple[PartitionIdentity, Any, Mapping[str, Any], Mapping[str, Any]]
        ] = []
        previous_completed_at: datetime | None = None
        repair_fingerprint: str | None = None
        repair_cohort_id: str | None = None
        repair_attempt_run_ids: list[str] = []
        for dataset, _, expected_operation in required_stage_contract:
            try:
                authority_chain = self.production_ledger.get_repair_chain(
                    incident.incident_id, dataset
                )
                record = self.production_ledger.get_repair_partition(
                    incident.incident_id, dataset
                )
            except ProductionLedgerError as exc:
                raise OrchestrationFailure(
                    "revalidation repair authority chain is inconsistent"
                ) from exc
            if not authority_chain or record is None:
                raise OrchestrationFailure(
                    "revalidation Gold has no complete durable repaired partition chain"
                )
            authority = authority_chain[-1]
            expected_root_parent_run_id = (
                repair_predecessor.identity.partition_run_id
                if not repaired_chain
                else repaired_chain[-1][0].partition_run_id
            )
            if (
                authority_chain[0].parent_partition_run_id
                != expected_root_parent_run_id
            ):
                raise OrchestrationFailure(
                    "repaired stages do not form one Source-to-Shadow successor chain"
                )
            for chain_index, chain_authority in enumerate(authority_chain):
                chain_record = self.production_ledger.get_partition(
                    chain_authority.identity
                )
                next_authority = (
                    authority_chain[chain_index + 1]
                    if chain_index + 1 < len(authority_chain)
                    else None
                )
                next_chain_record = (
                    None
                    if next_authority is None
                    else self.production_ledger.get_partition(
                        next_authority.identity
                    )
                )
                if not (
                    chain_record is not None
                    and chain_authority.scope_key
                    == f"incident:{incident.incident_id}"
                    and chain_authority.incident_id == incident.incident_id
                    and chain_authority.identity.source_id == "research_os"
                    and chain_authority.identity.dataset == dataset
                    and chain_authority.identity.partition_key
                    == incident.partition_key
                    and chain_authority.identity.generation != "base"
                    and chain_record.repair_incident_id == incident.incident_id
                    and chain_record.repair_parent_partition_run_id
                    == chain_authority.parent_partition_run_id
                    and chain_record.repair_parent_hash
                    == chain_authority.parent_terminal_hash
                    and chain_record.repair_fingerprint
                    == chain_authority.repair_fingerprint
                    and str(
                        chain_record.details.get("repair_cohort_id") or ""
                    ).strip()
                    == self._repair_cohort_id(
                        incident, chain_authority.repair_fingerprint
                    )
                    and chain_authority.created_at >= incident.occurred_at
                    and (
                        next_authority is None
                        or (
                            next_authority.parent_partition_run_id
                            == chain_authority.identity.partition_run_id
                            and (
                                chain_record.status
                                in {
                                    PartitionStatus.FAILED,
                                    PartitionStatus.DISPUTED,
                                    PartitionStatus.QUARANTINED,
                                }
                                or (
                                    dataset == "stage_shadow"
                                    and chain_record.status
                                    is PartitionStatus.SUCCEEDED
                                    and next_chain_record is not None
                                    and next_chain_record.status
                                    is PartitionStatus.FAILED
                                    and next_chain_record.details.get(
                                        "authority_kind"
                                    )
                                    == "typed_shadow_revalidation_rejection"
                                    and next_chain_record.details.get(
                                        "rejected_partition_run_id"
                                    )
                                    == chain_authority.identity.partition_run_id
                                    and next_chain_record.details.get(
                                        "rejected_output_hash"
                                    )
                                    == chain_record.output_hash
                                )
                            )
                            and chain_record.completed_at is not None
                            and chain_record.completed_at
                            >= incident.occurred_at
                        )
                    )
                ):
                    raise OrchestrationFailure(
                        "repaired partition retry chain is incomplete or mutable"
                    )
                if repair_fingerprint is None:
                    repair_fingerprint = chain_authority.repair_fingerprint
                elif (
                    chain_authority.repair_fingerprint
                    != repair_fingerprint
                ):
                    raise OrchestrationFailure(
                        "repaired partition stages do not share one repair fingerprint"
                    )
            identity = authority.identity
            if not (
                authority.scope_key == f"incident:{incident.incident_id}"
                and authority.incident_id == incident.incident_id
                and identity.source_id == "research_os"
                and identity.dataset == dataset
                and identity.partition_key == incident.partition_key
                and identity.generation != "base"
                and record.identity == identity
                and record.repair_incident_id == incident.incident_id
                and record.repair_parent_partition_run_id
                == authority.parent_partition_run_id
                and record.repair_parent_hash == authority.parent_terminal_hash
                and record.repair_fingerprint == authority.repair_fingerprint
            ):
                raise OrchestrationFailure(
                    "repaired partition differs from its successor selection authority"
                )
            operation_result = record.details.get("operation_result")
            outputs = (
                operation_result.get("outputs")
                if isinstance(operation_result, Mapping)
                else None
            )
            expected_operation_hash = (
                content_fingerprint(
                    dict(operation_result),
                    domain="factor-lab/research-os/v1/production-operation-result",
                )
                if isinstance(operation_result, Mapping)
                else None
            )
            completed_at = record.completed_at
            dagster_run_id = str(record.details.get("dagster_run_id") or "").strip()
            stage_repair_cohort_id = str(
                record.details.get("repair_cohort_id") or ""
            ).strip()
            if not (
                record.status is PartitionStatus.SUCCEEDED
                and completed_at is not None
                and completed_at <= revalidated_at
                and isinstance(record.input_hash, str)
                and re.fullmatch(r"[0-9a-f]{64}", record.input_hash)
                and record.output_hash == expected_operation_hash
                and isinstance(record.output_hash, str)
                and record.details.get("operation") == expected_operation.value
                and dagster_run_id
                and re.fullmatch(
                    r"repaircohort_[0-9a-f]{64}", stage_repair_cohort_id
                )
                and isinstance(operation_result, Mapping)
                and operation_result.get("operation") == expected_operation.value
                and operation_result.get("status") == "completed"
                and isinstance(outputs, Mapping)
            ):
                raise OrchestrationFailure(
                    "accepted Gold is not bound to a complete immutable repaired stage chain"
                )
            if previous_completed_at is not None and completed_at < previous_completed_at:
                raise OrchestrationFailure(
                    "repaired partition stages are not chronologically ordered"
                )
            previous_completed_at = completed_at
            if completed_at < incident.occurred_at:
                raise OrchestrationFailure(
                    "repaired partition stage predates the data incident"
                )
            if repair_cohort_id is None:
                repair_cohort_id = stage_repair_cohort_id
            elif stage_repair_cohort_id != repair_cohort_id:
                raise OrchestrationFailure(
                    "repaired partition stages do not share one durable repair cohort"
                )
            repair_attempt_run_ids.append(dagster_run_id)
            repaired_chain.append((identity, record, operation_result, outputs))

        source_outputs = repaired_chain[0][3]
        bronze_snapshot_ids = tuple(
            map(str, source_outputs.get("bronze_snapshot_ids") or ())
        )
        if not bronze_snapshot_ids or len(bronze_snapshot_ids) != len(
            set(bronze_snapshot_ids)
        ):
            raise OrchestrationFailure(
                "repaired source stage has no unique Bronze snapshot closure"
            )
        for bronze_snapshot_id in bronze_snapshot_ids:
            bronze = self.catalog.get_snapshot(bronze_snapshot_id)
            if bronze is None:
                raise OrchestrationFailure(
                    "repaired source output is not a cataloged Bronze snapshot"
                )
            try:
                self._validate_calendar_recovery_manifest_binding(
                    bronze.reference,
                    expected_tier=SnapshotTier.BRONZE,
                    context="revalidation Bronze",
                )
            except Exception as exc:
                raise OrchestrationFailure(
                    "revalidation Bronze manifest/reference binding is invalid"
                ) from exc
            if not (
                bronze.reference.quality_status is DataQualityStatus.ACCEPTED
                and bronze.reference.snapshot_id == bronze.reference.content_hash
                and not bronze.reference.parent_snapshot_ids
                and "raw_vendor_response"
                in {
                    str(label).strip().lower()
                    for label in bronze.reference.trust_labels
                }
                and bronze.created_at <= repaired_chain[0][1].completed_at
                and bronze.reference.as_of <= repaired_chain[0][1].completed_at
            ):
                raise OrchestrationFailure(
                    "repaired source output is not a cataloged Bronze snapshot"
                )

        silver_outputs = repaired_chain[1][3]
        silver_snapshot_id = str(silver_outputs.get("silver_snapshot_id") or "")
        silver_record = self.catalog.get_snapshot(silver_snapshot_id)
        if silver_record is None:
            raise OrchestrationFailure(
                "repaired Silver stage output is not cataloged"
            )
        silver_reference = silver_record.reference
        try:
            self._validate_calendar_recovery_manifest_binding(
                silver_reference,
                expected_tier=SnapshotTier.SILVER,
                context="revalidation Silver",
            )
        except Exception as exc:
            raise OrchestrationFailure(
                "revalidation Silver manifest/reference binding is invalid"
            ) from exc
        silver_labels = {
            str(label).strip().lower() for label in silver_reference.trust_labels
        }
        if not (
            silver_reference.quality_status is DataQualityStatus.ACCEPTED
            and {"point_in_time", "field_reconciled"}.issubset(silver_labels)
            and tuple(silver_reference.parent_snapshot_ids) == bronze_snapshot_ids
            and repaired_chain[1][1].output_snapshot_id == silver_snapshot_id
            and silver_record.created_at <= repaired_chain[1][1].completed_at
            and silver_reference.as_of <= repaired_chain[1][1].completed_at
        ):
            raise OrchestrationFailure(
                "repaired Silver snapshot is not bound to the source Bronze closure"
            )

        quality_outputs = repaired_chain[2][3]
        quality_report = quality_outputs.get("quality_report")
        if not (
            str(quality_outputs.get("silver_snapshot_id") or "")
            == silver_snapshot_id
            and isinstance(quality_report, Mapping)
            and quality_report.get("status") == "pass"
            and repaired_chain[2][1].output_snapshot_id == silver_snapshot_id
        ):
            raise OrchestrationFailure(
                "repaired data-quality stage does not accept the exact Silver snapshot"
            )

        snapshot = self.catalog.get_snapshot(snapshot_id)
        if snapshot is None:
            raise OrchestrationFailure("revalidation Gold snapshot is not cataloged")
        reference = snapshot.reference
        labels = {str(label).strip().lower() for label in reference.trust_labels}
        forbidden_markers = (
            "unverified",
            "disputed",
            "quarantined",
            "frozen",
            "canary",
            "controlled",
            "engineering",
            "retrospective",
            "non_forward",
            "non-forward",
            "legacy",
            "pseudo",
        )
        if not (
            reference.tier is SnapshotTier.GOLD
            and reference.quality_status is DataQualityStatus.ACCEPTED
            and reference.snapshot_id == reference.content_hash
            and {"point_in_time", "quality_accepted"}.issubset(labels)
            and not any(
                marker in label
                for label in labels
                for marker in forbidden_markers
            )
            and reference.uri.startswith("iceberg://")
        ):
            raise OrchestrationFailure(
                "revalidation requires formal forward-eligible accepted Gold evidence"
            )
        try:
            self._validate_calendar_recovery_manifest_binding(
                reference,
                expected_tier=SnapshotTier.GOLD,
                context="revalidation Gold",
            )
            _, separator, iceberg_tag = reference.uri.partition("#")
            if not separator or not iceberg_tag.endswith(reference.snapshot_id):
                raise OrchestrationFailure(
                    "revalidation Gold Iceberg tag is not bound to its snapshot"
                )
            assert_snapshot_promotion_allowed(
                self.catalog, reference.parent_snapshot_ids
            )
        except Exception as exc:
            raise OrchestrationFailure(
                "revalidation Gold parent or immutable manifest is invalid"
            ) from exc
        if silver_snapshot_id not in reference.parent_snapshot_ids:
            raise OrchestrationFailure(
                "revalidation Gold omits the repaired current Silver parent"
            )

        gold_identity, gold_stage, _, gold_outputs = repaired_chain[3]
        if not (
            gold_stage.output_snapshot_id == reference.snapshot_id
            and str(gold_outputs.get("snapshot_id") or "") == reference.snapshot_id
            and str(gold_outputs.get("uri") or "") == reference.uri
            and tuple(map(str, gold_outputs.get("parent_snapshot_ids") or ()))
            == tuple(reference.parent_snapshot_ids)
            and snapshot.created_at <= gold_stage.completed_at
            and reference.as_of <= gold_stage.completed_at
        ):
            raise OrchestrationFailure(
                "accepted Gold is not bound to the repaired Gold operation outputs"
            )
        evidence = DataRevalidation(
            incident_id=domain_incident.incident_id,
            snapshot_id=reference.snapshot_id,
            snapshot_content_hash=reference.content_hash,
            occurred_at=revalidated_at,
        )
        if len(repaired_chain) == len(stage_contract):
            shadow_identity, shadow_stage, _, shadow_outputs = repaired_chain[4]
            incident_partition_key = str(
                shadow_outputs.get("incident_partition_key") or ""
            ).strip()
            raw_validation_trade_date = str(
                shadow_outputs.get("validation_trade_date") or ""
            ).strip()
            try:
                validation_trade_date = date.fromisoformat(
                    raw_validation_trade_date
                )
            except ValueError as exc:
                raise RetryableShadowRepairEvidence(
                    "Shadow repair lacks a canonical fresh validation session"
                ) from exc
            executed = shadow_outputs.get("executed")
            projections = (
                executed.get("projections")
                if isinstance(executed, Mapping)
                else None
            )
            if not (
                shadow_stage.output_snapshot_id is None
                and shadow_outputs.get("input_mode") == "authoritative_pg"
                and incident_partition_key == incident.partition_key
                and validation_trade_date
                >= date.fromisoformat(incident.partition_key)
                and str(
                    shadow_stage.details.get(
                        "repair_validation_trade_date"
                    )
                    or ""
                ).strip()
                == validation_trade_date.isoformat()
                and isinstance(executed, Mapping)
                and isinstance(projections, Sequence)
                and not isinstance(projections, (str, bytes))
            ):
                raise RetryableShadowRepairEvidence(
                    "shadow-execution incident lacks a repaired authoritative account chain"
                )
            if self.shadow_authority is None:
                raise OrchestrationFailure(
                    "shadow-execution incident lacks current production role authority"
                )

            # The operation result is only an orchestration envelope.  Its
            # ``chain_verified`` flag, account list, NAV and hashes are all
            # caller-controlled JSON and therefore cannot resolve an incident
            # by themselves.  Re-select the production fleet from active role
            # bindings and bind every reported projection to the append-only
            # account ledger and its current tail.
            try:
                active_bindings = self.shadow_authority.active_fleet_bindings()
            except Exception as exc:
                raise OrchestrationFailure(
                    "shadow-execution incident cannot read current production role authority"
                ) from exc

            def binding_identity(binding: Any) -> tuple[str, ...]:
                role = binding.role
                role_value = role.value if isinstance(role, ShadowRole) else str(role)
                return (
                    str(binding.binding_id),
                    str(binding.binding_hash),
                    role_value,
                    str(binding.role_key),
                    str(binding.account_id),
                )

            binding_identities = tuple(map(binding_identity, active_bindings))
            champion_bindings = tuple(
                binding
                for binding in active_bindings
                if binding.role is ShadowRole.CHAMPION
            )
            challenger_bindings = tuple(
                binding
                for binding in active_bindings
                if binding.role is ShadowRole.CHALLENGER
            )
            active_account_ids = tuple(
                str(binding.account_id) for binding in active_bindings
            )
            binding_by_account = {
                str(binding.account_id): binding for binding in active_bindings
            }
            if not (
                len(champion_bindings) == 1
                and len(active_bindings)
                == len(champion_bindings) + len(challenger_bindings)
                and len(active_account_ids) == len(set(active_account_ids))
                and all(bool(binding.active) for binding in active_bindings)
            ):
                raise RetryableShadowRepairEvidence(
                    "shadow-execution production role binding set is invalid"
                )

            raw_challenger_ids = executed.get("challenger_account_ids")
            if not (
                isinstance(raw_challenger_ids, Sequence)
                and not isinstance(raw_challenger_ids, (str, bytes))
            ):
                raise RetryableShadowRepairEvidence(
                    "shadow-execution executed account fields are malformed"
                )
            reported_champion_id = str(
                executed.get("champion_account_id") or ""
            ).strip()
            reported_challenger_ids = tuple(
                str(account_id or "").strip() for account_id in raw_challenger_ids
            )
            expected_champion_id = str(champion_bindings[0].account_id)
            expected_challenger_ids = tuple(
                str(binding.account_id) for binding in challenger_bindings
            )
            if not (
                reported_champion_id == expected_champion_id
                and all(reported_challenger_ids)
                and len(reported_challenger_ids) == len(set(reported_challenger_ids))
                and set(reported_challenger_ids) == set(expected_challenger_ids)
            ):
                raise RetryableShadowRepairEvidence(
                    "shadow-execution executed accounts differ from active role bindings"
                )

            projection_by_account: dict[str, Mapping[str, Any]] = {}
            for projection in projections:
                if not isinstance(projection, Mapping):
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution repaired projection is malformed"
                    )
                account_id = str(projection.get("account_id") or "").strip()
                if not account_id or account_id in projection_by_account:
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution repaired projections are ambiguous"
                    )
                projection_by_account[account_id] = projection
            if set(projection_by_account) != set(active_account_ids):
                raise RetryableShadowRepairEvidence(
                    "shadow-execution repaired projections do not cover the active fleet"
                )

            verified_tails: dict[str, tuple[int, str]] = {}
            projection_tails: dict[str, tuple[int, str]] = {}
            formal_sessions: dict[str, Any] = {}
            formal_projection_reader = getattr(
                self.shadow_authority, "session_projection", None
            )
            fleet_closure_reader = getattr(
                self.shadow_authority, "fleet_closure", None
            )
            if getattr(self, "_production_authority", False) and not (
                callable(formal_projection_reader)
                and callable(fleet_closure_reader)
            ):
                raise OrchestrationFailure(
                    "shadow-execution production authority lacks formal session/closure readers"
                )
            for account_id in active_account_ids:
                projection = projection_by_account[account_id]
                account = self.catalog.get_shadow_account(account_id)
                if account is None or account.status != "active":
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution active role account is not active in the catalog"
                    )
                if not self.catalog.verify_shadow_chain(account_id):
                    raise OrchestrationFailure(
                        "shadow-execution repaired account chain is corrupt"
                    )
                reported_sequence = projection.get("last_event_sequence")
                reported_hash = str(
                    projection.get("last_event_hash") or ""
                ).strip()
                tail = (
                    None
                    if not re.fullmatch(r"[0-9a-f]{64}", reported_hash)
                    else self.catalog.get_shadow_event(
                        account_id=account_id,
                        event_hash=reported_hash,
                    )
                )
                if tail is None or tail.sequence_number != reported_sequence:
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution repaired account has no unique durable projection event"
                    )
                projected_state = tail.payload.get("account_state")
                step_metadata = tail.payload.get("research_os_shadow_step")
                if not (
                    projection.get("trade_date")
                    == validation_trade_date.isoformat()
                    and projection.get("chain_verified") is True
                    and isinstance(reported_sequence, int)
                    and not isinstance(reported_sequence, bool)
                    and re.fullmatch(r"[0-9a-f]{64}", reported_hash)
                    and tail.account_id == account_id
                    and tail.event_type == "account_projected"
                    and tail.occurred_at.astimezone(_SHANGHAI).date().isoformat()
                    == validation_trade_date.isoformat()
                    and tail.occurred_at >= gold_stage.completed_at
                    and tail.occurred_at <= shadow_stage.completed_at
                    and tail.sequence_number == reported_sequence
                    and tail.event_hash == reported_hash
                    and isinstance(projected_state, Mapping)
                    and isinstance(step_metadata, Mapping)
                    and step_metadata.get("kind") == "account_projection"
                    and str(step_metadata.get("step_id") or "").strip()
                    == str(projection.get("step_id") or "").strip()
                    and str(projection.get("step_id") or "").strip()
                ):
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution projection is detached from its durable account tail"
                    )
                projection_tail = (reported_sequence, reported_hash)
                current_tail = (
                    account.last_event_sequence,
                    account.last_event_hash,
                )
                if (
                    incident.status is IncidentStatus.OPEN
                    and current_tail != projection_tail
                ):
                    suffix = self.catalog.list_shadow_events(
                        account_id=account_id, limit=1
                    )
                    exact_partial_effect = (
                        account.last_event_sequence == reported_sequence + 1
                        and len(suffix) == 1
                        and suffix[0].sequence_number == account.last_event_sequence
                        and suffix[0].event_hash == account.last_event_hash
                        and suffix[0].previous_event_hash == reported_hash
                        and suffix[0].event_type == "data_revalidated"
                        and canonical_json(suffix[0].payload)
                        == canonical_json(evidence.to_dict())
                    )
                    if not exact_partial_effect:
                        raise RetryableShadowRepairEvidence(
                            "shadow-execution account tail advanced outside the exact revalidation effect"
                        )
                if projection.get("event_hash") is not None and (
                    str(projection.get("event_hash") or "").strip()
                    != tail.event_hash
                ):
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution projection event hash differs from its durable tail"
                    )
                if projection.get("nav") is not None:
                    try:
                        reported_nav = float(projection["nav"])
                        durable_nav = float(projected_state["nav"])
                    except (KeyError, TypeError, ValueError) as exc:
                        raise RetryableShadowRepairEvidence(
                            "shadow-execution projection NAV is malformed"
                        ) from exc
                    if not (
                        np.isfinite(reported_nav)
                        and np.isfinite(durable_nav)
                        and np.isclose(
                            reported_nav,
                            durable_nav,
                            rtol=1e-9,
                            atol=0.01,
                        )
                    ):
                        raise RetryableShadowRepairEvidence(
                            "shadow-execution projection NAV differs from its durable account"
                        )
                projection_tails[account_id] = projection_tail
                verified_tails[account_id] = current_tail

                if getattr(self, "_production_authority", False):
                    try:
                        formal = formal_projection_reader(
                            account_id=account_id,
                            trade_date=validation_trade_date,
                        )
                    except Exception as exc:
                        raise OrchestrationFailure(
                            "shadow-execution formal session authority is invalid"
                        ) from exc
                    binding = binding_by_account[account_id]
                    try:
                        projection_cash = float(projection.get("cash"))
                        projection_nav = float(projection.get("nav"))
                        projection_benchmark_nav = float(
                            projection.get("benchmark_nav")
                        )
                        raw_position_count = projection.get("position_count")
                        if not (
                            isinstance(raw_position_count, int)
                            and not isinstance(raw_position_count, bool)
                            and isinstance(projection.get("rebalanced"), bool)
                        ):
                            raise TypeError("typed projection fields are malformed")
                        projection_position_count = int(raw_position_count)
                    except (TypeError, ValueError) as exc:
                        raise RetryableShadowRepairEvidence(
                            "shadow-execution formal projection fields are malformed"
                        ) from exc
                    if not (
                        formal is not None
                        and formal.trade_date == validation_trade_date
                        and formal.role_binding_id == str(binding.binding_id)
                        and formal.account_event_sequence == reported_sequence
                        and formal.account_event_hash == reported_hash
                        and formal.decision_snapshot_id
                        == projection.get("decision_snapshot_id")
                        and formal.execution_snapshot_id
                        == projection.get("execution_snapshot_id")
                        and formal.mark_snapshot_id
                        == projection.get("mark_snapshot_id")
                        and formal.rebalanced
                        == projection.get("rebalanced")
                        and np.isfinite(projection_cash)
                        and np.isfinite(projection_nav)
                        and np.isfinite(projection_benchmark_nav)
                        and np.isclose(
                            formal.cash,
                            projection_cash,
                            atol=0.01,
                        )
                        and np.isclose(
                            formal.nav,
                            projection_nav,
                            atol=0.01,
                        )
                        and np.isclose(
                            formal.benchmark_nav,
                            projection_benchmark_nav,
                            atol=0.01,
                        )
                        and formal.position_count
                        == projection_position_count
                        and formal.created_at >= tail.occurred_at
                        and formal.created_at <= shadow_stage.completed_at
                    ):
                        raise RetryableShadowRepairEvidence(
                            "shadow-execution formal session differs from its repaired projection"
                        )
                    formal_sessions[account_id] = formal

            if getattr(self, "_production_authority", False):
                try:
                    closure = fleet_closure_reader(validation_trade_date)
                except Exception as exc:
                    raise OrchestrationFailure(
                        "shadow-execution fleet closure authority is invalid"
                    ) from exc
                expected_members = tuple(
                    sorted(
                        (
                            {
                                "binding_id": str(
                                    binding_by_account[account_id].binding_id
                                ),
                                "binding_hash": str(
                                    binding_by_account[account_id].binding_hash
                                ),
                                "role": (
                                    binding_by_account[account_id].role.value
                                    if isinstance(
                                        binding_by_account[account_id].role,
                                        ShadowRole,
                                    )
                                    else str(binding_by_account[account_id].role)
                                ),
                                "role_key": str(
                                    binding_by_account[account_id].role_key
                                ),
                                "account_id": account_id,
                                "session_hash": formal_sessions[
                                    account_id
                                ].session_hash,
                                "account_event_hash": formal_sessions[
                                    account_id
                                ].account_event_hash,
                            }
                            for account_id in active_account_ids
                        ),
                        key=lambda member: (
                            member["role"],
                            member["role_key"],
                            member["binding_id"],
                        ),
                    )
                )
                if not (
                    closure is not None
                    and closure.trade_date == validation_trade_date
                    and closure.members == expected_members
                    and closure.member_count == len(expected_members)
                    and closure.closed_at
                    >= max(item.created_at for item in formal_sessions.values())
                    and closure.closed_at <= shadow_stage.completed_at
                ):
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution repair lacks its exact immutable fleet closure"
                    )

            # Detect a role switch or append racing the validation window.  The
            # account ledger is append-only, so an observed tail cannot return
            # to the same sequence/hash after a mutation.
            try:
                current_binding_identities = tuple(
                    map(
                        binding_identity,
                        self.shadow_authority.active_fleet_bindings(),
                    )
                )
            except Exception as exc:
                raise OrchestrationFailure(
                    "shadow-execution incident cannot re-read production role authority"
                ) from exc
            if current_binding_identities != binding_identities:
                raise RetryableShadowRepairEvidence(
                    "shadow-execution production role bindings changed during revalidation"
                )
            for account_id, expected_tail in verified_tails.items():
                current_account = self.catalog.get_shadow_account(account_id)
                if current_account is None or (
                    current_account.last_event_sequence,
                    current_account.last_event_hash,
                ) != expected_tail:
                    raise RetryableShadowRepairEvidence(
                        "shadow-execution account tail changed during revalidation"
                    )
        else:  # pragma: no cover - the five-stage contract is fixed above.
            raise OrchestrationFailure(
                "data incident repair did not reach authoritative Shadow execution"
            )
        required_revalidation_time = max(
            shadow_stage.completed_at,
            snapshot.created_at,
            reference.as_of,
        )
        if revalidated_at < required_revalidation_time:
            raise OrchestrationFailure(
                "revalidation timestamp predates authoritative repaired Gold evidence"
            )

        base_resolution = {
            "revalidation_id": evidence.revalidation_id,
            "snapshot_id": reference.snapshot_id,
            "snapshot_content_hash": reference.content_hash,
            "repair_cohort_id": repair_cohort_id,
            "repair_attempt_run_ids": list(repair_attempt_run_ids),
            "validation_trade_date": validation_trade_date.isoformat(),
            "repaired_partition_key": shadow_identity.partition_key,
            "repaired_partition_run_id": shadow_identity.partition_run_id,
            "repaired_stage_completed_at": shadow_stage.completed_at.isoformat(),
            "repaired_gold_partition_run_id": gold_identity.partition_run_id,
            "repaired_gold_completed_at": gold_stage.completed_at.isoformat(),
        }

        expected_lifecycle_evidence = evidence.to_dict()

        def exact_lifecycle_event(sleeve_id: str) -> LifecycleEvent | None:
            key = f"data-revalidation:{evidence.revalidation_id}:{sleeve_id}"
            events = self.catalog.iter_lifecycle_events(
                sleeve_id=sleeve_id,
                cause="data_revalidation_passed",
            )
            try:
                matches = tuple(
                    item for item in events if item.idempotency_key == key
                )
            finally:
                close = getattr(events, "close", None)
                if close is not None:
                    close()
            if not matches:
                return None
            if len(matches) != 1:
                raise OrchestrationFailure(
                    "data revalidation lifecycle identity is ambiguous"
                )
            event = matches[0]
            if not (
                event.sleeve_id == sleeve_id
                and event.from_state is LifecycleState.FROZEN_DATA
                and event.to_state is LifecycleState.DORMANT
                and event.cause == "data_revalidation_passed"
                and event.occurred_at >= revalidated_at
                and canonical_json(event.evidence)
                == canonical_json(expected_lifecycle_evidence)
            ):
                raise OrchestrationFailure(
                    "data revalidation lifecycle evidence is inconsistent"
                )
            return event

        def exact_shadow_event(account_id: str) -> Any | None:
            events = self.catalog.iter_shadow_events_by_type(
                account_id=account_id,
                event_type="data_revalidated",
                since=None,
                through=None,
            )
            try:
                matches = tuple(
                    item
                    for item in events
                    if str(item.payload.get("revalidation_id") or "")
                    == evidence.revalidation_id
                )
            finally:
                close = getattr(events, "close", None)
                if close is not None:
                    close()
            if any(
                canonical_json(item.payload)
                != canonical_json(expected_lifecycle_evidence)
                for item in matches
            ):
                raise OrchestrationFailure(
                    "data revalidation shadow identity has conflicting evidence"
                )
            projection_tail = projection_tails.get(account_id)
            if projection_tail is None:
                raise OrchestrationFailure(
                    "data revalidation lacks the account projection authority"
                )
            causal_matches = tuple(
                item
                for item in matches
                if item.previous_event_hash == projection_tail[1]
                and item.sequence_number == projection_tail[0] + 1
            )
            if not causal_matches:
                return None
            if len(causal_matches) != 1:
                raise OrchestrationFailure(
                    "data revalidation has ambiguous causal Shadow evidence"
                )
            event = causal_matches[0]
            if not (
                event.account_id == account_id
                and event.event_type == "data_revalidated"
                and event.occurred_at >= revalidated_at
                and canonical_json(event.payload)
                == canonical_json(expected_lifecycle_evidence)
            ):
                raise OrchestrationFailure(
                    "data revalidation shadow evidence is inconsistent"
                )
            if incident.status is IncidentStatus.OPEN:
                current = self.catalog.get_shadow_account(account_id)
                if current is None or (
                    current.last_event_sequence,
                    current.last_event_hash,
                ) != (event.sequence_number, event.event_hash):
                    raise RetryableShadowRepairEvidence(
                        "data revalidation Shadow effect is no longer the account tail"
                    )
            return event

        def superseded_lifecycle_events(
            sleeve_id: str,
        ) -> tuple[LifecycleEvent, ...]:
            events = self.catalog.iter_lifecycle_events(
                sleeve_id=sleeve_id,
                cause="data_revalidation_passed",
            )
            try:
                matches = tuple(
                    item
                    for item in events
                    if str(item.evidence.get("incident_id") or "")
                    == evidence.incident_id
                    and str(item.evidence.get("revalidation_id") or "")
                    != evidence.revalidation_id
                )
            finally:
                close = getattr(events, "close", None)
                if close is not None:
                    close()
            for item in matches:
                prior_id = str(
                    item.evidence.get("revalidation_id") or ""
                ).strip()
                if not (
                    prior_id
                    and item.idempotency_key
                    == f"data-revalidation:{prior_id}:{sleeve_id}"
                    and item.from_state is LifecycleState.FROZEN_DATA
                    and item.to_state is LifecycleState.DORMANT
                    and item.cause == "data_revalidation_passed"
                ):
                    raise OrchestrationFailure(
                        "superseded data revalidation lifecycle evidence is inconsistent"
                    )
            return matches

        def verify_frozen_controls(
            lifecycle_records: Sequence[SleeveLifecycleRecord],
            account_ids: Sequence[str],
        ) -> None:
            intent = CashTargetIntent.for_incident(domain_incident)
            expected_incident_evidence = {
                "data_incident": domain_incident.to_dict(),
                "cash_target_intent": intent.to_dict(),
            }
            if any(
                record.state is not SleeveState.FROZEN_DATA
                and exact_lifecycle_event(record.sleeve_id) is None
                and not (
                    record.state is SleeveState.DORMANT
                    and superseded_lifecycle_events(record.sleeve_id)
                )
                for record in lifecycle_records
            ):
                raise OrchestrationFailure(
                    "incident controls do not cover the frozen or replayed lifecycle fleet"
                )
            for record in lifecycle_records:
                key = (
                    f"data-incident:{domain_incident.incident_id}:"
                    f"{record.sleeve_id}"
                )
                events = self.catalog.iter_lifecycle_events(
                    sleeve_id=record.sleeve_id,
                    cause="data_integrity_failure",
                )
                try:
                    matching_lifecycle = tuple(
                        event
                        for event in events
                        if event.idempotency_key == key
                    )
                finally:
                    close = getattr(events, "close", None)
                    if close is not None:
                        close()
                if not (
                    len(matching_lifecycle) == 1
                    and matching_lifecycle[0].to_state
                    is LifecycleState.FROZEN_DATA
                    and matching_lifecycle[0].cause == "data_integrity_failure"
                    and matching_lifecycle[0].occurred_at
                    >= domain_incident.occurred_at
                    and canonical_json(matching_lifecycle[0].evidence)
                    == canonical_json(expected_incident_evidence)
                ):
                    raise OrchestrationFailure(
                        "open incident lacks its exact frozen-fleet lifecycle evidence"
                    )
            for account_id in account_ids:
                events = self.catalog.iter_shadow_events_by_type(
                    account_id=account_id,
                    event_type="cash_target_intent",
                    since=None,
                    through=None,
                )
                try:
                    matching = tuple(
                        event
                        for event in events
                        if str(event.payload.get("intent_id") or "")
                        == intent.intent_id
                    )
                finally:
                    close = getattr(events, "close", None)
                    if close is not None:
                        close()
                if not (
                    len(matching) == 1
                    and matching[0].occurred_at >= domain_incident.occurred_at
                    and canonical_json(matching[0].payload)
                    == canonical_json(intent.to_dict())
                ):
                    raise OrchestrationFailure(
                        "open incident lacks its exact frozen-fleet cash intent"
                    )

        prepared: dict[str, Any] = {}

        def typed_effect_fence(
            *, sleeve_ids: Sequence[str], account_ids: Sequence[str]
        ) -> Mapping[str, Any]:
            if self.shadow_authority is None:
                raise RetryableShadowRepairEvidence(
                    "typed revalidation fence lacks production role authority"
                )
            current_bindings = tuple(
                self.shadow_authority.active_fleet_bindings()
            )
            if tuple(map(binding_identity, current_bindings)) != binding_identities:
                raise RetryableShadowRepairEvidence(
                    "production role bindings changed before the terminal fence"
                )
            selected_accounts = tuple(sorted(map(str, account_ids)))
            if {
                str(binding.account_id) for binding in current_bindings
            } != set(selected_accounts):
                raise RetryableShadowRepairEvidence(
                    "terminal revalidation fleet differs from its role authority"
                )
            account_tails: list[Mapping[str, Any]] = []
            for account_id in selected_accounts:
                account = self.catalog.get_shadow_account(account_id)
                if account is None:
                    raise RetryableShadowRepairEvidence(
                        "terminal revalidation lost a fenced Shadow account"
                    )
                account_tails.append(
                    {
                        "account_id": account_id,
                        "last_event_sequence": account.last_event_sequence,
                        "last_event_hash": account.last_event_hash,
                        "status": account.status,
                    }
                )
            selected_sleeves = tuple(sorted(map(str, sleeve_ids)))
            latest_lifecycle = self.catalog.list_latest_lifecycle_events(
                limit=max(1, len(selected_sleeves)),
                sleeve_ids=selected_sleeves,
            )
            lifecycle_by_sleeve = {
                item.sleeve_id: item for item in latest_lifecycle
            }
            if set(lifecycle_by_sleeve) != set(selected_sleeves):
                raise RetryableShadowRepairEvidence(
                    "terminal revalidation lost a fenced Sleeve lifecycle"
                )
            return {
                "schema_version": "research-os/typed-revalidation-fence/v1",
                "shadow_account_tails": account_tails,
                "shadow_role_bindings": [
                    {
                        "binding_id": str(binding.binding_id),
                        "binding_hash": str(binding.binding_hash),
                        "role": (
                            binding.role.value
                            if isinstance(binding.role, ShadowRole)
                            else str(binding.role)
                        ),
                        "role_key": str(binding.role_key),
                        "account_id": str(binding.account_id),
                    }
                    for binding in current_bindings
                ],
                "lifecycle_tails": [
                    {
                        "sleeve_id": sleeve_id,
                        "event_id": lifecycle_by_sleeve[sleeve_id].event_id,
                        "idempotency_key": lifecycle_by_sleeve[
                            sleeve_id
                        ].idempotency_key,
                        "to_state": lifecycle_by_sleeve[
                            sleeve_id
                        ].to_state.value,
                    }
                    for sleeve_id in selected_sleeves
                ],
            }

        def resolution_for_scope(
            authority: IncidentRecord,
            other_open: tuple[IncidentRecord, ...],
        ) -> Mapping[str, Any]:
            locked_domain = validated_domain_incident(authority, current=True)
            if (
                authority.incident_id != incident.incident_id
                or locked_domain.incident_id != domain_incident.incident_id
            ):
                raise OrchestrationFailure(
                    "data incident authority changed before revalidation"
                )
            blockers: list[str] = []
            for other in other_open:
                validated_domain_incident(other, current=False)
                blockers.append(other.incident_id)
            lifecycle_records, shadow_accounts = self._production_lifecycle_fleet(
                overlay_open_incidents=False
            )
            sleeve_ids = tuple(
                sorted(record.sleeve_id for record in lifecycle_records)
            )
            account_ids = tuple(
                sorted(
                    {
                        str(account_id)
                        for values in shadow_accounts.values()
                        for account_id in values
                    }
                )
            )
            action = "remained_frozen" if blockers else "restored_to_dormant"
            prepared.update(
                {
                    "authority_id": authority.incident_id,
                    "other_open_ids": tuple(blockers),
                    "lifecycle_records": lifecycle_records,
                    "shadow_accounts": shadow_accounts,
                    "sleeve_ids": sleeve_ids,
                    "account_ids": account_ids,
                    "fleet_action": action,
                }
            )
            return {
                **base_resolution,
                "fleet_action": action,
                "blocking_incident_ids": blockers,
                "restored_sleeves": ([] if blockers else list(sleeve_ids)),
                "revalidated_accounts": ([] if blockers else list(account_ids)),
            }

        def apply_revalidation(
            authority: IncidentRecord,
            other_open: tuple[IncidentRecord, ...],
        ) -> Mapping[str, Any]:
            if (
                prepared.get("authority_id") != authority.incident_id
                or prepared.get("other_open_ids")
                != tuple(item.incident_id for item in other_open)
            ):
                raise OrchestrationFailure(
                    "serialized revalidation scope changed before effects"
                )
            lifecycle_records = tuple(prepared["lifecycle_records"])
            shadow_accounts = dict(prepared["shadow_accounts"])
            sleeve_ids = tuple(prepared["sleeve_ids"])
            account_ids = tuple(prepared["account_ids"])
            action = str(prepared["fleet_action"])
            if self.shadow_authority is None or tuple(
                map(
                    binding_identity,
                    self.shadow_authority.active_fleet_bindings(),
                )
            ) != binding_identities:
                raise RetryableShadowRepairEvidence(
                    "production role bindings changed before revalidation effects"
                )
            if set(account_ids) != set(projection_tails):
                raise RetryableShadowRepairEvidence(
                    "revalidation effect fleet differs from the repaired projections"
                )
            # Terminalization is forbidden until this exact incident has
            # materialized both its frozen lifecycle evidence and every cash
            # target intent.  A raw FROZEN state from another incident is not
            # sufficient authority.
            verify_frozen_controls(lifecycle_records, account_ids)
            if action == "remained_frozen":
                return {
                    "fleet_action": action,
                    "restored_sleeves": (),
                    "revalidated_accounts": (),
                    "typed_effect_fence": typed_effect_fence(
                        sleeve_ids=sleeve_ids,
                        account_ids=account_ids,
                    ),
                }

            replay_records: list[SleeveLifecycleRecord] = []
            for record in lifecycle_records:
                prior = exact_lifecycle_event(record.sleeve_id)
                if prior is None:
                    if not (
                        record.state is SleeveState.FROZEN_DATA
                        or (
                            record.state is SleeveState.DORMANT
                            and superseded_lifecycle_events(record.sleeve_id)
                        )
                    ):
                        raise OrchestrationFailure(
                            "only a frozen_data fleet can be revalidated"
                        )
                    replay_records.append(record)
                else:
                    if record.state is not SleeveState.DORMANT:
                        raise OrchestrationFailure(
                            "partially committed revalidation has advanced unexpectedly"
                        )
                    replay_records.append(
                        replace(record, state=SleeveState.FROZEN_DATA)
                    )

            pending_accounts = tuple(
                account_id
                for account_id in account_ids
                if exact_shadow_event(account_id) is None
            )
            replay_accounts = {
                record.sleeve_id: pending_accounts for record in replay_records
            }
            try:
                restored = DataIncidentCoordinator(self.catalog).revalidate(
                    evidence,
                    lifecycle_records=tuple(replay_records),
                    shadow_accounts=replay_accounts,
                    expected_shadow_tails={
                        account_id: projection_tails[account_id][1]
                        for account_id in pending_accounts
                    },
                )
            except DataRevalidationConflict as exc:
                raise RetryableShadowRepairEvidence(str(exc)) from exc
            if tuple(sorted(row.sleeve_id for row in restored)) != sleeve_ids:
                raise OrchestrationFailure(
                    "data revalidation restored an unexpected Sleeve set"
                )
            for sleeve_id in sleeve_ids:
                if exact_lifecycle_event(sleeve_id) is None:
                    raise OrchestrationFailure(
                        "data revalidation lifecycle evidence is incomplete"
                    )
                if self.catalog.latest_lifecycle_state(sleeve_id) is not LifecycleState.DORMANT:
                    raise OrchestrationFailure(
                        "data revalidation did not leave the fleet dormant"
                    )
            for account_id in account_ids:
                if exact_shadow_event(account_id) is None:
                    raise OrchestrationFailure(
                        "data revalidation shadow evidence is incomplete"
                    )
            return {
                "fleet_action": action,
                "restored_sleeves": sleeve_ids,
                "revalidated_accounts": account_ids,
                "typed_effect_fence": typed_effect_fence(
                    sleeve_ids=sleeve_ids,
                    account_ids=account_ids,
                ),
            }

        if incident.status is IncidentStatus.OPEN:
            resolution_argument: Any = resolution_for_scope
        else:
            stored_resolution = incident.payload.get("resolution")
            if not isinstance(stored_resolution, Mapping):
                raise OrchestrationFailure(
                    "terminal data incident lacks immutable revalidation evidence"
                )
            if incident.resolved_at != revalidated_at:
                raise OrchestrationFailure(
                    "terminal revalidation retry timestamp differs from authority"
                )
            if any(
                stored_resolution.get(key) != value
                for key, value in base_resolution.items()
            ):
                raise OrchestrationFailure(
                    "terminal revalidation retry differs from authority evidence"
                )
            resolution_argument = dict(stored_resolution)

        try:
            resolve_kwargs: dict[str, Any] = {
                "resolved_at": revalidated_at,
                "evidence": resolution_argument,
                "apply_effects": apply_revalidation,
            }
            if getattr(self, "_production_authority", False):
                resolve_kwargs["require_effect_fence"] = True
            resolved, effect_result, effects_applied = (
                self.production_ledger._resolve_typed_data_incident_with_effects(
                    incident_id, **resolve_kwargs
                )
            )
        except TypedEffectFenceConflict as exc:
            raise RetryableShadowRepairEvidence(
                "typed revalidation effects changed before terminalization"
            ) from exc
        except ProductionLedgerError as exc:
            raise OrchestrationFailure(
                "data incident revalidation lost serialized authority"
            ) from exc

        resolution = resolved.payload.get("resolution")
        if not isinstance(resolution, Mapping):
            raise OrchestrationFailure(
                "resolved data incident lacks revalidation evidence"
            )
        action = str(resolution.get("fleet_action") or "")
        restored_sleeves = tuple(map(str, resolution.get("restored_sleeves") or ()))
        revalidated_accounts = tuple(
            map(str, resolution.get("revalidated_accounts") or ())
        )
        if action == "restored_to_dormant":
            for sleeve_id in restored_sleeves:
                if exact_lifecycle_event(sleeve_id) is None:
                    raise OrchestrationFailure(
                        "terminal revalidation lacks lifecycle evidence"
                    )
            for account_id in revalidated_accounts:
                if exact_shadow_event(account_id) is None:
                    raise OrchestrationFailure(
                        "terminal revalidation lacks shadow evidence"
                    )
        elif not (
            action == "remained_frozen"
            and not restored_sleeves
            and not revalidated_accounts
            and resolution.get("blocking_incident_ids")
        ):
            raise OrchestrationFailure(
                "terminal revalidation carries an invalid fleet action"
            )
        if effects_applied and not isinstance(effect_result, Mapping):
            raise OrchestrationFailure(
                "data revalidation effects returned no authoritative result"
            )
        return {
            "incident_id": resolved.incident_id,
            "status": resolved.status.value,
            "revalidation_id": evidence.revalidation_id,
            "snapshot_id": reference.snapshot_id,
            "fleet_action": action,
            "blocking_incident_ids": list(
                map(str, resolution.get("blocking_incident_ids") or ())
            ),
            "restored_sleeves": list(restored_sleeves),
            "revalidated_accounts": list(revalidated_accounts),
            "effects_applied": effects_applied,
        }

    def _poll_trading_partitions(self, cursor: str | None) -> TriggerPoll:
        if self.production_ledger is None:
            sensors = self.config.get("sensors") or {}
            raw_path = sensors.get("trading_partitions_path")
            if not raw_path:
                return TriggerPoll(
                    cursor=cursor,
                    message="trading partition sensor input is not configured",
                )
            path = self._input_path(raw_path)
            if not path.is_file():
                return TriggerPoll(cursor=cursor, message="no trading partition feed")
            document = json.loads(path.read_text(encoding="utf-8"))
            values = (
                document.get("partitions", [])
                if isinstance(document, Mapping)
                else document
            )
            unseen = [
                value
                for value in sorted({str(value) for value in values})
                if cursor is None or value > cursor
            ]
        else:
            unseen = list(
                self.production_ledger.accepted_calendar_partitions(
                    after_partition_key=cursor
                )
            )
        triggers = tuple(
            Trigger(
                partition_key=value,
                run_key=f"daily:{value}",
                metadata={"sensor": "new_trading_partition"},
            )
            for value in unseen
        )
        return TriggerPoll(
            triggers=triggers,
            cursor=(unseen[-1] if unseen else cursor),
            message=(f"{len(triggers)} new trading partitions" if triggers else "no new trading partition"),
        )

    def _poll_recovery_sla(self, cursor: str | None) -> TriggerPoll:
        try:
            seen = set(json.loads(cursor)) if cursor else set()
        except (TypeError, ValueError, json.JSONDecodeError):
            seen = set()
        now = self._now()
        keys: list[tuple[str, str, str]] = []
        for case in _active_recovery_cases(self.catalog):
            for checkpoint, due_at in (
                ("drift", case.drift_event_due_at),
                ("diagnosis", case.diagnosis_due_at),
                ("review", case.earliest_recovery_review_at),
            ):
                key = f"{case.recovery_case_id}:{checkpoint}:{due_at.isoformat()}"
                if due_at <= now:
                    keys.append((key, case.recovery_case_id, checkpoint))
        triggers = tuple(
            Trigger(
                partition_key=now.date().isoformat(),
                run_key=f"weekly-recovery:{hashlib.sha256(key.encode()).hexdigest()[:20]}",
                metadata={"recovery_case_id": case_id, "checkpoint": checkpoint},
            )
            for key, case_id, checkpoint in keys
            if key not in seen
        )
        updated = sorted((seen | {key for key, _, _ in keys}))[-1000:]
        return TriggerPoll(
            triggers=triggers,
            cursor=json.dumps(updated, separators=(",", ":")),
            message=(f"{len(triggers)} recovery checkpoints due" if triggers else "no recovery SLA due"),
        )


def create_services() -> ApplicationServices:
    """Load the mandatory orchestration configuration for Dagster injection."""

    runtime_env = _runtime_environment(os.environ)
    raw_path = str(runtime_env.get(ORCHESTRATION_CONFIG_ENV) or "").strip()
    if not raw_path:
        raise ServiceNotConfigured(
            f"set {ORCHESTRATION_CONFIG_ENV} to an application-services JSON document"
        )
    path = Path(raw_path).resolve()
    if not path.is_file():
        raise ServiceNotConfigured(f"orchestration configuration does not exist: {path}")
    config = _read_json(path)
    settings = ResearchOSSettings.from_env(runtime_env)
    effective_production = _effective_production_authority(settings)
    if effective_production and (
        str(getattr(settings, "environment", "local")).strip().lower()
        != "production"
    ):
        # Re-resolve every credential under production's *_FILE-only rules.
        # The database marker cannot be bypassed by launching the process with
        # an accidentally permissive local environment.
        runtime_env = dict(runtime_env)
        runtime_env["FACTOR_LAB_ENVIRONMENT"] = "production"
        settings = ResearchOSSettings.from_env(runtime_env)
        if not _effective_production_authority(settings):
            raise ServiceNotConfigured(
                "PostgreSQL production authority changed during service startup"
            )
    connect_args = _database_connect_args(settings)
    catalog = (
        ResearchCatalog(settings.database_url)
        if connect_args is None
        else ResearchCatalog(settings.database_url, connect_args=connect_args)
    )
    iceberg = config.get("iceberg") or {}
    if not isinstance(iceberg, Mapping):
        raise ServiceNotConfigured("iceberg configuration must be an object")
    publisher = PyIcebergGoldPublisher(
        catalog_name=str(iceberg.get("catalog_name") or "factorlab")
    )
    object_store_archive = S3ImmutableArchive.from_connection(
        endpoint=settings.object_store_endpoint,
        bucket=settings.object_store_bucket,
        access_key=settings.object_store_access_key,
        secret_key=settings.object_store_secret_key,
    )
    source_bundle_manifest = str(
        runtime_env.get(SOURCE_BUNDLE_MANIFEST_ENV) or ""
    ).strip()
    production_evidence = (
        validate_production_config(path, env=runtime_env)
        if effective_production
        else None
    )
    return ApplicationServices(
        config,
        settings=settings,
        catalog=catalog,
        iceberg_publisher=publisher,
        object_store_archive=object_store_archive,
        env=runtime_env,
        config_base=path.parent,
        source_bundle_manifest=source_bundle_manifest or None,
        configuration_path=path,
        production_config_evidence=production_evidence,
    )


__all__ = [
    "APPLICATION_SERVICES_SCHEMA_VERSION",
    "ORCHESTRATION_CONFIG_ENV",
    "WEBUI_ENV_FILE_ENV",
    "ApplicationServices",
    "create_services",
]
