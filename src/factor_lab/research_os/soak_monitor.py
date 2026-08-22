"""Database-backed Dagster code-location heartbeat soak evidence.

The sampler is intended to run from a Dagster sensor evaluation.  Reaching the
sampler therefore proves that the daemon completed a gRPC round trip through
the external code server.  Every sample is additionally bound to the current
Dagster PostgreSQL daemon heartbeat and the Linux PID-1 identity.  A restart
starts a new 24-hour window; callers cannot submit aggregate counters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import os
from pathlib import Path
import re
import socket
from typing import Any, Mapping

from .build_provenance import (
    SourceBundleProvenanceError,
    bind_verified_oci_deployment,
)
from .catalog import ResearchCatalog, RunRecord
from .docker_attestation import (
    COMPOSE_PROJECT as HOST_DOCKER_COMPOSE_PROJECT,
    COMPOSE_SERVICE as HOST_DOCKER_COMPOSE_SERVICE,
    persisted_attestation_binding_errors,
)
from .fingerprint import content_fingerprint
from .production_ledger import ProductionLedger
from .readiness_audit import (
    DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
    DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION,
    DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE,
    DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION,
    dagster_code_location_health_sample_evidence_hash,
    dagster_code_location_health_series_hash,
    dagster_code_location_soak_evidence_hash,
)

try:
    from sqlalchemy import MetaData, Table, inspect, select
except ImportError:  # pragma: no cover - production extras are mandatory there.
    MetaData = Table = inspect = select = None  # type: ignore[assignment]


CODE_LOCATION = "factor_lab_research_os"
SERVICE_NAME = "dagster-code-server"
HEARTBEAT_SOURCE = "dagster_postgresql"
MINIMUM_SOAK = timedelta(hours=24)
MAXIMUM_SAMPLE_GAP_SECONDS = 600.0
MINIMUM_SAMPLE_COUNT = 145
_CONTAINER_ID = re.compile(r"^[0-9a-f]{64}$")
_CONTAINER_IDENTITY = re.compile(r"^[0-9a-f]{12,64}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_OCI_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")


class DagsterSoakError(RuntimeError):
    """Code-location health evidence is unavailable or internally inconsistent."""


class DagsterSoakIncomplete(DagsterSoakError):
    """The current uninterrupted process has not accumulated 24 hours yet."""


@dataclass(frozen=True)
class LocalCodeServerRuntimeIdentity:
    """Physical identity observable by the code server inside its container."""

    container_identity: str
    process_identity: str
    init_started_at: datetime


@dataclass(frozen=True)
class VerifiedCodeServerDeployment:
    """A host attestation bound back to the currently executing code server."""

    build_identity_hash: str
    oci_image_id: str
    deployment_identity_hash: str
    host_attestation_hash: str
    host_attestation_run_id: str
    container_id: str
    compose_config_hash: str
    process_identity: str


def local_code_server_runtime_identity() -> LocalCodeServerRuntimeIdentity:
    """Measure Docker hostname, PID-1 start, boot and root continuity.

    Docker's default hostname is the first 12 characters of the full container
    ID inspected by the host attestor.  PID-1 start time distinguishes a
    recreated container even if an external runtime were to reuse a hostname.
    The root check prevents a nested/chrooted child from borrowing the init
    process evidence.
    """

    proc_stat = Path("/proc/1/stat")
    boot_id = Path("/proc/sys/kernel/random/boot_id")
    hostname_file = Path("/etc/hostname")
    system_stat = Path("/proc/stat")
    if not (
        proc_stat.is_file()
        and boot_id.is_file()
        and hostname_file.is_file()
        and system_stat.is_file()
    ):
        raise DagsterSoakError(
            "physical code-server process identity requires Linux /proc evidence"
        )
    try:
        container_identity = hostname_file.read_text(encoding="utf-8").strip()
        socket_identity = socket.gethostname().strip()
        # Fields after the final ')' begin with field 3 (state); starttime is
        # field 22 and therefore index 19 in this suffix.  rsplit remains safe
        # when the executable name itself contains spaces or parentheses.
        stat_suffix = proc_stat.read_text(encoding="utf-8").rsplit(")", 1)[1]
        stat_fields = stat_suffix.split()
        start_ticks = int(stat_fields[19])
        boot_time = int(
            next(
                line.split()[1]
                for line in system_stat.read_text(encoding="utf-8").splitlines()
                if line.startswith("btime ")
            )
        )
        clock_ticks = int(os.sysconf("SC_CLK_TCK"))
        current_root = os.stat("/")
        init_root = os.stat("/proc/1/root")
        boot_id_bytes = boot_id.read_bytes().strip()
    except (
        IndexError,
        OSError,
        StopIteration,
        TypeError,
        UnicodeError,
        ValueError,
    ):
        raise DagsterSoakError("Linux PID-1 runtime evidence is malformed") from None
    if (
        not _CONTAINER_IDENTITY.fullmatch(container_identity)
        or socket_identity != container_identity
    ):
        raise DagsterSoakError("code-server container identity is non-canonical")
    if clock_ticks <= 0 or not boot_id_bytes:
        raise DagsterSoakError("Linux PID-1 timing evidence is malformed")
    if (current_root.st_dev, current_root.st_ino) != (
        init_root.st_dev,
        init_root.st_ino,
    ):
        raise DagsterSoakError("code server does not share the PID-1 root filesystem")
    init_started_at = datetime.fromtimestamp(
        boot_time + start_ticks / clock_ticks,
        tz=timezone.utc,
    )
    process_identity = content_fingerprint(
        {
            "container_hostname_hash": hashlib.sha256(
                container_identity.encode("utf-8")
            ).hexdigest(),
            "boot_id_hash": hashlib.sha256(boot_id_bytes).hexdigest(),
            "pid1_start_ticks": str(start_ticks),
        },
        domain="factor-lab/research-os/v1/code-server-process-identity",
    )
    return LocalCodeServerRuntimeIdentity(
        container_identity=container_identity,
        process_identity=process_identity,
        init_started_at=init_started_at,
    )


def local_code_server_process_identity() -> str:
    """Hash Linux container/PID-1 facts without exposing host identifiers."""

    return local_code_server_runtime_identity().process_identity


def bind_current_code_server_to_host_attestation(
    run: RunRecord,
    *,
    provenance: Any,
) -> VerifiedCodeServerDeployment:
    """Bind an immutable host proof to the code server that is executing now.

    Host-attestation freshness is deliberately *not* used here: its ten-minute
    window protects admission of new formal work, while a 24-hour soak must be
    able to prove continuity without asking the host to mint a new credential
    every sensor tick.  Continuity is fail-closed on the full container ID,
    PID-1 start time/root, immutable source bundle, image, Compose contract and
    the content-addressed persisted attestation.
    """

    local = local_code_server_runtime_identity()
    metadata = dict(run.metadata)
    labels = metadata.get("service_labels")
    compose_config_hash = (
        str(labels.get("com.docker.compose.config-hash") or "")
        if isinstance(labels, Mapping)
        else ""
    )
    container_id = str(metadata.get("container_id") or "")
    image_id = str(metadata.get("oci_image_id") or "")
    base_digest = str(metadata.get("oci_base_digest") or "")
    repo_digests = tuple(map(str, metadata.get("oci_repo_digests") or ()))
    runtime_contract_hash = str(metadata.get("runtime_contract_hash") or "")
    host_attestation_hash = str(metadata.get("attestation_hash") or "")
    if not (
        _CONTAINER_ID.fullmatch(container_id)
        and container_id.startswith(local.container_identity)
        and _OCI_IMAGE_ID.fullmatch(image_id)
        and _OCI_IMAGE_ID.fullmatch(base_digest)
        and _HASH.fullmatch(runtime_contract_hash)
        and _HASH.fullmatch(host_attestation_hash)
        and compose_config_hash
        and isinstance(labels, Mapping)
        and labels.get("com.docker.compose.project") == HOST_DOCKER_COMPOSE_PROJECT
        and labels.get("com.docker.compose.service") == HOST_DOCKER_COMPOSE_SERVICE
    ):
        raise DagsterSoakError(
            "host attestation does not identify the current code-server container"
        )
    expected_source = {
        "source_bundle_manifest_hash": getattr(
            provenance, "image_source_digest", None
        ),
        "source_tree_hash": getattr(provenance, "code_hash", None),
        "configuration_tree_hash": getattr(
            provenance, "configuration_hash", None
        ),
        "dependency_lock_hash": getattr(
            provenance, "dependency_lock_hash", None
        ),
    }
    if any(metadata.get(key) != value for key, value in expected_source.items()):
        raise DagsterSoakError(
            "host attestation source bundle differs from the executing release"
        )
    try:
        bound = bind_verified_oci_deployment(
            provenance,
            oci_image_id=image_id,
            oci_repo_digests=repo_digests,
            oci_base_digests=(base_digest,),
        )
        attested_container_started_at = _parse_time(
            metadata.get("container_started_at")
        )
    except (SourceBundleProvenanceError, TypeError, ValueError):
        raise DagsterSoakError("host attestation build binding is invalid") from None
    if (
        not bound.formal_epoch_eligible
        or not _HASH.fullmatch(str(bound.build_identity_hash or ""))
        or abs(
            (local.init_started_at - attested_container_started_at).total_seconds()
        )
        > 5.0
    ):
        raise DagsterSoakError(
            "current code-server process does not match the attested container start"
        )
    deployment_identity_hash = content_fingerprint(
        {
            "container_id": container_id,
            "oci_image_id": image_id,
            "compose_config_hash": compose_config_hash,
            "build_identity_hash": bound.build_identity_hash,
            "runtime_contract_hash": runtime_contract_hash,
        },
        domain="research-os/host-docker-deployment-identity/v1",
    )
    stable_deployment = {
        "controlled_test_backend": False,
        "compose_config_hash": compose_config_hash,
        "build_identity_hash": bound.build_identity_hash,
        "runtime_contract_hash": runtime_contract_hash,
        "oci_image_id": image_id,
        "oci_repo_digests": list(repo_digests),
        "oci_base_digests": [base_digest],
    }
    proof = {
        "host_attestation_run_id": run.run_id,
        "host_attestation_hash": host_attestation_hash,
        "attested_at": metadata.get("deployment_verified_at"),
        "container_started_at": metadata.get("container_started_at"),
        "container_id": container_id,
        "deployment_identity_hash": deployment_identity_hash,
        "docker_authority_hash": metadata.get("docker_authority_hash"),
        "compose_config_hash": compose_config_hash,
        "build_identity_hash": bound.build_identity_hash,
        "runtime_contract_hash": runtime_contract_hash,
        "oci_image_id": image_id,
        "oci_repo_digests": list(repo_digests),
        "oci_base_digests": [base_digest],
        "executing_container_identity": local.container_identity,
        "executing_container_started_at": local.init_started_at.isoformat(),
        "executing_root_matches_init_root": True,
    }
    binding_errors = persisted_attestation_binding_errors(
        run=run,
        proof=proof,
        stable_deployment=stable_deployment,
    )
    if binding_errors:
        raise DagsterSoakError(
            "persisted host attestation does not bind to the current code server: "
            + ", ".join(binding_errors)
        )
    return VerifiedCodeServerDeployment(
        build_identity_hash=str(bound.build_identity_hash),
        oci_image_id=image_id,
        deployment_identity_hash=deployment_identity_hash,
        host_attestation_hash=host_attestation_hash,
        host_attestation_run_id=run.run_id,
        container_id=container_id,
        compose_config_hash=compose_config_hash,
        process_identity=local.process_identity,
    )


def _parse_time(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def _parse_dagster_heartbeat_time(value: Any) -> datetime:
    """Interpret Dagster's PostgreSQL heartbeat clock as UTC.

    Dagster owns ``daemon_heartbeats.timestamp`` and creates it as
    ``TIMESTAMP WITHOUT TIME ZONE`` while writing UTC values.  That storage
    contract is different from externally supplied provenance timestamps,
    which remain subject to :func:`_parse_time`'s strict timezone check.
    """

    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sample_hash(metadata: Mapping[str, Any]) -> str:
    return dagster_code_location_health_sample_evidence_hash(metadata)


class DagsterCodeLocationSoakMonitor:
    """Record physical samples and derive one immutable 24-hour soak run."""

    def __init__(
        self,
        catalog: ResearchCatalog,
        production_ledger: ProductionLedger,
        *,
        build_identity_hash: str,
        oci_image_id: str,
        process_identity: str | None = None,
        daemon_type: str = "SENSOR",
        deployment_identity_hash: str | None = None,
        host_attestation_hash: str | None = None,
        container_id: str | None = None,
        compose_config_hash: str | None = None,
    ) -> None:
        if len(str(build_identity_hash)) != 64:
            raise ValueError("build_identity_hash must be SHA-256")
        if daemon_type not in {"SENSOR", "SCHEDULER", "QUEUED_RUN_COORDINATOR"}:
            raise ValueError("unsupported Dagster daemon heartbeat type")
        self.catalog = catalog
        self.ledger = production_ledger
        self.build_identity_hash = str(build_identity_hash)
        self.oci_image_id = str(oci_image_id).strip()
        if not self.oci_image_id.startswith("sha256:") or len(self.oci_image_id) != 71:
            raise ValueError("oci_image_id must be a Docker SHA-256 image identity")
        self.process_identity = process_identity or local_code_server_process_identity()
        if len(self.process_identity) != 64:
            raise ValueError("process_identity must be SHA-256")
        self.daemon_type = daemon_type
        deployment_values = (
            str(deployment_identity_hash or ""),
            str(host_attestation_hash or ""),
            str(container_id or ""),
            str(compose_config_hash or ""),
        )
        if any(deployment_values) and not all(deployment_values):
            raise ValueError(
                "deployment attestation hash, container id and Compose config hash "
                "must be supplied together"
            )
        if deployment_values[0] and not (
            len(deployment_values[0]) == 64
            and len(deployment_values[1]) == 64
            and len(deployment_values[2]) == 64
            and deployment_values[3]
        ):
            raise ValueError("deployment identity fields are invalid")
        self.deployment_identity_hash = deployment_values[0] or None
        self.host_attestation_hash = deployment_values[1] or None
        self.container_id = deployment_values[2] or None
        self.compose_config_hash = deployment_values[3] or None

    def _latest_daemon_heartbeat(self, *, observed_at: datetime) -> datetime:
        if inspect is None:
            raise DagsterSoakError("SQLAlchemy is required for Dagster heartbeat evidence")
        inspector = inspect(self.ledger.engine)
        schema = (
            "dagster"
            if self.ledger.engine.dialect.name == "postgresql"
            and inspector.has_table("daemon_heartbeats", schema="dagster")
            else None
        )
        if not inspector.has_table("daemon_heartbeats", schema=schema):
            raise DagsterSoakError("Dagster heartbeat ledger is missing")
        metadata = MetaData()
        table = Table(
            "daemon_heartbeats", metadata, schema=schema, autoload_with=self.ledger.engine
        )
        if not {"daemon_type", "timestamp"}.issubset(set(table.c.keys())):
            raise DagsterSoakError("Dagster heartbeat ledger has an unknown schema")
        statement = select(table.c.timestamp).where(
            table.c.daemon_type == self.daemon_type
        )
        with self.ledger.engine.connect() as connection:
            values = tuple(row[0] for row in connection.execute(statement))
        parsed: list[datetime] = []
        for value in values:
            try:
                item = (
                    _parse_dagster_heartbeat_time(value)
                    if isinstance(value, (datetime, str))
                    else datetime.fromtimestamp(float(value), tz=timezone.utc)
                )
            except (TypeError, ValueError, OSError):
                continue
            if item <= observed_at:
                parsed.append(item)
        if not parsed:
            raise DagsterSoakError("matching Dagster daemon heartbeat is missing")
        heartbeat = max(parsed)
        gap = (observed_at - heartbeat).total_seconds()
        if gap < 0 or gap > MAXIMUM_SAMPLE_GAP_SECONDS:
            raise DagsterSoakError("Dagster daemon heartbeat is stale")
        return heartbeat

    def record_sample(self) -> RunRecord:
        """Append one database-clock and daemon-heartbeat-bound sample."""

        observed_at = self.catalog.database_now().astimezone(timezone.utc)
        daemon_heartbeat = self._latest_daemon_heartbeat(observed_at=observed_at)
        metadata = {
            "schema_version": DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION,
            "authority": "dagster_sensor_grpc_roundtrip_plus_pg_heartbeat",
            "physical": True,
            "service_name": SERVICE_NAME,
            "code_location": CODE_LOCATION,
            "heartbeat_source": HEARTBEAT_SOURCE,
            "daemon_type": self.daemon_type,
            "healthy": True,
            "sampled_at": observed_at.isoformat(),
            "dagster_heartbeat_at": daemon_heartbeat.isoformat(),
            "maximum_heartbeat_gap_seconds": MAXIMUM_SAMPLE_GAP_SECONDS,
            "process_identity": self.process_identity,
            "build_identity_hash": self.build_identity_hash,
            "oci_image_id": self.oci_image_id,
        }
        if self.deployment_identity_hash is not None:
            metadata.update(
                deployment_identity_hash=self.deployment_identity_hash,
                host_attestation_hash=self.host_attestation_hash,
                container_id=self.container_id,
                compose_config_hash=self.compose_config_hash,
            )
        sample_hash = _sample_hash(metadata)
        metadata["sample_evidence_hash"] = sample_hash
        proposed = RunRecord(
            run_id=f"dagster_health_sample_{sample_hash}",
            run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
            status="succeeded",
            input_fingerprint=sample_hash,
            started_at=observed_at,
            completed_at=observed_at,
            metadata=metadata,
        )
        stored, won = self.catalog.claim_run(proposed)
        if not won and stored != proposed:
            raise DagsterSoakError("health sample identity collided with different evidence")
        return stored

    @staticmethod
    def _valid_sample(run: RunRecord) -> bool:
        metadata = run.metadata
        try:
            observed_at = _parse_time(metadata["sampled_at"])
            daemon_heartbeat = _parse_time(metadata["dagster_heartbeat_at"])
        except (KeyError, TypeError, ValueError):
            return False
        return bool(
            run.run_type == DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE
            and run.status == "succeeded"
            and run.completed_at == observed_at
            and metadata.get("schema_version")
            == DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_SCHEMA_VERSION
            and metadata.get("physical") is True
            and metadata.get("healthy") is True
            and metadata.get("service_name") == SERVICE_NAME
            and metadata.get("code_location") == CODE_LOCATION
            and metadata.get("heartbeat_source") == HEARTBEAT_SOURCE
            and 0
            <= (observed_at - daemon_heartbeat).total_seconds()
            <= MAXIMUM_SAMPLE_GAP_SECONDS
            and float(metadata.get("maximum_heartbeat_gap_seconds") or 1e12)
            <= MAXIMUM_SAMPLE_GAP_SECONDS
            and metadata.get("sample_evidence_hash") == _sample_hash(metadata)
            and run.input_fingerprint == metadata.get("sample_evidence_hash")
        )

    def _continuous_samples(self) -> tuple[RunRecord, ...]:
        rows = [
            run
            for run in self.catalog.list_runs(
                limit=1_000,
                run_type=DAGSTER_CODE_LOCATION_HEALTH_SAMPLE_RUN_TYPE,
            )
            if self._valid_sample(run)
            and run.metadata.get("build_identity_hash") == self.build_identity_hash
            and run.metadata.get("oci_image_id") == self.oci_image_id
            and run.metadata.get("process_identity") == self.process_identity
            and run.metadata.get("daemon_type") == self.daemon_type
            and run.metadata.get("deployment_identity_hash")
            == self.deployment_identity_hash
            and run.metadata.get("container_id") == self.container_id
            and run.metadata.get("compose_config_hash")
            == self.compose_config_hash
        ]
        rows.sort(key=lambda item: (item.completed_at or item.started_at, item.run_id))
        if not rows:
            return ()
        suffix = [rows[-1]]
        for item in reversed(rows[:-1]):
            newer = suffix[0]
            gap = (
                (newer.completed_at or newer.started_at)
                - (item.completed_at or item.started_at)
            ).total_seconds()
            if gap < 0 or gap > MAXIMUM_SAMPLE_GAP_SECONDS:
                break
            suffix.insert(0, item)
        return tuple(suffix)

    def finalize(self, *, provenance: Any) -> RunRecord:
        """Derive a soak run from persisted samples and inspected OCI evidence."""

        if not bool(getattr(provenance, "formal_epoch_eligible", False)):
            raise DagsterSoakError("soak finalization requires inspected OCI provenance")
        if getattr(provenance, "build_identity_hash", None) != self.build_identity_hash:
            raise DagsterSoakError("OCI provenance differs from sampled code build")
        oci_image_id = str(getattr(provenance, "oci_image_id", None) or "")
        if not oci_image_id:
            raise DagsterSoakError("OCI image identity is missing")
        if oci_image_id != self.oci_image_id:
            raise DagsterSoakError("OCI provenance differs from sampled image")
        samples = self._continuous_samples()
        if len(samples) < MINIMUM_SAMPLE_COUNT:
            raise DagsterSoakIncomplete("fewer than 145 physical health samples")
        first = samples[0].completed_at or samples[0].started_at
        last = samples[-1].completed_at or samples[-1].started_at
        if last - first < MINIMUM_SOAK:
            raise DagsterSoakIncomplete("uninterrupted code-location soak is under 24 hours")
        observed_now = self.catalog.database_now().astimezone(timezone.utc)
        if (observed_now - last).total_seconds() > MAXIMUM_SAMPLE_GAP_SECONDS:
            raise DagsterSoakIncomplete("latest code-location sample is stale")
        gaps = [
            (right.completed_at - left.completed_at).total_seconds()  # type: ignore[union-attr]
            for left, right in zip(samples, samples[1:])
        ]
        maximum_gap = max(gaps, default=0.0)
        health_sample_hash = dagster_code_location_health_series_hash(
            tuple(
                str(item.metadata["sample_evidence_hash"])
                for item in samples
            )
        )
        metadata = {
            "schema_version": DAGSTER_CODE_LOCATION_SOAK_SCHEMA_VERSION,
            "authority": "derived_from_persisted_health_samples",
            "physical": True,
            "service_name": SERVICE_NAME,
            "code_location": CODE_LOCATION,
            "heartbeat_source": HEARTBEAT_SOURCE,
            "daemon_type": self.daemon_type,
            "health_sample_count": len(samples),
            "maximum_sample_gap_seconds": maximum_gap,
            "restart_count": 0,
            "health_sample_hash": health_sample_hash,
            "health_sample_run_ids": [item.run_id for item in samples],
            "process_identity": self.process_identity,
            "build_identity_hash": self.build_identity_hash,
            "oci_image_id": oci_image_id,
        }
        if self.deployment_identity_hash is not None:
            metadata.update(
                deployment_identity_hash=self.deployment_identity_hash,
                latest_host_attestation_hash=self.host_attestation_hash,
                container_id=self.container_id,
                compose_config_hash=self.compose_config_hash,
            )
        soak_hash = dagster_code_location_soak_evidence_hash(metadata)
        metadata["soak_evidence_hash"] = soak_hash
        proposed = RunRecord(
            run_id=f"dagster_code_location_soak_{soak_hash}",
            run_type=DAGSTER_CODE_LOCATION_SOAK_RUN_TYPE,
            status="succeeded",
            input_fingerprint=soak_hash,
            started_at=first,
            completed_at=last,
            metadata=metadata,
        )
        stored, won = self.catalog.claim_run(proposed)
        if not won and stored != proposed:
            raise DagsterSoakError("soak identity collided with different evidence")
        return stored


__all__ = [
    "CODE_LOCATION",
    "DagsterCodeLocationSoakMonitor",
    "DagsterSoakError",
    "DagsterSoakIncomplete",
    "HEARTBEAT_SOURCE",
    "LocalCodeServerRuntimeIdentity",
    "MAXIMUM_SAMPLE_GAP_SECONDS",
    "MINIMUM_SAMPLE_COUNT",
    "MINIMUM_SOAK",
    "SERVICE_NAME",
    "VerifiedCodeServerDeployment",
    "bind_current_code_server_to_host_attestation",
    "local_code_server_process_identity",
    "local_code_server_runtime_identity",
]
