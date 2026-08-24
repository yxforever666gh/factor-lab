"""Host-side Docker runtime and deployment attestation.

The attestor never trusts a Compose environment variable for the running
image identity.  It asks the host Docker daemon for the unique healthy
``factor-lab-research-os`` / ``dagster-code-server`` container, binds that
container to the daemon-inspected image ID and pinned base digest, then creates
an inert temporary container from the same image to copy and re-verify the
immutable source bundle.

The production factory hard-codes the Docker CLI runner and requires a
PostgreSQL ResearchCatalog.  Runner injection exists only in the controlled
test factory, whose persisted run type is permanently rejected for readiness.
No operation accepts a container ID, image hash, or caller-authored evidence.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
from typing import Any, Callable, Mapping, Protocol, Sequence

from .build_provenance import (
    SOURCE_BUNDLE_SCHEMA_VERSION,
    SourceBundleProvenanceError,
    verify_source_bundle_manifest,
)
from .catalog import ResearchCatalog, RunRecord
from .fingerprint import content_fingerprint


SCHEMA_VERSION = "research-os/host-docker-runtime-attestation/v2"
RUN_TYPE = "host_docker_runtime_attestation"
ATTEMPT_SCHEMA_VERSION = "research-os/host-docker-runtime-attestation-attempt/v1"
ATTEMPT_RUN_TYPE = "host_docker_runtime_attestation_attempt"
ATTEMPT_AUTHORITY = "host_local_docker_runtime_attestor"
CONTROLLED_TEST_RUN_TYPE = "host_docker_runtime_attestation_test"
COMPOSE_PROJECT = "factor-lab-research-os"
COMPOSE_SERVICE = "dagster-code-server"
READINESS_ADMISSION = "host_daemon_inspected_deployment"
CONTROLLED_TEST_REJECTION = "rejected_controlled_docker_runner"
IMAGE_BUNDLE_ROOT = "/opt/factor-lab"

_SHA256_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
_CONTAINER_ID = re.compile(r"^[0-9a-f]{64}$")
_REPO_DIGEST = re.compile(r"^[^\s@]+@sha256:[0-9a-f]{64}$")
_IMAGE_REFERENCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:+-]{0,511}$")
_FROM = re.compile(
    r"(?m)^FROM\s+(?P<name>[^\s@]+)@(?P<digest>sha256:[0-9a-f]{64})\s*$"
)
_BASE_LABEL = re.compile(
    r"org\.opencontainers\.image\.base\.digest\s*=\s*"
    r'"(?P<digest>sha256:[0-9a-f]{64})"'
)
_SERVICE_LABEL_KEYS = (
    "com.docker.compose.project",
    "com.docker.compose.service",
    "com.docker.compose.oneoff",
    "com.docker.compose.container-number",
    "com.docker.compose.config-hash",
    "com.docker.compose.image",
    "com.docker.compose.version",
)
_EXPECTED_ENTRYPOINT = ("/usr/local/bin/factor-lab-entrypoint",)
_EXPECTED_COMMAND = (
    "dagster",
    "code-server",
    "start",
    "--host",
    "0.0.0.0",
    "--port",
    "4000",
    "--module-name",
    "factor_lab.research_os.dagster_defs",
    "--working-directory",
    "/opt/factor-lab",
    "--location-name",
    "factor_lab_research_os",
)
_EXPECTED_MOUNTS = frozenset(
    {
        ("volume", "/opt/dagster/home/storage", True),
        ("bind", "/opt/factor-lab/runtime/data", True),
        ("bind", "/opt/factor-lab/runtime/artifacts", True),
        ("bind", "/run/secrets", False),
        ("bind", "/run/infra-secrets/postgres_password", False),
        ("bind", "/run/infra-secrets/minio_root_user", False),
        ("bind", "/run/infra-secrets/minio_root_password", False),
    }
)
_MOUNT_AUTHORITY_FILE = "configs/research_os_runtime_mounts.production.json"
_MOUNT_AUTHORITY_SCHEMA = "research-os/runtime-mount-authority/v1"
_RUNTIME_AUTHORITY_FILE = "configs/research_os_runtime_authority.production.json"
_RUNTIME_AUTHORITY_SCHEMA = "research-os/runtime-service-authority/v1"
_VOLUME_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$")
_DANGEROUS_ENVIRONMENT_NAMES = frozenset(
    {
        # Code-loading/shell hooks.
        "BASH_ENV",
        "ENV",
        "LD_LIBRARY_PATH",
        "LD_PRELOAD",
        "PYTHONHOME",
        "PYTHONPATH",
        # Network routing overrides.  Requests/curl and several SDKs honour
        # both upper- and lower-case spellings, so inspection normalises every
        # name with ``upper()`` before comparing it with this set.
        "ALL_PROXY",
        "FTP_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "RSYNC_PROXY",
        # Ambient trust-store/client-certificate overrides.  A reviewed image
        # supplies its own CA bundle; deployment-time values must never be
        # able to replace that trust boundary.
        "AWS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
        "GIT_SSL_CAINFO",
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
        "NODE_EXTRA_CA_CERTS",
        "PIP_CERT",
        "REQUESTS_CA_BUNDLE",
        "SSL_CERT_DIR",
        "SSL_CERT_FILE",
        "SSLKEYLOGFILE",
    }
)
_RUNTIME_CONTRACT_DOMAIN = "research-os/host-docker-runtime-contract/v1"
_DOCKER_AUTHORITY_DOMAIN = "research-os/host-docker-local-authority/v1"
_ATTEMPT_DOMAIN = "research-os/host-docker-runtime-attestation-attempt-identity/v1"
KERNEL_PROCESS_IDENTITY_SCHEME = "linux-boot-id-pid1-start-ticks-v1"
_BACKEND_SERVICES = ("minio", "postgres")
_BUSINESS_ENVIRONMENT_PREFIXES = (
    "AWS_",
    "DAGSTER_",
    "DIEMENG_",
    "FACTOR_LAB_",
    "PYICEBERG_",
    "RESEARCH_OS_",
    "TUSHARE_",
)
_CREDENTIAL_ENVIRONMENT_NAME = re.compile(
    r"(?:^|_)(?:ACCESS_KEY|API_KEY|CREDENTIAL|PASSWORD|PRIVATE_KEY|SECRET|TOKEN)(?:$|_)"
)
_DOCKER_RFC3339_NANO = re.compile(
    r"^(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d+))?(?P<zone>Z|[+-]\d{2}:\d{2})$"
)


class DockerAttestationError(RuntimeError):
    """Host Docker evidence is absent, ambiguous, or internally inconsistent."""


class DockerAttestationAdmissionError(DockerAttestationError):
    """The attestor is not running as a host-side PostgreSQL authority."""


class DockerCommandRunner(Protocol):
    def run(self, arguments: Sequence[str]) -> str: ...


class _SubprocessDockerRunner:
    """No-shell bridge pinned to one local daemon and trusted CLI location.

    The host process environment is not an authority for Docker routing.  A
    ``DOCKER_HOST``/context/TLS override or a PATH-preceding executable would
    otherwise let a remote or fake daemon mint apparently local evidence.
    Formal collection therefore fails before its first Docker subprocess when
    such routing is present, executes one fixed local endpoint explicitly, and
    uses a small sanitized child environment.
    """

    _SYSTEM_PATH = (
        r"C:\Windows\System32;C:\Windows"
        if os.name == "nt"
        else "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    )

    def __init__(self) -> None:
        self._initialized = False
        self._executable: Path | None = None
        self._endpoint = ""
        self._child_environment: dict[str, str] = {}
        self._authority_evidence: dict[str, Any] | None = None

    @property
    def authority_evidence(self) -> Mapping[str, Any]:
        if not self._initialized or self._authority_evidence is None:
            raise DockerAttestationError("local Docker authority is not verified")
        return dict(self._authority_evidence)

    @property
    def host_storage_evidence(self) -> Mapping[str, Any]:
        """Return a secret-free measurement of the local engine data root.

        Docker Desktop's Linux daemon reports ``/var/lib/docker`` inside its
        VM, so on Windows that evidence is incomplete without the protected
        Desktop settings store and the physical VHDX on the selected drive.
        Only the reviewed root, relative disk identity, and boolean checks are
        returned; no other settings-store content is retained.
        """

        self._initialize_local_authority()
        engine_root_raw = self._raw_run(
            ("info", "--format", "{{json .DockerRootDir}}")
        ).strip()
        try:
            engine_root = json.loads(engine_root_raw)
        except json.JSONDecodeError:
            raise DockerAttestationError(
                "local Docker authority returned malformed storage evidence"
            ) from None
        if not isinstance(engine_root, str) or not engine_root.startswith("/"):
            raise DockerAttestationError("Docker engine root is invalid")
        engine_root = engine_root.rstrip("/")
        if os.name != "nt":
            host_root = _canonical_bind_source(
                engine_root,
                require_physical=True,
            )
            return {
                "profile": "linux",
                "engine_root": engine_root,
                "host_data_root": host_root,
                "host_root_authority": "docker_engine_info",
                "required_disk_images": [],
            }

        settings_path = self._windows_docker_settings_store()
        try:
            if settings_path.is_symlink() or not settings_path.is_file():
                raise OSError
            settings = json.loads(settings_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise DockerAttestationError(
                "Docker Desktop settings-store authority is unreadable"
            ) from None
        raw_host_root = (
            settings.get("CustomWslDistroDir")
            if isinstance(settings, Mapping)
            else None
        )
        host_root = _canonical_bind_source(raw_host_root, require_physical=True)
        disk_relative = "disk/docker_data.vhdx"
        disk_path = Path(host_root) / Path(disk_relative)
        try:
            disk_stat = disk_path.stat()
        except OSError:
            raise DockerAttestationError(
                "Docker Desktop data disk is missing"
            ) from None
        if (
            disk_path.is_symlink()
            or not disk_path.is_file()
            or disk_stat.st_size <= 0
            or bool(getattr(disk_stat, "st_file_attributes", 0) & 0x400)
        ):
            raise DockerAttestationError(
                "Docker Desktop data disk is not a physical regular file"
            )
        return {
            "profile": "windows",
            "engine_root": engine_root,
            "host_data_root": host_root,
            "host_root_authority": "docker_desktop_settings_store",
            "required_disk_images": [
                {
                    "relative_path": disk_relative,
                    "physical_regular_file": True,
                }
            ],
        }

    @staticmethod
    def _windows_docker_settings_store() -> Path:
        """Resolve Roaming AppData through the Windows shell, not env vars."""

        try:
            import ctypes

            buffer = ctypes.create_unicode_buffer(32768)
            # CSIDL_APPDATA (26), SHGFP_TYPE_CURRENT (0).
            result = ctypes.windll.shell32.SHGetFolderPathW(
                None, 26, None, 0, buffer
            )
        except (AttributeError, OSError, ValueError):
            raise DockerAttestationError(
                "Windows Roaming AppData authority is unavailable"
            ) from None
        if result != 0 or not buffer.value:
            raise DockerAttestationError(
                "Windows Roaming AppData authority is unavailable"
            )
        return Path(buffer.value) / "Docker" / "settings-store.json"

    @staticmethod
    def _trusted_cli_candidates() -> tuple[Path, ...]:
        if os.name == "nt":
            return (
                Path(r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"),
            )
        return (Path("/usr/bin/docker"), Path("/usr/local/bin/docker"))

    @staticmethod
    def _file_sha256(path: Path) -> str:
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
        except OSError:
            raise DockerAttestationError(
                "trusted Docker CLI cannot be measured"
            ) from None
        return digest.hexdigest()

    def _sanitized_environment(self) -> dict[str, str]:
        allowed = (
            "SystemRoot",
            "WINDIR",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "HOMEDRIVE",
            "HOMEPATH",
            "LANG",
            "LC_ALL",
        )
        child = {
            name: os.environ[name]
            for name in allowed
            if name in os.environ and os.environ[name]
        }
        child["PATH"] = self._SYSTEM_PATH
        return child

    def _raw_run(self, arguments: Sequence[str]) -> str:
        assert self._executable is not None
        try:
            completed = subprocess.run(
                [str(self._executable), "--host", self._endpoint, *arguments],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="strict",
                timeout=180,
                env=self._child_environment,
            )
        except (
            OSError,
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            UnicodeError,
        ):
            command = str(arguments[0]) if arguments else "authority"
            raise DockerAttestationError(
                f"Docker {command} command failed during host attestation"
            ) from None
        return completed.stdout

    def _initialize_local_authority(self) -> None:
        if self._initialized:
            return
        # Reject every Docker routing/control override by name, without ever
        # reading or persisting its possibly sensitive value.
        if any(name.upper().startswith("DOCKER_") for name in os.environ):
            raise DockerAttestationError(
                "ambient Docker routing environment is forbidden"
            )
        candidates = tuple(
            candidate.resolve()
            for candidate in self._trusted_cli_candidates()
            if candidate.is_file()
        )
        if len(candidates) != 1:
            raise DockerAttestationError(
                "exactly one trusted host Docker CLI is required"
            )
        executable = candidates[0]
        discovered = shutil.which("docker")
        if discovered is None or Path(discovered).resolve() != executable:
            raise DockerAttestationError(
                "host PATH does not resolve to the trusted Docker CLI"
            )
        self._executable = executable
        self._endpoint = (
            "npipe:////./pipe/dockerDesktopLinuxEngine"
            if os.name == "nt"
            else "unix:///var/run/docker.sock"
        )
        self._child_environment = self._sanitized_environment()
        version_raw = self._raw_run(
            (
                "version",
                "--format",
                "{{json .Client.Version}}|{{json .Server.Version}}|{{json .Server.Os}}",
            )
        ).strip()
        try:
            client_raw, server_raw, server_os_raw = version_raw.split("|", 2)
            client_version = json.loads(client_raw)
            server_version = json.loads(server_raw)
            server_os = json.loads(server_os_raw)
        except (ValueError, json.JSONDecodeError):
            raise DockerAttestationError(
                "local Docker authority returned malformed version evidence"
            ) from None
        version_pattern = re.compile(r"^[0-9]+(?:\.[0-9]+){1,3}(?:[-+._A-Za-z0-9]*)?$")
        if not (
            isinstance(client_version, str)
            and isinstance(server_version, str)
            and version_pattern.fullmatch(client_version)
            and version_pattern.fullmatch(server_version)
            and server_os == "linux"
        ):
            raise DockerAttestationError(
                "local Docker authority is not the approved Linux engine"
            )
        self._authority_evidence = {
            "authority": "explicit_local_docker_engine_endpoint",
            "endpoint_policy": (
                "windows_docker_desktop_linux_named_pipe"
                if os.name == "nt"
                else "local_unix_docker_socket"
            ),
            "cli_path": str(executable),
            "cli_sha256": self._file_sha256(executable),
            "client_version": client_version,
            "server_version": server_version,
            "server_os": server_os,
            "ambient_docker_routing_rejected": True,
        }
        self._initialized = True

    def run(self, arguments: Sequence[str]) -> str:
        values = tuple(map(str, arguments))
        if not values:
            raise DockerAttestationError("empty Docker command")
        self._initialize_local_authority()
        return self._raw_run(values)


@dataclass(frozen=True)
class DockerRuntimeEvidence:
    container_id: str
    image_id: str
    image_reference: str
    repo_digests: tuple[str, ...]
    base_image_name: str
    base_image_digest: str
    service_labels: Mapping[str, str]
    state_status: str
    health_status: str
    health_failing_streak: int
    docker_authority: Mapping[str, Any]
    runtime_contract: Mapping[str, Any]
    runtime_contract_hash: str
    container_started_at: datetime
    inspected_at: datetime


@dataclass(frozen=True)
class DockerDeploymentEvidence:
    source_bundle_manifest_hash: str
    source_tree_hash: str
    configuration_tree_hash: str
    runtime_tree_hash: str
    dependency_lock_hash: str
    source_file_count: int
    configuration_file_count: int
    runtime_file_count: int
    base_image_name: str
    base_image_digest: str
    deployment_evidence_hash: str
    temporary_container_removed: bool
    running_container_bundle_verified: bool
    verified_at: datetime


@dataclass(frozen=True)
class HostDockerAttestationResult:
    run_id: str
    run_type: str
    physical: bool
    readiness_admission: str
    attestation_hash: str
    runtime: DockerRuntimeEvidence
    deployment: DockerDeploymentEvidence

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["runtime"]["inspected_at"] = self.runtime.inspected_at.isoformat()
        payload["deployment"]["verified_at"] = (
            self.deployment.verified_at.isoformat()
        )
        return payload


def persisted_attestation_binding_errors(
    *,
    run: RunRecord | None,
    proof: Mapping[str, Any] | None,
    stable_deployment: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Validate a canary's historical host proof without applying freshness.

    Freshness belongs to the *current* readiness attestation. A long-running
    canary instead keeps the exact proof that admitted it, while its evaluator
    fingerprint contains only stable image/build/Compose facts. This validator
    binds that historical proof back to the immutable ``ros_runs`` authority.
    """

    errors: list[str] = []
    if not isinstance(proof, Mapping):
        return ("runtime_attestation_proof_missing",)
    if not isinstance(stable_deployment, Mapping):
        return ("runtime_stable_deployment_missing",)

    attestation_hash = str(proof.get("host_attestation_hash") or "")
    attestation_run_id = str(proof.get("host_attestation_run_id") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", attestation_hash):
        errors.append("runtime_attestation_hash_invalid")
    if attestation_run_id != f"docker_attestation_{attestation_hash}":
        errors.append("runtime_attestation_run_id_invalid")
    if run is None:
        errors.append("runtime_attestation_run_missing")
        return tuple(sorted(set(errors)))

    metadata = dict(run.metadata)
    stored_hash = str(metadata.get("attestation_hash") or "")
    hash_payload = dict(metadata)
    hash_payload.pop("attestation_hash", None)
    recomputed_hash = content_fingerprint(hash_payload, domain=SCHEMA_VERSION)
    try:
        inspected_at = _aware(
            datetime.fromisoformat(
                str(metadata.get("inspected_at") or "").replace("Z", "+00:00")
            ),
            name="inspected_at",
        )
        verified_at = _aware(
            datetime.fromisoformat(
                str(metadata.get("deployment_verified_at") or "").replace(
                    "Z", "+00:00"
                )
            ),
            name="deployment_verified_at",
        )
        proof_attested_at = _aware(
            datetime.fromisoformat(
                str(proof.get("attested_at") or "").replace("Z", "+00:00")
            ),
            name="attested_at",
        )
        container_started_at = _aware(
            datetime.fromisoformat(
                str(metadata.get("container_started_at") or "").replace(
                    "Z", "+00:00"
                )
            ),
            name="container_started_at",
        )
        proof_container_started_at = _aware(
            datetime.fromisoformat(
                str(proof.get("container_started_at") or "").replace(
                    "Z", "+00:00"
                )
            ),
            name="proof container_started_at",
        )
        raw_executing_started_at = proof.get("executing_container_started_at")
        executing_container_started_at = (
            _aware(
                datetime.fromisoformat(
                    str(raw_executing_started_at).replace("Z", "+00:00")
                ),
                name="executing container_started_at",
            )
            if raw_executing_started_at not in {None, ""}
            else None
        )
    except (TypeError, ValueError, DockerAttestationError):
        inspected_at = verified_at = proof_attested_at = container_started_at = (
            proof_container_started_at
        ) = datetime.min.replace(tzinfo=timezone.utc)
        executing_container_started_at = None
        errors.append("runtime_attestation_time_invalid")

    service_labels = metadata.get("service_labels")
    runtime_contract = metadata.get("runtime_contract")
    runtime_contract_hash = str(metadata.get("runtime_contract_hash") or "")
    recomputed_runtime_contract_hash = (
        content_fingerprint(runtime_contract, domain=_RUNTIME_CONTRACT_DOMAIN)
        if isinstance(runtime_contract, Mapping)
        else ""
    )
    docker_authority = metadata.get("docker_authority")
    docker_authority_hash = str(metadata.get("docker_authority_hash") or "")
    recomputed_docker_authority_hash = (
        content_fingerprint(docker_authority, domain=_DOCKER_AUTHORITY_DOMAIN)
        if isinstance(docker_authority, Mapping)
        else ""
    )
    compose_config_hash = (
        str(service_labels.get("com.docker.compose.config-hash") or "")
        if isinstance(service_labels, Mapping)
        else ""
    )
    image_id = str(metadata.get("oci_image_id") or "")
    container_id = str(metadata.get("container_id") or "")
    executing_container_identity = str(
        proof.get("executing_container_identity") or ""
    )
    executing_process_identity = str(
        proof.get("executing_process_identity") or ""
    )
    executing_process_identity_scheme = str(
        proof.get("executing_process_identity_scheme") or ""
    )
    process_identity_fields_declared = bool(
        executing_process_identity or executing_process_identity_scheme
    )
    has_kernel_process_identity = bool(
        executing_process_identity_scheme
        == KERNEL_PROCESS_IDENTITY_SCHEME
        and re.fullmatch(r"[0-9a-f]{64}", executing_process_identity)
    )
    has_legacy_started_at = executing_container_started_at is not None
    ambiguous_process_continuity = bool(
        process_identity_fields_declared and has_legacy_started_at
    )
    if process_identity_fields_declared and not has_kernel_process_identity:
        errors.append("runtime_process_identity_invalid")
    if ambiguous_process_continuity:
        errors.append("runtime_process_continuity_ambiguous")
    valid_process_continuity = bool(
        (has_kernel_process_identity and not has_legacy_started_at)
        or (has_legacy_started_at and not process_identity_fields_declared)
    )
    if not valid_process_continuity and not ambiguous_process_continuity:
        errors.append("runtime_process_continuity_missing")
    repo_digests = tuple(map(str, metadata.get("oci_repo_digests") or ()))
    base_digest = str(metadata.get("oci_base_digest") or "")
    stable_repo_digests = tuple(
        map(str, stable_deployment.get("oci_repo_digests") or ())
    )
    stable_base_digests = tuple(
        map(str, stable_deployment.get("oci_base_digests") or ())
    )
    proof_repo_digests = tuple(map(str, proof.get("oci_repo_digests") or ()))
    proof_base_digests = tuple(map(str, proof.get("oci_base_digests") or ()))
    build_identity_hash = str(stable_deployment.get("build_identity_hash") or "")
    deployment_identity_hash = content_fingerprint(
        {
            "container_id": container_id,
            "oci_image_id": image_id,
            "compose_config_hash": compose_config_hash,
            "build_identity_hash": build_identity_hash,
            "runtime_contract_hash": runtime_contract_hash,
        },
        domain="research-os/host-docker-deployment-identity/v1",
    )
    try:
        health_failing_streak = int(metadata.get("health_failing_streak", -1))
    except (TypeError, ValueError):
        health_failing_streak = -1
        errors.append("runtime_attestation_health_invalid")

    deployment_values = {
        "source_bundle_manifest_hash": metadata.get("source_bundle_manifest_hash"),
        "source_tree_hash": metadata.get("source_tree_hash"),
        "configuration_tree_hash": metadata.get("configuration_tree_hash"),
        "runtime_tree_hash": metadata.get("runtime_tree_hash"),
        "dependency_lock_hash": metadata.get("dependency_lock_hash"),
        "source_file_count": metadata.get("source_file_count"),
        "configuration_file_count": metadata.get("configuration_file_count"),
        "runtime_file_count": metadata.get("runtime_file_count"),
        "base_image_name": metadata.get("oci_base_name"),
        "base_image_digest": base_digest,
    }
    recomputed_deployment_hash = content_fingerprint(
        {
            **deployment_values,
            "runtime_image_id": image_id,
            "temporary_container_removed": True,
            "running_container_bundle_verified": True,
            "verified_at": verified_at,
        },
        domain=f"{SCHEMA_VERSION}/deployment",
    )

    if not (
        run.run_id == attestation_run_id
        and run.run_type == RUN_TYPE
        and run.status == "succeeded"
        and run.input_fingerprint == attestation_hash
        and run.started_at == inspected_at
        and run.completed_at == verified_at
        and proof_attested_at == verified_at
        and stored_hash == attestation_hash == recomputed_hash
    ):
        errors.append("runtime_attestation_run_invalid")
    if not (
        metadata.get("schema_version") == SCHEMA_VERSION
        and metadata.get("authority")
        == "host_docker_daemon_plus_verified_image_bundle"
        and metadata.get("physical") is True
        and metadata.get("controlled_test_runner") is False
        and metadata.get("readiness_admission") == READINESS_ADMISSION
        and metadata.get("compose_project") == COMPOSE_PROJECT
        and metadata.get("compose_service") == COMPOSE_SERVICE
        and metadata.get("container_state") == "running"
        and metadata.get("container_health") == "healthy"
        and health_failing_streak == 0
        and metadata.get("daemon_image_id_verified") is True
        and metadata.get("pinned_base_digest_verified") is True
        and metadata.get("deployment_bundle_verified") is True
        and metadata.get("temporary_container_removed") is True
        and metadata.get("running_container_bundle_verified") is True
        and metadata.get("formal_readiness_eligible") is True
        and isinstance(docker_authority, Mapping)
        and docker_authority.get("authority")
        == "explicit_local_docker_engine_endpoint"
        and docker_authority.get("ambient_docker_routing_rejected") is True
        and docker_authority.get("server_os") == "linux"
        and re.fullmatch(r"[0-9a-f]{64}", docker_authority_hash)
        and docker_authority_hash == recomputed_docker_authority_hash
        and proof.get("docker_authority_hash") == docker_authority_hash
        and runtime_contract_hash == recomputed_runtime_contract_hash
        and metadata.get("deployment_evidence_hash") == recomputed_deployment_hash
    ):
        errors.append("runtime_attestation_contract_invalid")
    if not (
        re.fullmatch(r"[0-9a-f]{64}", container_id)
        and _SHA256_ID.fullmatch(image_id)
        and _SHA256_ID.fullmatch(base_digest)
        and isinstance(service_labels, Mapping)
        and service_labels.get("com.docker.compose.project") == COMPOSE_PROJECT
        and service_labels.get("com.docker.compose.service") == COMPOSE_SERVICE
        and compose_config_hash
    ):
        errors.append("runtime_attestation_oci_identity_invalid")
    if not (
        stable_deployment.get("controlled_test_backend") is False
        and re.fullmatch(r"[0-9a-f]{64}", build_identity_hash)
        and proof.get("build_identity_hash") == build_identity_hash
        and proof.get("compose_config_hash") == compose_config_hash
        and stable_deployment.get("compose_config_hash") == compose_config_hash
        and proof.get("oci_image_id") == image_id
        and stable_deployment.get("oci_image_id") == image_id
        and proof_repo_digests == repo_digests == stable_repo_digests
        and proof_base_digests == (base_digest,) == stable_base_digests
        and proof.get("container_id") == container_id
        and 12 <= len(executing_container_identity) <= 64
        and all(
            character in "0123456789abcdef"
            for character in executing_container_identity
        )
        and container_id.startswith(executing_container_identity)
        and proof_container_started_at == container_started_at
        and container_started_at <= inspected_at
        and valid_process_continuity
        and (
            (has_kernel_process_identity and not has_legacy_started_at)
            or (
                not process_identity_fields_declared
                and executing_container_started_at is not None
                and abs(
                    (
                        executing_container_started_at
                        - container_started_at
                    ).total_seconds()
                )
                <= 5.0
            )
        )
        and proof.get("executing_root_matches_init_root") is True
        and proof.get("deployment_identity_hash") == deployment_identity_hash
        and proof.get("runtime_contract_hash") == runtime_contract_hash
        and stable_deployment.get("runtime_contract_hash")
        == runtime_contract_hash
    ):
        errors.append("runtime_attestation_stable_binding_invalid")
    return tuple(sorted(set(errors)))


def _aware(value: datetime, *, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DockerAttestationError(f"{name} must include a timezone")
    return value.astimezone(timezone.utc)


def _parse_docker_timestamp(value: Any, *, name: str) -> datetime:
    """Parse Docker's RFC3339Nano timestamp without losing ordering precision.

    Python's ``datetime`` stores microseconds and rejects the nine-digit
    fractional seconds emitted by Docker Desktop on Windows.  Truncate only
    the sub-microsecond tail, which cannot be represented by the catalog.
    """

    match = _DOCKER_RFC3339_NANO.fullmatch(str(value or ""))
    if match is None:
        raise DockerAttestationError(f"{name} is not a Docker RFC3339 timestamp")
    fraction = match.group("fraction")
    rendered_fraction = ""
    if fraction:
        rendered_fraction = f".{fraction[:6].ljust(6, '0')}"
    zone = "+00:00" if match.group("zone") == "Z" else match.group("zone")
    try:
        parsed = datetime.fromisoformat(
            f"{match.group('whole')}{rendered_fraction}{zone}"
        )
    except ValueError:
        raise DockerAttestationError(
            f"{name} is not a Docker RFC3339 timestamp"
        ) from None
    return _aware(parsed, name=name)


def _parse_single_record(raw: str, *, kind: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        raise DockerAttestationError(
            f"Docker returned malformed {kind} inspection evidence"
        ) from None
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(
        payload[0], Mapping
    ):
        raise DockerAttestationError(
            f"Docker returned non-canonical {kind} inspection evidence"
        )
    return payload[0]


def _safe_label_value(value: Any) -> str:
    rendered = str(value or "")
    if not rendered or len(rendered) > 512 or any(
        ord(character) < 32 for character in rendered
    ):
        raise DockerAttestationError("Docker Compose service label is invalid")
    return rendered


def _catalog_is_postgresql(catalog: ResearchCatalog) -> bool:
    backend = getattr(catalog, "_backend", None)
    engine = getattr(backend, "_engine", None)
    return getattr(getattr(engine, "dialect", None), "name", None) == "postgresql"


def _canonical_image_name(value: str) -> str:
    """Normalize Docker Hub's implicit registry/library aliases."""

    name = str(value or "").strip()
    if not _IMAGE_REFERENCE.fullmatch(name):
        raise DockerAttestationError("base image name is invalid")
    first, separator, _ = name.partition("/")
    if not separator:
        return f"docker.io/library/{name}"
    if "." not in first and ":" not in first and first != "localhost":
        return f"docker.io/{name}"
    return name


def _canonical_bind_source(value: Any, *, require_physical: bool) -> str:
    """Canonicalize one daemon-reported host bind without following aliases."""

    raw = str(value or "").strip().replace("\\", "/").rstrip("/")
    if not raw or "\x00" in raw or any(part == ".." for part in raw.split("/")):
        raise DockerAttestationError("Docker bind source identity is invalid")
    if os.name == "nt":
        desktop_path = re.fullmatch(
            r"/(?:run/desktop/mnt/host|host_mnt)/([A-Za-z])/(.+)", raw
        )
        if desktop_path is not None:
            raw = f"{desktop_path.group(1).upper()}:/{desktop_path.group(2)}"
        if not re.fullmatch(r"[A-Za-z]:/.+", raw):
            raise DockerAttestationError(
                "Windows Docker bind source is not an absolute drive path"
            )
        raw = raw[0].upper() + raw[1:]
    elif not raw.startswith("/"):
        raise DockerAttestationError("Docker bind source is not absolute")
    if require_physical:
        path = Path(raw)
        try:
            resolved = path.resolve(strict=True)
        except OSError:
            raise DockerAttestationError(
                "Docker bind source is missing or cannot be resolved"
            ) from None
        rendered = str(resolved).replace("\\", "/").rstrip("/")
        if os.name == "nt":
            rendered = rendered[0].upper() + rendered[1:]
        if rendered.casefold() != raw.casefold():
            raise DockerAttestationError(
                "Docker bind source resolves through an unapproved alias"
            )
    return raw


def _parse_environment(raw: Any) -> dict[str, str]:
    """Parse Docker's ``NAME=value`` list without ever rendering values."""

    if not isinstance(raw, list):
        raise DockerAttestationError("container environment contract is invalid")
    parsed: dict[str, str] = {}
    for item in raw:
        name, separator, value = str(item).partition("=")
        name = name.strip().upper()
        if (
            separator != "="
            or not re.fullmatch(r"[A-Z_][A-Z0-9_]{0,127}", name)
            or name in parsed
        ):
            raise DockerAttestationError("container environment contract is invalid")
        if _CREDENTIAL_ENVIRONMENT_NAME.search(name) and not name.endswith("_FILE"):
            raise DockerAttestationError(
                "credential-shaped environment must use an approved *_FILE variable"
            )
        parsed[name] = value
    return parsed


def _deployment_environment(
    *, container_environment: Mapping[str, str], image_environment: Mapping[str, str]
) -> dict[str, str]:
    """Return deploy-time overrides, excluding immutable image defaults."""

    return {
        name: value
        for name, value in sorted(container_environment.items())
        if image_environment.get(name) != value
    }


def _business_environment(values: Mapping[str, str]) -> dict[str, str]:
    return {
        name: value
        for name, value in sorted(values.items())
        if name.startswith(_BUSINESS_ENVIRONMENT_PREFIXES)
    }


def _pinned_repo_digest(image_reference: str) -> str:
    name, separator, digest = str(image_reference or "").partition("@")
    if separator != "@" or not _SHA256_ID.fullmatch(digest):
        raise DockerAttestationError("backend image reference is not digest-pinned")
    last_slash = name.rfind("/")
    last_colon = name.rfind(":")
    repository = name[:last_colon] if last_colon > last_slash else name
    if not repository or not _IMAGE_REFERENCE.fullmatch(repository):
        raise DockerAttestationError("backend image repository is invalid")
    return f"{repository}@{digest}"


def _normalized_mounts(raw: Any, *, require_physical: bool) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        raise DockerAttestationError("container mount contract is malformed")
    identities: list[dict[str, Any]] = []
    destinations: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise DockerAttestationError("container mount evidence is malformed")
        kind = str(item.get("Type") or "")
        destination = str(item.get("Destination") or "")
        if not destination.startswith("/") or destination in destinations:
            raise DockerAttestationError("container mount destination is invalid")
        destinations.add(destination)
        identity: dict[str, Any] = {
            "type": kind,
            "destination": destination,
            "read_write": bool(item.get("RW")),
        }
        if kind == "bind":
            identity["source"] = _canonical_bind_source(
                item.get("Source"), require_physical=require_physical
            )
        elif kind == "volume":
            name = str(item.get("Name") or "")
            if not _VOLUME_NAME.fullmatch(name):
                raise DockerAttestationError("named-volume identity is invalid")
            identity["name"] = name
        else:
            raise DockerAttestationError("unsupported container mount type")
        identities.append(identity)
    return sorted(identities, key=lambda item: str(item["destination"]))


def _normalized_healthcheck(raw: Any) -> dict[str, Any] | None:
    if raw in (None, {}):
        return None
    if not isinstance(raw, Mapping):
        raise DockerAttestationError("container healthcheck contract is malformed")
    test = raw.get("Test")
    try:
        normalized = {
            "test": list(map(str, test or ())),
            "interval_ns": int(raw.get("Interval") or 0),
            "timeout_ns": int(raw.get("Timeout") or 0),
            "retries": int(raw.get("Retries") or 0),
        }
    except (TypeError, ValueError):
        raise DockerAttestationError("container healthcheck contract is invalid") from None
    if not normalized["test"] or any(
        normalized[key] <= 0 for key in ("interval_ns", "timeout_ns", "retries")
    ):
        raise DockerAttestationError("container healthcheck contract is invalid")
    return normalized


def _normalized_ports(raw: Any) -> list[dict[str, str]]:
    if raw in (None, {}):
        return []
    if not isinstance(raw, Mapping):
        raise DockerAttestationError("container published-port contract is malformed")
    ports: list[dict[str, str]] = []
    for container_port, bindings in raw.items():
        if not isinstance(bindings, list) or len(bindings) != 1 or not isinstance(
            bindings[0], Mapping
        ):
            raise DockerAttestationError("container published-port contract is invalid")
        host_ip = str(bindings[0].get("HostIp") or "")
        host_port = str(bindings[0].get("HostPort") or "")
        if (
            host_ip != "127.0.0.1"
            or not re.fullmatch(r"[1-9][0-9]{0,4}", host_port)
            or int(host_port) > 65535
            or not re.fullmatch(
                r"[1-9][0-9]{0,4}/(?:tcp|udp)", str(container_port)
            )
        ):
            raise DockerAttestationError("container exposes an unapproved host port")
        ports.append(
            {
                "container": str(container_port),
                "host_ip": host_ip,
                "host_port": host_port,
            }
        )
    return sorted(ports, key=lambda item: str(item["container"]))


def _normalized_security(host_config: Any) -> dict[str, Any]:
    if not isinstance(host_config, Mapping):
        raise DockerAttestationError("container host security contract is malformed")
    raw_tmpfs = host_config.get("Tmpfs") or {}
    if not isinstance(raw_tmpfs, Mapping):
        raise DockerAttestationError("container tmpfs contract is malformed")
    return {
        "privileged": host_config.get("Privileged") is True,
        "read_only_root_filesystem": host_config.get("ReadonlyRootfs") is True,
        "cap_add": sorted(map(str, host_config.get("CapAdd") or ())),
        "cap_drop": sorted(map(str, host_config.get("CapDrop") or ())),
        "security_options": sorted(map(str, host_config.get("SecurityOpt") or ())),
        "tmpfs": {
            str(destination): sorted(
                option.strip()
                for option in str(options).split(",")
                if option.strip()
            )
            for destination, options in sorted(raw_tmpfs.items())
        },
    }


def _network_identity(
    container: Mapping[str, Any], *, service: str
) -> tuple[dict[str, Any], str]:
    settings = container.get("NetworkSettings")
    networks = settings.get("Networks") if isinstance(settings, Mapping) else None
    if not isinstance(networks, Mapping) or len(networks) != 1:
        raise DockerAttestationError(
            f"{service} must attach to exactly one Compose network"
        )
    name, raw = next(iter(networks.items()))
    if not isinstance(raw, Mapping):
        raise DockerAttestationError(f"{service} network evidence is malformed")
    aliases = raw.get("Aliases")
    network_id = str(raw.get("NetworkID") or "")
    if (
        not _VOLUME_NAME.fullmatch(str(name))
        or not isinstance(aliases, list)
        or any(not isinstance(value, str) or not value for value in aliases)
        or len(set(aliases)) != len(aliases)
        or not _CONTAINER_ID.fullmatch(network_id)
    ):
        raise DockerAttestationError(f"{service} network evidence is invalid")
    return (
        {
            "name": str(name),
            "aliases": sorted(aliases),
            "single_network": True,
        },
        network_id,
    )


def _safe_attempt_error(exc: BaseException) -> str:
    if isinstance(exc, DockerAttestationAdmissionError):
        return "docker_attestation_admission_error"
    if isinstance(exc, DockerAttestationError):
        return "docker_attestation_error"
    return "docker_attestation_internal_error"


def host_docker_attempt_fingerprint(*, started_at: datetime, nonce: str) -> str:
    """Recompute the immutable identity of one host-originated attempt."""

    normalized = _aware(started_at, name="attempt_started_at")
    if not re.fullmatch(r"[0-9a-f]{32}", str(nonce or "")):
        raise DockerAttestationError("Docker attestation attempt nonce is invalid")
    return content_fingerprint(
        {
            "schema_version": ATTEMPT_SCHEMA_VERSION,
            "authority": ATTEMPT_AUTHORITY,
            "physical": True,
            "started_at": normalized,
            "attempt_nonce": nonce,
        },
        domain=_ATTEMPT_DOMAIN,
    )


class HostDockerRuntimeAttestor:
    """Measure the running code-server and the exact files in its image."""

    def __init__(
        self,
        *,
        catalog: ResearchCatalog | None,
        runner: DockerCommandRunner,
        clock: Callable[[], datetime],
        controlled_test: bool,
    ) -> None:
        self.catalog = catalog
        self.runner = runner
        self._clock = clock
        self.controlled_test = bool(controlled_test)

    @classmethod
    def from_host(
        cls,
        *,
        catalog: ResearchCatalog,
    ) -> "HostDockerRuntimeAttestor":
        """Construct the only formal producer; no Docker facts are accepted."""

        return cls(
            catalog=catalog,
            runner=_SubprocessDockerRunner(),
            clock=lambda: datetime.now(timezone.utc),
            controlled_test=False,
        )

    @classmethod
    def for_controlled_test(
        cls,
        *,
        runner: DockerCommandRunner,
        catalog: ResearchCatalog | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> "HostDockerRuntimeAttestor":
        """Exercise Docker parsing/copying while producing rejected evidence."""

        return cls(
            catalog=catalog,
            runner=runner,
            clock=clock,
            controlled_test=True,
        )

    def _assert_admission(self) -> None:
        if self.controlled_test:
            return
        if self.catalog is None or not _catalog_is_postgresql(self.catalog):
            raise DockerAttestationAdmissionError(
                "formal host Docker attestation requires PostgreSQL persistence"
            )
        # A formal producer must execute outside the workload container.  This
        # check intentionally fails before any Docker command is attempted.
        if Path("/.dockerenv").exists():
            raise DockerAttestationAdmissionError(
                "formal Docker attestation must execute on the deployment host"
            )

    def _image_record(self, image_id: str) -> Mapping[str, Any]:
        image = _parse_single_record(
            self.runner.run(("image", "inspect", image_id)),
            kind="image",
        )
        if str(image.get("Id") or "") != image_id or not _SHA256_ID.fullmatch(
            image_id
        ):
            raise DockerAttestationError("daemon image identity is invalid")
        return image

    def _host_storage_contract(self) -> dict[str, Any]:
        evidence = getattr(self.runner, "host_storage_evidence", None)
        if callable(evidence):
            evidence = evidence()
        if not isinstance(evidence, Mapping):
            raise DockerAttestationError(
                "formal Docker runner has no verified host storage authority"
            )
        profile = str(evidence.get("profile") or "")
        engine_root = str(evidence.get("engine_root") or "").rstrip("/")
        host_data_root = str(evidence.get("host_data_root") or "")
        root_authority = str(evidence.get("host_root_authority") or "")
        disks = evidence.get("required_disk_images")
        if (
            profile not in {"windows", "linux"}
            or profile != ("windows" if os.name == "nt" else "linux")
            or not engine_root.startswith("/")
            or not host_data_root
            or root_authority
            not in {"docker_desktop_settings_store", "docker_engine_info"}
            or not isinstance(disks, list)
        ):
            raise DockerAttestationError("host Docker storage evidence is invalid")
        normalized_disks: list[dict[str, Any]] = []
        for item in disks:
            if not isinstance(item, Mapping):
                raise DockerAttestationError(
                    "host Docker disk-image evidence is malformed"
                )
            relative = str(item.get("relative_path") or "")
            if (
                not relative
                or relative.startswith(("/", "\\"))
                or ".." in relative.replace("\\", "/").split("/")
                or item.get("physical_regular_file") is not True
            ):
                raise DockerAttestationError(
                    "host Docker disk-image evidence is invalid"
                )
            normalized_disks.append(
                {
                    "relative_path": relative.replace("\\", "/"),
                    "physical_regular_file": True,
                }
            )
        return {
            "profile": profile,
            "engine_root": engine_root,
            "host_data_root": host_data_root.replace("\\", "/").rstrip("/"),
            "host_root_authority": root_authority,
            "required_disk_images": sorted(
                normalized_disks, key=lambda item: str(item["relative_path"])
            ),
        }

    def _inspect_backend_service(
        self, *, service: str, shared_network_id: str
    ) -> tuple[dict[str, Any], str]:
        selected = self.runner.run(
            (
                "ps",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={COMPOSE_PROJECT}",
                "--filter",
                f"label=com.docker.compose.service={service}",
                "--filter",
                "status=running",
                "--format",
                "{{.ID}}",
            )
        )
        ids = tuple(value.strip() for value in selected.splitlines() if value.strip())
        if len(ids) != 1 or not _CONTAINER_ID.fullmatch(ids[0]):
            raise DockerAttestationError(
                f"expected exactly one running {service} Compose service"
            )
        container = _parse_single_record(
            self.runner.run(("inspect", "--type", "container", ids[0])),
            kind=f"{service} container",
        )
        config = container.get("Config")
        state = container.get("State")
        host_config = container.get("HostConfig")
        mounts = container.get("Mounts")
        health = state.get("Health") if isinstance(state, Mapping) else None
        labels = config.get("Labels") if isinstance(config, Mapping) else None
        if not (
            str(container.get("Id") or "") == ids[0]
            and isinstance(config, Mapping)
            and isinstance(state, Mapping)
            and isinstance(host_config, Mapping)
            and isinstance(mounts, list)
            and isinstance(health, Mapping)
            and isinstance(labels, Mapping)
            and labels.get("com.docker.compose.project") == COMPOSE_PROJECT
            and labels.get("com.docker.compose.service") == service
            and str(labels.get("com.docker.compose.oneoff", "False")).lower()
            in {"false", "0"}
            and state.get("Running") is True
            and state.get("Status") == "running"
            and health.get("Status") == "healthy"
        ):
            raise DockerAttestationError(
                f"{service} is not the unique healthy approved Compose service"
            )
        try:
            failing_streak = int(health.get("FailingStreak") or 0)
        except (TypeError, ValueError):
            raise DockerAttestationError(
                f"{service} health streak is invalid"
            ) from None
        if failing_streak != 0:
            raise DockerAttestationError(
                f"healthy {service} has a nonzero failure streak"
            )
        network, network_id = _network_identity(container, service=service)
        if network_id != shared_network_id:
            raise DockerAttestationError(
                f"{service} does not share the code-server Compose network"
            )

        image_id = str(container.get("Image") or "")
        image_reference = str(config.get("Image") or "").strip()
        expected_repo_digest = _pinned_repo_digest(image_reference)
        image = self._image_record(image_id)
        repo_digests = image.get("RepoDigests")
        image_config = image.get("Config")
        if (
            not isinstance(repo_digests, list)
            or expected_repo_digest not in repo_digests
            or not isinstance(image_config, Mapping)
        ):
            raise DockerAttestationError(
                f"{service} image is not bound to its pinned repository digest"
            )
        container_environment = _parse_environment(config.get("Env") or [])
        image_environment = _parse_environment(image_config.get("Env") or [])
        deployment_environment = _deployment_environment(
            container_environment=container_environment,
            image_environment=image_environment,
        )
        mount_contract: list[dict[str, Any]] = []
        destinations: set[str] = set()
        for item in mounts:
            if not isinstance(item, Mapping):
                raise DockerAttestationError(
                    f"{service} mount evidence is malformed"
                )
            kind = str(item.get("Type") or "")
            destination = str(item.get("Destination") or "")
            if not destination.startswith("/") or destination in destinations:
                raise DockerAttestationError(
                    f"{service} mount destination is invalid"
                )
            destinations.add(destination)
            identity: dict[str, Any] = {
                "type": kind,
                "destination": destination,
                "read_write": bool(item.get("RW")),
            }
            if kind == "bind":
                identity["source"] = _canonical_bind_source(
                    item.get("Source"),
                    require_physical=not self.controlled_test,
                )
            elif kind == "volume":
                name = str(item.get("Name") or "")
                if not _VOLUME_NAME.fullmatch(name):
                    raise DockerAttestationError(
                        f"{service} named-volume identity is invalid"
                    )
                identity["name"] = name
            else:
                raise DockerAttestationError(
                    f"{service} has an unsupported mount type"
                )
            mount_contract.append(identity)

        volumes = [item for item in mounts if isinstance(item, Mapping) and item.get("Type") == "volume"]
        if len(volumes) != 1:
            raise DockerAttestationError(
                f"{service} must have exactly one persistent named volume"
            )
        volume = volumes[0]
        volume_name = str(volume.get("Name") or "")
        volume_destination = str(volume.get("Destination") or "")
        if not (
            _VOLUME_NAME.fullmatch(volume_name)
            and volume_destination.startswith("/")
            and volume.get("RW") is True
        ):
            raise DockerAttestationError(
                f"{service} persistent volume evidence is invalid"
            )
        raw_healthcheck = config.get("Healthcheck")
        port_bindings = host_config.get("PortBindings")
        if not isinstance(raw_healthcheck, Mapping) or not isinstance(
            port_bindings, Mapping
        ):
            raise DockerAttestationError(
                f"{service} healthcheck or port-binding contract is missing"
            )
        health_test = raw_healthcheck.get("Test")
        try:
            health_contract = {
                "test": list(map(str, health_test or ())),
                "interval_ns": int(raw_healthcheck.get("Interval") or 0),
                "timeout_ns": int(raw_healthcheck.get("Timeout") or 0),
                "retries": int(raw_healthcheck.get("Retries") or 0),
            }
        except (TypeError, ValueError):
            raise DockerAttestationError(
                f"{service} healthcheck contract is invalid"
            ) from None
        if not health_contract["test"] or any(
            health_contract[key] <= 0
            for key in ("interval_ns", "timeout_ns", "retries")
        ):
            raise DockerAttestationError(
                f"{service} healthcheck contract is invalid"
            )
        published_ports: list[dict[str, str]] = []
        for container_port, bindings in port_bindings.items():
            if not isinstance(bindings, list) or len(bindings) != 1 or not isinstance(
                bindings[0], Mapping
            ):
                raise DockerAttestationError(
                    f"{service} published-port contract is invalid"
                )
            host_ip = str(bindings[0].get("HostIp") or "")
            host_port = str(bindings[0].get("HostPort") or "")
            if (
                host_ip != "127.0.0.1"
                or not re.fullmatch(r"[1-9][0-9]{0,4}", host_port)
                or int(host_port) > 65535
                or not re.fullmatch(r"[1-9][0-9]{0,4}/(?:tcp|udp)", str(container_port))
            ):
                raise DockerAttestationError(
                    f"{service} exposes an unapproved host port"
                )
            published_ports.append(
                {
                    "container": str(container_port),
                    "host_ip": host_ip,
                    "host_port": host_port,
                }
            )
        cap_add = tuple(sorted(map(str, host_config.get("CapAdd") or ())))
        cap_drop = tuple(sorted(map(str, host_config.get("CapDrop") or ())))
        security_options = tuple(
            sorted(map(str, host_config.get("SecurityOpt") or ()))
        )
        return (
            {
                "entrypoint": list(map(str, config.get("Entrypoint") or ())),
                "command": list(map(str, config.get("Cmd") or ())),
                "user": str(config.get("User") or ""),
                "image_reference": image_reference,
                "image_id": image_id,
                "pinned_repo_digest": expected_repo_digest,
                "environment": dict(sorted(deployment_environment.items())),
                "network": network,
                "mounts": sorted(
                    mount_contract, key=lambda item: str(item["destination"])
                ),
                "volume_destination": volume_destination,
                "volume_name": volume_name,
                "healthcheck": health_contract,
                "published_ports": sorted(
                    published_ports, key=lambda item: str(item["container"])
                ),
                "security": {
                    "privileged": host_config.get("Privileged") is True,
                    "read_only_root_filesystem": host_config.get("ReadonlyRootfs")
                    is True,
                    "cap_add": list(cap_add),
                    "cap_drop": list(cap_drop),
                    "security_options": list(security_options),
                },
                "healthy": True,
                "health_failing_streak": 0,
            },
            volume_name,
        )

    def _inspect_volume(self, *, name: str, engine_root: str) -> dict[str, Any]:
        volume = _parse_single_record(
            self.runner.run(("volume", "inspect", name)),
            kind="volume",
        )
        labels = volume.get("Labels")
        options = volume.get("Options")
        mountpoint = str(volume.get("Mountpoint") or "").rstrip("/")
        expected_mountpoint = f"{engine_root}/volumes/{name}/_data"
        if not (
            str(volume.get("Name") or "") == name
            and volume.get("Driver") == "local"
            and volume.get("Scope") == "local"
            and options in (None, {})
            and isinstance(labels, Mapping)
            and labels.get("com.docker.compose.project") == COMPOSE_PROJECT
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(labels.get("com.docker.compose.config-hash") or ""),
            )
            and mountpoint == expected_mountpoint
        ):
            raise DockerAttestationError(
                "named-volume authority differs from the local Docker engine"
            )
        compose_volume = str(labels.get("com.docker.compose.volume") or "")
        if not _VOLUME_NAME.fullmatch(compose_volume):
            raise DockerAttestationError("Compose volume label is invalid")
        return {
            "name": name,
            "driver": "local",
            "scope": "local",
            "mountpoint": mountpoint,
            "compose_project": COMPOSE_PROJECT,
            "compose_volume": compose_volume,
            "compose_config_hash": str(
                labels.get("com.docker.compose.config-hash")
            ),
        }

    def _inspect_shared_network(
        self,
        *,
        name: str,
        network_id: str,
        runtime_container_id: str,
        runtime_image_id: str,
        runtime_image_reference: str,
    ) -> tuple[dict[str, Any], frozenset[str]]:
        runtime_image = self._image_record(runtime_image_id)
        runtime_image_config = runtime_image.get("Config")
        if not isinstance(runtime_image_config, Mapping):
            raise DockerAttestationError(
                "verified runtime image lacks environment authority"
            )
        runtime_image_environment = _parse_environment(
            runtime_image_config.get("Env") or []
        )
        network = _parse_single_record(
            self.runner.run(("network", "inspect", name)),
            kind="network",
        )
        labels = network.get("Labels")
        containers = network.get("Containers")
        if not (
            str(network.get("Name") or "") == name
            and str(network.get("Id") or "") == network_id
            and network.get("Driver") == "bridge"
            and network.get("Internal") is False
            and network.get("Attachable") is False
            and isinstance(labels, Mapping)
            and labels.get("com.docker.compose.project") == COMPOSE_PROJECT
            and labels.get("com.docker.compose.network") == "default"
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(labels.get("com.docker.compose.config-hash") or ""),
            )
            and isinstance(containers, Mapping)
            and containers
        ):
            raise DockerAttestationError(
                "shared Compose network authority is invalid"
            )
        members: dict[str, Any] = {}
        all_aliases: set[str] = set()
        code_server_aliases: list[str] | None = None
        for raw_id, raw_member in containers.items():
            member_id = str(raw_id)
            if not _CONTAINER_ID.fullmatch(member_id) or not isinstance(
                raw_member, Mapping
            ):
                raise DockerAttestationError(
                    "shared Compose network member is malformed"
                )
            member_name = str(raw_member.get("Name") or "")
            if not _VOLUME_NAME.fullmatch(member_name) or member_name in members:
                raise DockerAttestationError(
                    "shared Compose network member identity is invalid"
                )
            container = _parse_single_record(
                self.runner.run(("inspect", "--type", "container", member_id)),
                kind="network member container",
            )
            config = container.get("Config")
            host_config = container.get("HostConfig")
            member_labels = config.get("Labels") if isinstance(config, Mapping) else None
            contract, member_network_id = _network_identity(
                container,
                service=member_name,
            )
            aliases = contract["aliases"]
            compose_service = str(
                member_labels.get("com.docker.compose.service") or ""
            ) if isinstance(member_labels, Mapping) else ""
            if not (
                str(container.get("Id") or "") == member_id
                and str(container.get("Name") or "").lstrip("/") == member_name
                and isinstance(member_labels, Mapping)
                and member_labels.get("com.docker.compose.project") == COMPOSE_PROJECT
                and str(member_labels.get("com.docker.compose.oneoff", "False")).lower()
                in {"false", "0"}
                and member_network_id == network_id
                and contract["name"] == name
                and not all_aliases.intersection(aliases)
            ):
                raise DockerAttestationError(
                    "shared Compose network contains an unbound or alias-colliding member"
                )
            if compose_service == COMPOSE_SERVICE and member_id != runtime_container_id:
                raise DockerAttestationError(
                    "shared Compose network code-server identity is ambiguous"
                )
            if compose_service in {
                COMPOSE_SERVICE,
                "dagster-daemon",
                "dagster-webserver",
                "research-os-webui",
            }:
                member_environment = _parse_environment(config.get("Env") or [])
                if not (
                    str(container.get("Image") or "") == runtime_image_id
                    and str(config.get("Image") or "") == runtime_image_reference
                    and isinstance(host_config, Mapping)
                    and host_config.get("Privileged") is False
                    and not host_config.get("CapAdd")
                    and not (
                        set(member_environment) & _DANGEROUS_ENVIRONMENT_NAMES
                    )
                ):
                    raise DockerAttestationError(
                        "shared Compose application member differs from the verified image authority"
                    )
                application_contract = {
                    "entrypoint": list(map(str, config.get("Entrypoint") or ())),
                    "command": list(map(str, config.get("Cmd") or ())),
                    "working_directory": str(config.get("WorkingDir") or ""),
                    "user": str(config.get("User") or ""),
                    "environment": _deployment_environment(
                        container_environment=member_environment,
                        image_environment=runtime_image_environment,
                    ),
                    "mounts": _normalized_mounts(
                        container.get("Mounts"),
                        require_physical=not self.controlled_test,
                    ),
                    "healthcheck": _normalized_healthcheck(
                        config.get("Healthcheck")
                    ),
                    "published_ports": _normalized_ports(
                        host_config.get("PortBindings")
                    ),
                    "security": _normalized_security(host_config),
                }
            else:
                application_contract = None
            all_aliases.update(aliases)
            members[member_name] = {
                "compose_service": compose_service,
                "aliases": aliases,
                "verified_runtime_image": compose_service
                in {
                    COMPOSE_SERVICE,
                    "dagster-daemon",
                    "dagster-webserver",
                    "research-os-webui",
                },
                "application_contract": (
                    None if compose_service == COMPOSE_SERVICE else application_contract
                ),
            }
            if members[member_name]["compose_service"] == COMPOSE_SERVICE:
                if code_server_aliases is not None:
                    raise DockerAttestationError(
                        "shared Compose network has multiple code-server members"
                    )
                code_server_aliases = list(aliases)
        if code_server_aliases is None:
            raise DockerAttestationError(
                "shared Compose network lacks the attested code-server"
            )
        return (
            {
                "name": name,
                "aliases": code_server_aliases,
                "single_network": True,
                "driver": "bridge",
                "internal": False,
                "attachable": False,
                "compose_network": "default",
                "compose_config_hash": str(
                    labels.get("com.docker.compose.config-hash")
                ),
                "running_members": dict(sorted(members.items())),
            },
            frozenset(map(str, containers)),
        )

    def _audit_protected_resource_consumers(
        self,
        *,
        allowed_container_ids: frozenset[str],
        protected_volume_names: frozenset[str],
        protected_bind_sources: frozenset[str],
    ) -> dict[str, Any]:
        selected = self.runner.run(
            (
                "ps",
                "--no-trunc",
                "--filter",
                "status=running",
                "--format",
                "{{.ID}}",
            )
        )
        running_ids = tuple(
            value.strip() for value in selected.splitlines() if value.strip()
        )
        if (
            not running_ids
            or len(running_ids) != len(set(running_ids))
            or any(not _CONTAINER_ID.fullmatch(value) for value in running_ids)
            or not allowed_container_ids.issubset(running_ids)
        ):
            raise DockerAttestationError(
                "global running-container enumeration is incomplete or malformed"
            )
        protected_casefold = tuple(
            source.casefold().rstrip("/") for source in protected_bind_sources
        )
        for container_id in running_ids:
            if container_id in allowed_container_ids:
                continue
            container = _parse_single_record(
                self.runner.run(("inspect", "--type", "container", container_id)),
                kind="global running container",
            )
            host_config = container.get("HostConfig")
            mounts = container.get("Mounts")
            if not isinstance(host_config, Mapping) or not isinstance(mounts, list):
                raise DockerAttestationError(
                    "global running-container evidence is malformed"
                )
            network_mode = str(host_config.get("NetworkMode") or "")
            if network_mode.startswith("container:"):
                target = network_mode.partition(":")[2]
                if any(value.startswith(target) for value in allowed_container_ids):
                    raise DockerAttestationError(
                        "unapproved container shares a protected network namespace"
                    )
            for mount in mounts:
                if not isinstance(mount, Mapping):
                    raise DockerAttestationError(
                        "global running-container mount evidence is malformed"
                    )
                kind = str(mount.get("Type") or "")
                if kind == "volume" and str(mount.get("Name") or "") in protected_volume_names:
                    raise DockerAttestationError(
                        "unapproved container consumes a protected named volume"
                    )
                if kind == "bind":
                    source = _canonical_bind_source(
                        mount.get("Source"),
                        require_physical=not self.controlled_test,
                    ).casefold().rstrip("/")
                    if any(
                        source == protected
                        or source.startswith(f"{protected}/")
                        or protected.startswith(f"{source}/")
                        for protected in protected_casefold
                    ):
                        raise DockerAttestationError(
                            "unapproved container consumes a protected bind root"
                        )
        return {
            "global_running_containers_inspected": len(running_ids),
            "allowed_protected_consumers": len(allowed_container_ids),
            "protected_resource_exclusivity_verified": True,
        }

    def _inspect_runtime(self) -> DockerRuntimeEvidence:
        inspected_at = _aware(self._clock(), name="inspected_at")
        selected = self.runner.run(
            (
                "ps",
                "--no-trunc",
                "--filter",
                f"label=com.docker.compose.project={COMPOSE_PROJECT}",
                "--filter",
                f"label=com.docker.compose.service={COMPOSE_SERVICE}",
                "--filter",
                "status=running",
                "--format",
                "{{.ID}}",
            )
        )
        docker_authority = getattr(self.runner, "authority_evidence", None)
        if callable(docker_authority):
            docker_authority = docker_authority()
        if not isinstance(docker_authority, Mapping):
            if self.controlled_test:
                docker_authority = {
                    "authority": "controlled_test_injected_runner",
                    "formal_readiness_eligible": False,
                }
            else:
                raise DockerAttestationError(
                    "formal Docker runner has no verified local authority"
                )
        container_ids = tuple(
            value.strip() for value in selected.splitlines() if value.strip()
        )
        if len(container_ids) != 1 or not _CONTAINER_ID.fullmatch(container_ids[0]):
            raise DockerAttestationError(
                "expected exactly one full running code-server container ID"
            )
        selected_id = container_ids[0]
        container = _parse_single_record(
            self.runner.run(("inspect", "--type", "container", selected_id)),
            kind="container",
        )
        container_id = str(container.get("Id") or "")
        image_id = str(container.get("Image") or "")
        config = container.get("Config")
        state = container.get("State")
        host_config = container.get("HostConfig")
        raw_mounts = container.get("Mounts")
        if (
            not isinstance(config, Mapping)
            or not isinstance(state, Mapping)
            or not isinstance(host_config, Mapping)
            or not isinstance(raw_mounts, list)
        ):
            raise DockerAttestationError(
                "container inspection lacks Config, State, HostConfig, or Mount evidence"
            )
        raw_labels = config.get("Labels")
        health = state.get("Health")
        if not isinstance(raw_labels, Mapping) or not isinstance(health, Mapping):
            raise DockerAttestationError(
                "container inspection lacks Compose labels or health evidence"
            )
        labels = {
            key: _safe_label_value(raw_labels[key])
            for key in _SERVICE_LABEL_KEYS
            if key in raw_labels
        }
        if not (
            container_id == selected_id
            and _CONTAINER_ID.fullmatch(container_id)
            and _SHA256_ID.fullmatch(image_id)
            and labels.get("com.docker.compose.project") == COMPOSE_PROJECT
            and labels.get("com.docker.compose.service") == COMPOSE_SERVICE
            and labels.get("com.docker.compose.oneoff", "False").lower()
            in {"false", "0"}
            and state.get("Running") is True
            and state.get("Status") == "running"
            and health.get("Status") == "healthy"
        ):
            raise DockerAttestationError(
                "selected code-server container is not the unique healthy Compose service"
            )
        try:
            failing_streak = int(health.get("FailingStreak") or 0)
        except (TypeError, ValueError):
            raise DockerAttestationError("container health streak is invalid") from None
        if failing_streak != 0:
            raise DockerAttestationError("healthy code-server has a nonzero failure streak")
        try:
            container_started_at = _parse_docker_timestamp(
                state.get("StartedAt"),
                name="container_started_at",
            )
        except (TypeError, ValueError, DockerAttestationError):
            raise DockerAttestationError(
                "container start timestamp is invalid"
            ) from None
        if container_started_at > inspected_at:
            raise DockerAttestationError("container start timestamp is in the future")

        entrypoint = tuple(map(str, config.get("Entrypoint") or ()))
        command = tuple(map(str, config.get("Cmd") or ()))
        working_directory = str(config.get("WorkingDir") or "")
        runtime_user = str(config.get("User") or "")
        hostname = str(config.get("Hostname") or "")
        container_environment = _parse_environment(config.get("Env") or [])
        environment_names = set(container_environment)
        dangerous_environment = tuple(
            sorted(environment_names & _DANGEROUS_ENVIRONMENT_NAMES)
        )
        network_contract, network_id = _network_identity(
            container,
            service=COMPOSE_SERVICE,
        )
        mount_contract: set[tuple[str, str, bool]] = set()
        mount_identities: list[dict[str, Any]] = []
        for item in raw_mounts:
            if not isinstance(item, Mapping):
                raise DockerAttestationError("container mount evidence is malformed")
            kind = str(item.get("Type") or "")
            destination = str(item.get("Destination") or "")
            read_write = bool(item.get("RW"))
            mount_contract.add((kind, destination, read_write))
            identity: dict[str, Any] = {
                "type": kind,
                "destination": destination,
                "read_write": read_write,
            }
            if kind == "bind":
                identity["source"] = _canonical_bind_source(
                    item.get("Source"),
                    require_physical=not self.controlled_test,
                )
            elif kind == "volume":
                volume_name = str(item.get("Name") or "").strip()
                if not _VOLUME_NAME.fullmatch(volume_name):
                    raise DockerAttestationError(
                        "Docker volume source identity is invalid"
                    )
                identity["name"] = volume_name
            else:
                raise DockerAttestationError("unsupported Docker mount type")
            mount_identities.append(identity)
        cap_add = tuple(sorted(map(str, host_config.get("CapAdd") or ())))
        cap_drop = tuple(sorted(map(str, host_config.get("CapDrop") or ())))
        security_options = tuple(
            sorted(map(str, host_config.get("SecurityOpt") or ()))
        )
        raw_tmpfs = host_config.get("Tmpfs") or {}
        if not isinstance(raw_tmpfs, Mapping):
            raise DockerAttestationError("container tmpfs contract is malformed")
        tmpfs_options = {
            str(path): frozenset(
                part.strip() for part in str(options).split(",") if part.strip()
            )
            for path, options in raw_tmpfs.items()
        }
        tmp_options = tmpfs_options.get("/tmp", frozenset())
        if not (
            entrypoint == _EXPECTED_ENTRYPOINT
            and command == _EXPECTED_COMMAND
            and working_directory == IMAGE_BUNDLE_ROOT
            and runtime_user == "dagster"
            and hostname == container_id[:12]
            and mount_contract == _EXPECTED_MOUNTS
            and not dangerous_environment
            and host_config.get("Privileged") is False
            and host_config.get("ReadonlyRootfs") is True
            and not cap_add
            and set(cap_drop) == {"ALL"}
            and "no-new-privileges:true" in security_options
            and set(tmpfs_options) == {"/tmp"}
            and {"rw", "nosuid", "noexec"}.issubset(tmp_options)
        ):
            raise DockerAttestationError(
                "running code-server runtime contract differs from the approved service"
            )
        image = _parse_single_record(
            self.runner.run(("image", "inspect", image_id)),
            kind="image",
        )
        daemon_image_id = str(image.get("Id") or "")
        image_config = image.get("Config")
        if not isinstance(image_config, Mapping):
            raise DockerAttestationError("image inspection lacks Config evidence")
        image_labels = image_config.get("Labels")
        if not isinstance(image_labels, Mapping):
            raise DockerAttestationError("image inspection lacks OCI labels")
        base_name = str(
            image_labels.get("org.opencontainers.image.base.name") or ""
        ).strip()
        base_digest = str(
            image_labels.get("org.opencontainers.image.base.digest") or ""
        ).strip()
        repo_values = image.get("RepoDigests") or ()
        image_reference = str(config.get("Image") or "").strip()
        if not isinstance(repo_values, list):
            raise DockerAttestationError("image RepoDigests evidence is invalid")
        repo_digests = tuple(sorted({str(item) for item in repo_values if item}))
        if not (
            daemon_image_id == image_id
            and _SHA256_ID.fullmatch(daemon_image_id)
            and base_name
            and _SHA256_ID.fullmatch(base_digest)
            and _IMAGE_REFERENCE.fullmatch(image_reference)
            and all(_REPO_DIGEST.fullmatch(item) for item in repo_digests)
        ):
            raise DockerAttestationError(
                "container image ID or pinned base-image evidence is invalid"
            )
        image_environment = _parse_environment(image_config.get("Env") or [])
        deployment_environment = _deployment_environment(
            container_environment=container_environment,
            image_environment=image_environment,
        )
        business_environment = _business_environment(deployment_environment)
        unapproved_environment = sorted(
            set(deployment_environment) - set(business_environment)
        )
        # Compose may only override the protected Research OS business
        # variables. Image-provided PATH/locale/runtime defaults remain outside
        # this contract, but deploy-time shell and routing overrides do not.
        if unapproved_environment:
            raise DockerAttestationError(
                "code-server has unapproved deploy-time environment overrides"
            )

        storage_contract = self._host_storage_contract()
        backend_contracts: dict[str, Any] = {}
        volume_names = {
            str(item["name"])
            for item in mount_identities
            if item["type"] == "volume"
        }
        for backend_service in _BACKEND_SERVICES:
            backend, backend_volume = self._inspect_backend_service(
                service=backend_service,
                shared_network_id=network_id,
            )
            backend_contracts[backend_service] = backend
            volume_names.add(backend_volume)
        network_contract, allowed_container_ids = self._inspect_shared_network(
            name=str(network_contract["name"]),
            network_id=network_id,
            runtime_container_id=container_id,
            runtime_image_id=daemon_image_id,
            runtime_image_reference=image_reference,
        )
        protected_bind_sources = {
            str(item["source"])
            for item in mount_identities
            if item["type"] == "bind"
        }
        for backend in backend_contracts.values():
            protected_bind_sources.update(
                str(item["source"])
                for item in backend["mounts"]
                if item["type"] == "bind"
            )
        for member in network_contract["running_members"].values():
            application = member.get("application_contract")
            if isinstance(application, Mapping):
                protected_bind_sources.update(
                    str(item["source"])
                    for item in application["mounts"]
                    if item["type"] == "bind"
                )
        consumer_audit = self._audit_protected_resource_consumers(
            allowed_container_ids=allowed_container_ids,
            protected_volume_names=frozenset(volume_names),
            protected_bind_sources=frozenset(protected_bind_sources),
        )
        volumes = {
            name: self._inspect_volume(
                name=name,
                engine_root=str(storage_contract["engine_root"]),
            )
            for name in sorted(volume_names)
        }
        runtime_contract = {
            "entrypoint": list(entrypoint),
            "command": list(command),
            "working_directory": working_directory,
            "user": runtime_user,
            "hostname_policy": "docker_default_container_id_prefix",
            "mounts": sorted(
                mount_identities,
                key=lambda item: str(item["destination"]),
            ),
            "read_only_root_filesystem": True,
            "cap_drop": list(cap_drop),
            "security_options": list(security_options),
            "tmpfs_paths": sorted(tmpfs_options),
            "dangerous_environment_names": list(dangerous_environment),
            "business_environment": business_environment,
            "network": network_contract,
            "backend_services": backend_contracts,
            "volumes": volumes,
            "host_storage": storage_contract,
            "protected_resource_consumer_audit": consumer_audit,
        }
        runtime_contract_hash = content_fingerprint(
            runtime_contract,
            domain=_RUNTIME_CONTRACT_DOMAIN,
        )
        return DockerRuntimeEvidence(
            container_id=container_id,
            image_id=daemon_image_id,
            image_reference=image_reference,
            repo_digests=repo_digests,
            base_image_name=base_name,
            base_image_digest=base_digest,
            service_labels=labels,
            state_status="running",
            health_status="healthy",
            health_failing_streak=failing_streak,
            docker_authority=dict(docker_authority),
            runtime_contract=runtime_contract,
            runtime_contract_hash=runtime_contract_hash,
            container_started_at=container_started_at,
            inspected_at=inspected_at,
        )

    def _copy(self, temporary_container_id: str, remote: str, local: Path) -> None:
        local.parent.mkdir(parents=True, exist_ok=True)
        self.runner.run(
            ("cp", f"{temporary_container_id}:{IMAGE_BUNDLE_ROOT}/{remote}", str(local))
        )

    @staticmethod
    def _dockerfile_base(dockerfile: Path) -> tuple[str, str]:
        try:
            text = dockerfile.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            raise DockerAttestationError(
                "copied deployment Dockerfile is unreadable"
            ) from None
        base = _FROM.search(text)
        label = _BASE_LABEL.search(text)
        if base is None or label is None or base.group("digest") != label.group("digest"):
            raise DockerAttestationError(
                "deployment Dockerfile does not pin one matching base digest"
            )
        return base.group("name"), base.group("digest")

    @staticmethod
    def _verify_mount_authority(
        root: Path,
        runtime: DockerRuntimeEvidence,
    ) -> None:
        path = root / _MOUNT_AUTHORITY_FILE
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise DockerAttestationError(
                "protected runtime mount authority is unreadable"
            ) from None
        profiles = payload.get("profiles") if isinstance(payload, Mapping) else None
        profile_name = "windows" if os.name == "nt" else "linux"
        selected = profiles.get(profile_name) if isinstance(profiles, Mapping) else None
        if not (
            isinstance(payload, Mapping)
            and payload.get("schema_version") == _MOUNT_AUTHORITY_SCHEMA
            and set(profiles or {}) == {"windows", "linux"}
            and isinstance(selected, Mapping)
        ):
            raise DockerAttestationError(
                "protected runtime mount authority contract is invalid"
            )
        expected: list[dict[str, Any]] = []
        for destination, raw in selected.items():
            if not isinstance(raw, Mapping):
                raise DockerAttestationError(
                    "protected runtime mount identity is malformed"
                )
            kind = str(raw.get("type") or "")
            identity: dict[str, Any] = {
                "type": kind,
                "destination": str(destination),
                "read_write": bool(raw.get("read_write")),
            }
            if kind == "bind":
                identity["source"] = _canonical_bind_source(
                    raw.get("source"), require_physical=False
                )
            elif kind == "volume":
                name = str(raw.get("name") or "")
                if not _VOLUME_NAME.fullmatch(name):
                    raise DockerAttestationError(
                        "protected Docker volume identity is invalid"
                    )
                identity["name"] = name
            else:
                raise DockerAttestationError(
                    "protected runtime mount type is unsupported"
                )
            expected.append(identity)
        observed = runtime.runtime_contract.get("mounts")
        if (
            not isinstance(observed, list)
            or sorted(expected, key=lambda item: str(item["destination"]))
            != observed
        ):
            raise DockerAttestationError(
                "running code-server mount sources differ from protected H-drive authority"
            )

    @staticmethod
    def _verify_runtime_authority(
        root: Path,
        runtime: DockerRuntimeEvidence,
    ) -> None:
        path = root / _RUNTIME_AUTHORITY_FILE
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise DockerAttestationError(
                "protected runtime service authority is unreadable"
            ) from None
        required_keys = {
            "application_services",
            "protected_resources",
            "schema_version",
            "runtime_environment",
            "backend_services",
            "network",
            "volumes",
            "storage_profiles",
        }
        if not (
            isinstance(payload, Mapping)
            and set(payload) == required_keys
            and payload.get("schema_version") == _RUNTIME_AUTHORITY_SCHEMA
        ):
            raise DockerAttestationError(
                "protected runtime service authority contract is invalid"
            )
        runtime_environment = payload.get("runtime_environment")
        application_authority = payload.get("application_services")
        protected_resources = payload.get("protected_resources")
        backend_authority = payload.get("backend_services")
        network_authority = payload.get("network")
        volume_authority = payload.get("volumes")
        storage_profiles = payload.get("storage_profiles")
        profile_name = "windows" if os.name == "nt" else "linux"
        storage_authority = (
            storage_profiles.get(profile_name)
            if isinstance(storage_profiles, Mapping)
            else None
        )
        if not (
            isinstance(runtime_environment, Mapping)
            and isinstance(application_authority, Mapping)
            and set(application_authority)
            == {"dagster-daemon", "dagster-webserver", "research-os-webui"}
            and isinstance(protected_resources, Mapping)
            and isinstance(backend_authority, Mapping)
            and set(backend_authority) == set(_BACKEND_SERVICES)
            and isinstance(network_authority, Mapping)
            and isinstance(volume_authority, Mapping)
            and isinstance(storage_profiles, Mapping)
            and set(storage_profiles) == {"windows", "linux"}
            and isinstance(storage_authority, Mapping)
        ):
            raise DockerAttestationError(
                "protected runtime service authority contract is malformed"
            )

        expected_runtime_environment: dict[str, str] = {}
        for raw_name, raw_value in runtime_environment.items():
            name = str(raw_name).strip().upper()
            if not isinstance(raw_value, str):
                raise DockerAttestationError(
                    "protected code-server environment contract is invalid"
                )
            parsed = _parse_environment([f"{name}={raw_value}"])
            if name in expected_runtime_environment:
                raise DockerAttestationError(
                    "protected code-server environment contract is invalid"
                )
            expected_runtime_environment.update(parsed)
        if any(
            re.search(r"://[^/@:\s]+:[^/@\s]+@", value)
            for value in expected_runtime_environment.values()
        ):
            raise DockerAttestationError(
                "protected runtime environment embeds a credential"
            )

        normalized_resources: dict[str, dict[str, Any]] = {}
        for resource_name, raw_resource in protected_resources.items():
            if not isinstance(raw_resource, Mapping):
                raise DockerAttestationError(
                    "protected resource authority is malformed"
                )
            kind = str(raw_resource.get("type") or "")
            if kind == "volume":
                name = str(raw_resource.get("name") or "")
                if not _VOLUME_NAME.fullmatch(name):
                    raise DockerAttestationError(
                        "protected volume resource identity is invalid"
                    )
                normalized_resources[str(resource_name)] = {
                    "type": "volume",
                    "name": name,
                }
            elif kind == "bind":
                sources = raw_resource.get("sources")
                if not isinstance(sources, Mapping) or set(sources) != {
                    "windows",
                    "linux",
                }:
                    raise DockerAttestationError(
                        "protected bind resource identity is invalid"
                    )
                normalized_resources[str(resource_name)] = {
                    "type": "bind",
                    "source": _canonical_bind_source(
                        sources.get(profile_name), require_physical=False
                    ),
                }
            else:
                raise DockerAttestationError(
                    "protected resource type is unsupported"
                )

        expected_applications: dict[str, dict[str, Any]] = {}
        for service, raw_application in application_authority.items():
            if not isinstance(raw_application, Mapping):
                raise DockerAttestationError(
                    "protected application service authority is malformed"
                )
            if raw_application.get("environment_contract") == "runtime_environment":
                if "environment" in raw_application:
                    raise DockerAttestationError(
                        "protected application environment contract is ambiguous"
                    )
                application_environment = dict(expected_runtime_environment)
            else:
                raw_environment = raw_application.get("environment")
                if not isinstance(raw_environment, Mapping):
                    raise DockerAttestationError(
                        "protected application environment contract is invalid"
                    )
                application_environment: dict[str, str] = {}
                for raw_name, raw_value in raw_environment.items():
                    if not isinstance(raw_value, str):
                        raise DockerAttestationError(
                            "protected application environment value is invalid"
                        )
                    application_environment.update(
                        _parse_environment(
                            [f"{str(raw_name).upper()}={raw_value}"]
                        )
                    )
            raw_mounts = raw_application.get("mounts")
            if not isinstance(raw_mounts, Mapping):
                raise DockerAttestationError(
                    "protected application mount contract is invalid"
                )
            application_mounts: list[dict[str, Any]] = []
            for destination, raw_mount in raw_mounts.items():
                if not isinstance(raw_mount, Mapping):
                    raise DockerAttestationError(
                        "protected application mount identity is malformed"
                    )
                resource = normalized_resources.get(str(raw_mount.get("resource") or ""))
                if resource is None:
                    raise DockerAttestationError(
                        "protected application mount references an unknown resource"
                    )
                identity = {
                    "type": resource["type"],
                    "destination": str(destination),
                    "read_write": bool(raw_mount.get("read_write")),
                }
                suffix = str(raw_mount.get("suffix") or "").strip("/\\")
                if resource["type"] == "bind":
                    source = str(resource["source"])
                    identity["source"] = (
                        f"{source}/{suffix}" if suffix else source
                    )
                elif suffix:
                    raise DockerAttestationError(
                        "protected volume resource cannot have a suffix"
                    )
                else:
                    identity["name"] = resource["name"]
                application_mounts.append(identity)
            raw_security = raw_application.get("security")
            raw_ports = raw_application.get("published_ports")
            if not isinstance(raw_security, Mapping) or not isinstance(
                raw_ports, list
            ):
                raise DockerAttestationError(
                    "protected application security contract is invalid"
                )
            expected_applications[str(service)] = {
                "entrypoint": list(
                    map(str, raw_application.get("entrypoint") or ())
                ),
                "command": list(map(str, raw_application.get("command") or ())),
                "working_directory": str(
                    raw_application.get("working_directory") or ""
                ),
                "user": str(raw_application.get("user") or ""),
                "environment": dict(sorted(application_environment.items())),
                "mounts": sorted(
                    application_mounts,
                    key=lambda item: str(item["destination"]),
                ),
                "healthcheck": (
                    None
                    if raw_application.get("healthcheck") is None
                    else dict(raw_application["healthcheck"])
                ),
                "published_ports": sorted(
                    [dict(value) for value in raw_ports],
                    key=lambda item: str(item["container"]),
                ),
                "security": {
                    "privileged": bool(raw_security.get("privileged")),
                    "read_only_root_filesystem": bool(
                        raw_security.get("read_only_root_filesystem")
                    ),
                    "cap_add": sorted(map(str, raw_security.get("cap_add") or ())),
                    "cap_drop": sorted(
                        map(str, raw_security.get("cap_drop") or ())
                    ),
                    "security_options": sorted(
                        map(str, raw_security.get("security_options") or ())
                    ),
                    "tmpfs": {
                        str(destination): sorted(map(str, options))
                        for destination, options in sorted(
                            (raw_security.get("tmpfs") or {}).items()
                        )
                    },
                },
            }

        observed = runtime.runtime_contract
        observed_environment = observed.get("business_environment")
        expected_environment = dict(sorted(expected_runtime_environment.items()))
        if observed_environment != expected_environment:
            observed_names = (
                set(observed_environment)
                if isinstance(observed_environment, Mapping)
                else set()
            )
            differing_names = sorted(
                name
                for name in observed_names | set(expected_environment)
                if not isinstance(observed_environment, Mapping)
                or observed_environment.get(name) != expected_environment.get(name)
            )
            raise DockerAttestationError(
                "code-server business environment differs from protected authority: "
                + ",".join(differing_names)
            )

        network_name = str(network_authority.get("name") or "")
        code_aliases = network_authority.get("code_server_aliases")
        running_members = network_authority.get("running_members")
        if not isinstance(running_members, Mapping):
            raise DockerAttestationError(
                "protected network membership contract is invalid"
            )
        expected_members: dict[str, Any] = {}
        member_prefix = f"{COMPOSE_PROJECT}-"
        for member_name, raw_aliases in running_members.items():
            rendered_name = str(member_name)
            if not (
                rendered_name.startswith(member_prefix)
                and rendered_name.endswith("-1")
                and isinstance(raw_aliases, list)
                and len(raw_aliases) == len(set(map(str, raw_aliases)))
            ):
                raise DockerAttestationError(
                    "protected network member authority is malformed"
                )
            member_service = rendered_name[len(member_prefix) : -2]
            expected_members[rendered_name] = {
                "compose_service": member_service,
                "aliases": sorted(map(str, raw_aliases)),
                "verified_runtime_image": member_service
                in {
                    COMPOSE_SERVICE,
                    "dagster-daemon",
                    "dagster-webserver",
                    "research-os-webui",
                },
                "application_contract": expected_applications.get(member_service),
            }
        expected_network = {
            "name": network_name,
            "aliases": sorted(map(str, code_aliases or ())),
            "single_network": True,
            "driver": str(network_authority.get("driver") or ""),
            "internal": bool(network_authority.get("internal")),
            "attachable": bool(network_authority.get("attachable")),
            "compose_network": str(
                network_authority.get("compose_network") or ""
            ),
            "running_members": dict(sorted(expected_members.items())),
        }
        actual_network = observed.get("network")
        consumer_audit = observed.get("protected_resource_consumer_audit")
        if (
            not _VOLUME_NAME.fullmatch(network_name)
            or not isinstance(code_aliases, list)
            or len(code_aliases) != len(set(code_aliases))
            or not isinstance(actual_network, Mapping)
            or {
                key: actual_network.get(key)
                for key in expected_network
            }
            != expected_network
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(actual_network.get("compose_config_hash") or ""),
            )
            or not isinstance(consumer_audit, Mapping)
            or consumer_audit.get("protected_resource_exclusivity_verified")
            is not True
            or consumer_audit.get("allowed_protected_consumers")
            != len(expected_members)
            or not isinstance(
                consumer_audit.get("global_running_containers_inspected"), int
            )
            or consumer_audit.get("global_running_containers_inspected", 0)
            < len(expected_members)
        ):
            raise DockerAttestationError(
                "code-server network differs from protected authority"
            )

        observed_backends = observed.get("backend_services")
        if not isinstance(observed_backends, Mapping) or set(
            observed_backends
        ) != set(_BACKEND_SERVICES):
            raise DockerAttestationError(
                "backend service evidence is incomplete"
            )
        expected_backend_volumes: set[str] = set()
        for service in _BACKEND_SERVICES:
            expected = backend_authority.get(service)
            actual = observed_backends.get(service)
            if not isinstance(expected, Mapping) or not isinstance(actual, Mapping):
                raise DockerAttestationError(
                    "protected backend service authority is malformed"
                )
            expected_environment = expected.get("environment")
            aliases = expected.get("network_aliases")
            expected_mounts = expected.get("mounts")
            if not isinstance(expected_environment, Mapping) or not isinstance(
                aliases, list
            ) or not isinstance(expected_mounts, Mapping):
                raise DockerAttestationError(
                    "protected backend service contract is invalid"
                )
            normalized_environment: dict[str, str] = {}
            for raw_name, raw_value in expected_environment.items():
                if not isinstance(raw_value, str):
                    raise DockerAttestationError(
                        "protected backend environment contract is invalid"
                    )
                normalized_environment.update(
                    _parse_environment([f"{str(raw_name).upper()}={raw_value}"])
                )
            image_reference = str(expected.get("image_reference") or "")
            volume_name = str(expected.get("volume_name") or "")
            volume_destination = str(expected.get("volume_destination") or "")
            expected_backend_volumes.add(volume_name)
            normalized_mounts: list[dict[str, Any]] = []
            for destination, raw_mount in expected_mounts.items():
                if not isinstance(raw_mount, Mapping):
                    raise DockerAttestationError(
                        "protected backend mount contract is malformed"
                    )
                kind = str(raw_mount.get("type") or "")
                mount_identity: dict[str, Any] = {
                    "type": kind,
                    "destination": str(destination),
                    "read_write": bool(raw_mount.get("read_write")),
                }
                if kind == "bind":
                    sources = raw_mount.get("sources")
                    if not isinstance(sources, Mapping) or set(sources) != {
                        "windows",
                        "linux",
                    }:
                        raise DockerAttestationError(
                            "protected backend secret mount is invalid"
                        )
                    mount_identity["source"] = _canonical_bind_source(
                        sources.get(profile_name), require_physical=False
                    )
                elif kind == "volume":
                    mount_identity["name"] = str(raw_mount.get("name") or "")
                else:
                    raise DockerAttestationError(
                        "protected backend mount type is unsupported"
                    )
                normalized_mounts.append(mount_identity)
            expected_healthcheck = expected.get("healthcheck")
            expected_ports = expected.get("published_ports")
            expected_security = expected.get("security")
            if not (
                actual.get("entrypoint")
                == list(map(str, expected.get("entrypoint") or ()))
                and actual.get("command")
                == list(map(str, expected.get("command") or ()))
                and actual.get("user") == str(expected.get("user") or "")
                and actual.get("image_reference") == image_reference
                and actual.get("pinned_repo_digest")
                == _pinned_repo_digest(image_reference)
                and _SHA256_ID.fullmatch(str(actual.get("image_id") or ""))
                and actual.get("environment")
                == dict(sorted(normalized_environment.items()))
                and actual.get("network")
                == {
                    "name": network_name,
                    "aliases": sorted(map(str, aliases)),
                    "single_network": True,
                }
                and actual.get("mounts")
                == sorted(
                    normalized_mounts, key=lambda item: str(item["destination"])
                )
                and actual.get("volume_destination") == volume_destination
                and actual.get("volume_name") == volume_name
                and isinstance(expected_healthcheck, Mapping)
                and actual.get("healthcheck") == dict(expected_healthcheck)
                and isinstance(expected_ports, list)
                and actual.get("published_ports")
                == sorted(
                    [dict(value) for value in expected_ports],
                    key=lambda item: str(item["container"]),
                )
                and isinstance(expected_security, Mapping)
                and actual.get("security") == dict(expected_security)
                and actual.get("healthy") is True
                and actual.get("health_failing_streak") == 0
            ):
                raise DockerAttestationError(
                    f"{service} runtime differs from protected backend authority"
                )

        raw_disks = storage_authority.get("required_disk_images")
        if not isinstance(raw_disks, list):
            raise DockerAttestationError(
                "protected Docker storage authority is invalid"
            )
        expected_storage = {
            "profile": profile_name,
            "engine_root": str(storage_authority.get("engine_root") or "").rstrip(
                "/"
            ),
            "host_data_root": _canonical_bind_source(
                storage_authority.get("host_data_root"), require_physical=False
            ),
            "host_root_authority": str(
                storage_authority.get("host_root_authority") or ""
            ),
            "required_disk_images": [
                {
                    "relative_path": str(value).replace("\\", "/"),
                    "physical_regular_file": True,
                }
                for value in sorted(map(str, raw_disks))
            ],
        }
        if observed.get("host_storage") != expected_storage:
            raise DockerAttestationError(
                "Docker engine storage root differs from protected H-drive authority"
            )

        observed_volumes = observed.get("volumes")
        if not isinstance(observed_volumes, Mapping) or set(
            observed_volumes
        ) != set(volume_authority):
            raise DockerAttestationError(
                "named-volume evidence differs from protected authority"
            )
        expected_mount_volume = next(
            (
                str(item.get("name") or "")
                for item in observed.get("mounts", ())
                if isinstance(item, Mapping)
                and item.get("destination") == "/opt/dagster/home/storage"
            ),
            "",
        )
        if set(volume_authority) != expected_backend_volumes | {
            expected_mount_volume
        }:
            raise DockerAttestationError(
                "protected volume authority does not cover every stateful service"
            )
        engine_root = str(expected_storage["engine_root"])
        for volume_name, expected in volume_authority.items():
            actual = observed_volumes.get(volume_name)
            if not isinstance(expected, Mapping) or not isinstance(actual, Mapping):
                raise DockerAttestationError(
                    "protected named-volume authority is malformed"
                )
            if not (
                actual.get("name") == volume_name
                and actual.get("driver") == "local"
                and actual.get("scope") == "local"
                and actual.get("mountpoint")
                == f"{engine_root}/volumes/{volume_name}/_data"
                and actual.get("compose_project") == COMPOSE_PROJECT
                and actual.get("compose_volume")
                == str(expected.get("compose_volume") or "")
                and re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(actual.get("compose_config_hash") or ""),
                )
            ):
                raise DockerAttestationError(
                    "named-volume labels or mountpoint differ from protected authority"
                )

    def _verify_container_bundle(
        self,
        *,
        container_id: str,
        runtime: DockerRuntimeEvidence,
    ) -> dict[str, Any]:
        """Copy and verify protected code/config bytes from one container."""

        with tempfile.TemporaryDirectory(
            prefix="factor-lab-deployment-attestation-"
        ) as temporary:
            root = Path(temporary)
            self._copy(
                container_id,
                ".factor-lab-source-bundle.json",
                root / ".factor-lab-source-bundle.json",
            )
            self._copy(container_id, "src", root / "src")
            self._copy(container_id, "configs", root / "configs")
            self._copy(
                container_id,
                "infra/research_os",
                root / "infra" / "research_os",
            )
            self._copy(container_id, "uv.lock", root / "uv.lock")
            try:
                provenance = verify_source_bundle_manifest(
                    root / ".factor-lab-source-bundle.json",
                    bundle_root=root,
                )
            except SourceBundleProvenanceError:
                raise DockerAttestationError(
                    "copied container source bundle failed immutable verification"
                ) from None
            expected_paths = (
                provenance.source_root == (root / "src").resolve()
                and provenance.configuration_root == (root / "configs").resolve()
                and provenance.runtime_root
                == (root / "infra" / "research_os").resolve()
                and provenance.dependency_lock == (root / "uv.lock").resolve()
            )
            if not expected_paths or provenance.runtime_tree_hash is None:
                raise DockerAttestationError(
                    "container source bundle does not cover required deployment paths"
                )
            self._verify_mount_authority(root, runtime)
            self._verify_runtime_authority(root, runtime)
            dockerfile_name, dockerfile_digest = self._dockerfile_base(
                root / "infra" / "research_os" / "Dockerfile.dagster"
            )
            if (
                _canonical_image_name(dockerfile_name)
                != _canonical_image_name(runtime.base_image_name)
                or dockerfile_digest != runtime.base_image_digest
            ):
                raise DockerAttestationError(
                    "container OCI base labels differ from the verified Dockerfile pin"
                )
            return {
                "source_bundle_manifest_hash": provenance.manifest_hash,
                "source_tree_hash": provenance.source_tree_hash,
                "configuration_tree_hash": provenance.configuration_tree_hash,
                "runtime_tree_hash": provenance.runtime_tree_hash,
                "dependency_lock_hash": provenance.dependency_lock_hash,
                "source_file_count": provenance.source_file_count,
                "configuration_file_count": provenance.configuration_file_count,
                "runtime_file_count": provenance.runtime_file_count,
                "base_image_name": runtime.base_image_name,
                "base_image_digest": dockerfile_digest,
            }

    def _verify_deployment(
        self, runtime: DockerRuntimeEvidence
    ) -> DockerDeploymentEvidence:
        temporary_container_id: str | None = None
        cleanup_error: DockerAttestationError | None = None
        verified: dict[str, Any] | None = None
        try:
            created = self.runner.run(
                ("create", "--entrypoint", "/bin/true", runtime.image_id)
            )
            candidate = created.strip()
            if not _CONTAINER_ID.fullmatch(candidate):
                raise DockerAttestationError(
                    "Docker create returned a non-canonical temporary container ID"
                )
            temporary_container_id = candidate
            image_verified = self._verify_container_bundle(
                container_id=candidate,
                runtime=runtime,
            )
            running_verified = self._verify_container_bundle(
                container_id=runtime.container_id,
                runtime=runtime,
            )
            if running_verified != image_verified:
                raise DockerAttestationError(
                    "running container bundle differs from its verified image"
                )
            verified = image_verified
        finally:
            if temporary_container_id is not None:
                try:
                    self.runner.run(("rm", "-f", temporary_container_id))
                except DockerAttestationError as exc:
                    cleanup_error = exc
        if cleanup_error is not None:
            raise DockerAttestationError(
                "temporary deployment-attestation container cleanup failed"
            ) from None
        if verified is None:
            raise DockerAttestationError(
                "deployment image verification did not produce evidence"
            )
        verified_at = _aware(self._clock(), name="deployment_verified_at")
        deployment_hash = content_fingerprint(
            {
                **verified,
                "runtime_image_id": runtime.image_id,
                "temporary_container_removed": True,
                "running_container_bundle_verified": True,
                "verified_at": verified_at,
            },
            domain=f"{SCHEMA_VERSION}/deployment",
        )
        return DockerDeploymentEvidence(
            **verified,
            deployment_evidence_hash=deployment_hash,
            temporary_container_removed=True,
            running_container_bundle_verified=True,
            verified_at=verified_at,
        )

    def _attest_once(self) -> HostDockerAttestationResult:
        self._assert_admission()
        runtime = self._inspect_runtime()
        deployment = self._verify_deployment(runtime)
        if deployment.verified_at < runtime.inspected_at:
            raise DockerAttestationError(
                "host clock moved backwards during Docker attestation"
            )
        physical = not self.controlled_test
        run_type = RUN_TYPE if physical else CONTROLLED_TEST_RUN_TYPE
        readiness_admission = (
            READINESS_ADMISSION if physical else CONTROLLED_TEST_REJECTION
        )
        metadata = {
            "schema_version": SCHEMA_VERSION,
            "authority": "host_docker_daemon_plus_verified_image_bundle",
            "physical": physical,
            "controlled_test_runner": self.controlled_test,
            "readiness_admission": readiness_admission,
            "compose_project": COMPOSE_PROJECT,
            "compose_service": COMPOSE_SERVICE,
            "container_id": runtime.container_id,
            "oci_image_id": runtime.image_id,
            "image_reference": runtime.image_reference,
            "oci_repo_digests": list(runtime.repo_digests),
            "oci_base_name": runtime.base_image_name,
            "oci_base_digest": runtime.base_image_digest,
            "service_labels": dict(runtime.service_labels),
            "container_state": runtime.state_status,
            "container_health": runtime.health_status,
            "health_failing_streak": runtime.health_failing_streak,
            "docker_authority": dict(runtime.docker_authority),
            "docker_authority_hash": content_fingerprint(
                runtime.docker_authority,
                domain=_DOCKER_AUTHORITY_DOMAIN,
            ),
            "runtime_contract": dict(runtime.runtime_contract),
            "runtime_contract_hash": runtime.runtime_contract_hash,
            "container_started_at": runtime.container_started_at.isoformat(),
            "inspected_at": runtime.inspected_at.isoformat(),
            "daemon_image_id_verified": True,
            "pinned_base_digest_verified": True,
            "source_bundle_schema_version": SOURCE_BUNDLE_SCHEMA_VERSION,
            "source_bundle_manifest_hash": deployment.source_bundle_manifest_hash,
            "source_tree_hash": deployment.source_tree_hash,
            "configuration_tree_hash": deployment.configuration_tree_hash,
            "runtime_tree_hash": deployment.runtime_tree_hash,
            "dependency_lock_hash": deployment.dependency_lock_hash,
            "source_file_count": deployment.source_file_count,
            "configuration_file_count": deployment.configuration_file_count,
            "runtime_file_count": deployment.runtime_file_count,
            "deployment_evidence_hash": deployment.deployment_evidence_hash,
            "temporary_container_removed": deployment.temporary_container_removed,
            "running_container_bundle_verified": (
                deployment.running_container_bundle_verified
            ),
            "deployment_verified_at": deployment.verified_at.isoformat(),
            "deployment_bundle_verified": True,
            "formal_readiness_eligible": physical,
        }
        attestation_hash = content_fingerprint(
            metadata,
            domain=SCHEMA_VERSION,
        )
        metadata["attestation_hash"] = attestation_hash
        run_id = f"docker_attestation_{attestation_hash}"
        if self.catalog is not None:
            proposed = RunRecord(
                run_id=run_id,
                run_type=run_type,
                status="succeeded",
                input_fingerprint=attestation_hash,
                started_at=runtime.inspected_at,
                completed_at=deployment.verified_at,
                metadata=metadata,
            )
            stored, won = self.catalog.claim_run(proposed)
            if not won and stored != proposed:
                raise DockerAttestationError(
                    "Docker attestation identity collided with different evidence"
                )
        return HostDockerAttestationResult(
            run_id=run_id,
            run_type=run_type,
            physical=physical,
            readiness_admission=readiness_admission,
            attestation_hash=attestation_hash,
            runtime=runtime,
            deployment=deployment,
        )

    def attest(self) -> HostDockerAttestationResult:
        """Inspect and persist one host-derived runtime/deployment attestation.

        Formal collection first records a monotonic attempt. A later runtime,
        network, backend, or storage drift therefore leaves a durable failed
        attempt and cannot be hidden by an older fresh success. Controlled
        runners remain permanently ineligible and do not mint formal attempts.
        """

        if self.controlled_test:
            return self._attest_once()
        self._assert_admission()
        assert self.catalog is not None
        attempt_started_at = _aware(
            self.catalog.database_now(),
            name="attempt_started_at",
        )
        attempt_nonce = secrets.token_hex(16)
        attempt_fingerprint = host_docker_attempt_fingerprint(
            started_at=attempt_started_at,
            nonce=attempt_nonce,
        )
        attempt_run_id = f"docker_attestation_attempt_{attempt_fingerprint}"
        base_metadata = {
            "schema_version": ATTEMPT_SCHEMA_VERSION,
            "authority": ATTEMPT_AUTHORITY,
            "physical": True,
            "attempt_nonce": attempt_nonce,
        }
        running = RunRecord(
            run_id=attempt_run_id,
            run_type=ATTEMPT_RUN_TYPE,
            status="running",
            input_fingerprint=attempt_fingerprint,
            started_at=attempt_started_at,
            metadata={
                **base_metadata,
                "outcome": "running",
                "formal_readiness_eligible": False,
            },
        )
        stored, won = self.catalog.claim_run(running)
        if not won or stored != running:
            raise DockerAttestationError(
                "host Docker attestation attempt identity collided"
            )
        try:
            result = self._attest_once()
        except BaseException as exc:
            error_type = _safe_attempt_error(exc)
            completed_at = max(
                attempt_started_at,
                _aware(
                    self.catalog.database_now(),
                    name="attempt_completed_at",
                ),
            )
            self.catalog.save_run(
                RunRecord(
                    run_id=attempt_run_id,
                    run_type=ATTEMPT_RUN_TYPE,
                    status="failed",
                    input_fingerprint=attempt_fingerprint,
                    started_at=attempt_started_at,
                    completed_at=completed_at,
                    error=error_type,
                    metadata={
                        **base_metadata,
                        "outcome": "failed",
                        "error_type": error_type,
                        "formal_readiness_eligible": False,
                    },
                )
            )
            raise
        completed_at = max(
            attempt_started_at,
            _aware(
                self.catalog.database_now(),
                name="attempt_completed_at",
            ),
        )
        self.catalog.save_run(
            RunRecord(
                run_id=attempt_run_id,
                run_type=ATTEMPT_RUN_TYPE,
                status="succeeded",
                input_fingerprint=attempt_fingerprint,
                started_at=attempt_started_at,
                completed_at=completed_at,
                metadata={
                    **base_metadata,
                    "outcome": "succeeded",
                    "attestation_run_id": result.run_id,
                    "attestation_hash": result.attestation_hash,
                    "formal_readiness_eligible": True,
                },
            )
        )
        return result


__all__ = [
    "ATTEMPT_AUTHORITY",
    "ATTEMPT_RUN_TYPE",
    "ATTEMPT_SCHEMA_VERSION",
    "COMPOSE_PROJECT",
    "COMPOSE_SERVICE",
    "CONTROLLED_TEST_REJECTION",
    "CONTROLLED_TEST_RUN_TYPE",
    "DockerAttestationAdmissionError",
    "DockerAttestationError",
    "DockerCommandRunner",
    "DockerDeploymentEvidence",
    "DockerRuntimeEvidence",
    "HostDockerAttestationResult",
    "HostDockerRuntimeAttestor",
    "KERNEL_PROCESS_IDENTITY_SCHEME",
    "host_docker_attempt_fingerprint",
    "persisted_attestation_binding_errors",
    "READINESS_ADMISSION",
    "RUN_TYPE",
    "SCHEMA_VERSION",
]
