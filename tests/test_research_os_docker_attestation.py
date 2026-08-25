from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect
import json
import os
from pathlib import Path
import shutil

import pytest

from factor_lab.research_os import docker_attestation as attestation_module
from factor_lab.research_os.build_provenance import write_source_bundle_manifest
from factor_lab.research_os.catalog import ResearchCatalog, RunRecord
from factor_lab.research_os.docker_attestation import (
    ATTEMPT_AUTHORITY,
    ATTEMPT_RUN_TYPE,
    ATTEMPT_SCHEMA_VERSION,
    COMPOSE_PROJECT,
    COMPOSE_SERVICE,
    CONTROLLED_TEST_REJECTION,
    CONTROLLED_TEST_RUN_TYPE,
    RUN_TYPE,
    SCHEMA_VERSION,
    DockerAttestationAdmissionError,
    DockerAttestationError,
    HostDockerRuntimeAttestor,
    host_docker_attempt_fingerprint,
    persisted_attestation_binding_errors,
)
from factor_lab.research_os.fingerprint import content_fingerprint


NOW = datetime(2026, 8, 23, 14, 0, tzinfo=timezone.utc)
CONTAINER_ID = "1" * 64
TEMPORARY_CONTAINER_ID = "2" * 64
IMAGE_ID = "sha256:" + "3" * 64
OTHER_IMAGE_ID = "sha256:" + "4" * 64
BASE_DIGEST = "sha256:" + "5" * 64
OTHER_BASE_DIGEST = "sha256:" + "6" * 64
POSTGRES_CONTAINER_ID = "8" * 64
MINIO_CONTAINER_ID = "9" * 64
WEBSERVER_CONTAINER_ID = "a" * 64
DAEMON_CONTAINER_ID = "b" * 64
WEBUI_CONTAINER_ID = "c" * 64
NETWORK_ID = "d" * 64
POSTGRES_IMAGE_ID = "sha256:" + "e" * 64
MINIO_IMAGE_ID = "sha256:" + "f" * 64
POSTGRES_IMAGE_REFERENCE = (
    "postgres:16.4-alpine@sha256:"
    "5660c2cbfea50c7a9127d17dc4e48543eedd3d7a41a595a2dfa572471e37e64c"
)
MINIO_IMAGE_REFERENCE = (
    "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:"
    "a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
)
BASE_NAME = "docker.io/library/python:3.11-slim-bookworm"
DOCKERFILE_BASE_NAME = "python:3.11-slim-bookworm"


def _write_bundle(root: Path, *, base_digest: str = BASE_DIGEST) -> None:
    (root / "src" / "factor_lab").mkdir(parents=True)
    (root / "src" / "factor_lab" / "__init__.py").write_text(
        "VALUE = 1\n", encoding="utf-8"
    )
    (root / "configs").mkdir()
    (root / "configs" / "research_os_orchestration.production.json").write_text(
        '{"schema_version":"test"}\n', encoding="utf-8"
    )
    shutil.copy2(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_runtime_mounts.production.json",
        root / "configs" / "research_os_runtime_mounts.production.json",
    )
    shutil.copy2(
        Path(__file__).resolve().parents[1]
        / "configs"
        / "research_os_runtime_authority.production.json",
        root / "configs" / "research_os_runtime_authority.production.json",
    )
    runtime = root / "infra" / "research_os"
    runtime.mkdir(parents=True)
    (runtime / "Dockerfile.dagster").write_text(
        "\n".join(
            (
                f"FROM {DOCKERFILE_BASE_NAME}@{base_digest}",
                "",
                f'LABEL org.opencontainers.image.base.name="{BASE_NAME}" ' + "\\",
                f'      org.opencontainers.image.base.digest="{base_digest}"',
                "",
            )
        ),
        encoding="utf-8",
    )
    (runtime / "docker-compose.yml").write_text(
        f"name: {COMPOSE_PROJECT}\n", encoding="utf-8"
    )
    (root / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    write_source_bundle_manifest(
        root / ".factor-lab-source-bundle.json",
        bundle_root=root,
        source_root="src",
        configuration_root="configs",
        dependency_lock="uv.lock",
        runtime_root="infra/research_os",
    )


class _FakeDockerRunner:
    def __init__(self, bundle_root: Path) -> None:
        self.bundle_root = bundle_root
        self.commands: list[tuple[str, ...]] = []
        self.ps_ids = (CONTAINER_ID,)
        self.health_status = "healthy"
        self.failing_streak = 0
        self.container_image_id = IMAGE_ID
        self.daemon_image_id = IMAGE_ID
        self.image_base_digest = BASE_DIGEST
        self.fail_copy: str | None = None
        self.fail_remove = False
        self.tamper_copy: str | None = None
        self.tamper_running_copy: str | None = None
        self.removed: list[str] = []
        self.command_override: list[str] | None = None
        self.environment_append: list[str] = []
        self.extra_mounts: list[dict[str, object]] = []
        self.backend_extra_mounts: dict[str, list[dict[str, object]]] = {
            "postgres": [],
            "minio": [],
        }
        self.backend_command_override: dict[str, list[str] | None] = {
            "postgres": None,
            "minio": None,
        }
        self.backend_privileged: dict[str, bool] = {
            "postgres": False,
            "minio": False,
        }
        self.backend_cap_add: dict[str, list[str]] = {
            "postgres": [],
            "minio": [],
        }
        self.backend_host_ip: dict[str, str] = {
            "postgres": "127.0.0.1",
            "minio": "127.0.0.1",
        }
        self.extra_network_member: tuple[str, str, str] | None = None
        self.off_network_container: dict[str, object] | None = None
        self.duplicate_network_alias: str | None = None
        self.aux_image_override: dict[str, str] = {}
        self.aux_command_override: dict[str, list[str]] = {}
        self.aux_extra_mounts: dict[str, list[dict[str, object]]] = {
            "dagster-daemon": [],
            "dagster-webserver": [],
            "research-os-webui": [],
        }
        self.host_storage_override: dict[str, object] | None = None
        self.volume_mountpoint_override: dict[str, str] = {}
        authority = json.loads(
            (
                bundle_root
                / "configs"
                / "research_os_runtime_authority.production.json"
            ).read_text(encoding="utf-8")
        )
        self.runtime_environment = dict(authority["runtime_environment"])
        self.application_services = dict(authority["application_services"])
        self.protected_resources = dict(authority["protected_resources"])

    @property
    def authority_evidence(self) -> dict[str, object]:
        return {
            "authority": "explicit_local_docker_engine_endpoint",
            "endpoint_policy": (
                "windows_docker_desktop_linux_named_pipe"
                if os.name == "nt"
                else "local_unix_docker_socket"
            ),
            "cli_path": (
                "C:/Program Files/Docker/Docker/resources/bin/docker.exe"
                if os.name == "nt"
                else "/usr/bin/docker"
            ),
            "cli_sha256": "6" * 64,
            "client_version": "29.7.2",
            "server_version": "29.7.2",
            "server_os": "linux",
            "ambient_docker_routing_rejected": True,
        }

    @property
    def host_storage_evidence(self) -> dict[str, object]:
        if self.host_storage_override is not None:
            return dict(self.host_storage_override)
        if os.name == "nt":
            return {
                "profile": "windows",
                "engine_root": "/var/lib/docker",
                "host_data_root": "H:/Program Data/Docker/DockerDesktopWSL",
                "host_root_authority": "docker_desktop_settings_store",
                "required_disk_images": [
                    {
                        "relative_path": "disk/docker_data.vhdx",
                        "physical_regular_file": True,
                    }
                ],
            }
        return {
            "profile": "linux",
            "engine_root": "/var/lib/docker",
            "host_data_root": "/var/lib/docker",
            "host_root_authority": "docker_engine_info",
            "required_disk_images": [],
        }

    def _container(self) -> str:
        runtime_root = (
            "H:/Program Data/factor-lab-runtime"
            if os.name == "nt"
            else "/srv/factor-lab-runtime"
        )
        return json.dumps(
            [
                {
                    "Id": CONTAINER_ID,
                    "Name": "/factor-lab-research-os-dagster-code-server-1",
                    "Image": self.container_image_id,
                    "Config": {
                        "Image": "factor-lab-research-os:local",
                        "Hostname": CONTAINER_ID[:12],
                        "Entrypoint": ["/usr/local/bin/factor-lab-entrypoint"],
                        "Cmd": self.command_override or [
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
                        ],
                        "WorkingDir": "/opt/factor-lab",
                        "User": "dagster",
                        "Env": [
                            f"{name}={value}"
                            for name, value in sorted(self.runtime_environment.items())
                        ] + self.environment_append,
                        "Labels": {
                            "com.docker.compose.project": COMPOSE_PROJECT,
                            "com.docker.compose.service": COMPOSE_SERVICE,
                            "com.docker.compose.oneoff": "False",
                            "com.docker.compose.container-number": "1",
                            "com.docker.compose.config-hash": "a" * 64,
                            "secret.token": "must-not-appear",
                        },
                    },
                    "HostConfig": {
                        "Privileged": False,
                        "ReadonlyRootfs": True,
                        "CapAdd": None,
                        "CapDrop": ["ALL"],
                        "SecurityOpt": ["no-new-privileges:true"],
                        "Tmpfs": {"/tmp": "rw,noexec,nosuid,size=67108864"},
                    },
                    "Mounts": [
                        {"Type": "volume", "Name": "factor-lab-research-os_research-os-dagster", "Source": "/var/lib/docker/volumes/factor-lab-research-os_research-os-dagster/_data", "Destination": "/opt/dagster/home/storage", "RW": True},
                        {"Type": "bind", "Source": f"{runtime_root}/data", "Destination": "/opt/factor-lab/runtime/data", "RW": True},
                        {"Type": "bind", "Source": f"{runtime_root}/artifacts", "Destination": "/opt/factor-lab/runtime/artifacts", "RW": True},
                        {"Type": "bind", "Source": f"{runtime_root}/secrets/settings", "Destination": "/run/secrets", "RW": False},
                        {"Type": "bind", "Source": f"{runtime_root}/secrets/postgres_password", "Destination": "/run/infra-secrets/postgres_password", "RW": False},
                        {"Type": "bind", "Source": f"{runtime_root}/secrets/minio_root_user", "Destination": "/run/infra-secrets/minio_root_user", "RW": False},
                        {"Type": "bind", "Source": f"{runtime_root}/secrets/minio_root_password", "Destination": "/run/infra-secrets/minio_root_password", "RW": False},
                    ] + self.extra_mounts,
                    "NetworkSettings": {
                        "Networks": {
                            "factor-lab-research-os_default": {
                                "Aliases": [
                                    "factor-lab-research-os-dagster-code-server-1",
                                    "dagster-code-server",
                                ],
                                "NetworkID": NETWORK_ID,
                            }
                        }
                    },
                    "State": {
                        "Running": True,
                        "Status": "running",
                        "StartedAt": "2026-08-23T13:00:00.123456789Z",
                        "Health": {
                            "Status": self.health_status,
                            "FailingStreak": self.failing_streak,
                            "Log": [{"Output": "must-not-appear"}],
                        },
                    },
                }
            ]
        )

    def _image(self) -> str:
        return json.dumps(
            [
                {
                    "Id": self.daemon_image_id,
                    "RepoDigests": [
                        "factor-lab-research-os@sha256:" + "7" * 64
                    ],
                    "Config": {
                        "Env": ["PATH=/usr/local/bin:/usr/bin:/bin"],
                        "Labels": {
                            "org.opencontainers.image.base.name": BASE_NAME,
                            "org.opencontainers.image.base.digest": self.image_base_digest,
                            "secret.label": "must-not-appear",
                        },
                    },
                }
            ]
        )

    def _backend_container(self, service: str) -> str:
        runtime_root = (
            "H:/Program Data/factor-lab-runtime"
            if os.name == "nt"
            else "/srv/factor-lab-runtime"
        )
        if service == "postgres":
            container_id = POSTGRES_CONTAINER_ID
            image_id = POSTGRES_IMAGE_ID
            image_reference = POSTGRES_IMAGE_REFERENCE
            entrypoint = ["docker-entrypoint.sh"]
            command = self.backend_command_override[service] or ["postgres"]
            environment = [
                "PATH=/usr/local/bin:/usr/bin:/bin",
                "POSTGRES_DB=factor_lab",
                "POSTGRES_PASSWORD_FILE=/run/secrets/postgres_password",
                "POSTGRES_USER=factor_lab",
            ]
            mounts = [
                {
                    "Type": "volume",
                    "Name": "factor-lab-research-os_research-os-postgres",
                    "Source": "/var/lib/docker/volumes/factor-lab-research-os_research-os-postgres/_data",
                    "Destination": "/var/lib/postgresql/data",
                    "RW": True,
                },
                {
                    "Type": "bind",
                    "Source": f"{runtime_root}/secrets/postgres_password",
                    "Destination": "/run/secrets/postgres_password",
                    "RW": False,
                },
            ]
            healthcheck = {
                "Test": [
                    "CMD-SHELL",
                    "pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}",
                ],
                "Interval": 5_000_000_000,
                "Timeout": 5_000_000_000,
                "Retries": 20,
            }
            port_bindings = {
                "5432/tcp": [
                    {
                        "HostIp": self.backend_host_ip[service],
                        "HostPort": "15432",
                    }
                ]
            }
        else:
            container_id = MINIO_CONTAINER_ID
            image_id = MINIO_IMAGE_ID
            image_reference = MINIO_IMAGE_REFERENCE
            entrypoint = ["/usr/bin/docker-entrypoint.sh"]
            command = self.backend_command_override[service] or [
                "server",
                "/data",
                "--console-address",
                ":9001",
            ]
            environment = [
                "PATH=/usr/local/bin:/usr/bin:/bin",
                "MINIO_ACCESS_KEY_FILE=access_key",
                "MINIO_CONFIG_ENV_FILE=config.env",
                "MINIO_ROOT_PASSWORD_FILE=/run/secrets/minio_root_password",
                "MINIO_ROOT_USER_FILE=/run/secrets/minio_root_user",
                "MINIO_SECRET_KEY_FILE=secret_key",
            ]
            mounts = [
                {
                    "Type": "volume",
                    "Name": "factor-lab-research-os_research-os-minio",
                    "Source": "/var/lib/docker/volumes/factor-lab-research-os_research-os-minio/_data",
                    "Destination": "/data",
                    "RW": True,
                },
                {
                    "Type": "bind",
                    "Source": f"{runtime_root}/secrets/minio_root_password",
                    "Destination": "/run/secrets/minio_root_password",
                    "RW": False,
                },
                {
                    "Type": "bind",
                    "Source": f"{runtime_root}/secrets/minio_root_user",
                    "Destination": "/run/secrets/minio_root_user",
                    "RW": False,
                },
            ]
            healthcheck = {
                "Test": [
                    "CMD",
                    "curl",
                    "--fail",
                    "--silent",
                    "--show-error",
                    "http://127.0.0.1:9000/minio/health/live",
                ],
                "Interval": 5_000_000_000,
                "Timeout": 5_000_000_000,
                "Retries": 20,
            }
            port_bindings = {
                port: [
                    {
                        "HostIp": self.backend_host_ip[service],
                        "HostPort": port.split("/", 1)[0],
                    }
                ]
                for port in ("9000/tcp", "9001/tcp")
            }
        container_name = f"factor-lab-research-os-{service}-1"
        return json.dumps(
            [
                {
                    "Id": container_id,
                    "Name": f"/{container_name}",
                    "Image": image_id,
                    "Config": {
                        "Image": image_reference,
                        "Entrypoint": entrypoint,
                        "Cmd": command,
                        "User": "",
                        "Env": environment,
                        "Healthcheck": healthcheck,
                        "Labels": {
                            "com.docker.compose.project": COMPOSE_PROJECT,
                            "com.docker.compose.service": service,
                            "com.docker.compose.oneoff": "False",
                        },
                    },
                    "HostConfig": {
                        "Privileged": self.backend_privileged[service],
                        "ReadonlyRootfs": False,
                        "CapAdd": self.backend_cap_add[service] or None,
                        "CapDrop": None,
                        "SecurityOpt": None,
                        "PortBindings": port_bindings,
                    },
                    "Mounts": mounts + self.backend_extra_mounts[service],
                    "NetworkSettings": {
                        "Networks": {
                            "factor-lab-research-os_default": {
                                "Aliases": [container_name, service],
                                "NetworkID": NETWORK_ID,
                            }
                        }
                    },
                    "State": {
                        "Running": True,
                        "Status": "running",
                        "Health": {"Status": "healthy", "FailingStreak": 0},
                    },
                }
            ]
        )

    def _backend_image(self, service: str) -> str:
        if service == "postgres":
            image_id = POSTGRES_IMAGE_ID
            repo_digest = (
                "postgres@sha256:"
                "5660c2cbfea50c7a9127d17dc4e48543eedd3d7a41a595a2dfa572471e37e64c"
            )
            environment = ["PATH=/usr/local/bin:/usr/bin:/bin"]
        else:
            image_id = MINIO_IMAGE_ID
            repo_digest = (
                "quay.io/minio/minio@sha256:"
                "a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
            )
            environment = [
                "PATH=/usr/local/bin:/usr/bin:/bin",
                "MINIO_ACCESS_KEY_FILE=access_key",
                "MINIO_CONFIG_ENV_FILE=config.env",
                "MINIO_SECRET_KEY_FILE=secret_key",
            ]
        return json.dumps(
            [
                {
                    "Id": image_id,
                    "RepoDigests": [repo_digest],
                    "Config": {"Env": environment, "Labels": {}},
                }
            ]
        )

    def _aux_container(
        self, *, container_id: str, service: str, container_name: str, aliases: list[str]
    ) -> str:
        if service not in self.application_services:
            return json.dumps(
                [
                    {
                        "Id": container_id,
                        "Name": f"/{container_name}",
                        "Image": IMAGE_ID,
                        "Config": {
                            "Image": "factor-lab-research-os:local",
                            "Env": ["PATH=/usr/local/bin:/usr/bin:/bin"],
                            "Labels": {
                                "com.docker.compose.project": COMPOSE_PROJECT,
                                "com.docker.compose.service": service,
                                "com.docker.compose.oneoff": "False",
                            },
                        },
                        "HostConfig": {"Privileged": False, "CapAdd": None},
                        "Mounts": [],
                        "NetworkSettings": {
                            "Networks": {
                                "factor-lab-research-os_default": {
                                    "Aliases": aliases,
                                    "NetworkID": NETWORK_ID,
                                }
                            }
                        },
                    }
                ]
            )
        authority = self.application_services[service]
        if authority.get("environment_contract") == "runtime_environment":
            deployment_environment = self.runtime_environment
        else:
            deployment_environment = authority["environment"]
        profile = "windows" if os.name == "nt" else "linux"
        mounts: list[dict[str, object]] = []
        for destination, mount_authority in authority["mounts"].items():
            resource = self.protected_resources[mount_authority["resource"]]
            mount: dict[str, object] = {
                "Type": resource["type"],
                "Destination": destination,
                "RW": mount_authority["read_write"],
            }
            if resource["type"] == "volume":
                mount["Name"] = resource["name"]
                mount["Source"] = (
                    f"/var/lib/docker/volumes/{resource['name']}/_data"
                )
            else:
                source = resource["sources"][profile]
                suffix = str(mount_authority.get("suffix") or "").strip("/\\")
                mount["Source"] = f"{source}/{suffix}" if suffix else source
            mounts.append(mount)
        raw_health = authority.get("healthcheck")
        healthcheck = (
            None
            if raw_health is None
            else {
                "Test": raw_health["test"],
                "Interval": raw_health["interval_ns"],
                "Timeout": raw_health["timeout_ns"],
                "Retries": raw_health["retries"],
            }
        )
        security = authority["security"]
        tmpfs = {
            destination: ",".join(options)
            for destination, options in security["tmpfs"].items()
        }
        ports: dict[str, list[dict[str, str]]] = {}
        for port in authority["published_ports"]:
            ports[port["container"]] = [
                {"HostIp": port["host_ip"], "HostPort": port["host_port"]}
            ]
        return json.dumps(
            [
                {
                    "Id": container_id,
                    "Name": f"/{container_name}",
                    "Image": self.aux_image_override.get(service, IMAGE_ID),
                    "Config": {
                        "Image": "factor-lab-research-os:local",
                        "Entrypoint": authority["entrypoint"],
                        "Cmd": self.aux_command_override.get(
                            service, authority["command"]
                        ),
                        "WorkingDir": authority["working_directory"],
                        "User": authority["user"],
                        "Env": ["PATH=/usr/local/bin:/usr/bin:/bin"]
                        + [
                            f"{name}={value}"
                            for name, value in sorted(deployment_environment.items())
                        ],
                        "Healthcheck": healthcheck,
                        "Labels": {
                            "com.docker.compose.project": COMPOSE_PROJECT,
                            "com.docker.compose.service": service,
                            "com.docker.compose.oneoff": "False",
                        },
                    },
                    "HostConfig": {
                        "Privileged": security["privileged"],
                        "ReadonlyRootfs": security["read_only_root_filesystem"],
                        "CapAdd": security["cap_add"] or None,
                        "CapDrop": security["cap_drop"] or None,
                        "SecurityOpt": security["security_options"] or None,
                        "Tmpfs": tmpfs or None,
                        "PortBindings": ports,
                    },
                    "Mounts": mounts + self.aux_extra_mounts[service],
                    "NetworkSettings": {
                        "Networks": {
                            "factor-lab-research-os_default": {
                                "Aliases": aliases,
                                "NetworkID": NETWORK_ID,
                            }
                        }
                    },
                }
            ]
        )

    def _network(self) -> str:
        member_specs = [
            (CONTAINER_ID, "dagster-code-server", "factor-lab-research-os-dagster-code-server-1"),
            (DAEMON_CONTAINER_ID, "dagster-daemon", "factor-lab-research-os-dagster-daemon-1"),
            (WEBSERVER_CONTAINER_ID, "dagster-webserver", "factor-lab-research-os-dagster-webserver-1"),
            (MINIO_CONTAINER_ID, "minio", "factor-lab-research-os-minio-1"),
            (POSTGRES_CONTAINER_ID, "postgres", "factor-lab-research-os-postgres-1"),
            (WEBUI_CONTAINER_ID, "research-os-webui", "factor-lab-research-os-research-os-webui-1"),
        ]
        if self.extra_network_member is not None:
            member_specs.append(self.extra_network_member)
        containers = {
            container_id: {"Name": container_name}
            for container_id, _service, container_name in member_specs
        }
        return json.dumps(
            [
                {
                    "Name": "factor-lab-research-os_default",
                    "Id": NETWORK_ID,
                    "Driver": "bridge",
                    "Internal": False,
                    "Attachable": False,
                    "Labels": {
                        "com.docker.compose.project": COMPOSE_PROJECT,
                        "com.docker.compose.network": "default",
                        "com.docker.compose.config-hash": "4" * 64,
                    },
                    "Containers": containers,
                }
            ]
        )

    def _volume(self, name: str) -> str:
        logical = name.removeprefix("factor-lab-research-os_")
        return json.dumps(
            [
                {
                    "Name": name,
                    "Driver": "local",
                    "Scope": "local",
                    "Options": None,
                    "Mountpoint": self.volume_mountpoint_override.get(
                        name, f"/var/lib/docker/volumes/{name}/_data"
                    ),
                    "Labels": {
                        "com.docker.compose.project": COMPOSE_PROJECT,
                        "com.docker.compose.volume": logical,
                        "com.docker.compose.config-hash": "5" * 64,
                    },
                }
            ]
        )

    def _copy(self, source: Path, destination: Path) -> None:
        if source.is_dir():
            shutil.copytree(source, destination)
        else:
            shutil.copy2(source, destination)

    def run(self, arguments):
        command = tuple(map(str, arguments))
        self.commands.append(command)
        if command[0] == "ps":
            service_filter = next(
                (
                    value.rsplit("=", 1)[-1]
                    for value in command
                    if value.startswith("label=com.docker.compose.service=")
                ),
                None,
            )
            if service_filter is None:
                ids = [
                    CONTAINER_ID,
                    DAEMON_CONTAINER_ID,
                    WEBSERVER_CONTAINER_ID,
                    MINIO_CONTAINER_ID,
                    POSTGRES_CONTAINER_ID,
                    WEBUI_CONTAINER_ID,
                ]
                if self.extra_network_member is not None:
                    ids.append(self.extra_network_member[0])
                if self.off_network_container is not None:
                    ids.append(str(self.off_network_container["Id"]))
                return "\n".join(ids) + "\n"
            ids = {
                COMPOSE_SERVICE: self.ps_ids,
                "postgres": (POSTGRES_CONTAINER_ID,),
                "minio": (MINIO_CONTAINER_ID,),
            }[service_filter]
            return "\n".join(ids) + ("\n" if ids else "")
        if command[:3] == ("inspect", "--type", "container"):
            requested = command[3]
            if requested == CONTAINER_ID:
                return self._container()
            if requested == POSTGRES_CONTAINER_ID:
                return self._backend_container("postgres")
            if requested == MINIO_CONTAINER_ID:
                return self._backend_container("minio")
            members = {
                DAEMON_CONTAINER_ID: (
                    "dagster-daemon",
                    "factor-lab-research-os-dagster-daemon-1",
                ),
                WEBSERVER_CONTAINER_ID: (
                    "dagster-webserver",
                    "factor-lab-research-os-dagster-webserver-1",
                ),
                WEBUI_CONTAINER_ID: (
                    "research-os-webui",
                    "factor-lab-research-os-research-os-webui-1",
                ),
            }
            if requested in members:
                service, container_name = members[requested]
                aliases = [container_name, service]
                if self.duplicate_network_alias and requested == WEBUI_CONTAINER_ID:
                    aliases.append(self.duplicate_network_alias)
                return self._aux_container(
                    container_id=requested,
                    service=service,
                    container_name=container_name,
                    aliases=aliases,
                )
            if self.extra_network_member is not None and requested == self.extra_network_member[0]:
                container_id, service, container_name = self.extra_network_member
                return self._aux_container(
                    container_id=container_id,
                    service=service,
                    container_name=container_name,
                    aliases=[container_name, service],
                )
            if (
                self.off_network_container is not None
                and requested == self.off_network_container.get("Id")
            ):
                return json.dumps([self.off_network_container])
            raise AssertionError(f"unexpected container inspect: {requested}")
        if command[:2] == ("image", "inspect"):
            if command[2] == POSTGRES_IMAGE_ID:
                return self._backend_image("postgres")
            if command[2] == MINIO_IMAGE_ID:
                return self._backend_image("minio")
            return self._image()
        if command[:2] == ("network", "inspect"):
            return self._network()
        if command[:2] == ("volume", "inspect"):
            return self._volume(command[2])
        if command[0] == "create":
            return TEMPORARY_CONTAINER_ID + "\n"
        if command[0] == "cp":
            source_container, remote = command[1].split(":", 1)
            relative = remote.removeprefix("/opt/factor-lab/")
            if self.fail_copy == relative:
                raise DockerAttestationError("controlled copy failure")
            source = self.bundle_root / relative
            destination = Path(command[2])
            self._copy(source, destination)
            if self.tamper_copy == relative:
                target = (
                    destination / "factor_lab" / "__init__.py"
                    if destination.is_dir()
                    else destination
                )
                target.write_bytes(target.read_bytes() + b"tampered")
            if (
                source_container == CONTAINER_ID
                and self.tamper_running_copy == relative
            ):
                target = (
                    destination / "factor_lab" / "__init__.py"
                    if destination.is_dir()
                    else destination
                )
                target.write_bytes(target.read_bytes() + b"running-overlay")
            return ""
        if command[:2] == ("rm", "-f"):
            if self.fail_remove:
                raise DockerAttestationError("controlled remove failure")
            self.removed.append(command[2])
            return TEMPORARY_CONTAINER_ID + "\n"
        raise AssertionError(f"unexpected Docker command: {command!r}")


def _runtime(tmp_path: Path):
    bundle = tmp_path / "image-bundle"
    bundle.mkdir()
    _write_bundle(bundle)
    catalog = ResearchCatalog(tmp_path / "catalog.db")
    catalog.initialize_schema()
    runner = _FakeDockerRunner(bundle)
    service = HostDockerRuntimeAttestor.for_controlled_test(
        runner=runner,
        catalog=catalog,
        clock=lambda: NOW,
    )
    return catalog, runner, service


def test_controlled_attestation_selects_fixed_service_verifies_bundle_and_redacts_secrets(
    tmp_path: Path,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    try:
        result = service.attest()

        assert result.run_type == CONTROLLED_TEST_RUN_TYPE
        assert result.physical is False
        assert result.readiness_admission == CONTROLLED_TEST_REJECTION
        assert result.runtime.container_id == CONTAINER_ID
        assert result.runtime.image_id == IMAGE_ID
        assert result.runtime.health_status == "healthy"
        assert result.runtime.service_labels == {
            "com.docker.compose.project": COMPOSE_PROJECT,
            "com.docker.compose.service": COMPOSE_SERVICE,
            "com.docker.compose.oneoff": "False",
            "com.docker.compose.container-number": "1",
            "com.docker.compose.config-hash": "a" * 64,
        }
        assert result.deployment.base_image_digest == BASE_DIGEST
        assert result.deployment.temporary_container_removed is True
        assert result.deployment.running_container_bundle_verified is True
        assert len(result.runtime.runtime_contract_hash) == 64
        assert "hostname" not in result.runtime.runtime_contract
        assert runner.removed == [TEMPORARY_CONTAINER_ID]
        assert runner.commands[0] == (
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
        persisted = catalog.get_run(result.run_id)
        assert persisted is not None
        assert persisted.status == "succeeded"
        assert persisted.input_fingerprint == result.attestation_hash
        unsigned = dict(persisted.metadata)
        unsigned.pop("attestation_hash")
        assert content_fingerprint(unsigned, domain=SCHEMA_VERSION) == result.attestation_hash
        assert persisted.metadata["formal_readiness_eligible"] is False
        encoded = json.dumps(persisted.metadata, sort_keys=True)
        assert "must-not-appear" not in encoded
        assert str(tmp_path) not in encoded
        assert catalog.list_runs(limit=100, run_type=RUN_TYPE) == []
    finally:
        catalog.close()

@pytest.mark.parametrize("override", ["mount", "command", "pythonpath"])
def test_runtime_contract_rejects_mount_command_and_environment_overrides(
    tmp_path: Path,
    override: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    if override == "mount":
        runner.extra_mounts.append(
            {
                "Type": "bind",
                "Source": str(tmp_path),
                "Destination": "/opt/factor-lab/src",
                "RW": False,
            }
        )
    elif override == "command":
        runner.command_override = ["python", "-m", "factor_lab.unreviewed"]
    else:
        runner.environment_append.append("PYTHONPATH=/unreviewed")
    try:
        with pytest.raises(DockerAttestationError, match="runtime contract"):
            service.attest()
        assert not any(command[0] == "create" for command in runner.commands)
    finally:
        catalog.close()


@pytest.mark.parametrize(
    "variable_name",
    [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
        "NO_PROXY",
        "no_proxy",
        "REQUESTS_CA_BUNDLE",
        "requests_ca_bundle",
        "CURL_CA_BUNDLE",
        "SSL_CERT_FILE",
        "ssl_cert_dir",
        "AWS_CA_BUNDLE",
        "NODE_EXTRA_CA_CERTS",
        "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH",
    ],
)
def test_runtime_contract_rejects_ambient_network_and_tls_overrides_before_copy(
    tmp_path: Path,
    variable_name: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    private_value = "https://credential-at-unreviewed-route.invalid/private"
    runner.environment_append.append(f"{variable_name}={private_value}")
    try:
        with pytest.raises(DockerAttestationError, match="runtime contract") as caught:
            service.attest()

        assert private_value not in str(caught.value)
        assert not any(command[0] in {"create", "cp"} for command in runner.commands)
        assert catalog.list_runs(limit=100) == []
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("TUSHARE_TOKEN", "direct-secret-must-never-persist"),
        ("POSTGRES_PASSWORD", "direct-password-must-never-persist"),
        ("AWS_ACCESS_KEY_ID", "direct-access-key-must-never-persist"),
    ],
)
def test_runtime_contract_rejects_direct_credential_environment(
    tmp_path: Path,
    name: str,
    value: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    runner.environment_append.append(f"{name}={value}")
    try:
        with pytest.raises(DockerAttestationError, match=r"approved \*_FILE") as caught:
            service.attest()
        assert value not in str(caught.value)
        assert catalog.list_runs(limit=100) == []
    finally:
        catalog.close()


@pytest.mark.parametrize(
    "name",
    [
        "FACTOR_LAB_DATABASE_URL",
        "FACTOR_LAB_OBJECT_STORE_ENDPOINT",
        "AWS_ENDPOINT_URL",
    ],
)
def test_runtime_contract_rejects_database_and_object_store_routing_drift(
    tmp_path: Path,
    name: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    private_override = "http://unreviewed-route.invalid/private"
    runner.runtime_environment[name] = private_override
    try:
        with pytest.raises(DockerAttestationError, match="business environment") as caught:
            service.attest()
        assert private_override not in str(caught.value)
        assert catalog.list_runs(limit=100) == []
    finally:
        catalog.close()


@pytest.mark.parametrize(
    "drift",
    [
        "extra_member",
        "duplicate_alias",
        "wrong_image",
        "wrong_command",
        "extra_mount",
    ],
)
def test_shared_network_rejects_unknown_alias_collision_and_replaced_member(
    tmp_path: Path,
    drift: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    if drift == "extra_member":
        runner.extra_network_member = (
            "7" * 64,
            "unapproved",
            "factor-lab-research-os-unapproved-1",
        )
    elif drift == "duplicate_alias":
        runner.duplicate_network_alias = "postgres"
    elif drift == "wrong_image":
        runner.aux_image_override["research-os-webui"] = OTHER_IMAGE_ID
    elif drift == "wrong_command":
        runner.aux_command_override["dagster-daemon"] = [
            "python",
            "-m",
            "unreviewed",
        ]
    else:
        runner.aux_extra_mounts["dagster-daemon"].append(
            {
                "Type": "bind",
                "Source": str(tmp_path),
                "Destination": "/host",
                "RW": True,
            }
        )
    try:
        with pytest.raises(DockerAttestationError, match="network|application member"):
            service.attest()
        assert catalog.list_runs(limit=100) == []
    finally:
        catalog.close()


@pytest.mark.parametrize("drift", ["volume", "bind", "container_namespace"])
def test_global_audit_rejects_off_network_protected_resource_consumers(
    tmp_path: Path,
    drift: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    mounts: list[dict[str, object]] = []
    network_mode = "none"
    if drift == "volume":
        mounts.append(
            {
                "Type": "volume",
                "Name": "factor-lab-research-os_research-os-postgres",
                "Destination": "/stolen",
                "RW": True,
            }
        )
    elif drift == "bind":
        runtime_root = (
            "H:/Program Data/factor-lab-runtime"
            if os.name == "nt"
            else "/srv/factor-lab-runtime"
        )
        mounts.append(
            {
                "Type": "bind",
                "Source": f"{runtime_root}/secrets",
                "Destination": "/stolen",
                "RW": False,
            }
        )
    else:
        network_mode = f"container:{CONTAINER_ID[:12]}"
    runner.off_network_container = {
        "Id": "0" * 64,
        "Name": "/unapproved-consumer",
        "Config": {"Labels": {}},
        "HostConfig": {"NetworkMode": network_mode},
        "Mounts": mounts,
    }
    try:
        with pytest.raises(
            DockerAttestationError,
            match="protected|network namespace",
        ):
            service.attest()
        assert catalog.list_runs(limit=100) == []
    finally:
        catalog.close()


def test_global_audit_allows_unrelated_off_network_container(tmp_path: Path) -> None:
    catalog, runner, service = _runtime(tmp_path)
    runner.off_network_container = {
        "Id": "0" * 64,
        "Name": "/unrelated-plugin",
        "Config": {"Labels": {}},
        "HostConfig": {"NetworkMode": "none"},
        "Mounts": [
            {
                "Type": "volume",
                "Name": "unrelated-plugin-data",
                "Destination": "/data",
                "RW": True,
            }
        ],
    }
    try:
        result = service.attest()
        assert result.runtime.runtime_contract[
            "protected_resource_consumer_audit"
        ]["protected_resource_exclusivity_verified"] is True
    finally:
        catalog.close()


@pytest.mark.parametrize(
    "drift",
    ["extra_mount", "command", "privileged", "cap_add", "public_port"],
)
def test_backend_runtime_contract_rejects_mount_command_privilege_and_public_port_drift(
    tmp_path: Path,
    drift: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    if drift == "extra_mount":
        runner.backend_extra_mounts["postgres"].append(
            {
                "Type": "bind",
                "Source": str(tmp_path),
                "Destination": "/unapproved",
                "RW": False,
            }
        )
    elif drift == "command":
        runner.backend_command_override["postgres"] = ["postgres", "-c", "fsync=off"]
    elif drift == "privileged":
        runner.backend_privileged["postgres"] = True
    elif drift == "cap_add":
        runner.backend_cap_add["postgres"] = ["SYS_ADMIN"]
    else:
        runner.backend_host_ip["postgres"] = "0.0.0.0"
    try:
        with pytest.raises(DockerAttestationError, match="postgres"):
            service.attest()
        assert catalog.list_runs(limit=100) == []
    finally:
        catalog.close()


def test_host_storage_and_named_volume_must_match_protected_authority(
    tmp_path: Path,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    base = dict(runner.host_storage_evidence)
    base["host_data_root"] = (
        "C:/unapproved/DockerDesktopWSL"
        if os.name == "nt"
        else "/tmp/unapproved-docker-root"
    )
    runner.host_storage_override = base
    try:
        with pytest.raises(DockerAttestationError, match="storage root"):
            service.attest()
    finally:
        catalog.close()

    volume_case = tmp_path / "volume-case"
    volume_case.mkdir()
    catalog, runner, service = _runtime(volume_case)
    name = "factor-lab-research-os_research-os-postgres"
    runner.volume_mountpoint_override[name] = "/var/lib/docker/volumes/unapproved/_data"
    try:
        with pytest.raises(DockerAttestationError, match="named-volume authority"):
            service.attest()
    finally:
        catalog.close()


def test_formal_attestation_attempt_records_safe_failure_and_never_falls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, runner, _controlled = _runtime(tmp_path)
    runner.ps_ids = ()
    service = HostDockerRuntimeAttestor(
        catalog=catalog,
        runner=runner,
        clock=lambda: NOW,
        controlled_test=False,
    )
    monkeypatch.setattr(service, "_assert_admission", lambda: None)
    times = iter((NOW - timedelta(seconds=1), NOW + timedelta(seconds=1)))
    monkeypatch.setattr(catalog, "database_now", lambda: next(times))
    try:
        with pytest.raises(DockerAttestationError, match="exactly one"):
            service.attest()
        attempts = catalog.list_runs(limit=10, run_type=ATTEMPT_RUN_TYPE)
        assert len(attempts) == 1
        attempt = attempts[0]
        assert attempt.status == "failed"
        assert attempt.error == "docker_attestation_error"
        assert attempt.metadata["schema_version"] == ATTEMPT_SCHEMA_VERSION
        assert attempt.metadata["authority"] == ATTEMPT_AUTHORITY
        assert attempt.metadata["outcome"] == "failed"
        assert attempt.metadata["formal_readiness_eligible"] is False
        assert attempt.input_fingerprint == host_docker_attempt_fingerprint(
            started_at=attempt.started_at,
            nonce=str(attempt.metadata["attempt_nonce"]),
        )
        assert "exactly one" not in json.dumps(attempt.metadata)
        assert catalog.list_runs(limit=10, run_type=RUN_TYPE) == []
    finally:
        catalog.close()


def test_formal_attestation_attempt_binds_new_success_to_authoritative_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, runner, _controlled = _runtime(tmp_path)
    service = HostDockerRuntimeAttestor(
        catalog=catalog,
        runner=runner,
        clock=lambda: NOW,
        controlled_test=False,
    )
    monkeypatch.setattr(service, "_assert_admission", lambda: None)
    canonical = attestation_module._canonical_bind_source
    monkeypatch.setattr(
        attestation_module,
        "_canonical_bind_source",
        lambda value, *, require_physical: canonical(value, require_physical=False),
    )
    times = iter((NOW - timedelta(seconds=1), NOW + timedelta(seconds=1)))
    monkeypatch.setattr(catalog, "database_now", lambda: next(times))
    try:
        result = service.attest()
        attempts = catalog.list_runs(limit=10, run_type=ATTEMPT_RUN_TYPE)
        assert len(attempts) == 1
        attempt = attempts[0]
        assert attempt.status == "succeeded"
        assert attempt.metadata["outcome"] == "succeeded"
        assert attempt.metadata["attestation_run_id"] == result.run_id
        assert attempt.metadata["attestation_hash"] == result.attestation_hash
        assert attempt.metadata["formal_readiness_eligible"] is True
        assert catalog.get_run(result.run_id) is not None
    finally:
        catalog.close()


def test_formal_attempt_completion_covers_later_bound_attestation_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog, runner, _controlled = _runtime(tmp_path)
    deployment_verified_at = NOW + timedelta(milliseconds=5)
    clock_times = iter((NOW, deployment_verified_at))
    service = HostDockerRuntimeAttestor(
        catalog=catalog,
        runner=runner,
        clock=lambda: next(clock_times),
        controlled_test=False,
    )
    monkeypatch.setattr(service, "_assert_admission", lambda: None)
    canonical = attestation_module._canonical_bind_source
    monkeypatch.setattr(
        attestation_module,
        "_canonical_bind_source",
        lambda value, *, require_physical: canonical(value, require_physical=False),
    )
    # PostgreSQL transaction_timestamp() can be sampled just before the
    # separately clocked deployment result is completed.  The attempt must
    # still cover the proof it claims to bind.
    database_times = iter((NOW - timedelta(seconds=1), NOW))
    monkeypatch.setattr(catalog, "database_now", lambda: next(database_times))
    try:
        result = service.attest()
        bound = catalog.get_run(result.run_id)
        attempt = catalog.list_runs(limit=10, run_type=ATTEMPT_RUN_TYPE)[0]

        assert bound is not None and bound.completed_at is not None
        assert bound.completed_at == deployment_verified_at
        assert result.deployment.verified_at == deployment_verified_at
        assert attempt.completed_at == deployment_verified_at
        assert attempt.completed_at >= bound.completed_at
        assert attempt.completed_at >= result.deployment.verified_at
        assert attempt.completed_at >= attempt.started_at
    finally:
        catalog.close()


def test_running_container_bundle_overlay_is_rejected_and_temp_removed(
    tmp_path: Path,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    runner.tamper_running_copy = "src"
    try:
        with pytest.raises(DockerAttestationError, match="source bundle"):
            service.attest()
        assert runner.removed == [TEMPORARY_CONTAINER_ID]
    finally:
        catalog.close()


def test_persisted_runtime_proof_binds_historical_run_to_stable_deployment(
    tmp_path: Path,
) -> None:
    catalog, _runner, service = _runtime(tmp_path)
    try:
        controlled = service.attest()
        stored = catalog.get_run(controlled.run_id)
        assert stored is not None
        metadata = {
            **dict(stored.metadata),
            "physical": True,
            "controlled_test_runner": False,
            "readiness_admission": "host_daemon_inspected_deployment",
            "formal_readiness_eligible": True,
        }
        docker_authority = {
            "authority": "explicit_local_docker_engine_endpoint",
            "endpoint_policy": "local_unix_docker_socket",
            "cli_path": "/usr/bin/docker",
            "cli_sha256": "6" * 64,
            "client_version": "29.7.2",
            "server_version": "29.7.2",
            "server_os": "linux",
            "ambient_docker_routing_rejected": True,
        }
        metadata["docker_authority"] = docker_authority
        metadata["docker_authority_hash"] = content_fingerprint(
            docker_authority,
            domain="research-os/host-docker-local-authority/v1",
        )
        metadata.pop("attestation_hash", None)
        attestation_hash = content_fingerprint(metadata, domain=SCHEMA_VERSION)
        metadata["attestation_hash"] = attestation_hash
        physical = RunRecord(
            run_id=f"docker_attestation_{attestation_hash}",
            run_type=RUN_TYPE,
            status="succeeded",
            input_fingerprint=attestation_hash,
            started_at=stored.started_at,
            completed_at=stored.completed_at,
            metadata=metadata,
        )
        build_identity_hash = "8" * 64
        runtime_contract_hash = str(metadata["runtime_contract_hash"])
        stable = {
            "controlled_test_backend": False,
            "compose_config_hash": "a" * 64,
            "build_identity_hash": build_identity_hash,
            "runtime_contract_hash": runtime_contract_hash,
            "oci_image_id": IMAGE_ID,
            "oci_repo_digests": [
                "factor-lab-research-os@sha256:" + "7" * 64
            ],
            "oci_base_digests": [BASE_DIGEST],
        }
        deployment_identity_hash = content_fingerprint(
            {
                "container_id": CONTAINER_ID,
                "oci_image_id": IMAGE_ID,
                "compose_config_hash": "a" * 64,
                "build_identity_hash": build_identity_hash,
                "runtime_contract_hash": runtime_contract_hash,
            },
            domain="research-os/host-docker-deployment-identity/v1",
        )
        proof = {
            "host_attestation_run_id": physical.run_id,
            "host_attestation_hash": attestation_hash,
            "attested_at": physical.completed_at.isoformat(),
            "container_started_at": metadata["container_started_at"],
            "container_id": CONTAINER_ID,
            "executing_container_identity": CONTAINER_ID[:12],
            "executing_container_started_at": metadata["container_started_at"],
            "executing_root_matches_init_root": True,
            "docker_authority_hash": metadata["docker_authority_hash"],
            "deployment_identity_hash": deployment_identity_hash,
            **{key: value for key, value in stable.items() if key != "controlled_test_backend"},
        }

        assert persisted_attestation_binding_errors(
            run=physical,
            proof=proof,
            stable_deployment=stable,
        ) == ()
        kernel_process_proof = dict(proof)
        kernel_process_proof.pop("executing_container_started_at")
        kernel_process_proof.update(
            executing_process_identity_scheme=(
                "linux-boot-id-pid1-start-ticks-v1"
            ),
            executing_process_identity="b" * 64,
        )
        assert persisted_attestation_binding_errors(
            run=physical,
            proof=kernel_process_proof,
            stable_deployment=stable,
        ) == ()
        hybrid_process_proof = {
            **kernel_process_proof,
            "executing_container_started_at": proof[
                "executing_container_started_at"
            ],
        }
        assert {
            "runtime_process_continuity_ambiguous",
            "runtime_attestation_stable_binding_invalid",
        }.issubset(
            set(
                persisted_attestation_binding_errors(
                    run=physical,
                    proof=hybrid_process_proof,
                    stable_deployment=stable,
                )
            )
        )
        malformed_process_proof = {
            **kernel_process_proof,
            "executing_process_identity": "not-a-hash",
        }
        assert {
            "runtime_process_continuity_missing",
            "runtime_process_identity_invalid",
            "runtime_attestation_stable_binding_invalid",
        }.issubset(
            set(
                persisted_attestation_binding_errors(
                    run=physical,
                    proof=malformed_process_proof,
                    stable_deployment=stable,
                )
            )
        )
        assert "runtime_attestation_run_invalid" in persisted_attestation_binding_errors(
            run=physical,
            proof={**proof, "host_attestation_hash": "9" * 64},
            stable_deployment=stable,
        )
        assert "runtime_attestation_stable_binding_invalid" in (
            persisted_attestation_binding_errors(
                run=physical,
                proof=proof,
                stable_deployment={**stable, "oci_image_id": OTHER_IMAGE_ID},
            )
        )
        missing_executor = dict(proof)
        missing_executor.pop("executing_container_identity")
        assert "runtime_attestation_stable_binding_invalid" in (
            persisted_attestation_binding_errors(
                run=physical,
                proof=missing_executor,
                stable_deployment=stable,
            )
        )
        assert "runtime_attestation_stable_binding_invalid" in (
            persisted_attestation_binding_errors(
                run=physical,
                proof={**proof, "executing_container_identity": "f" * 12},
                stable_deployment=stable,
            )
        )
        assert "runtime_attestation_stable_binding_invalid" in (
            persisted_attestation_binding_errors(
                run=physical,
                proof={**proof, "executing_root_matches_init_root": False},
                stable_deployment=stable,
            )
        )
        assert "runtime_attestation_run_missing" in persisted_attestation_binding_errors(
            run=None,
            proof=proof,
            stable_deployment=stable,
        )
    finally:
        catalog.close()


@pytest.mark.parametrize("ids", [(), (CONTAINER_ID, "8" * 64)])
def test_zero_or_multiple_running_service_containers_fail_closed(
    tmp_path: Path,
    ids: tuple[str, ...],
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    runner.ps_ids = ids
    try:
        with pytest.raises(DockerAttestationError, match="exactly one"):
            service.attest()

        assert not any(command[0] == "create" for command in runner.commands)
        assert catalog.list_runs(limit=100, run_type=CONTROLLED_TEST_RUN_TYPE) == []
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("health_status", "unhealthy", "not the unique healthy"),
        ("failing_streak", 1, "nonzero failure streak"),
        ("daemon_image_id", OTHER_IMAGE_ID, "image ID or pinned"),
        ("image_base_digest", "not-a-digest", "image ID or pinned"),
    ],
)
def test_unhealthy_or_unbound_runtime_image_is_rejected(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    setattr(runner, field, value)
    try:
        with pytest.raises(DockerAttestationError, match=message):
            service.attest()
        assert catalog.list_runs(limit=100, run_type=CONTROLLED_TEST_RUN_TYPE) == []
    finally:
        catalog.close()


def test_verified_dockerfile_base_pin_must_match_image_label_and_cleanup_occurs(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "mismatched-bundle"
    bundle.mkdir()
    _write_bundle(bundle, base_digest=OTHER_BASE_DIGEST)
    catalog = ResearchCatalog(tmp_path / "catalog.db")
    catalog.initialize_schema()
    runner = _FakeDockerRunner(bundle)
    service = HostDockerRuntimeAttestor.for_controlled_test(
        runner=runner,
        catalog=catalog,
        clock=lambda: NOW,
    )
    try:
        with pytest.raises(DockerAttestationError, match="differ from"):
            service.attest()

        assert runner.removed == [TEMPORARY_CONTAINER_ID]
        assert catalog.list_runs(limit=100, run_type=CONTROLLED_TEST_RUN_TYPE) == []
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("destination", "field", "replacement"),
    [
        (
            "/opt/factor-lab/runtime/data",
            "Source",
            "C:/unapproved/factor-lab-runtime/data"
            if os.name == "nt"
            else "/tmp/unapproved-factor-lab-data",
        ),
        (
            "/run/secrets",
            "Source",
            "C:/unapproved/secrets"
            if os.name == "nt"
            else "/tmp/unapproved-secrets",
        ),
        (
            "/opt/dagster/home/storage",
            "Name",
            "unapproved-dagster-volume",
        ),
    ],
)
def test_mount_source_identity_must_match_protected_deployment_authority(
    tmp_path: Path,
    destination: str,
    field: str,
    replacement: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    original = runner._container

    def changed_container() -> str:
        payload = json.loads(original())
        mount = next(
            item
            for item in payload[0]["Mounts"]
            if item["Destination"] == destination
        )
        mount[field] = replacement
        return json.dumps(payload)

    runner._container = changed_container  # type: ignore[method-assign]
    try:
        with pytest.raises(DockerAttestationError, match="mount sources"):
            service.attest()
        assert runner.removed == [TEMPORARY_CONTAINER_ID]
    finally:
        catalog.close()


@pytest.mark.parametrize(
    ("failure_kind", "message"),
    [
        ("tamper", "immutable verification"),
        ("copy", "controlled copy failure"),
        ("remove", "cleanup failed"),
    ],
)
def test_deployment_copy_tamper_failure_and_cleanup_failure_never_persist(
    tmp_path: Path,
    failure_kind: str,
    message: str,
) -> None:
    catalog, runner, service = _runtime(tmp_path)
    if failure_kind == "tamper":
        runner.tamper_copy = "src"
    elif failure_kind == "copy":
        runner.fail_copy = "configs"
    else:
        runner.fail_remove = True
    try:
        with pytest.raises(DockerAttestationError, match=message):
            service.attest()

        if failure_kind != "remove":
            assert runner.removed == [TEMPORARY_CONTAINER_ID]
        assert catalog.list_runs(limit=100, run_type=CONTROLLED_TEST_RUN_TYPE) == []
    finally:
        catalog.close()


def test_formal_factory_rejects_sqlite_before_any_docker_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    _write_bundle(bundle)
    runner = _FakeDockerRunner(bundle)
    catalog = ResearchCatalog(tmp_path / "catalog.db")
    catalog.initialize_schema()
    monkeypatch.setattr(attestation_module, "_SubprocessDockerRunner", lambda: runner)
    try:
        service = HostDockerRuntimeAttestor.from_host(catalog=catalog)
        with pytest.raises(DockerAttestationAdmissionError, match="PostgreSQL"):
            service.attest()
        assert runner.commands == []
    finally:
        catalog.close()


@pytest.mark.parametrize("name", ["DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY"])
def test_subprocess_runner_rejects_ambient_docker_routing_before_execution(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    calls: list[object] = []
    monkeypatch.setenv(name, "must-not-be-read-or-persisted")
    monkeypatch.setattr(
        attestation_module.subprocess,
        "run",
        lambda *_args, **_kwargs: calls.append(object()),
    )
    runner = attestation_module._SubprocessDockerRunner()

    with pytest.raises(DockerAttestationError, match="routing environment") as exc:
        runner.run(("ps",))

    assert calls == []
    assert "must-not-be-read-or-persisted" not in str(exc.value)


def test_subprocess_runner_rejects_path_precedence_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    trusted = tmp_path / "trusted" / ("docker.exe" if os.name == "nt" else "docker")
    fake = tmp_path / "fake" / ("docker.exe" if os.name == "nt" else "docker")
    trusted.parent.mkdir()
    fake.parent.mkdir()
    trusted.write_bytes(b"trusted")
    fake.write_bytes(b"fake")
    trusted.chmod(0o755)
    fake.chmod(0o755)
    monkeypatch.setattr(
        attestation_module._SubprocessDockerRunner,
        "_trusted_cli_candidates",
        staticmethod(lambda: (trusted,)),
    )
    monkeypatch.setenv("PATH", str(fake.parent))
    calls: list[object] = []
    monkeypatch.setattr(
        attestation_module.subprocess,
        "run",
        lambda *_args, **_kwargs: calls.append(object()),
    )
    runner = attestation_module._SubprocessDockerRunner()

    with pytest.raises(DockerAttestationError, match="trusted Docker CLI"):
        runner.run(("ps",))

    assert calls == []


def test_public_operation_accepts_no_container_or_image_identity() -> None:
    attest_parameters = tuple(
        inspect.signature(HostDockerRuntimeAttestor.attest).parameters
    )
    host_parameters = set(
        inspect.signature(HostDockerRuntimeAttestor.from_host).parameters
    )
    source = inspect.getsource(attestation_module)

    assert attest_parameters == ("self",)
    assert not {
        "container_id",
        "image_id",
        "image_hash",
        "base_digest",
    }.intersection(host_parameters)
    assert "FACTOR_LAB_OCI_IMAGE_ID" not in source
    assert '["docker", *values]' not in source
    assert '"--host", self._endpoint' in source
