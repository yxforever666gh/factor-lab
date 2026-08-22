from pathlib import Path
import json
import os
import shutil
import subprocess

import pytest
import yaml

from factor_lab.research_os.runtime import ResearchOSSettings


COMPOSE = Path("infra/research_os/docker-compose.yml")
ENV_EXAMPLE = Path("infra/research_os/.env.example")
ALEMBIC = Path("infra/research_os/alembic.ini")


def test_research_os_compose_is_loopback_only_and_has_no_secret_defaults() -> None:
    text = COMPOSE.read_text(encoding="utf-8")
    alembic = ALEMBIC.read_text(encoding="utf-8")

    assert '127.0.0.1:${RESEARCH_OS_POSTGRES_PORT:-5433}:5432' in text
    assert '127.0.0.1:${RESEARCH_OS_MINIO_API_PORT:-9000}:9000' in text
    assert '127.0.0.1:${RESEARCH_OS_MINIO_CONSOLE_PORT:-9001}:9001' in text
    assert '127.0.0.1:${RESEARCH_OS_DAGSTER_PORT:-8766}:3000' in text
    assert '127.0.0.1:${RESEARCH_OS_WEBUI_PORT:-8765}:8765' in text
    assert "RESEARCH_OS_POSTGRES_PASSWORD:-" not in text
    assert "RESEARCH_OS_MINIO_ROOT_PASSWORD:-" not in text
    assert "fixture-postgres-password" not in text
    assert "factor-lab-local-minio" not in text
    assert "fixture-postgres-password" not in alembic
    assert "RESEARCH_OS_DATABASE_URL" in alembic
    assert "python:3.11-slim-bookworm@sha256:" in Path(
        "infra/research_os/Dockerfile.dagster"
    ).read_text(encoding="utf-8")
    assert "org.opencontainers.image.base.digest" in Path(
        "infra/research_os/Dockerfile.dagster"
    ).read_text(encoding="utf-8")
    for image in (
        "alpine:3.20.3@sha256:",
        "postgres:16.4-alpine@sha256:",
        "quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:",
        "quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z@sha256:",
    ):
        assert image in text
    assert text.count("image: factor-lab-research-os:${RESEARCH_OS_IMAGE_TAG:-local}") == 6


def test_dagster_uses_one_compose_managed_external_code_server() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    workspace = Path("infra/research_os/workspace.yaml").read_text(encoding="utf-8")

    section = compose.split("  dagster-code-server:", 1)[1].split(
        "  dagster-webserver:", 1
    )[0]
    assert "dagster\n      - code-server\n      - start" in section
    # A workspace ``grpc_server`` is user-managed, and Dagster constructs that
    # location with client heartbeat disabled.  Enabling the server watchdog
    # here would therefore kill a healthy process after its timeout.
    assert "--heartbeat" not in section
    assert "--heartbeat-timeout" not in section
    assert "--heartbeat-ttl" not in section
    assert "restart: unless-stopped" in section
    assert "DagsterGrpcClient" in section
    assert "condition: service_healthy" in compose.split(
        "  dagster-webserver:", 1
    )[1]
    assert "grpc_server:" in workspace
    assert "host: dagster-code-server" in workspace
    assert "port: 4000" in workspace
    assert "python_module:" not in workspace


def test_compose_ci_execs_reenter_the_secret_file_entrypoint() -> None:
    workflow = Path(".github/workflows/research-os-ci.yml").read_text(
        encoding="utf-8"
    )

    # docker exec starts a sibling process and does not inherit the PGPASSFILE
    # or AWS credential-file exports created by PID 1.  Every credentialed CI
    # probe must therefore pass through the entrypoint itself.
    assert workflow.count("docker compose -f infra/research_os/docker-compose.yml exec") == workflow.count(
        "/usr/local/bin/factor-lab-entrypoint"
    )
    assert "dagster job list -w /opt/dagster/workspace.yaml" in workflow
    assert "grep -q 'research_os_daily_job'" in workflow
    assert "RESEARCH_OS_SETTINGS_SECRETS_ROOT=$settings_secret_root" in workflow
    assert "RESEARCH_OS_WEBUI_SETTINGS_ROOT=$artifact_root/settings" in workflow
    assert "RESEARCH_OS_WEBUI_POSTGRES_PASSWORD_FILE=" in workflow
    assert "RESEARCH_OS_SECRETS_EDITOR_ROOT=" not in workflow


def test_code_server_has_writable_h_drive_cache_while_webui_is_read_only() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    code_server = compose.split("  dagster-code-server:", 1)[1].split(
        "  dagster-webserver:", 1
    )[0]
    webui = compose.split("  research-os-webui:", 1)[1].split("\nvolumes:", 1)[0]

    assert "target: /opt/factor-lab/runtime/data\n        read_only: true" not in code_server
    assert "target: /opt/factor-lab/runtime/data" not in webui
    assert "target: /opt/factor-lab/runtime/artifacts\n        read_only: true" in webui
    assert "target: /opt/factor-lab/runtime/artifacts/settings" in webui


def test_research_os_compose_has_fail_closed_weak_credential_guard() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    example = ENV_EXAMPLE.read_text(encoding="utf-8")

    assert "credential-check:" in compose
    assert 'value="$${1}"' in compose
    assert '[ "$${#value}" -ge 16 ]' in compose
    assert "awk 'END { print NR }'" in compose
    assert "*change-me*|*changeme*|*replace-me*|*password*" in compose
    assert "RESEARCH_OS_POSTGRES_PASSWORD_FILE=H:/Program Data/factor-lab-runtime/secrets/" in example
    assert "RESEARCH_OS_MINIO_ROOT_PASSWORD_FILE=H:/Program Data/factor-lab-runtime/secrets/" in example
    assert "RESEARCH_OS_TUSHARE_TOKEN_FILE=H:/Program Data/factor-lab-runtime/secrets/" in example
    assert "RESEARCH_OS_DIEMENG_API_KEY_FILE=H:/Program Data/factor-lab-runtime/secrets/" in example
    assert "RESEARCH_OS_WEBUI_POSTGRES_PASSWORD_FILE=H:/Program Data/factor-lab-runtime/secrets/" in example
    assert "RESEARCH_OS_POSTGRES_PASSWORD=\n" not in example
    assert "RESEARCH_OS_MINIO_ROOT_PASSWORD=\n" not in example


def test_research_os_compose_mounts_h_drive_roots_at_fixed_container_paths() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")
    example = ENV_EXAMPLE.read_text(encoding="utf-8")

    assert "source: ${RESEARCH_OS_DATA_ROOT:?" in compose
    assert "target: /opt/factor-lab/runtime/data" in compose
    assert "source: ${RESEARCH_OS_ARTIFACT_ROOT:?" in compose
    assert "target: /opt/factor-lab/runtime/artifacts" in compose
    assert "target: /run/secrets" in compose
    assert "RESEARCH_OS_DATA_ROOT=H:/Program Data/factor-lab-runtime/data" in example
    assert "RESEARCH_OS_ARTIFACT_ROOT=H:/Program Data/factor-lab-runtime/artifacts" in example
    assert "RESEARCH_OS_SETTINGS_SECRETS_ROOT=H:/Program Data/factor-lab-runtime/secrets/settings" in example
    assert "RESEARCH_OS_WEBUI_SETTINGS_ROOT=H:/Program Data/factor-lab-runtime/artifacts/settings" in example
    assert "FACTOR_LAB_SECRETS_DIR: /run/secrets" in compose


def test_webui_is_read_only_loopback_and_reuses_production_mount_contract() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")

    section = compose.split("  research-os-webui:", 1)[1].split("\nvolumes:", 1)[0]
    assert "read_only: true" in section
    assert "cap_drop: [ALL]" in section
    assert "target: /run/secrets" not in section
    assert "<<: *research-os-environment" not in section
    assert "FACTOR_LAB_ENV_FILE: /opt/factor-lab/runtime/artifacts/settings/webui.env" in section
    assert "FACTOR_LAB_SECRETS_DIR: /opt/factor-lab/runtime/secrets-editor" in section
    assert "target: /opt/factor-lab/runtime/secrets-editor" in section
    assert "target: /run/webui-db-secret/password" in section
    assert "FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE" not in section
    assert "TUSHARE_TOKEN_FILE" not in section
    assert "DIEMENG_API_KEY_FILE" not in section
    assert "DAGSTER_POSTGRES_URL" not in section
    assert "RESEARCH_OS_WEBUI_POSTGRES_USER" in section
    assert "factor_lab.webui_app:app" in section
    assert '127.0.0.1:${RESEARCH_OS_WEBUI_PORT:-8765}:8765' in section


def test_research_os_compose_never_places_raw_credentials_in_app_environment() -> None:
    compose = COMPOSE.read_text(encoding="utf-8")

    assert "FACTOR_LAB_OBJECT_STORE_SECRET_KEY:" not in compose
    assert "AWS_SECRET_ACCESS_KEY:" not in compose
    assert "TUSHARE_TOKEN:" not in compose
    assert "DIEMENG_API_KEY:" not in compose
    assert "postgresql+psycopg://${RESEARCH_OS_POSTGRES_USER:?set RESEARCH_OS_POSTGRES_USER}:" not in compose
    assert "FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE: /run/infra-secrets/" in compose
    assert "TUSHARE_TOKEN_FILE: /run/secrets/" in compose
    assert "env_file:" not in compose
    assert "FACTOR_LAB_DATA_SOURCE_ORDER:" not in compose


def test_entrypoint_does_not_shadow_hot_reloaded_webui_profile_settings() -> None:
    entrypoint = Path("infra/research_os/container-entrypoint.sh").read_text(
        encoding="utf-8"
    )

    assert "load_data_source_settings" not in entrypoint
    assert "FACTOR_LAB_DATA_SOURCE_PROFILES_JSON=*" not in entrypoint
    assert "FACTOR_LAB_DATA_SOURCE_ORDER=*" not in entrypoint
    assert "FACTOR_LAB_PRIMARY_DATA_SOURCE=*" not in entrypoint
    assert "FACTOR_LAB_LLM_PROFILES_JSON=*" not in entrypoint
    assert "FACTOR_LAB_LLM_API_KEY_REF=*" not in entrypoint
    assert 'export "$key=$value"' not in entrypoint
    assert "eval " not in entrypoint


def test_credential_file_consumers_only_write_ephemeral_runtime_secrets() -> None:
    model = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = model["services"]

    for service_name in (
        "catalog-migrate",
        "dagster-code-server",
        "dagster-webserver",
        "dagster-daemon",
        "webui-db-bootstrap",
        "research-os-webui",
    ):
        service = services[service_name]
        assert service["read_only"] is True
        assert service["cap_drop"] == ["ALL"]
        assert "no-new-privileges:true" in service["security_opt"]
        assert any(
            str(mount).startswith("/tmp:rw") for mount in service["tmpfs"]
        )

    minio_init = services["minio-init"]
    assert minio_init["read_only"] is True
    assert minio_init["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in minio_init["security_opt"]
    assert minio_init["environment"]["MC_CONFIG_DIR"] == "/tmp/.mc"
    assert any(str(mount).startswith("/tmp:rw") for mount in minio_init["tmpfs"])


def test_compose_model_isolates_webui_from_worker_credentials_and_writes() -> None:
    model = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = model["services"]
    webui = services["research-os-webui"]
    environment = webui["environment"]

    assert environment["RESEARCH_OS_POSTGRES_USER"].startswith(
        "${RESEARCH_OS_WEBUI_POSTGRES_USER"
    )
    assert environment["FACTOR_LAB_DATABASE_URL"].startswith(
        "postgresql+psycopg://${RESEARCH_OS_WEBUI_POSTGRES_USER"
    )
    forbidden = {
        "FACTOR_LAB_OBJECT_STORE_ACCESS_KEY_FILE",
        "FACTOR_LAB_OBJECT_STORE_SECRET_KEY_FILE",
        "FACTOR_LAB_ICEBERG_CATALOG_URI",
        "DAGSTER_POSTGRES_URL",
        "TUSHARE_TOKEN_FILE",
        "DIEMENG_API_KEY_FILE",
        "FACTOR_LAB_SECRETS_ROOT",
    }
    assert forbidden.isdisjoint(environment)
    assert webui["secrets"] == [
        {
            "source": "webui_postgres_password",
            "target": "/run/webui-db-secret/password",
        }
    ]
    targets = {volume["target"]: volume for volume in webui["volumes"]}
    assert "/run/secrets" not in targets
    assert "/opt/factor-lab/runtime/data" not in targets
    assert targets["/opt/factor-lab/runtime/artifacts"]["read_only"] is True
    assert "read_only" not in targets["/opt/factor-lab/runtime/artifacts/settings"]
    assert "read_only" not in targets["/opt/factor-lab/runtime/secrets-editor"]

    bootstrap = services["webui-db-bootstrap"]
    assert bootstrap["depends_on"]["catalog-migrate"]["condition"] == "service_completed_successfully"
    assert {item["source"] for item in bootstrap["secrets"]} == {
        "postgres_password",
        "webui_postgres_password",
    }
    assert webui["depends_on"]["webui-db-bootstrap"]["condition"] == "service_completed_successfully"


def test_docker_compose_render_keeps_webui_secret_surface_minimal(
    tmp_path: Path,
) -> None:
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker CLI is not installed")
    data = tmp_path / "data"
    artifacts = tmp_path / "artifacts"
    settings = artifacts / "settings"
    editor = tmp_path / "settings-secrets"
    for directory in (data, artifacts, settings, editor):
        directory.mkdir(parents=True, exist_ok=True)
    secret_paths: dict[str, Path] = {}
    for name in (
        "postgres_password",
        "webui_postgres_password",
        "minio_root_user",
        "minio_root_password",
        "tushare_token",
        "diemeng_api_key",
    ):
        path = tmp_path / name
        path.write_text(f"test-only-{name}-credential\n", encoding="utf-8")
        secret_paths[name] = path
    env = {
        **os.environ,
        "RESEARCH_OS_POSTGRES_USER": "factor_lab_owner",
        "RESEARCH_OS_WEBUI_POSTGRES_USER": "factor_lab_webui",
        "RESEARCH_OS_DATA_ROOT": str(data),
        "RESEARCH_OS_ARTIFACT_ROOT": str(artifacts),
        "RESEARCH_OS_WEBUI_SETTINGS_ROOT": str(settings),
        "RESEARCH_OS_SETTINGS_SECRETS_ROOT": str(editor),
        "RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(secret_paths["postgres_password"]),
        "RESEARCH_OS_WEBUI_POSTGRES_PASSWORD_FILE": str(secret_paths["webui_postgres_password"]),
        "RESEARCH_OS_MINIO_ROOT_USER_FILE": str(secret_paths["minio_root_user"]),
        "RESEARCH_OS_MINIO_ROOT_PASSWORD_FILE": str(secret_paths["minio_root_password"]),
        "RESEARCH_OS_TUSHARE_TOKEN_FILE": str(secret_paths["tushare_token"]),
        "RESEARCH_OS_DIEMENG_API_KEY_FILE": str(secret_paths["diemeng_api_key"]),
    }
    rendered = subprocess.run(
        [docker, "compose", "-f", str(COMPOSE), "config", "--format", "json"],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if rendered.returncode != 0 and "unknown flag" in rendered.stderr.lower():
        pytest.skip("Docker Compose does not support JSON config output")
    assert rendered.returncode == 0, rendered.stderr
    model = json.loads(rendered.stdout)
    webui = model["services"]["research-os-webui"]
    minio_healthcheck = model["services"]["minio"]["healthcheck"]["test"]
    assert minio_healthcheck == [
        "CMD",
        "curl",
        "--fail",
        "--silent",
        "--show-error",
        "http://127.0.0.1:9000/minio/health/live",
    ]
    assert not any(
        marker in json.dumps(minio_healthcheck).lower()
        for marker in ("/run/secrets", "cat ", "mc alias", "password")
    )
    assert {item["source"] for item in webui["secrets"]} == {
        "webui_postgres_password"
    }
    assert "/run/secrets" not in {
        volume.get("target") for volume in webui.get("volumes", [])
    }
    serialized = json.dumps(webui, sort_keys=True)
    assert "minio_root_password" not in serialized
    assert "postgres_password" not in serialized.replace(
        "webui_postgres_password", ""
    )
    assert "tushare_token" not in serialized
    assert "diemeng_api_key" not in serialized


def test_runtime_defaults_do_not_embed_local_passwords_or_object_store_secrets() -> None:
    settings = ResearchOSSettings()

    assert settings.object_store_access_key == ""
    assert settings.object_store_secret_key == ""
    assert "@" not in settings.database_url
    assert "@" not in settings.iceberg_catalog_uri
