"""Runtime configuration and idempotent run coordination for Research OS.

Production defaults deliberately point at PostgreSQL and the object-store data
lake.  SQLite remains an explicit test/development override; it is never chosen
silently when the production catalog is unavailable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import importlib.util
import os
from pathlib import Path
import socket
from typing import Any, Callable, Mapping, TypeVar
from urllib.parse import urlparse

from .catalog import ResearchCatalog, RunRecord
from .credentials import (
    CredentialResolutionError,
    read_secret_file,
    resolve_env_secret,
)
from .fingerprint import content_fingerprint


T = TypeVar("T")


@dataclass(frozen=True)
class ResearchOSSettings:
    """Process-level settings shared by CLI, Dagster and WebUI read models."""

    database_url: str = "postgresql+psycopg://127.0.0.1:5433/factor_lab"
    database_password_file: Path | None = field(default=None, repr=False, compare=False)
    object_store_endpoint: str = "http://127.0.0.1:9000"
    object_store_bucket: str = "factor-lab"
    object_store_access_key: str = ""
    object_store_secret_key: str = ""
    iceberg_catalog_uri: str = "postgresql+psycopg2://127.0.0.1:5433/factor_lab"
    lake_root: Path = Path("artifacts/research_os/lake")
    snapshot_root: Path = Path("artifacts/research_os/snapshots")
    legacy_sqlite_path: Path = Path("artifacts/factor_lab.db")
    environment: str = "local"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ResearchOSSettings":
        values = os.environ if env is None else env
        environment = values.get("FACTOR_LAB_ENVIRONMENT", cls.environment)
        production = str(environment).strip().lower() == "production"
        database_url = values.get("FACTOR_LAB_DATABASE_URL", cls.database_url)
        iceberg_catalog_uri = values.get(
            "FACTOR_LAB_ICEBERG_CATALOG_URI", cls.iceberg_catalog_uri
        )
        if production:
            for label, uri in (
                ("FACTOR_LAB_DATABASE_URL", database_url),
                ("FACTOR_LAB_ICEBERG_CATALOG_URI", iceberg_catalog_uri),
            ):
                if urlparse(uri).password:
                    raise CredentialResolutionError(
                        f"{label} must not embed a password in production; "
                        "use a PostgreSQL password-file credential"
                    )
        canonical_password_file = str(
            values.get("FACTOR_LAB_POSTGRES_PASSWORD_FILE") or ""
        ).strip()
        compose_password_file = str(
            values.get("RESEARCH_OS_POSTGRES_PASSWORD_FILE") or ""
        ).strip()
        if (
            canonical_password_file
            and compose_password_file
            and Path(canonical_password_file).resolve()
            != Path(compose_password_file).resolve()
        ):
            raise CredentialResolutionError(
                "PostgreSQL password file aliases disagree"
            )
        password_file_value = canonical_password_file or compose_password_file
        plain_database_password = str(
            values.get("FACTOR_LAB_POSTGRES_PASSWORD") or ""
        )
        if plain_database_password:
            raise CredentialResolutionError(
                "FACTOR_LAB_POSTGRES_PASSWORD is forbidden in production and "
                "deployment workflows; use "
                "FACTOR_LAB_POSTGRES_PASSWORD_FILE"
            )
        database_password_file = (
            Path(password_file_value) if password_file_value else None
        )
        if database_password_file is not None:
            # Validate the reference now, but do not retain the secret value.
            read_secret_file(
                database_password_file,
                label="FACTOR_LAB_POSTGRES_PASSWORD",
            )
        return cls(
            database_url=database_url,
            database_password_file=database_password_file,
            object_store_endpoint=values.get(
                "FACTOR_LAB_OBJECT_STORE_ENDPOINT", cls.object_store_endpoint
            ),
            object_store_bucket=values.get(
                "FACTOR_LAB_OBJECT_STORE_BUCKET", cls.object_store_bucket
            ),
            object_store_access_key=resolve_env_secret(
                "FACTOR_LAB_OBJECT_STORE_ACCESS_KEY",
                env=values,
                default=cls.object_store_access_key,
                allow_plain_env=not production,
            ),
            object_store_secret_key=resolve_env_secret(
                "FACTOR_LAB_OBJECT_STORE_SECRET_KEY",
                env=values,
                default=cls.object_store_secret_key,
                allow_plain_env=not production,
            ),
            iceberg_catalog_uri=iceberg_catalog_uri,
            lake_root=Path(values.get("FACTOR_LAB_LAKE_ROOT", str(cls.lake_root))),
            snapshot_root=Path(
                values.get("FACTOR_LAB_SNAPSHOT_ROOT", str(cls.snapshot_root))
            ),
            legacy_sqlite_path=Path(
                values.get(
                    "FACTOR_LAB_LEGACY_SQLITE_PATH", str(cls.legacy_sqlite_path)
                )
            ),
            environment=environment,
        )

    @property
    def uses_postgresql(self) -> bool:
        return self.database_url.startswith(("postgresql://", "postgresql+"))

    def public_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["lake_root"] = str(self.lake_root)
        payload["snapshot_root"] = str(self.snapshot_root)
        payload["legacy_sqlite_path"] = str(self.legacy_sqlite_path)
        payload.pop("database_password_file", None)
        payload["database_credential_configured"] = (
            self.database_password_file is not None
        )
        payload["object_store_access_key"] = (
            "***" if self.object_store_access_key else ""
        )
        payload["object_store_secret_key"] = "***"
        parsed = urlparse(self.database_url)
        if parsed.password:
            payload["database_url"] = self.database_url.replace(parsed.password, "***")
        parsed_iceberg = urlparse(self.iceberg_catalog_uri)
        if parsed_iceberg.password:
            payload["iceberg_catalog_uri"] = self.iceberg_catalog_uri.replace(
                parsed_iceberg.password, "***"
            )
        return payload

    def database_connect_args(self, *, timeout: float | None = None) -> dict[str, Any]:
        """Resolve a password file only at driver construction time."""

        connect_args: dict[str, Any] = {}
        if self.database_password_file is not None:
            connect_args["password"] = read_secret_file(
                self.database_password_file,
                label="FACTOR_LAB_POSTGRES_PASSWORD",
            )
        if timeout is not None:
            connect_args["connect_timeout"] = max(1, int(timeout))
        return connect_args


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    detail: str
    blocking: bool = False


@dataclass(frozen=True)
class DoctorReport:
    status: str
    checks: tuple[DoctorCheck, ...]
    settings: Mapping[str, Any]

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "ready": self.ready,
            "checks": [asdict(item) for item in self.checks],
            "settings": dict(self.settings),
        }


def _module_check(module: str, *, blocking: bool) -> DoctorCheck:
    installed = importlib.util.find_spec(module) is not None
    return DoctorCheck(
        name=f"python:{module}",
        status="pass" if installed else "fail",
        detail="installed" if installed else "not installed",
        blocking=blocking and not installed,
    )


def _tcp_check(name: str, uri: str, *, blocking: bool, timeout: float) -> DoctorCheck:
    parsed = urlparse(uri)
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        return DoctorCheck(name, "fail", "URI has no host/port", blocking=blocking)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        return DoctorCheck(
            name,
            "fail",
            f"{host}:{port} unavailable ({type(exc).__name__})",
            blocking=blocking,
        )
    return DoctorCheck(name, "pass", f"{host}:{port} reachable")


def _postgresql_query_check(settings: ResearchOSSettings, *, timeout: float) -> DoctorCheck:
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(
            settings.database_url,
            pool_pre_ping=True,
            connect_args=settings.database_connect_args(timeout=timeout),
        )
        try:
            with engine.connect() as connection:
                value = connection.execute(text("SELECT 1")).scalar_one()
        finally:
            engine.dispose()
        if value != 1:
            raise RuntimeError("SELECT 1 returned an unexpected value")
    except Exception as exc:
        return DoctorCheck(
            "postgresql_auth",
            "fail",
            f"catalog query failed ({type(exc).__name__})",
            blocking=True,
        )
    return DoctorCheck("postgresql_auth", "pass", "authenticated query succeeded")


def _object_store_bucket_check(settings: ResearchOSSettings) -> DoctorCheck:
    try:
        from .object_store import S3ImmutableArchive

        archive = S3ImmutableArchive.from_connection(
            endpoint=settings.object_store_endpoint,
            bucket=settings.object_store_bucket,
            access_key=settings.object_store_access_key,
            secret_key=settings.object_store_secret_key,
        )
        if not archive.filesystem.exists(settings.object_store_bucket):
            raise RuntimeError("configured bucket does not exist")
    except Exception as exc:
        return DoctorCheck(
            "object_store_auth",
            "fail",
            f"bucket check failed ({type(exc).__name__})",
            blocking=True,
        )
    return DoctorCheck(
        "object_store_auth",
        "pass",
        f"bucket {settings.object_store_bucket!r} is reachable",
    )


def doctor(
    settings: ResearchOSSettings,
    *,
    check_network: bool = True,
    timeout: float = 0.5,
) -> DoctorReport:
    checks = [
        _module_check("pydantic", blocking=True),
        _module_check("sqlalchemy", blocking=settings.uses_postgresql),
        _module_check("psycopg", blocking=settings.uses_postgresql),
        _module_check("dagster", blocking=True),
        _module_check("pyiceberg", blocking=True),
        _module_check("polars", blocking=True),
        _module_check("duckdb", blocking=True),
    ]
    if check_network:
        checks.append(
            _tcp_check(
                "postgresql",
                settings.database_url,
                blocking=settings.uses_postgresql,
                timeout=timeout,
            )
        )
        if settings.uses_postgresql:
            checks.append(_postgresql_query_check(settings, timeout=timeout))
        checks.append(_object_store_bucket_check(settings))
        checks.append(
            _tcp_check(
                "object_store",
                settings.object_store_endpoint,
                blocking=True,
                timeout=timeout,
            )
        )
    for name, path in (
        ("lake_root", settings.lake_root),
        ("snapshot_root", settings.snapshot_root),
    ):
        parent = path if path.exists() else path.parent
        checks.append(
            DoctorCheck(
                name=name,
                status="pass" if parent.exists() else "warn",
                detail=str(path.resolve()),
                blocking=False,
            )
        )
    blocked = any(item.status == "fail" and item.blocking for item in checks)
    degraded = any(item.status != "pass" for item in checks)
    return DoctorReport(
        status="blocked" if blocked else ("degraded" if degraded else "ready"),
        checks=tuple(checks),
        settings=settings.public_dict(),
    )


class RunCoordinator:
    """Persist every deterministic operation in the authoritative run ledger."""

    def __init__(self, catalog: ResearchCatalog) -> None:
        self.catalog = catalog

    def execute(
        self,
        run_type: str,
        inputs: Mapping[str, Any],
        action: Callable[[], T],
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> T:
        now = datetime.now(timezone.utc)
        input_fingerprint = content_fingerprint(
            dict(inputs), domain=f"factor-lab/research-os/v1/run/{run_type}"
        )
        run_id = f"run_{run_type.replace(' ', '_')}__{input_fingerprint[:32]}"
        initial = RunRecord(
            run_id=run_id,
            run_type=run_type,
            status="running",
            input_fingerprint=input_fingerprint,
            started_at=now,
            metadata=dict(metadata or {}),
        )
        self.catalog.save_run(initial)
        try:
            result = action()
        except Exception as exc:
            self.catalog.save_run(
                RunRecord(
                    **{
                        **asdict(initial),
                        "status": "failed",
                        "completed_at": datetime.now(timezone.utc),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
            )
            raise
        result_metadata = dict(metadata or {})
        if hasattr(result, "to_dict"):
            result_metadata["result"] = result.to_dict()  # type: ignore[union-attr]
        elif isinstance(result, Mapping):
            result_metadata["result"] = dict(result)
        self.catalog.save_run(
            RunRecord(
                **{
                    **asdict(initial),
                    "status": "completed",
                    "completed_at": datetime.now(timezone.utc),
                    "metadata": result_metadata,
                }
            )
        )
        return result


__all__ = [
    "DoctorCheck",
    "DoctorReport",
    "ResearchOSSettings",
    "RunCoordinator",
    "doctor",
]
