from __future__ import annotations

"""Provision and verify the PostgreSQL login used by the read-only WebUI.

This module is run by a one-shot Compose service after Alembic.  Both database
passwords arrive through files and are passed to psycopg as parameters; they
are never placed in a DSN, command line, SQL log, or process environment.
"""

from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat
from typing import Mapping


_ROLE_NAME = re.compile(r"[a-z_][a-z0-9_]{0,62}")
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)


class WebUIRoleBootstrapError(RuntimeError):
    pass


@dataclass(frozen=True)
class WebUIDatabaseRoleConfig:
    host: str
    port: int
    database: str
    owner_role: str
    webui_role: str
    owner_password_file: Path
    webui_password_file: Path

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None
    ) -> "WebUIDatabaseRoleConfig":
        values = os.environ if env is None else env
        owner_role = _validated_role(
            values.get("RESEARCH_OS_POSTGRES_USER", ""), "owner role"
        )
        webui_role = _validated_role(
            values.get("RESEARCH_OS_WEBUI_POSTGRES_USER", ""), "WebUI role"
        )
        if owner_role == webui_role:
            raise WebUIRoleBootstrapError(
                "WebUI PostgreSQL role must differ from the migration owner"
            )
        try:
            port = int(values.get("RESEARCH_OS_POSTGRES_PORT_INTERNAL", "5432"))
        except ValueError as exc:
            raise WebUIRoleBootstrapError("PostgreSQL port must be an integer") from exc
        if not 1 <= port <= 65535:
            raise WebUIRoleBootstrapError("PostgreSQL port is outside 1..65535")
        host = str(values.get("RESEARCH_OS_POSTGRES_HOST") or "").strip()
        database = str(values.get("RESEARCH_OS_POSTGRES_DB") or "").strip()
        if not host or not database or "\x00" in host + database:
            raise WebUIRoleBootstrapError("PostgreSQL host/database is missing")
        return cls(
            host=host,
            port=port,
            database=database,
            owner_role=owner_role,
            webui_role=webui_role,
            owner_password_file=Path(
                str(values.get("FACTOR_LAB_POSTGRES_PASSWORD_FILE") or "")
            ),
            webui_password_file=Path(
                str(values.get("FACTOR_LAB_WEBUI_POSTGRES_PASSWORD_FILE") or "")
            ),
        )


def _validated_role(value: str, label: str) -> str:
    role = str(value or "").strip()
    if not _ROLE_NAME.fullmatch(role):
        raise WebUIRoleBootstrapError(
            f"{label} must be a lowercase PostgreSQL identifier"
        )
    return role


def _read_one_line_secret(path: Path, label: str) -> str:
    if not str(path):
        raise WebUIRoleBootstrapError(f"{label} file is missing")
    try:
        info = os.lstat(path)
    except FileNotFoundError as exc:
        raise WebUIRoleBootstrapError(f"{label} file is missing") from exc
    if stat.S_ISLNK(info.st_mode) or bool(
        int(getattr(info, "st_file_attributes", 0)) & _REPARSE_POINT
    ):
        raise WebUIRoleBootstrapError(f"{label} file cannot be a link/reparse point")
    if not stat.S_ISREG(info.st_mode):
        raise WebUIRoleBootstrapError(f"{label} file must be regular")
    lines = path.read_text(encoding="utf-8-sig").splitlines()
    if len(lines) != 1 or not lines[0] or "\x00" in lines[0]:
        raise WebUIRoleBootstrapError(
            f"{label} file must contain exactly one non-empty line"
        )
    return lines[0]


def _validate_webui_password(owner_password: str, webui_password: str) -> None:
    lowered_password = webui_password.casefold()
    forbidden_fragments = (
        "change-me",
        "changeme",
        "replace-me",
        "password",
        "factor-lab-local",
    )
    if len(webui_password) < 16 or any(
        fragment in lowered_password for fragment in forbidden_fragments
    ):
        raise WebUIRoleBootstrapError(
            "WebUI PostgreSQL password is too short or uses a forbidden placeholder"
        )
    if owner_password == webui_password:
        raise WebUIRoleBootstrapError(
            "WebUI PostgreSQL password must differ from the migration owner"
        )


def provision_webui_readonly_role(config: WebUIDatabaseRoleConfig) -> None:
    try:
        import psycopg
        from psycopg import sql
    except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover
        raise WebUIRoleBootstrapError("psycopg 3 is required") from exc

    owner_password = _read_one_line_secret(
        config.owner_password_file, "PostgreSQL owner password"
    )
    webui_password = _read_one_line_secret(
        config.webui_password_file, "WebUI PostgreSQL password"
    )
    _validate_webui_password(owner_password, webui_password)

    connect = {
        "host": config.host,
        "port": config.port,
        "dbname": config.database,
        "user": config.owner_role,
        "password": owner_password,
        "connect_timeout": 10,
    }
    role = sql.Identifier(config.webui_role)
    owner = sql.Identifier(config.owner_role)
    database = sql.Identifier(config.database)
    public = sql.Identifier("public")
    with psycopg.connect(**connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (config.webui_role,)
            )
            if cursor.fetchone() is None:
                cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(role))
            cursor.execute(
                sql.SQL(
                    "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {}"
                ).format(role, sql.Literal(webui_password))
            )
            cursor.execute(
                sql.SQL("ALTER ROLE {} SET default_transaction_read_only TO 'on'").format(
                    role
                )
            )
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, role)
            )
            cursor.execute(
                sql.SQL("REVOKE CREATE ON SCHEMA {} FROM {}").format(public, role)
            )
            cursor.execute(
                sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(public, role)
            )
            cursor.execute(
                sql.SQL(
                    "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {} FROM {}"
                ).format(public, role)
            )
            cursor.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                    public, role
                )
            )
            cursor.execute(
                sql.SQL(
                    "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {} FROM {}"
                ).format(public, role)
            )
            cursor.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                    "REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER "
                    "ON TABLES FROM {}"
                ).format(owner, public, role)
            )
            cursor.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                    "GRANT SELECT ON TABLES TO {}"
                ).format(owner, public, role)
            )
            cursor.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                    "REVOKE USAGE, UPDATE ON SEQUENCES FROM {}"
                ).format(owner, public, role)
            )
            cursor.execute(
                """
                SELECT c.relname
                FROM pg_class AS c
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
                  AND has_table_privilege(
                        %s,
                        format('%%I.%%I', n.nspname, c.relname),
                        'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
                      )
                ORDER BY c.relname
                """,
                (config.webui_role,),
            )
            writable = [str(row[0]) for row in cursor.fetchall()]
            if writable:
                raise WebUIRoleBootstrapError(
                    "WebUI role inherited write privileges on public tables"
                )

    # A second login proves the dedicated password works and the server applies
    # the read-only session default before the WebUI container starts.
    with psycopg.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.webui_role,
        password=webui_password,
        connect_timeout=10,
    ) as verification:
        with verification.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            if str(cursor.fetchone()[0]).lower() != "on":
                raise WebUIRoleBootstrapError(
                    "WebUI PostgreSQL session did not start read-only"
                )
            cursor.execute("SELECT version_num FROM alembic_version")
            if cursor.fetchone() is None:
                raise WebUIRoleBootstrapError(
                    "WebUI role cannot read the migrated catalog"
                )


def main() -> int:
    provision_webui_readonly_role(WebUIDatabaseRoleConfig.from_env())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
