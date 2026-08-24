"""Credential boundary for online Research OS Alembic migrations.

Alembic constructs its SQLAlchemy engine outside :class:`ResearchCatalog`, so
it must opt into the same password-file contract explicitly.  Keep the secret
out of the database URL and resolve it only when the online engine is built.
"""

from __future__ import annotations

import os
from typing import Mapping
from urllib.parse import urlparse

from .credentials import CredentialResolutionError
from .runtime import ResearchOSSettings


def online_migration_connect_args(
    database_url: str,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Return safe driver arguments for one online migration engine.

    Non-PostgreSQL development migrations need no external credential.  For
    PostgreSQL, reuse ``ResearchOSSettings`` so secret-file validation, alias
    agreement and plaintext-password rejection remain one shared contract.
    Only database-related variables are forwarded: an Alembic migration must
    not read unrelated object-store or vendor credentials.
    """

    parsed = urlparse(str(database_url or ""))
    if not parsed.scheme.startswith("postgresql"):
        return {}

    values = os.environ if env is None else env
    database_values = {
        # Enforce the production URL/password rules for every PostgreSQL
        # migration, including a manual host invocation that omitted the
        # broader application environment flag.
        "FACTOR_LAB_ENVIRONMENT": "production",
        "FACTOR_LAB_DATABASE_URL": str(database_url),
        "FACTOR_LAB_ICEBERG_CATALOG_URI": str(database_url),
        "FACTOR_LAB_POSTGRES_PASSWORD_FILE": str(
            values.get("FACTOR_LAB_POSTGRES_PASSWORD_FILE") or ""
        ),
        "RESEARCH_OS_POSTGRES_PASSWORD_FILE": str(
            values.get("RESEARCH_OS_POSTGRES_PASSWORD_FILE") or ""
        ),
        "FACTOR_LAB_POSTGRES_PASSWORD": str(
            values.get("FACTOR_LAB_POSTGRES_PASSWORD") or ""
        ),
    }
    settings = ResearchOSSettings.from_env(database_values)
    connect_args = settings.database_connect_args()
    if not connect_args.get("password"):
        # Passing an empty connect_args mapping would let libpq/psycopg fall
        # back to ambient PGPASSWORD, PGPASSFILE or ~/.pgpass state.  Online
        # production migrations have one credential authority: the reviewed
        # password-file aliases above.
        raise CredentialResolutionError(
            "PostgreSQL online migration requires a password-file credential"
        )
    return connect_args


__all__ = ["online_migration_connect_args"]
