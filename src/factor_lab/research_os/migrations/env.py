"""Alembic environment for the Research OS PostgreSQL catalog."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from factor_lab.research_os.orm import Base, SQLALCHEMY_AVAILABLE


if not SQLALCHEMY_AVAILABLE or Base is None:
    raise RuntimeError("Alembic migrations require the Research OS SQLAlchemy extras")

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = os.environ.get("RESEARCH_OS_DATABASE_URL")
if database_url:
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
elif not context.is_offline_mode():
    raise RuntimeError(
        "online Research OS migrations require RESEARCH_OS_DATABASE_URL"
    )

target_metadata = Base.metadata


def _include_research_os_object(
    object_, name: str | None, type_: str, reflected: bool, compare_to
) -> bool:
    """Keep Alembic scoped to ros_*; Iceberg and Dagster own their tables."""

    if type_ == "table" and reflected and name and not name.startswith("ros_"):
        return False
    return True


def run_migrations_offline() -> None:
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        include_object=_include_research_os_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            include_object=_include_research_os_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
