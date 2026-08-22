"""Create an isolated PostgreSQL schema for Dagster's own migrations.

Revision ID: 0002_dagster_schema
Revises: 0001_research_os_foundation
"""

from __future__ import annotations

from alembic import op


revision = "0002_dagster_schema"
down_revision = "0001_research_os_foundation"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Dagster and Research OS both use Alembic internally.  A separate schema
    # gives each component its own alembic_version table while retaining one
    # PostgreSQL instance as the operational fact store.
    if op.get_bind().dialect.name == "postgresql":
        op.execute("CREATE SCHEMA IF NOT EXISTS dagster")


def downgrade() -> None:
    # Deliberately no CASCADE: downgrade must not silently destroy Dagster run
    # history.  Operators must empty/archive the schema explicitly first.
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP SCHEMA IF EXISTS dagster")
