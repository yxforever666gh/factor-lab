"""Add an Alembic-owned database runtime authority marker.

Revision ID: 0008_production_authority
Revises: 0007_production_ledger
"""

from __future__ import annotations

import hashlib
import json

from alembic import op
import sqlalchemy as sa


revision = "0008_production_authority"
down_revision = "0007_production_ledger"
branch_labels = None
depends_on = None

MARKER_KEY = "research_os"
AUTHORITY_SCHEMA = "research-os/runtime-authority/v1"
HASH_DOMAIN = "factor-lab/research-os/v1/runtime-authority-marker"


def _marker_hash(environment: str) -> str:
    payload = {
        "authority_schema": AUTHORITY_SCHEMA,
        "environment": environment,
        "marker_key": MARKER_KEY,
    }
    envelope = {"domain": HASH_DOMAIN, "payload": payload}
    encoded = json.dumps(
        envelope,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def upgrade() -> None:
    op.create_table(
        "ros_runtime_authority",
        sa.Column("marker_key", sa.String(32), primary_key=True),
        sa.Column("environment", sa.String(16), nullable=False),
        sa.Column("authority_schema", sa.String(80), nullable=False),
        sa.Column("marker_hash", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "installed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "marker_key = 'research_os'",
            name="ck_ros_runtime_authority_singleton",
        ),
        sa.CheckConstraint(
            "environment IN ('production','test')",
            name="ck_ros_runtime_authority_environment",
        ),
    )
    dialect = op.get_bind().dialect.name
    environment = "production" if dialect == "postgresql" else "test"
    marker = sa.table(
        "ros_runtime_authority",
        sa.column("marker_key", sa.String(32)),
        sa.column("environment", sa.String(16)),
        sa.column("authority_schema", sa.String(80)),
        sa.column("marker_hash", sa.String(64)),
    )
    op.bulk_insert(
        marker,
        [
            {
                "marker_key": MARKER_KEY,
                "environment": environment,
                "authority_schema": AUTHORITY_SCHEMA,
                "marker_hash": _marker_hash(environment),
            }
        ],
    )


def downgrade() -> None:
    op.drop_table("ros_runtime_authority")
