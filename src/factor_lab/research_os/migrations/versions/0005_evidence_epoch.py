"""Persist the unique architecture freeze and forward evidence window.

Revision ID: 0005_evidence_epoch
Revises: 0004_atomic_trials
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0005_evidence_epoch"
down_revision = "0004_atomic_trials"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ros_evidence_epochs",
        sa.Column("epoch_slot", sa.String(32), primary_key=True),
        sa.Column("epoch_id", sa.String(80), nullable=False, unique=True),
        sa.Column("schema_version", sa.String(80), nullable=False),
        sa.Column("architecture_version", sa.String(160), nullable=False),
        sa.Column("frozen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("code_hash", sa.String(64), nullable=False),
        sa.Column("configuration_hash", sa.String(64), nullable=False),
        sa.Column("dependency_lock_hash", sa.String(64), nullable=False),
        sa.Column("dirty_patch_hash", sa.String(64), nullable=False),
        sa.Column("epoch_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("first_forward_session", sa.String(10), nullable=True),
        sa.Column(
            "calendar_snapshot_id",
            sa.String(160),
            sa.ForeignKey("ros_data_snapshots.snapshot_id"),
            nullable=True,
        ),
        sa.Column("calendar_snapshot_hash", sa.String(64), nullable=True),
        sa.Column("calendar_content_hash", sa.String(64), nullable=True),
        sa.Column("evidence_window_hash", sa.String(64), nullable=True, unique=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "epoch_slot = 'research_os'",
            name="ck_ros_evidence_epoch_singleton",
        ),
        sa.CheckConstraint(
            "((first_forward_session IS NULL AND calendar_snapshot_id IS NULL "
            "AND calendar_snapshot_hash IS NULL AND calendar_content_hash IS NULL "
            "AND evidence_window_hash IS NULL AND activated_at IS NULL) OR "
            "(first_forward_session IS NOT NULL AND calendar_snapshot_id IS NOT NULL "
            "AND calendar_snapshot_hash IS NOT NULL AND calendar_content_hash IS NOT NULL "
            "AND evidence_window_hash IS NOT NULL AND activated_at IS NOT NULL))",
            name="ck_ros_evidence_epoch_activation_complete",
        ),
    )


def downgrade() -> None:
    op.drop_table("ros_evidence_epochs")
