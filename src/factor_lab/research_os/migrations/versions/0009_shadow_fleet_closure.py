"""Add immutable daily Champion/Challenger fleet closure authority.

Revision ID: 0009_shadow_fleet_closure
Revises: 0008_production_authority
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0009_shadow_fleet_closure"
down_revision = "0008_production_authority"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ros_shadow_fleet_closures",
        sa.Column("closure_id", sa.String(96), primary_key=True),
        sa.Column("trade_date", sa.String(10), nullable=False, unique=True),
        sa.Column("evidence_class", sa.String(24), nullable=False),
        sa.Column(
            "epoch_id",
            sa.String(80),
            sa.ForeignKey("ros_evidence_epochs.epoch_id"),
            nullable=True,
        ),
        sa.Column("evidence_window_hash", sa.String(64), nullable=True),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("members_json", sa.JSON(), nullable=False),
        sa.Column("closure_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "evidence_class IN ('engineering','forward')",
            name="ck_ros_shadow_fleet_evidence_class",
        ),
        sa.CheckConstraint(
            "member_count > 0",
            name="ck_ros_shadow_fleet_member_count",
        ),
        sa.CheckConstraint(
            "((evidence_class = 'engineering' AND epoch_id IS NULL "
            "AND evidence_window_hash IS NULL) OR "
            "(evidence_class = 'forward' AND epoch_id IS NOT NULL "
            "AND evidence_window_hash IS NOT NULL))",
            name="ck_ros_shadow_fleet_epoch_binding",
        ),
    )
    op.create_index(
        "ix_ros_shadow_fleet_epoch_date",
        "ros_shadow_fleet_closures",
        ["epoch_id", "trade_date"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ros_shadow_fleet_epoch_date",
        table_name="ros_shadow_fleet_closures",
    )
    op.drop_table("ros_shadow_fleet_closures")
