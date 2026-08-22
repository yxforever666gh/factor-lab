"""Replace the permanent epoch singleton with versioned epochs and one pointer.

Revision ID: 0010_evidence_epoch_versions
Revises: 0009_shadow_fleet_closure
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0010_evidence_epoch_versions"
down_revision = "0009_shadow_fleet_closure"
branch_labels = None
depends_on = None


_OPEN_ACTIVE = sa.text("activated_at IS NOT NULL AND closed_at IS NULL")


def upgrade() -> None:
    with op.batch_alter_table("ros_evidence_epochs") as batch:
        batch.drop_constraint(
            "ck_ros_evidence_epoch_singleton", type_="check"
        )
        batch.alter_column(
            "epoch_slot",
            existing_type=sa.String(32),
            type_=sa.String(80),
            existing_nullable=False,
        )
        batch.add_column(
            sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column("superseded_by_epoch_id", sa.String(80), nullable=True)
        )
        batch.create_foreign_key(
            "fk_ros_evidence_epoch_superseded_by",
            "ros_evidence_epochs",
            ["superseded_by_epoch_id"],
            ["epoch_id"],
        )
        batch.create_check_constraint(
            "ck_ros_evidence_epoch_closure_complete",
            "((closed_at IS NULL AND superseded_by_epoch_id IS NULL) OR "
            "(closed_at IS NOT NULL AND superseded_by_epoch_id IS NOT NULL))",
        )

    op.create_index(
        "ix_ros_evidence_epochs_frozen_at",
        "ros_evidence_epochs",
        ["frozen_at"],
    )
    op.create_index(
        "uq_ros_evidence_epoch_one_active",
        "ros_evidence_epochs",
        [sa.text("(1)")],
        unique=True,
        postgresql_where=_OPEN_ACTIVE,
        sqlite_where=_OPEN_ACTIVE,
    )
    op.create_table(
        "ros_evidence_epoch_active_pointer",
        sa.Column("pointer_key", sa.String(32), primary_key=True),
        sa.Column(
            "epoch_id",
            sa.String(80),
            sa.ForeignKey("ros_evidence_epochs.epoch_id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "pointer_key = 'research_os'",
            name="ck_ros_evidence_epoch_pointer_singleton",
        ),
    )
    # The pre-versioning schema allowed at most one row. Preserve it as the
    # active pointer only when its forward window had already been activated.
    op.execute(
        "INSERT INTO ros_evidence_epoch_active_pointer"
        "(pointer_key, epoch_id, updated_at) "
        "SELECT 'research_os', epoch_id, activated_at "
        "FROM ros_evidence_epochs WHERE activated_at IS NOT NULL"
    )


def downgrade() -> None:
    op.drop_table("ros_evidence_epoch_active_pointer")
    op.drop_index(
        "uq_ros_evidence_epoch_one_active", table_name="ros_evidence_epochs"
    )
    op.drop_index(
        "ix_ros_evidence_epochs_frozen_at", table_name="ros_evidence_epochs"
    )
    # This intentionally fails on a catalog that already contains multiple
    # retained versions rather than silently deleting historical evidence.
    op.execute("UPDATE ros_evidence_epochs SET epoch_slot = 'research_os'")
    with op.batch_alter_table("ros_evidence_epochs") as batch:
        batch.drop_constraint(
            "ck_ros_evidence_epoch_closure_complete", type_="check"
        )
        batch.drop_constraint(
            "fk_ros_evidence_epoch_superseded_by", type_="foreignkey"
        )
        batch.drop_column("superseded_by_epoch_id")
        batch.drop_column("closed_at")
        batch.alter_column(
            "epoch_slot",
            existing_type=sa.String(80),
            type_=sa.String(32),
            existing_nullable=False,
        )
        batch.create_check_constraint(
            "ck_ros_evidence_epoch_singleton",
            "epoch_slot = 'research_os'",
        )
