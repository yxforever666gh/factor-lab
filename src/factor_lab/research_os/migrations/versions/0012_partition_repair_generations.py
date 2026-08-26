"""Add immutable incident-selected partition repair generations.

Revision ID: 0012_partition_repair_gen
Revises: 0011_shadow_execution_incidents
"""

from __future__ import annotations

from alembic import context, op
import sqlalchemy as sa


revision = "0012_partition_repair_gen"
down_revision = "0011_shadow_execution_incidents"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ros_partition_runs") as batch:
        batch.drop_constraint(
            "uq_ros_partition_source_dataset_key", type_="unique"
        )
        batch.add_column(
            sa.Column(
                "generation",
                sa.String(80),
                nullable=False,
                server_default="base",
            )
        )
        batch.add_column(
            sa.Column("repair_incident_id", sa.String(96), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "repair_parent_partition_run_id", sa.String(96), nullable=True
            )
        )
        batch.add_column(
            sa.Column("repair_parent_hash", sa.String(64), nullable=True)
        )
        batch.add_column(
            sa.Column("repair_fingerprint", sa.String(64), nullable=True)
        )
        batch.create_foreign_key(
            "fk_ros_partition_repair_incident",
            "ros_data_incidents",
            ["repair_incident_id"],
            ["incident_id"],
        )
        batch.create_foreign_key(
            "fk_ros_partition_repair_parent",
            "ros_partition_runs",
            ["repair_parent_partition_run_id"],
            ["partition_run_id"],
        )
        batch.create_unique_constraint(
            "uq_ros_partition_source_dataset_key_generation",
            ["source_id", "dataset", "partition_key", "generation"],
        )
        batch.create_check_constraint(
            "ck_ros_partition_repair_generation",
            "((generation = 'base' AND repair_incident_id IS NULL "
            "AND repair_parent_partition_run_id IS NULL "
            "AND repair_parent_hash IS NULL AND repair_fingerprint IS NULL) OR "
            "(generation <> 'base' "
            "AND repair_parent_partition_run_id IS NOT NULL "
            "AND repair_parent_hash IS NOT NULL AND repair_fingerprint IS NOT NULL))",
        )

    op.create_table(
        "ros_partition_repair_authorities",
        sa.Column("authority_id", sa.String(96), primary_key=True),
        sa.Column("scope_key", sa.String(160), nullable=False),
        sa.Column(
            "incident_id",
            sa.String(96),
            sa.ForeignKey("ros_data_incidents.incident_id"),
            nullable=True,
        ),
        sa.Column("source_id", sa.String(80), nullable=False),
        sa.Column("dataset", sa.String(80), nullable=False),
        sa.Column("partition_key", sa.String(32), nullable=False),
        sa.Column("generation", sa.String(80), nullable=False),
        sa.Column(
            "parent_partition_run_id",
            sa.String(96),
            sa.ForeignKey("ros_partition_runs.partition_run_id"),
            nullable=False,
        ),
        sa.Column("parent_terminal_hash", sa.String(64), nullable=False),
        sa.Column(
            "successor_partition_run_id",
            sa.String(96),
            sa.ForeignKey("ros_partition_runs.partition_run_id"),
            nullable=False,
        ),
        sa.Column("repair_fingerprint", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "parent_partition_run_id",
            name="uq_ros_partition_repair_parent",
        ),
        sa.UniqueConstraint(
            "successor_partition_run_id",
            name="uq_ros_partition_repair_successor",
        ),
    )
    op.create_index(
        "ix_ros_partition_repair_incident",
        "ros_partition_repair_authorities",
        ["incident_id", "partition_key", "dataset"],
    )
    op.create_index(
        "ix_ros_partition_repair_scope_slot",
        "ros_partition_repair_authorities",
        ["scope_key", "source_id", "dataset", "partition_key"],
    )


def downgrade() -> None:
    # Repair generations and their authority rows are immutable safety
    # evidence.  A batch-table downgrade would otherwise start dropping the
    # authority table and constraints before discovering that the old
    # one-row-per-partition schema cannot represent the retained evidence.
    # Refuse *before* emitting or executing any DDL so a failed downgrade is a
    # true no-op.
    if context.is_offline_mode():
        raise RuntimeError(
            "Refusing offline downgrade 0012_partition_repair_gen: "
            "an online immutable repair-evidence preflight is required"
        )

    connection = op.get_bind()
    non_base_generations = int(
        connection.execute(
            sa.text(
                "SELECT COUNT(*) FROM ros_partition_runs "
                "WHERE generation <> 'base'"
            )
        ).scalar_one()
    )
    repair_authorities = int(
        connection.execute(
            sa.text("SELECT COUNT(*) FROM ros_partition_repair_authorities")
        ).scalar_one()
    )
    if non_base_generations or repair_authorities:
        raise RuntimeError(
            "Refusing downgrade 0012_partition_repair_gen: immutable "
            "partition repair evidence exists "
            f"(non_base_generations={non_base_generations}, "
            f"repair_authorities={repair_authorities})"
        )

    op.drop_index(
        "ix_ros_partition_repair_scope_slot",
        table_name="ros_partition_repair_authorities",
    )
    op.drop_index(
        "ix_ros_partition_repair_incident",
        table_name="ros_partition_repair_authorities",
    )
    op.drop_table("ros_partition_repair_authorities")
    with op.batch_alter_table("ros_partition_runs") as batch:
        batch.drop_constraint(
            "ck_ros_partition_repair_generation", type_="check"
        )
        batch.drop_constraint(
            "uq_ros_partition_source_dataset_key_generation", type_="unique"
        )
        batch.drop_constraint(
            "fk_ros_partition_repair_parent", type_="foreignkey"
        )
        batch.drop_constraint(
            "fk_ros_partition_repair_incident", type_="foreignkey"
        )
        batch.create_unique_constraint(
            "uq_ros_partition_source_dataset_key",
            ["source_id", "dataset", "partition_key"],
        )
        batch.drop_column("repair_fingerprint")
        batch.drop_column("repair_parent_hash")
        batch.drop_column("repair_parent_partition_run_id")
        batch.drop_column("repair_incident_id")
        batch.drop_column("generation")
