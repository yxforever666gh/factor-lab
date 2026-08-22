"""Make confirmatory trial admission and completion durable.

Revision ID: 0004_atomic_trials
Revises: 0003_cleanup_dagster
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0004_atomic_trials"
down_revision = "0003_cleanup_dagster"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("ros_trial_ledger") as batch:
        batch.add_column(
            sa.Column(
                "admission_status",
                sa.String(16),
                nullable=False,
                server_default="admitted",
            )
        )
        batch.add_column(
            sa.Column("experiment_fingerprint", sa.String(64), nullable=True)
        )
        batch.add_column(
            sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True)
        )
        batch.add_column(
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            )
        )
        batch.create_check_constraint(
            "ck_ros_trials_admission_status",
            "admission_status IN ('admitted', 'rejected')",
        )
    op.execute(
        """
        UPDATE ros_trial_ledger AS trial
        SET experiment_fingerprint = COALESCE(
                trial.metadata_json ->> 'experiment_fingerprint',
                experiment.fingerprint
            ),
            completed_at = trial.occurred_at,
            updated_at = trial.occurred_at
        FROM ros_experiments AS experiment
        WHERE trial.experiment_id = experiment.experiment_id
        """
    )
    op.execute(
        """
        UPDATE ros_trial_ledger
        SET experiment_fingerprint = metadata_json ->> 'experiment_fingerprint',
            completed_at = occurred_at,
            updated_at = occurred_at
        WHERE experiment_id IS NULL
        """
    )
    op.create_index(
        "ix_ros_trials_admission_month_family",
        "ros_trial_ledger",
        ["admission_status", "occurred_at", "family"],
    )
    op.create_index(
        "ix_ros_trials_fingerprint",
        "ros_trial_ledger",
        ["experiment_fingerprint"],
    )
    op.create_index(
        "uq_ros_trials_admitted_fingerprint",
        "ros_trial_ledger",
        ["experiment_fingerprint"],
        unique=True,
        postgresql_where=sa.text(
            "admission_status = 'admitted' AND experiment_fingerprint IS NOT NULL"
        ),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_ros_trials_admitted_fingerprint", table_name="ros_trial_ledger"
    )
    op.drop_index("ix_ros_trials_fingerprint", table_name="ros_trial_ledger")
    op.drop_index(
        "ix_ros_trials_admission_month_family", table_name="ros_trial_ledger"
    )
    with op.batch_alter_table("ros_trial_ledger") as batch:
        batch.drop_constraint("ck_ros_trials_admission_status", type_="check")
        batch.drop_column("updated_at")
        batch.drop_column("completed_at")
        batch.drop_column("experiment_fingerprint")
        batch.drop_column("admission_status")
