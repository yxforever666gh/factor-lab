"""Add authoritative research families, semantic trials and resumable submissions.

Revision ID: 0006_research_submissions
Revises: 0005_evidence_epoch
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0006_research_submissions"
down_revision = "0005_evidence_epoch"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ros_trial_ledger",
        sa.Column("research_equivalence_hash", sa.String(64), nullable=True),
    )
    op.create_index(
        "ix_ros_trials_equivalence",
        "ros_trial_ledger",
        ["research_equivalence_hash"],
    )
    op.create_index(
        "uq_ros_trials_admitted_equivalence",
        "ros_trial_ledger",
        ["research_equivalence_hash"],
        unique=True,
        postgresql_where=sa.text(
            "admission_status = 'admitted' AND research_equivalence_hash IS NOT NULL"
        ),
        sqlite_where=sa.text(
            "admission_status = 'admitted' AND research_equivalence_hash IS NOT NULL"
        ),
    )

    op.create_table(
        "ros_research_families",
        sa.Column("family_id", sa.String(160), primary_key=True),
        sa.Column("registry_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("definition_json", sa.JSON(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "ros_research_submissions",
        sa.Column("submission_id", sa.String(96), primary_key=True),
        sa.Column("proposal_decision_id", sa.String(96), nullable=False),
        sa.Column(
            "family_id",
            sa.String(160),
            sa.ForeignKey("ros_research_families.family_id"),
            nullable=False,
        ),
        sa.Column(
            "recovery_case_id",
            sa.String(160),
            sa.ForeignKey("ros_recovery_cases.recovery_case_id"),
            nullable=True,
        ),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("research_equivalence_hash", sa.String(64), nullable=False),
        sa.Column("experiment_fingerprint", sa.String(64), nullable=False),
        sa.Column("trial_id", sa.String(80), nullable=False, unique=True),
        sa.Column("spec_json", sa.JSON(), nullable=False),
        sa.Column("lease_owner", sa.String(160), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "experiment_id",
            sa.String(80),
            sa.ForeignKey("ros_experiments.experiment_id"),
            nullable=True,
        ),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('reviewed','reserved','running','completed','failed','missing_data')",
            name="ck_ros_submission_status",
        ),
        sa.UniqueConstraint(
            "proposal_decision_id",
            "research_equivalence_hash",
            name="uq_ros_submission_decision_equivalence",
        ),
    )
    op.create_index(
        "ix_ros_submission_status_time",
        "ros_research_submissions",
        ["status", "updated_at"],
    )
    op.create_index(
        "ix_ros_submission_family",
        "ros_research_submissions",
        ["family_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_ros_submission_family", table_name="ros_research_submissions")
    op.drop_index("ix_ros_submission_status_time", table_name="ros_research_submissions")
    op.drop_table("ros_research_submissions")
    op.drop_table("ros_research_families")
    op.drop_index(
        "uq_ros_trials_admitted_equivalence", table_name="ros_trial_ledger"
    )
    op.drop_index("ix_ros_trials_equivalence", table_name="ros_trial_ledger")
    op.drop_column("ros_trial_ledger", "research_equivalence_hash")
