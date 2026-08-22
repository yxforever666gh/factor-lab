"""Create the authoritative Research OS catalog.

Revision ID: 0001_research_os_foundation
Revises: None
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0001_research_os_foundation"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ros_schema_metadata",
        sa.Column("component", sa.String(80), primary_key=True),
        sa.Column("schema_version", sa.String(80), nullable=False),
        sa.Column(
            "installed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.bulk_insert(
        sa.table(
            "ros_schema_metadata",
            sa.column("component", sa.String),
            sa.column("schema_version", sa.String),
        ),
        [{"component": "research_os", "schema_version": "research-os/v1"}],
    )

    op.create_table(
        "ros_data_snapshots",
        sa.Column("snapshot_id", sa.String(160), primary_key=True),
        sa.Column("schema_version", sa.String(80), nullable=False),
        sa.Column("tier", sa.String(32), nullable=False),
        sa.Column("uri", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("quality_status", sa.String(32), nullable=False),
        sa.Column("ref_json", sa.JSON, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_ros_data_snapshots_as_of", "ros_data_snapshots", ["as_of"])
    op.create_index(
        "ix_ros_data_snapshots_quality", "ros_data_snapshots", ["quality_status"]
    )

    op.create_table(
        "ros_experiments",
        sa.Column("experiment_id", sa.String(80), primary_key=True),
        sa.Column("fingerprint", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "snapshot_id",
            sa.String(160),
            sa.ForeignKey("ros_data_snapshots.snapshot_id"),
            nullable=False,
        ),
        sa.Column("candidate_kind", sa.String(24), nullable=False),
        sa.Column("candidate_id", sa.String(160), nullable=False),
        sa.Column("family", sa.String(160), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("spec_json", sa.JSON, nullable=False),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index("ix_ros_experiments_snapshot", "ros_experiments", ["snapshot_id"])
    op.create_index(
        "ix_ros_experiments_candidate",
        "ros_experiments",
        ["candidate_kind", "candidate_id"],
    )
    op.create_index("ix_ros_experiments_family", "ros_experiments", ["family"])

    op.create_table(
        "ros_experiment_results",
        sa.Column("result_id", sa.String(80), primary_key=True),
        sa.Column(
            "experiment_id",
            sa.String(80),
            sa.ForeignKey("ros_experiments.experiment_id"),
            nullable=False,
        ),
        sa.Column("result_hash", sa.String(64), nullable=False),
        sa.Column("outcome", sa.String(40), nullable=False),
        sa.Column("metrics_json", sa.JSON, nullable=False),
        sa.Column("artifact_uri", sa.Text, nullable=True),
        sa.Column("authoritative", sa.Boolean, nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "experiment_id", "result_hash", name="uq_ros_result_experiment_hash"
        ),
    )
    op.create_index(
        "ix_ros_results_experiment", "ros_experiment_results", ["experiment_id"]
    )
    op.create_index(
        "uq_ros_results_one_authoritative",
        "ros_experiment_results",
        ["experiment_id"],
        unique=True,
        postgresql_where=sa.text("authoritative"),
    )

    op.create_table(
        "ros_trial_ledger",
        sa.Column("trial_id", sa.String(80), primary_key=True),
        sa.Column(
            "experiment_id",
            sa.String(80),
            sa.ForeignKey("ros_experiments.experiment_id"),
            nullable=True,
        ),
        sa.Column("family", sa.String(160), nullable=False),
        sa.Column("candidate_id", sa.String(160), nullable=False),
        sa.Column("outcome", sa.String(40), nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("p_value", sa.Float, nullable=True),
        sa.Column("alpha_spent", sa.Float, nullable=False),
        sa.Column("metadata_json", sa.JSON, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_ros_trials_family_time", "ros_trial_ledger", ["family", "occurred_at"]
    )
    op.create_index("ix_ros_trials_candidate", "ros_trial_ledger", ["candidate_id"])

    op.create_table(
        "ros_lifecycle_events",
        sa.Column("event_id", sa.String(80), primary_key=True),
        sa.Column("idempotency_key", sa.String(160), nullable=False, unique=True),
        sa.Column("sleeve_id", sa.String(160), nullable=False),
        sa.Column("from_state", sa.String(32), nullable=True),
        sa.Column("to_state", sa.String(32), nullable=False),
        sa.Column("cause", sa.Text, nullable=False),
        sa.Column("evidence_json", sa.JSON, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_ros_lifecycle_sleeve_time",
        "ros_lifecycle_events",
        ["sleeve_id", "occurred_at"],
    )

    op.create_table(
        "ros_recovery_cases",
        sa.Column("recovery_case_id", sa.String(160), primary_key=True),
        sa.Column("sleeve_id", sa.String(160), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("lifecycle_state", sa.String(32), nullable=False),
        sa.Column("case_json", sa.JSON, nullable=False),
        sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_ros_recovery_sleeve_status",
        "ros_recovery_cases",
        ["sleeve_id", "status"],
    )

    op.create_table(
        "ros_runs",
        sa.Column("run_id", sa.String(160), primary_key=True),
        sa.Column("run_type", sa.String(80), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("input_fingerprint", sa.String(64), nullable=False),
        sa.Column("metadata_json", sa.JSON, nullable=False),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_ros_runs_type_time", "ros_runs", ["run_type", "started_at"])
    op.create_index("ix_ros_runs_status", "ros_runs", ["status"])

    op.create_table(
        "ros_legacy_evidence",
        sa.Column("evidence_id", sa.String(80), primary_key=True),
        sa.Column("source_uri", sa.Text, nullable=False),
        sa.Column("content_hash", sa.String(64), nullable=False),
        sa.Column("trust_label", sa.String(80), nullable=False),
        sa.Column("reasons_json", sa.JSON, nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "source_uri", "content_hash", name="uq_ros_legacy_source_content"
        ),
    )
    op.create_index(
        "ix_ros_legacy_evidence_trust_time",
        "ros_legacy_evidence",
        ["trust_label", "imported_at"],
    )

    op.create_table(
        "ros_shadow_accounts",
        sa.Column("account_id", sa.String(160), primary_key=True),
        sa.Column("name", sa.String(240), nullable=False),
        sa.Column("currency", sa.String(12), nullable=False),
        sa.Column("initial_capital", sa.Float, nullable=False),
        sa.Column("cash", sa.Float, nullable=False),
        sa.Column("nav", sa.Float, nullable=False),
        sa.Column("benchmark_nav", sa.Float, nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_event_sequence", sa.Integer, nullable=False),
        sa.Column("last_event_hash", sa.String(64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("initial_capital > 0", name="ck_ros_shadow_initial_capital"),
    )
    op.create_table(
        "ros_shadow_events",
        sa.Column("event_id", sa.String(80), primary_key=True),
        sa.Column(
            "account_id",
            sa.String(160),
            sa.ForeignKey("ros_shadow_accounts.account_id"),
            nullable=False,
        ),
        sa.Column("sequence_number", sa.Integer, nullable=False),
        sa.Column("event_type", sa.String(80), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload_json", sa.JSON, nullable=False),
        sa.Column("previous_event_hash", sa.String(64), nullable=False),
        sa.Column("event_hash", sa.String(64), nullable=False, unique=True),
        sa.UniqueConstraint(
            "account_id", "sequence_number", name="uq_ros_shadow_account_sequence"
        ),
    )
    op.create_index(
        "ix_ros_shadow_events_account_time",
        "ros_shadow_events",
        ["account_id", "occurred_at"],
    )
    op.create_table(
        "ros_shadow_positions",
        sa.Column(
            "account_id",
            sa.String(160),
            sa.ForeignKey("ros_shadow_accounts.account_id"),
            primary_key=True,
        ),
        sa.Column("ticker", sa.String(32), primary_key=True),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("average_cost", sa.Float, nullable=False),
        sa.Column("market_price", sa.Float, nullable=False),
        sa.Column("market_value", sa.Float, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_event_sequence", sa.Integer, nullable=False),
        sa.CheckConstraint("quantity >= 0", name="ck_ros_shadow_position_long_only"),
    )


def downgrade() -> None:
    op.drop_table("ros_shadow_positions")
    op.drop_index("ix_ros_shadow_events_account_time", table_name="ros_shadow_events")
    op.drop_table("ros_shadow_events")
    op.drop_table("ros_shadow_accounts")
    op.drop_index(
        "ix_ros_legacy_evidence_trust_time", table_name="ros_legacy_evidence"
    )
    op.drop_table("ros_legacy_evidence")
    op.drop_index("ix_ros_runs_status", table_name="ros_runs")
    op.drop_index("ix_ros_runs_type_time", table_name="ros_runs")
    op.drop_table("ros_runs")
    op.drop_index("ix_ros_recovery_sleeve_status", table_name="ros_recovery_cases")
    op.drop_table("ros_recovery_cases")
    op.drop_index("ix_ros_lifecycle_sleeve_time", table_name="ros_lifecycle_events")
    op.drop_table("ros_lifecycle_events")
    op.drop_index("ix_ros_trials_candidate", table_name="ros_trial_ledger")
    op.drop_index("ix_ros_trials_family_time", table_name="ros_trial_ledger")
    op.drop_table("ros_trial_ledger")
    op.drop_index(
        "uq_ros_results_one_authoritative", table_name="ros_experiment_results"
    )
    op.drop_index("ix_ros_results_experiment", table_name="ros_experiment_results")
    op.drop_table("ros_experiment_results")
    op.drop_index("ix_ros_experiments_family", table_name="ros_experiments")
    op.drop_index("ix_ros_experiments_candidate", table_name="ros_experiments")
    op.drop_index("ix_ros_experiments_snapshot", table_name="ros_experiments")
    op.drop_table("ros_experiments")
    op.drop_index("ix_ros_data_snapshots_quality", table_name="ros_data_snapshots")
    op.drop_index("ix_ros_data_snapshots_as_of", table_name="ros_data_snapshots")
    op.drop_table("ros_data_snapshots")
    op.drop_table("ros_schema_metadata")
