"""Add production partition, incident, role and shadow-session ledgers.

Revision ID: 0007_production_ledger
Revises: 0006_research_submissions
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "0007_production_ledger"
down_revision = "0006_research_submissions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ros_source_capabilities",
        sa.Column("source_id", sa.String(80), primary_key=True),
        sa.Column("dataset", sa.String(80), primary_key=True),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("contract_hash", sa.String(64), nullable=False),
        sa.Column("probe_hash", sa.String(64), nullable=True),
        sa.Column("fields_json", sa.JSON(), nullable=False),
        sa.Column("detail", sa.Text(), nullable=False),
        sa.Column("probed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('accepted','degraded','unavailable','disputed')",
            name="ck_ros_source_capability_status",
        ),
    )
    op.create_index(
        "ix_ros_source_capabilities_status",
        "ros_source_capabilities",
        ["status", "updated_at"],
    )

    op.create_table(
        "ros_partition_runs",
        sa.Column("partition_run_id", sa.String(96), primary_key=True),
        sa.Column("source_id", sa.String(80), nullable=False),
        sa.Column("dataset", sa.String(80), nullable=False),
        sa.Column("partition_key", sa.String(32), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("lease_owner", sa.String(160), nullable=True),
        sa.Column("lease_token", sa.String(64), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "run_id",
            sa.String(160),
            sa.ForeignKey("ros_runs.run_id"),
            nullable=True,
        ),
        sa.Column(
            "output_snapshot_id",
            sa.String(160),
            sa.ForeignKey("ros_data_snapshots.snapshot_id"),
            nullable=True,
        ),
        sa.Column("input_hash", sa.String(64), nullable=True),
        sa.Column("output_hash", sa.String(64), nullable=True),
        sa.Column("vendor_revision", sa.String(160), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("error_code", sa.String(120), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending','running','succeeded','disputed','quarantined','failed')",
            name="ck_ros_partition_status",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_ros_partition_attempts"),
        sa.CheckConstraint(
            "((status = 'running' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL AND started_at IS NOT NULL) OR "
            "(status <> 'running' AND lease_owner IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL))",
            name="ck_ros_partition_lease_state",
        ),
        sa.UniqueConstraint(
            "source_id",
            "dataset",
            "partition_key",
            name="uq_ros_partition_source_dataset_key",
        ),
    )
    op.create_index(
        "ix_ros_partition_status_key",
        "ros_partition_runs",
        ["status", "partition_key"],
    )
    op.create_index(
        "ix_ros_partition_source_dataset",
        "ros_partition_runs",
        ["source_id", "dataset", "partition_key"],
    )
    op.create_index(
        "ix_ros_partition_lease_expiry",
        "ros_partition_runs",
        ["status", "lease_expires_at"],
    )

    op.create_table(
        "ros_data_incidents",
        sa.Column("incident_id", sa.String(96), primary_key=True),
        sa.Column("incident_hash", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "partition_run_id",
            sa.String(96),
            sa.ForeignKey("ros_partition_runs.partition_run_id"),
            nullable=True,
        ),
        sa.Column("partition_key", sa.String(32), nullable=False),
        sa.Column("stage", sa.String(24), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("error_code", sa.String(120), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("source_ids_json", sa.JSON(), nullable=False),
        sa.Column("evidence_hashes_json", sa.JSON(), nullable=False),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolution_hash", sa.String(64), nullable=True),
        sa.CheckConstraint(
            "stage IN ('source','silver','data_quality','gold')",
            name="ck_ros_data_incident_stage",
        ),
        sa.CheckConstraint(
            "status IN ('open','resolved','superseded')",
            name="ck_ros_data_incident_status",
        ),
        sa.CheckConstraint(
            "((status = 'open' AND resolved_at IS NULL AND resolution_hash IS NULL) OR "
            "(status <> 'open' AND resolved_at IS NOT NULL AND resolution_hash IS NOT NULL))",
            name="ck_ros_data_incident_resolution",
        ),
    )
    op.create_index(
        "ix_ros_data_incidents_status_time",
        "ros_data_incidents",
        ["status", "occurred_at"],
    )
    op.create_index(
        "ix_ros_data_incidents_partition",
        "ros_data_incidents",
        ["partition_key", "stage"],
    )

    op.create_table(
        "ros_sleeve_clusters",
        sa.Column("cluster_record_id", sa.String(96), primary_key=True),
        sa.Column("manifest_id", sa.String(96), nullable=False),
        sa.Column("cluster_id", sa.String(160), nullable=False),
        sa.Column("family_id", sa.String(160), nullable=False),
        sa.Column(
            "representative_experiment_id",
            sa.String(80),
            sa.ForeignKey("ros_experiments.experiment_id"),
            nullable=False,
        ),
        sa.Column("member_experiment_ids_json", sa.JSON(), nullable=False),
        sa.Column("active_returns_hash", sa.String(64), nullable=False),
        sa.Column("decision_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "manifest_id", "cluster_id", name="uq_ros_sleeve_cluster_manifest"
        ),
    )
    op.create_index(
        "ix_ros_sleeve_clusters_family",
        "ros_sleeve_clusters",
        ["family_id", "created_at"],
    )

    op.create_table(
        "ros_shadow_role_bindings",
        sa.Column("binding_id", sa.String(96), primary_key=True),
        sa.Column("binding_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("role", sa.String(24), nullable=False),
        sa.Column("role_key", sa.String(160), nullable=False),
        sa.Column(
            "account_id",
            sa.String(160),
            sa.ForeignKey("ros_shadow_accounts.account_id"),
            nullable=False,
        ),
        sa.Column("sleeve_id", sa.String(160), nullable=True),
        sa.Column(
            "experiment_id",
            sa.String(80),
            sa.ForeignKey("ros_experiments.experiment_id"),
            nullable=True,
        ),
        sa.Column(
            "epoch_id",
            sa.String(80),
            sa.ForeignKey("ros_evidence_epochs.epoch_id"),
            nullable=True,
        ),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("bound_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("unbound_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.CheckConstraint(
            "role IN ('champion','challenger','sleeve')",
            name="ck_ros_shadow_binding_role",
        ),
        sa.CheckConstraint(
            "((active AND unbound_at IS NULL) OR ((NOT active) AND unbound_at IS NOT NULL))",
            name="ck_ros_shadow_binding_active",
        ),
    )
    op.create_index(
        "uq_ros_shadow_binding_active_role",
        "ros_shadow_role_bindings",
        ["role", "role_key"],
        unique=True,
        postgresql_where=sa.text("active"),
        sqlite_where=sa.text("active = 1"),
    )
    op.create_index(
        "ix_ros_shadow_binding_account",
        "ros_shadow_role_bindings",
        ["account_id", "active"],
    )

    op.create_table(
        "ros_shadow_sessions",
        sa.Column(
            "account_id",
            sa.String(160),
            sa.ForeignKey("ros_shadow_accounts.account_id"),
            primary_key=True,
        ),
        sa.Column("trade_date", sa.String(10), primary_key=True),
        sa.Column(
            "role_binding_id",
            sa.String(96),
            sa.ForeignKey("ros_shadow_role_bindings.binding_id"),
            nullable=True,
        ),
        sa.Column(
            "epoch_id",
            sa.String(80),
            sa.ForeignKey("ros_evidence_epochs.epoch_id"),
            nullable=True,
        ),
        sa.Column("evidence_window_hash", sa.String(64), nullable=True),
        sa.Column("evidence_class", sa.String(24), nullable=False),
        sa.Column(
            "decision_snapshot_id",
            sa.String(160),
            sa.ForeignKey("ros_data_snapshots.snapshot_id"),
            nullable=True,
        ),
        sa.Column(
            "execution_snapshot_id",
            sa.String(160),
            sa.ForeignKey("ros_data_snapshots.snapshot_id"),
            nullable=False,
        ),
        sa.Column(
            "mark_snapshot_id",
            sa.String(160),
            sa.ForeignKey("ros_data_snapshots.snapshot_id"),
            nullable=False,
        ),
        sa.Column("rebalanced", sa.Boolean(), nullable=False),
        sa.Column("cash", sa.Float(), nullable=False),
        sa.Column("positions_value", sa.Float(), nullable=False),
        sa.Column("nav", sa.Float(), nullable=False),
        sa.Column("benchmark_nav", sa.Float(), nullable=False),
        sa.Column("position_count", sa.Integer(), nullable=False),
        sa.Column("account_event_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("account_event_sequence", sa.Integer(), nullable=False),
        sa.Column("session_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "evidence_class IN ('engineering','forward')",
            name="ck_ros_shadow_session_evidence_class",
        ),
        sa.CheckConstraint(
            "((evidence_class = 'engineering' AND epoch_id IS NULL "
            "AND evidence_window_hash IS NULL) OR "
            "(evidence_class = 'forward' AND epoch_id IS NOT NULL "
            "AND evidence_window_hash IS NOT NULL))",
            name="ck_ros_shadow_session_epoch_binding",
        ),
        sa.CheckConstraint(
            "cash >= 0 AND positions_value >= 0 AND nav > 0 "
            "AND benchmark_nav > 0 AND position_count >= 0",
            name="ck_ros_shadow_session_account_values",
        ),
        sa.CheckConstraint(
            "ABS((cash + positions_value) - nav) <= 0.01",
            name="ck_ros_shadow_session_account_equation",
        ),
        sa.CheckConstraint(
            "execution_snapshot_id <> mark_snapshot_id",
            name="ck_ros_shadow_session_snapshot_roles",
        ),
        sa.CheckConstraint(
            "decision_snapshot_id IS NULL OR "
            "(decision_snapshot_id <> execution_snapshot_id AND "
            "decision_snapshot_id <> mark_snapshot_id)",
            name="ck_ros_shadow_session_decision_snapshot_role",
        ),
    )
    op.create_index(
        "ix_ros_shadow_sessions_epoch_date",
        "ros_shadow_sessions",
        ["epoch_id", "trade_date"],
    )
    op.create_index(
        "ix_ros_shadow_sessions_binding_date",
        "ros_shadow_sessions",
        ["role_binding_id", "trade_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_ros_shadow_sessions_binding_date", table_name="ros_shadow_sessions")
    op.drop_index("ix_ros_shadow_sessions_epoch_date", table_name="ros_shadow_sessions")
    op.drop_table("ros_shadow_sessions")
    op.drop_index("ix_ros_shadow_binding_account", table_name="ros_shadow_role_bindings")
    op.drop_index(
        "uq_ros_shadow_binding_active_role", table_name="ros_shadow_role_bindings"
    )
    op.drop_table("ros_shadow_role_bindings")
    op.drop_index("ix_ros_sleeve_clusters_family", table_name="ros_sleeve_clusters")
    op.drop_table("ros_sleeve_clusters")
    op.drop_index("ix_ros_data_incidents_partition", table_name="ros_data_incidents")
    op.drop_index("ix_ros_data_incidents_status_time", table_name="ros_data_incidents")
    op.drop_table("ros_data_incidents")
    op.drop_index("ix_ros_partition_lease_expiry", table_name="ros_partition_runs")
    op.drop_index("ix_ros_partition_source_dataset", table_name="ros_partition_runs")
    op.drop_index("ix_ros_partition_status_key", table_name="ros_partition_runs")
    op.drop_table("ros_partition_runs")
    op.drop_index(
        "ix_ros_source_capabilities_status", table_name="ros_source_capabilities"
    )
    op.drop_table("ros_source_capabilities")
